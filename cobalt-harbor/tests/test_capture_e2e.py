"""Testes end-to-end (offline, sem cluster) para captura de .py, .sql e notebooks Databricks (.py e .ipynb).

Rode com:
    python tests/test_capture_e2e.py
ou:
    pytest
"""
from __future__ import annotations

import contextlib
import json
import re
import tempfile
from pathlib import Path
from typing import Any

import spark_lineage.capture_plans as cp
from spark_lineage.impact import trace_column_impact
from spark_lineage.sources import build_sources, extract_tag


# --------------------------------------------------------------------------- #
# Fake SparkSession / DataFrame para testes E2E determinísticos e offline
# --------------------------------------------------------------------------- #


class _FakeType:
    def __init__(self, type_str: str) -> None:
        self._s = type_str

    def simpleString(self) -> str:  # noqa: N802
        return self._s


class _FakeField:
    def __init__(self, name: str, type_str: str, comment: str | None = None) -> None:
        self.name = name
        self.dataType = _FakeType(type_str)
        self.metadata = {"comment": comment} if comment else {}


class _FakeSchema:
    def __init__(self, fields: list[_FakeField]) -> None:
        self.fields = fields


class _FakeConf:
    def set(self, _k: str, _v: str) -> None:
        return None


class _FakeWriter:
    def __init__(self, df: _FakeDF) -> None:
        self._df = df
        self._mode = "append"

    def mode(self, m: str) -> _FakeWriter:
        self._mode = m
        return self

    def format(self, _fmt: str) -> _FakeWriter:
        return self

    def option(self, _k: str, _v: Any) -> _FakeWriter:
        return self

    def saveAsTable(self, name: str) -> None:  # noqa: N802
        raise AssertionError(f"saveAsTable({name}) não deveria executar de verdade durante a captura!")

    def save(self, path: str | None = None) -> None:
        raise AssertionError(f"save({path}) não deveria executar de verdade durante a captura!")


class _FakeDF:
    def __init__(
        self,
        analyzed: str,
        physical: str,
        fields: list[_FakeField],
        formatted: str = "",
        rows_data: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self._analyzed = analyzed
        self._physical = physical
        self._formatted = formatted
        self.schema = _FakeSchema(fields)
        self._rows_data = rows_data or []

    @property
    def write(self) -> _FakeWriter:
        return _FakeWriter(self)

    def filter(self, cond: str) -> _FakeDF:
        indented = "\n".join(f"   {ln}" for ln in self._analyzed.splitlines())
        new_analyzed = f"Filter {cond}\n+- {indented.lstrip()}"
        return _FakeDF(new_analyzed, self._physical, self.schema.fields, self._formatted)

    def join(self, other: _FakeDF, cond: str, how: str = "Inner") -> _FakeDF:
        left_ind = "\n".join(f":  {ln}" for ln in self._analyzed.splitlines())
        right_ind = "\n".join(f"   {ln}" for ln in other._analyzed.splitlines())
        new_analyzed = f"Join {how}, {cond}\n:- {left_ind[3:]}\n+- {right_ind[3:]}"
        new_phys = self._physical + "\n" + other._physical
        new_fmt = (self._formatted + "\n\n" + other._formatted).strip()
        return _FakeDF(new_analyzed, new_phys, self.schema.fields + other.schema.fields, new_fmt)

    def withColumn(self, alias: str, expr: str, aid: int = 900) -> _FakeDF:  # noqa: N802
        base_cols = ", ".join(f"{f.name}#{i + 1}" for i, f in enumerate(self.schema.fields))
        indented = "\n".join(f"   {ln}" for ln in self._analyzed.splitlines())
        new_analyzed = f"Project [{base_cols}, {expr} AS {alias}#{aid}]\n+- {indented.lstrip()}"
        new_fields = [*self.schema.fields, _FakeField(alias, "string")]
        return _FakeDF(new_analyzed, self._physical, new_fields, self._formatted)

    def createOrReplaceTempView(self, _name: str) -> None:  # noqa: N802
        return None

    def explain(self, extended: bool = False, mode: str | None = None) -> None:
        if mode == "formatted":
            print(self._formatted or f"== Physical Plan ==\n{self._physical}")
            return
        _ = extended
        print(f"== Analyzed Logical Plan ==\n{self._analyzed}\n\n== Physical Plan ==\n{self._physical}")

    def count(self) -> int:
        raise AssertionError("count() não deveria executar de verdade durante a captura!")

    def collect(self) -> list[tuple[Any, ...]]:
        return list(self._rows_data)


class _FakeReader:
    def __init__(self, session: _FakeSparkSession) -> None:
        self._session = session
        self._options: dict[str, str] = {}

    def format(self, _fmt: str) -> _FakeReader:
        return self

    def option(self, k: str, v: str) -> _FakeReader:
        self._options[k] = v
        return self

    def table(self, name: str) -> _FakeDF:
        return self._session.table(name)

    def parquet(self, path: str) -> _FakeDF:
        return self._session._make_table_df(path)


class _FakeSparkSession:
    def __init__(self, catalog_tables: dict[str, list[_FakeField]] | None = None) -> None:
        self._catalog = catalog_tables if catalog_tables is not None else dict(_CATALOG)
        self._temp_views: dict[str, _FakeDF] = {}
        self.conf = _FakeConf()
        self.sql_log: list[str] = []

    @property
    def read(self) -> _FakeReader:
        return _FakeReader(self)

    def _make_table_df(self, name: str, base_id: int = 1) -> _FakeDF:
        norm = name.replace("`", "").strip()
        if norm in self._temp_views:
            return self._temp_views[norm]
        fields = self._catalog.get(norm)
        if fields is None:
            # Permite sufixo curto se existir no catálogo fake
            for k, v in self._catalog.items():
                if k.endswith(f".{norm}"):
                    norm, fields = k, v
                    break
        if fields is None:
            fields = [_FakeField("id", "int")]
        cols_attr = ",".join(f"{f.name}#{base_id + i}" for i, f in enumerate(fields))
        schema_str = ",".join(f"{f.name}:{f.dataType.simpleString()}" for f in fields)
        analyzed = f"Relation {norm}[{cols_attr}] parquet"
        physical = f"PhotonScan parquet {norm}[{cols_attr}] ReadSchema: struct<{schema_str}>"
        formatted = (
            f"== Physical Plan ==\n"
            f"(1) PhotonScan parquet {norm}\n"
            f"Output [{len(fields)}]: [{cols_attr}]\n"
            f"ReadSchema: struct<{schema_str}>\n"
        )
        return _FakeDF(analyzed, physical, fields, formatted)

    def table(self, name: str) -> _FakeDF:
        return self._make_table_df(name)

    def sql(self, query: str) -> _FakeDF:
        self.sql_log.append(query)
        q = query.strip()
        q_up = q.upper()
        if q_up.startswith("EXPLAIN FORMATTED "):
            inner = q[len("EXPLAIN FORMATTED ") :].strip()
            df_in = self._build_sql_plan_df(inner)
            return _FakeDF("", "", [], rows_data=[(df_in._formatted,)])
        if q_up.startswith("EXPLAIN EXTENDED "):
            inner = q[len("EXPLAIN EXTENDED ") :].strip()
            df_in = self._build_sql_plan_df(inner)
            txt = f"== Analyzed Logical Plan ==\n{df_in._analyzed}\n\n== Physical Plan ==\n{df_in._physical}"
            return _FakeDF("", "", [], rows_data=[(txt,)])
        m_tv = re.match(r"CREATE\s+(?:OR\s+REPLACE\s+)?TEMP(?:ORARY)?\s+VIEW\s+([\w.]+)\s+AS\s+(.+)", q, re.I | re.S)
        if m_tv:
            vname, body = m_tv.group(1), m_tv.group(2)
            self._temp_views[vname] = self._build_sql_plan_df(body)
            return _FakeDF("", "", [])
        if q_up.startswith("USE ") or q_up.startswith("SET ") or "SELECT 1 AS _BLOCKED" in q_up:
            return _FakeDF("", "", [])
        return self._build_sql_plan_df(q)

    def _build_sql_plan_df(self, sql_body: str) -> _FakeDF:
        # Resolve a primeira tabela FROM / JOIN / DELETE FROM encontrada
        srcs = cp.extract_sql_sources(sql_body)
        base_df = self._make_table_df(srcs[0], base_id=1) if srcs else _FakeDF("OneRowRelation", "", [])
        if len(srcs) > 1:
            other_df = self._make_table_df(srcs[1], base_id=100)
            base_df = base_df.join(other_df, "(cbco#1 = cod_bco#100)")
        m_ins = re.match(r"INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?([`'\w.]+)", sql_body, re.I)
        if m_ins:
            tgt = m_ins.group(1).replace("`", "")
            t_fields = self._catalog.get(tgt, base_df.schema.fields)
            t_cols = ", ".join(f"{f.name}#{500 + i}" for i, f in enumerate(t_fields))
            src_cols = ", ".join(f"{f.name}#{1 + i}" for i, f in enumerate(base_df.schema.fields))
            indented = "\n".join(f"      {ln}" for ln in base_df._analyzed.splitlines())
            analyzed = (
                f"AppendData RelationV2[{t_cols}] {tgt}\n"
                f"+- Project [{src_cols}]\n"
                f"   +- {indented.lstrip()}"
            )
            return _FakeDF(analyzed, base_df._physical, t_fields, base_df._formatted)
        return base_df


@contextlib.contextmanager
def _fake_spark_env(catalog_tables: dict[str, list[_FakeField]]):
    fake_spark = _FakeSparkSession(catalog_tables)
    old_get_spark = cp.get_spark
    old_dfs = list(cp.DF_CLASSES)
    old_writers = list(cp.WRITER_CLASSES)
    old_sessions = list(cp.SESSION_CLASSES)
    old_readers = list(cp.READER_CLASSES)
    cp.get_spark = lambda: fake_spark
    cp.DF_CLASSES[:] = [_FakeDF]
    cp.WRITER_CLASSES[:] = [_FakeWriter]
    cp.SESSION_CLASSES[:] = [_FakeSparkSession]
    cp.READER_CLASSES[:] = [_FakeReader]
    try:
        yield fake_spark
    finally:
        cp.get_spark = old_get_spark
        cp.DF_CLASSES[:] = old_dfs
        cp.WRITER_CLASSES[:] = old_writers
        cp.SESSION_CLASSES[:] = old_sessions
        cp.READER_CLASSES[:] = old_readers


_CATALOG = {
    "dev.cadastros.tb_banco": [
        _FakeField("cbco", "int", "Código COMPE"),
        _FakeField("nm_banco", "string", "Nome da instituição"),
        _FakeField("dt_ref", "date"),
    ],
    "dev.compliance.tb_restritiva": [
        _FakeField("cod_bco", "int", "Código restrito"),
        _FakeField("ds_motivo", "string"),
    ],
    "dev.tgt.tb_saida": [
        _FakeField("id_banco", "int"),
        _FakeField("nome_final", "string"),
        _FakeField("dt_ref", "date"),
    ],
}


# --------------------------------------------------------------------------- #
# Testes E2E: .py, .sql, Databricks Notebook (.py) e .ipynb
# --------------------------------------------------------------------------- #


def test_e2e_python_script_with_branches() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "jobs").mkdir()
        script = root / "jobs" / "etl.py"
        script.write_text(
            "df_bco = spark.table('dev.cadastros.tb_banco').filter('(cbco#1 > 0)')\n"
            "if df_bco.count() == 0:\n"
            "    df_rest = spark.read.table('dev.compliance.tb_restritiva')\n"
            "    df_join = df_bco.join(df_rest, '(cbco#1 = cod_bco#1)')\n"
            "    df_join.write.saveAsTable('dev.tgt.tb_reprocessar')\n"
            "    spark.sql('OPTIMIZE dev.tgt.tb_reprocessar')\n"
            "else:\n"
            "    df_out = df_bco.withColumn('cod_fmt', 'lpad(cbco#1, 3, 0)', aid=901)\n"
            "    df_out.write.saveAsTable('dev.tgt.tb_normal')\n"
            "df_bco.display()\n",
            encoding="utf-8",
        )
        with _fake_spark_env(_CATALOG):
            res = cp.capture_plans(str(root), "jobs/etl.py")

    assert res["status"] == "ok"
    # Ambos os lados do if foram explorados automaticamente
    assert len(res["branches"]) == 1
    assert set(res["branches"][0]["explored_values"]) == {False, True}

    # Fontes e locais exatos de leitura (spark.table e spark.read.table)
    src_by_tbl = {s["table"]: s for s in res["sources"]}
    assert "dev.cadastros.tb_banco" in src_by_tbl
    assert "dev.compliance.tb_restritiva" in src_by_tbl
    assert any(s.endswith("etl.py:1") for s in src_by_tbl["dev.cadastros.tb_banco"]["call_sites"])
    assert any(s.endswith("etl.py:3") for s in src_by_tbl["dev.compliance.tb_restritiva"]["call_sites"])

    # Destinos dos dois ramos capturados
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert dest_tbls == {"dev.tgt.tb_normal", "dev.tgt.tb_reprocessar"}
    # OPTIMIZE bloqueado em events
    assert any(e["kind"] == "sql_blocked" and e["statement_kind"] == "OPTIMIZE" for e in res["events"])

    # Impacto da coluna cbco pega filtro, join, derivação cod_fmt e os dois destinos
    imp = trace_column_impact(res, table="tb_banco", column="cbco")
    assert imp["summary"]["found"] is True
    assert imp["comment"] == "Código COMPE"
    assert {d["table"] for d in imp["destinations_affected"]} == {"dev.tgt.tb_normal", "dev.tgt.tb_reprocessar"}
    # Verifica que `sources` não inclui `branches`, mas `extract_tag(..., tag='branches')` retorna
    doc = build_sources(res)
    assert "branches" not in doc
    assert len(extract_tag(res, doc, tag="branches")) == 1


def test_e2e_sql_file_and_sql_notebook() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "sql").mkdir()
        (root / "sql" / "setup.sql").write_text(
            "USE CATALOG ${catalog};\n"
            "CREATE OR REPLACE TEMP VIEW vw_bancos AS\n"
            "SELECT cbco, nm_banco, dt_ref FROM dev.cadastros.tb_banco WHERE dt_ref = :dt_ref;\n",
            encoding="utf-8",
        )
        (root / "sql" / "pipeline.sql").write_text(
            "-- Databricks notebook source\n"
            "-- MAGIC %run ./setup\n"
            "-- COMMAND ----------\n"
            "-- Inserção no destino a partir da view criada no %run\n"
            "INSERT INTO ${catalog}.tgt.tb_saida\n"
            "SELECT cbco, nm_banco, dt_ref FROM vw_bancos;\n"
            "-- COMMAND ----------\n"
            "SELECT cbco, nm_banco FROM vw_bancos\n"
            "-- COMMAND ----------\n"
            "OPTIMIZE ${catalog}.tgt.tb_saida;\n"
            "-- COMMAND ----------\n"
            "-- MAGIC %python\n"
            "-- MAGIC if dbutils.widgets.get('modo') == 'EXTRA':\n"
            "-- MAGIC     spark.table('dev.compliance.tb_restritiva').write.saveAsTable('dev.tgt.tb_extra')\n",
            encoding="utf-8",
        )
        with _fake_spark_env(_CATALOG) as fake_spark:
            res = cp.capture_plans(
                str(root),
                "sql/pipeline.sql",
                params={"catalog": "dev", "dt_ref": "2026-11-20", "modo": "NORMAL"},
            )

    assert res["status"] == "ok"
    # Parâmetros substituídos corretamente nas queries enviadas ao Spark
    assert any("USE CATALOG dev" in q for q in fake_spark.sql_log)
    assert any("dt_ref = '2026-11-20'" in q for q in fake_spark.sql_log)
    assert any("EXPLAIN EXTENDED INSERT INTO dev.tgt.tb_saida" in q for q in fake_spark.sql_log)

    # Destinos: tb_saida (do SQL INSERT) e tb_extra (da célula -- MAGIC %python forçada pelo BranchExplorer!)
    dest_by_tbl = {d["table"]: d for d in res["destinations"]}
    assert "dev.tgt.tb_saida" in dest_by_tbl
    assert "sql:INSERT" in dest_by_tbl["dev.tgt.tb_saida"]["actions"]
    assert any(cs.endswith("pipeline.sql:5") for cs in dest_by_tbl["dev.tgt.tb_saida"]["call_sites"])
    assert "dev.tgt.tb_extra" in dest_by_tbl
    assert len(res["branches"]) == 1 and set(res["branches"][0]["explored_values"]) == {False, True}

    # Impacto posicional no INSERT: dev.cadastros.tb_banco.cbco -> dev.tgt.tb_saida.id_banco
    imp = trace_column_impact(res, table="tb_banco", column="cbco")
    saida_dest = next(d for d in imp["destinations_affected"] if d["table"] == "dev.tgt.tb_saida")
    assert [c["name"] for c in saida_dest["columns"]] == ["id_banco"]


def test_e2e_databricks_py_notebook_magic_run_sql_and_widgets() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "notebooks").mkdir()
        (root / "notebooks" / "utils_nb.py").write_text(
            "# Databricks notebook source\n"
            "def filtrar_bancos(df):\n"
            "    return df.filter('(cbco#1 > 0)')\n"
            "# COMMAND ----------\n"
            "# MAGIC %sql\n"
            "# MAGIC CREATE OR REPLACE TEMP VIEW vw_restritiva AS\n"
            "# MAGIC SELECT cod_bco, ds_motivo FROM dev.compliance.tb_restritiva\n",
            encoding="utf-8",
        )
        (root / "notebooks" / "child_nb.py").write_text(
            "# Databricks notebook source\n"
            "dbutils.widgets.text('modo', 'PADRAO')\n"
            "if dbutils.widgets.get('modo') == 'FILHO':\n"
            "    spark.table('dev.compliance.tb_restritiva').write.saveAsTable('dev.tgt.tb_filho')\n"
            "dbutils.notebook.exit('OK_FILHO')\n",
            encoding="utf-8",
        )
        (root / "notebooks" / "main_nb.py").write_text(
            "# Databricks notebook source\n"
            "# MAGIC %md\n"
            "# MAGIC # Pipeline Principal\n"
            "# COMMAND ----------\n"
            "# MAGIC %pip install lib-inexistente\n"
            "# COMMAND ----------\n"
            "# MAGIC %sh echo nao-executar\n"
            "# COMMAND ----------\n"
            "# MAGIC %run ./utils_nb\n"
            "# COMMAND ----------\n"
            "dbutils.widgets.text('dt_ref', '2026-11-20')\n"
            "dbutils.widgets.dropdown('modo', 'INCREMENTAL', ['INCREMENTAL', 'REPROC'])\n"
            "df_b = filtrar_bancos(spark.table('dev.cadastros.tb_banco'))\n"
            "# COMMAND ----------\n"
            "# MAGIC %sql\n"
            "# MAGIC INSERT INTO dev.tgt.tb_saida\n"
            "# MAGIC SELECT cbco, nm_banco, dt_ref FROM dev.cadastros.tb_banco WHERE dt_ref = :dt_ref\n"
            "# COMMAND ----------\n"
            "if dbutils.widgets.get('modo') == 'REPROC':\n"
            "    df_b.write.saveAsTable('dev.tgt.tb_reproc_nb')\n"
            "else:\n"
            "    df_b.write.saveAsTable('dev.tgt.tb_incr_nb')\n"
            "# COMMAND ----------\n"
            "ret = dbutils.notebook.run('./child_nb', 60, {'modo': 'FILHO'})\n"
            "assert ret == 'OK_FILHO'\n",
            encoding="utf-8",
        )
        with _fake_spark_env(_CATALOG) as fake_spark:
            # Sem passar --param dt_ref: usa o default '2026-11-20' registrado por dbutils.widgets.text!
            res = cp.capture_plans(str(root), "notebooks/main_nb.py")

    assert res["status"] == "ok", res["error"]
    # Default do widget foi substituído na célula %sql
    assert any("dt_ref = '2026-11-20'" in q for q in fake_spark.sql_log)
    # %pip e %sh registrados em events (e nunca executados)
    ev_kinds = {e["kind"] for e in res["events"]}
    assert {"magic_pip", "magic_sh"} <= ev_kinds
    # Destinos: do %sql, dos 2 ramos do if no main_nb E do child_nb chamado via dbutils.notebook.run!
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert {"dev.tgt.tb_saida", "dev.tgt.tb_incr_nb", "dev.tgt.tb_reproc_nb", "dev.tgt.tb_filho"} <= dest_tbls
    # Linha exata do INSERT dentro da célula %sql preservada (linha 16 de main_nb.py)
    saida = next(d for d in res["destinations"] if d["table"] == "dev.tgt.tb_saida")
    assert any(cs.endswith("main_nb.py:16") for cs in saida["call_sites"])


def test_e2e_ipynb_notebook() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        nb_json = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {},
            "cells": [
                {"cell_type": "markdown", "metadata": {}, "source": ["# Titulo"]},
                {"cell_type": "code", "metadata": {}, "source": ["%pip install foo"]},
                {
                    "cell_type": "code",
                    "metadata": {},
                    "source": [
                        "%sql\n",
                        "SELECT cbco, nm_banco FROM dev.cadastros.tb_banco\n",
                    ],
                },
                {
                    "cell_type": "code",
                    "metadata": {},
                    "source": [
                        "flag = False\n",
                        "if flag:\n",
                        "    spark.table('dev.compliance.tb_restritiva').write.saveAsTable('dev.tgt.tb_ipynb_true')\n",
                        "else:\n",
                        "    spark.table('dev.cadastros.tb_banco').write.saveAsTable('dev.tgt.tb_ipynb_false')\n",
                    ],
                },
            ],
        }
        (root / "analise.ipynb").write_text(json.dumps(nb_json), encoding="utf-8")
        with _fake_spark_env(_CATALOG):
            res = cp.capture_plans(str(root), "analise.ipynb")

    assert res["status"] == "ok", res["error"]
    assert any(e["kind"] == "magic_pip" for e in res["events"])
    assert {d["table"] for d in res["destinations"]} == {"dev.tgt.tb_ipynb_true", "dev.tgt.tb_ipynb_false"}
    assert any(
        o["action"] == "sql:SELECT"
        for p in res["plans"]
        for o in p["occurrences"]
    )


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("OK ", name)
