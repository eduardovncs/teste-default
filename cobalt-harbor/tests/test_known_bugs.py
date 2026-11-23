"""Testes de regressão dos 15 bugs documentados em RELATORIO_BUGS.md."""

from __future__ import annotations

import contextlib
import importlib.util
import json
import tempfile
from pathlib import Path
from typing import Any

import spark_lineage.capture_plans as cp
from spark_lineage.impact import trace_column_impact
from spark_lineage.sources import build_sources

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REPRO_DIR = _REPO_ROOT / "repro"


def _load_e2e_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "_test_capture_e2e_helpers", _REPO_ROOT / "tests" / "test_capture_e2e.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


e2e: Any = _load_e2e_module()


class _FakeMergeWriter:
    def whenMatched(self) -> _FakeMergeWriter:  # noqa: N802
        return self

    def updateAll(self) -> _FakeMergeWriter:  # noqa: N802
        return self

    def merge(self) -> None:
        raise AssertionError("merge() não deveria executar de verdade durante a captura!")


class _ExtendedFakeWriter(e2e._FakeWriter):
    def __init__(self, df: Any) -> None:
        super().__init__(df)
        self._options: dict[str, Any] = {}

    def option(self, k: str, v: Any) -> _ExtendedFakeWriter:
        self._options[k] = v
        return self


class _ExtendedFakeDF(e2e._FakeDF):
    @property
    def write(self) -> _ExtendedFakeWriter:
        return _ExtendedFakeWriter(self)

    def mergeInto(self, _table: str, _condition: Any) -> _FakeMergeWriter:  # noqa: N802
        return _FakeMergeWriter()


class _ExtendedFakeSparkSession(e2e._FakeSparkSession):
    def _make_table_df(self, name: str, base_id: int = 1) -> _ExtendedFakeDF:
        base = super()._make_table_df(name, base_id=base_id)
        return _ExtendedFakeDF(base._analyzed, base._physical, base.schema.fields, base._formatted, base._rows_data)

    def range(self, _n: int) -> _ExtendedFakeDF:
        return _ExtendedFakeDF(
            "Relation range_src[id#1] parquet",
            "PhotonScan parquet range_src[id#1] ReadSchema: struct<id:bigint>",
            [e2e._FakeField("id", "bigint")],
            "(1) PhotonScan parquet range_src\nOutput [1]: [id#1]\nReadSchema: struct<id:bigint>\n",
        )

    def sql(self, query: str) -> _ExtendedFakeDF:
        if "SELECT 1 AS _BLOCKED" in query.upper():
            self.sql_log.append(query)
            return _ExtendedFakeDF("Project [1 AS _blocked#99]", "", [e2e._FakeField("_blocked", "int")])
        base = super().sql(query)
        return _ExtendedFakeDF(base._analyzed, base._physical, base.schema.fields, base._formatted, base._rows_data)


@contextlib.contextmanager
def _extended_spark_env(catalog_tables: dict[str, list[Any]]):
    fake_spark = _ExtendedFakeSparkSession(catalog_tables)
    old_get_spark = cp.get_spark
    old_dfs = list(cp.DF_CLASSES)
    old_writers = list(cp.WRITER_CLASSES)
    old_sessions = list(cp.SESSION_CLASSES)
    old_readers = list(cp.READER_CLASSES)
    old_merge_writers = list(cp.MERGE_WRITER_CLASSES)
    cp_mod: Any = cp
    cp_mod.get_spark = lambda: fake_spark
    cp.DF_CLASSES[:] = [_ExtendedFakeDF]
    cp.WRITER_CLASSES[:] = [_ExtendedFakeWriter]
    cp.SESSION_CLASSES[:] = [_ExtendedFakeSparkSession]
    cp.READER_CLASSES[:] = [e2e._FakeReader]
    cp.MERGE_WRITER_CLASSES[:] = [_FakeMergeWriter]
    try:
        yield fake_spark
    finally:
        cp_mod.get_spark = old_get_spark
        cp.DF_CLASSES[:] = old_dfs
        cp.WRITER_CLASSES[:] = old_writers
        cp.SESSION_CLASSES[:] = old_sessions
        cp.READER_CLASSES[:] = old_readers
        cp.MERGE_WRITER_CLASSES[:] = old_merge_writers


# --------------------------------------------------------------------------- #
# Bugs em captura de jobs Python (.py)
# --------------------------------------------------------------------------- #


def test_bug_write_save_with_option_path_captures_destination() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "df.write.format('delta').mode('overwrite').option('path', '/mnt/repro_lineage/vendas').save()\n",
            encoding="utf-8",
        )
        with _extended_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok"
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert "/mnt/repro_lineage/vendas" in dest_tbls


def test_bug_df_merge_into_captures_destination() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "df.mergeInto('dev.tgt.tb_merge_destino', 't.cbco = s.cbco').whenMatched().updateAll().merge()\n",
            encoding="utf-8",
        )
        with _extended_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok"
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert "dev.tgt.tb_merge_destino" in dest_tbls


def test_bug_delta_table_merge_captures_destination() -> None:
    from spark_lineage.sources import is_write_action

    assert is_write_action("delta.merge(source)") is True


def test_bug_branch_explorer_explores_os_getenv_branch() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "import os\n"
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "if os.getenv('MODO') == 'REPROC':\n"
            "    df.write.saveAsTable('dev.tgt.tb_reprocessamento')\n"
            "else:\n"
            "    df.write.saveAsTable('dev.tgt.tb_normal')\n",
            encoding="utf-8",
        )
        with _extended_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok"
    assert len(res["branches"]) == 1
    assert {d["table"] for d in res["destinations"]} == {"dev.tgt.tb_reprocessamento", "dev.tgt.tb_normal"}


def test_bug_with_select_alias_update_not_blocked_as_insert() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "df_cte = spark.sql('''\n"
            "    WITH base AS (\n"
            "        SELECT cbco, CAST(dt_ref AS STRING) update\n"
            "        FROM dev.cadastros.tb_banco\n"
            "    )\n"
            "    SELECT cbco, update FROM base\n"
            "''')\n"
            "df_cte.write.saveAsTable('dev.tgt.tb_from_cte')\n",
            encoding="utf-8",
        )
        with _extended_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok"
    dest_by_tbl = {d["table"]: d for d in res["destinations"]}
    assert "FROM" not in dest_by_tbl
    assert "dev.tgt.tb_from_cte" in dest_by_tbl
    col_names = [c["name"] for c in (dest_by_tbl["dev.tgt.tb_from_cte"]["columns"] or [])]
    assert "_blocked" not in col_names


def test_bug_repro_file_job_bugs_py() -> None:
    with _extended_spark_env(e2e._CATALOG):
        res = cp.capture_plans(str(_REPRO_DIR), "job_bugs.py", fetch_catalog=False)

    assert res["status"] == "ok"
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert {
        "/mnt/repro_lineage/vendas",
        "dev.tgt.tb_merge_destino",
        "dev.tgt.tb_reprocessamento",
        "dev.tgt.tb_normal",
        "dev.tgt.tb_from_cte",
    } == dest_tbls


# --------------------------------------------------------------------------- #
# Bugs em classificação, parsing e pré-processamento SQL (.sql)
# --------------------------------------------------------------------------- #


def test_bug_sql_from_first_insert_is_classified_as_write() -> None:
    query = "FROM dev.cadastros.tb_banco INSERT INTO dev.tgt.tb_saida SELECT cbco, nm_banco, dt_ref"
    mode, kind = cp.classify_sql(query)
    assert mode == "explain"
    assert kind == "INSERT"
    assert cp.extract_sql_target(query) == "dev.tgt.tb_saida"


def test_bug_sql_double_dash_inside_string_preserves_sources() -> None:
    query = "SELECT cbco, COALESCE(nm_banco, '--') AS nm_banco FROM dev.cadastros.tb_banco"
    assert cp.extract_sql_sources(query) == ["dev.cadastros.tb_banco"]


def test_bug_sql_double_dash_in_cte_does_not_bypass_insert_interception() -> None:
    query = (
        "WITH base AS (SELECT cbco, COALESCE(nm_banco, '--') AS nm_banco FROM dev.cadastros.tb_banco)\n"
        "INSERT INTO dev.tgt.tb_saida SELECT cbco, nm_banco FROM base"
    )
    mode, kind = cp.classify_sql(query)
    assert mode == "explain"
    assert kind == "INSERT"


def test_bug_sql_create_view_extracts_target_name() -> None:
    query = "CREATE OR REPLACE VIEW dev.tgt.vw_publicada AS SELECT cbco FROM dev.cadastros.tb_banco"
    assert cp.extract_sql_target(query) == "dev.tgt.vw_publicada"


def test_bug_sql_with_cte_delete_preserves_delete_keyword() -> None:
    query = "WITH alvo AS (SELECT cbco FROM dev.cadastros.tb_banco) DELETE FROM dev.tgt.tb_saida WHERE id_banco = 1"
    mode, kind = cp.classify_sql(query)
    assert mode == "explain"
    assert kind == "DELETE"


def test_bug_substitute_sql_params_ignores_string_literals() -> None:
    query = "SELECT id, 'etiqueta: :modo' AS info FROM vw_base WHERE modo = :modo"
    out = cp.substitute_sql_params(query, {"modo": "PROD"})
    assert out == "SELECT id, 'etiqueta: :modo' AS info FROM vw_base WHERE modo = 'PROD'"


def test_bug_repro_file_pipeline_bugs_sql() -> None:
    with _extended_spark_env(e2e._CATALOG) as fake_spark:
        res = cp.capture_plans(str(_REPRO_DIR), "pipeline_bugs.sql", fetch_catalog=False)

    assert res["status"] == "ok"
    dest_tbls = [d["table"] for d in res["destinations"]]
    assert dest_tbls == ["dev.tgt.vw_publicada"]
    assert any(site.endswith("pipeline_bugs.sql:25") for site in res["read_sites"].get("vw_base", []))
    assert "'etiqueta: 'PROD''" not in fake_spark.sql_log[-1]


# --------------------------------------------------------------------------- #
# Bugs em notebooks Jupyter (.ipynb)
# --------------------------------------------------------------------------- #


def test_bug_ipynb_double_percent_sql_cell_is_captured() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        nb = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {},
            "cells": [
                {
                    "cell_type": "code",
                    "metadata": {},
                    "source": ["%%sql\n", "SELECT cbco FROM dev.cadastros.tb_banco\n"],
                }
            ],
        }
        (root / "nb.ipynb").write_text(json.dumps(nb), encoding="utf-8")
        with _extended_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "nb.ipynb", fetch_catalog=False)

    assert res["status"] == "ok"
    assert len(res["plans"]) == 1
    assert any(s["table"] == "dev.cadastros.tb_banco" for s in res["sources"])


def test_bug_ipynb_line_magic_followed_by_python_executes_python() -> None:
    with _extended_spark_env(e2e._CATALOG):
        res = cp.capture_plans(str(_REPRO_DIR), "notebook_bugs.ipynb", fetch_catalog=False)

    assert res["status"] == "ok"
    assert any(e["kind"] == "magic_pip" for e in res["events"])
    assert any(d["table"] == "dev.tgt.tb_pos_pip" for d in res["destinations"])
    assert len(res["plans"]) >= 2


# --------------------------------------------------------------------------- #
# Bugs em análise de impacto (impact.py) e consolidação de fontes (sources.py)
# --------------------------------------------------------------------------- #


def test_bug_impact_match_table_does_not_collide_with_prefix_table() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": "Project [cbco#1]\n+- Relation dev.cadastros.tb_banco_historico[cbco#1,nm#2] parquet",
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_hist_out",
                        "call_site": {"file": "job.py", "line": 5},
                    }
                ],
            }
        ],
    }
    rep = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="cbco")
    assert rep["summary"]["found"] is False
    assert rep["destinations_affected"] == []


def test_bug_impact_filter_with_in_subquery_preserves_output_columns() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "cbco: int, nm_banco: string\n"
                        "Filter cbco#1 IN (list#99 [])\n"
                        ":  +- Project [cod_bco#10]\n"
                        ":     +- Relation dev.compliance.tb_restritiva[cod_bco#10,ds_motivo#11] parquet\n"
                        "+- Relation dev.cadastros.tb_banco[cbco#1,nm_banco#2] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_filtrada",
                        "call_site": {"file": "job.py", "line": 10},
                    }
                ],
            }
        ],
    }
    rep = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="cbco")
    assert rep["summary"]["found"] is True
    out_cols = [c["name"] for c in rep["hops"][0]["plans"][0]["output_columns_affected"]]
    assert "cbco" in out_cols


def test_bug_sources_in_place_delete_unqualified_not_listed_as_source() -> None:
    plan_doc = {
        "entrypoint": "job.sql",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "DeleteFromTable (id_banco#1 = 341)\n"
                        "+- Relation dev.tgt.tb_saida[id_banco#1,nome_final#2] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "sql:DELETE",
                        "target": "tb_saida",
                        "call_site": {"file": "job.sql", "line": 3},
                    }
                ],
            }
        ],
    }
    doc = build_sources(plan_doc)
    assert doc["sources"] == []
    assert [d["table"] for d in doc["destinations"]] == ["tb_saida"]


if __name__ == "__main__":
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            _fn()
            print(f"OK  {_name}")
