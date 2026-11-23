"""Testes de reprodução de bugs conhecidos — Rodada 4.

Todos os testes abaixo especificam o comportamento ESPERADO e estão marcados com
``@pytest.mark.xfail(strict=True)`` enquanto os bugs da Rodada 4 permanecem abertos:
- ``pytest -v`` mostra cada bug reproduzido como ``XFAIL`` sem quebrar a suíte.
- ``pytest tests/test_known_bugs_r4.py --runxfail`` exibe as falhas reais detalhadas.
- Ao corrigir um bug, o teste passará a reportar ``XPASS(strict)``, indicando que o
  marcador pode ser removido para virar teste de regressão permanente.
"""

from __future__ import annotations

import contextlib
import importlib.util
import json
import tempfile
from pathlib import Path
from typing import Any

import spark_lineage.capture_plans as cp
from spark_lineage import cli
from spark_lineage.impact import trace_column_impact
from spark_lineage.plan_parser import parse_formatted_scans, parse_read_schema, parse_scans
from spark_lineage.sources import build_sources

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REPRO_DIR = _REPO_ROOT / "repro"


def _load_e2e_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "_test_capture_e2e_helpers_r4",
        _REPO_ROOT / "tests" / "test_capture_e2e.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


e2e: Any = _load_e2e_module()


class _FakeLogicalPlan:
    """Imita pyspark.sql.connect.plan.LogicalPlan (sem .explain, .filter ou .write)."""


class _ConnectWriterWithPlan:
    def __init__(self, plan: _FakeLogicalPlan, session: Any) -> None:
        self._df = plan
        self._spark = session

    def saveAsTable(self, _name: str) -> None:  # noqa: N802
        raise AssertionError("saveAsTable não deveria executar!")


class _ConnectStreamWriterWithPlan:
    def __init__(self, plan: _FakeLogicalPlan, session: Any) -> None:
        self._write_stream = type("_WS", (), {"_child": plan})()
        self._session = session

    def foreachBatch(self, _fn: Any) -> _ConnectStreamWriterWithPlan:  # noqa: N802
        return self

    def start(self) -> Any:
        raise AssertionError("start não deveria executar!")


class _ExtendedDFR4(e2e._FakeDF):
    def head(self, _n: int | None = None) -> Any:
        raise AssertionError("head() não deveria executar de verdade!")

    def toPandas(self) -> Any:  # noqa: N802
        raise AssertionError("toPandas() não deveria executar de verdade!")


class _ExtendedSparkR4(e2e._FakeSparkSession):
    def _wrap(self, df: Any) -> _ExtendedDFR4:
        return _ExtendedDFR4(df._analyzed, df._physical, df.schema.fields, df._formatted, df._rows_data)

    def _make_table_df(self, name: str, base_id: int = 1) -> _ExtendedDFR4:
        return self._wrap(super()._make_table_df(name, base_id=base_id))

    def range(self, _n: int) -> _ExtendedDFR4:
        return _ExtendedDFR4(
            "Relation range_src[id#1] parquet",
            "PhotonScan parquet range_src[id#1] ReadSchema: struct<id:bigint>",
            [e2e._FakeField("id", "bigint")],
            "(1) PhotonScan parquet range_src\nOutput [1]: [id#1]\nReadSchema: struct<id:bigint>\n",
        )

    def sql(self, query: str) -> _ExtendedDFR4:
        q_up = query.upper()
        if "OR REPLACE" in q_up and "IF NOT EXISTS" in q_up:
            raise RuntimeError("ParseException: CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed")
        return self._wrap(super().sql(query))


@contextlib.contextmanager
def _spark_env_r4(catalog_tables: dict[str, list[Any]]):
    fake_spark = _ExtendedSparkR4(catalog_tables)
    old_get_spark = cp.get_spark
    old_dfs = list(cp.DF_CLASSES)
    old_writers = list(cp.WRITER_CLASSES)
    old_sessions = list(cp.SESSION_CLASSES)
    old_readers = list(cp.READER_CLASSES)
    cp_mod: Any = cp
    cp_mod.get_spark = lambda: fake_spark
    cp.DF_CLASSES[:] = [_ExtendedDFR4]
    cp.WRITER_CLASSES[:] = [e2e._FakeWriter]
    cp.SESSION_CLASSES[:] = [_ExtendedSparkR4]
    cp.READER_CLASSES[:] = [e2e._FakeReader]
    try:
        yield fake_spark
    finally:
        cp_mod.get_spark = old_get_spark
        cp.DF_CLASSES[:] = old_dfs
        cp.WRITER_CLASSES[:] = old_writers
        cp.SESSION_CLASSES[:] = old_sessions
        cp.READER_CLASSES[:] = old_readers


# --------------------------------------------------------------------------- #
# R4-1: LogicalPlan cru em record_written_table e writeStream.foreachBatch
# --------------------------------------------------------------------------- #


def test_r4_record_written_table_wraps_connect_logical_plan_as_dataframe() -> None:
    cap = cp.Capturer(Path("."), "synthetic", 1, {})
    lp = _FakeLogicalPlan()
    fake_df = _ExtendedDFR4("Relation dev.src.a[id#1] parquet", "", [e2e._FakeField("id", "int")])

    orig_as_df = cp._as_dataframe
    cp_mod: Any = cp
    cp_mod._as_dataframe = lambda obj, owner=None: fake_df if obj is lp else orig_as_df(obj, owner)
    try:
        wrapper = cp._make_writer_factory(cap, "saveAsTable", lambda: None)(None)
        writer = _ConnectWriterWithPlan(lp, session=object())
        wrapper(writer, "dev.stg.tb_x")
    finally:
        cp_mod._as_dataframe = orig_as_df

    stored = cap.get_written_table("dev.stg.tb_x")
    assert stored is not None
    assert hasattr(stored, "explain") and hasattr(stored, "write")


def test_r4_foreach_batch_receives_dataframe_not_logical_plan() -> None:
    cap = cp.Capturer(Path("."), "synthetic", 1, {})
    lp = _FakeLogicalPlan()
    fake_df = _ExtendedDFR4("Relation dev.src.a[id#1] parquet", "", [e2e._FakeField("id", "int")])
    received: list[Any] = []

    orig_as_df = cp._as_dataframe
    cp_mod: Any = cp
    cp_mod._as_dataframe = lambda obj, owner=None: fake_df if obj is lp else orig_as_df(obj, owner)
    try:
        sw = _ConnectStreamWriterWithPlan(lp, session=object())
        cp._make_foreach_batch_factory()(None)(sw, lambda bdf, _bid: received.append(bdf))
        cp._make_writer_factory(cap, "start", cp._Stub, prefix="writeStream")(None)(sw)
    finally:
        cp_mod._as_dataframe = orig_as_df

    assert len(received) == 1
    assert hasattr(received[0], "explain") and hasattr(received[0], "write")


# --------------------------------------------------------------------------- #
# R4-2, R4-3, R4-6: SQL reescrita, CACHE sem AS e extract_sql_targets
# --------------------------------------------------------------------------- #


def test_r4_create_temp_view_if_not_exists_not_rewritten_to_invalid_sql() -> None:
    q = "CREATE TEMP VIEW IF NOT EXISTS vw_x AS SELECT cbco FROM dev.cadastros.tb_banco"
    rewritten = cp._rewrite_safe_temp_view_sql(q, "CREATE_TEMP_VIEW")
    assert not ("OR REPLACE" in rewritten.upper() and "IF NOT EXISTS" in rewritten.upper())


def test_r4_cache_table_without_as_keyword_is_recognized() -> None:
    q = "CACHE TABLE vw_x SELECT cbco FROM dev.cadastros.tb_banco"
    mode, kind = cp.classify_sql(q)
    assert mode == "pass"
    assert kind == "CACHE_TABLE_AS"


def test_r4_insert_overwrite_directory_does_not_extract_keyword_directory() -> None:
    q = "INSERT OVERWRITE DIRECTORY '/mnt/out/r4' USING PARQUET SELECT cbco FROM dev.cadastros.tb_banco"
    targets = cp.extract_sql_targets(q)
    assert "DIRECTORY" not in targets
    assert "/mnt/out/r4" in targets


def test_r4_merge_with_schema_evolution_extracts_target() -> None:
    q = (
        "MERGE WITH SCHEMA EVOLUTION INTO dev.tgt.tb_merge_ev t "
        "USING dev.cadastros.tb_banco s ON t.id = s.cbco "
        "WHEN MATCHED THEN UPDATE SET *"
    )
    assert cp.extract_sql_targets(q) == ["dev.tgt.tb_merge_ev"]


def test_r4_delete_without_from_extracts_target() -> None:
    q = "DELETE dev.tgt.tb_del WHERE id = 1"
    assert cp.extract_sql_targets(q) == ["dev.tgt.tb_del"]


def test_r4_create_or_refresh_streaming_table_extracts_target() -> None:
    q = "CREATE OR REFRESH STREAMING TABLE dev.tgt.tb_stream AS SELECT * FROM STREAM(dev.cadastros.tb_banco)"
    assert cp.extract_sql_targets(q) == ["dev.tgt.tb_stream"]


# --------------------------------------------------------------------------- #
# R4-4: Ternários encadeados/múltiplos na mesma linha colidindo em site_id
# --------------------------------------------------------------------------- #


def test_r4_chained_ternary_on_same_line_explores_all_branches() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "tern.py").write_text(
            "modo = 'C'\n"
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "dst = 'dev.tgt.tb_a' if modo == 'A' else ('dev.tgt.tb_b' if modo == 'B' else 'dev.tgt.tb_c')\n"
            "df.write.saveAsTable(dst)\n",
            encoding="utf-8",
        )
        with _spark_env_r4(e2e._CATALOG):
            res = cp.capture_plans(str(root), "tern.py", fetch_catalog=False)

    assert res["status"] == "ok"
    assert {d["table"] for d in res["destinations"]} == {"dev.tgt.tb_a", "dev.tgt.tb_b", "dev.tgt.tb_c"}


# --------------------------------------------------------------------------- #
# R4-5, R4-21: sources.py (paths sem nome com mesmo schema, scans None, case)
# --------------------------------------------------------------------------- #


def test_r4_sources_distinct_unnamed_paths_with_same_schema() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "read_sites": {"/mnt/dados/2025": ["job.py:1"], "/mnt/dados/2026": ["job.py:2"]},
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": "Union\n:- Relation [id#1,valor#2] parquet\n+- Relation [id#3,valor#4] parquet\n",
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_union",
                        "call_site": {"file": "job.py", "line": 4},
                    }
                ],
            }
        ],
    }
    doc = build_sources(plan_doc)
    assert {s["table"] for s in doc["sources"]} == {"/mnt/dados/2025", "/mnt/dados/2026"}


def test_r4_sources_unnamed_scans_do_not_mix_columns_across_files() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "read_sites": {"/mnt/dados/clientes": ["job.py:1"], "/mnt/dados/pedidos": ["job.py:2"]},
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "Join Inner, (id#1 = id#3)\n"
                        ":- Relation [id#1,nome#2] parquet\n"
                        "+- Relation [id#3,valor#4] parquet\n"
                    ),
                    "physical": (
                        "PhotonScan parquet [id#1,nome#2] ReadSchema: struct<id:int,nome:string>\n"
                        "PhotonScan parquet [id#3,valor#4] ReadSchema: struct<id:int,valor:double>\n"
                    ),
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_join",
                        "call_site": {"file": "job.py", "line": 4},
                    }
                ],
            }
        ],
    }
    doc = build_sources(plan_doc)
    cols_by_tbl = {s["table"]: {c["name"] for c in s["columns"]} for s in doc["sources"]}
    assert cols_by_tbl["/mnt/dados/clientes"] == {"id", "nome"}
    assert cols_by_tbl["/mnt/dados/pedidos"] == {"id", "valor"}


def test_r4_sources_catalog_enrichment_is_case_insensitive() -> None:
    plan_doc = {
        "entrypoint": "job.sql",
        "status": "ok",
        "catalog": {"dev.tgt.tb_saida": {"id_banco": {"type": "int", "comment": "Chave"}}},
        "plans": [
            {
                "id": "P001",
                "output_columns": [{"name": "ID_BANCO", "type": None}],
                "sections": {"analyzed": "Relation DEV.CADASTROS.TB_BANCO[CBCO#1] parquet", "physical": ""},
                "occurrences": [
                    {
                        "action": "sql:INSERT",
                        "target": "DEV.TGT.TB_SAIDA",
                        "call_site": {"file": "job.sql", "line": 1},
                    }
                ],
            }
        ],
    }
    doc = build_sources(plan_doc)
    col = doc["destinations"][0]["columns"][0]
    assert col["type"] == "int"
    assert col.get("comment") == "Chave"


# --------------------------------------------------------------------------- #
# R4-7, R4-18, R4-19: plan_parser.py
# --------------------------------------------------------------------------- #


def test_r4_plan_parser_strips_backticks_from_column_names() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "catalog": {"dev.cadastros.tb_banco": {"cod-banco": {"type": "int", "comment": "Código"}}},
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": "Relation dev.cadastros.tb_banco[`cod-banco`#1,nome#2] parquet",
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_out",
                        "call_site": {"file": "job.py", "line": 3},
                    }
                ],
            }
        ],
    }
    doc = build_sources(plan_doc)
    col_names = [c["name"] for c in doc["sources"][0]["columns"]]
    assert col_names == ["cod-banco", "nome"]
    imp = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="cod-banco")
    assert imp["summary"]["found"] is True


def test_r4_plan_parser_formatted_jdbc_scan() -> None:
    fmt_jdbc = "(1) Scan JDBCRelation(public.tb_vendas) [numPartitions=1]\nOutput [2]: [id#1, valor#2]\n"
    scans = parse_formatted_scans(fmt_jdbc)
    assert len(scans) == 1
    assert [c["name"] for c in scans[0]["columns"]] == ["id", "valor"]


def test_r4_plan_parser_read_schema_strips_backticks() -> None:
    schema, trunc = parse_read_schema("ReadSchema: struct<`cod-banco`:int,nome:string>")
    assert not trunc
    assert schema == {"cod-banco": "int", "nome": "string"}


def test_r4_plan_parser_filters_with_unbalanced_paren_in_string() -> None:
    line = (
        "PhotonScan parquet dev.sch.t[id#1,obs#2] "
        "DataFilters: [(obs#2 = 'erro (1')], Format: parquet, ReadSchema: struct<id:int,obs:string>"
    )
    scan = parse_scans(line)[0]
    assert scan["filters"] == ["(obs = 'erro (1')"]


# --------------------------------------------------------------------------- #
# R4-8, R4-9, R4-10, R4-20: impact.py
# --------------------------------------------------------------------------- #


def test_r4_impact_tracks_codegen_filescan_inside_in_memory_relation() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "Project [cbco#1]\n"
                        "+- InMemoryRelation [cbco#1, nm_banco#2], StorageLevel(memory)\n"
                        "      +- *(1) FileScan parquet dev.cadastros.tb_banco[cbco#1,nm_banco#2] "
                        "ReadSchema: struct<cbco:int,nm_banco:string>\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_out",
                        "call_site": {"file": "job.py", "line": 5},
                    }
                ],
            }
        ],
    }
    imp = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="cbco")
    assert imp["summary"]["found"] is True


def test_r4_impact_left_anti_join_does_not_output_right_columns() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "Join LeftAnti, (cbco#1 = cod_bco#10)\n"
                        ":- Relation dev.cadastros.tb_banco[cbco#1,nm_banco#2] parquet\n"
                        "+- Relation dev.compliance.tb_restritiva[cod_bco#10,ds_motivo#11] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_limpa",
                        "call_site": {"file": "job.py", "line": 5},
                    }
                ],
            }
        ],
    }
    imp = trace_column_impact(plan_doc, table="dev.compliance.tb_restritiva", column="cod_bco")
    assert imp["hops"][0]["plans"][0]["output_columns_affected"] == []


def test_r4_impact_tracks_column_through_expand_rollup() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "Aggregate [cbco#10, dt_ref#11, spark_grouping_id#12L], "
                        "[cbco#10, dt_ref#11, sum(vl#3) AS total#13]\n"
                        "+- Expand [[cbco#1, dt_ref#2, vl#3, 0], [cbco#1, null, vl#3, 1]], "
                        "[cbco#10, dt_ref#11, vl#3, spark_grouping_id#12L]\n"
                        "   +- Relation dev.cadastros.tb_banco[cbco#1,dt_ref#2,vl#3] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_rollup",
                        "call_site": {"file": "job.py", "line": 6},
                    }
                ],
            }
        ],
    }
    imp = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="cbco")
    out_cols = [c["name"] for c in imp["hops"][0]["plans"][0]["output_columns_affected"]]
    assert "cbco" in out_cols


def test_r4_impact_handles_existence_join() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "Join ExistenceJoin(exists#99), (cbco#1 = cod_bco#10)\n"
                        ":- Relation dev.cadastros.tb_banco[cbco#1,nm_banco#2] parquet\n"
                        "+- Relation dev.compliance.tb_restritiva[cod_bco#10,ds_motivo#11] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_out",
                        "call_site": {"file": "job.py", "line": 4},
                    }
                ],
            }
        ],
    }
    imp = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="cbco")
    assert len(imp["hops"][0]["plans"][0]["joins"]) == 1


def test_r4_impact_except_preserves_left_output_columns() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "Except false\n"
                        ":- Relation dev.cadastros.tb_banco[cbco#1,nm_banco#2] parquet\n"
                        "+- Relation dev.compliance.tb_restritiva[cod_bco#10,ds_motivo#11] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_diff",
                        "call_site": {"file": "job.py", "line": 4},
                    }
                ],
            }
        ],
    }
    imp = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="cbco")
    out_cols = [c["name"] for c in imp["hops"][0]["plans"][0]["output_columns_affected"]]
    assert "cbco" in out_cols


# --------------------------------------------------------------------------- #
# R4-11, R4-12, R4-13 a R4-17: notebooks .ipynb, widgets, head, pandas, CLI
# --------------------------------------------------------------------------- #


def test_r4_capturer_pandas_cache_returns_copy() -> None:
    cap = cp.Capturer(Path("."), "synthetic", 1, {})
    df = _ExtendedDFR4("Relation a[id#1] parquet", "", [e2e._FakeField("id", "int")])
    # Simula passada 0 (has_flipped=True) populando cache e mutando o DataFrame retornado
    cap.call_site = lambda: {"file": "job.py", "line": 1, "call_chain": [{"file": "job.py", "line": 1}]}  # type: ignore[method-assign]
    pdf0 = cap.pandas(df)
    assert pdf0 is not None
    pdf0["coluna_mutada"] = 999
    # Na passada 1 (has_flipped=False), o cache não pode vazar a mutação da passada 0
    cap.explorer.has_flipped = False
    pdf1 = cap.pandas(df)
    assert pdf1 is not None
    assert "coluna_mutada" not in pdf1.columns


def test_r4_cli_output_creates_missing_parent_directory() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        out_file = Path(tmp) / "nao_existe" / "sub" / "sources.json"
        cli._write_optional_output(str(out_file), {"ok": True}, "{}\n", "sources")
        assert out_file.is_file()


def test_r4_ipynb_indented_line_magic_preserves_indentation() -> None:
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [
            {
                "cell_type": "code",
                "metadata": {},
                "source": ["if True:\n", "    %pip install foo\n", "    x = 1\n"],
            }
        ],
    }
    pre = cp.preprocess_notebook_source(json.dumps(nb), "indent.ipynb")
    compile(pre, "indent.ipynb", "exec")


def test_r4_ipynb_single_line_percent_time_executes_inline_code() -> None:
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [
            {
                "cell_type": "code",
                "metadata": {},
                "source": ['%time df.write.saveAsTable("dev.tgt.tb_t1")\n'],
            }
        ],
    }
    pre = cp.preprocess_notebook_source(json.dumps(nb), "t1.ipynb")
    assert 'df.write.saveAsTable("dev.tgt.tb_t1")' in pre


def test_r4_ipynb_comment_before_percent_sql_compiles() -> None:
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [
            {
                "cell_type": "code",
                "metadata": {},
                "source": ["# Query vendas\n", "%sql\n", "SELECT 1\n"],
            }
        ],
    }
    pre = cp.preprocess_notebook_source(json.dumps(nb), "cmt.ipynb")
    compile(pre, "cmt.ipynb", "exec")


def test_r4_dbutils_widgets_remove_clears_default() -> None:
    db = cp.DBUtilsStub({})
    db.widgets.text("modo", "REPROC")
    db.widgets.remove("modo")
    assert db.widgets.get("modo") == ""


def test_r4_df_head_none_returns_single_row() -> None:
    cap = cp.Capturer(Path("."), "synthetic", 1, {})
    cap_any: Any = cap
    cap_any.rows = lambda _df: ["ROW0"]
    cap_any.first_row = lambda _df: "ROW0"
    res = cp.DF_ACTIONS["head"](cap, None, (None,), {})
    assert res == "ROW0"


# --------------------------------------------------------------------------- #
# Reprodutores consolidados em repro/
# --------------------------------------------------------------------------- #


def test_r4_repro_file_job_bugs_r4_py() -> None:
    with _spark_env_r4(e2e._CATALOG):
        res = cp.capture_plans(str(_REPRO_DIR), "job_bugs_r4.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert {
        "dev.tgt.tb_a",
        "dev.tgt.tb_b",
        "dev.tgt.tb_c",
        "dev.tgt.tb_widget_removido",
        "dev.tgt.tb_head_none_ok",
    } <= dest_tbls


def test_r4_repro_file_pipeline_bugs_r4_sql() -> None:
    with _spark_env_r4(e2e._CATALOG):
        res = cp.capture_plans(str(_REPRO_DIR), "pipeline_bugs_r4.sql", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert "DIRECTORY" not in dest_tbls
    assert {
        "/mnt/out/r4",
        "dev.tgt.tb_merge_ev",
        "dev.tgt.tb_del_sem_from",
        "dev.tgt.tb_streaming_dlt",
    } <= dest_tbls


def test_r4_repro_file_notebook_bugs_r4_ipynb() -> None:
    with _spark_env_r4(e2e._CATALOG):
        res = cp.capture_plans(str(_REPRO_DIR), "notebook_bugs_r4.ipynb", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert {"dev.tgt.tb_time_inline", "dev.tgt.tb_indent_pip"} <= {d["table"] for d in res["destinations"]}

if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"OK  {name}")
