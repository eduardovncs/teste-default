"""Testes de regressão de bugs conhecidos — Rodada 5."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from spark_lineage import capture_plans as cp
from spark_lineage import cli, doctor
from spark_lineage.impact import trace_column_impact
from spark_lineage.plan_parser import (
    _parse_attr,
    parse_formatted_scans,
    parse_merge_targets,
    parse_read_schema,
    parse_relations,
    parse_scans,
)
from spark_lineage.sources import build_sources, render_markdown

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REPRO_DIR = _REPO_ROOT / "repro"


def _load_e2e_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "_test_capture_e2e_helpers_r5",
        _REPO_ROOT / "tests" / "test_capture_e2e.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


e2e: Any = _load_e2e_module()


@contextlib.contextmanager
def _clean_databricks_env(**overrides: str) -> Any:
    old = dict(os.environ)
    try:
        for k in list(os.environ):
            if k.startswith("DATABRICKS_") or k == "SPARK_REMOTE":
                del os.environ[k]
        os.environ.update(overrides)
        yield
    finally:
        os.environ.clear()
        os.environ.update(old)


class _GlobalViewDF(e2e._FakeDF):
    def __init__(self, *args: Any, spark_ref: Any = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._spark_ref = spark_ref

    def createGlobalTempView(self, name: str) -> None:  # noqa: N802
        if name in self._spark_ref._global_views:
            raise RuntimeError(f"[TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] global view '{name}' already exists")
        self._spark_ref._global_views[name] = self

    def createOrReplaceGlobalTempView(self, name: str) -> None:  # noqa: N802
        self._spark_ref._global_views[name] = self


class _GlobalViewSpark(e2e._FakeSparkSession):
    def __init__(self) -> None:
        super().__init__()
        self._global_views: dict[str, Any] = {}

    def _make_table_df(self, name: str, base_id: int = 1) -> _GlobalViewDF:
        base = super()._make_table_df(name, base_id=base_id)
        return _GlobalViewDF(base._analyzed, base._physical, base.schema.fields, base._formatted, spark_ref=self)


# =========================================================================== #
# R5-1: doctor.py — KeyError em render_doctor_text + '%' em ~/.databrickscfg
# =========================================================================== #


def test_r5_doctor_render_text_survives_invalid_databrickscfg(tmp_path: Path) -> None:
    cfg = tmp_path / ".databrickscfg"
    cfg.write_text("[DEFAULT\nhost = https://x.cloud.databricks.com\n", encoding="utf-8")

    with _clean_databricks_env(DATABRICKS_CONFIG_FILE=str(cfg)):
        report = doctor.run_doctor(connect=False, check_auth=False)
        rendered = doctor.render_doctor_text(report)

    assert report["databricks_config"]["status"] == "error"
    assert "Erro ao ler" in rendered


def test_r5_doctor_config_allows_percent_in_values(tmp_path: Path) -> None:
    cfg = tmp_path / ".databrickscfg"
    cfg.write_text(
        "[DEFAULT]\nhost = https://x.cloud.databricks.com\ntoken = dapi123%abc\ncluster_id = 0101-abc\n",
        encoding="utf-8",
    )

    with _clean_databricks_env(DATABRICKS_CONFIG_FILE=str(cfg)):
        res = doctor._check_databricks_config(profile="DEFAULT")

    assert res["status"] == "ok", f"Esperava status='ok' para valor com '%', obtido: {res}"


# =========================================================================== #
# R5-2: doctor.py — perfil nomeado herda [DEFAULT]
# =========================================================================== #


def test_r5_doctor_named_profile_does_not_inherit_default(tmp_path: Path) -> None:
    cfg = tmp_path / ".databrickscfg"
    cfg.write_text(
        "[DEFAULT]\n"
        "host = https://def.cloud.databricks.com\n"
        "token = dapi-default\n"
        "cluster_id = 0101-default\n\n"
        "[staging]\n"
        "host = https://stg.cloud.databricks.com\n",
        encoding="utf-8",
    )

    with _clean_databricks_env(DATABRICKS_CONFIG_FILE=str(cfg)):
        res = doctor._check_databricks_config(profile="staging")

    assert res["status"] == "error", f"Perfil [staging] sem token/cluster_id não deveria herdar de [DEFAULT]: {res}"
    assert res["auth_type"] is None
    assert res["compute"] is None


# =========================================================================== #
# R5-3: doctor.py — DATABRICKS_CLIENT_ID (OAuth M2M via env vars)
# =========================================================================== #


def test_r5_doctor_env_settings_recognizes_oauth_m2m_client_id(tmp_path: Path) -> None:
    with _clean_databricks_env(
        DATABRICKS_CONFIG_FILE=str(tmp_path / "inexistente.cfg"),
        DATABRICKS_HOST="https://m2m.cloud.databricks.com",
        DATABRICKS_CLIENT_ID="my-client-id",
        DATABRICKS_CLIENT_SECRET="dummy-test-value",  # noqa: S106
        DATABRICKS_CLUSTER_ID="0101-m2m",
    ):
        res = doctor._check_databricks_config()

    assert res["status"] == "ok", f"OAuth M2M via env vars deveria ser aceito: {res}"
    assert res["auth_type"] == "oauth-m2m"


# =========================================================================== #
# R5-4: cli.py — --catalog --tag catalog retorna catálogo velho
# =========================================================================== #


def test_r5_cli_sources_catalog_flag_updates_tag_catalog_output(tmp_path: Path) -> None:
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(
        json.dumps(
            {
                "status": "ok",
                "entrypoint": "job.py",
                "plans": [
                    {
                        "id": "P001",
                        "sections": {"analyzed": "Relation dev.src.tb[a#1] parquet"},
                        "occurrences": [],
                    }
                ],
                "catalog": {"dev.src.tb": {"a": {"type": "old_type"}}},
            }
        ),
        encoding="utf-8",
    )

    cli_mod: Any = cli
    old_fetch = cli_mod.fetch_catalog_schema
    cli_mod.fetch_catalog_schema = lambda _tables: {"dev.src.tb": {"a": {"type": "fresh_type", "comment": "novo"}}}
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cli.main(["sources", "--plans", str(plan_file), "--catalog", "--tag", "catalog", "--format", "json"])
    finally:
        cli_mod.fetch_catalog_schema = old_fetch

    payload = json.loads(buf.getvalue())
    assert payload["dev.src.tb"]["a"]["type"] == "fresh_type"


# =========================================================================== #
# R5-5: sources.py — scans_by_table e _finalize_source case-sensitive
# =========================================================================== #


def test_r5_sources_scan_matching_is_case_insensitive() -> None:
    result = {
        "status": "ok",
        "entrypoint": "job.py",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": "Relation DEV.SRC.TB[ID#1, VALOR#2] parquet",
                    "physical": "FileScan parquet dev.src.tb[id#1, valor#2] ReadSchema: struct<id:int,valor:double>",
                },
                "occurrences": [{"action": "collect", "target": None, "call_site": None}],
            }
        ],
    }
    doc = build_sources(result)
    cols = {c["name"].lower(): c for c in doc["sources"][0]["columns"]}
    assert cols["id"]["read"] is True, f"Esperava read=True após match do scan case-insensitive: {cols}"
    assert cols["valor"]["type"] == "double"


def test_r5_sources_finalize_does_not_duplicate_case_variant_columns() -> None:
    result = {
        "status": "ok",
        "entrypoint": "job.py",
        "plans": [
            {
                "id": "P001",
                "sections": {"analyzed": "Relation DEV.SRC.TB[ID#1, VALOR#2] parquet"},
                "occurrences": [],
            }
        ],
        "catalog": {
            "dev.src.tb": {
                "id": {"type": "int", "comment": "pk"},
                "valor": {"type": "double", "comment": "val"},
                "extra": {"type": "string", "comment": "nao lida"},
            }
        },
    }
    doc = build_sources(result)
    col_names_lower = [c["name"].lower() for c in doc["sources"][0]["columns"]]
    assert len(col_names_lower) == 3, f"Colunas duplicadas por diferença de caixa: {doc['sources'][0]['columns']}"
    assert set(col_names_lower) == {"id", "valor", "extra"}


# =========================================================================== #
# R5-6: sources.py — mesma tabela em caixas diferentes cria fontes duplicadas
# =========================================================================== #


def test_r5_sources_merges_same_table_across_case_variants() -> None:
    result = {
        "status": "ok",
        "entrypoint": "job.py",
        "plans": [
            {"id": "P001", "sections": {"analyzed": "Relation DEV.SRC.TB[id#1] parquet"}, "occurrences": []},
            {"id": "P002", "sections": {"analyzed": "Relation dev.src.tb[id#2] parquet"}, "occurrences": []},
        ],
    }
    doc = build_sources(result)
    assert len(doc["sources"]) == 1, f"Fontes duplicadas por caixa: {[s['table'] for s in doc['sources']]}"
    assert set(doc["sources"][0]["plans"]) == {"P001", "P002"}


# =========================================================================== #
# R5-7: sources.py — _md_events KeyError quando falta 'query'
# =========================================================================== #


def test_r5_sources_render_markdown_events_without_query_key() -> None:
    result = {
        "status": "ok",
        "entrypoint": "job.py",
        "plans": [],
        "events": [{"kind": "dbutils", "detail": "notebook_run"}],
    }
    doc = build_sources(result)
    md = render_markdown(doc, tag="events")
    assert "dbutils" in md
    assert "notebook_run" in md


# =========================================================================== #
# R5-8: plan_parser.py — Scan ExistingRDD não ignorado
# =========================================================================== #


def test_r5_plan_parser_ignores_existing_rdd_scans_and_relations() -> None:
    phys = "*(1) Scan ExistingRDD[id#1,nome#2]"
    fmt = "(1) Scan ExistingRDD[id#1,nome#2]\nOutput [2]: [id#1, nome#2]\n"
    assert parse_relations(phys) == []
    assert parse_scans(phys) == []
    assert parse_formatted_scans(fmt) == []


def test_r5_sources_does_not_create_phantom_source_for_existing_rdd() -> None:
    result = {
        "status": "ok",
        "entrypoint": "job.py",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": "*(1) Scan ExistingRDD[id#1,nome#2]",
                    "physical": "*(1) Scan ExistingRDD[id#1,nome#2]",
                },
                "occurrences": [{"action": "write.saveAsTable", "target": "dev.tgt.tb", "call_site": None}],
            }
        ],
    }
    doc = build_sources(result)
    assert doc["sources"] == [], f"ExistingRDD não deveria virar fonte: {doc['sources']}"


# =========================================================================== #
# R5-9: plan_parser.py — '#' sem id em _parse_attr e ':' em parse_read_schema
# =========================================================================== #


def test_r5_plan_parser_attr_preserves_hash_without_expr_id() -> None:
    assert _parse_attr("`cod#ref`") == {"name": "cod#ref", "id": None}


def test_r5_plan_parser_read_schema_preserves_colon_in_backticked_column() -> None:
    schema, truncated = parse_read_schema("ReadSchema: struct<`meta:id`:string,val:int>")
    assert not truncated
    assert schema == {"meta:id": "string", "val": "int"}


# =========================================================================== #
# R5-10: plan_parser.py — MergeIntoCommand sem SubqueryAlias
# =========================================================================== #


def test_r5_plan_parser_merge_targets_without_subquery_alias() -> None:
    analyzed = (
        "MergeIntoCommandEdge Relation dev.src.origem[id#1, valor#2], "
        "Relation dev.tgt.destino[id#3, valor#4], `dev`.`tgt`.`destino`, Delta[...], `valor` = valor#2"
    )
    merges = parse_merge_targets(analyzed)
    assert len(merges) == 1
    assert merges[0]["table"] == "dev.tgt.destino"
    assert "valor" in merges[0]["columns"]


# =========================================================================== #
# R5-11: impact.py — Aggregate global sem GROUP BY (1 bloco [...])
# =========================================================================== #


def test_r5_impact_handles_global_aggregate_without_group_by() -> None:
    plan = {
        "id": "P001",
        "sections": {
            "analyzed": "\n".join(
                [
                    "Aggregate [sum(valor#2) AS total#10]",
                    "+- Relation dev.src.vendas[id#1, valor#2] parquet",
                ]
            )
        },
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.tgt.resumo", "call_site": None}],
    }
    rep = trace_column_impact({"status": "ok", "entrypoint": "j.py", "plans": [plan]}, "dev.src.vendas", "valor")
    p = rep["hops"][0]["plans"][0]
    assert any(t["kind"] == "aggregate" and t["output_column"] == "total" for t in p["transformations"])
    assert p["output_columns_affected"] == [{"name": "total", "direct": False, "via": "valor"}]


# =========================================================================== #
# R5-12: impact.py — _extract_bracket_groups não ignora ']' em literais
# =========================================================================== #


def test_r5_impact_extract_bracket_groups_ignores_brackets_in_string_literals() -> None:
    plan = {
        "id": "P001",
        "sections": {
            "analyzed": "\n".join(
                [
                    "Project [regexp_replace(texto#1, ']', '') AS limpo#10, valor#2 AS vl_final#11]",
                    "+- Relation dev.src.tb[texto#1, valor#2] parquet",
                ]
            )
        },
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.tgt.dst", "call_site": None}],
    }
    rep = trace_column_impact({"status": "ok", "entrypoint": "j.py", "plans": [plan]}, "dev.src.tb", "valor")
    outputs = [c["name"] for c in rep["hops"][0]["plans"][0]["output_columns_affected"]]
    assert outputs == ["vl_final"]


# =========================================================================== #
# R5-13: impact.py — Subqueries escalar (Project) e IN (Filter)
# =========================================================================== #


def test_r5_impact_propagates_scalar_subquery_in_project() -> None:
    plan = {
        "id": "P001",
        "sections": {
            "analyzed": "\n".join(
                [
                    "Project [id#1, scalar-subquery#10 [] AS media#11]",
                    ":  +- Aggregate [], [avg(x#2) AS avg(x)#12]",
                    ":     +- Relation dev.src.tb_ref[x#2] parquet",
                    "+- Relation dev.src.tb_main[id#1] parquet",
                ]
            )
        },
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.tgt.dst", "call_site": None}],
    }
    rep = trace_column_impact({"status": "ok", "entrypoint": "j.py", "plans": [plan]}, "dev.src.tb_ref", "x")
    outputs = [c["name"] for c in rep["hops"][0]["plans"][0]["output_columns_affected"]]
    assert "media" in outputs


def test_r5_impact_records_in_subquery_filter() -> None:
    plan = {
        "id": "P001",
        "sections": {
            "analyzed": "\n".join(
                [
                    "Filter id#1 IN (list#10 [])",
                    ":  +- Project [ref_id#2]",
                    ":     +- Relation dev.src.tb_ref[ref_id#2] parquet",
                    "+- Relation dev.src.tb_main[id#1, valor#3] parquet",
                ]
            )
        },
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.tgt.dst", "call_site": None}],
    }
    rep = trace_column_impact({"status": "ok", "entrypoint": "j.py", "plans": [plan]}, "dev.src.tb_ref", "ref_id")
    assert len(rep["hops"][0]["plans"][0]["filters"]) >= 1


# =========================================================================== #
# R5-14: impact.py — cast + aritmética e alias apenas de caixa
# =========================================================================== #


def test_r5_impact_cast_plus_arithmetic_is_derive_not_direct_cast() -> None:
    plan = {
        "id": "P001",
        "sections": {
            "analyzed": "\n".join(
                [
                    "Project [cast(valor#1 as double) + 10.0 AS novo_valor#2]",
                    "+- Relation dev.src.tb[valor#1] parquet",
                ]
            )
        },
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.tgt.dst", "call_site": None}],
    }
    rep = trace_column_impact({"status": "ok", "entrypoint": "j.py", "plans": [plan]}, "dev.src.tb", "valor")
    p = rep["hops"][0]["plans"][0]
    assert p["transformations"][0]["kind"] == "derive"
    assert p["output_columns_affected"][0]["direct"] is False


def test_r5_impact_case_only_alias_is_not_derive() -> None:
    plan = {
        "id": "P001",
        "sections": {
            "analyzed": "\n".join(
                [
                    "Project [VALOR#1 AS valor#2]",
                    "+- Relation dev.src.tb[VALOR#1] parquet",
                ]
            )
        },
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.tgt.dst", "call_site": None}],
    }
    rep = trace_column_impact({"status": "ok", "entrypoint": "j.py", "plans": [plan]}, "dev.src.tb", "valor")
    p = rep["hops"][0]["plans"][0]
    assert p["output_columns_affected"][0]["direct"] is True
    assert not any(t["kind"] == "derive" for t in p["transformations"])


# =========================================================================== #
# R5-15: impact.py — multi-hop qualificação assimétrica, dest_summary e catalog CI
# =========================================================================== #


def test_r5_impact_multihop_matches_qualified_dest_with_unqualified_read() -> None:
    p1 = {
        "id": "P001",
        "sections": {"analyzed": "Relation dev.src.raw[valor#1] parquet"},
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.stg.intermediaria", "call_site": None}],
    }
    p2 = {
        "id": "P002",
        "sections": {"analyzed": "Relation stg.intermediaria[valor#10] parquet"},
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.gold.final", "call_site": None}],
    }
    rep = trace_column_impact({"status": "ok", "entrypoint": "j.py", "plans": [p1, p2]}, "dev.src.raw", "valor")
    dest_tables = [d["table"] for d in rep["destinations_affected"]]
    assert "dev.gold.final" in dest_tables
    assert rep["summary"]["hops"] == 2


def test_r5_impact_merges_destinations_case_insensitively() -> None:
    p1 = {
        "id": "P001",
        "sections": {"analyzed": "Relation dev.src.tb[valor#1] parquet"},
        "occurrences": [{"action": "write.saveAsTable", "target": "DEV.TGT.DST", "call_site": None}],
    }
    p2 = {
        "id": "P002",
        "sections": {"analyzed": "Project [valor#1 AS v2#2]\n+- Relation dev.src.tb[valor#1] parquet"},
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.tgt.dst", "call_site": None}],
    }
    rep = trace_column_impact({"status": "ok", "entrypoint": "j.py", "plans": [p1, p2]}, "dev.src.tb", "valor")
    assert len(rep["destinations_affected"]) == 1


def test_r5_impact_catalog_lookup_is_case_insensitive() -> None:
    rep = trace_column_impact(
        {
            "status": "ok",
            "entrypoint": "j.py",
            "plans": [],
            "catalog": {"dev.src.tb": {"valor": {"type": "decimal(18,2)", "comment": "valor monetário"}}},
        },
        table="DEV.SRC.TB",
        column="valor",
    )
    assert rep["type"] == "decimal(18,2)"
    assert rep["comment"] == "valor monetário"


# =========================================================================== #
# R5-16: impact.py — Except / Intersect lado direito silenciado
# =========================================================================== #


def test_r5_impact_except_records_right_side_column_usage() -> None:
    plan = {
        "id": "P001",
        "sections": {
            "analyzed": "\n".join(
                [
                    "Except false",
                    ":- Relation dev.src.tb_a[id#1] parquet",
                    "+- Relation dev.src.tb_b[id#2] parquet",
                ]
            )
        },
        "occurrences": [{"action": "write.saveAsTable", "target": "dev.tgt.dst", "call_site": None}],
    }
    rep = trace_column_impact({"status": "ok", "entrypoint": "j.py", "plans": [plan]}, "dev.src.tb_b", "id")
    assert rep["summary"]["operations_count"] >= 1


# =========================================================================== #
# R5-17: capture_plans.py — regressão R4-4 em _execute_forced_pass
# =========================================================================== #


def test_r5_forced_ternary_exception_warning_preserves_file_and_line() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        script = root / "tern_err.py"
        script.write_text(
            "flag = True\nx = 'ok' if flag else (_ for _ in ()).throw(ValueError('boom'))\n",
            encoding="utf-8",
        )
        cp_mod: Any = cp
        old_get = cp_mod.get_spark
        cp_mod.get_spark = lambda: None
        try:
            res = cp.capture_plans(
                str(root), "tern_err.py", inject_spark=False, explore_branches=True, fetch_catalog=False
            )
        finally:
            cp_mod.get_spark = old_get

    warn = next(w for w in res["capture_warnings"] if "boom" in w.get("message", ""))
    assert warn["call_site"] == {"file": "tern_err.py", "line": 2}, f"call_site corrompido: {warn['call_site']}"


# =========================================================================== #
# R5-18: capture_plans.py — operadores multilinha != e % em .ipynb
# =========================================================================== #


def test_r5_ipynb_multiline_not_equal_and_modulo_operators_compile_and_run() -> None:
    fake_spark = e2e._FakeSparkSession()
    cp_mod: Any = cp
    old_get = cp_mod.get_spark
    old_dfs = list(cp.DF_CLASSES)
    old_writers = list(cp.WRITER_CLASSES)
    old_sessions = list(cp.SESSION_CLASSES)
    old_readers = list(cp.READER_CLASSES)
    cp_mod.get_spark = lambda: fake_spark
    cp.DF_CLASSES[:] = [e2e._FakeDF]
    cp.WRITER_CLASSES[:] = [e2e._FakeWriter]
    cp.SESSION_CLASSES[:] = [e2e._FakeSparkSession]
    cp.READER_CLASSES[:] = [e2e._FakeReader]
    try:
        res = cp.capture_plans(
            str(_REPRO_DIR),
            "notebook_bugs_r5.ipynb",
            explore_branches=False,
            fetch_catalog=False,
        )
    finally:
        cp_mod.get_spark = old_get
        cp.DF_CLASSES[:] = old_dfs
        cp.WRITER_CLASSES[:] = old_writers
        cp.SESSION_CLASSES[:] = old_sessions
        cp.READER_CLASSES[:] = old_readers

    assert res["status"] == "ok", f"Falhou com erro: {res['error']}"
    dests = {d["table"] for d in res["destinations"]}
    assert "dev.tgt.tb_nb_r5_ops" in dests


# =========================================================================== #
# R5-19: capture_plans.py — _row_cache colisão de schema e mutação in-place
# =========================================================================== #


def test_r5_capturer_rows_cache_separates_schemas_at_same_call_site() -> None:
    from pyspark.sql import types as spark_types

    class _Field:
        def __init__(self, name: str, dt: Any) -> None:
            self.name = name
            self.dataType = dt

    class _DF:
        def __init__(self, fields: list[Any]) -> None:
            self.schema = type("_S", (), {"fields": fields})()

    explorer = cp.BranchExplorer(enabled=True)
    cap: Any = cp.Capturer(Path(".").resolve(), "synthetic", 1, {}, explorer=explorer)
    cap.call_site = lambda: {
        "file": "job.py",
        "line": 10,
        "function": "helper",
        "code": "df.collect()",
        "call_chain": [{"file": "job.py", "line": 10}],
    }

    df_a = _DF([_Field("col_a", spark_types.StringType())])
    df_b = _DF([_Field("col_b", spark_types.IntegerType())])

    explorer.start_pass(0)
    assert hasattr(cap.rows(df_a)[0], "col_a")
    assert hasattr(cap.rows(df_b)[0], "col_b")

    explorer.start_pass(1, target_site="job.py:50", target_value=False, planned_forces={"job.py:50": False})
    rows_a_p1 = cap.rows(df_a)
    assert hasattr(rows_a_p1[0], "col_a"), f"Retornou schema errado do cache: {rows_a_p1}"


def test_r5_capturer_rows_cache_returns_copy_preventing_mutation() -> None:
    from pyspark.sql import types as spark_types

    class _Field:
        def __init__(self, name: str, dt: Any) -> None:
            self.name = name
            self.dataType = dt

    class _DF:
        def __init__(self, fields: list[Any]) -> None:
            self.schema = type("_S", (), {"fields": fields})()

    explorer = cp.BranchExplorer(enabled=True)
    cap: Any = cp.Capturer(Path(".").resolve(), "synthetic", 1, {}, explorer=explorer)
    cap.call_site = lambda: {
        "file": "job.py",
        "line": 12,
        "function": "f",
        "code": "df.collect()",
        "call_chain": [{"file": "job.py", "line": 12}],
    }
    df = _DF([_Field("id", spark_types.IntegerType())])

    explorer.start_pass(0)
    rows_p0 = cap.rows(df)
    rows_p0.clear()

    explorer.start_pass(1, target_site="job.py:50", target_value=False, planned_forces={"job.py:50": False})
    rows_p1 = cap.rows(df)
    assert len(rows_p1) == 1, "Mutação in-place na lista de collect() vazou para a passada seguinte"


# =========================================================================== #
# R5-20: capture_plans.py — comentários em CACHE TABLE/TEMP VIEW + createGlobalTempView
# =========================================================================== #


def test_r5_rewrite_cache_table_with_inline_comment_before_select() -> None:
    q = "CACHE TABLE vw_r5 AS /* comentario */ SELECT * FROM dev.src.tb"
    mode, kind = cp.classify_sql(q)
    assert (mode, kind) == ("pass", "CACHE_TABLE_AS")
    rewritten = cp._rewrite_safe_temp_view_sql(q, kind)
    assert rewritten.upper().startswith("CREATE OR REPLACE TEMP VIEW")


def test_r5_rewrite_create_temp_view_with_inline_comment() -> None:
    q = "CREATE /* inline */ TEMP VIEW vw_r5 AS SELECT * FROM dev.src.tb"
    mode, kind = cp.classify_sql(q)
    assert (mode, kind) == ("pass", "CREATE_TEMP_VIEW")
    rewritten = cp._rewrite_safe_temp_view_sql(q, kind)
    assert "OR REPLACE" in rewritten.upper()


def test_r5_create_global_temp_view_is_patched_for_branch_passes() -> None:
    fake_spark = _GlobalViewSpark()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "job_gv.py").write_text(
            "df = spark.table('dev.src.tb')\n"
            "df.createGlobalTempView('vw_global')\n"
            "flag = True\n"
            "if flag:\n"
            "    df.write.saveAsTable('dev.tgt.a')\n"
            "else:\n"
            "    df.write.saveAsTable('dev.tgt.b')\n",
            encoding="utf-8",
        )
        cp_mod: Any = cp
        old_get = cp_mod.get_spark
        old_df_cls = list(cp.DF_CLASSES)
        old_writers = list(cp.WRITER_CLASSES)
        old_sessions = list(cp.SESSION_CLASSES)
        old_readers = list(cp.READER_CLASSES)
        cp_mod.get_spark = lambda: fake_spark
        cp.DF_CLASSES[:] = [_GlobalViewDF, e2e._FakeDF]
        cp.WRITER_CLASSES[:] = [e2e._FakeWriter]
        cp.SESSION_CLASSES[:] = [_GlobalViewSpark]
        cp.READER_CLASSES[:] = [e2e._FakeReader]
        try:
            res = cp.capture_plans(str(root), "job_gv.py", explore_branches=True, fetch_catalog=False)
        finally:
            cp_mod.get_spark = old_get
            cp.DF_CLASSES[:] = old_df_cls
            cp.WRITER_CLASSES[:] = old_writers
            cp.SESSION_CLASSES[:] = old_sessions
            cp.READER_CLASSES[:] = old_readers

    dests = {d["table"] for d in res["destinations"]}
    assert dests == {"dev.tgt.a", "dev.tgt.b"}, f"2a passada falhou em createGlobalTempView: {res['capture_warnings']}"


# =========================================================================== #
# R5-21: capture_plans.py — SQL params, multi-table DIRECTORY, extract_sql_sources
# =========================================================================== #


def test_r5_substitute_sql_params_escapes_single_quote_with_sql_quotes() -> None:
    out = cp.substitute_sql_params("SELECT * FROM tb WHERE nome = :nome", {"nome": "O'Brien"})
    assert out == "SELECT * FROM tb WHERE nome = 'O''Brien'", f"Gerou literal SQL inválido: {out}"


def test_r5_multi_table_insert_overwrite_directory_does_not_extract_keyword() -> None:
    q = "FROM dev.src.tb INSERT OVERWRITE DIRECTORY '/mnt/out1' SELECT id INSERT INTO dev.tgt.tb2 SELECT id"
    targets = cp.extract_sql_targets(q)
    assert "DIRECTORY" not in targets
    assert "dev.tgt.tb2" in targets


def test_r5_extract_sql_sources_ignores_extract_from_and_using_delta() -> None:
    s1 = cp.extract_sql_sources("SELECT EXTRACT(YEAR FROM dt_ref) FROM dev.src.vendas")
    assert s1 == ["dev.src.vendas"], f"Extraiu coluna falsa de EXTRACT: {s1}"

    s2 = cp.extract_sql_sources("CREATE TABLE dev.tgt.dst USING DELTA AS SELECT * FROM dev.src.vendas")
    assert s2 == ["dev.src.vendas"], f"Extraiu formato 'DELTA' como tabela: {s2}"


# =========================================================================== #
# R5-22: capture_plans.py — %timeit, # MAGIC %md indentado, %run comentário e ciclo
# =========================================================================== #


def test_r5_single_line_percent_timeit_executes_inline_code() -> None:
    src_py = "# MAGIC %timeit df.write.saveAsTable('dev.tgt.tb_timeit')\n"
    pre = cp.preprocess_notebook_source(src_py, "nb.py")
    assert "df.write.saveAsTable('dev.tgt.tb_timeit')" in pre


def test_r5_indented_magic_md_inside_if_does_not_cause_indentation_error() -> None:
    src_py = "if True:\n    # MAGIC %md\n    # MAGIC documentação\n"
    pre = cp.preprocess_notebook_source(src_py, "nb.py")
    compile(pre, "nb.py", "exec")


def test_r5_magic_run_strips_inline_comment_from_target_path() -> None:
    path_part, run_params = cp._parse_magic_run_args("./helper $dt=2026-01-01 # carrega utilitarios")
    assert path_part == "./helper"
    assert run_params == {"dt": "2026-01-01"}


def test_r5_magic_run_cycle_does_not_raise_recursion_error() -> None:
    cp_mod: Any = cp
    old_get = cp_mod.get_spark
    cp_mod.get_spark = lambda: None
    try:
        res = cp.capture_plans(
            str(_REPRO_DIR / "run_cycle"),
            "nb_a.py",
            inject_spark=False,
            explore_branches=False,
            fetch_catalog=False,
        )
    finally:
        cp_mod.get_spark = old_get

    assert res["status"] == "ok", f"Ciclo de %run não foi interrompido com segurança: {res['error']}"


# =========================================================================== #
# R5-23: capture_plans.py — crases em Delta/mergeInto, reader PATH e _split_sql_statements
# =========================================================================== #


def test_r5_delta_table_target_and_merge_into_normalize_backticks() -> None:
    fake_dt = type("_FakeDT", (), {"_tableOrViewName": "`dev`.`tgt`.`dst`"})()
    assert cp._delta_table_target(fake_dt) == "dev.tgt.dst"


def test_r5_reader_targets_matches_uppercase_option_keys() -> None:
    class _FakeReader:
        def __init__(self) -> None:
            self._options = {"PATH": "/mnt/dados/vendas"}

    assert cp._reader_targets(_FakeReader(), (), {}) == ["/mnt/dados/vendas"]


def test_r5_split_sql_statements_line_number_skips_leading_block_comment() -> None:
    stmts = cp._split_sql_statements([(0, "/* header */"), (1, ""), (2, "INSERT INTO dst SELECT 1;")])
    assert len(stmts) == 1
    assert stmts[0][0] == 2, f"Linha inicial errada (apontou para o comentário): {stmts[0][0]}"


def test_r5_split_sql_statements_does_not_escape_backslash_in_backticks() -> None:
    stmts = cp._split_sql_statements([(0, "SELECT `a\\` FROM tb; SELECT 2;")])
    assert len(stmts) == 2, f"Backslash dentro de crase engoliu o ';' e fundiu os comandos: {stmts}"

if __name__ == "__main__":
    import inspect

    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            if "tmp_path" in inspect.signature(fn).parameters:
                with tempfile.TemporaryDirectory() as _td:
                    fn(Path(_td))
            else:
                fn()
            print(f"OK  {name}")
