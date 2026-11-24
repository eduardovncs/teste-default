"""Testes do formato compactado padrão (spark-lineage-compact/v1) e sua integração com CLI/sources/query/impact."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import tempfile
from pathlib import Path
from typing import Any

from spark_lineage import (
    COMPACT_FORMAT,
    build_sources,
    cli,
    compact_plan,
    extract_tag,
    is_compact_doc,
    render_compact_markdown,
    trace_column_impact,
)
from spark_lineage import capture_plans as cp

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_e2e_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "_test_capture_e2e_helpers_cmp",
        _REPO_ROOT / "tests" / "test_capture_e2e.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


e2e: Any = _load_e2e_module()


def _sample_multihop_raw() -> dict[str, Any]:
    return {
        "project_root": "/proj",
        "entrypoint": "jobs/etl_compe.py",
        "params": {"modo": "NORMAL"},
        "settings": {"explore_branches": True, "branch_workers": 4},
        "status": "ok",
        "error": None,
        "catalog": {
            "dev.cadastros.tb_banco": {
                "cbco": {"type": "int", "comment": "Código COMPE do banco"},
                "nm_banco": {"type": "string", "comment": "Nome do banco"},
                "col_inutil": {"type": "string", "comment": "Não utilizada"},
            },
            "dev.gold.tb_final": {
                "cod_banco": {"type": "string", "comment": "Código COMPE formatado"},
            },
        },
        "branches": [
            {
                "site": "jobs/etl_compe.py:20",
                "condition": "modo == 'REPROC'",
                "natural_value": False,
                "explored_values": [False, True],
                "status_by_value": {"False": "ok", "True": "ok"},
            }
        ],
        "events": [{"kind": "sql_blocked", "statement_kind": "OPTIMIZE", "query": "OPTIMIZE dev.gold.tb_final"}],
        "capture_warnings": [],
        "read_sites": {"dev.cadastros.tb_banco": ["jobs/etl_compe.py:10"]},
        "plans": [
            {
                "id": "P001",
                "hash": "aaa111",
                "output_columns": [{"name": "cbco", "type": "int"}],
                "sections": {
                    "parsed": "== Parsed Logical Plan ==\n" + ("x" * 2000),
                    "analyzed": (
                        "Project [cbco#1]\n"
                        "+- Filter (cbco#1 > 0)\n"
                        "   +- Relation dev.cadastros.tb_banco[cbco#1,nm_banco#2,col_inutil#3] parquet\n"
                    ),
                    "optimized": "== Optimized Logical Plan ==\n" + ("y" * 2000),
                    "physical": (
                        "PhotonScan parquet dev.cadastros.tb_banco[cbco#1] "
                        "DataFilters: [(cbco#1 > 0)], ReadSchema: struct<cbco:int>"
                    ),
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.stg.tb_intermediaria",
                        "call_site": {"file": "jobs/etl_compe.py", "line": 15},
                    }
                ],
            },
            {
                "id": "P002",
                "hash": "bbb222",
                "output_columns": [{"name": "cod_banco", "type": "string"}],
                "sections": {
                    "analyzed": (
                        "Project [cast(cbco#10 as string) AS cod_banco#11]\n"
                        "+- Relation dev.stg.tb_intermediaria[cbco#10] parquet\n"
                    ),
                    "physical": "PhotonScan parquet dev.stg.tb_intermediaria[cbco#10] ReadSchema: struct<cbco:int>",
                },
                "occurrences": [
                    {
                        "action": "sql:INSERT",
                        "target": "dev.gold.tb_final",
                        "call_site": {"file": "jobs/etl_compe.py", "line": 25},
                        "extra": {
                            "query": "INSERT INTO dev.gold.tb_final SELECT CAST(cbco AS STRING) FROM dev.stg.tb_intermediaria"
                        },
                    }
                ],
            },
        ],
    }


def test_compact_plan_drops_raw_sections_and_preserves_sources_queries_and_lineage() -> None:
    raw = _sample_multihop_raw()
    compact = compact_plan(raw)

    assert is_compact_doc(compact)
    assert compact["format"] == COMPACT_FORMAT
    assert "plans" not in compact
    assert compact["plans_analyzed"] == 2
    assert len(json.dumps(compact)) < len(json.dumps(raw))
    assert len(compact["queries"]) == 1
    assert compact["queries"][0]["target"] == "dev.gold.tb_final"
    assert "cbco" in compact["column_lineage"]["dev.cadastros.tb_banco"]
    # Coluna nunca lida (`read == False`) não entra em column_lineage
    assert "col_inutil" not in compact["column_lineage"]["dev.cadastros.tb_banco"]


def test_sources_query_and_impact_work_identically_on_compact_doc() -> None:
    raw = _sample_multihop_raw()
    compact = compact_plan(raw)

    # 1. build_sources e extract_tag em cima do compactado
    doc_raw = build_sources(raw)
    doc_cmp = build_sources(compact)
    assert [s["table"] for s in doc_cmp["sources"]] == [s["table"] for s in doc_raw["sources"]]
    assert [d["table"] for d in doc_cmp["destinations"]] == [d["table"] for d in doc_raw["destinations"]]
    assert doc_cmp["flows"] == doc_raw["flows"]

    assert len(extract_tag(compact, doc_cmp, tag="queries")) == 1
    assert len(extract_tag(compact, doc_cmp, tag="queries", table="tb_final")) == 1
    assert extract_tag(compact, doc_cmp, tag="queries", table="tb_outra") == []
    assert len(extract_tag(compact, doc_cmp, tag="branches")) == 1
    assert "dev.cadastros.tb_banco" in extract_tag(compact, doc_cmp, tag="column_lineage", table="tb_banco")

    # 2. trace_column_impact multi-hop idêntico (inclusive por sufixo e case-insensitive)
    imp_raw = trace_column_impact(raw, table="tb_banco", column="CBCO")
    imp_cmp = trace_column_impact(compact, table="tb_banco", column="CBCO")
    assert imp_cmp["summary"] == imp_raw["summary"]
    assert imp_cmp["summary"]["found"] is True
    assert imp_cmp["summary"]["hops"] == 2
    assert imp_cmp["destinations_affected"] == imp_raw["destinations_affected"]
    assert imp_cmp["type"] == "int"
    assert imp_cmp["comment"] == "Código COMPE do banco"

    # 3. Coluna não usada retorna found=False mantendo metadados do catálogo
    imp_unused = trace_column_impact(compact, table="tb_banco", column="col_inutil")
    assert imp_unused["summary"]["found"] is False
    assert imp_unused["destinations_affected"] == []
    assert imp_unused["comment"] == "Não utilizada"


def test_compact_plan_filters_by_tables_and_columns() -> None:
    raw = _sample_multihop_raw()
    filtered = compact_plan(raw, tables=["tb_banco"], columns=["cbco"])
    assert list(filtered["column_lineage"].keys()) == ["dev.cadastros.tb_banco"]
    assert list(filtered["column_lineage"]["dev.cadastros.tb_banco"].keys()) == ["cbco"]

    md = render_compact_markdown(filtered)
    assert "Linhagem por coluna" in md
    assert "`cbco`" in md
    assert "dev.gold.tb_final" in md


def test_cli_capture_writes_compact_by_default_and_supports_raw_flags() -> None:
    src = (
        "df = spark.table('dev.cadastros.tb_banco')\n"
        "df_f = df.filter('cbco > 0')\n"
        "if dbutils.widgets.get('modo') == 'REPROC':\n"
        "    df_f.write.saveAsTable('dev.tgt.tb_reproc')\n"
        "else:\n"
        "    df_f.write.saveAsTable('dev.tgt.tb_normal')\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(src, encoding="utf-8")
        out_compact = root / "out" / "lineage.json"
        out_raw = root / "out" / "plan_raw.json"
        out_raw_mode = root / "out" / "only_raw.json"

        with e2e._fake_spark_env(e2e._CATALOG):
            rc = cli.main(
                [
                    "capture",
                    "--project-root",
                    str(root),
                    "--entrypoint",
                    "job.py",
                    "--output",
                    str(out_compact),
                    "--raw-output",
                    str(out_raw),
                    "--no-catalog",
                ]
            )
            assert rc == 0

            rc_raw = cli.main(
                [
                    "capture",
                    "--project-root",
                    str(root),
                    "--entrypoint",
                    "job.py",
                    "--output",
                    str(out_raw_mode),
                    "--raw",
                    "--no-catalog",
                ]
            )
            assert rc_raw == 0

        compact_data = json.loads(out_compact.read_text(encoding="utf-8"))
        raw_data = json.loads(out_raw.read_text(encoding="utf-8"))
        raw_mode_data = json.loads(out_raw_mode.read_text(encoding="utf-8"))

        # Padrão da CLI é compacto (sem plans brutos)
        assert is_compact_doc(compact_data)
        assert "plans" not in compact_data
        assert "plans" in raw_data and len(raw_data["plans"]) >= 1
        assert "plans" in raw_mode_data and not is_compact_doc(raw_mode_data)

        # CLI `query`, `impact` e `compact --format md` funcionam direto sobre o arquivo compactado
        buf_q = io.StringIO()
        with contextlib.redirect_stdout(buf_q):
            assert cli.main(["query", "--plans", str(out_compact), "--tag", "destinations"]) == 0
        dests = json.loads(buf_q.getvalue())
        assert {d["table"] for d in dests} == {"dev.tgt.tb_reproc", "dev.tgt.tb_normal"}

        buf_i = io.StringIO()
        with contextlib.redirect_stdout(buf_i):
            assert cli.main(["impact", "--plans", str(out_compact), "--table", "tb_banco", "--column", "cbco"]) == 0
        imp = json.loads(buf_i.getvalue())
        assert imp["summary"]["found"] is True
        assert {d["table"] for d in imp["destinations_affected"]} == {"dev.tgt.tb_reproc", "dev.tgt.tb_normal"}

        buf_md = io.StringIO()
        with contextlib.redirect_stdout(buf_md):
            assert cli.main(["compact", "--plans", str(out_compact), "--format", "md"]) == 0
        assert "Linhagem por coluna" in buf_md.getvalue()


def test_sources_catalog_refresh_works_on_compact_doc() -> None:
    raw = _sample_multihop_raw()
    raw["catalog"] = {}
    compact = compact_plan(raw)

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "compact.json"
        p.write_text(json.dumps(compact), encoding="utf-8")

        cli_mod: Any = cli
        old_fetch = cli_mod.fetch_catalog_schema
        cli_mod.fetch_catalog_schema = lambda tables: {
            t: {"cbco": {"type": "int", "comment": "Atualizado"}} for t in tables
        }
        try:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                assert cli.main(["sources", "--plans", str(p), "--catalog", "--tag", "sources"]) == 0
        finally:
            cli_mod.fetch_catalog_schema = old_fetch

    sources = json.loads(buf.getvalue())
    bco = next(s for s in sources if s["table"] == "dev.cadastros.tb_banco")
    cbco_col = next(c for c in bco["columns"] if c["name"] == "cbco")
    assert cbco_col["comment"] == "Atualizado"


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"OK  {name}")
