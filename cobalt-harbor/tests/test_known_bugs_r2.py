"""Testes de reprodução de bugs conhecidos — Rodada 2.

Todos os testes abaixo especificam o comportamento ESPERADO e estão marcados com
``@pytest.mark.xfail(strict=True)`` enquanto os bugs da Rodada 2 permanecem abertos:
- ``pytest -v`` mostra cada bug reproduzido como ``XFAIL`` sem quebrar a suíte.
- ``pytest tests/test_known_bugs_r2.py --runxfail`` exibe as falhas reais detalhadas.
- Ao corrigir um bug, o teste passará a reportar ``XPASS(strict)``, indicando que o
  marcador pode ser removido para virar teste de regressão permanente.
"""

from __future__ import annotations

import ast
import contextlib
import importlib.util
import json
import tempfile
from pathlib import Path
from typing import Any

import spark_lineage.capture_plans as cp
from spark_lineage.impact import trace_column_impact
from spark_lineage.plan_parser import parse_formatted_scans, parse_merge_targets, parse_relations
from spark_lineage.sources import build_sources, extract_tag

_REPO_ROOT = Path(__file__).resolve().parent.parent
_REPRO_DIR = _REPO_ROOT / "repro"


def _load_e2e_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "_test_capture_e2e_helpers_r2",
        _REPO_ROOT / "tests" / "test_capture_e2e.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


e2e: Any = _load_e2e_module()


class _FakeProtoMap:
    """Simula o ScalarMapContainer protobuf usado em DataStreamWriter._write_proto.options."""

    def __init__(self, data: dict[str, str]) -> None:
        self._data = dict(data)

    def __getitem__(self, key: str) -> str:
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def items(self) -> Any:
        return self._data.items()


class _FakeWriteProto:
    def __init__(self) -> None:
        self.options = _FakeProtoMap({})


class _FakeStreamWriter:
    def __init__(self, df: Any) -> None:
        self._df = df
        self._write_proto = _FakeWriteProto()

    def format(self, _fmt: str) -> _FakeStreamWriter:
        return self

    def option(self, k: str, v: str) -> _FakeStreamWriter:
        self._write_proto.options._data[k] = v
        return self

    def start(self, path: str | None = None) -> Any:
        raise AssertionError(f"writeStream.start({path}) não deveria executar de verdade!")


class _ExtendedFakeWriterR2(e2e._FakeWriter):
    def jdbc(self, url: str, table: str, mode: str | None = None, properties: Any = None) -> None:
        raise AssertionError(f"jdbc({url}, {table}, {mode}, {properties}) não deveria executar!")


class _ExtendedFakeDFR2(e2e._FakeDF):
    @property
    def write(self) -> _ExtendedFakeWriterR2:
        return _ExtendedFakeWriterR2(self)

    @property
    def writeStream(self) -> _FakeStreamWriter:  # noqa: N802
        return _FakeStreamWriter(self)


class _ExtendedFakeSparkSessionR2(e2e._FakeSparkSession):
    def _make_table_df(self, name: str, base_id: int = 1) -> _ExtendedFakeDFR2:
        base = super()._make_table_df(name, base_id=base_id)
        return _ExtendedFakeDFR2(base._analyzed, base._physical, base.schema.fields, base._formatted, base._rows_data)

    def range(self, _n: int) -> _ExtendedFakeDFR2:
        return _ExtendedFakeDFR2(
            "Relation range_src[id#1] parquet",
            "PhotonScan parquet range_src[id#1] ReadSchema: struct<id:bigint>",
            [e2e._FakeField("id", "bigint")],
            "(1) PhotonScan parquet range_src\nOutput [1]: [id#1]\nReadSchema: struct<id:bigint>\n",
        )

    def sql(self, query: str) -> _ExtendedFakeDFR2:
        base = super().sql(query)
        return _ExtendedFakeDFR2(base._analyzed, base._physical, base.schema.fields, base._formatted, base._rows_data)


@contextlib.contextmanager
def _extended_spark_env_r2(catalog_tables: dict[str, list[Any]]):
    fake_spark = _ExtendedFakeSparkSessionR2(catalog_tables)
    old_get_spark = cp.get_spark
    old_dfs = list(cp.DF_CLASSES)
    old_writers = list(cp.WRITER_CLASSES)
    old_stream_writers = list(cp.STREAM_WRITER_CLASSES)
    old_sessions = list(cp.SESSION_CLASSES)
    old_readers = list(cp.READER_CLASSES)
    cp_mod: Any = cp
    cp_mod.get_spark = lambda: fake_spark
    cp.DF_CLASSES[:] = [_ExtendedFakeDFR2]
    cp.WRITER_CLASSES[:] = [_ExtendedFakeWriterR2]
    cp.STREAM_WRITER_CLASSES[:] = [_FakeStreamWriter]
    cp.SESSION_CLASSES[:] = [_ExtendedFakeSparkSessionR2]
    cp.READER_CLASSES[:] = [e2e._FakeReader]
    try:
        yield fake_spark
    finally:
        cp_mod.get_spark = old_get_spark
        cp.DF_CLASSES[:] = old_dfs
        cp.WRITER_CLASSES[:] = old_writers
        cp.STREAM_WRITER_CLASSES[:] = old_stream_writers
        cp.SESSION_CLASSES[:] = old_sessions
        cp.READER_CLASSES[:] = old_readers


# --------------------------------------------------------------------------- #
# R2-1: Aspas duplas no Spark SQL e _top_level_sql
# --------------------------------------------------------------------------- #


def test_r2_sql_double_quoted_string_does_not_leak_fake_sources() -> None:
    query = 'SELECT id, "texto FROM tabela_falsa JOIN outra_falsa" AS msg FROM dev.cadastros.tb_banco'
    assert cp.extract_sql_sources(query) == ["dev.cadastros.tb_banco"]


def test_r2_sql_double_quoted_paren_does_not_bypass_from_insert() -> None:
    query = 'FROM dev.cadastros.tb_banco WHERE nm_banco = "(teste" INSERT INTO dev.tgt.tb_saida SELECT cbco'
    mode, kind = cp.classify_sql(query)
    assert mode == "explain"
    assert kind == "INSERT"
    assert cp.extract_sql_target(query) == "dev.tgt.tb_saida"


# --------------------------------------------------------------------------- #
# R2-2: Streaming (DataStreamWriter._write_proto.options no Spark Connect)
# --------------------------------------------------------------------------- #


def test_r2_write_stream_option_path_captures_destination() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "stream_job.py").write_text(
            "df = spark.range(5)\ndf.writeStream.format('delta').option('path', '/mnt/stream/saida').start()\n",
            encoding="utf-8",
        )
        with _extended_spark_env_r2(e2e._CATALOG):
            res = cp.capture_plans(str(root), "stream_job.py", fetch_catalog=False)

    assert res["status"] == "ok"
    assert any(d["table"] == "/mnt/stream/saida" for d in res["destinations"])


# --------------------------------------------------------------------------- #
# R2-3: write.jdbc / read.jdbc capturando URL JDBC em vez do nome da tabela
# --------------------------------------------------------------------------- #


def test_r2_write_jdbc_captures_table_instead_of_url() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "jdbc_job.py").write_text(
            "df = spark.range(5)\ndf.write.jdbc('jdbc:postgresql://host:5432/db', 'public.tb_vendas')\n",
            encoding="utf-8",
        )
        with _extended_spark_env_r2(e2e._CATALOG):
            res = cp.capture_plans(str(root), "jdbc_job.py", fetch_catalog=False)

    assert res["status"] == "ok"
    assert [d["table"] for d in res["destinations"]] == ["public.tb_vendas"]


# --------------------------------------------------------------------------- #
# R2-4: Multi-table insert (FROM ... INSERT INTO tgt1 ... INSERT INTO tgt2 ...)
# --------------------------------------------------------------------------- #


def test_r2_sql_multi_table_insert_captures_all_destinations() -> None:
    with _extended_spark_env_r2(e2e._CATALOG):
        res = cp.capture_plans(str(_REPRO_DIR), "pipeline_bugs_r2.sql", fetch_catalog=False)

    assert res["status"] == "ok"
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert {"dev.tgt.tb_bypass", "dev.tgt.tb_ativos", "dev.tgt.tb_inativos"} <= dest_tbls


# --------------------------------------------------------------------------- #
# R2-5: Crases internas em saveAsTable + sources._match_table
# --------------------------------------------------------------------------- #


def test_r2_backticked_save_as_table_matches_in_sources_query() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {"analyzed": "Relation dev.cadastros.tb_banco[cbco#1] parquet", "physical": ""},
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "`dev`.`tgt`.`tb_crase`",
                        "call_site": {"file": "job.py", "line": 3},
                    }
                ],
            }
        ],
    }
    doc = build_sources(plan_doc)
    matched = extract_tag(plan_doc, doc, tag="destinations", table="dev.tgt.tb_crase")
    assert len(matched) == 1


# --------------------------------------------------------------------------- #
# R2-6: Vazamento de parâmetros $param entre múltiplos %run sequenciais
# --------------------------------------------------------------------------- #


def test_r2_magic_run_does_not_leak_params_between_notebooks() -> None:
    with _extended_spark_env_r2(e2e._CATALOG):
        res = cp.capture_plans(
            str(_REPRO_DIR / "run_leak"),
            "main.py",
            explore_branches=False,
            fetch_catalog=False,
        )

    assert res["status"] == "ok"
    assert {d["table"] for d in res["destinations"]} == {"dev.tgt.tb_carga", "dev.tgt.tb_validacao"}


# --------------------------------------------------------------------------- #
# R2-7: Notebooks .ipynb (%sh multilinha, !pip, %%time)
# --------------------------------------------------------------------------- #


def test_r2_ipynb_multiline_percent_sh_does_not_crash() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        nb = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {},
            "cells": [
                {"cell_type": "code", "metadata": {}, "source": ["%sh\n", "echo hello\n", "ls -la\n"]},
                {
                    "cell_type": "code",
                    "metadata": {},
                    "source": ["df = spark.range(5)\n", 'df.write.saveAsTable("dev.tgt.tb_after_sh")\n'],
                },
            ],
        }
        (root / "sh_nb.ipynb").write_text(json.dumps(nb), encoding="utf-8")
        with _extended_spark_env_r2(e2e._CATALOG):
            res = cp.capture_plans(str(root), "sh_nb.ipynb", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(d["table"] == "dev.tgt.tb_after_sh" for d in res["destinations"])


def test_r2_ipynb_bang_shell_command_does_not_crash() -> None:
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
                    "source": [
                        "!pip install --quiet dummy-pkg\n",
                        "df = spark.range(5)\n",
                        'df.write.saveAsTable("dev.tgt.tb_after_bang")\n',
                    ],
                }
            ],
        }
        (root / "bang_nb.ipynb").write_text(json.dumps(nb), encoding="utf-8")
        with _extended_spark_env_r2(e2e._CATALOG):
            res = cp.capture_plans(str(root), "bang_nb.ipynb", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(d["table"] == "dev.tgt.tb_after_bang" for d in res["destinations"])


def test_r2_ipynb_cell_magic_time_preserves_python_body() -> None:
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
                    "source": ["%%time\n", "df = spark.range(5)\n", 'df.write.saveAsTable("dev.tgt.tb_timed")\n'],
                }
            ],
        }
        (root / "time_nb.ipynb").write_text(json.dumps(nb), encoding="utf-8")
        with _extended_spark_env_r2(e2e._CATALOG):
            res = cp.capture_plans(str(root), "time_nb.ipynb", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(d["table"] == "dev.tgt.tb_timed" for d in res["destinations"])


def test_r2_repro_file_notebook_bugs_r2_ipynb() -> None:
    with _extended_spark_env_r2(e2e._CATALOG):
        res = cp.capture_plans(str(_REPRO_DIR), "notebook_bugs_r2.ipynb", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert {"dev.tgt.tb_timed", "dev.tgt.tb_pos_bang"} <= {d["table"] for d in res["destinations"]}


# --------------------------------------------------------------------------- #
# R2-8: BranchExplorer (loop infinito em while + .path em _SKIP_ATTRS)
# --------------------------------------------------------------------------- #


def test_r2_branch_explorer_does_not_infinite_loop_on_while_break() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "loop.py").write_text(
            "iters = 0\n"
            "while True:\n"
            "    iters += 1\n"
            "    if iters > 50:\n"
            "        raise RuntimeError('infinite loop detected')\n"
            "    df = spark.range(5)\n"
            "    if df.count() > 0:\n"
            "        df.write.saveAsTable('dev.tgt.tb_loop_ok')\n"
            "        break\n",
            encoding="utf-8",
        )
        with _extended_spark_env_r2(e2e._CATALOG):
            res = cp.capture_plans(str(root), "loop.py", fetch_catalog=False)

    assert res["status"] == "ok"
    assert not any("infinite loop detected" in w for w in res["warnings"])


def test_r2_branch_explorer_instruments_user_object_path_attribute() -> None:
    expr = ast.parse('cfg.path.endswith(".parquet")', mode="eval").body
    assert cp._should_instrument_if(expr) is True


def test_r2_repro_file_job_bugs_r2_py() -> None:
    with _extended_spark_env_r2(e2e._CATALOG):
        res = cp.capture_plans(str(_REPRO_DIR), "job_bugs_r2.py", fetch_catalog=False)

    assert res["status"] == "ok"
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert "public.tb_vendas" in dest_tbls
    assert {"dev.tgt.tb_parquet", "dev.tgt.tb_csv", "dev.tgt.tb_loop_ok"} <= dest_tbls
    assert len(extract_tag(res, build_sources(res), tag="destinations", table="dev.tgt.tb_crase")) == 1
    assert not any("loop infinito" in w for w in res["warnings"])


# --------------------------------------------------------------------------- #
# R2-9, R2-10, R2-11: plan_parser.py (RelationV2, MERGE sem alias, formatted)
# --------------------------------------------------------------------------- #


def test_r2_plan_parser_extracts_relation_v2() -> None:
    rels = parse_relations("RelationV2[id#1, nome#2] dev.cadastros.tb_banco")
    assert len(rels) == 1
    assert rels[0]["table"] == "dev.cadastros.tb_banco"
    assert [c["name"] for c in rels[0]["columns"]] == ["id", "nome"]


def test_r2_plan_parser_merge_targets_with_qualified_subquery_alias() -> None:
    merge_line = (
        "+- MergeIntoCommandEdge SubqueryAlias dev.src.orig, SubqueryAlias dev.tgt.alvo, "
        "`dev`.`tgt`.`alvo`, Delta[version=3], (t.id = s.id), "
        "[Update [actions: [`id` = id#5, `valor` = valor#6]]]"
    )
    assert parse_merge_targets(merge_line) == [{"table": "dev.tgt.alvo", "columns": ["id", "valor"]}]


def test_r2_plan_parser_formatted_scan_with_codegen_id() -> None:
    formatted = (
        "(1) Scan parquet dev.sch.pix [codegen id : 1]\n"
        "Output [2]: [id#1, vl#2]\n"
        "ReadSchema: struct<id:int,vl:double>\n"
    )
    scans = parse_formatted_scans(formatted)
    assert len(scans) == 1
    assert scans[0]["table"] == "dev.sch.pix"


def test_r2_plan_parser_formatted_batch_scan_extracts_table() -> None:
    formatted = "(1) BatchScan dev.sch.pix\nOutput [2]: [id#1, vl#2]\nReadSchema: struct<id:int,vl:double>\n"
    scans = parse_formatted_scans(formatted)
    assert len(scans) == 1
    assert scans[0]["table"] == "dev.sch.pix"


# --------------------------------------------------------------------------- #
# R2-12, R2-13: impact.py (acentos/espaços, Generate/explode, DELETE sem schema)
# --------------------------------------------------------------------------- #


def test_r2_impact_handles_accented_columns_and_spaced_aliases() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "Project [upper(descrição#1) AS `descrição limpa`#2]\n"
                        "+- Filter (descrição#1 != '')\n"
                        "   +- Relation dev.cadastros.tb_banco[descrição#1] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_pt",
                        "call_site": {"file": "job.py", "line": 4},
                    }
                ],
            }
        ],
    }
    rep = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="descrição")
    p = rep["hops"][0]["plans"][0]
    assert p["filters"][0]["columns_used"] == ["descrição"]
    assert [c["name"] for c in p["output_columns_affected"]] == ["descrição limpa"]


def test_r2_impact_tracks_column_through_generate_explode() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "Project [id#1, tag#5]\n"
                        "+- Generate explode(tags#2), false, [tag#5]\n"
                        "   +- Relation dev.cadastros.tb_pedidos[id#1,tags#2] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_tags",
                        "call_site": {"file": "job.py", "line": 5},
                    }
                ],
            }
        ],
    }
    rep = trace_column_impact(plan_doc, table="dev.cadastros.tb_pedidos", column="tags")
    out_cols = [c["name"] for c in rep["hops"][0]["plans"][0]["output_columns_affected"]]
    assert "tag" in out_cols


def test_r2_impact_ignores_unqualified_in_place_delete() -> None:
    plan_doc = {
        "entrypoint": "job.sql",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "DeleteFromTable (cbco#1 = 341)\n"
                        "+- Relation dev.cadastros.tb_banco[cbco#1,nm_banco#2] parquet\n"
                    ),
                    "physical": "",
                },
                "occurrences": [
                    {
                        "action": "sql:DELETE",
                        "target": "tb_banco",
                        "call_site": {"file": "job.sql", "line": 2},
                    }
                ],
            }
        ],
    }
    rep = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="cbco")
    assert rep["summary"]["found"] is False
    assert rep["destinations_affected"] == []

if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"OK  {name}")
