"""Testes de reprodução de bugs conhecidos — Rodada 3 (cache/persist, tableExists, runtime e streaming).

Todos os testes abaixo especificam o comportamento ESPERADO e estão marcados com
``@pytest.mark.xfail(strict=True)`` enquanto os bugs da Rodada 3 permanecem abertos:
- ``pytest -v`` mostra cada bug reproduzido como ``XFAIL`` sem quebrar a suíte.
- ``pytest tests/test_known_bugs_r3.py --runxfail`` exibe as falhas reais detalhadas.
- Ao corrigir um bug, o teste passará a reportar ``XPASS(strict)``, indicando que o
  marcador pode ser removido para virar teste de regressão permanente.
"""

from __future__ import annotations

import ast
import contextlib
import importlib.util
import re
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
        "_test_capture_e2e_helpers_r3",
        _REPO_ROOT / "tests" / "test_capture_e2e.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


e2e: Any = _load_e2e_module()


class _ServerlessCatalog:
    """Simula o Catalog do Databricks Serverless / Connect 16.4."""

    def __init__(self, session: _ServerlessSparkSession) -> None:
        self._session = session
        self.table_exists_calls: list[tuple[str, str | None]] = []

    def cacheTable(self, table_name: str, storage_level: Any = None) -> None:  # noqa: N802
        _ = table_name, storage_level
        raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] CACHE TABLE is not supported on serverless compute")

    def uncacheTable(self, table_name: str) -> None:  # noqa: N802
        _ = table_name
        raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] UNCACHE TABLE is not supported on serverless compute")

    def clearCache(self) -> None:  # noqa: N802
        raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] CLEAR CACHE is not supported on serverless compute")

    def isCached(self, table_name: str) -> bool:  # noqa: N802
        _ = table_name
        raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] isCached is not supported on serverless compute")

    def tableExists(self, table_name: str, db_name: str | None = None) -> bool:  # noqa: N802
        if self._session.stopped:
            raise RuntimeError("SparkSession is stopped")
        self.table_exists_calls.append((table_name, db_name))
        if db_name is not None:
            raise RuntimeError("[DEPRECATED_FEATURE_BLOCKED] dbName argument is deprecated")
        if not table_name or table_name.startswith(".") or ".." in table_name:
            raise RuntimeError(f"[PARSE_SYNTAX_ERROR] Invalid table name: {table_name!r}")
        norm = table_name.replace("`", "").strip()
        return norm in self._session._catalog or norm in self._session._temp_views


class _ForeachBatchStreamWriter:
    def __init__(self, df: Any) -> None:
        self._df = df
        self._foreach_batch_fn: Any = None

    def format(self, _fmt: str) -> _ForeachBatchStreamWriter:
        return self

    def foreachBatch(self, func: Any) -> _ForeachBatchStreamWriter:  # noqa: N802
        self._foreach_batch_fn = func
        return self

    def start(self, path: str | None = None) -> Any:
        raise AssertionError(f"writeStream.start({path}) não deveria executar de verdade!")


class _ServerlessDF(e2e._FakeDF):
    def __init__(
        self,
        analyzed: str,
        physical: str,
        fields: list[Any],
        formatted: str = "",
        rows_data: list[tuple[Any, ...]] | None = None,
        session: _ServerlessSparkSession | None = None,
    ) -> None:
        super().__init__(analyzed, physical, fields, formatted, rows_data)
        self._session = session

    @property
    def writeStream(self) -> _ForeachBatchStreamWriter:  # noqa: N802
        return _ForeachBatchStreamWriter(self)

    def cache(self) -> _ServerlessDF:
        raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] DataFrame.cache() is not supported on serverless compute")

    def persist(self, storage_level: Any = None) -> _ServerlessDF:
        _ = storage_level
        raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] DataFrame.persist() is not supported on serverless compute")

    def unpersist(self, blocking: bool = False) -> _ServerlessDF:
        _ = blocking
        raise RuntimeError(
            "[NOT_SUPPORTED_WITH_SERVERLESS] DataFrame.unpersist() is not supported on serverless compute"
        )

    @property
    def storageLevel(self) -> Any:  # noqa: N802
        raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] storageLevel is not supported on serverless compute")

    @property
    def is_cached(self) -> bool:
        return bool(self.storageLevel)

    def createTempView(self, name: str) -> None:  # noqa: N802
        assert self._session is not None
        if name in self._session._temp_views:
            raise RuntimeError(f"[TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] Temporary view `{name}` already exists")
        self._session._temp_views[name] = self

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802
        assert self._session is not None
        self._session._temp_views[name] = self


class _ServerlessSparkSession(e2e._FakeSparkSession):
    def __init__(self, catalog_tables: dict[str, list[Any]], *, strict_tables: bool = False) -> None:
        super().__init__(catalog_tables)
        self.catalog = _ServerlessCatalog(self)
        self.stopped = False
        self._strict_tables = strict_tables

    def _wrap(self, df: Any) -> _ServerlessDF:
        return _ServerlessDF(df._analyzed, df._physical, df.schema.fields, df._formatted, df._rows_data, session=self)

    def _make_table_df(self, name: str, base_id: int = 1) -> _ServerlessDF:
        if self.stopped:
            raise RuntimeError("SparkSession is stopped")
        norm = name.replace("`", "").strip()
        if self._strict_tables and norm not in self._catalog and norm not in self._temp_views:
            raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{norm}` cannot be found")
        return self._wrap(super()._make_table_df(name, base_id=base_id))

    def range(self, _n: int) -> _ServerlessDF:
        if self.stopped:
            raise RuntimeError("SparkSession is stopped")
        return _ServerlessDF(
            "Relation range_src[id#1] parquet",
            "PhotonScan parquet range_src[id#1] ReadSchema: struct<id:bigint>",
            [e2e._FakeField("id", "bigint")],
            "(1) PhotonScan parquet range_src\nOutput [1]: [id#1]\nReadSchema: struct<id:bigint>\n",
            session=self,
        )

    def sql(self, query: str) -> _ServerlessDF:
        if self.stopped:
            raise RuntimeError("SparkSession is stopped")
        q = query.strip()
        # Diferencia CREATE TEMP VIEW (falha se já existe) de CREATE OR REPLACE TEMP VIEW
        m_no_rep = re.match(r"CREATE\s+TEMP(?:ORARY)?\s+VIEW\s+([\w.]+)\s+AS\s+(.+)", q, re.I | re.S)
        if m_no_rep:
            self.sql_log.append(query)
            vname, body = m_no_rep.group(1), m_no_rep.group(2)
            if vname in self._temp_views:
                raise RuntimeError(f"[TEMP_TABLE_OR_VIEW_ALREADY_EXISTS] Temporary view `{vname}` already exists")
            self._temp_views[vname] = self._wrap(self._build_sql_plan_df(body))
            return _ServerlessDF("", "", [], session=self)
        return self._wrap(super().sql(query))

    def stop(self) -> None:
        self.stopped = True


@contextlib.contextmanager
def _serverless_spark_env(catalog_tables: dict[str, list[Any]], *, strict_tables: bool = False):
    fake_spark = _ServerlessSparkSession(catalog_tables, strict_tables=strict_tables)
    old_get_spark = cp.get_spark
    old_dfs = list(cp.DF_CLASSES)
    old_writers = list(cp.WRITER_CLASSES)
    old_stream_writers = list(cp.STREAM_WRITER_CLASSES)
    old_sessions = list(cp.SESSION_CLASSES)
    old_readers = list(cp.READER_CLASSES)
    old_catalogs = list(cp.CATALOG_CLASSES)
    cp_mod: Any = cp
    cp_mod.get_spark = lambda: fake_spark
    cp.DF_CLASSES[:] = [_ServerlessDF]
    cp.WRITER_CLASSES[:] = [e2e._FakeWriter]
    cp.STREAM_WRITER_CLASSES[:] = [_ForeachBatchStreamWriter]
    cp.SESSION_CLASSES[:] = [_ServerlessSparkSession]
    cp.READER_CLASSES[:] = [e2e._FakeReader]
    cp.CATALOG_CLASSES[:] = [_ServerlessCatalog]
    try:
        yield fake_spark
    finally:
        cp_mod.get_spark = old_get_spark
        cp.DF_CLASSES[:] = old_dfs
        cp.WRITER_CLASSES[:] = old_writers
        cp.STREAM_WRITER_CLASSES[:] = old_stream_writers
        cp.SESSION_CLASSES[:] = old_sessions
        cp.READER_CLASSES[:] = old_readers
        cp.CATALOG_CLASSES[:] = old_catalogs


# --------------------------------------------------------------------------- #
# R3-1 a R3-4: Bloco cache / persist / InMemoryRelation
# --------------------------------------------------------------------------- #


def test_r3_df_cache_persist_unpersist_intercepted_on_serverless() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "df = spark.table('dev.cadastros.tb_banco').cache()\n"
            "df = df.persist()\n"
            "df.write.saveAsTable('dev.tgt.tb_cached_out')\n"
            "df.unpersist()\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert [d["table"] for d in res["destinations"]] == ["dev.tgt.tb_cached_out"]


def test_r3_df_storage_level_and_is_cached_safe_on_serverless() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "if not df.is_cached:\n"
            "    df.write.saveAsTable('dev.tgt.tb_after_iscached')\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(d["table"] == "dev.tgt.tb_after_iscached" for d in res["destinations"])


def test_r3_catalog_cache_methods_are_intercepted() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "spark.catalog.cacheTable('dev.cadastros.tb_banco')\n"
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "df.write.saveAsTable('dev.tgt.tb_cat_cache')\n"
            "spark.catalog.uncacheTable('dev.cadastros.tb_banco')\n"
            "spark.catalog.clearCache()\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert [d["table"] for d in res["destinations"]] == ["dev.tgt.tb_cat_cache"]


def test_r3_sql_cache_table_as_select_creates_temp_view() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.sql").write_text(
            "CACHE TABLE vw_cache_bco AS\n"
            "SELECT cbco, nm_banco, dt_ref FROM dev.cadastros.tb_banco;\n"
            "-- COMMAND ----------\n"
            "INSERT INTO dev.tgt.tb_saida\n"
            "SELECT cbco, nm_banco, dt_ref FROM vw_cache_bco;\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG, strict_tables=True):
            res = cp.capture_plans(str(root), "job.sql", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(s["table"] == "dev.cadastros.tb_banco" for s in res["sources"])
    assert any(d["table"] == "dev.tgt.tb_saida" for d in res["destinations"])


def test_r3_in_memory_relation_extracts_sources_and_impact() -> None:
    analyzed_cached = (
        "Project [cbco#1, nm_banco#2]\n"
        "+- Filter (cbco#1 > 0)\n"
        "   +- InMemoryRelation [cbco#1, nm_banco#2], StorageLevel(disk, memory, deserialized, 1 replicas)\n"
        "         +- *(1) ColumnarToRow\n"
        "            +- FileScan parquet dev.cadastros.tb_banco[cbco#1,nm_banco#2] "
        "ReadSchema: struct<cbco:int,nm_banco:string>\n"
    )
    physical_cached = (
        "*(1) Project [cbco#1, nm_banco#2]\n"
        "+- *(1) Filter (cbco#1 > 0)\n"
        "   +- InMemoryTableScan [cbco#1, nm_banco#2], [isnotnull(cbco#1), (cbco#1 > 0)]\n"
        "         +- InMemoryRelation [cbco#1, nm_banco#2], StorageLevel(disk, memory, deserialized, 1 replicas)\n"
        "               +- *(1) ColumnarToRow\n"
        "                  +- FileScan parquet dev.cadastros.tb_banco[cbco#1,nm_banco#2] "
        "ReadSchema: struct<cbco:int,nm_banco:string>\n"
    )
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "plans": [
            {
                "id": "P001",
                "sections": {"analyzed": analyzed_cached, "physical": physical_cached},
                "occurrences": [
                    {
                        "action": "write.saveAsTable",
                        "target": "dev.tgt.tb_out",
                        "call_site": {"file": "job.py", "line": 10},
                    }
                ],
            }
        ],
    }
    doc = build_sources(plan_doc)
    assert any(s["table"] == "dev.cadastros.tb_banco" for s in doc["sources"])
    imp = trace_column_impact(plan_doc, table="dev.cadastros.tb_banco", column="cbco")
    assert imp["summary"]["found"] is True


# --------------------------------------------------------------------------- #
# R3-5: spark.catalog.tableExists
# --------------------------------------------------------------------------- #


def test_r3_table_exists_sees_previously_written_staging_table() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "df.write.mode('overwrite').saveAsTable('dev.stg.tb_intermediaria')\n"
            "if spark.catalog.tableExists('dev.stg.tb_intermediaria'):\n"
            "    spark.table('dev.stg.tb_intermediaria').write.saveAsTable('dev.tgt.tb_final')\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG, strict_tables=True):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert {"dev.stg.tb_intermediaria", "dev.tgt.tb_final"} <= dest_tbls


def test_r3_table_exists_two_args_normalized_for_serverless() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "if spark.catalog.tableExists('tb_banco', 'dev.cadastros'):\n"
            "    df.write.saveAsTable('dev.tgt.tb_2args_ok')\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(d["table"] == "dev.tgt.tb_2args_ok" for d in res["destinations"])


def test_r3_table_exists_malformed_name_from_empty_widget_does_not_crash() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "cat = dbutils.widgets.get('catalog')\n"
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "if spark.catalog.tableExists(f'{cat}.tgt.tb_saida'):\n"
            "    df.write.mode('append').saveAsTable('dev.tgt.tb_saida')\n"
            "else:\n"
            "    df.write.mode('overwrite').saveAsTable('dev.tgt.tb_saida')\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(d["table"] == "dev.tgt.tb_saida" for d in res["destinations"])


def test_r3_table_exists_cached_across_branch_passes() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "exists = spark.catalog.tableExists('dev.cadastros.tb_banco')\n"
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "if exists:\n"
            "    if df.count() == 0:\n"
            "        df.write.saveAsTable('dev.tgt.tb_a')\n"
            "    else:\n"
            "        df.write.saveAsTable('dev.tgt.tb_b')\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG) as fake_spark:
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok"
    # Sem cache, tableExists é chamado 1 + N passadas (3x aqui); deveria ser cacheado em 1 chamada
    assert len(fake_spark.catalog.table_exists_calls) == 1


# --------------------------------------------------------------------------- #
# R3-6 a R3-11: spark.stop, createTempView, retry loop, foreachBatch, ternário, paths
# --------------------------------------------------------------------------- #


def test_r3_spark_stop_does_not_kill_session_during_capture() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "if df.count() == 0:\n"
            "    df.write.saveAsTable('dev.tgt.tb_reproc')\n"
            "else:\n"
            "    df.write.saveAsTable('dev.tgt.tb_normal')\n"
            "spark.stop()\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=True)

    assert res["status"] == "ok"
    assert {d["table"] for d in res["destinations"]} == {"dev.tgt.tb_reproc", "dev.tgt.tb_normal"}
    assert res["catalog"].get("dev.cadastros.tb_banco") is not None


def test_r3_create_temp_view_does_not_fail_on_branch_reexecution() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job.py").write_text(
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "df.createTempView('vw_bancos')\n"
            "if df.count() == 0:\n"
            "    spark.table('vw_bancos').write.saveAsTable('dev.tgt.tb_reproc')\n"
            "else:\n"
            "    spark.table('vw_bancos').write.saveAsTable('dev.tgt.tb_normal')\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", fetch_catalog=False)

    assert res["status"] == "ok"
    assert {d["table"] for d in res["destinations"]} == {"dev.tgt.tb_reproc", "dev.tgt.tb_normal"}


def test_r3_branch_explorer_does_not_retry_unreachable_failed_branch() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        # Passada 0 descobre ramo 1 (linha 4) e ramo 2 (linha 9).
        # O 1º erro antes de alcançar o alvo não deveria consumir as 10 passadas e impedir explorar outros ramos.
        (root / "job.py").write_text(
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "df.createTempView('vw_once')\n"
            "if df.count() == 0:\n"
            "    df.write.saveAsTable('dev.tgt.tb_1')\n"
            "else:\n"
            "    df.write.saveAsTable('dev.tgt.tb_2')\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job.py", max_branch_passes=5, fetch_catalog=False)

    # Mesmo falhando, o ramo quebrado deveria ser tentado no máximo 1 vez (não 5 vezes repetidas)
    assert len(res["capture_warnings"]) <= 1


def test_r3_write_stream_foreach_batch_captures_inner_writes() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "stream_fb.py").write_text(
            "def processar_lote(batch_df, batch_id):\n"
            "    _ = batch_id\n"
            "    batch_df.write.mode('append').saveAsTable('dev.tgt.tb_stream_batch')\n"
            "df = spark.table('dev.cadastros.tb_banco')\n"
            "df.writeStream.foreachBatch(processar_lote).start()\n",
            encoding="utf-8",
        )
        with _serverless_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "stream_fb.py", fetch_catalog=False)

    assert res["status"] == "ok"
    assert any(d["table"] == "dev.tgt.tb_stream_batch" for d in res["destinations"])


def test_r3_branch_explorer_instruments_ternary_if_exp() -> None:
    tree = cp._BranchTransformer("job.py").visit(ast.parse("dst = 'dev.tgt.a' if modo == 'REPROC' else 'dev.tgt.b'"))
    assert "__spark_lineage_branch__" in ast.unparse(tree)


def test_r3_sources_keeps_distinct_unnamed_parquet_paths_separate() -> None:
    plan_doc = {
        "entrypoint": "job.py",
        "status": "ok",
        "read_sites": {
            "/mnt/dados/clientes": ["job.py:1"],
            "/mnt/dados/pedidos": ["job.py:2"],
        },
        "plans": [
            {
                "id": "P001",
                "sections": {
                    "analyzed": (
                        "Join Inner, (id#1 = id#3)\n"
                        ":- Relation [id#1,nome#2] parquet\n"
                        "+- Relation [id#3,valor#4] parquet\n"
                    ),
                    "physical": "",
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
    assert len(doc["sources"]) == 2


# --------------------------------------------------------------------------- #
# Reprodutores consolidados em repro/
# --------------------------------------------------------------------------- #


def test_r3_repro_file_job_bugs_r3_py() -> None:
    with _serverless_spark_env(e2e._CATALOG, strict_tables=True):
        res = cp.capture_plans(str(_REPRO_DIR), "job_bugs_r3.py", fetch_catalog=True)

    assert res["status"] == "ok", res["error"]
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert {
        "dev.stg.tb_staging_r3",
        "dev.tgt.tb_pos_staging",
        "dev.tgt.tb_tern_reproc",
        "dev.tgt.tb_tern_normal",
        "dev.tgt.tb_ramo_true",
        "dev.tgt.tb_ramo_false",
    } <= dest_tbls


def test_r3_repro_file_pipeline_bugs_r3_sql() -> None:
    with _serverless_spark_env(e2e._CATALOG, strict_tables=True):
        res = cp.capture_plans(str(_REPRO_DIR), "pipeline_bugs_r3.sql", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    dest_tbls = {d["table"] for d in res["destinations"]}
    assert {"dev.tgt.tb_from_cache", "dev.tgt.tb_extra_r3", "dev.tgt.tb_normal_r3"} <= dest_tbls

if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"OK  {name}")
