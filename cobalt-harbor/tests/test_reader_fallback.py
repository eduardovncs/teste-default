"""Testes (TDD) para fallback de leitura de arquivos (CSV/Parquet/JSON/Load) e re-leitura de staging em path."""

from __future__ import annotations

import contextlib
import importlib.util
import re
import tempfile
from pathlib import Path
from typing import Any

import spark_lineage.capture_plans as cp
from spark_lineage.impact import trace_column_impact

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_e2e_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "_test_capture_e2e_helpers_rf",
        _REPO_ROOT / "tests" / "test_capture_e2e.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


e2e: Any = _load_e2e_module()


class _StrictFileWriter(e2e._FakeWriter):
    def csv(self, path: str, **_kw: Any) -> None:
        raise AssertionError(f"write.csv({path}) não deveria executar de verdade!")

    def parquet(self, path: str, **_kw: Any) -> None:
        raise AssertionError(f"write.parquet({path}) não deveria executar de verdade!")

    def json(self, path: str, **_kw: Any) -> None:
        raise AssertionError(f"write.json({path}) não deveria executar de verdade!")


class _StrictFileDF(e2e._FakeDF):
    def __init__(
        self,
        analyzed: str,
        physical: str,
        fields: list[Any],
        formatted: str = "",
        rows_data: list[tuple[Any, ...]] | None = None,
        session: _StrictFileSparkSession | None = None,
    ) -> None:
        super().__init__(analyzed, physical, fields, formatted, rows_data)
        self._session = session

    @property
    def write(self) -> _StrictFileWriter:
        return _StrictFileWriter(self)

    def _col_names(self) -> set[str]:
        return {f.name.lower() for f in self.schema.fields}

    def __getitem__(self, col_name: str) -> str:
        if col_name.lower() not in self._col_names():
            raise RuntimeError(f"[UNRESOLVED_COLUMN] A column with name `{col_name}` cannot be resolved.")
        return col_name

    def select(self, *cols: str) -> _StrictFileDF:
        flat: list[str] = []
        for c in cols:
            if isinstance(c, (list, tuple)):
                flat.extend(str(x) for x in c)
            else:
                flat.append(str(c))
        existing = {f.name.lower(): f for f in self.schema.fields}
        out_fields: list[Any] = []
        proj_items: list[str] = []
        for idx, c in enumerate(flat, start=1):
            if c == "*":
                out_fields.extend(self.schema.fields)
                continue
            if c.lower() not in existing:
                raise RuntimeError(f"[UNRESOLVED_COLUMN] A column with name `{c}` cannot be resolved.")
            f = existing[c.lower()]
            out_fields.append(f)
            proj_items.append(f"{f.name}#{idx}")
        indented = "\n".join(f"   {ln}" for ln in self._analyzed.splitlines())
        new_analyzed = f"Project [{', '.join(proj_items)}]\n+- {indented.lstrip()}"
        return _StrictFileDF(new_analyzed, self._physical, out_fields, self._formatted, session=self._session)

    def join(self, other: Any, on: Any = None, how: str = "inner") -> _StrictFileDF:
        if not isinstance(other, _StrictFileDF):
            raise TypeError(f"other must be a Spark DataFrame, got {type(other).__name__}")
        if isinstance(on, str):
            keys = [] if ("(" in on or "=" in on) else [on]
        elif isinstance(on, (list, tuple)):
            keys = [str(k) for k in on]
        else:
            keys = []
        for k in keys:
            if k.lower() not in self._col_names():
                raise RuntimeError(f"[UNRESOLVED_COLUMN] Left DF missing join key `{k}`")
            if k.lower() not in other._col_names():
                raise RuntimeError(f"[UNRESOLVED_COLUMN] Right DF missing join key `{k}`")
        cond = on if (isinstance(on, str) and not keys) else (" AND ".join(f"({k}#1 = {k}#101)" for k in keys) or "(true)")
        left_ind = "\n".join(f":  {ln}" for ln in self._analyzed.splitlines())
        right_ind = "\n".join(f"   {ln}" for ln in other._analyzed.splitlines())
        new_analyzed = f"Join {how.capitalize()}, {cond}\n:- {left_ind[3:]}\n+- {right_ind[3:]}"
        new_phys = (self._physical + "\n" + other._physical).strip()
        new_fmt = (self._formatted + "\n\n" + other._formatted).strip()
        seen = {f.name.lower() for f in self.schema.fields}
        merged_fields = list(self.schema.fields) + [f for f in other.schema.fields if f.name.lower() not in seen]
        return _StrictFileDF(new_analyzed, new_phys, merged_fields, new_fmt, session=self._session)

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802
        if self._session is not None:
            self._session._temp_views[name] = self


class _StrictFileReader:
    def __init__(self, session: _StrictFileSparkSession) -> None:
        self._session = session
        self._options: dict[str, Any] = {}
        self._format: str | None = None
        self._schema: Any = None

    def format(self, fmt: str) -> _StrictFileReader:
        self._format = fmt
        return self

    def option(self, k: str, v: Any) -> _StrictFileReader:
        self._options[k] = v
        return self

    def options(self, **kw: Any) -> _StrictFileReader:
        self._options.update(kw)
        return self

    def schema(self, schema: Any) -> _StrictFileReader:
        self._schema = schema
        return self

    def table(self, name: str) -> _StrictFileDF:
        return self._session.table(name)

    def _fail_missing(self, path: Any, fmt: str) -> _StrictFileDF:
        raise RuntimeError(f"[PATH_NOT_FOUND] Path does not exist ({fmt}): {path}")

    def csv(self, path: Any, schema: Any = None, **_kw: Any) -> _StrictFileDF:
        _ = schema
        return self._fail_missing(path, "csv")

    def parquet(self, path: Any, **_kw: Any) -> _StrictFileDF:
        return self._fail_missing(path, "parquet")

    def json(self, path: Any, schema: Any = None, **_kw: Any) -> _StrictFileDF:
        _ = schema
        return self._fail_missing(path, "json")

    def load(self, path: Any = None, format: str | None = None, schema: Any = None, **_kw: Any) -> _StrictFileDF:  # noqa: A002
        _ = schema
        target = path or self._options.get("path")
        return self._fail_missing(target, format or self._format or "load")


class _StrictFileSparkSession(e2e._FakeSparkSession):
    def _wrap(self, df: Any) -> _StrictFileDF:
        return _StrictFileDF(
            df._analyzed, df._physical, df.schema.fields, df._formatted, df._rows_data, session=self
        )

    @property
    def read(self) -> _StrictFileReader:
        return _StrictFileReader(self)

    def _make_table_df(self, name: str, base_id: int = 1) -> _StrictFileDF:
        return self._wrap(super()._make_table_df(name, base_id=base_id))

    def createDataFrame(self, _data: Any, schema: Any = None) -> _StrictFileDF:  # noqa: N802
        fields: list[Any] = []
        if isinstance(schema, str):
            for part in schema.split(","):
                toks = part.strip().split()
                if len(toks) >= 2:
                    fields.append(e2e._FakeField(toks[0].strip("`"), toks[1].lower()))
        elif hasattr(schema, "fields"):
            fields = [e2e._FakeField(f.name, f.dataType.simpleString()) for f in schema.fields]
        elif isinstance(schema, (list, tuple)):
            fields = [e2e._FakeField(str(c), "string") for c in schema]
        cols_attr = ",".join(f"{f.name}#{i + 1}" for i, f in enumerate(fields))
        return _StrictFileDF(f"LocalRelation <empty>, [{cols_attr}]", "LocalTableScan <empty>", fields, session=self)

    def sql(self, query: str) -> _StrictFileDF:
        q = query.strip()
        # Suporta SELECT sintético `SELECT CAST(NULL AS TYPE) AS `col`, ... WHERE 1=0`
        if re.search(r"\bWHERE\s+1\s*=\s*0\b", q, re.I) and re.match(r"^SELECT\b", q, re.I):
            self.sql_log.append(query)
            matches = re.findall(r"CAST\s*\(\s*NULL\s+AS\s+([A-Za-z0-9_(),<>]+)\s*\)\s+AS\s+`?([^`,\s]+)`?", q, re.I)
            if matches:
                fields = [e2e._FakeField(name, typ.lower()) for typ, name in matches]
                cols_attr = ",".join(f"{f.name}#{i + 1}" for i, f in enumerate(fields))
                return _StrictFileDF(
                    f"LocalRelation <empty>, [{cols_attr}]", "LocalTableScan <empty>", fields, session=self
                )
        return self._wrap(super().sql(query))


@contextlib.contextmanager
def _strict_file_spark_env(catalog_tables: dict[str, list[Any]]):
    fake_spark = _StrictFileSparkSession(catalog_tables)
    old_get_spark = cp.get_spark
    old_dfs = list(cp.DF_CLASSES)
    old_writers = list(cp.WRITER_CLASSES)
    old_sessions = list(cp.SESSION_CLASSES)
    old_readers = list(cp.READER_CLASSES)
    cp_mod: Any = cp
    cp_mod.get_spark = lambda: fake_spark
    cp.DF_CLASSES[:] = [_StrictFileDF]
    cp.WRITER_CLASSES[:] = [_StrictFileWriter]
    cp.SESSION_CLASSES[:] = [_StrictFileSparkSession]
    cp.READER_CLASSES[:] = [_StrictFileReader]
    try:
        yield fake_spark
    finally:
        cp_mod.get_spark = old_get_spark
        cp.DF_CLASSES[:] = old_dfs
        cp.WRITER_CLASSES[:] = old_writers
        cp.SESSION_CLASSES[:] = old_sessions
        cp.READER_CLASSES[:] = old_readers


def test_csv_written_then_read_back_to_volume_uses_captured_df() -> None:
    """Caso real: job transforma tabela COMPE, grava CSV temporário, relê esse CSV e salva em um Volume."""
    src = (
        "df = spark.table('dev.cadastros.tb_banco')\n"
        "df_sel = df.select('cbco', 'nm_banco')\n"
        "df_sel.write.mode('overwrite').csv('/mnt/tmp/staging_bancos.csv')\n"
        "df_lido = spark.read.option('header', True).csv('/mnt/tmp/staging_bancos.csv')\n"
        "df_lido.select('cbco', 'nm_banco').write.mode('overwrite').csv('/Volumes/dev/compe/export/bancos.csv')\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_csv_volume.py").write_text(src, encoding="utf-8")
        with _strict_file_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_csv_volume.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(s["table"] == "dev.cadastros.tb_banco" for s in res["sources"])
    dests = {d["table"] for d in res["destinations"]}
    assert {"/mnt/tmp/staging_bancos.csv", "/Volumes/dev/compe/export/bancos.csv"} <= dests


def test_missing_csv_with_explicit_reader_schema_creates_empty_df() -> None:
    """CSV inexistente com `.schema(...)` no reader usa o schema explícito e registra fallback."""
    src = (
        "df_csv = spark.read.schema('cod_ext STRING, valor DOUBLE').option('header', True).csv('/mnt/landing/a.csv')\n"
        "df_csv.select('cod_ext', 'valor').write.saveAsTable('dev.tgt.tb_from_schema')\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_schema.py").write_text(src, encoding="utf-8")
        with _strict_file_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_schema.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(d["table"] == "dev.tgt.tb_from_schema" for d in res["destinations"])
    assert any(e["kind"] == "reader_fallback" for e in res["events"])
    assert any("/mnt/landing/a.csv" in w for w in res["warnings"])


def test_missing_csv_without_schema_infers_columns_from_ast() -> None:
    """CSV inexistente sem `.schema(...)` infere colunas usadas na AST e não derruba a captura das tabelas COMPE."""
    src = (
        "df_csv = spark.read.option('header', True).csv('/mnt/landing/sem_schema.csv')\n"
        "_ = df_csv['status_csv']\n"
        "df_csv_sel = df_csv.select('cod_parceiro', 'dt_mov')\n"
        "df_csv_sel.write.save('/Volumes/dev/export/parceiros')\n"
        "\n"
        "df_compe = spark.table('dev.cadastros.tb_banco')\n"
        "df_compe.select('cbco', 'nm_banco').write.saveAsTable('dev.tgt.tb_compe_ok')\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_ast_csv.py").write_text(src, encoding="utf-8")
        with _strict_file_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_ast_csv.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(s["table"] == "dev.cadastros.tb_banco" for s in res["sources"])
    dests = {d["table"] for d in res["destinations"]}
    assert {"/Volumes/dev/export/parceiros", "dev.tgt.tb_compe_ok"} <= dests
    assert any(e["kind"] == "reader_fallback" for e in res["events"])


def test_missing_csv_joined_with_compe_table_preserves_compe_lineage() -> None:
    """CSV inexistente em JOIN com tabela COMPE gera DataFrame Spark real e preserva linhagem/impacto COMPE."""
    src = (
        "df_compe = spark.table('dev.cadastros.tb_banco')\n"
        "df_csv = spark.read.option('header', True).csv('/mnt/landing/depara_bco.csv')\n"
        "df_out = df_compe.join(df_csv, on='cbco', how='left')\n"
        "df_out.write.saveAsTable('dev.tgt.tb_compe_enriquecida')\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_join_csv.py").write_text(src, encoding="utf-8")
        with _strict_file_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_join_csv.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(s["table"] == "dev.cadastros.tb_banco" for s in res["sources"])
    assert any(d["table"] == "dev.tgt.tb_compe_enriquecida" for d in res["destinations"])
    imp = trace_column_impact(res, table="dev.cadastros.tb_banco", column="cbco")
    assert imp["summary"]["found"] is True
    assert any(d["table"] == "dev.tgt.tb_compe_enriquecida" for d in imp["destinations_affected"])


def test_missing_csv_temp_view_used_in_sql_with_compe_table() -> None:
    """CSV inexistente registrado como temp view funciona em SQL junto com tabela COMPE."""
    src = (
        "df_csv = spark.read.option('header', True).csv('/mnt/landing/aux.csv')\n"
        "df_csv.createOrReplaceTempView('vw_aux_csv')\n"
        "spark.sql(\n"
        "    'INSERT INTO dev.tgt.tb_sql_out '\n"
        "    'SELECT b.cbco FROM dev.cadastros.tb_banco b JOIN vw_aux_csv a ON b.cbco = a.cbco'\n"
        ")\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_sql_csv.py").write_text(src, encoding="utf-8")
        with _strict_file_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_sql_csv.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(s["table"] == "dev.cadastros.tb_banco" for s in res["sources"])
    assert any(d["table"] == "dev.tgt.tb_sql_out" for d in res["destinations"])
    assert any(e["kind"] == "reader_fallback" for e in res["events"])


def test_missing_csv_without_referenced_columns_still_recovers() -> None:
    """CSV inexistente sem colunas mencionadas na AST ainda gera DF sintético mínimo sem quebrar."""
    src = (
        "df_csv = spark.read.csv('/mnt/landing/opaco.csv')\n"
        "df_csv.write.save('/Volumes/dev/compe/out/opaco')\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_opaque.py").write_text(src, encoding="utf-8")
        with _strict_file_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_opaque.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(d["table"] == "/Volumes/dev/compe/out/opaco" for d in res["destinations"])
    assert any(e["kind"] == "reader_fallback" for e in res["events"])


def test_missing_parquet_json_and_format_load_also_use_fallback() -> None:
    """Proteção cobre também `.parquet()`, `.json()` e `.format(...).load()` sem derrubar tabelas COMPE."""
    src = (
        "df_p = spark.read.parquet('/mnt/landing/missing.parquet')\n"
        "df_j = spark.read.json('/mnt/landing/missing.json')\n"
        "df_l = spark.read.format('csv').option('header', True).load('/mnt/landing/missing_load.csv')\n"
        "df_p.select('col_p').write.save('/Volumes/dev/out/p')\n"
        "df_j.select('col_j').write.save('/Volumes/dev/out/j')\n"
        "df_l.select('col_l').write.save('/Volumes/dev/out/l')\n"
        "spark.table('dev.cadastros.tb_banco').write.saveAsTable('dev.tgt.tb_compe_final')\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_multi_fmt.py").write_text(src, encoding="utf-8")
        with _strict_file_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_multi_fmt.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert any(s["table"] == "dev.cadastros.tb_banco" for s in res["sources"])
    assert any(d["table"] == "dev.tgt.tb_compe_final" for d in res["destinations"])
    fallback_events = [e for e in res["events"] if e["kind"] == "reader_fallback"]
    assert len(fallback_events) == 3


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"OK  {name}")
