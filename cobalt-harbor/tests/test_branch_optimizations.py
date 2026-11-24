"""Testes das otimizações do BranchExplorer: poda de funções sink (sem retorno/Spark) e exploração paralela."""

from __future__ import annotations

import ast
import importlib.util
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import spark_lineage.capture_plans as cp

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_e2e_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "_test_capture_e2e_helpers_opt",
        _REPO_ROOT / "tests" / "test_capture_e2e.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


e2e: Any = _load_e2e_module()


def test_sink_log_function_is_not_instrumented() -> None:
    src = (
        "import json\n"
        "import logging\n"
        "from datetime import datetime\n"
        "logger = logging.getLogger('etl')\n"
        "\n"
        "def log_msg(etapa: str, nivel: str = 'INFO', detalhes: dict | None = None, verbose: bool = False) -> None:\n"
        "    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')\n"
        "    if nivel == 'ERROR':\n"
        "        logger.error(f'{ts} [ERRO] {etapa}')\n"
        "    elif nivel == 'WARN':\n"
        "        logger.warning(f'{ts} [WARN] {etapa}')\n"
        "    else:\n"
        "        if verbose:\n"
        "            print(f'{ts} [INFO] {etapa}: {json.dumps(detalhes or {})}')\n"
        "        logger.info(etapa)\n"
        "\n"
        "def log_erro(etapa: str, critico: bool = False) -> None:\n"
        "    if critico:\n"
        "        log_msg(etapa, nivel='ERROR', verbose=True)\n"
        "    else:\n"
        "        log_msg(etapa, nivel='WARN')\n"
        "\n"
        "log_msg('inicio', verbose=True)\n"
        "log_erro('aviso', critico=False)\n"
        "df = spark.table('dev.cadastros.tb_banco')\n"
        "if df.count() == 0:\n"
        "    df.write.saveAsTable('dev.tgt.tb_vazio')\n"
        "else:\n"
        "    df.write.saveAsTable('dev.tgt.tb_ok')\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_log.py").write_text(src, encoding="utf-8")
        with e2e._fake_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_log.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert {d["table"] for d in res["destinations"]} == {"dev.tgt.tb_vazio", "dev.tgt.tb_ok"}
    # Apenas o `if df.count() == 0` deve ser explorado; os 4 ifs dentro de log_msg/log_erro foram podados.
    assert len(res["branches"]) == 1, f"Esperava 1 ramo (somente o Spark), obtido: {res['branches']}"
    assert "df.count() == 0" in res["branches"][0]["condition"]


def test_non_sink_function_returning_table_name_is_still_instrumented() -> None:
    src = (
        "def resolve_destino(modo: str) -> str:\n"
        "    if modo == 'REPROC':\n"
        "        return 'dev.tgt.tb_reproc'\n"
        "    return 'dev.tgt.tb_normal'\n"
        "\n"
        "df = spark.table('dev.cadastros.tb_banco')\n"
        "dst = resolve_destino(dbutils.widgets.get('modo'))\n"
        "df.write.saveAsTable(dst)\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_ret.py").write_text(src, encoding="utf-8")
        with e2e._fake_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_ret.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert {d["table"] for d in res["destinations"]} == {"dev.tgt.tb_reproc", "dev.tgt.tb_normal"}


def test_non_sink_function_mutating_dict_or_list_is_still_instrumented() -> None:
    src = (
        "def configurar(cfg: dict, destinos: list, reproc: bool) -> None:\n"
        "    if reproc:\n"
        "        cfg['dst'] = 'dev.tgt.tb_cfg_reproc'\n"
        "    else:\n"
        "        cfg['dst'] = 'dev.tgt.tb_cfg_normal'\n"
        "    if cfg['dst'].endswith('reproc'):\n"
        "        destinos.append('dev.tgt.tb_extra_reproc')\n"
        "\n"
        "cfg = {}\n"
        "extras = []\n"
        "configurar(cfg, extras, dbutils.widgets.get('modo') == 'REPROC')\n"
        "df = spark.table('dev.cadastros.tb_banco')\n"
        "df.write.saveAsTable(cfg['dst'])\n"
        "for t in extras:\n"
        "    df.write.saveAsTable(t)\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "job_mut.py").write_text(src, encoding="utf-8")
        with e2e._fake_spark_env(e2e._CATALOG):
            res = cp.capture_plans(str(root), "job_mut.py", fetch_catalog=False)

    assert res["status"] == "ok", res["error"]
    assert {"dev.tgt.tb_cfg_reproc", "dev.tgt.tb_cfg_normal", "dev.tgt.tb_extra_reproc"} <= {
        d["table"] for d in res["destinations"]
    }


def test_non_sink_function_with_spark_or_df_is_still_instrumented() -> None:
    tree = ast.parse(
        "def gravar(df, flag):\n"
        "    if flag:\n"
        "        df.write.saveAsTable('dev.tgt.a')\n"
        "    else:\n"
        "        df.write.saveAsTable('dev.tgt.b')\n"
    )
    transformed = cp._BranchTransformer("job.py").visit(tree)
    assert "__spark_lineage_branch__" in ast.unparse(transformed)


def test_parallel_branch_exploration_runs_concurrently_and_captures_all() -> None:
    active_lock = threading.Lock()
    active_now = 0
    max_concurrent = 0

    class _SlowFakeSpark(e2e._FakeSparkSession):
        def _make_table_df(self, name: str, base_id: int = 1) -> e2e._FakeDF:
            nonlocal active_now, max_concurrent
            with active_lock:
                active_now += 1
                max_concurrent = max(max_concurrent, active_now)
            try:
                time.sleep(0.12)
                return super()._make_table_df(name, base_id=base_id)
            finally:
                with active_lock:
                    active_now -= 1

    src = (
        "df = spark.table('dev.cadastros.tb_banco')\n"
        "f1 = dbutils.widgets.get('f1') == '1'\n"
        "f2 = dbutils.widgets.get('f2') == '1'\n"
        "f3 = dbutils.widgets.get('f3') == '1'\n"
        "f4 = dbutils.widgets.get('f4') == '1'\n"
        "if f1:\n"
        "    df.write.saveAsTable('dev.tgt.tb_1_t')\n"
        "else:\n"
        "    df.write.saveAsTable('dev.tgt.tb_1_f')\n"
        "if f2:\n"
        "    df.write.saveAsTable('dev.tgt.tb_2_t')\n"
        "else:\n"
        "    df.write.saveAsTable('dev.tgt.tb_2_f')\n"
        "if f3:\n"
        "    df.write.saveAsTable('dev.tgt.tb_3_t')\n"
        "else:\n"
        "    df.write.saveAsTable('dev.tgt.tb_3_f')\n"
        "if f4:\n"
        "    df.write.saveAsTable('dev.tgt.tb_4_t')\n"
        "else:\n"
        "    df.write.saveAsTable('dev.tgt.tb_4_f')\n"
    )
    fake_spark = _SlowFakeSpark(e2e._CATALOG)
    cp_mod: Any = cp
    old_get = cp_mod.get_spark
    old_dfs = list(cp.DF_CLASSES)
    old_writers = list(cp.WRITER_CLASSES)
    old_sessions = list(cp.SESSION_CLASSES)
    old_readers = list(cp.READER_CLASSES)
    cp_mod.get_spark = lambda: fake_spark
    cp.DF_CLASSES[:] = [e2e._FakeDF]
    cp.WRITER_CLASSES[:] = [e2e._FakeWriter]
    cp.SESSION_CLASSES[:] = [_SlowFakeSpark]
    cp.READER_CLASSES[:] = [e2e._FakeReader]
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "par_job.py").write_text(src, encoding="utf-8")
            t0 = time.monotonic()
            res = cp.capture_plans(str(root), "par_job.py", fetch_catalog=False, branch_workers=4)
            elapsed = time.monotonic() - t0
    finally:
        cp_mod.get_spark = old_get
        cp.DF_CLASSES[:] = old_dfs
        cp.WRITER_CLASSES[:] = old_writers
        cp.SESSION_CLASSES[:] = old_sessions
        cp.READER_CLASSES[:] = old_readers

    assert res["status"] == "ok", res["error"]
    expected = {f"dev.tgt.tb_{i}_{v}" for i in range(1, 5) for v in ("t", "f")}
    assert {d["table"] for d in res["destinations"]} == expected
    assert max_concurrent >= 2, f"Esperava execução concorrente (>=2 threads), obtido: {max_concurrent}"
    # Passada 0 (~0.12s) + 4 passadas paralelas (~0.12s) deve ficar muito abaixo de 5 * 0.12s = 0.60s.
    assert elapsed < 0.50, f"Execução paralela lenta demais: {elapsed:.2f}s"


def test_parallel_branch_exploration_nested_branches_and_thread_isolation() -> None:
    src = (
        "dbutils.widgets.text('etapa', 'BASE')\n"
        "df = spark.table('dev.cadastros.tb_banco')\n"
        "modo = dbutils.widgets.get('modo')\n"
        "if modo == 'A':\n"
        "    dbutils.widgets.text('etapa', 'R_A')\n"
        "    if dbutils.widgets.get('sub') == 'X':\n"
        "        df.write.saveAsTable('dev.tgt.tb_a_x')\n"
        "    else:\n"
        "        df.write.saveAsTable('dev.tgt.tb_a_y')\n"
        "else:\n"
        "    if dbutils.widgets.get('etapa') == 'BASE':\n"
        "        df.write.saveAsTable('dev.tgt.tb_b_base')\n"
        "    else:\n"
        "        df.write.saveAsTable('dev.tgt.tb_b_other')\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "nested.py").write_text(src, encoding="utf-8")
        with e2e._fake_spark_env(e2e._CATALOG):
            res_par = cp.capture_plans(str(root), "nested.py", fetch_catalog=False, branch_workers=4)
        with e2e._fake_spark_env(e2e._CATALOG):
            res_seq = cp.capture_plans(str(root), "nested.py", fetch_catalog=False, branch_workers=1)

    expected = {"dev.tgt.tb_a_x", "dev.tgt.tb_a_y", "dev.tgt.tb_b_base", "dev.tgt.tb_b_other"}
    assert res_par["status"] == "ok", res_par["error"]
    assert res_seq["status"] == "ok", res_seq["error"]
    assert {d["table"] for d in res_par["destinations"]} == expected
    assert {d["table"] for d in res_seq["destinations"]} == expected
    assert res_par["settings"]["branch_workers"] == 4
    assert res_seq["settings"]["branch_workers"] == 1


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"OK  {name}")
