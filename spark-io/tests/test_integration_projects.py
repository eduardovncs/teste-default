"""Testes de integração (TDD) sobre projetos realistas multi-arquivo."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from spark_io import map_spark_io
from spark_io.cli import main as cli_main


def _write(base: Path, rel: str, content: str) -> Path:
    file_path = base / rel
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(textwrap.dedent(content).lstrip("\n"), encoding="utf-8")
    return file_path


def _write_ipynb(base: Path, rel: str, cells: list[str]) -> Path:
    nb = {
        "cells": [
            {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": c.splitlines(True)}
            for c in cells
        ],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return _write(base, rel, json.dumps(nb, ensure_ascii=False, indent=2))


def test_integration_full_hybrid_databricks_project_from_entrypoint(tmp_path: Path) -> None:
    """Valida exatamente entradas, saídas e dependências de um projeto completo a partir do entrypoint."""
    _write(
        tmp_path,
        "src/config.py",
        """
        SCHEMA_ING = "compe_ing"
        SCHEMA_GOLD = "compe_gold"
        TABELAS_DIM = ["tb_bancos", "tb_agencias"]
        """,
    )
    _write(
        tmp_path,
        "src/ambiente.py",
        """
        import os

        def descobrir_sigla():
            return os.environ["AMBIENTE_CLUSTER"]
        """,
    )
    _write(
        tmp_path,
        "src/io_helpers.py",
        """
        def ler_tabela(conector, nome_tabela):
            return conector.read.table(nome_tabela)

        def gravar_delta(df, destino, modo="overwrite"):
            df.write.format("delta").mode(modo).option("overwriteSchema", "true").saveAsTable(destino)
        """,
    )
    _write(
        tmp_path,
        "notebooks/comum_macros.py",
        """
        # Databricks notebook source
        SCHEMA_AUX = "aux_ing"

        def carregar_aux(tabela):
            return conector.read.table(tabela)
        """,
    )
    _write(
        tmp_path,
        "notebooks/etapa_enriquecimento.py",
        """
        # Databricks notebook source
        import pymongo
        import requests

        tbl_mov = dbutils.widgets.get("tabela_mov")
        amb = obter_env_runtime()

        df_mov = conector.read.table(f"{amb}_pag.{dbutils.widgets.get('schema_mov')}.{tbl_mov}")
        df_mov.createOrReplaceTempView("vw_mov_temp")

        # COMMAND ----------
        # MAGIC %sql
        # MAGIC INSERT OVERWRITE TABLE pr_pag.compe_silver.tb_mov_enriquecida
        # MAGIC SELECT m.*, b.nome
        # MAGIC FROM vw_mov_temp m
        # MAGIC JOIN pr_pag.compe_ing.tb_lookup_compe b ON m.cbco = b.cbco

        # COMMAND ----------
        # Código fora do Spark (MongoDB + API) não deve executar nem virar tabela Spark falsa
        client = pymongo.MongoClient("mongodb://prod.internal:27017")
        client["banco_prod"]["colecao_compe"].insert_many([{"cbco": 341}])
        requests.post("https://api.interna.banco/v1/notificar", json={"status": "ok"})
        """,
    )
    _write_ipynb(
        tmp_path,
        "notebooks/etapa_merge.ipynb",
        [
            'df_silver = conector.read.table("pr_pag.compe_silver.tb_mov_enriquecida")',
            (
                "%%sql\n"
                "MERGE INTO pr_pag.compe_gold.tb_consolidado t\n"
                "USING pr_pag.compe_silver.tb_mov_enriquecida s\n"
                "ON t.id = s.id\n"
                "WHEN MATCHED THEN UPDATE SET t.cbco = s.cbco"
            ),
        ],
    )
    # Arquivo órfão no repo: não alcançável pelo entrypoint, NÃO deve entrar no resultado
    _write(
        tmp_path,
        "notebooks/legado_nao_usado.py",
        """
        conector.read.table("pr_pag.legado_ing.tb_nao_deve_aparecer")
        df.write.saveAsTable("pr_pag.legado_gold.tb_fantasma")
        """,
    )
    _write(
        tmp_path,
        "entrypoint.py",
        """
        # Databricks notebook source
        # MAGIC %run /Shared/corp/auditoria
        # COMMAND ----------
        # MAGIC %run ./notebooks/comum_macros
        # COMMAND ----------
        from lib_corporativa import ler_base
        from src.ambiente import descobrir_sigla
        from src.config import SCHEMA_GOLD, SCHEMA_ING, TABELAS_DIM
        from src.io_helpers import gravar_delta, ler_tabela

        sigla = descobrir_sigla()

        for dim in TABELAS_DIM:
            ler_tabela(conector, f"{sigla}_pag.{SCHEMA_ING}.{dim}")

        df_aux = carregar_aux(f"{sigla}_pag.{SCHEMA_AUX}.tb_feriados")
        df_ext = ler_base("pr_pag.ext_ing.tb_cip")

        dbutils.notebook.run(
            "./notebooks/etapa_enriquecimento",
            600,
            {"tabela_mov": "tb_movimentos", "schema_mov": "compe_ing"},
        )
        dbutils.notebook.run("./notebooks/etapa_merge", 600)

        if executar_reprocessamento:
            gravar_delta(df_aux, f"{sigla}_pag.{SCHEMA_GOLD}.tb_reprocesso", modo="append")
        else:
            gravar_delta(df_aux, f"{sigla}_pag.{SCHEMA_GOLD}.tb_diario")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="entrypoint.py", env_prefix="pr")

    assert {e.target for e in res.inputs} == {
        "pr_pag.compe_ing.tb_bancos",
        "pr_pag.compe_ing.tb_agencias",
        "pr_pag.aux_ing.tb_feriados",
        "pr_pag.compe_ing.tb_movimentos",
        "pr_pag.compe_ing.tb_lookup_compe",
        "pr_pag.compe_silver.tb_mov_enriquecida",
    }
    assert {e.target for e in res.outputs} == {
        "pr_pag.compe_silver.tb_mov_enriquecida",
        "pr_pag.compe_gold.tb_consolidado",
        "pr_pag.compe_gold.tb_reprocesso",
        "pr_pag.compe_gold.tb_diario",
    }
    assert {(r.call, r.target) for r in res.unclassified_table_refs} == {
        ("ler_base", "pr_pag.ext_ing.tb_cip"),
    }
    assert {(e.via, e.target_path) for e in res.external_dependencies} == {
        ("%run", "/Shared/corp/auditoria"),
    }
    assert res.unresolved_dynamic == []
    assert "pr_pag.legado_ing.tb_nao_deve_aparecer" not in {e.target for e in res.inputs}
    assert "pr_pag.legado_gold.tb_fantasma" not in {e.target for e in res.outputs}


def test_integration_directory_scan_via_cli_exact_io_and_no_phantoms(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Valida varredura completa de diretório via CLI em projeto misto (.py + .sql + .ipynb)."""
    _write(
        tmp_path,
        "jobs/step1.py",
        """
        df = session.read.table(f"{get_stage()}_cred.cred_ing.tb_contratos")
        df.write.mode("overwrite").saveAsTable(f"{get_stage()}_cred.cred_stg.tb_contratos_norm")
        """,
    )
    _write(
        tmp_path,
        "sql/step2.sql",
        """
        CREATE OR REPLACE TABLE pr_cred.cred_gold.tb_contratos_final AS
        SELECT c.*, b.cbco_st
        FROM pr_cred.cred_stg.tb_contratos_norm c
        LEFT JOIN pr_cred.cred_ing.tb_depara_bancos b
          ON c.cbco = b.cbco;
        """,
    )
    _write_ipynb(
        tmp_path,
        "notebooks/step3_export.ipynb",
        [
            'df_final = conector.read.table("pr_cred.cred_gold.tb_contratos_final")',
            'df_final.write.format("parquet").mode("overwrite").save("/mnt/exports/compe_final")',
        ],
    )

    exit_code = cli_main(["--path", str(tmp_path), "--env-prefix", "pr"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert {item["target"] for item in payload["inputs"]} == {
        "pr_cred.cred_ing.tb_contratos",
        "pr_cred.cred_stg.tb_contratos_norm",
        "pr_cred.cred_ing.tb_depara_bancos",
        "pr_cred.cred_gold.tb_contratos_final",
    }
    assert {item["target"] for item in payload["outputs"]} == {
        "pr_cred.cred_stg.tb_contratos_norm",
        "pr_cred.cred_gold.tb_contratos_final",
        "/mnt/exports/compe_final",
    }
    assert payload["unresolved_dynamic"] == []
    assert payload["unclassified_table_refs"] == []
