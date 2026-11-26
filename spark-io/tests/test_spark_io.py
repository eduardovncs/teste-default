"""Testes TDD da Fase 1 — mapeamento estático de entradas e saídas Spark."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from spark_io import CustomWrapperSpec, map_spark_io
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


# --------------------------------------------------------------------------- #
# 1. Primitivas Spark/Delta (qualquer receiver, sem depender do nome `spark`)
# --------------------------------------------------------------------------- #


def test_maps_spark_reads_and_writes_on_arbitrary_receivers(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "job.py",
        """
        df1 = conector.read.table("pr_xpto.xpto_ing.tb_clientes")
        df2 = self.spark.read.format("delta").option("path", "/mnt/raw/pagamentos").load()
        df3 = sessao.read.parquet("/mnt/raw/ext")
        df4 = conector.table("pr_xpto.xpto_ing.tb_bancos")
        df_out = df1.join(df4, "cbco")
        df_out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("pr_xpto.gold.tb_saida")
        df_out.write.format("parquet").mode("append").option("path", "/mnt/out/arqs").save()
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="job.py")

    input_targets = {e.target for e in res.inputs}
    assert {
        "pr_xpto.xpto_ing.tb_clientes",
        "/mnt/raw/pagamentos",
        "/mnt/raw/ext",
        "pr_xpto.xpto_ing.tb_bancos",
    } <= input_targets

    out_by_target = {e.target: e for e in res.outputs}
    assert "pr_xpto.gold.tb_saida" in out_by_target
    assert out_by_target["pr_xpto.gold.tb_saida"].mode == "overwrite"
    assert out_by_target["pr_xpto.gold.tb_saida"].options.get("overwriteSchema") == "true"
    assert "/mnt/out/arqs" in out_by_target
    assert out_by_target["/mnt/out/arqs"].mode == "append"
    assert out_by_target["/mnt/out/arqs"].target_type == "path"


def test_maps_delta_table_write_to_merge_into_and_streaming(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "delta_job.py",
        """
        from delta.tables import DeltaTable

        src = conector.readStream.table("pr_xpto.xpto_ing.stream_in")
        src.writeStream.toTable("pr_xpto.gold.stream_out")
        src.writeTo("pr_xpto.gold.tb_v2").overwritePartitions()
        src.mergeInto("pr_xpto.gold.tb_merge_v4", "a.id = b.id").whenMatched().updateAll().merge()

        dt = DeltaTable.forName(conector, "pr_xpto.gold.tb_delta_alvo")
        dt.alias("t").merge(src.alias("s"), "t.id = s.id").whenMatchedUpdateAll().execute()
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="delta_job.py")

    assert "pr_xpto.xpto_ing.stream_in" in {e.target for e in res.inputs}
    output_targets = {e.target for e in res.outputs}
    assert {
        "pr_xpto.gold.stream_out",
        "pr_xpto.gold.tb_v2",
        "pr_xpto.gold.tb_merge_v4",
        "pr_xpto.gold.tb_delta_alvo",
    } <= output_targets


# --------------------------------------------------------------------------- #
# 2. Avaliação parcial no AST (agnóstica ao nome de variável + env_prefix=pr)
# --------------------------------------------------------------------------- #


def test_resolves_user_example_wrapper_and_dynamic_env_prefix(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/lib.py",
        """
        def ler_tabela(nome_tabela):
            return conector.read.table(nome_tabela)
        """,
    )
    _write(
        tmp_path,
        "src/ambiente.py",
        """
        def get_env():
            return os.getenv("AMBIENTE_EXECUCAO")
        """,
    )
    _write(
        tmp_path,
        "main.py",
        """
        from src.lib import ler_tabela
        from src import ambiente

        env = ambiente.get_env()
        tabela = "xpto"
        df = ler_tabela(f"{env}_xpto.xpto_ing.{tabela}")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="main.py", env_prefix="pr")

    assert any(e.target == "pr_xpto.xpto_ing.xpto" and not e.dynamic for e in res.inputs)
    assert any(w.name.endswith("ler_tabela") and w.kind == "read" for w in res.wrappers)


def test_env_prefix_is_agnostic_to_variable_and_call_names(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "job.py",
        """
        sigla = descobrir_ambiente()
        df1 = conector.read.table(f"{sigla}_fin.fin_ing.tb_a")
        df2 = conector.read.table(f"{obter_prefixo()}_cred.cred_ing.tb_b")
        df3 = conector.read.table(f"{cfg['amb']}_core.core_ing.tb_c")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="job.py", env_prefix="pr")

    input_targets = {e.target for e in res.inputs}
    assert {
        "pr_fin.fin_ing.tb_a",
        "pr_cred.cred_ing.tb_b",
        "pr_core.core_ing.tb_c",
    } <= input_targets
    assert all(not e.dynamic for e in res.inputs)


def test_partial_evaluation_preserves_wildcard_when_whole_catalog_is_dynamic_or_uses_vars(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "job.py",
        """
        cat = obter_catalogo()
        df = conector.read.table(f"{cat}.xpto_ing.tb_contas")
        """,
    )

    res_partial = map_spark_io(tmp_path, entrypoint="job.py", env_prefix="pr")
    assert any(e.target == "<*>.xpto_ing.tb_contas" and e.dynamic for e in res_partial.inputs)

    res_with_var = map_spark_io(tmp_path, entrypoint="job.py", env_prefix="pr", variables={"cat": "pr_core"})
    assert any(e.target == "pr_core.xpto_ing.tb_contas" and not e.dynamic for e in res_with_var.inputs)


def test_partial_evaluation_handles_concat_format_dicts_loops_and_if_else(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "job.py",
        """
        PREFIXO = "pr_xpto"
        CFG = {"origem": "pr_xpto.xpto_ing.tb_dict"}
        TABELAS = ["tb_l1", "tb_l2"]

        df_dict = conector.read.table(CFG["origem"])
        df_concat = conector.read.table(PREFIXO + ".xpto_ing.tb_concat")
        df_fmt = conector.read.table("{}.xpto_ing.tb_fmt".format(PREFIXO))

        for t in TABELAS:
            conector.read.table(f"{amb}_xpto.xpto_ing.{t}")

        if modo == "REPROC":
            df_dict.write.saveAsTable("pr_xpto.gold.tb_reproc")
        else:
            df_dict.write.saveAsTable("pr_xpto.gold.tb_normal")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="job.py", env_prefix="pr")

    assert {
        "pr_xpto.xpto_ing.tb_dict",
        "pr_xpto.xpto_ing.tb_concat",
        "pr_xpto.xpto_ing.tb_fmt",
        "pr_xpto.xpto_ing.tb_l1",
        "pr_xpto.xpto_ing.tb_l2",
    } <= {e.target for e in res.inputs}
    assert {"pr_xpto.gold.tb_reproc", "pr_xpto.gold.tb_normal"} <= {e.target for e in res.outputs}


# --------------------------------------------------------------------------- #
# 3. Wrappers multi-hop, externos desconhecidos e expressões opacas
# --------------------------------------------------------------------------- #


def test_propagates_multihop_wrappers_and_handles_external_or_opaque_calls(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "utils/raw.py",
        """
        def ler_raw(tbl):
            return conector.read.table(tbl)
        """,
    )
    _write(
        tmp_path,
        "utils/io.py",
        """
        from utils.raw import ler_raw

        def ler_camada(nome):
            return ler_raw(nome)
        """,
    )
    _write(
        tmp_path,
        "job.py",
        """
        from utils.io import ler_camada
        from pacote_externo import ler_base, carregar_custom

        df1 = ler_camada("pr_xpto.xpto_ing.tb_multihop")
        df2 = ler_base("pr_xpto.xpto_ing.tb_externa")
        df3 = carregar_custom("pr_xpto.xpto_ing.tb_custom")
        df4 = conector.read.table(obter_nome_do_mongo())
        """,
    )

    res = map_spark_io(
        tmp_path,
        entrypoint="job.py",
        custom_wrappers={"carregar_custom": CustomWrapperSpec(kind="read")},
    )

    input_targets = {e.target for e in res.inputs}
    assert "pr_xpto.xpto_ing.tb_multihop" in input_targets
    assert "pr_xpto.xpto_ing.tb_custom" in input_targets

    assert any(r.call == "ler_base" and r.target == "pr_xpto.xpto_ing.tb_externa" for r in res.unclassified_table_refs)
    assert any(u.kind == "read" and "obter_nome_do_mongo" in u.expr for u in res.unresolved_dynamic)


# --------------------------------------------------------------------------- #
# 4. Dependências de notebooks (%run, ciclos, dbutils.notebook.run + widgets)
# --------------------------------------------------------------------------- #


def test_resolves_percent_run_scope_sharing_and_avoids_cycles(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "utils/helpers.py",
        """
        # Databricks notebook source
        SCHEMA_ING = "xpto_ing"

        def ler_com_conector(tbl):
            return conector.read.table(tbl)
        """,
    )
    _write(
        tmp_path,
        "nb_a.py",
        """
        # Databricks notebook source
        # MAGIC %run ./utils/helpers
        # COMMAND ----------
        # MAGIC %run ./nb_b
        # COMMAND ----------
        df = ler_com_conector(f"{env}_xpto.{SCHEMA_ING}.tb_via_run")
        """,
    )
    _write(
        tmp_path,
        "nb_b.py",
        """
        # Databricks notebook source
        # MAGIC %run ./nb_a
        # COMMAND ----------
        conector.read.table("pr_xpto.xpto_ing.tb_do_nb_b")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="nb_a.py", env_prefix="pr")

    input_targets = {e.target for e in res.inputs}
    assert "pr_xpto.xpto_ing.tb_via_run" in input_targets
    assert "pr_xpto.xpto_ing.tb_do_nb_b" in input_targets
    assert any(d.via == "%run" and d.target_file.endswith("utils/helpers.py") for d in res.dependencies)


def test_resolves_dbutils_notebook_run_widgets_and_external_workspace_paths(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "filho.py",
        """
        # Databricks notebook source
        tbl = dbutils.widgets.get("tabela")
        df = conector.read.table(f"{amb}_xpto.xpto_ing.{tbl}")
        df.write.saveAsTable("pr_xpto.gold.tb_filho_out")
        """,
    )
    _write(
        tmp_path,
        "orquestrador.py",
        """
        # Databricks notebook source
        # MAGIC %run /Shared/corp/funcoes_globais
        # COMMAND ----------
        dbutils.notebook.run("./filho", 600, {"tabela": "tb_widget"})
        dbutils.notebook.run("/Repos/outro_time/pipeline_ext", 600)
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="orquestrador.py", env_prefix="pr")

    assert "pr_xpto.xpto_ing.tb_widget" in {e.target for e in res.inputs}
    assert "pr_xpto.gold.tb_filho_out" in {e.target for e in res.outputs}
    assert any(d.via == "dbutils.notebook.run" and d.arguments.get("tabela") == "tb_widget" for d in res.dependencies)
    ext_paths = {(e.via, e.target_path) for e in res.external_dependencies}
    assert ("%run", "/Shared/corp/funcoes_globais") in ext_paths
    assert ("dbutils.notebook.run", "/Repos/outro_time/pipeline_ext") in ext_paths


# --------------------------------------------------------------------------- #
# 5. Múltiplos formatos (.py Databricks, .ipynb, .sql) e SQL via sqlglot
# --------------------------------------------------------------------------- #


def test_parses_databricks_magic_sql_ipynb_and_pure_sql_files(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "nb_databricks.py",
        """
        # Databricks notebook source
        # MAGIC %md
        # MAGIC # Título do notebook
        # COMMAND ----------
        # MAGIC %sql
        # MAGIC CREATE OR REPLACE TABLE pr_xpto.gold.tb_magic_out AS
        # MAGIC SELECT * FROM pr_xpto.xpto_ing.tb_magic_in
        # COMMAND ----------
        conector.sql(f"INSERT INTO {env}_xpto.gold.tb_fstr_out SELECT * FROM {env}_xpto.xpto_ing.tb_fstr_in")
        """,
    )
    _write_ipynb(
        tmp_path,
        "nb_jupyter.ipynb",
        [
            'df = conector.read.table("pr_xpto.xpto_ing.tb_ipynb_py")',
            (
                "%%sql\n"
                "MERGE INTO pr_xpto.gold.tb_ipynb_merge t\n"
                "USING pr_xpto.xpto_ing.tb_ipynb_src s\n"
                "ON t.id = s.id\n"
                "WHEN MATCHED THEN UPDATE SET t.cbco = s.cbco"
            ),
        ],
    )
    _write(
        tmp_path,
        "pipeline.sql",
        """
        /* Comentário de bloco */
        WITH base AS (
            SELECT id, '-- não é comentário' AS txt
            FROM pr_xpto.xpto_ing.tb_sql_src
        )
        INSERT OVERWRITE TABLE pr_xpto.gold.tb_sql_dst
        SELECT id, txt FROM base;
        """,
    )

    res = map_spark_io(tmp_path, env_prefix="pr")

    input_targets = {e.target for e in res.inputs}
    output_targets = {e.target for e in res.outputs}
    assert {
        "pr_xpto.xpto_ing.tb_magic_in",
        "pr_xpto.xpto_ing.tb_fstr_in",
        "pr_xpto.xpto_ing.tb_ipynb_py",
        "pr_xpto.xpto_ing.tb_ipynb_src",
        "pr_xpto.xpto_ing.tb_sql_src",
    } <= input_targets
    assert {
        "pr_xpto.gold.tb_magic_out",
        "pr_xpto.gold.tb_fstr_out",
        "pr_xpto.gold.tb_ipynb_merge",
        "pr_xpto.gold.tb_sql_dst",
    } <= output_targets


# --------------------------------------------------------------------------- #
# 6. Segurança (zero execução) e controle de falsos positivos
# --------------------------------------------------------------------------- #


def test_never_executes_user_code_and_avoids_false_positives(tmp_path: Path) -> None:
    sentinel = tmp_path / "NUNCA_CRIAR.txt"
    _write(
        tmp_path,
        "perigoso.py",
        f"""
        import logging
        from pyspark.sql.functions import col

        Path({str(sentinel)!r}).write_text("side-effect")
        raise RuntimeError("Este código jamais deve ser executado")

        logger = logging.getLogger("app.core.service")
        url = "https://api.banco.com/v1/bancos"
        versao = "1.2.3"
        df = conector.read.table("pr_xpto.xpto_ing.tb_segura")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="perigoso.py")

    assert not sentinel.exists()
    assert "pr_xpto.xpto_ing.tb_segura" in {e.target for e in res.inputs}
    unclassified_targets = {r.target for r in res.unclassified_table_refs}
    assert "app.core.service" not in unclassified_targets
    assert "1.2.3" not in unclassified_targets
    assert "https://api.banco.com/v1/bancos" not in unclassified_targets


# --------------------------------------------------------------------------- #
# 7. CLI JSON
# --------------------------------------------------------------------------- #


def test_cli_outputs_valid_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _write(tmp_path, "job.py", 'df = conector.read.table(f"{env}_xpto.xpto_ing.tb_cli")\n')

    code = cli_main(["--path", str(tmp_path), "--entrypoint", "job.py", "--env-prefix", "pr"])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert any(item["target"] == "pr_xpto.xpto_ing.tb_cli" for item in payload["inputs"])


# --------------------------------------------------------------------------- #
# 8. Dicionários aninhados, helpers pass-through/constantes e wrappers indiretos
# --------------------------------------------------------------------------- #


def test_resolves_nested_dict_passthrough_helper_and_indirect_wrapper(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "sample.py",
        """
        input_Tables = {
            "pr": {"tabela_gsal": "pr_xpt.gsal_ing.tcredt"},
            "ex": {"tabela_gsal": "ex_xpt.gsal_ing.tcredt"},
        }

        def _validate_fqn(table):
            return table

        def enviroment_of_cluster(spark):
            return "pr"

        def hive_get_table(spark, table):
            table_fqn = _validate_fqn(table)
            return spark.table(table_fqn)

        def main(spark):
            env = enviroment_of_cluster(spark)
            t_gsal = _validate_fqn(input_Tables[env]["tabela_gsal"])
            df = hive_get_table(spark, t_gsal)
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="sample.py", env_prefix="pr")

    assert [e.target for e in res.inputs] == ["pr_xpt.gsal_ing.tcredt"]
    assert res.inputs[0].via == "wrapper:hive_get_table"
    assert not res.inputs[0].dynamic
    assert any(
        w.name == "hive_get_table" and w.target_param == "table" and w.target_param_index == 1 for w in res.wrappers
    )


def test_nested_dict_falls_back_to_env_prefix_when_env_helper_is_opaque(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "cfg.py",
        """
        TABLES = {
            "pr": {"gsal": "pr_xpt.gsal_ing.tcredt", "out": "pr_xpt.gsal_gold.tfinal"},
            "dv": {"gsal": "dv_xpt.gsal_ing.tcredt", "out": "dv_xpt.gsal_gold.tfinal"},
        }
        """,
    )
    _write(
        tmp_path,
        "job.py",
        """
        from cfg import TABLES
        from lib_ext import get_cluster_env

        def _clean(name):
            return name.strip()

        def write_hive(df, target):
            fqn = _clean(target)
            df.write.mode("overwrite").saveAsTable(fqn)

        def run(spark):
            env = get_cluster_env(spark)
            src = _clean(TABLES[env]["gsal"])
            dst = _clean(TABLES[env]["out"])
            df = spark.table(src)
            write_hive(df, dst)
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="job.py", env_prefix="pr")

    assert [e.target for e in res.inputs] == ["pr_xpt.gsal_ing.tcredt"]
    assert [e.target for e in res.outputs] == ["pr_xpt.gsal_gold.tfinal"]
    assert res.outputs[0].via == "wrapper:write_hive"
    assert res.unresolved_dynamic == []


# --------------------------------------------------------------------------- #
# 9. Cobertura total (return, classes, builders, imports locais e safety net)
# --------------------------------------------------------------------------- #


def test_captures_reads_and_writes_in_return_class_async_and_while(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "oop_job.py",
        """
        def carregar_direto():
            return conector.read.table("pr_xpto.xpto_ing.tb_return")

        async def carregar_async():
            return conector.table("pr_xpto.xpto_ing.tb_async")

        class PipelineCompe:
            def executar(self):
                df = self.spark.read.table("pr_xpto.xpto_ing.tb_class")
                while tem_lote:
                    extra = self.spark.read.parquet("/mnt/raw/while_in")
                df.write.mode("overwrite").saveAsTable("pr_xpto.gold.tb_class_out")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="oop_job.py", env_prefix="pr")

    assert {
        "pr_xpto.xpto_ing.tb_return",
        "pr_xpto.xpto_ing.tb_async",
        "pr_xpto.xpto_ing.tb_class",
        "/mnt/raw/while_in",
    } <= {e.target for e in res.inputs}
    assert "pr_xpto.gold.tb_class_out" in {e.target for e in res.outputs}


def test_captures_multiline_reader_and_writer_builders(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "builders.py",
        """
        reader = conector.read.format("parquet").option("header", "true")
        reader = reader.option("path", "/mnt/raw/builder_opt")
        df1 = reader.load()

        r2 = conector.read.format("delta")
        df2 = r2.table("pr_xpto.xpto_ing.tb_builder")

        writer = df1.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        writer.saveAsTable("pr_xpto.gold.tb_writer_out")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="builders.py", env_prefix="pr")

    assert {"/mnt/raw/builder_opt", "pr_xpto.xpto_ing.tb_builder"} <= {e.target for e in res.inputs}
    out_by_target = {e.target: e for e in res.outputs}
    assert "pr_xpto.gold.tb_writer_out" in out_by_target
    assert out_by_target["pr_xpto.gold.tb_writer_out"].mode == "overwrite"
    assert out_by_target["pr_xpto.gold.tb_writer_out"].options.get("overwriteSchema") == "true"


def test_follows_imported_local_functions_called_from_entrypoint(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "src/etapa.py",
        """
        SCHEMA = "xpto_ing"

        def rodar(spark):
            df = spark.read.table(f"{env}_xpto.{SCHEMA}.tb_modulo")
            df.write.saveAsTable("pr_xpto.gold.tb_modulo_out")

        def funcao_nao_chamada(spark):
            return spark.read.table("pr_xpto.xpto_ing.tb_morta")
        """,
    )
    _write(
        tmp_path,
        "entrypoint.py",
        """
        from src.etapa import rodar

        rodar(conector)
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="entrypoint.py", env_prefix="pr")

    assert {e.target for e in res.inputs} == {"pr_xpto.xpto_ing.tb_modulo"}
    assert {e.target for e in res.outputs} == {"pr_xpto.gold.tb_modulo_out"}


def test_captures_list_of_paths_and_fixed_tables_inside_wrappers(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "job.py",
        """
        def ler_com_dim(spark, tbl):
            dim = spark.table("pr_xpto.xpto_ing.tb_dim_fixa")
            return spark.table(tbl).join(dim, "cbco")

        df_paths = conector.read.parquet(["/mnt/raw/p1", "/mnt/raw/p2"])
        df_fato = ler_com_dim(conector, "pr_xpto.xpto_ing.tb_fato")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="job.py", env_prefix="pr")

    assert {
        "/mnt/raw/p1",
        "/mnt/raw/p2",
        "pr_xpto.xpto_ing.tb_fato",
        "pr_xpto.xpto_ing.tb_dim_fixa",
    } <= {e.target for e in res.inputs}


def test_safety_net_never_silently_drops_exotic_spark_read_or_write(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "exotico.py",
        """
        dfs = [conector.read.load(p) for p in listar_caminhos_externos()]
        leitor = lambda nome: conector.table(nome)
        conector.sql(montar_query_dinamica())
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="exotico.py", env_prefix="pr")

    unresolved_lines = {u.line for u in res.unresolved_dynamic}
    assert {1, 2, 3} <= unresolved_lines
    assert all(u.kind == "read" for u in res.unresolved_dynamic if u.line in {1, 2, 3})


def test_wrapper_with_format_and_fstring_save_as_table(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "save_wrapper.py",
        """
        def save_into_hdfs_as_text(spark, df, table_name):
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                f"{table_name}".format(table_name=table_name)
            )

        def save_with_plain_format(df, table_name):
            df.write.mode("append").saveAsTable("{table_name}".format(table_name=table_name))

        def main(spark, df):
            save_into_hdfs_as_text(spark, df, "pr_adqemp.aabr_wks.book_consignado_delta")
            save_with_plain_format(df, "pr_adqemp.aabr_wks.book_consignado_hist")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="save_wrapper.py", env_prefix="pr")

    out_by_target = {e.target: e for e in res.outputs}
    assert set(out_by_target) == {
        "pr_adqemp.aabr_wks.book_consignado_delta",
        "pr_adqemp.aabr_wks.book_consignado_hist",
    }
    first = out_by_target["pr_adqemp.aabr_wks.book_consignado_delta"]
    assert first.via == "wrapper:save_into_hdfs_as_text"
    assert first.mode == "overwrite"
    assert first.data_format == "delta"
    assert first.options.get("overwriteSchema") == "true"
    assert res.unresolved_dynamic == []


def test_sql_drop_truncate_alter_optimize_are_not_recorded_as_inputs(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "ddl_job.py",
        """
        conector.sql("DROP TABLE IF EXISTS pr_xpto.gold.tb_antiga")
        conector.sql("DROP VIEW IF EXISTS vw_lixo")
        conector.sql("TRUNCATE TABLE pr_xpto.gold.tb_truncada")
        conector.sql("ALTER TABLE pr_xpto.gold.tb_alterada ADD COLUMNS (cbco_st STRING)")
        conector.sql("OPTIMIZE pr_xpto.gold.tb_otimizada ZORDER BY (cbco)")
        conector.sql("VACUUM pr_xpto.gold.tb_vacuum RETAIN 168 HOURS")
        conector.sql("DESCRIBE TABLE pr_xpto.gold.tb_desc")
        conector.sql("CACHE TABLE vw_cache AS SELECT * FROM pr_xpto.xpto_ing.tb_real_src")
        conector.sql("INSERT INTO pr_xpto.gold.tb_real_dst SELECT * FROM vw_cache")
        """,
    )

    res = map_spark_io(tmp_path, entrypoint="ddl_job.py", env_prefix="pr")

    assert {e.target for e in res.inputs} == {"pr_xpto.xpto_ing.tb_real_src"}
    assert {e.target for e in res.outputs} == {"pr_xpto.gold.tb_real_dst"}
    assert res.unresolved_dynamic == []
