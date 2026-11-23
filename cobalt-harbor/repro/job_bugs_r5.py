# Databricks notebook source
"""Reprodutor Python — Rodada 5 de bugs do spark-lineage.

Cobre:
- R5-17: ternário (ast.IfExp) que lança exceção no ramo forçado -> call_site corrompido
- R5-19: helper/loop que chama df.collect() em DataFrames de schemas distintos entre passadas
- R5-20: df.createGlobalTempView() sem patch -> falha na 2a passada do BranchExplorer
- R5-22: %timeit de linha única apagado
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def coletar_linhas(df):
    return df.collect()


# R5-19: mesmo call_site chamado para dois DataFrames com schemas distintos + mutação in-place
df_clientes = spark.table("dev.src.tb_clientes")
df_vendas = spark.table("dev.src.tb_vendas")

rows_cli = coletar_linhas(df_clientes)
rows_cli.clear()  # muta lista retornada por collect()
rows_vnd = coletar_linhas(df_vendas)

# R5-20: createGlobalTempView não redirecionado para createOrReplaceGlobalTempView
df_vendas.createGlobalTempView("vw_global_r5")

# R5-17: ternário cujo ramo alternativo lança erro -> site_id "job_bugs_r5.py:<linha>:<col>"
flag = True
alvo = "dev.tgt.tb_ok" if flag else (_ for _ in ()).throw(ValueError("ramo alternativo invalido"))

# MAGIC %timeit df_vendas.write.mode("overwrite").saveAsTable(alvo)
