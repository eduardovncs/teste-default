# Databricks notebook source
if dbutils.widgets.get("etapa") == "CARGA":
    spark.range(5).write.saveAsTable("dev.tgt.tb_carga")
