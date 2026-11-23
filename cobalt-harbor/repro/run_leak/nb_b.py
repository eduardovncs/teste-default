# Databricks notebook source
if dbutils.widgets.get("etapa") == "VALIDACAO":
    spark.range(5).write.saveAsTable("dev.tgt.tb_validacao")
