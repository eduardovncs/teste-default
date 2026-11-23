-- Databricks notebook source
-- Reprodutor de bugs (Rodada 3) em SQL (.sql):
-- 1. `CACHE TABLE vw_cache_base AS SELECT ...` é bloqueado ('block', 'CACHE') e não cria a view,
--    quebrando o INSERT seguinte (`TABLE_OR_VIEW_NOT_FOUND`).
-- 2. `CREATE TEMP VIEW vw_sem_replace AS ...` (sem OR REPLACE) falha na passada 1 do BranchExplorer.
CACHE TABLE vw_cache_base AS
SELECT id, CAST(id AS STRING) AS nome FROM range(5);

-- COMMAND ----------
INSERT INTO dev.tgt.tb_from_cache
SELECT id, nome FROM vw_cache_base;

-- COMMAND ----------
CREATE TEMP VIEW vw_sem_replace AS
SELECT id FROM range(5);

-- COMMAND ----------
-- MAGIC %python
-- MAGIC if dbutils.widgets.get("modo") == "EXTRA":
-- MAGIC     spark.table("vw_sem_replace").write.saveAsTable("dev.tgt.tb_extra_r3")
-- MAGIC else:
-- MAGIC     spark.table("vw_sem_replace").write.saveAsTable("dev.tgt.tb_normal_r3")
