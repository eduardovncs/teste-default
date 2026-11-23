-- Databricks notebook source
-- Reprodutor de bugs (Rodada 4) em SQL (.sql):
-- 1. `CREATE TEMP VIEW IF NOT EXISTS` é reescrito para `CREATE OR REPLACE TEMP VIEW IF NOT EXISTS` (SQL inválido!).
-- 2. `CACHE TABLE vw_no_as SELECT ...` (sem `AS`, válido no Spark SQL) ainda é bloqueado.
-- 3. `INSERT OVERWRITE DIRECTORY '/mnt/out'` extrai `'DIRECTORY'` como tabela de destino.
-- 4. `MERGE WITH SCHEMA EVOLUTION INTO`, `DELETE <tb>` (sem `FROM`) e `CREATE OR REFRESH STREAMING TABLE` retornam target=None.

CREATE TEMP VIEW IF NOT EXISTS vw_base_r4 AS
SELECT id, CAST(id AS STRING) AS nome FROM range(5);

-- COMMAND ----------
CACHE TABLE vw_cache_sem_as
SELECT id, nome FROM vw_base_r4;

-- COMMAND ----------
INSERT OVERWRITE DIRECTORY '/mnt/out/r4'
USING PARQUET
SELECT id, nome FROM vw_cache_sem_as;

-- COMMAND ----------
MERGE WITH SCHEMA EVOLUTION INTO dev.tgt.tb_merge_ev t
USING vw_base_r4 s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *;

-- COMMAND ----------
DELETE dev.tgt.tb_del_sem_from WHERE id = 1;

-- COMMAND ----------
CREATE OR REFRESH STREAMING TABLE dev.tgt.tb_streaming_dlt AS
SELECT id, nome FROM vw_base_r4;
