-- Databricks notebook source
-- Reprodutor de bugs (Rodada 2) no spark-lineage em SQL (.sql):
-- 1. String entre aspas duplas com `FROM`/`JOIN` gera tabelas fantasmas em `read_sites`.
-- 2. `FROM ... WHERE ... = "(teste" INSERT INTO ...`: `(` em string com aspas duplas
--    desbalanceia `_top_level_sql`, fazendo `classify_sql` devolver `('pass', 'FROM')` (bypass de DML!).
-- 3. Multi-table insert (`FROM ... INSERT INTO tgt1 ... INSERT INTO tgt2 ...`):
--    só o 1º destino (`tb_ativos`) é extraído; `tb_inativos` é perdido.
CREATE OR REPLACE TEMP VIEW vw_base AS
SELECT id, CAST(id AS STRING) AS nome FROM range(5);

-- COMMAND ----------
-- Bug R2-1a: `"..."` no Spark SQL é literal de string, mas `extract_sql_sources` extrai `tabela_falsa` e `outra_falsa`
SELECT id, "texto FROM tabela_falsa JOIN outra_falsa" AS msg FROM vw_base;

-- COMMAND ----------
-- Bug R2-1b: `(` dentro de string com aspas duplas burla detecção de INSERT em `FROM ... INSERT INTO`
FROM vw_base
WHERE nome = "(teste"
INSERT INTO dev.tgt.tb_bypass
SELECT id, nome;

-- COMMAND ----------
-- Bug R2-4: multi-table insert perde o segundo destino (`dev.tgt.tb_inativos`)
FROM vw_base
INSERT INTO dev.tgt.tb_ativos SELECT id, nome WHERE id > 0
INSERT INTO dev.tgt.tb_inativos SELECT id, nome WHERE id <= 0;
