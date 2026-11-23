-- Databricks notebook source
-- Reprodutor de bugs no spark-lineage ao capturar um arquivo/notebook .sql:
-- 1. CREATE OR REPLACE VIEW: `extract_sql_target` só aceita TABLE (retorna None para VIEW).
-- 2. WITH ... SELECT com `) update`: `classify_sql` classifica como `sql:INSERT` (destino fantasma).
-- 3. Literal de string com `--`: `_COMMENT_RE` devora o resto da linha, sumindo com `FROM vw_base` em `read_sites`.
-- 4. Parâmetro `:modo` dentro de literal de string: `substitute_sql_params` injeta aspas extras e quebra o SQL.
CREATE OR REPLACE TEMP VIEW vw_base AS
SELECT id, CAST(id AS STRING) AS nome FROM range(5);

-- COMMAND ----------
-- Bug 1: destino de CREATE OR REPLACE VIEW vira `table: null` em `destinations`
CREATE OR REPLACE VIEW dev.tgt.vw_publicada AS
SELECT id, nome FROM vw_base;

-- COMMAND ----------
-- Bug 2: SELECT puro com alias `update` após `)` vira destino fantasma `sql:INSERT`
WITH base AS (
    SELECT id, CAST(id AS STRING) update
    FROM vw_base
)
SELECT id, update FROM base;

-- COMMAND ----------
-- Bug 3: '--' dentro de string faz `extract_sql_sources` ignorar `FROM vw_base`
SELECT id, COALESCE(nome, '--') AS nome_fmt FROM vw_base;

-- COMMAND ----------
-- MAGIC %python
-- MAGIC dbutils.widgets.text("modo", "PROD")

-- COMMAND ----------
-- Bug 4: `:modo` dentro de string literal é substituído por `'PROD'`, gerando SQL inválido ('etiqueta: 'PROD'')
SELECT id, 'etiqueta: :modo' AS info FROM vw_base;
