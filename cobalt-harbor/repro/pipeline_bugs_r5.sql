-- Reprodutor SQL — Rodada 5 de bugs do spark-lineage.
-- Cobre: R5-20, R5-21, R5-23.

/* Cabeçalho em comentário de bloco (R5-23):
   o comando real começa na linha abaixo, não nesta linha.
*/
CREATE /* inline */ TEMP VIEW vw_r5_base AS
SELECT
    id,
    EXTRACT(YEAR FROM dt_ref) AS ano_ref,
    nome
FROM dev.src.tb_vendas
WHERE nome = :cliente_nome;

-- COMMAND ----------

-- R5-20: comentário entre AS e SELECT faz _rewrite_safe_temp_view_sql não reescrever CACHE TABLE
CACHE TABLE vw_r5_cache AS /* comentario */ SELECT id, ano_ref FROM vw_r5_base;

-- COMMAND ----------

-- R5-21: multi-table insert com INSERT OVERWRITE DIRECTORY (extrai 'DIRECTORY' como tabela)
FROM vw_r5_cache
INSERT OVERWRITE DIRECTORY '/mnt/repro_r5/saida_dir' SELECT id
INSERT INTO dev.tgt.tb_r5_final SELECT id, ano_ref;

-- COMMAND ----------

-- R5-21: CREATE TABLE ... USING DELTA faz extract_sql_sources extrair 'DELTA'
CREATE TABLE dev.tgt.tb_r5_delta USING DELTA AS
SELECT id, ano_ref FROM vw_r5_cache;
