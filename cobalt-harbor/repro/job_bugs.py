"""Reprodutor de bugs no spark-lineage ao capturar um job PySpark (.py).

Cada bloco abaixo representa um padrão válido e comum em PySpark/Databricks
que produz resultado incorreto no `spark-lineage capture`:

1. `df.write.option("path", ...).save()`:
   - Esperado: "/mnt/repro_lineage/vendas" listado em `destinations` e `flows`.
   - Atual: `Capturer._target` ignora `.option("path", ...)` (retorna `None`) e
     `sources.py` descarta a escrita silenciosamente.

2. `df.mergeInto("dev.tgt.tb_merge_destino", ...)`:
   - Esperado: "dev.tgt.tb_merge_destino" listado em `destinations` e `flows`.
   - Atual: `capture_plans.py` grava `action="mergeInto(source)"`, mas
     `sources.is_write_action("mergeInto(source)")` retorna `False`, descartando o destino.

3. `if os.getenv("MODO") == "REPROC":`:
   - Esperado: `BranchExplorer` explora ambos os ramos (True e False), capturando
     "dev.tgt.tb_reprocessamento" e "dev.tgt.tb_normal".
   - Atual: `"os"` está em `_SKIP_NAMES`, então o `if` não é instrumentado;
     `branches` fica vazio e "dev.tgt.tb_reprocessamento" nunca é capturado.

4. `spark.sql("WITH base AS (SELECT id, CAST(id AS STRING) update ...) SELECT ...")`:
   - Esperado: tratado como `SELECT` normal; `dev.tgt.tb_from_cte` recebe colunas `id` e `update`.
   - Atual: `_CTE_DML_RE` casa `) update` e classifica a leitura como `("explain", "INSERT")`,
     criando um destino fantasma `<sql:INSERT: ...>` e retornando `SELECT 1 AS _blocked WHERE 1=0`,
     corrompendo o schema/linhagem de `dev.tgt.tb_from_cte` (que vira `_blocked`).
"""

import os

from pyspark.sql import functions as F

df = spark.range(5)

# Bug 1: destino passado via .option("path", ...) em .save() sem argumentos
df.write.format("delta").mode("overwrite").option("path", "/mnt/repro_lineage/vendas").save()

# Bug 2: df.mergeInto (API Spark 4 / Databricks Connect 16.4) ignorado por is_write_action
df.mergeInto("dev.tgt.tb_merge_destino", F.col("id") == F.col("id")).whenMatched().updateAll().merge()

# Bug 3: decisão de negócio usando os.getenv ignorada pelo BranchExplorer
if os.getenv("MODO") == "REPROC":
    df.write.saveAsTable("dev.tgt.tb_reprocessamento")
else:
    df.write.saveAsTable("dev.tgt.tb_normal")

# Bug 4: SELECT com WITH CTE contendo `) update` (alias após parêntese) bloqueado como INSERT
df_cte = spark.sql("""
    WITH base AS (
        SELECT id, CAST(id AS STRING) update
        FROM range(5)
    )
    SELECT id, update FROM base
""")
df_cte.write.saveAsTable("dev.tgt.tb_from_cte")
