"""Reprodutor de bugs (Rodada 3) no spark-lineage: cache/persist, tableExists, createTempView e spark.stop().

Padrões comuns em jobs PySpark/Databricks que falham durante `spark-lineage capture`:

1. `df.cache()` / `df.persist()` / `df.unpersist()`:
   - Esperado: interceptados sem efeito colateral (devolvendo `df`), permitindo capturar
     jobs que usam cache mesmo em Databricks Serverless e preservando `Relation` nos planos.
   - Atual: não estão em `DF_ACTIONS` — quebram no Serverless (`[NOT_SUPPORTED_WITH_SERVERLESS]`)
     e materializam `InMemoryRelation` no Classic (fazendo `sources` ficar vazio).

2. `spark.catalog.cacheTable(...)` / `clearCache()`:
   - Esperado: bloqueados/interceptados como no-op (sem materializar tabela nem quebrar no Serverless).
   - Atual: passam direto via `_execute_and_fetch` (fora da trava `execute_command`).

3. `spark.catalog.tableExists(...)`:
   - Esperado: reconhecer tabelas staging gravadas antes no mesmo job, aceitar 2 argumentos
     `tableExists(tb, db)` e não derrubar a captura se um widget não foi passado (`".tgt.tb"`).
   - Atual: consulta o catálogo real -> retorna `False` para staging interceptada, estoura
     `DEPRECATED_FEATURE_BLOCKED` com 2 args no Serverless e `ParseException` com nome vazio.

4. `df.createTempView("vw_bancos")` + `BranchExplorer`:
   - Esperado: passada 1 do `BranchExplorer` consegue explorar o ramo alternativo.
   - Atual: `createTempView` (sem `OR REPLACE`) estoura `TEMP_TABLE_OR_VIEW_ALREADY_EXISTS` na passada 1
     e o `BranchExplorer` repete o mesmo ramo quebrado até esgotar `max_branch_passes`.

5. Ternário `destino = ... if cond else ...` (`ast.IfExp`):
   - Esperado: ambos os destinos (`tb_tern_reproc` e `tb_tern_normal`) descobertos.
   - Atual: `_BranchTransformer` ignora `ast.IfExp`.

6. `spark.stop()` ao final do job:
   - Esperado: interceptado como no-op durante a captura.
   - Atual: encerra a sessão na passada 0, quebrando todas as passadas extras e a leitura do catálogo.
"""

df = spark.range(5).cache()
df = df.persist()

# Bug R3-2: Catalog.cacheTable / clearCache não interceptados
spark.catalog.cacheTable("dev.cadastros.tb_banco")

# Bug R3-5: createTempView (sem OR REPLACE) falha na passada 1 do BranchExplorer
df.createTempView("vw_bancos_r3")

# Bug R3-4: tabela staging gravada antes + tableExists
df.write.mode("overwrite").saveAsTable("dev.stg.tb_staging_r3")
if spark.catalog.tableExists("dev.stg.tb_staging_r3"):
    spark.table("dev.stg.tb_staging_r3").write.saveAsTable("dev.tgt.tb_pos_staging")

# Bug R3-9: expressão ternária não instrumentada
modo = dbutils.widgets.get("modo") or "NORMAL"
destino_tern = "dev.tgt.tb_tern_reproc" if modo == "REPROC" else "dev.tgt.tb_tern_normal"
df.write.saveAsTable(destino_tern)

# Ramo if/else comum para exercitar reexecução do BranchExplorer
if df.count() == 0:
    df.write.saveAsTable("dev.tgt.tb_ramo_true")
else:
    df.write.saveAsTable("dev.tgt.tb_ramo_false")

df.unpersist()
spark.catalog.clearCache()

# Bug R3-6: spark.stop() mata a sessão antes das passadas do BranchExplorer e do Unity Catalog
spark.stop()
