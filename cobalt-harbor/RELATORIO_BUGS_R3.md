# Relatório de Bugs — Rodada 3 (`spark-lineage`)

Após a correção das Rodadas 1 e 2 ([`RELATORIO_BUGS.md`](RELATORIO_BUGS.md) e [`RELATORIO_BUGS_R2.md`](RELATORIO_BUGS_R2.md) — todos os 56 testes anteriores passando), esta terceira auditoria focou em **`cache` / `persist`**, **`spark.catalog.tableExists`**, ciclo de vida de sessão/views e streaming, identificando **11 novos bugs reais** em `capture_plans.py`, `plan_parser.py`, `sources.py` e `impact.py`, acompanhados de reprodutores em [`repro/`](repro/) e **17 testes automatizados** (`xfail(strict=True)`) em [`tests/test_known_bugs_r3.py`](tests/test_known_bugs_r3.py).

## Resumo Executivo

| # | Severidade | Categoria | Módulo(s) | Resumo do problema | Teste(s) em `tests/test_known_bugs_r3.py` |
|---|---|---|---|---|---|
| R3-1 | Crítica | Cache / Persist | `capture_plans.py` | `df.cache()`, `df.persist()`, `df.unpersist()`, `df.storageLevel` e `df.is_cached` não são interceptados -> crash imediato no Databricks Serverless e cache real no Classic | `test_r3_df_cache_persist_unpersist_intercepted_on_serverless`, `test_r3_df_storage_level_and_is_cached_safe_on_serverless` |
| R3-2 | Crítica (Segurança / Serverless) | Cache / Catalog | `capture_plans.py` | `spark.catalog.cacheTable()`, `uncacheTable()`, `clearCache()` e `isCached()` burlam `execute_command` (vão via `_execute_and_fetch`), materializando tabela no Classic ou dando crash no Serverless | `test_r3_catalog_cache_methods_are_intercepted` |
| R3-3 | Alta | SQL / Cache | `capture_plans.py` | `CACHE [LAZY] TABLE vw AS SELECT ...` retorna `('block', 'CACHE')` e não cria a temp view -> queries seguintes falham com `TABLE_OR_VIEW_NOT_FOUND` | `test_r3_sql_cache_table_as_select_creates_temp_view` |
| R3-4 | Alta | Cache / Parser / Impacto | `plan_parser.py`, `sources.py`, `impact.py` | Planos com `InMemoryRelation` / `InMemoryTableScan` produzem `sources: []` e `impact.found == False` mesmo quando o `FileScan`/`PhotonScan` está no plano físico | `test_r3_in_memory_relation_extracts_sources_and_impact` |
| R3-5 | Alta | Catalog (`tableExists`) | `capture_plans.py` | `spark.catalog.tableExists(...)`: (a) retorna `False` para staging gravada antes no mesmo job; (b) 2 args `(tb, db)` estoura `DEPRECATED_FEATURE_BLOCKED` no Serverless; (c) nome vazio/malformado de widget estoura `ParseException`; (d) sem cache entre passadas do `BranchExplorer` | `test_r3_table_exists_sees_previously_written_staging_table`, `test_r3_table_exists_two_args_normalized_for_serverless`, `test_r3_table_exists_malformed_name_from_empty_widget_does_not_crash`, `test_r3_table_exists_cached_across_branch_passes` |
| R3-6 | Crítica | Runtime | `capture_plans.py` | `spark.stop()` não é interceptado e encerra a sessão na passada 0, quebrando passadas do `BranchExplorer` e a leitura do Unity Catalog | `test_r3_spark_stop_does_not_kill_session_during_capture` |
| R3-7 | Alta | Runtime / Views | `capture_plans.py` | `df.createTempView(...)` e SQL `CREATE TEMP VIEW` (sem `OR REPLACE`) falham na passada 1 com `TEMP_TABLE_OR_VIEW_ALREADY_EXISTS` | `test_r3_create_temp_view_does_not_fail_on_branch_reexecution` |
| R3-8 | Alta | Exploração AST | `capture_plans.py` | Ramo que falha antes de ser alcançado nunca entra em `visited` -> `BranchExplorer` repete o mesmo ramo quebrado até esgotar `max_branch_passes` | `test_r3_branch_explorer_does_not_retry_unreachable_failed_branch` |
| R3-9 | Alta | Streaming | `capture_plans.py` | `df.writeStream.foreachBatch(fn).start()` descarta todas as escritas/merges dentro de `fn` (`destinations: []`) | `test_r3_write_stream_foreach_batch_captures_inner_writes` |
| R3-10 | Média | Exploração AST | `capture_plans.py` | Expressões ternárias (`a if cond else b` / `ast.IfExp`) e `match`/`case` (`ast.Match`) não são instrumentadas | `test_r3_branch_explorer_instruments_ternary_if_exp` |
| R3-11 | Média | Fontes | `sources.py` | Leituras de múltiplos paths sem nome (`spark.read.parquet("/mnt/a")` + `"/mnt/b"`) colidem em uma única entrada `<relação sem nome (parquet)>` | `test_r3_sources_keeps_distinct_unnamed_parquet_paths_separate` |

---

## Arquivos Reprodutores (Rodada 3)

1. [`repro/job_bugs_r3.py`](repro/job_bugs_r3.py) — reproduz `df.cache()`/`persist()`/`unpersist()`, `spark.catalog.cacheTable()`/`clearCache()`, `createTempView`, `tableExists` em tabela staging, ternário `ast.IfExp` e `spark.stop()`.
2. [`repro/pipeline_bugs_r3.sql`](repro/pipeline_bugs_r3.sql) — reproduz `CACHE TABLE vw AS SELECT ...` bloqueado e `CREATE TEMP VIEW` sem `OR REPLACE` falhando na reexecução de ramos.

Comandos de execução via CLI:

```bash
spark-lineage capture --project-root repro --entrypoint job_bugs_r3.py --no-catalog
spark-lineage capture --project-root repro --entrypoint pipeline_bugs_r3.sql --no-catalog
```

---

## Detalhamento Técnico

### Bloco `cache` / `persist`

#### R3-1 — `df.cache()`, `df.persist()`, `df.unpersist()`, `df.storageLevel` e `df.is_cached` não interceptados
- **Severidade:** Crítica
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1772–1792, `DF_ACTIONS`).
- **Esperado:** `cache()`, `persist()` e `unpersist()` interceptados como no-op devolvendo `df` (igual a `checkpoint`/`localCheckpoint`); `is_cached` devolvendo `False` e `storageLevel` devolvendo `StorageLevel.NONE`.
- **Obtido:**
  - No **Databricks Serverless**: crash imediato (`[NOT_SUPPORTED_WITH_SERVERLESS]`), encerrando a captura com `status="error"`.
  - Em **clusters Classic**: materializa/polui cache real no cluster e transforma os planos seguintes em `InMemoryRelation` (ver R3-4).
- **Direção de correção:** Adicionar `"cache"`, `"persist"` e `"unpersist"` em `DF_ACTIONS` retornando `df` (sem chamar `original`), e patchear as properties `is_cached` / `storageLevel` em `DF_CLASSES`.

#### R3-2 — `spark.catalog.cacheTable()`, `uncacheTable()`, `clearCache()` e `isCached()` burlam `execute_command`
- **Severidade:** Crítica (Segurança / Serverless)
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 2110–2112, `_patch_merge_and_catalog`).
- **Esperado:** Interceptados e registrados em `events` sem executar no cluster (retornando `None`, ou `False` para `isCached`).
- **Obtido:** No `databricks-connect` 16.4, esses métodos chamam `self._execute_and_fetch(plan.CacheTable(...))` (via `ExecutePlan`, fora de `execute_command`), furando `_patch_command_guard`: materializam a tabela inteira na memória no Classic ou dão crash imediato no Serverless.
- **Direção de correção:** Estender `_patch_merge_and_catalog` para patchear `cacheTable`, `uncacheTable`, `clearCache`, `isCached` (e também `refreshTable`, `refreshByPath`, `dropGlobalTempView`).

#### R3-3 — SQL `CACHE [LAZY] TABLE vw AS SELECT ...` bloqueado
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 462–491 e 1936–1953).
- **Reprodução:** [`repro/pipeline_bugs_r3.sql`](repro/pipeline_bugs_r3.sql)
  ```sql
  CACHE TABLE vw_cache_base AS
  SELECT id, CAST(id AS STRING) AS nome FROM range(5);

  INSERT INTO dev.tgt.tb_from_cache
  SELECT id, nome FROM vw_cache_base;
  ```
- **Esperado:** Cria a temp view `vw_cache_base` sem materializar cache, permitindo que o `INSERT` seguinte capture a linhagem completa.
- **Obtido:** `classify_sql` retorna `('block', 'CACHE')`; `vw_cache_base` nunca é criada e o `INSERT` falha com `TABLE_OR_VIEW_NOT_FOUND`.
- **Direção de correção:** Detectar `CACHE [LAZY] TABLE <nome> [OPTIONS (...)] AS <SELECT>` e reescrever/executar como `CREATE OR REPLACE TEMP VIEW <nome> AS <SELECT>` durante a captura (e bloquear `CACHE TABLE <nome>` sem `AS`, registrando evento).

#### R3-4 — Planos com `InMemoryRelation` / `InMemoryTableScan` perdem fontes e impacto
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/plan_parser.py` (linha 30), `src/spark_lineage/sources.py` (linhas 227–239) e `src/spark_lineage/impact.py` (linhas 213–259).
- **Esperado:** Mesmo se um `plan.json` contiver `InMemoryRelation` / `InMemoryTableScan` (ex.: plano capturado em cluster Classic), a tabela subjacente (`FileScan`/`PhotonScan` aninhado) deve aparecer em `sources` e ser rastreada em `impact.py`.
- **Obtido:** `build_sources()` devolve `sources: []` com aviso `Nós de leitura não interpretados: InMemoryRelation x1`, e `trace_column_impact()` devolve `found: False`.
- **Direção de correção:** Além de evitar `InMemoryRelation` na captura (R3-1), fazer fallback para scans em `sources.py` quando `InMemoryRelation` estiver presente e adicionar suporte em `plan_parser.py`/`impact.py` para descer até o scan/relation interno de `InMemoryRelation`.

---

### `spark.catalog.tableExists`

#### R3-5 — 4 falhas com `spark.catalog.tableExists(...)`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (sem wrapper em `CATALOG_CLASSES` para `tableExists`).
- **Problemas:**
  - **R3-5a (Staging no mesmo job):** `df.write.saveAsTable("dev.stg.x")` é interceptado e não cria a tabela; logo depois `spark.catalog.tableExists("dev.stg.x")` devolve `False`, e na passada forçada `spark.table("dev.stg.x")` falha com `TABLE_OR_VIEW_NOT_FOUND`.
  - **R3-5b (Assinatura legada de 2 argumentos):** `spark.catalog.tableExists("tb_banco", "dev.cadastros")` falha com `DEPRECATED_FEATURE_BLOCKED` em ambientes Databricks Connect/Serverless com bloqueio de APIs depreciadas.
  - **R3-5c (Nome malformado / catálogo sem permissão):** Widget ausente (`f"{cat}.tgt.tb"` -> `".tgt.tb"`) ou catálogo sem `USE CATALOG` lança `ParseException`/`PERMISSION_DENIED` e derruba toda a captura (`status="error"`).
  - **R3-5d (RPCs repetidos):** Sem cache no `Capturer`, cada chamada a `tableExists` dispara um `ExecutePlan` remoto em **todas** as passadas do `BranchExplorer`.
- **Direção de correção:** Envolver `Catalog.tableExists` para: (1) normalizar `(tableName, dbName)` em `f"{dbName}.{tableName}"`; (2) consultar um mapa em memória de tabelas gravadas durante a captura (opcionalmente registradas como temp view ou interceptadas em `spark.table`); (3) capturar `ParseException`/`PERMISSION_DENIED` devolvendo `False` com aviso; e (4) cachear o resultado por nome de tabela durante a sessão de captura.

---

### Runtime, `BranchExplorer`, Streaming e Fontes

#### R3-6 — `spark.stop()` encerra a sessão durante a captura
- **Severidade:** Crítica
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1982–1988, `_patch_sql_and_readers`).
- **Esperado:** `spark.stop()` interceptado como no-op (registrando evento se desejado) durante a captura.
- **Obtido:** Na passada 0, `spark.stop()` mata a sessão -> todas as passadas do `BranchExplorer` quebram com `SparkSession is stopped` e `_maybe_fetch_catalog` falha para todas as tabelas (`catalog: {t: None}`).
- **Direção de correção:** Patchear `stop` em `SESSION_CLASSES` como no-op durante a captura.

#### R3-7 — `df.createTempView(...)` e SQL `CREATE TEMP VIEW` (sem `OR REPLACE`) quebram na passada 1
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1772–1792 e 1936–1942).
- **Esperado:** Reexecuções do `BranchExplorer` conseguem recriar temp views sem erro.
- **Obtido:** Na passada 1, `df.createTempView("vw")` e `CREATE TEMP VIEW vw AS ...` falham com `TEMP_TABLE_OR_VIEW_ALREADY_EXISTS`, bloqueando a exploração dos ramos abaixo.
- **Direção de correção:** Redirecionar `createTempView` -> `createOrReplaceTempView` (e `CREATE TEMP VIEW` -> `CREATE OR REPLACE TEMP VIEW`) durante a captura.

#### R3-8 — `BranchExplorer` repete ramo inalcançável até esgotar `max_branch_passes`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1200–1220, `mark_target_status` / `next_unvisited`).
- **Esperado:** Se uma passada forçada lançar erro antes de atingir `target_site`, o par `(target_site, target_value)` deve ser marcado como já tentado para que o explorador siga para os demais ramos válidos.
- **Obtido:** `target_value` nunca entra em `info["visited"]`, fazendo `next_unvisited()` selecionar o **mesmo ramo quebrado** repetidamente até estourar `max_branch_passes`.
- **Direção de correção:** Adicionar `self.target_value` a `info["visited"]` dentro de `mark_target_status()` (quando `self.target_value is not None`).

#### R3-9 — `df.writeStream.foreachBatch(fn).start()` perde todas as escritas internas
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1839–1875).
- **Esperado:** Ao interceptar `writeStream.start()`, caso haja callback registrado via `foreachBatch(fn)`, invocar `fn(df, 0)` para capturar os `write`/`merge` dentro do callback.
- **Obtido:** `fn` nunca é chamada e `destinations` fica `[]`.
- **Direção de correção:** Patchear `foreachBatch` em `STREAM_WRITER_CLASSES` para guardar `func` no writer (sem serializar com cloudpickle) e chamá-la com `(df, 0)` dentro do wrapper de `start()`.

#### R3-10 — Expressões ternárias (`ast.IfExp`) e `match`/`case` (`ast.Match`) não são instrumentadas
- **Severidade:** Média
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 990–1020, `_BranchTransformer`).
- **Esperado:** `destino = "dev.tgt.a" if modo == "REPROC" else "dev.tgt.b"` ter ambos os ramos explorados.
- **Obtido:** `_BranchTransformer` só visita `ast.If`, ignorando `ast.IfExp` e `ast.Match`.
- **Direção de correção:** Implementar `visit_IfExp` em `_BranchTransformer` envolvendo `node.test` com `__spark_lineage_branch__` (com os mesmos filtros de `_should_instrument_if`).

#### R3-11 — Múltiplos arquivos lidos por path colidem em `<relação sem nome (parquet)>`
- **Severidade:** Média
- **Onde:** `src/spark_lineage/sources.py` (linhas 214–225).
- **Esperado:** Duas leituras distintas (`spark.read.parquet("/mnt/dados/clientes")` e `spark.read.parquet("/mnt/dados/pedidos")`) representadas como 2 fontes distintas.
- **Obtido:** Ambas colapsam em uma única entrada `<relação sem nome (parquet)>`, perdendo os paths de `read_sites` e misturando colunas/filtros.
- **Direção de correção:** Extrair o caminho de `Location:` do plano físico/formatted (ou diferenciar relações sem nome por índice/assinatura de colunas e associar aos `read_sites`).

---

## Como Rodar os Testes (Rodada 3)

```bash
# Roda toda a suíte (56 passed + 17 xfailed, exit code 0)
pytest -v

# Força a exibição das 17 falhas reais da Rodada 3
pytest tests/test_known_bugs_r3.py --runxfail
```
