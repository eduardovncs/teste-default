# Relatório de Bugs — Rodada 4 (`spark-lineage`)

Após a correção das Rodadas 1, 2 e 3 ([`RELATORIO_BUGS.md`](RELATORIO_BUGS.md), [`RELATORIO_BUGS_R2.md`](RELATORIO_BUGS_R2.md) e [`RELATORIO_BUGS_R3.md`](RELATORIO_BUGS_R3.md) — todos os 73 testes anteriores passando), esta quarta auditoria aprofundada percorreu todos os módulos (`capture_plans.py`, `plan_parser.py`, `sources.py`, `impact.py` e `cli.py`) e identificou **21 novos bugs reais**, acompanhados de 3 reprodutores em [`repro/`](repro/) e **31 testes automatizados** (`xfail(strict=True)`) em [`tests/test_known_bugs_r4.py`](tests/test_known_bugs_r4.py).

## Resumo Executivo

| # | Severidade | Categoria | Módulo(s) | Resumo do problema | Teste(s) em `tests/test_known_bugs_r4.py` |
|---|---|---|---|---|---|
| R4-1 | Crítica | Spark Connect | `capture_plans.py` | `record_written_table` e `writeStream.foreachBatch` usam `LogicalPlan` cru sem `_as_dataframe` -> `AttributeError` no Databricks Connect real | `test_r4_record_written_table_wraps_connect_logical_plan_as_dataframe`, `test_r4_foreach_batch_receives_dataframe_not_logical_plan` |
| R4-2 | Alta (Regressão R3) | SQL | `capture_plans.py` | `CREATE TEMP VIEW IF NOT EXISTS` é reescrito para `CREATE OR REPLACE TEMP VIEW IF NOT EXISTS` (erro de sintaxe no Spark SQL) | `test_r4_create_temp_view_if_not_exists_not_rewritten_to_invalid_sql` |
| R4-3 | Alta | SQL / Cache | `capture_plans.py` | `CACHE TABLE vw SELECT ...` (sem `AS`, sintaxe oficial do Spark SQL) ainda é bloqueado como `('block', 'CACHE')` | `test_r4_cache_table_without_as_keyword_is_recognized` |
| R4-4 | Alta (Regressão R3) | Exploração AST | `capture_plans.py` | Ternários encadeados/múltiplos na mesma linha colidem em `site_id` (sem `col_offset`) e perdem ramos intermediários | `test_r4_chained_ternary_on_same_line_explores_all_branches` |
| R4-5 | Alta (Regressão R3) | Fontes | `sources.py` | Dois paths sem nome com mesmo schema colidem em `_unnamed_sig_map`; e `scans_by_table.get(None)` mistura colunas entre arquivos diferentes | `test_r4_sources_distinct_unnamed_paths_with_same_schema`, `test_r4_sources_unnamed_scans_do_not_mix_columns_across_files` |
| R4-6 | Alta | SQL | `capture_plans.py` | `extract_sql_targets` extrai `'DIRECTORY'` em `INSERT OVERWRITE DIRECTORY` e retorna `[]` em `MERGE WITH SCHEMA EVOLUTION INTO`, `DELETE <tb>` (sem `FROM`) e `CREATE OR REFRESH STREAMING TABLE` | `test_r4_insert_overwrite_directory_does_not_extract_keyword_directory`, `test_r4_merge_with_schema_evolution_extracts_target`, `test_r4_delete_without_from_extracts_target`, `test_r4_create_or_refresh_streaming_table_extracts_target` |
| R4-7 | Alta | Parser / Fontes / Impacto | `plan_parser.py` | `_parse_attr` mantém crases nos nomes de coluna (`` `cod-banco` ``), duplicando colunas no catálogo e quebrando `trace_column_impact` | `test_r4_plan_parser_strips_backticks_from_column_names` |
| R4-8 | Alta | Impacto / Cache | `impact.py` | `*(1) FileScan` (WholeStageCodegen) dentro de `InMemoryRelation` recebe `op='*(1)'` em `_node_from_line` e falha no `impact` (`found=False`) | `test_r4_impact_tracks_codegen_filescan_inside_in_memory_relation` |
| R4-9 | Alta | Impacto | `impact.py` | `Join LeftAnti` e `Join LeftSemi` concatenam ambos os lados e emitem falsamente colunas do lado direito em `output_columns_affected` | `test_r4_impact_left_anti_join_does_not_output_right_columns` |
| R4-10 | Alta | Impacto | `impact.py` | Operador `Expand` (`ROLLUP`, `CUBE`, `GROUPING SETS`, `UNPIVOT`, múltiplos `COUNT(DISTINCT)`) remapeia IDs e zera `output_columns_affected` | `test_r4_impact_tracks_column_through_expand_rollup` |
| R4-11 | Média | Captura `.py` | `capture_plans.py` | `Capturer.pandas()` devolve o mesmo `DataFrame` pandas mutável do cache sem `.copy()`, vazando mutações entre passadas | `test_r4_capturer_pandas_cache_returns_copy` |
| R4-12 | Média | CLI | `cli.py`, `capture_plans.py` | `--output sub/dir/arquivo.json` quebra com `FileNotFoundError` se o diretório pai não existir | `test_r4_cli_output_creates_missing_parent_directory` |
| R4-13 | Alta | Notebook `.ipynb` | `capture_plans.py` | `%pip` / `%run` indentado dentro de bloco `if`/`def` em `.ipynb` perde indentação e gera `IndentationError` | `test_r4_ipynb_indented_line_magic_preserves_indentation` |
| R4-14 | Alta | Notebook `.ipynb` | `capture_plans.py` | `%time` em linha única (`%time df.write.saveAsTable(...)`) é silenciosamente apagado para `#` | `test_r4_ipynb_single_line_percent_time_executes_inline_code` |
| R4-15 | Alta | Notebook `.ipynb` | `capture_plans.py` | Comentário Python (`# ...`) antes de `%sql` em célula `.ipynb` faz o SQL ficar como Python cru (`SyntaxError`) | `test_r4_ipynb_comment_before_percent_sql_compiles` |
| R4-16 | Média | Widgets | `capture_plans.py` | `dbutils.widgets.remove()` e `removeAll()` são no-ops e não limpam defaults registrados | `test_r4_dbutils_widgets_remove_clears_default` |
| R4-17 | Média | Captura `.py` | `capture_plans.py` | `df.head(None)` devolve `list` em vez de uma única `Row` (divergindo do PySpark) | `test_r4_df_head_none_returns_single_row` |
| R4-18 | Alta | Parser de planos | `plan_parser.py` | `parse_formatted_scans` falha em `Scan JDBCRelation(...) [numPartitions=1]`; `parse_read_schema` mantém crases em `` struct<`cod-banco`:int> `` | `test_r4_plan_parser_formatted_jdbc_scan`, `test_r4_plan_parser_read_schema_strips_backticks` |
| R4-19 | Média | Parser de planos | `plan_parser.py` | Parêntese desbalanceado dentro de literal de string em `DataFilters` (ex.: `'erro (1'`) faz `_metadata_value` engolir `Format:` e `ReadSchema:` | `test_r4_plan_parser_filters_with_unbalanced_paren_in_string` |
| R4-20 | Alta | Impacto | `impact.py` | `Join ExistenceJoin(exists#99)` falha em `_JOIN_RE` (perde join); `Except`/`Intersect` usa `children[-1]` em `_passthrough` e descarta colunas da esquerda | `test_r4_impact_handles_existence_join`, `test_r4_impact_except_preserves_left_output_columns` |
| R4-21 | Alta | Fontes / Catálogo | `sources.py` | Lookup no `catalog` em `enrich_destinations` e `source_list` é case-sensitive (SQL em maiúsculas `DEV.TGT.TB_SAIDA` / `ID_BANCO` perde tipos e comentários) | `test_r4_sources_catalog_enrichment_is_case_insensitive` |

---

## Arquivos Reprodutores (Rodada 4)

1. [`repro/job_bugs_r4.py`](repro/job_bugs_r4.py) — reproduz ternário encadeado na mesma linha (**R4-4**), `dbutils.widgets.remove` (**R4-16**) e `df.head(None)` (**R4-17**).
2. [`repro/pipeline_bugs_r4.sql`](repro/pipeline_bugs_r4.sql) — reproduz `CREATE TEMP VIEW IF NOT EXISTS` (**R4-2**), `CACHE TABLE` sem `AS` (**R4-3**) e os 4 casos de `extract_sql_targets` (**R4-6**).
3. [`repro/notebook_bugs_r4.ipynb`](repro/notebook_bugs_r4.ipynb) — reproduz `%time` de 1 linha (**R4-14**), `%pip` indentado em `if` (**R4-13**) e comentário antes de `%sql` (**R4-15**).

Comandos de execução via CLI:

```bash
spark-lineage capture --project-root repro --entrypoint job_bugs_r4.py --no-catalog
spark-lineage capture --project-root repro --entrypoint pipeline_bugs_r4.sql --no-catalog
spark-lineage capture --project-root repro --entrypoint notebook_bugs_r4.ipynb --no-catalog
```

---

## Detalhamento Técnico

### Spark Connect / Captura `.py`

#### R4-1 — `record_written_table` e `writeStream.foreachBatch` usam `LogicalPlan` cru no Spark Connect
- **Severidade:** Crítica
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1942–1964, `_make_writer_factory`).
- **Problema:** No `databricks-connect` 16.4, `_writer_source(writer)` retorna um `LogicalPlan` (não um `DataFrame`).
  - Na linha 1952, `cap.record_written_table(target, df)` armazena o `LogicalPlan` cru -> qualquer `spark.table(target)` seguinte devolve `LogicalPlan` sem `.filter()` / `.write` (`AttributeError`).
  - Na linha 1961, `fb(df, 0)` em `foreachBatch` também entrega o `LogicalPlan` cru ao callback -> `batch_df.write.saveAsTable(...)` falha com `AttributeError` no Databricks Connect real.
- **Direção de correção:** Converter `df_obj = _as_dataframe(df, writer)` (com `try/except`) antes de `record_written_table` e antes de chamar `fb(df_obj, 0)`.

#### R4-4 — Ternários encadeados/múltiplos na mesma linha colidem em `site_id`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1051–1067, `_BranchTransformer.visit_IfExp`).
- **Reprodução:** [`repro/job_bugs_r4.py`](repro/job_bugs_r4.py)
  ```python
  destino = "dev.tgt.tb_a" if modo == "A" else ("dev.tgt.tb_b" if modo == "B" else "dev.tgt.tb_c")
  ```
- **Esperado:** Explorar os 3 destinos (`tb_a`, `tb_b`, `tb_c`).
- **Obtido:** Ambos os `IfExp` recebem `site_id = "job_bugs_r4.py:29"` -> `tb_b` nunca é explorado.
- **Direção de correção:** Incluir `col_offset` no `site_id` de `visit_IfExp` (ex.: `f"{self.rel_file}:{node.lineno}:{node.col_offset}"`).

#### R4-11 — `Capturer.pandas()` devolve referência mutável do cache
- **Severidade:** Média
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1573–1585).
- **Problema:** Se a passada 0 adiciona/remove colunas in-place no `pandas.DataFrame`, passadas seguintes recebem o objeto já mutado.
- **Direção de correção:** Retornar `self._pandas_cache[ck].copy()` (e armazenar `res.copy()`).

#### R4-16 — `dbutils.widgets.remove()` e `removeAll()` não removem defaults
- **Severidade:** Média
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 190–244, `_Widgets`).
- **Problema:** Caem no `__getattr__` genérico (no-op); após `dbutils.widgets.remove("modo")`, `get("modo")` ainda devolve o default anterior.
- **Direção de correção:** Implementar `remove(name)` (`self._defaults.pop(name, None)`) e `removeAll()` (`self._defaults.clear()`), além de restaurar `_defaults` entre passadas do `BranchExplorer`.

#### R4-17 — `df.head(None)` devolve `list` em vez de `Row`
- **Severidade:** Média
- **Onde:** `src/spark_lineage/capture_plans.py` (linha 1825, `DF_ACTIONS["head"]`).
- **Problema:** Quando `n=None` é passado explicitamente (`df.head(None)`), `not a` é `False` e `c.rows(df)[:None]` retorna `[Row(...)]` em vez de `Row(...)`.
- **Direção de correção:** Tratar `n = a[0] if a else k.get("n")` e retornar `c.first_row(df)` sempre que `n is None`.

---

### SQL e Reescrita de Views

#### R4-2 — `CREATE TEMP VIEW IF NOT EXISTS` reescrito para SQL inválido
- **Severidade:** Alta (Regressão R3)
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 493–496 e 2059–2060).
- **Reprodução:** [`repro/pipeline_bugs_r4.sql`](repro/pipeline_bugs_r4.sql)
- **Problema:** `CREATE TEMP VIEW IF NOT EXISTS vw_x AS ...` vira `CREATE OR REPLACE TEMP VIEW IF NOT EXISTS vw_x AS ...`, rejeitado pelo Spark SQL (`CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed`).
- **Direção de correção:** Remover `IF NOT EXISTS` ao inserir `OR REPLACE` (ou não inserir `OR REPLACE` quando `IF NOT EXISTS` já está presente — preferindo `CREATE OR REPLACE TEMP VIEW <nome>` sem `IF NOT EXISTS`).

#### R4-3 — `CACHE TABLE vw SELECT ...` (sem `AS`) ainda bloqueado
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 488–492, `_CACHE_TABLE_AS_RE`).
- **Problema:** No Spark SQL, `AS` é opcional em `CACHE [LAZY] TABLE vw [OPTIONS (...)] [AS] SELECT ...`, mas a regex exige `AS\s+`.
- **Direção de correção:** Tornar `AS` opcional antes de `(?:SELECT|WITH|VALUES|TABLE|FROM|\()\b`.

#### R4-6 — 4 sintaxes Spark/Databricks SQL com falha em `extract_sql_targets`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 318–330, `_SQL_TARGET_RE`).
- **Problemas:**
  - `INSERT OVERWRITE DIRECTORY '/mnt/out/r4' ...` -> extrai `['DIRECTORY']` em vez de `['/mnt/out/r4']`.
  - `MERGE WITH SCHEMA EVOLUTION INTO dev.tgt.tb ...` -> retorna `[]`.
  - `DELETE dev.tgt.tb WHERE id = 1` (sem `FROM`) -> retorna `[]`.
  - `CREATE OR REFRESH STREAMING TABLE dev.tgt.tb ...` -> retorna `[]`.
- **Direção de correção:** Suportar `(?:LOCAL\s+)?DIRECTORY\s+('[^']+'|"[^"]+")`, `MERGE(?:\s+WITH\s+SCHEMA\s+EVOLUTION)?\s+INTO`, `DELETE(?:\s+FROM)?` e `(?:OR\s+(?:REPLACE|REFRESH))?` + `STREAMING` em `_SQL_TARGET_RE`.

---

### Notebooks `.ipynb`

#### R4-13 — `%pip` / `%run` indentado em `.ipynb` gera `IndentationError`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linha 947, `_preprocess_ipynb_source`).
- **Problema:** Usa `f"# MAGIC {ln}"` em vez de `f"{indent}# MAGIC {stripped}"`, perdendo a indentação dentro de blocos `if`/`def`.

#### R4-14 — `%time` em linha única é apagado para `#`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 799–803, `_process_magic_block`).
- **Problema:** `_TRANSPARENT_PY_MAGICS` só restaura linhas com `idx > first_idx`, descartando o código inline de `%time df.write.saveAsTable(...)`.

#### R4-15 — Comentário antes de `%sql` em `.ipynb` gera `SyntaxError`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 932–937, `_preprocess_ipynb_source`).
- **Problema:** `first` pega `# Consulta de vendas` -> `is_block_magic=False` -> `%sql` vira magic isolado e `SELECT ...` fica como código Python cru.

---

### `sources.py`, `plan_parser.py`, `impact.py` e `cli.py`

#### R4-5 — Paths sem nome com mesmo schema colidem e scans `None` misturam colunas
- **Severidade:** Alta (Regressão R3)
- **Onde:** `src/spark_lineage/sources.py` (linhas 194–205 e 265).
- **Problemas:**
  - Dois `spark.read.parquet` com mesmo schema (`/mnt/dados/2025` e `/mnt/dados/2026`) geram a mesma chave `sig` em `_unnamed_sig_map` -> o 2º path some.
  - `scans_by_table.get(None, [])` aplica **todos** os scans sem nome a **todas** as relações sem nome, contaminando colunas e filtros entre arquivos distintos.
- **Direção de correção:** Casar relações sem nome com seus respectivos scans por posição ou conjunto de Expression IDs (`c["id"]`), e diferenciar relações sem nome por seus Expression IDs em vez de apenas `(format, nomes_colunas)`.

#### R4-21 — Lookup case-sensitive no `catalog` em `sources.py`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/sources.py` (linhas 310, 317 e 322).
- **Problema:** `DEV.TGT.TB_SAIDA` / `ID_BANCO` (SQL em maiúsculas) não encontra `"dev.tgt.tb_saida"` / `"id_banco"` no `catalog`, perdendo tipos e comentários.
- **Direção de correção:** Indexar lookup de tabela e coluna no `catalog` em minúsculas (case-insensitive).

#### R4-7 & R4-18 — Crases em colunas (`_parse_attr`, `parse_read_schema`) e `Scan JDBCRelation`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/plan_parser.py` (linhas 40–42, 120–125 e 333).
- **Problemas:**
  - `_parse_attr` e `parse_read_schema` preservam crases em `` `cod-banco` ``, causando duplicação em `sources.py` e `found=False` em `impact.py`.
  - `_FMT_SCAN_HDR_RE` rejeita `(1) Scan JDBCRelation(public.tb_vendas) [numPartitions=1]`.
- **Direção de correção:** Remover crases (`strip("`")`) no nome em `_parse_attr` e `parse_read_schema`; aceitar sufixo opcional `\[[^\]]+\]` em `_FMT_SCAN_HDR_RE`.

#### R4-19 — Parêntese desbalanceado em literal de string dentro de `DataFilters`
- **Severidade:** Média
- **Onde:** `src/spark_lineage/plan_parser.py` (linhas 273–290, `_is_balanced` / `_metadata_value`).
- **Problema:** `DataFilters: [(obs#2 = 'erro (1')], Format: parquet, ReadSchema: ...` faz `_is_balanced` retornar `False` e engole `Format:` e `ReadSchema:` na lista de filtros.
- **Direção de correção:** Ignorar caracteres dentro de `'...'` em `_is_balanced` e `split_top_level`.

#### R4-8, R4-9, R4-10 & R4-20 — 5 lacunas de operadores em `impact.py`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/impact.py` (linhas 30, 125–134, 217–234, 314–324 e 433–435).
- **Problemas:**
  - **R4-8 (`*(1) FileScan`):** `_node_from_line` define `op = "*(1)"`, impedindo o reconhecimento de scans WholeStageCodegen dentro de `InMemoryRelation`.
  - **R4-9 (`LeftAnti` / `LeftSemi`):** `_join` inclui colunas do lado direito na saída, gerando falsos impactos downstream.
  - **R4-10 (`Expand`):** `ROLLUP`/`CUBE`/`GROUPING SETS`/`UNPIVOT` remapeiam Expression IDs sem handler em `_ColumnTracer`.
  - **R4-20a (`ExistenceJoin`):** `_JOIN_RE` exige `jtype` `\w+` e ignora `Join ExistenceJoin(exists#99), (...)`.
  - **R4-20b (`Except` / `Intersect`):** caem em `_passthrough` (que usa `children[-1]`), descartando colunas da tabela esquerda (`children[0]`).
- **Direção de correção:** Remover prefixo `*(N)` em `_node_from_line`; em `_join`, emitir apenas `children[0]` para `LeftSemi`/`LeftAnti` e aceitar `ExistenceJoin(...)` em `_JOIN_RE`; adicionar handlers para `Expand` e `Except`/`Intersect`.

#### R4-12 — `--output sub/dir/arquivo.json` falha sem criar diretório pai
- **Severidade:** Média
- **Onde:** `src/spark_lineage/cli.py` (linha 68) e `src/spark_lineage/capture_plans.py` (linha 2690).
- **Direção de correção:** Chamar `Path(output_path).parent.mkdir(parents=True, exist_ok=True)` antes de `write_text(...)`.

---

## Como Rodar os Testes (Rodada 4)

```bash
# Roda toda a suíte (73 passed + 31 xfailed, exit code 0)
pytest -v

# Força a exibição das 31 falhas reais da Rodada 4
pytest tests/test_known_bugs_r4.py --runxfail
```
