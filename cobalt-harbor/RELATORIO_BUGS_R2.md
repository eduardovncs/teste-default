# Relatório de Bugs — Rodada 2 (`spark-lineage`)

Após a correção dos 15 bugs da Rodada 1 ([`RELATORIO_BUGS.md`](RELATORIO_BUGS.md) — todos os 35 testes anteriores passando), esta segunda auditoria identificou **13 novos bugs reais** em `capture_plans.py`, `sources.py`, `plan_parser.py` e `impact.py`, acompanhados de arquivos reprodutores em [`repro/`](repro/) e **21 testes automatizados** (`xfail(strict=True)`) em [`tests/test_known_bugs_r2.py`](tests/test_known_bugs_r2.py).

## Resumo Executivo

| # | Severidade | Categoria | Módulo(s) | Resumo do problema | Teste(s) em `tests/test_known_bugs_r2.py` |
|---|---|---|---|---|---|
| R2-1 | Crítica (Segurança) | SQL | `capture_plans.py` | Aspas duplas (`"..."`) no Spark SQL não são mascaradas em `_strip_sql_comments` e `_top_level_sql` não respeita aspas -> fontes fantasmas e bypass de DML em `FROM ... INSERT INTO` | `test_r2_sql_double_quoted_string_does_not_leak_fake_sources`, `test_r2_sql_double_quoted_paren_does_not_bypass_from_insert` |
| R2-2 | Alta | Streaming | `capture_plans.py` | `DataStreamWriter` (Spark Connect) guarda opções em `_write_proto.options` (protobuf map, não `dict`) -> `writeStream.option("path", ...).start()` perde o destino | `test_r2_write_stream_option_path_captures_destination` |
| R2-3 | Alta | Captura `.py` | `capture_plans.py` | `df.write.jdbc(url, table)` (e `read.jdbc`) captura a URL JDBC (`a[0]`) como tabela de destino/origem em vez do nome da tabela | `test_r2_write_jdbc_captures_table_instead_of_url` |
| R2-4 | Alta | SQL | `capture_plans.py` | Multi-table insert (`FROM ... INSERT INTO tgt1 ... INSERT INTO tgt2 ...`) só extrai o 1º destino | `test_r2_sql_multi_table_insert_captures_all_destinations` |
| R2-5 | Alta | Fontes/Destinos | `capture_plans.py`, `sources.py` | `saveAsTable("`dev`.`tgt`.`tb_crase`")` mantém crases internas e `sources._match_table` usa `.strip("`")` em vez de `.replace("`", "")` -> consultas por `--table` retornam `[]` | `test_r2_backticked_save_as_table_matches_in_sources_query` |
| R2-6 | Alta | Notebooks `%run` | `capture_plans.py` | `exec_magic_run` usa `dbutils.widgets._p.setdefault` sem restaurar ao final -> 2º `%run` com mesmo `$param` recebe o valor do 1º | `test_r2_magic_run_does_not_leak_params_between_notebooks` |
| R2-7 | Alta | Notebook `.ipynb` | `capture_plans.py` | `%sh` multilinha gera `SyntaxError`, `!pip` gera `SyntaxError`, e `%%time` apaga todo o código Python da célula para `#` | `test_r2_ipynb_multiline_percent_sh_does_not_crash`, `test_r2_ipynb_bang_shell_command_does_not_crash`, `test_r2_ipynb_cell_magic_time_preserves_python_body` |
| R2-8 | Alta | Exploração AST | `capture_plans.py` | `BranchExplorer` entra em loop infinito em `while True` com `break` condicional; `"path"` em `_SKIP_ATTRS` silencia `if cfg.path...` | `test_r2_branch_explorer_does_not_infinite_loop_on_while_break`, `test_r2_branch_explorer_instruments_user_object_path_attribute` |
| R2-9 | Alta | Parser de planos | `plan_parser.py` | Nós `RelationV2` (DataSource V2 / Iceberg / Delta V2) são ignorados sem aviso (`parse_relations` retorna `[]`) | `test_r2_plan_parser_extracts_relation_v2` |
| R2-10 | Alta | Parser de planos | `plan_parser.py` | `parse_merge_targets` exige `SubqueryAlias \w+` e falha em `MERGE INTO` sem alias curto (com pontos no nome) | `test_r2_plan_parser_merge_targets_with_qualified_subquery_alias` |
| R2-11 | Alta | Parser de planos | `plan_parser.py` | `parse_formatted_scans` falha em `[codegen id : N]` (Spark clássico) e troca `table` por `format` em `BatchScan dev.sch.pix` | `test_r2_plan_parser_formatted_scan_with_codegen_id`, `test_r2_plan_parser_formatted_batch_scan_extracts_table` |
| R2-12 | Alta | Impacto | `impact.py` | Colunas com acento (`descrição`) e aliases com espaço (`` `descrição limpa` ``) quebram `_ATTR_ID_RE` (ASCII-only) e `_ALIAS_RE` | `test_r2_impact_handles_accented_columns_and_spaced_aliases` |
| R2-13 | Alta | Impacto | `impact.py` | Operador `Generate` (`explode`/`LATERAL VIEW`) não propaga linhagem; `_trace_single_plan` faz `_match_table` unidirecional em `DELETE` não qualificado | `test_r2_impact_tracks_column_through_generate_explode`, `test_r2_impact_ignores_unqualified_in_place_delete` |

---

## Arquivos Reprodutores (Rodada 2)

1. [`repro/job_bugs_r2.py`](repro/job_bugs_r2.py) — reproduz os bugs **R2-3, R2-5 e R2-8** em um job `.py`.
2. [`repro/pipeline_bugs_r2.sql`](repro/pipeline_bugs_r2.sql) — reproduz os bugs **R2-1 e R2-4** em um arquivo `.sql`.
3. [`repro/notebook_bugs_r2.ipynb`](repro/notebook_bugs_r2.ipynb) — reproduz os bugs **R2-7a, R2-7b e R2-7c** em um notebook `.ipynb`.
4. [`repro/run_leak/main.py`](repro/run_leak/main.py) (+ [`nb_a.py`](repro/run_leak/nb_a.py) e [`nb_b.py`](repro/run_leak/nb_b.py)) — reproduz o bug **R2-6** de vazamento de `$param` entre múltiplos `%run`.

Comandos de execução via CLI:

```bash
spark-lineage capture --project-root repro --entrypoint job_bugs_r2.py --no-catalog
spark-lineage capture --project-root repro --entrypoint pipeline_bugs_r2.sql --no-catalog
spark-lineage capture --project-root repro --entrypoint notebook_bugs_r2.ipynb --no-catalog
spark-lineage capture --project-root repro/run_leak --entrypoint main.py --no-explore-branches --no-catalog
```

---

## Detalhamento Técnico

### R2-1 — Aspas duplas no Spark SQL (`"..."`) e `_top_level_sql`
- **Severidade:** Crítica (Segurança + Linhagem)
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 360–371 e 444–459).
- **Reprodução:** [`repro/pipeline_bugs_r2.sql`](repro/pipeline_bugs_r2.sql)
  ```sql
  SELECT id, "texto FROM tabela_falsa JOIN outra_falsa" AS msg FROM dev.cadastros.tb_banco;

  FROM dev.cadastros.tb_banco
  WHERE nm_banco = "(teste"
  INSERT INTO dev.tgt.tb_saida
  SELECT cbco;
  ```
- **Esperado:** `extract_sql_sources` retorna `["dev.cadastros.tb_banco"]`; `classify_sql` do `FROM ... INSERT INTO` retorna `("explain", "INSERT")`.
- **Obtido:** `extract_sql_sources` devolve `['tabela_falsa', 'outra_falsa', 'dev.cadastros.tb_banco']`; `classify_sql` devolve `("pass", "FROM")` (bypass de DML!).
- **Causa raiz:** `_strip_sql_comments(..., blank_strings=True)` só mascara strings quando `quote == "'"`, mas no Spark SQL `"..."` também é literal de string. Além disso, `_top_level_sql()` conta `(` e `)` sem ignorar strings/identificadores entre aspas, desbalanceando `depth` quando existe `(` dentro de `"..."`.
- **Direção de correção:** Mascarar também `quote == '"'` quando `blank_strings=True` e fazer `_top_level_sql` ignorar parênteses dentro de `'...'`, `"..."` e `` `...` ``.

### R2-2 — `DataStreamWriter.option("path", ...).start()` no Spark Connect
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1694–1711, `_writer_option_target`).
- **Esperado:** `df.writeStream.format("delta").option("path", "/mnt/stream/saida").start()` registra `"/mnt/stream/saida"` em `destinations`.
- **Obtido:** `target=None`, destino descartado.
- **Causa raiz:** No `databricks-connect` 16.4, `DataStreamWriter.option()` armazena em `writer._write_proto.options` (um `ScalarMapContainer` protobuf, que não passa em `isinstance(opts, dict)`).
- **Direção de correção:** Incluir `getattr(getattr(writer, "_write_proto", None), "options", None)` e aceitar qualquer `Mapping` ou objeto com `.get`/`.items()` em vez de exigir `dict`.

### R2-3 — `df.write.jdbc(url, table)` e `spark.read.jdbc(url, table)` capturam a URL JDBC
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1246–1253 e 1788–1794).
- **Reprodução:** [`repro/job_bugs_r2.py`](repro/job_bugs_r2.py)
  ```python
  df.write.jdbc("jdbc:postgresql://host:5432/db", "public.tb_vendas")
  ```
- **Esperado:** Destino `"public.tb_vendas"`.
- **Obtido:** Destino `"jdbc:postgresql://host:5432/db"` (a URL JDBC em vez da tabela).
- **Causa raiz:** `Capturer._target` (e `_reader_targets`) retorna `a[0]` sem tratamento especial para o método `jdbc(url, table, ...)`, cujo 1º argumento posicional é a URL e o 2º é a tabela.
- **Direção de correção:** Quando o método for `jdbc`, priorizar `kw.get("table")` ou `a[1]` (e só usar `_writer_option_target` / `_reader_targets` `dbtable` como fallback).

### R2-4 — Multi-table insert (`FROM ... INSERT INTO tgt1 ... INSERT INTO tgt2 ...`) perde destinos
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 508–512 e 1773).
- **Reprodução:** [`repro/pipeline_bugs_r2.sql`](repro/pipeline_bugs_r2.sql)
  ```sql
  FROM vw_base
  INSERT INTO dev.tgt.tb_ativos SELECT id, nome WHERE id > 0
  INSERT INTO dev.tgt.tb_inativos SELECT id, nome WHERE id <= 0;
  ```
- **Esperado:** Ambos os destinos (`dev.tgt.tb_ativos` e `dev.tgt.tb_inativos`) em `destinations`.
- **Obtido:** Apenas `dev.tgt.tb_ativos` é extraído; `dev.tgt.tb_inativos` some.
- **Causa raiz:** `extract_sql_target()` usa `_SQL_INSERT_TARGET_RE.search(...)` e devolve uma única string.
- **Direção de correção:** Criar `extract_sql_targets()` usando `finditer()` em `_top_level_sql(main_s)` e registrar uma ocorrência por destino em `_explain_sql_dml`.

### R2-5 — Tabela com crases em `saveAsTable` não casa em `sources._match_table`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1246–1253) e `src/spark_lineage/sources.py` (linhas 488–493).
- **Reprodução:** [`repro/job_bugs_r2.py`](repro/job_bugs_r2.py)
  ```python
  df.write.saveAsTable("`dev`.`tgt`.`tb_crase`")
  ```
- **Esperado:** `extract_tag(..., tag="destinations", table="dev.tgt.tb_crase")` encontra o destino.
- **Obtido:** Retorna `[]`.
- **Causa raiz:** `Capturer._target` não normaliza crases e `sources._match_table` usa `.strip("`")` (só remove das pontas) em vez de `.replace("`", "")` (mantendo crases internas como `` dev`.`tgt`.`tb_crase ``).
- **Direção de correção:** Usar `.replace("`", "")` em `sources._match_table` e normalizar crases em `Capturer._target` / `_writer_table`.

### R2-6 — Vazamento de `$param` entre múltiplos `%run` sequenciais
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 2375–2376, `exec_magic_run`).
- **Reprodução:** [`repro/run_leak/main.py`](repro/run_leak/main.py)
  ```python
  # MAGIC %run ./nb_a $etapa=CARGA
  # COMMAND ----------
  # MAGIC %run ./nb_b $etapa=VALIDACAO
  ```
- **Esperado:** `nb_a` recebe `etapa="CARGA"` e `nb_b` recebe `etapa="VALIDACAO"` -> destinos `dev.tgt.tb_carga` e `dev.tgt.tb_validacao`.
- **Obtido:** Apenas `dev.tgt.tb_carga` (o 2º `%run` recebe `etapa="CARGA"`).
- **Causa raiz:** `dbutils.widgets._p.setdefault(k, v)` persiste `"etapa": "CARGA"` em `_p` sem salvar/restaurar em um `try/finally` (como já é feito em `run_child_notebook`).
- **Direção de correção:** Salvar `prev_params = dict(dbutils.widgets._p)`, atualizar `dbutils.widgets._p.update(run_params)` (respeitando precedência de `--param` CLI se desejado) e restaurar no `finally`.

### R2-7 — Falhas em `_preprocess_ipynb_source` (`%sh` multilinha, `!pip`, `%%time`)
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 794 e 815–847).
- **Reprodução:** [`repro/notebook_bugs_r2.ipynb`](repro/notebook_bugs_r2.ipynb)
- **Problemas:**
  - **R2-7a:** `%sh` e `%bash` faltam em `_IPYNB_BLOCK_MAGICS` -> célula `%sh\necho hello\nls -la` deixa as linhas de shell como Python cru e falha com `SyntaxError`. (Em `.sql`, linha 794 também não lista `%bash`/`%%`.)
  - **R2-7b:** Comandos shell com `!` (`!pip install ...`) não são convertidos -> `SyntaxError`.
  - **R2-7c:** Cell magics Python (`%%time`, `%%timeit`, `%%capture`) viram `# MAGIC %%time` + `# MAGIC <código>` e `_process_magic_block` apaga toda a célula para `#`, perdendo o código PySpark inteiro.
- **Direção de correção:** Incluir `%sh`/`%bash` em `_IPYNB_BLOCK_MAGICS`; converter linhas `!cmd` em `__spark_lineage_magic__('sh', ...)` fora de strings multilinha; e tratar cell magics transparentes (`%%time`, `%%timeit`, `%%capture`) preservando o corpo Python das linhas seguintes (como `%python`).

### R2-8 — `BranchExplorer`: loop infinito em `while True` + `.path` em `_SKIP_ATTRS`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 873 e 1076–1083).
- **Reprodução:** [`repro/job_bugs_r2.py`](repro/job_bugs_r2.py)
- **Problemas:**
  - **R2-8a:** Em `while True: ... if df.count() > 0: ... break`, na passada forçada (`False`), `decide()` força `False` em **todas** as iterações do laço -> loop infinito.
  - **R2-8b:** `"path"` em `_SKIP_ATTRS` ignora qualquer atributo `.path` em objetos de negócio (`if cfg.path.endswith(".parquet"):`), pulando o ramo `else`.
- **Direção de correção:** Em `BranchExplorer.decide`, aplicar o valor forçado apenas na 1ª visita do `site_id` (ou limitar o número de forças por `site_id` na passada), voltando ao valor natural em iterações subsequentes do laço; e em `_is_technical_guard`, filtrar `sys.path`/`sys.modules`/`sys.version_info`/`sys.platform` verificando o receptor `sys` (não qualquer `.path`).

### R2-9 — `RelationV2` silenciosamente ignorado em `plan_parser.py`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/plan_parser.py` (linhas 30 e 44).
- **Reprodução:** `parse_relations("RelationV2[id#1, nome#2] dev.cadastros.tb_banco")` -> `[]`.
- **Causa raiz:** `_REL_RE` só casa `Relation ...`, e `"RelationV2"` está em `_IGNORED_REL_CLASSES`, ocultando leituras DataSource V2 (Iceberg / Delta V2 / Unity Catalog V2) sem gerar aviso.
- **Direção de correção:** Adicionar parsing de `RelationV2[cols] table` (diferenciando nós de leitura dos alvos de escrita sob `AppendData`/`OverwriteByExpression`).

### R2-10 — `parse_merge_targets` falha em `SubqueryAlias` qualificado com pontos
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/plan_parser.py` (linhas 34–36).
- **Reprodução:** `MergeIntoCommandEdge SubqueryAlias dev.src.orig, SubqueryAlias dev.tgt.alvo, ...` -> `[]`.
- **Causa raiz:** `_MERGE_RE` usa `SubqueryAlias\s+\w+`, que não casa `.` nem crases quando o `MERGE INTO` não usa aliases curtos (`s`/`t`).
- **Direção de correção:** Trocar `\w+` por `[^,]+` (ou `` [`\w.\-]+ ``) nos dois `SubqueryAlias`.

### R2-11 — `parse_formatted_scans` falha com `[codegen id : N]` e com `BatchScan`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/plan_parser.py` (linha 39, `_FMT_SCAN_HDR_RE`).
- **Reprodução:**
  - `(1) Scan parquet dev.sch.pix [codegen id : 1]` -> `[]`.
  - `(1) BatchScan dev.sch.pix` -> `[{'table': None, 'format': 'dev.sch.pix', ...}]`.
- **Causa raiz:** `_FMT_SCAN_HDR_RE` não permite sufixo opcional `\[codegen id\s*:\s*\d+\]` e exige sempre `fmt` + `table` (mas `BatchScan` não imprime `fmt` antes do nome da tabela).
- **Direção de correção:** Permitir sufixo opcional `(?:\s*\[codegen id\s*:\s*\d+\])?` e tratar `BatchScan <table>` preenchendo `table` corretamente.

### R2-12 — Colunas com acentos e aliases com espaço quebram `impact.py`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/impact.py` (linhas 26–27).
- **Reprodução:** `Project [upper(descrição#1) AS \`descrição limpa\`#2]` + `Filter (descrição#1 != '')` -> `columns_used == ['o']` e `output_columns_affected == []`.
- **Causa raiz:** `_ATTR_ID_RE` usa `[A-Za-z0-9_`]+` (apenas ASCII) e `_ALIAS_RE` usa `[^#\s`,\]]+` (proíbe espaço dentro de crases).
- **Direção de correção:** Suportar identificadores Unicode (`\w+`) e nomes entre crases `` `[^`]+` `` tanto em `_ATTR_ID_RE` quanto em `_ALIAS_RE`.

### R2-13 — `Generate` (`explode`) sem handler e `DELETE` não qualificado no `impact.py`
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/impact.py` (linhas 213–228 e 444).
- **Problemas:**
  - **R2-13a:** `Generate explode(tags#2), false, [tag#5]` cai em `_passthrough`, não rastreia `tag#5` e zera `output_columns_affected`.
  - **R2-13b:** Linha 444 checa `any(_match_table(t, source_table) for t in in_place_dml_targets(plan))` apenas em uma direção; quando `t="tb_banco"` e `source_table="dev.cadastros.tb_banco"`, o `DELETE` in-place não é filtrado e aparece falsamente como destino downstream.
- **Direção de correção:** Adicionar handler `_generate` que rastreia os atributos gerados em `[...]` quando a expressão do gerador usa colunas rastreadas; e tornar a linha 444 bidirecional (`_match_table(t, source_table) or _match_table(source_table, t)`), alinhada a `sources.py`.

---

## Como Rodar os Testes (Rodada 2)

```bash
# Roda toda a suíte (35 passed + 21 xfailed, exit code 0)
pytest -v

# Força a exibição das 21 falhas reais da Rodada 2
pytest tests/test_known_bugs_r2.py --runxfail
```
