# Relatório de Bugs — `spark-lineage`

Este relatório documenta **15 bugs reais** identificados na análise do `spark-lineage` (`capture_plans.py`, `sources.py` e `impact.py`), acompanhados de arquivos reprodutores autocontidos em [`repro/`](repro/) e testes automatizados em [`tests/test_known_bugs.py`](tests/test_known_bugs.py).

## Resumo Executivo

| # | Severidade | Categoria | Módulo(s) | Resumo do problema | Teste em `tests/test_known_bugs.py` |
|---|---|---|---|---|---|
| 1 | Alta | Captura `.py` | `capture_plans.py`, `sources.py` | `df.write.option("path", ...).save()` perde o destino (`target=None`) | `test_bug_write_save_with_option_path_captures_destination` |
| 2 | Alta | Captura `.py` | `capture_plans.py`, `sources.py` | `df.mergeInto(...)` descartado por `is_write_action("mergeInto(source)") == False` | `test_bug_df_merge_into_captures_destination` |
| 3 | Alta | Captura `.py` | `capture_plans.py`, `sources.py` | `DeltaTable.merge(...).execute()` não registra tabela de destino | `test_bug_delta_table_merge_captures_destination` |
| 4 | Média | Exploração AST | `capture_plans.py` | `if os.getenv(...)` / `sys.argv` ignorado pelo `BranchExplorer` | `test_bug_branch_explorer_explores_os_getenv_branch` |
| 5 | Crítica | SQL / Captura | `capture_plans.py` | `WITH ... SELECT` com `) update` bloqueado como `INSERT` e corrompe DataFrame (`_blocked`) | `test_bug_with_select_alias_update_not_blocked_as_insert` |
| 6 | Crítica (Segurança) | SQL | `capture_plans.py` | `FROM src INSERT INTO tgt SELECT ...` passa direto como leitura (`pass`) sem bloqueio de DML | `test_bug_sql_from_first_insert_is_classified_as_write` |
| 7 | Crítica (Segurança) | SQL | `capture_plans.py` | `'--'` dentro de string literal devora o resto da linha em `_COMMENT_RE` (perde fontes e burla bloqueio de `WITH ... INSERT`) | `test_bug_sql_double_dash_inside_string_preserves_sources` / `test_bug_sql_double_dash_in_cte_does_not_bypass_insert_interception` |
| 8 | Média | SQL | `capture_plans.py` | `CREATE OR REPLACE VIEW` retorna `target=None` (`table: null` em `destinations`) | `test_bug_sql_create_view_extracts_target_name` |
| 9 | Média | SQL | `capture_plans.py`, `sources.py` | `WITH ... DELETE/UPDATE/MERGE` sempre classificado como `sql:INSERT` | `test_bug_sql_with_cte_delete_preserves_delete_keyword` |
| 10 | Alta | SQL | `capture_plans.py` | `substitute_sql_params` substitui `:param` dentro de literais de string, gerando SQL inválido | `test_bug_substitute_sql_params_ignores_string_literals` |
| 11 | Alta | Notebook `.ipynb` | `capture_plans.py` | Célula `%%sql` (cell magic padrão Jupyter) é silenciosamente apagada (`plans: 0`) | `test_bug_ipynb_double_percent_sql_cell_is_captured` |
| 12 | Alta | Notebook `.ipynb` | `capture_plans.py` | Célula com `%pip` / `%run` seguida de código Python descarta todo o código Python | `test_bug_ipynb_line_magic_followed_by_python_executes_python` |
| 13 | Alta | Impacto | `impact.py`, `sources.py` | `_match_table` usa `q in c` (substring) e confunde `tb_banco` com `tb_banco_historico` | `test_bug_impact_match_table_does_not_collide_with_prefix_table` |
| 14 | Alta | Impacto | `impact.py` | `_ColumnTracer._filter` propaga `children[0]` (subquery `IN`) em vez de `children[-1]`, zerando `output_columns_affected` | `test_bug_impact_filter_with_in_subquery_preserves_output_columns` |
| 15 | Média | Fontes/Destinos | `sources.py` | `in_place_dml_targets` compara nome exato sem normalizar sufixo/case (`tb_saida != dev.tgt.tb_saida`) | `test_bug_sources_in_place_delete_unqualified_not_listed_as_source` |

---

## Arquivos Reprodutores

Todos os reprodutores abaixo utilizam apenas `spark.range(5)` / `range(5)` (sem dependência de tabelas existentes no catálogo):

1. [`repro/job_bugs.py`](repro/job_bugs.py) — reproduz os bugs **1, 2, 4 e 5** em um único job `.py`.
2. [`repro/pipeline_bugs.sql`](repro/pipeline_bugs.sql) — reproduz os bugs **5, 7a, 8 e 10** em um notebook `.sql`.
3. [`repro/notebook_bugs.ipynb`](repro/notebook_bugs.ipynb) — reproduz os bugs **11 e 12** em um notebook Jupyter `.ipynb`.

Comandos de execução via CLI:

```bash
spark-lineage capture --project-root repro --entrypoint job_bugs.py --no-catalog
spark-lineage capture --project-root repro --entrypoint pipeline_bugs.sql --no-catalog
spark-lineage capture --project-root repro --entrypoint notebook_bugs.ipynb --no-catalog
```

---

## Detalhamento Técnico dos Bugs

### Bugs em Jobs Python (`.py`)

#### Bug 1 — `df.write.option("path", ...).save()` descarta o destino
- **Severidade:** Alta
- **Reprodução:** [`repro/job_bugs.py`](repro/job_bugs.py)
  ```python
  df.write.format("delta").mode("overwrite").option("path", "/mnt/repro_lineage/vendas").save()
  ```
- **Esperado:** `"/mnt/repro_lineage/vendas"` listado em `destinations` (ação `write.save`) e em `flows[*].writes`.
- **Obtido:** O destino desaparece completamente (`destinations` não contém o caminho).
- **Causa raiz:**
  - `Capturer._target()` (`src/spark_lineage/capture_plans.py`, linhas 1032–1039) só inspeciona argumentos posicionais `a[0]` e kwargs (`path`, `name`, `tableName`, `table`), ignorando opções acumuladas no writer (`writer._options` / `writer._write.options`, ao contrário do que `_reader_targets` faz para leituras nas linhas 1554–1560).
  - Com `target=None`, `_SourcesBuilder._add_writes()` (`src/spark_lineage/sources.py`, linhas 242–255) cai no `else: continue` e descarta silenciosamente a gravação.
- **Direção de correção:** Extrair `path`/`dbtable`/`table` de `writer._write.options` (Spark Connect) e `writer._options` (Classic) em `_make_writer_factory` quando `_target(a, kw)` for `None`.

#### Bug 2 — `df.mergeInto(...)` (Spark 4 / Databricks Connect 16.4) descartado em `sources.py`
- **Severidade:** Alta
- **Reprodução:** [`repro/job_bugs.py`](repro/job_bugs.py)
  ```python
  df.mergeInto("dev.tgt.tb_merge_destino", F.col("id") == F.col("id")).whenMatched().updateAll().merge()
  ```
- **Esperado:** `"dev.tgt.tb_merge_destino"` listado em `destinations` e `flows`.
- **Obtido:** O destino é completamente ignorado (`destinations` não inclui `dev.tgt.tb_merge_destino`).
- **Causa raiz:** `_make_merge_into_factory()` (`src/spark_lineage/capture_plans.py`, linhas 1690–1695) registra `action="mergeInto(source)"` com `target=table`, mas `is_write_action()` (`src/spark_lineage/sources.py`, linhas 90–101) só retorna `True` para prefixos `write.`, `writeStream.` e `sql:`.
- **Direção de correção:** Incluir `"mergeInto(source)"` e `"delta.merge(source)"` (ou unificar o prefixo de ação) em `is_write_action()`.

#### Bug 3 — `DeltaTable.forName(...).merge(...).execute()` não registra tabela de destino
- **Severidade:** Alta
- **Reprodução:** `DeltaTable.forName(spark, "dev.tgt.alvo").merge(df_src, cond)...execute()`
- **Esperado:** `"dev.tgt.alvo"` listado em `destinations` e `flows`.
- **Obtido:** `destinations` fica vazio.
- **Causa raiz:**
  - `_make_delta_merge_factory()` (`src/spark_lineage/capture_plans.py`, linhas 1625–1633) chama `cap.capture(source, "delta.merge(source)", target=None, ...)` sem extrair `table._tableOrViewName` / `table._path`.
  - `DeltaMergeBuilder.execute` é substituído por `_make_delta_event_factory` (linha 1679), que não captura um plano `MergeIntoCommand`.
  - `is_write_action("delta.merge(source)")` retorna `False` em `sources.py`.
- **Direção de correção:** Extrair o alvo de `DeltaTable` (`_tableOrViewName` ou `_path`) no patch de `DeltaTable.merge` e reconhecê-lo em `is_write_action()`.

#### Bug 4 — `BranchExplorer` ignora `if` de negócio usando `os.getenv` / `os.environ` / `sys.argv`
- **Severidade:** Média
- **Reprodução:** [`repro/job_bugs.py`](repro/job_bugs.py)
  ```python
  if os.getenv("MODO") == "REPROC":
      df.write.saveAsTable("dev.tgt.tb_reprocessamento")
  else:
      df.write.saveAsTable("dev.tgt.tb_normal")
  ```
- **Esperado:** `branches` com 1 ramo explorado (`True` e `False`) e ambos os destinos (`dev.tgt.tb_reprocessamento` e `dev.tgt.tb_normal`) capturados.
- **Obtido:** `branches: []`; `"dev.tgt.tb_reprocessamento"` nunca é visitado nem listado em `destinations`.
- **Causa raiz:** `_SKIP_NAMES` (`src/spark_lineage/capture_plans.py`, linhas 658–680) inclui `"os"` e `"sys"`, fazendo `_should_instrument_if()` ignorar qualquer `if` que mencione esses módulos em qualquer subexpressão.
- **Direção de correção:** Restringir o filtro de guardas técnicos a padrões específicos (ex.: `sys.path`, `sys.version_info`, `os.path.exists` de bootstrap), permitindo instrumentar `os.getenv`, `os.environ.get` e `sys.argv`.

---

### Bugs em Classificação, Parsing e Pré-processamento SQL (`.sql` e `spark.sql`)

#### Bug 5 — Falso positivo em `_CTE_DML_RE` bloqueia `WITH ... SELECT` e cria destino fantasma `"FROM"`
- **Severidade:** Crítica
- **Reprodução:** [`repro/job_bugs.py`](repro/job_bugs.py) e [`repro/pipeline_bugs.sql`](repro/pipeline_bugs.sql)
  ```sql
  WITH base AS (
      SELECT id, CAST(id AS STRING) update
      FROM range(5)
  )
  SELECT id, update FROM base
  ```
- **Esperado:** Classificado como `("pass", "WITH")` (leitura). No `.py`, `df_cte` mantém colunas `id`, `update`.
- **Obtido:**
  - `classify_sql()` retorna `("explain", "INSERT")`.
  - `_SQL_TARGET_RE` casa `UPDATE FROM` dentro da CTE e cria um **destino fantasma literalmente chamado `"FROM"`** (`action: "sql:INSERT"`).
  - No `.py`, `spark.sql()` devolve `SELECT 1 AS _blocked WHERE 1=0`, corrompendo o schema gravado em `dev.tgt.tb_from_cte` para `[{"name": "_blocked", "type": "int"}]` (ou derrubando o job com `UNRESOLVED_COLUMN` se houver `.select("id")` na sequência).
- **Causa raiz:** `_CTE_DML_RE = re.compile(r"\)\s*(INSERT|MERGE|UPDATE|DELETE)\b", re.I)` (`src/spark_lineage/capture_plans.py`, linha 318) busca o padrão em qualquer ponto da string sem controlar nível de parênteses nem distinguir aliases de coluna após `)`.
- **Direção de correção:** Encontrar o verbo principal no nível de parêntese `0` logo após o encerramento das definições de CTE (`WITH nome AS (...), nome2 AS (...) <VERBO>`).

#### Bug 6 — `FROM src INSERT INTO tgt SELECT ...` burla interceptação de DML
- **Severidade:** Crítica (Segurança + Linhagem)
- **Reprodução:**
  ```sql
  FROM dev.cadastros.tb_banco
  INSERT INTO dev.tgt.tb_saida
  SELECT cbco, nm_banco, dt_ref
  ```
- **Esperado:** Classificado como `("explain", "INSERT")`, sem execução real, com destino `"dev.tgt.tb_saida"`.
- **Obtido:** `classify_sql()` retorna `("pass", "FROM")` e `extract_sql_target()` retorna `None`.
- **Causa raiz:** `"FROM"` está em `_READ_KW` (`src/spark_lineage/capture_plans.py`, linha 306), e `sql_command` está em `_ALLOWED_COMMANDS` (linha 1424). Assim, um `INSERT` no formato `FROM ... INSERT INTO ...` (suportado nativamente pelo Spark SQL, inclusive multi-table insert) passa direto para execução real no cluster sem registrar destino.
- **Direção de correção:** Verificar presença de `INSERT (INTO|OVERWRITE)` no nível superior quando `kw == "FROM"` e extrair todos os alvos de `INSERT`.

#### Bug 7 — `'--'` dentro de string literal corrompe parsing de fontes e burla bloqueio de `WITH ... INSERT`
- **Severidade:** Crítica (Segurança + Linhagem)
- **Reprodução:** [`repro/pipeline_bugs.sql`](repro/pipeline_bugs.sql) (linha 25) e:
  ```sql
  WITH base AS (SELECT cbco, COALESCE(nm_banco, '--') AS nm_banco FROM dev.cadastros.tb_banco)
  INSERT INTO dev.tgt.tb_saida SELECT cbco, nm_banco FROM base
  ```
- **Esperado:**
  - `extract_sql_sources` inclui `vw_base` / `dev.cadastros.tb_banco`.
  - `classify_sql` classifica o `WITH ... INSERT` como `("explain", "INSERT")`.
- **Obtido:**
  - Em `pipeline_bugs.sql`, a linha 25 (`SELECT id, COALESCE(nome, '--') ... FROM vw_base`) desaparece de `read_sites["vw_base"]`.
  - No `WITH ... INSERT`, `_COMMENT_RE` remove `--') ... FROM dev.cadastros.tb_banco)`, engolindo o `)` da CTE; `_CTE_DML_RE` falha e `classify_sql` devolve `("pass", "WITH")`, deixando o `INSERT` passar sem bloqueio.
- **Causa raiz:** `_COMMENT_RE = re.compile(r"(--[^\n]*(\n|$))|(/\*.*?\*/)", re.S)` (`src/spark_lineage/capture_plans.py`, linha 299) é aplicado cegamente sem respeitar literais entre aspas simples/duplas/crases.
- **Direção de correção:** Remover comentários com um scanner lexical que preserve literais `'...'`, `"..."` e `` `...` `` (similar ao já existente em `_split_sql_statements`).

#### Bug 8 — `CREATE OR REPLACE VIEW` gera destino com `table: null`
- **Severidade:** Média
- **Reprodução:** [`repro/pipeline_bugs.sql`](repro/pipeline_bugs.sql)
  ```sql
  CREATE OR REPLACE VIEW dev.tgt.vw_publicada AS
  SELECT id, nome FROM vw_base;
  ```
- **Esperado:** Destino com `table: "dev.tgt.vw_publicada"`.
- **Obtido:** Destino com `table: null` (`actions: ["sql:CREATE"]`).
- **Causa raiz:** `_SQL_TARGET_RE` (`src/spark_lineage/capture_plans.py`, linha 326) só contempla `TABLE`, não `VIEW` (nem `MATERIALIZED VIEW` / `EXTERNAL TABLE`).
- **Direção de correção:** Expandir a regex para aceitar `(?:EXTERNAL\s+)?TABLE|(?:MATERIALIZED\s+)?VIEW`.

#### Bug 9 — `WITH ... DELETE/UPDATE/MERGE` perde o verbo DML real
- **Severidade:** Média
- **Reprodução:**
  ```sql
  WITH alvo AS (SELECT cbco FROM dev.cadastros.tb_banco)
  DELETE FROM dev.tgt.tb_saida WHERE id_banco = 1
  ```
- **Esperado:** `classify_sql()` retorna `("explain", "DELETE")`.
- **Obtido:** Retorna `("explain", "INSERT")` (hardcoded).
- **Causa raiz:** Linha 355 de `src/spark_lineage/capture_plans.py` faz `return "explain", "INSERT"` ignorando o grupo capturado por `_CTE_DML_RE`. Com `action="sql:INSERT"`, `in_place_dml_targets()` deixa de reconhecer `DELETE`/`UPDATE`, listando a tabela alvo como fonte.
- **Direção de correção:** Retornar `m.group(1).upper()` (ou o verbo real obtido pelo parser de nível 0).

#### Bug 10 — `substitute_sql_params` substitui `:param` dentro de strings literais
- **Severidade:** Alta
- **Reprodução:** [`repro/pipeline_bugs.sql`](repro/pipeline_bugs.sql)
  ```sql
  SELECT id, 'etiqueta: :modo' AS info FROM vw_base;
  ```
  com widget `modo="PROD"`.
- **Esperado:** Literal preservado como `'etiqueta: :modo'`.
- **Obtido:** Query reescrita para `SELECT id, 'etiqueta: 'PROD'' AS info FROM vw_base` — **erro de sintaxe SQL** em runtime.
- **Causa raiz:** `_SQL_COLON_PARAM_RE` (`src/spark_lineage/capture_plans.py`, linhas 334 e 410) substitui `:chave` por `repr(valor)` em toda a string SQL, sem ignorar trechos entre aspas simples/duplas ou comentários.
- **Direção de correção:** Aplicar substituição de `:param` apenas em tokens fora de literais e comentários.

---

### Bugs em Notebooks Jupyter (`.ipynb`)

#### Bug 11 — Célula `%%sql` é silenciosamente apagada
- **Severidade:** Alta
- **Reprodução:** [`repro/notebook_bugs.ipynb`](repro/notebook_bugs.ipynb) (Célula 1)
  ```sql
  %%sql
  SELECT id, id * 2 AS dobro FROM range(5)
  ```
- **Esperado:** 1 plano SQL capturado (`sql:SELECT`).
- **Obtido:** `plans: 0` — todas as linhas da célula são transformadas em `#`.
- **Causa raiz:** `_preprocess_ipynb_source()` (`src/spark_lineage/capture_plans.py`, linhas 629–630) transforma `%%sql` em `# MAGIC %%sql`, mas `_process_magic_block()` (linha 543) só verifica `%sql` (com um único `%`) e ignora `%%sql`, deixando a célula comentada.
- **Direção de correção:** Normalizar `%%<magic>` para `%<magic>` (ou aceitar `%%` em `_process_magic_block`) para cell magics padrão do Jupyter (`%%sql`, `%%python`, `%%bash`, `%%sh`, `%%md`).

#### Bug 12 — Line magic (`%pip` / `%run`) seguida de código Python apaga o código Python da célula
- **Severidade:** Alta
- **Reprodução:** [`repro/notebook_bugs.ipynb`](repro/notebook_bugs.ipynb) (Célula 2)
  ```python
  %pip install --quiet dummy-pkg
  df = spark.range(5)
  df.write.saveAsTable("dev.tgt.tb_pos_pip")
  ```
- **Esperado:** Evento `magic_pip` registrado **e** destino `dev.tgt.tb_pos_pip` capturado.
- **Obtido:** `destinations: []` — o código Python inteiro é engolido na string do evento `magic_pip` e nunca executado.
- **Causa raiz:** `_preprocess_ipynb_source()` (`src/spark_lineage/capture_plans.py`, linhas 629–630) verifica apenas `if first.startswith("%"):` e prefixa **todas** as linhas da célula com `# MAGIC `; em seguida `_process_magic_block()` (linhas 525–534) trata todo o bloco como `%pip`.
- **Direção de correção:** Tratar line magics de 1 linha (`%pip`, `%run`, `%sh`, etc.) linha a linha no `.ipynb`, mantendo o restante das linhas da célula como código Python normal (e aplicar prefixo de bloco inteiro apenas para cell magics de bloco como `%sql`/`%%sql`, `%md`/`%%md`, `%scala`, `%r`).

---

### Bugs em Análise de Impacto (`impact.py`) e Consolidação de Fontes (`sources.py`)

#### Bug 13 — `_match_table` com `q in c` gera falsos positivos entre tabelas com prefixo comum
- **Severidade:** Alta
- **Reprodução:** Rastrear `table="dev.cadastros.tb_banco", column="cbco"` quando o plano lê apenas `dev.cadastros.tb_banco_historico`.
- **Esperado:** `summary.found == False`, `destinations_affected == []`.
- **Obtido:** `summary.found == True` e lista os destinos de `tb_banco_historico` como se fossem impactados por `tb_banco`.
- **Causa raiz:** `_match_table()` (`src/spark_lineage/impact.py`, linha 41, e `src/spark_lineage/sources.py`, linha 489) faz `return c == q or c.endswith(f".{q}") or q in c`. O termo `q in c` faz `"dev.cadastros.tb_banco"` casar com `"dev.cadastros.tb_banco_historico"`.
- **Direção de correção:** Remover `or q in c` (aceitar apenas correspondência exata `c == q` ou sufixo delimitado por ponto `c.endswith(f".{q}")`).

#### Bug 14 — `Filter` com subquery (`IN (SELECT ...)` / `EXISTS`) perde todas as colunas de saída no `impact`
- **Severidade:** Alta
- **Reprodução:** Plano `analyzed`:
  ```text
  Filter cbco#1 IN (list#99 [])
  :  +- Project [cod_bco#10]
  :     +- Relation dev.compliance.tb_restritiva[cod_bco#10,ds_motivo#11] parquet
  +- Relation dev.cadastros.tb_banco[cbco#1,nm_banco#2] parquet
  ```
- **Esperado:** `output_columns_affected` contém `cbco` (preservando rastreio multi-hop).
- **Obtido:** `output_columns_affected == []` (vazio), quebrando o encadeamento em tabelas staging.
- **Causa raiz:** `_ColumnTracer._filter()` (`src/spark_lineage/impact.py`, linha 303) atribui `node.output_attrs = _node_output_attrs(node.children[0])`. No plano do Spark, subqueries de filtro aparecem como primeiros filhos (`children[0]`), enquanto a relação principal é o último filho (`children[-1]`).
- **Direção de correção:** Propagar `node.children[-1]` em `_filter` (e revisar `_node_output_attrs` / `_passthrough` para nós com subqueries).

#### Bug 15 — `in_place_dml_targets` não casa tabela alvo não qualificada ou com caixa diferente
- **Severidade:** Média
- **Reprodução:** `DELETE FROM tb_saida ...` gerando `Relation dev.tgt.tb_saida[...]` no plano `analyzed`.
- **Esperado:** `sources == []` e `destinations == ["tb_saida"]`.
- **Obtido:** `dev.tgt.tb_saida` aparece indevidamente em `sources` enquanto `tb_saida` aparece em `destinations`.
- **Causa raiz:** `_add_reads()` (`src/spark_lineage/sources.py`, linha 232) verifica `rel["table"] in in_place` (comparação exata de string), falhando quando o DML usa nome sem catálogo/schema (`tb_saida`) ou maiúsculas (`DEV.TGT.TB_SAIDA`).
- **Direção de correção:** Comparar via correspondência case-insensitive de nome completo ou sufixo delimitado por ponto (`c == t or c.endswith(f".{t}")`).

---

## Como Rodar os Testes Adicionados

Os 18 testes em [`tests/test_known_bugs.py`](tests/test_known_bugs.py) validam o **comportamento esperado** de cada bug e estão marcados com `@pytest.mark.xfail(strict=True)`:

```bash
# Roda toda a suíte (17 passed + 18 xfailed, mantendo exit code 0)
pytest -v

# Força a execução dos xfails para ver todas as 18 falhas reais detalhadas
pytest tests/test_known_bugs.py --runxfail
```
