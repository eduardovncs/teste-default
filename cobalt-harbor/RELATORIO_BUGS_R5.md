# Relatório de Bugs — Rodada 5 (`spark-lineage`)

Após a correção das Rodadas 1, 2, 3 e 4 ([`RELATORIO_BUGS.md`](RELATORIO_BUGS.md), [`RELATORIO_BUGS_R2.md`](RELATORIO_BUGS_R2.md), [`RELATORIO_BUGS_R3.md`](RELATORIO_BUGS_R3.md) e [`RELATORIO_BUGS_R4.md`](RELATORIO_BUGS_R4.md) — todos os 104 testes anteriores passando), esta quinta auditoria percorreu todos os 6 módulos (`doctor.py`, `cli.py`, `sources.py`, `plan_parser.py`, `impact.py` e `capture_plans.py`) e identificou **23 novos bugs reais**, acompanhados de 4 conjuntos reprodutores em [`repro/`](repro/) e **42 testes automatizados** (`xfail(strict=True)`) em [`tests/test_known_bugs_r5.py`](tests/test_known_bugs_r5.py).

## Resumo Executivo

| # | Severidade | Categoria | Módulo(s) | Resumo do problema | Teste(s) em `tests/test_known_bugs_r5.py` |
|---|---|---|---|---|---|
| R5-1 | Alta | Diagnóstico | `doctor.py` | `render_doctor_text` lança `KeyError: 'profiles_available'` se `~/.databrickscfg` falhar ao ser lido; e `ConfigParser()` falha em `%` literal | `test_r5_doctor_render_text_survives_invalid_databrickscfg`, `test_r5_doctor_config_allows_percent_in_values` |
| R5-2 | Alta | Diagnóstico | `doctor.py` | `_read_profile` herda silenciosamente credenciais/compute de `[DEFAULT]` em perfis nomeados (divergindo do `databricks.sdk`) | `test_r5_doctor_named_profile_does_not_inherit_default` |
| R5-3 | Média | Diagnóstico | `doctor.py` | `_env_settings` ignora `DATABRICKS_CLIENT_ID` (OAuth M2M via env vars) e acusa falso erro `credencial ausente` | `test_r5_doctor_env_settings_recognizes_oauth_m2m_client_id` |
| R5-4 | Média | CLI | `cli.py`, `sources.py` | `sources --catalog --tag catalog` descarta o catálogo recém-buscado e devolve `result["catalog"]` antigo | `test_r5_cli_sources_catalog_flag_updates_tag_catalog_output` |
| R5-5 | Alta (Incompletude R4) | Fontes / Catálogo | `sources.py` | `scans_by_table.get(rtbl)` e `_finalize_source` são case-sensitive: perde match do scan (`read=None`) e duplica colunas quando caixa difere do catálogo | `test_r5_sources_scan_matching_is_case_insensitive`, `test_r5_sources_finalize_does_not_duplicate_case_variant_columns` |
| R5-6 | Média | Fontes | `sources.py` | Mesma tabela lida em caixas diferentes entre planos (`DEV.SRC.TB` vs `dev.src.tb`) cria entradas duplicadas em `sources` | `test_r5_sources_merges_same_table_across_case_variants` |
| R5-7 | Média | Markdown | `sources.py` | `_md_events` lança `KeyError: 'query'` em eventos já normalizados (com `detail` e `len <= 4`) sem chave `query` | `test_r5_sources_render_markdown_events_without_query_key` |
| R5-8 | Alta | Parser / Fontes | `plan_parser.py` | `Scan ExistingRDD` (`spark.createDataFrame` / RDD em memória) não é ignorado e vira fonte fantasma `<relação sem nome (ExistingRDD)>` | `test_r5_plan_parser_ignores_existing_rdd_scans_and_relations`, `test_r5_sources_does_not_create_phantom_source_for_existing_rdd` |
| R5-9 | Média | Parser | `plan_parser.py` | `_parse_attr` trunca nomes de coluna contendo `#` sem sufixo `#id`; `parse_read_schema` quebra colunas com `:` entre crases | `test_r5_plan_parser_attr_preserves_hash_without_expr_id`, `test_r5_plan_parser_read_schema_preserves_colon_in_backticked_column` |
| R5-10 | Média | Parser | `plan_parser.py` | `parse_merge_targets` exige dois `SubqueryAlias` literais e retorna `[]` em `MergeIntoCommand` sem `.alias()` | `test_r5_plan_parser_merge_targets_without_subquery_alias` |
| R5-11 | Crítica / Alta | Impacto | `impact.py` | `_aggregate` exige `len(groups) >= 2` e ignora agregações globais sem `GROUP BY` (`Aggregate [sum(valor#2) AS total#10]`), devolvendo a coluna original sem transformação | `test_r5_impact_handles_global_aggregate_without_group_by` |
| R5-12 | Alta | Impacto | `impact.py` | `_extract_bracket_groups` não ignora `]`/`[` dentro de literais `'...'`, truncando `Project`/`Aggregate` (ex.: `regexp_replace(t, ']', '')`) | `test_r5_impact_extract_bracket_groups_ignores_brackets_in_string_literals` |
| R5-13 | Alta | Impacto | `impact.py` | Subqueries `scalar-subquery#id []` em `Project` e `list#id []` em `Filter IN (...)` interrompem o rastreio da coluna interna | `test_r5_impact_propagates_scalar_subquery_in_project`, `test_r5_impact_records_in_subquery_filter` |
| R5-14 | Média | Impacto | `impact.py` | `_project_kind` classifica `cast(x as double) + 10.0` como `cast` (`direct=True`), e `VALOR#1 AS valor#2` como `derive` (`direct=False`) | `test_r5_impact_cast_plus_arithmetic_is_derive_not_direct_cast`, `test_r5_impact_case_only_alias_is_not_derive` |
| R5-15 | Alta | Impacto | `impact.py` | Multi-hop para no hop 1 quando destino é `cat.sch.stg` e leitura seguinte é `sch.stg`; `_merge_destination` e `catalog.get` são case-sensitive | `test_r5_impact_multihop_matches_qualified_dest_with_unqualified_read`, `test_r5_impact_merges_destinations_case_insensitively`, `test_r5_impact_catalog_lookup_is_case_insensitive` |
| R5-16 | Média | Impacto | `impact.py` | `Except` / `Intersect` silenciam completamente a coluna rastreada quando ela participa do lado direito (`children[1]`) | `test_r5_impact_except_records_right_side_column_usage` |
| R5-17 | Alta (Regressão R4) | Exploração AST | `capture_plans.py` | `_execute_forced_pass` faz `site_id.rpartition(":")` em `arquivo:linha:coluna` (de `ast.IfExp`), corrompendo `call_site` com `col_offset` na linha | `test_r5_forced_ternary_exception_warning_preserves_file_and_line` |
| R5-18 | Alta | Notebook `.ipynb` | `capture_plans.py` | `_preprocess_ipynb_source` trata operadores Python multilinha `!=` e `%` no início da linha como magics `!` (`%sh`) e `%`, causando `SyntaxError` ou corrupção silenciosa | `test_r5_ipynb_multiline_not_equal_and_modulo_operators_compile_and_run` |
| R5-19 | Alta | Captura `.py` | `capture_plans.py` | `Capturer.rows` (e `pandas`) compartilham cache só por `call_site` (colidindo schemas distintos em loops/helpers) e `rows` retorna a lista mutável sem cópia | `test_r5_capturer_rows_cache_separates_schemas_at_same_call_site`, `test_r5_capturer_rows_cache_returns_copy_preventing_mutation` |
| R5-20 | Alta | SQL / Cache / Views | `capture_plans.py` | Comentários SQL intermediários fazem `_rewrite_safe_temp_view_sql` não reescrever `CACHE TABLE` / `CREATE TEMP VIEW`; e `createGlobalTempView` não é patcheado | `test_r5_rewrite_cache_table_with_inline_comment_before_select`, `test_r5_rewrite_create_temp_view_with_inline_comment`, `test_r5_create_global_temp_view_is_patched_for_branch_passes` |
| R5-21 | Média | SQL | `capture_plans.py` | `substitute_sql_params` usa `repr()` (aspas duplas `"O'Brien"` em SQL); `_SQL_INSERT_TARGET_RE` extrai `'DIRECTORY'`; `extract_sql_sources` extrai `DELTA` e colunas de `EXTRACT(... FROM)` | `test_r5_substitute_sql_params_escapes_single_quote_with_sql_quotes`, `test_r5_multi_table_insert_overwrite_directory_does_not_extract_keyword`, `test_r5_extract_sql_sources_ignores_extract_from_and_using_delta` |
| R5-22 | Média | Magics & `%run` | `capture_plans.py` | `%timeit <stmt>` de linha única ainda é apagado; `# MAGIC %md` indentado gera `IndentationError`; `%run` com comentário inline falha; ciclo de `%run` causa `RecursionError` | `test_r5_single_line_percent_timeit_executes_inline_code`, `test_r5_indented_magic_md_inside_if_does_not_cause_indentation_error`, `test_r5_magic_run_strips_inline_comment_from_target_path`, `test_r5_magic_run_cycle_does_not_raise_recursion_error` |
| R5-23 | Média | Delta / Readers / SQL | `capture_plans.py` | `_delta_table_target` e `mergeInto` mantêm crases; `_reader_targets` ignora `.option("PATH", ...)` maiúscula; `_split_sql_statements` aponta linha para `/* comentário */` e escapa `\` em crases | `test_r5_delta_table_target_and_merge_into_normalize_backticks`, `test_r5_reader_targets_matches_uppercase_option_keys`, `test_r5_split_sql_statements_line_number_skips_leading_block_comment`, `test_r5_split_sql_statements_does_not_escape_backslash_in_backticks` |

---

## Arquivos Reprodutores (Rodada 5)

1. [`repro/job_bugs_r5.py`](repro/job_bugs_r5.py) — reproduz ternário que falha no ramo forçado (**R5-17**), colisão de `_row_cache` + mutação in-place (**R5-19**), `createGlobalTempView` (**R5-20**) e `%timeit` de linha única (**R5-22**).
2. [`repro/pipeline_bugs_r5.sql`](repro/pipeline_bugs_r5.sql) — reproduz comentários em `CREATE TEMP VIEW` / `CACHE TABLE` (**R5-20**), `:param` com apóstrofo, multi-table `INSERT OVERWRITE DIRECTORY`, `EXTRACT(... FROM)` e `USING DELTA` (**R5-21**), e comentário de bloco `/* ... */` antes da query (**R5-23**).
3. [`repro/notebook_bugs_r5.ipynb`](repro/notebook_bugs_r5.ipynb) — reproduz operadores multilinha `!=` e `%` (**R5-18**) e `%timeit` de linha única (**R5-22**).
4. [`repro/run_cycle/nb_a.py`](repro/run_cycle/nb_a.py) e [`repro/run_cycle/nb_b.py`](repro/run_cycle/nb_b.py) — reproduz ciclo de `%run` causando `RecursionError` (**R5-22**).

Comandos de execução via CLI:

```bash
spark-lineage capture --project-root repro --entrypoint job_bugs_r5.py --no-catalog
spark-lineage capture --project-root repro --entrypoint pipeline_bugs_r5.sql --param cliente_nome="O'Brien" --no-catalog
spark-lineage capture --project-root repro --entrypoint notebook_bugs_r5.ipynb --no-catalog
spark-lineage capture --project-root repro/run_cycle --entrypoint nb_a.py --no-catalog
```

---

## Detalhamento Técnico

### Diagnóstico (`doctor.py`)

#### R5-1 — `render_doctor_text` quebra com `KeyError` ao ler `.databrickscfg` inválido e `ConfigParser` rejeita `%` literal
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/doctor.py` (linhas 140–141, 201–208, 424–429).
- **Problema:**
  1. `configparser.ConfigParser()` usa `BasicInterpolation` por padrão; qualquer `%` em um valor (token, senha, URL codificada) gera `InterpolationSyntaxError`.
  2. No bloco `except` de `_check_databricks_config`, o dict retornado omite `profiles_available` e `env_vars`, fazendo `_render_databricks` lançar `KeyError: 'profiles_available'` (e depois `KeyError: 'env_vars'`).
- **Direção de correção:** Usar `configparser.ConfigParser(interpolation=None)` e incluir `profiles_available: []` e `env_vars: env_present` no retorno de erro (ou usar `.get(...)` em `_render_databricks`).

#### R5-2 — `_read_profile` herda chaves de `[DEFAULT]` para perfis nomeados
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/doctor.py` (linhas 140–147).
- **Problema:** `dict(cp.items(active_profile))` mescla automaticamente todas as chaves de `[DEFAULT]` em qualquer seção. O `databricks.sdk.config.Config._known_file_config_loader` explicitamente **não** herda `[DEFAULT]` em perfis nomeados (`ini_file._sections`), pois no Databricks CLI/SDK cada perfil é independente. Assim, `doctor --profile staging` aprova perfis incompletos que falham ao conectar.
- **Direção de correção:** Para `active_profile == "DEFAULT"`, ler `dict(cp.defaults())`; para perfis nomeados, ler apenas as chaves próprias da seção (ex.: `dict(cp._sections[active_profile])` ou `ConfigParser(default_section="__none__")`).

#### R5-3 — `_env_settings` ignora `DATABRICKS_CLIENT_ID` (OAuth M2M)
- **Severidade:** Média
- **Onde:** `src/spark_lineage/doctor.py` (linhas 47–55, 160–168).
- **Problema:** `_profile_auth_type` aceita `client_id` como `oauth-m2m` no arquivo, mas `_env_settings` só verifica `DATABRICKS_AUTH_TYPE` e `DATABRICKS_TOKEN`, falhando em pipelines CI/CD configurados com `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`.
- **Direção de correção:** Reconhecer `DATABRICKS_CLIENT_ID` em `_env_settings` (retornando `"oauth-m2m"`) e incluí-la em `_ENV_VARS`.

---

### CLI (`cli.py`)

#### R5-4 — `sources --catalog --tag catalog` devolve o catálogo antigo do `plan.json`
- **Severidade:** Média
- **Onde:** `src/spark_lineage/cli.py` (linhas 85–89) e `src/spark_lineage/sources.py` (linhas 589–593).
- **Problema:** Quando `--catalog` é passado, `cmd_sources` busca o catálogo novo na variável local `catalog` e o passa para `build_sources`, mas `extract_tag(result, doc, tag="catalog")` lê `result.get("catalog")` (os dados antigos do arquivo).
- **Direção de correção:** Atualizar `result = {**result, "catalog": catalog}` (ou passar o `catalog` atualizado para `extract_tag`).

---

### Consolidação de Fontes (`sources.py`)

#### R5-5 — Match de scans e deduplicação de colunas do catálogo são case-sensitive
- **Severidade:** Alta (incompletude do R4-21)
- **Onde:** `src/spark_lineage/sources.py` (linhas 156–167, 252–254, 291, 412–415).
- **Problema:**
  1. `scans_by_table.get(rtbl, [])` faz lookup case-sensitive: se o plano analisado traz `DEV.SRC.TB` e o scan físico traz `dev.src.tb`, o scan não é associado (`has_scan=False`, `read=None`).
  2. `_finalize_source` faz `[c for c in cat if c not in entry["columns"]]` case-sensitive, duplicando cada coluna (`ID`, `VALOR`, `id`, `valor`).
- **Direção de correção:** Indexar `scans_by_table` por `table.lower()` (mantendo `None` separado), normalizar lookup de colunas por `name.lower()` em `_merge_scan`, `_read_flag` e `_finalize_source`.

#### R5-6 — Mesma tabela em caixas diferentes entre planos cria fontes duplicadas
- **Severidade:** Média
- **Onde:** `src/spark_lineage/sources.py` (linha 240).
- **Problema:** `self.sources.setdefault(tname, ...)` cria duas entradas distintas para `DEV.SRC.TB` e `dev.src.tb`.
- **Direção de correção:** Indexar `self.sources` por chave normalizada `tname.lower()` (preservando a grafia original no campo `"table"`).

#### R5-7 — `_md_events` lança `KeyError: 'query'` em eventos sem chave `query`
- **Severidade:** Média
- **Onde:** `src/spark_lineage/sources.py` (linhas 508–515, 725–729).
- **Problema:** `_normalize_event` devolve `e` diretamente quando `"detail" in e and len(e) <= 4` mesmo que `"query"` não exista; em seguida `_md_events` faz `e["query"]` e quebra com `KeyError`.
- **Direção de correção:** Garantir `e.get("query", "")` em `_md_events` (ou exigir `"query" in e` no atalho de `_normalize_event`).

---

### Parser de Planos (`plan_parser.py`)

#### R5-8 — `Scan ExistingRDD` não é ignorado e gera fonte fantasma
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/plan_parser.py` (linhas 197, 433, 470).
- **Problema:** DataFrames criados em memória (`spark.createDataFrame` / RDD) aparecem como `Scan ExistingRDD[id#1,nome#2]`. Diferentemente de `LocalTableScan` e `OneRowRelation`, `ExistingRDD` não é filtrado em `parse_relations`, `_formatted_header` nem `_parse_physical_scan`, gerando uma fonte fantasma `<relação sem nome (ExistingRDD)>`.
- **Direção de correção:** Ignorar `ExistingRDD` (e `InMemoryTableScan`) nos três pontos.

#### R5-9 — `_parse_attr` corrompe `#` sem ID e `parse_read_schema` corrompe `:` entre crases
- **Severidade:** Média
- **Onde:** `src/spark_lineage/plan_parser.py` (linhas 135–140, 371–373).
- **Problema:**
  - `_parse_attr("`cod#ref`")` faz `rpartition("#")`, vê que `ref\`` não é dígito e ainda assim descarta `#ref`, retornando `{"name": "cod", "id": None}`.
  - `parse_read_schema("ReadSchema: struct<`meta:id`:string>")` usa `field.partition(":")` e quebra no `:` interno, retornando `{"meta": "id`:string"}`.
- **Direção de correção:** Em `_parse_attr`, só remover `#rest` quando `digits.isdigit()` for verdadeiro; em `parse_read_schema`, localizar o `:` fora de crases `` `...` ``.

#### R5-10 — `parse_merge_targets` falha em `MergeIntoCommand` sem `SubqueryAlias`
- **Severidade:** Média
- **Onde:** `src/spark_lineage/plan_parser.py` (linhas 35–37).
- **Problema:** `_MERGE_RE` exige literalmente `SubqueryAlias [^,]+, SubqueryAlias [^,]+, ...`, retornando `[]` quando o MERGE não usa aliases e o plano contém `Relation ...` diretamente.
- **Direção de correção:** Tornar `_MERGE_RE` agnóstica ao nó da origem/destino antes de `, <table>, Delta[` (ex.: capturar o token imediatamente anterior a `, Delta[` após `MergeIntoCommand\w*`).

---

### Rastreio de Impacto (`impact.py`)

#### R5-11 — `_aggregate` ignora agregações globais sem `GROUP BY`
- **Severidade:** Crítica / Alta
- **Onde:** `src/spark_lineage/impact.py` (linhas 362–366).
- **Problema:** Quando não há `GROUP BY`, o Catalyst emite `Aggregate [sum(valor#2) AS total#10]` (apenas **1** grupo `[...]`). `_aggregate` faz `if len(groups) < 2: return`, não registra a agregação, não rastreia `total#10`, e o fallback devolve a coluna original `valor` (`direct=True`) como se nenhuma agregação tivesse ocorrido.
- **Direção de correção:** Tratar `len(groups) == 1` como `grp_str, agg_str = "", groups[0]`.

#### R5-12 — `_extract_bracket_groups` não ignora `]`/`[` dentro de strings literais
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/impact.py` (linhas 45–60).
- **Problema:** Em `Project [regexp_replace(texto#1, ']', '') AS limpo#10, valor#2 AS vl_final#11]`, o `]` dentro da string `']'` fecha o bloco prematuramente e descarta todas as colunas após ele (`valor` desaparece).
- **Direção de correção:** Ignorar caracteres dentro de literais `'...'` e crases `` `...` `` (respeitando escapes `\'` / `''`), como já feito em `plan_parser._is_balanced`.

#### R5-13 — Subqueries (`scalar-subquery#id []` e `list#id []`) quebram a propagação de impacto
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/impact.py` (linhas 310–318, 343–360).
- **Problema:** No Catalyst, subqueries em `Project` (`scalar-subquery#10 [] AS media#11`) e em `Filter` (`id#1 IN (list#10 [])`) aparecem como filhos extras (`children[:-1]`) do nó pai. Como `#10` não é o Expression ID da coluna interna da subquery, `self._used(...)` retorna vazio e o impacto é perdido.
- **Direção de correção:** Associar os filhos de subquery (`node.children[:-1]`) às referências `scalar-subquery#id` / `list#id` (ou propagar IDs rastreados nos `output_attrs` dos filhos de subquery para a expressão que os referencia).

#### R5-14 — `_project_kind` erra em `cast(x as tipo) + expr` e em alias apenas de caixa (`VALOR AS valor`)
- **Severidade:** Média
- **Onde:** `src/spark_lineage/impact.py` (linhas 185–199).
- **Problema:**
  - `strip_ids(rhs).lower().startswith("cast(")` marca `cast(valor as double) + 10.0` como `kind="cast"` com `direct=True` (deveria ser `derive` com `direct=False`).
  - `_is_pure_passthrough` compara `==` case-sensitive, classificando `VALOR#1 AS valor#2` como `derive` (`direct=False`).
- **Direção de correção:** Só classificar como `"cast"` quando `rhs` inteiro for um único `cast(...)` fechado no final (sem operadores externos); e comparar case-insensitively em `_is_pure_passthrough`.

#### R5-15 — Multi-hop assimétrico (`cat.sch.stg` -> `sch.stg`), `_merge_destination` e `catalog.get` case-sensitive
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/impact.py` (linhas 268, 604–607, 636).
- **Problema:**
  1. `_relation` só checa `_match_table(rel["table"], self.source_table)`: se o hop 1 gravou em `dev.stg.intermediaria` e o hop 2 lê `stg.intermediaria`, `_match_table("stg.intermediaria", "dev.stg.intermediaria")` é `False` e o rastreio para no hop 1.
  2. `_merge_destination` usa `d["table"]` sem normalizar caixa, duplicando `DEV.TGT.DST` e `dev.tgt.dst`.
  3. `trace_column_impact` chama `catalog.get(resolved_table)` case-sensitive, perdendo `type` e `comment`.
- **Direção de correção:** Checar `_match_table(rel["table"], self.source_table) or _match_table(self.source_table, rel["table"])`, indexar `dest_summary` por `d["table"].replace("`", "").strip().lower()`, e fazer lookup case-insensitive no `catalog`.

#### R5-16 — `Except` / `Intersect` silenciam colunas do lado direito (`children[1]`)
- **Severidade:** Média
- **Onde:** `src/spark_lineage/impact.py` (linhas 471–474).
- **Problema:** `_set_op` apenas copia `children[0]`; quando a coluna rastreada pertence a `children[1]`, ela filtra/exclui linhas do resultado mas nenhuma operação/filtro é registrada (`operations_count == 0`).
- **Direção de correção:** Verificar se algum atributo de `children[1]` está em `self.tracked` e registrar uma operação/filtro correspondente (ex.: `except`/`intersect`) na lista de transformações ou filtros.

---

### Captura, AST, SQL e Notebooks (`capture_plans.py`)

#### R5-17 — `_execute_forced_pass` corrompe `call_site` quando um ternário (`ast.IfExp`) lança exceção
- **Severidade:** Alta (regressão do R4-4)
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1084, 2568–2571).
- **Problema:** A correção do R4-4 alterou `site_id` de `visit_IfExp` para `f"{self.rel_file}:{node.lineno}:{node.col_offset}"` (ex.: `"tern_err.py:2:4"`), mas `_execute_forced_pass` ainda faz `site_id.rpartition(":")`, gravando `{"file": "tern_err.py:2", "line": 4}`.
- **Direção de correção:** Parsear `site_id` tratando o sufixo opcional `:col_offset` (ex.: `re.match(r"^(.*):(\d+)(?::\d+)?$", site_id)`).

#### R5-18 — `_preprocess_ipynb_source` trata operadores Python multilinha `!=` e `%` como magics
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 972–983).
- **Problema:** Em código formatado pelo PEP 8 / Black / Ruff:
  ```python
  ativo = (
      status
      != "CANCELADO"
  )
  resto = (
      total
      % 10
  )
  ```
  `stripped.startswith("!")` captura `!= "CANCELADO"` e o transforma em `__spark_lineage_magic__('sh', ...)` (`SyntaxError`), enquanto `stripped.startswith("%")` captura `% 10` e o apaga para `#` (corrupção silenciosa!).
- **Direção de correção:** Só tratar linha como magic `%...` quando começar por `%[A-Za-z_]` (ou `%%[A-Za-z_]`), e só tratar `!` como shell magic quando **não** começar por `!=` (e idealmente fora de parênteses/colchetes/chaves abertos).

#### R5-19 — `Capturer.rows` (e `pandas`) colidem schemas distintos no mesmo `call_site` e `rows` devolve lista mutável
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 1618–1625, 1648–1660).
- **Problema:**
  1. `_row_cache` e `_pandas_cache` usam apenas `call_site` como chave. Se uma função helper ou loop chama `df.collect()` / `df.toPandas()` para DataFrames com schemas diferentes, na passada seguinte (`has_flipped=False`) o cache devolve a linha do último schema visto para o primeiro DataFrame.
  2. `rows()` devolve `self._row_cache[ck]` diretamente (sem cópia): mutações como `rows.clear()` ou `rows.pop()` corrompem o cache entre passadas.
- **Direção de correção:** Incluir a assinatura do schema (`tuple((f.name, f.dataType.simpleString()) for f in df.schema.fields)`) na chave de cache de `rows()` e `pandas()`, e devolver `list(self._row_cache[key])` (cópia rasa).

#### R5-20 — Comentários SQL quebram `_rewrite_safe_temp_view_sql` e `createGlobalTempView` não é patcheado
- **Severidade:** Alta
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 500–509, 1941, 2090–2097).
- **Problema:**
  1. `classify_sql` remove comentários antes de casar `_CACHE_TABLE_AS_RE` e `_TEMP_VIEW_RE`, mas `_rewrite_safe_temp_view_sql` roda sobre a query bruta com comentários. Assim, `CACHE TABLE vw AS /* c */ SELECT ...` e `CREATE /* c */ TEMP VIEW ...` passam sem reescrita (executando `CACHE TABLE` de verdade no Spark ou falhando na 2ª passada).
  2. `_patch_dataframes` patcheia `createTempView`, mas não `createGlobalTempView`, que falha com `TEMP_TABLE_OR_VIEW_ALREADY_EXISTS` na 2ª passada do `BranchExplorer`.
- **Direção de correção:** Aplicar a reescrita de `CACHE_TABLE_AS` e `CREATE_TEMP_VIEW` sobre `_strip_sql_comments(query)` (ou aceitar comentários entre tokens nas regexes), e redirecionar `createGlobalTempView` -> `createOrReplaceGlobalTempView`.

#### R5-21 — SQL: `repr()` em `substitute_sql_params`, `'DIRECTORY'` em multi-table insert e falsas fontes em `extract_sql_sources`
- **Severidade:** Média
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 343–348, 568, 625).
- **Problema:**
  1. `repr("O'Brien")` gera `"O'Brien"` com aspas duplas (que no Spark ANSI SQL é interpretado como identificador, não string). O correto é `'O''Brien'`.
  2. `_SQL_INSERT_TARGET_RE` não tem `(?!DIRECTORY\b)(?!LOCAL\b)`, extraindo `'DIRECTORY'` em `FROM src INSERT OVERWRITE DIRECTORY '/mnt/out' ...`.
  3. `_SQL_FROM_RE` captura `dt_ref` em `EXTRACT(YEAR FROM dt_ref)` e `DELTA` em `CREATE TABLE dst USING DELTA`.
- **Direção de correção:** Escapar strings SQL com `"'" + val.replace("'", "''") + "'"`, alinhar `_SQL_INSERT_TARGET_RE` com `_SQL_TARGET_RE`, e ignorar `FROM` dentro de `EXTRACT(...)`/`TRIM(...)` e formatos comuns (`DELTA`, `PARQUET`, `CSV`, `JSON`, `ORC`, `ICEBERG`, `AVRO`, `TEXT`, `JDBC`, `HIVE`) após `USING` em DDL `CREATE TABLE`.

#### R5-22 — Magics e `%run`: `%timeit` de linha única, `# MAGIC %md` indentado, comentário inline em `%run` e ciclos de `%run`
- **Severidade:** Média
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 793–794, 817–826, 2429–2439, 2717–2737).
- **Problema:**
  1. Em `_process_magic_block`, apenas `s.startswith("%time ")` extrai `inline_py`; `%timeit <stmt>` de linha única continua sendo apagado.
  2. `# MAGIC %md` dentro de um bloco `if:`/`def:` vira `#` vazio -> `IndentationError`.
  3. `_parse_magic_run_args("./helper $dt=2026-01-01 # comentario")` mantém `# comentario` no caminho do arquivo.
  4. `exec_magic_run` (e `run_child_notebook`) não rastreiam a pilha ativa de notebooks, estourando `RecursionError` em ciclos (`nb_a -> nb_b -> nb_a`).
- **Direção de correção:** Suportar `%timeit ` (removendo eventuais flags `-n`/`-r`) em `inline_py`; emitir `pass` se um bloco `%md` indentado for o único corpo ou preservar estrutura; descartar `#...` fora de aspas em `_parse_magic_run_args`; manter um `set[Path]` de notebooks em execução na pilha para quebrar ciclos com aviso.

#### R5-23 — Crases em Delta/`mergeInto`, opções maiúsculas em `_reader_targets` e `_split_sql_statements`
- **Severidade:** Média
- **Onde:** `src/spark_lineage/capture_plans.py` (linhas 710–711, 727, 2086, 2178, 2255).
- **Problema:**
  1. `_delta_table_target` e `_make_merge_into_factory` não chamam `_normalize_target`, preservando crases (`` `dev`.`tgt`.`dst` ``).
  2. `_reader_targets` checa apenas `opts.get("path")` em minúsculas, ignorando `.option("PATH", ...)` / `.option("dbTable", ...)`.
  3. `_split_sql_statements` define `cur_start_line` na linha de um `/* comentário */` inicial (deslocando o número da linha reportado no `call_site`) e trata `\` como escape dentro de crases `` `...` `` (divergindo de `_strip_sql_comments`).
- **Direção de correção:** Usar `_normalize_target` em `_delta_table_target` e `_make_merge_into_factory`; iterar `opts.items()` case-insensitively em `_reader_targets`; só marcar `cur_start_line` ao encontrar token real fora de comentários em `_split_sql_statements` e condicionar escape `\` a `in_quote != "`"`.

---

## Como Executar os Testes

```bash
# Suíte completa (104 regressões das R1-R4 passando + 42 novos bugs da R5 como xfail strict)
pytest -v

# Exibir as falhas detalhadas dos 42 testes da Rodada 5
pytest tests/test_known_bugs_r5.py --runxfail -v
```
