# spark-lineage

CLI e biblioteca Python para captura de planos de execução Spark/Databricks **sem executar gravações ou DML**, gerando um artefato compactado por job pronto para consultas offline e consumo por LLMs — com mapeamento completo de fontes, destinos, queries SQL, ramos de decisão e impacto multi-hop coluna a coluna.

## Principais recursos

- **Captura segura sem efeito colateral:** intercepta `write.*`, `writeTo.*`, `writeStream.*` (incluindo `foreachBatch`), `DeltaTable.merge`, `df.mergeInto`, DMLs SQL (`INSERT`, `MERGE`, `UPDATE`, `DELETE`, `CREATE`) e comandos de cache/sessão (`df.cache`/`persist`, `Catalog.cacheTable`, `spark.stop`) antes da execução.
- **Suporte a múltiplos formatos de entrypoint:** scripts `.py`, arquivos/notebooks `.sql`, notebooks Databricks exportados em `.py` (`# MAGIC %sql`, `%run`, `%python`, `%md`, `dbutils.widgets`, `dbutils.notebook.run`) e notebooks Jupyter `.ipynb`.
- **Exploração automática e paralela de ramos (`BranchExplorer`):** instrumenta `if/else` e expressões ternárias (`a if cond else b`) em memória via AST, executa passadas em paralelo (`--branch-workers`, padrão `4`) e poda automaticamente funções *sink* puras (como helpers de log sem retorno e sem Spark).
- **Resiliência a arquivos ausentes e staging no mesmo job:**
  - Tabelas e caminhos de arquivo gravados durante a captura (`saveAsTable`, `write.csv`/`parquet`/`json`/`save`) são preservados em memória para re-leituras posteriores no mesmo job (`tableExists`, `spark.table`, `spark.read.csv/...`).
  - Quando a leitura de um arquivo externo (`csv`, `parquet`, `json`, `orc`, `text`, `load`) falha por `PATH_NOT_FOUND`/permissão, cria automaticamente um `DataFrame` Spark vazio usando `.schema(...)` ou inferindo as colunas referenciadas na AST (preservando `join`s e `TempView`s com tabelas do catálogo e registrando aviso em `events`/`warnings`).
- **Artefato compactado por padrão (`spark-lineage-compact/v1`):** o `capture` descarta os textos brutos extensos de `plans[*].sections` após pré-calcular `sources`, `views`, `destinations`, `flows`, `branches`, `queries` e `column_lineage` — reduzindo drasticamente o tamanho do JSON enquanto mantém `sources`, `query` e `impact` 100% funcionais offline.

## Requisitos e Instalação

- **Python 3.10 a 3.12** (recomendado **Python 3.12** para Databricks Runtime 16+ / Serverless).
- `databricks-connect` já inclui sua própria versão compatível de `pyspark` e `delta.tables` — não instale `pyspark` ou `delta-spark` avulsos no mesmo ambiente virtual.

```bash
pip install -r requirements.txt
pip install -e .
```

## Uso no terminal (CLI)

Após instalar, o comando `spark-lineage` (ou `python -m spark_lineage`) fica disponível:

### 0. Diagnosticar o ambiente (`doctor`)

Verifica versão do Python, bibliotecas instaladas, conflito com `pyspark` avulso, configuração Databricks (`~/.databrickscfg` / variáveis de ambiente incluindo PAT e OAuth M2M) e, opcionalmente, testa a conexão real com o compute:

```bash
spark-lineage doctor
spark-lineage doctor --connect --profile DEFAULT
spark-lineage doctor --format json
```

### 1. Capturar linhagem de um job (`capture`)

Por padrão, grava o arquivo **compactado** (`spark-lineage-compact/v1`) em `--output`, já contendo fontes, destinos, fluxos, ramos, queries SQL e impacto por coluna pré-calculados:

```bash
# Captura padrão (compacta, 4 workers paralelos para ramos, com Unity Catalog)
spark-lineage capture \
    --project-root ./meu_projeto \
    --entrypoint jobs/etl_vendas.py \
    --output lineage.json \
    --param data_ref=2026-11-01

# Ajustar paralelismo de ramos (1 = sequencial) e salvar também o bruto para depuração
spark-lineage capture \
    --project-root ./meu_projeto \
    --entrypoint notebooks/pipeline.ipynb \
    --output lineage.json \
    --raw-output plan_raw.json \
    --branch-workers 4 \
    --max-branch-passes 10

# Salvar o JSON bruto diretamente em --output (modo legado)
spark-lineage capture \
    --project-root ./meu_projeto \
    --entrypoint sql/carga.sql \
    --output plan_raw.json \
    --raw
```

### 2. Consultar fontes, destinos, fluxos, queries e ramos (`sources` / `query`)

Funciona 100% offline tanto sobre o arquivo compactado quanto sobre o bruto (saída padrão em JSON no stdout; use `--format md` para Markdown):

```bash
# Resumo geral (fontes, destinos, fluxos, views, eventos e avisos)
spark-lineage sources --plans lineage.json

# Consultar tags específicas (com filtro opcional por tabela ou sufixo)
spark-lineage query --plans lineage.json --tag sources --table tb_banco
spark-lineage query --plans lineage.json --tag destinations
spark-lineage query --plans lineage.json --tag flows --table tb_vendas
spark-lineage query --plans lineage.json --tag queries
spark-lineage query --plans lineage.json --tag branches
spark-lineage query --plans lineage.json --tag column_lineage --table tb_banco

# Atualizar tipos/comentários no Unity Catalog ao vivo
spark-lineage sources --plans lineage.json --catalog --format md
```

Tags válidas: `all`, `sources`, `destinations`, `flows`, `views`, `branches`, `queries`, `column_lineage`, `events`, `warnings`, `catalog`, `plans`.

### 3. Rastrear impacto multi-hop de uma coluna (`impact`)

Mostra todos os filtros, joins, transformações (`rename`, `cast`, `derive`, `aggregate`, `window`, `group_by`, `union`, `expand`, `generate`) e destinos afetados através de múltiplas etapas (staging -> gold), lendo direto do arquivo compactado ou bruto:

```bash
spark-lineage impact \
    --plans lineage.json \
    --table tb_banco \
    --column cbco

# Em Markdown
spark-lineage impact --plans lineage.json --table tb_banco --column cbco --format md
```

### 4. Compactar, filtrar ou renderizar resumo para LLM (`compact`)

Converte um `plan_raw.json` antigo para o formato compactado, filtra por tabelas/colunas específicas ou renderiza um Markdown consolidado pronto para enviar a uma LLM:

```bash
# Gerar Markdown consolidado do job inteiro (fontes + destinos + queries + ramos + linhagem por coluna)
spark-lineage compact --plans lineage.json --format md

# Filtrar apenas tabelas e colunas de interesse
spark-lineage compact \
    --plans lineage.json \
    --table tb_banco \
    --columns cbco,nm_banco \
    --format md \
    --output resumo_llm.md
```

## Uso como biblioteca Python

```python
from spark_lineage import (
    build_sources,
    compact_plan,
    render_compact_markdown,
    run_doctor,
    trace_column_impact,
)
from spark_lineage.capture_plans import capture_plans

# 0. Diagnóstico do ambiente
print(run_doctor(connect=False))

# 1. Captura planos (requer sessão Spark / Databricks Connect)
raw = capture_plans("./meu_projeto", "jobs/etl_vendas.py", branch_workers=4)

# 2. Compacta o resultado (sem sections brutos, com queries e column_lineage pré-calculados)
compact = compact_plan(raw)
print(render_compact_markdown(compact))

# 3. Consultas offline (funcionam igualmente sobre `compact` ou `raw`)
doc = build_sources(compact)
impact = trace_column_impact(compact, table="tb_banco", column="cbco")
```

## Estrutura do pacote

- `src/spark_lineage/capture_plans.py`: monkey-patching de actions/writers/readers/Delta/Catalog, pré-processamento de `.sql`/notebooks/`.ipynb`, `BranchExplorer` paralelo com poda de funções sink e fallback de leitura de arquivos.
- `src/spark_lineage/plan_parser.py`: parser puro (sem Spark/rede) de planos Catalyst (`Relation`, `RelationV2`, `View`, `MergeIntoCommand*`, `PhotonScan`/`FileScan`/`BatchScan`, `explain(mode="formatted")`).
- `src/spark_lineage/sources.py`: consolidação de fontes, destinos, fluxos, queries, eventos, avisos e renderização Markdown/JSON por tag.
- `src/spark_lineage/impact.py`: rastreio multi-hop de impacto por coluna (`Filter`, `Join`, `Project`, `Aggregate`, `Window`, `Generate`, `Expand`, `Union`, `Except`/`Intersect`, CTEs e subqueries).
- `src/spark_lineage/compact.py`: geração e renderização do artefato compactado `spark-lineage-compact/v1`.
- `src/spark_lineage/doctor.py`: diagnóstico de ambiente Python, pacotes e configuração/autenticação Databricks.
- `src/spark_lineage/cli.py`: CLI unificada (`doctor`, `capture`, `sources`, `query`, `impact`, `compact`).

## Testes

```bash
# Com pytest:
pytest -v

# Ou executando cada módulo diretamente com o Python do ambiente virtual:
python tests/test_plan_parser.py
python tests/test_capture_e2e.py
python tests/test_branch_optimizations.py
python tests/test_reader_fallback.py
python tests/test_compact.py
```
