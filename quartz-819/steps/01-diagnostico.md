# Etapa 1: Diagnóstico

Analise o processo de ponta a ponta sem modificar nenhum arquivo ou tabela. O objetivo é mapear impacto funcional, de schema/histórico e riscos de performance.

## 1.1 Descoberta e classificação de origens e destinos
Inspecione todos os notebooks, scripts (PySpark e Spark SQL) e arquivos de configuração (YAML/JSON/parâmetros de job) do processo. Classifique cada tabela (ou fonte externa como Parquet/CSV/JSON em volumes/storage, JDBC, API, Kafka ou temp view) em três grupos:

- **Origens `*_ing`** (tabelas em schemas com sufixo `_ing`):
  - Candidatas à lógica de convivência (`col` + `col_st`).
- **Origens não-`_ing`** (tabelas de outras camadas/times ou fontes fora do catálogo):
  - Tendem a migrar *in-place* de `INT` para `STRING` (sem criação garantida de `*_st`).
- **Tabelas de destino (escrita)**:
  - Considere **apenas** destinos em que a coluna de código de banco é efetivamente gravada (se for usada só em joins/filtros intermediários e não aparecer no schema de saída, ignore o destino).
  - Identifique o modo de escrita específico (`APPEND`, `MERGE`, `OVERWRITE`, `INSERT OVERWRITE`, `replaceWhere` / partição dinâmica, `CREATE OR REPLACE TABLE`, ou exportação para arquivo/API).
  - Sinalize impactos de Delta Lake e histórico: em `APPEND`/`MERGE`, tentar gravar `STRING` sobre coluna `INT` falha por incompatibilidade de schema (migração exige recriação ou backfill); em `OVERWRITE`, alterar `INT` → `STRING` falha sem `overwriteSchema` (e em `replaceWhere`/partição dinâmica, valide a compatibilidade com a versão do Delta).

**Inspeção de metadados (quando houver acesso ao catálogo/metastore):**
- Verifique o tipo atual da coluna e se já existe `*_st`.
- Execute `DESCRIBE DETAIL <tabela>` (custo ~zero) para obter `sizeInBytes`, `partitionColumns` e `clusteringColumns`, classificando o porte como referência: **pequena** (`< 5 GB`), **média** (`5–100 GB`) ou **volumosa** (`> 100 GB`).
- **Plano B (sem acesso ao catálogo):** marque `Tipo Atual`, `*_st Existe?` e `Porte` como `Desconhecido` e forneça os comandos `DESCRIBE TABLE` / `DESCRIBE DETAIL` prontos para o usuário executar, caso queira complementar.

## 1.2 Descoberta de colunas de código de banco
- **Busca primária**: procure colunas, aliases e variáveis contendo `cbco` (case-insensitive, incluindo nomes compostos como `cbco_dest`, `cbco_orig`, cujas colunas `*_st` seguem o padrão `cbco_dest_st`).
- **Busca secundária**: amplie a busca para termos relacionados (`bco`, `banco`, `compe`, `cod_bco`, `num_bco`, `cempr`), inspecionando comentários/descrições quando disponíveis. Descarte falsos positivos como `ispb` (8 dígitos), agência, conta ou código de empresa que não representem banco.
- **Linhagem interna**: rastreie a propagação de cada coluna candidata — aliases (`AS`), renomeações (`withColumnRenamed`), `SELECT *`, CTEs, views temporárias e DataFrames intermediários até a escrita final.

## 1.3 Mapeamento de operações e pontos críticos
Para cada coluna identificada, mapeie onde ela é usada e classifique os riscos:

- **Riscos funcionais, de schema e de dados**:
  - `JOIN`s (especialmente entre origens `*_ing` e não-`_ing`, tabelas lookup/dimensão ou colunas com tipos divergentes).
  - `UNION`, `UNION ALL` e `unionByName` (incompatibilidade de schema ou cast implícito).
  - Filtros, `CASE WHEN` e regras com literais ou comparações numéricas (ex.: `cbco = 341`, `cbco IN (1, 33)`, `cbco > 0`, `cbco != 0`), incluindo listas em arquivos de configuração/widgets.
  - Comparações de faixa e ordenação (`<`, `>`, `BETWEEN`, `ORDER BY`, `MIN`/`MAX` — ordem lexicográfica difere da numérica).
  - `CAST` explícito (`.cast("int")`, `IntegerType()`), schemas fixos (`StructType`, `spark.read.schema(...)`, DDL rígido) e UDFs/pandas UDFs.
  - Chaves compostas/concatenações (`concat(cbco, ...)`) e *hash*/*surrogate keys* (`1` vs `'001'` altera a chave gerada).
  - Escritas nas tabelas de destino mapeadas em 1.1, exportações externas (CSV, layouts posicionais/CNAB, APIs) e pipelines Streaming / Auto Loader / DLT (impacto em checkpoints e evolução de schema).
- **Riscos de performance (sinalize explicitamente)**:
  - `JOIN`s sobre tabelas volumosas (`> 100 GB`) migrando de `INT` para `STRING` (maior consumo de CPU/memória).
  - Filtros (`WHERE` / `.filter()`) onde aplicar transformações na coluna ou causar cast implícito impeça *file pruning* / *data skipping*.
  - Uso da coluna em `PARTITIONED BY` ou `CLUSTER BY` (Liquid Clustering), conforme código ou `DESCRIBE DETAIL`.

## 1.4 Fechamento da Etapa 1 (Saída Padronizada e Gate)
Apresente o resultado seguindo a estrutura abaixo:

```markdown
### Resumo do Diagnóstico

**Arquivos/Notebooks analisados:**
- `<caminho/nome do arquivo ou notebook>`

#### 1. Tabelas e Fontes Mapeadas
| Tabela / Fonte | Papel | Schema `_ing`? | Coluna(s) de Banco | Tipo Atual | `*_st` Existe? | Porte / Partição-Clustering | Modo de Escrita |
|---|---|---|---|---|---|---|---|
| `<catalog.schema.tabela>` | Origem / Destino | Sim / Não / N/A | `cbco` | `INT` / Desconhecido | Sim / Não / Desconhecido | `<ex.: 2.1 TB / dt_ref>` | N/A / `APPEND` / `MERGE` / `OVERWRITE` |

*(Incluir destinos apenas quando a coluna de código de banco for efetivamente gravada.)*

#### 2. Fluxo das Colunas de Código de Banco no Pipeline
*(Listar apenas as colunas de código de banco identificadas e os passos/transformações em que elas participam.)*
- `<tabela.coluna>` → `<transformações / aliases / joins em que a coluna participa>` → `<destino ou uso final>`

#### 3. Pontos Críticos
**Funcionais, Schema e Histórico:**
- `<arquivo/célula>`: `<descrição do risco>`

**Performance (Joins / File Pruning / Clustering):**
- `<arquivo/célula>`: `<descrição do risco>`
```

Ao final do resumo:
1. Pergunte se alguma coluna listada não corresponde a código de banco ou se alguma tabela/coluna relevante ficou de fora.
2. Informe que, se o usuário quiser inspecionar como estão os dados reais (valores `<= 0`, `> 999`, inválidos ou divergências na `*_st`) antes de responder às perguntas, é possível executar a etapa opcional de **Perfilamento de Dados**.
3. Aguarde a confirmação do usuário antes de prosseguir.
