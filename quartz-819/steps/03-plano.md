# Etapa 3: Plano de Alteração

Com base no Diagnóstico e nas decisões confirmadas pelo usuário, elabore um plano de alteração detalhado, objetivo e auditável sem modificar nenhum arquivo, notebook ou tabela nesta etapa.

**Antes de montar o plano:**
- Leia `references/normalizacao.md` para mapear exatamente as decisões confirmadas para os parâmetros da função padrão `normalizar_compe` (PySpark) ou para os exemplos de referência em Spark SQL.

## O que o Plano Deve Cobrir

### 1. Estratégia de Normalização
- **PySpark / Notebooks**:
  - Confirme o local/empacotamento da função `normalizar_compe` definido pelo usuário (módulo compartilhado em Repos, `%run`, wheel ou célula dedicada no notebook, sem duplicação).
  - Mostre a chamada prevista com os parâmetros exatos derivados das decisões (`prioridade_conflito`, `sanitizar`, `acao_st_invalida`, `acao_legada_invalida`, `descartar_st` e `col_saida` quando aplicável).
  - Diferencie explicitamente as chamadas para:
    - Origens `*_ing`: informar `col_st` (ou omitir quando seguir `{col_legada}_st`).
    - Origens não-`_ing` defensivas: usar `col_st=""` para ignorar busca por `*_st`.
    - Destinos que adicionam `*_st` mantendo a coluna legada: informar `col_saida="<col>_st"` e especificar como a coluna legada `INT` será preenchida para códigos alfanuméricos.
- **Spark SQL**:
  - Apresente o padrão de expressão SQL baseado nos exemplos de `references/normalizacao.md` adaptado às decisões do usuário (incluindo proteção ANSI quando necessário).
  - Registre explicitamente que o SQL estático não detecta dinamicamente a existência das colunas, exigindo ajuste manual futuro caso `col_st` ainda não exista ou quando a coluna legada for removida.

### 2. Mapa de Alterações por Arquivo / Notebook / Célula
Para cada arquivo, notebook ou configuração impactado, descreva objetivamente o que será alterado:
- **Leituras / Origens**: ponto exato onde a normalização será aplicada (logo após a leitura ou após filtros nativos).
- **Filtros, Literais e Regras**: conversão de literais numéricos, revisão de comparações de faixa/ordenação (`<`, `>`, `BETWEEN`, `ORDER BY`), `CASE WHEN`, chaves compostas/hash, `CAST` explícito, schemas fixos (`StructType`/DDL), arquivos de configuração e, quando definido, filtro nativo pré-normalização (`col = 1 OR col_st = '001'`) para preservar *file pruning*.
- **Joins e Unions**: garantia de que todos os lados envolvidos estejam normalizados como `STRING` de 3 caracteres antes de `JOIN`, `UNION` ou `GROUP BY`, eliminando casts implícitos.
- **Escritas / Destinos (Delta Lake / Exportações)**:
  - Alteração para `STRING`: incluir `overwriteSchema` (quando `OVERWRITE`) ou ajustar DDL/recriação/`MERGE` conforme o modo de escrita.
  - Adição de `*_st`: incluir `mergeSchema` / projeção mantendo a coluna legada (com a regra para códigos alfanuméricos).
  - Ajustes em exportações externas (CSV, CNAB/posicional, APIs) ou Streaming/Auto Loader, se aplicável.

### 3. Impactos Fora do Código *(incluir somente os aplicáveis)*
- **Histórico / Backfill**: necessidade de recriação ou backfill em destinos `APPEND`/`MERGE` que migram de `INT` para `STRING`.
- **Dependências não-`_ing` não alteradas**: lista de tabelas mantidas como estão para acompanhamento futuro.
- **Consumidores downstream**: impacto em views, dashboards ou pipelines que leem a tabela de destino cujo schema será alterado.

### 4. Mitigações de Performance *(incluir somente se houver risco de performance mapeado)*
- Liste as medidas concretas adotadas no plano (ex.: filtro nativo antes da normalização em colunas de partição/`CLUSTER BY`, normalizar uma única vez por DataFrame, uso de `sem_validacao` quando aprovado).

## Fechamento da Etapa 3 (Saída Padronizada e Gate)
Apresente o plano consolidado seguindo a estrutura abaixo:

```markdown
### Plano de Alteração Proposto

#### 1. Estratégia de Normalização
- **Tecnologia**: `<PySpark / Spark SQL / Misto>`
- **Local / Empacotamento de `normalizar_compe`** *(PySpark)*: `<módulo em Repos / %run / célula do notebook>`
- **Chamadas / Expressões previstas**:
  - Origens `*_ing`: `<chamada normalizar_compe(...) com parâmetros ou snippet SQL>`
  - Origens não-`_ing` *(se aplicável)*: `<chamada normalizar_compe(..., col_st="", ...) ou snippet SQL>`
  - Destino com `*_st` *(se aplicável)*: `<uso de col_saida e regra para coluna legada INT em alfanuméricos>`

#### 2. Mapa de Alterações por Arquivo / Célula
| Arquivo / Notebook (Célula) | Tipo de Alteração | Detalhe do que será feito |
| :--- | :--- | :--- |
| `<caminho/arquivo>` | Leitura / Filtro / Join / Schema / Escrita | `<descrição objetiva da alteração>` |

#### 3. Impactos Fora do Código *(se aplicável)*
- `<backfill / recriação / DDL / dependências não-_ing / consumidores downstream>`

#### 4. Mitigações de Performance *(se aplicável)*
- `<medidas aplicadas no plano para preservar file pruning e reduzir custo>`
```

Encerre perguntando explicitamente se o usuário aprova o plano proposto ou deseja ajustar algum ponto antes de iniciar a implementação.
