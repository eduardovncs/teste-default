# Etapa 2: Perguntas

Com base no Diagnóstico confirmado (e nos achados do Perfilamento, se executado), colete as decisões do usuário sobre regras de negócio, tratamento de exceções, schema de saída e trade-offs de performance.

**Regras gerais:**
- Faça apenas os blocos relevantes para o que foi encontrado no Diagnóstico, agrupados de forma objetiva.
- Quando o Perfilamento tiver sido executado, use os números reais encontrados para contextualizar cada pergunta.
- Opcionalmente, ofereça os **defaults recomendados** (`prioridade_conflito="st"`, `sanitizar=True`, `acao_st_invalida="fallback"`, `acao_legada_invalida="null"`, `descartar_st=False`) para o usuário aprovar em bloco ou ajustar apenas o que desejar.

## Bloco A — Regras de convivência nas origens `*_ing` (`col` + `col_st`)
*(Aplique somente quando houver ao menos uma origem `*_ing` no escopo. Antes de listar as perguntas, avise brevemente que acumular validações — sanitização, regex, checagem de faixa e comparação dupla — aumenta o custo em tabelas volumosas.)*

1. **Conflito entre colunas válidas**: se `col` (convertida) e `col_st` vierem ambas preenchidas e válidas, mas diferentes, qual prevalece?
   - Priorizar `col_st` (`st`)
   - Priorizar `col` (`legada`)
   - Atribuir `NULL` (`null`)
   - Falhar o job (`erro`)
2. **Sanitização de colunas `STRING`**: aplicar `UPPER(TRIM(...))` antes de validar `^[A-Z0-9]{3}$`, ou validar estritamente o valor bruto?
   - Aplicar `UPPER(TRIM)` (`True`)
   - Validar valor bruto (`False`)
3. **Quando `col_st` vier preenchida mas inválida** (ex.: `'000'`, `'12'`, `'1234'`, espaços ou caracteres inválidos):
   - Fazer fallback para a coluna legada `col` (`fallback`)
   - Converter para `NULL` (`null`)
   - Filtrar/descartar o registro — *aviso: descarta linhas silenciosamente* (`filtrar`)
   - Falhar o job (`erro`)
4. **Validação da coluna numérica legada (`col`)**: como tratar valores fora da faixa `1..999` (ou decimais com fração)?
   - Converter para `NULL` (`null`)
   - Filtrar/descartar o registro — *aviso: descarta linhas silenciosamente* (`filtrar`)
   - Falhar o job — *atenção: se `0` na origem significar "não informado", o job falhará* (`erro`)
   - Converter sem validar faixa por performance — *preserva valores fora de `0..999` sem truncar* (`sem_validacao`)
5. **Coluna `col_st` residual após normalizar**: manter `col_st` no DataFrame (`descartar_st=False`) ou removê-la após normalizar para a coluna alvo (`descartar_st=True`, recomendado quando há `SELECT *` ou escrita direta)?

## Bloco B — Origens fora de `*_ing`
*(Pergunte somente se houver origens não-`_ing` no Diagnóstico.)*

- Como essas tabelas tendem a migrar *in-place* de `INT` para `STRING` no futuro, qual estratégia adotar?
  - **Normalização defensiva**: aplica `normalizar_compe` ignorando `*_st` (`col_st=""`), compatível tanto com `INT` atual quanto com futura mudança *in-place* para `STRING`; ou
  - **Manter sem alteração** por enquanto e apenas documentar a dependência.
- **Se escolher normalização defensiva e o Bloco A não tiver sido aplicado** (pipeline sem origens `*_ing`), defina também:
  - Como tratar valores inválidos na coluna legada (`null` / `filtrar` / `erro` / `sem_validacao`); e
  - Se deve aplicar sanitização `UPPER(TRIM)` quando a coluna vier como `STRING` (`True` / `False`).
- Se a coluna for usada em `JOIN` ou `WHERE` em tabela volumosa, sinalize o impacto de performance.

## Bloco C — Tabelas de destino, Delta Lake e histórico
*(Pergunte somente se houver tabela de destino mapeada no Diagnóstico — onde a coluna de código de banco é efetivamente gravada.)*

- Qual estratégia adotar no schema de saída?
  - **Alterar a coluna de destino para `STRING`**: lembre que em `APPEND`/`MERGE` o Delta falha por incompatibilidade de schema (exigindo recriação ou backfill), e em `OVERWRITE` exige `overwriteSchema`.
  - **Adicionar uma coluna `*_st` na saída** (usando `col_saida`) mantendo a coluna antiga por compatibilidade com consumidores downstream (alertando sobre `mergeSchema`). **Se escolher esta opção**, defina também o que gravar na coluna legada `INT` quando o código for alfanumérico (`NULL`, `0` ou manter valor original).

## Bloco D — Filtros, literais e empacotamento
- *(Somente se houver filtros/regras com literais ou comparações numéricas no Diagnóstico)*:
  - Confirmar conversão dos literais (`1` → `'001'`, `IN (1, 33)` → `('001', '033')`, revisão de `> 0` / `!= 0` / faixas).
  - Em `WHERE` sobre tabelas volumosas, particionadas ou com Liquid Clustering, perguntar se prefere preservar filtros nativos nas colunas originais (`col = 1 OR col_st = '001'`) antes da normalização para manter *file pruning*.
- *(Somente em PySpark/Notebooks)*: confirmar onde disponibilizar a função `normalizar_compe` (ex.: módulo utilitário compartilhado em Repos/`%run`/wheel ou célula dedicada no notebook).

## Fechamento da Etapa 2 (Saída Padronizada e Gate)
Ao receber as respostas do usuário, apresente o resumo consolidado seguindo a estrutura abaixo:

```markdown
### Resumo das Decisões Definidas

#### 1. Origens `*_ing` (Convivência `col` + `col_st`) *(se aplicável)*
- **Prioridade em divergência (`prioridade_conflito`)**: `<st / legada / null / erro>`
- **Sanitização de `STRING` (`sanitizar`)**: `<True (UPPER/TRIM) / False (bruto)>`
- **Ação quando `col_st` é inválida (`acao_st_invalida`)**: `<fallback / null / filtrar / erro>`
- **Ação quando `col` legada é inválida (`acao_legada_invalida`)**: `<null / filtrar / erro / sem_validacao>`
- **Coluna `col_st` residual (`descartar_st`)**: `<False (manter) / True (remover)>`

#### 2. Origens não-`_ing` *(se aplicável)*
- **Estratégia**: `<normalização defensiva / manter e documentar>`
- **Parâmetros da defensiva *(se Bloco A não foi aplicado)***: `acao_legada_invalida=<...>`, `sanitizar=<...>`

#### 3. Tabelas de Destino *(se aplicável)*
- **Estratégia de schema / Delta / histórico**: `<alterar para STRING / adicionar *_st (col_saida)>`
- **Valor na coluna legada `INT` para códigos alfanuméricos *(se adicionar `*_st`)***: `<NULL / 0 / manter original>`

#### 4. Filtros, Literais e Empacotamento *(se aplicável)*
- **Literais e filtro nativo (*file pruning*)**: `<escolha>`
- **Local da função `normalizar_compe`** *(PySpark)*: `<módulo compartilhado / %run / célula no notebook>`

#### 5. Alertas de Performance *(incluir somente se o Diagnóstico apontou risco de performance E as escolhas acima agravarem esse ponto)*
- `<descrição objetiva do impacto esperado em joins/file pruning/custo e sugestão opcional de simplificação>`
```

Peça confirmação explícita desse resumo antes de prosseguir.
