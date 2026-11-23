# Etapa 1b: Perfilamento de Dados *(Opcional)*

Execute esta etapa somente se o usuário optar por inspecionar os dados atuais das tabelas mapeadas no Diagnóstico antes de definir as regras da migração.

O objetivo é coletar evidências reais sobre qualidade dos dados, anomalias e convivência (`col` vs `col_st`) mantendo o custo controlado — especialmente em tabelas volumosas.

## 1. Porte da Tabela e Definição de Intervalo (Obrigatório antes de varrer dados)

Antes de rodar qualquer `SELECT` sobre os dados:

1. **Reaproveite os metadados do Diagnóstico**:
   - Utilize `sizeInBytes`, `partitionColumns` e `clusteringColumns` já obtidos via `DESCRIBE DETAIL` no Diagnóstico (se ainda não tiverem sido executados, rode `DESCRIBE DETAIL <tabela>` agora, com custo ~zero).
2. **Proponha um recorte proporcional ao porte e confirme com o usuário**:
   - **Tabelas pequenas (`< 5 GB`)**: sugira perfilamento completo.
   - **Tabelas médias (`5–100 GB`) ou volumosas (`> 100 GB`)**: sempre pergunte qual intervalo/recorte utilizar, sugerindo opções baseadas na coluna de partição ou clustering identificada (ex.: última partição, últimos 7 ou 30 dias, ou período em que `*_st` já esteja preenchida). Não execute full scan sem autorização explícita.
   - *(Checagem rápida de faixa)*: para colunas numéricas em tabelas grandes, `MIN(col)` e `MAX(col)` podem aproveitar estatísticas do Delta Log para detectar valores `<= 0` ou `> 999` sem full scan — **somente se a coluna possuir estatísticas coletadas** (caso contrário, executará scan).
3. **Boas práticas de execução**:
   - Consolide todas as métricas de cada tabela em uma **única consulta (single-pass)** usando agregações condicionais (`COUNT_IF` / `SUM(CASE WHEN ...)`), lendo apenas as colunas necessárias.
   - **Amostras sob demanda**: só execute a consulta de exemplos (`LIMIT 10`, dentro do mesmo recorte) se a contagem de anomalias for `> 0`. Evite `COLLECT_SET` sem limite (risco de OOM).
   - Caso não haja acesso de execução SQL/Spark no ambiente, gere as consultas prontas (já com o recorte combinado) para o usuário executar e trazer os resultados.

## 2. Métricas a Coletar

### 2.1 Para toda coluna numérica legada (`col`)
- Total de registros no recorte e quantidade de `NULL`.
- Quantidade de valores `<= 0` (ex.: `0`, negativos — inválidos).
- Quantidade de valores `> 999` (ex.: `1234` — risco de truncamento).
- Quantidade de valores válidos na faixa `1..999`.
- Amostras sob demanda: valores anômalos (`<= 0` ou `> 999`).

### 2.2 Para colunas `*_st` (quando já existirem na tabela)
Classifique em categorias **mutuamente exclusivas**:
- `NULL` ou vazios/somente espaços.
- `'000'` (incluindo após `TRIM`; inválido mesmo atendendo à regex).
- **Recuperáveis com `UPPER(TRIM)`**: falham em `^[A-Z0-9]{3}$` no valor bruto, mas passam após `UPPER(TRIM(col_st))` e não são `'000'`.
- **Outros inválidos**: falham em `^[A-Z0-9]{3}$` mesmo após `UPPER(TRIM(col_st))` (excluindo `'000'`).
- Amostras sob demanda: exemplos de valores inválidos.

### 2.3 Convivência `col` vs `col_st` (quando ambas existirem)
- Distribuição de preenchimento: apenas `col`, apenas `col_st`, ambas preenchidas, ambas `NULL`.
- **Divergências**: registros em que ambas estão preenchidas e válidas, mas diferem entre si (comparando `LPAD(CAST(CAST(col AS BIGINT) AS STRING), 3, '0')` com `UPPER(TRIM(col_st))`).
- **Casos de fallback**: registros em que `col_st` está preenchida porém inválida, enquanto `col` possui valor válido (`1..999`).
- Amostras sob demanda: pares distintos `(col, col_st)` de divergências ou fallback.

## 3. Fechamento da Etapa 1b (Saída Padronizada)
Apresente o resumo seguindo a estrutura abaixo:

```markdown
### Resumo do Perfilamento de Dados

**Porte e recorte aplicado:**
| Tabela | Tamanho Aprox. | Partição / Clustering | Recorte Utilizado |
|---|---|---|---|
| `<tabela>` | `<ex.: 2.1 TB>` | `<ex.: dt_ref>` | `<ex.: dt_ref >= current_date() - 7>` |

#### 1. Qualidade da Coluna Numérica (`col`)
| Tabela | Coluna | Total | Nulos | `<= 0` (Inválido) | `> 999` (Fora da Faixa) | Válidos (`1..999`) |
|---|---|---|---|---|---|---|
| `<tabela>` | `cbco` | 0 | 0 | 0 | 0 | 0 |

#### 2. Qualidade de `*_st` e Convivência *(somente tabelas onde `*_st` já existe)*
| Tabela | Coluna `*_st` | Nulos/Vazios | `'000'` | Recuperáveis (`UPPER/TRIM`) | Outros Inválidos | Divergentes (`col` ≠ `col_st`) | Fallback (`_st` inv. + `col` vál.) |
|---|---|---|---|---|---|---|---|
| `<tabela>` | `cbco_st` | 0 | 0 | 0 | 0 | 0 | 0 |

#### 3. Amostras de Anomalias e Observações
- `<tabela.coluna>`: `<exemplos de valores inválidos/divergentes encontrados e impacto prático>`
```

Ao final do resumo:
1. Destaque de forma objetiva os achados que mais influenciam as decisões (ex.: se há valores `> 999`, se `0` aparece em massa indicando possível sentinela de "não informado", se há sujeira recuperável com `UPPER(TRIM)` ou divergência real entre `col` e `col_st`).
2. Pergunte se o usuário deseja perfilar outra tabela/recorte ou se podemos seguir adiante.
