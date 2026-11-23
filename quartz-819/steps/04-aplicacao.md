# Etapa 4: Aplicação

Aplique as alterações somente após a aprovação explícita do Plano de Alteração pelo usuário.

**Regras gerais:**
- Consulte `references/normalizacao.md` ao implementar para garantir fidelidade à função padrão `normalizar_compe` (PySpark) ou aos exemplos de referência (Spark SQL).
- Siga estritamente o plano aprovado, mantendo o diff enxuto e preservando o formato do artefato (`.py` Databricks source com marcadores `# COMMAND ----------`, `.ipynb`, `.sql`, módulos Python) e toda lógica não relacionada à migração.
- Se encontrar algo inesperado durante a edição (ex.: outra coluna de banco não mapeada, escrita Delta não prevista, `StructType` rígido ou conflito de schema), pare e informe o usuário antes de sair do plano.
- Não execute ações destrutivas em tabelas reais (`DROP TABLE`, `TRUNCATE`, recriação ou backfill direto em produção) sem autorização explícita; concentre-se na alteração do código/notebook/SQL do pipeline.

## Diretrizes de Implementação

- **PySpark / Notebooks**:
  - Disponibilize a função padrão `normalizar_compe` exatamente conforme `references/normalizacao.md` no local definido no plano (módulo compartilhado, `%run` ou célula dedicada), sem duplicar código.
  - Aplique `normalizar_compe(...)` com os parâmetros aprovados (`prioridade_conflito`, `sanitizar`, `acao_st_invalida`, `acao_legada_invalida`, `descartar_st`, `col_saida` quando aplicável; `col_st=""` em origens não-`_ing` defensivas).
  - Normalize cada origem **uma única vez** (logo após a leitura ou logo após filtros nativos de *file pruning*), evitando reaplicar a função sobre o mesmo DataFrame.
- **Spark SQL**:
  - Aplique as expressões definidas no plano seguindo os exemplos de `references/normalizacao.md`.
- **Demais ajustes do plano**:
  - Execute exatamente as conversões de literais, filtros nativos pré-normalização, ajustes de `CAST`/`StructType`/chaves compostas/ordenação, padronização de tipos em `JOIN`/`UNION` (`STRING` de 3 caracteres), regra da coluna legada `INT` (se adicionar `*_st` na saída) e opções Delta (`overwriteSchema`, `mergeSchema`, DDL/`MERGE`) definidas no plano aprovado.

## Fechamento da Etapa 4 (Saída Padronizada e Gate)
Após concluir todas as edições, apresente o resumo consolidado seguindo a estrutura abaixo:

```markdown
### Resumo das Alterações Aplicadas

#### 1. Arquivos / Notebooks Modificados
| Arquivo / Notebook (Célula) | Alterações Realizadas |
| :--- | :--- |
| `<caminho/arquivo>` | `<resumo objetivo do que foi alterado>` |

#### 2. Função `normalizar_compe` *(se PySpark)*
- **Local**: `<caminho do módulo ou célula>`
- **Chamadas aplicadas**: `<resumo dos parâmetros utilizados>`

#### 3. Pendências Operacionais Fora do Código *(se aplicável)*
- `<backfill / recriação de tabela / DDL manual / dependências não-_ing apenas documentadas>`

#### 4. Pontos de Atenção Encontrados na Edição *(incluir somente se houver)*
- `<qualquer detalhe relevante observado durante a aplicação>`
```

Peça confirmação do usuário sobre as alterações aplicadas antes de prosseguir.
