# Etapa 5: Validação

Verifique de forma sistemática se as alterações aplicadas atendem a todas as decisões confirmadas, cobrem os casos de borda do COMPE alfanumérico e não introduziram regressões de schema, dados ou performance.

**Regra de segurança:**
- Nunca execute gravações em tabelas de produção para testar. Utilize DataFrames sintéticos, ambiente de desenvolvimento/homologação ou planos de execução (`EXPLAIN`).

## 1. Revisão Estática do Código Alterado
- Confirme que todas as ocorrências mapeadas no Diagnóstico e aprovadas no Plano foram ajustadas.
- Verifique se não restaram literais ou comparações numéricas esquecidas (`cbco = 1`, `IN (1, 33)`, `> 0`, `!= 0`), `CAST`s para inteiro, `StructType`s antigos, chaves compostas/hash ou ordenações afetadas.
- Confirme que todos os `JOIN`s, `UNION`s e agregações comparam colunas já normalizadas como `STRING` de 3 caracteres, sem casts implícitos.
- Confirme que as opções de escrita Delta (`overwriteSchema`, `mergeSchema`, DDL/`MERGE`) e o descarte/manutenção de `col_st` correspondem exatamente à estratégia aprovada.

## 2. Validação dos Cenários de Borda (Dados Sintéticos)
Execute os testes com DataFrames sintéticos (ou forneça o script de teste pronto para execução pelo usuário, caso não haja ambiente Spark disponível) cobrindo os cenários abaixo conforme os parâmetros escolhidos:

- **Conversão legada válida**: `1` → `'001'`, `33` → `'033'`, `999` → `'999'`.
- **`NULL` + `NULL`**: deve resultar em `NULL` sem disparar erro (mesmo com `acao_legada_invalida="erro"`).
- **Alfanumérico em `col_st` com legada zerada**: `(col=0, col_st='A01')` → deve resultar em `'A01'` sem disparar `acao_legada_invalida` nem falsa divergência (inclusive com `sem_validacao` + `prioridade_conflito="legada"` ou `"erro"`).
- **Legada inválida (`<= 0`, `> 999` ou decimal com fração)**: `0`, `-1`, `1234`, `1.5` (sem `col_st` válida) → validar comportamento conforme `acao_legada_invalida` (`null`, `filtrar`, `erro`, `sem_validacao` — em `sem_validacao`, garantir que `1234` vira `'1234'` sem truncar para `'123'`).
- **`col_st` inválida**: `'000'`, `'12'`, `'1234'`, `'@#!'`, `''` → validar comportamento conforme `acao_st_invalida` (`fallback`, `null`, `filtrar`, `erro`).
- **Sanitização (`sanitizar`)**: `' a01 '` → `'A01'` quando `sanitizar=True`, ou tratada como inválida quando `sanitizar=False`.
- **Divergência entre válidas**: `(col=1, col_st='033')` → validar comportamento conforme `prioridade_conflito` (`st`, `legada`, `null`, `erro`).
- **Colunas resultantes, case-insensitivity e idempotência**: validar `col_saida` (quando usada), `descartar_st=True/False`, nome de coluna em caixa alta (`CBCO`), contagem de linhas quando `filtrar` for usado e idempotência (aplicar `normalizar_compe` 2× produz o mesmo resultado).
- **Ciclo de vida do schema *(PySpark com `normalizar_compe`)***:
  1. Apenas `col_legada` (`INT`) presente.
  2. Convivência `col_legada` (`INT`) + `col_st` (`STRING`).
  3. Apenas `col_st` (`STRING`) presente (coluna legada já removida).
  4. Apenas `col_legada` já migrada *in-place* para `STRING` (com `'1'` → `'001'` e `'A01'` → `'A01'`).

## 3. Verificação de Performance e Plano de Execução *(incluir somente se houver risco de performance mapeado)*
- Execute ou forneça o comando `EXPLAIN` (ou `df.explain(True)`) para confirmar:
  - Se filtros nativos preservados antes da normalização mantêm *PartitionFilters* / *PushedFilters* (*file pruning* / *data skipping*).
  - Se não há casts implícitos inesperados nos `JOIN`s em tabelas volumosas.

## 4. Prontidão Operacional
- Consolide apenas as pendências operacionais que ainda restam antes ou durante o deploy (ex.: execução em dev/homologação, backfill/recriação em `APPEND`/`MERGE`, DDL manual, alinhamento com consumidores downstream ou acompanhamento de origens não-`_ing` não alteradas).

## Fechamento da Etapa 5 (Saída Padronizada)
Apresente o relatório final de validação seguindo a estrutura abaixo:

```markdown
### Relatório Final de Validação

#### 1. Revisão Estática
- **Cobertura das alterações**: `<OK / Pendências>`
- **Literais, CASTs, schemas fixos e tipos em JOINs/UNIONs**: `<OK / Pontos encontrados>`
- **Escrita Delta / Schema / `col_st` residual**: `<OK / Não aplicável>`

#### 2. Testes de Cenários de Borda
- **Status**: `<Executados e aprovados / Script fornecido para execução pelo usuário>`
- **Cenários cobertos**: `<resumo dos resultados ou lista de casos validados>`

#### 3. Performance / Plano de Execução *(se aplicável)*
- **Status de `EXPLAIN` / *file pruning***: `<resultado ou comando recomendado>`

#### 4. Checklist Pré-Produção *(se aplicável)*
- `<ações operacionais pendentes fora do código>`
```

Encerre perguntando ao usuário se valida o encerramento da migração ou se deseja algum ajuste adicional.
