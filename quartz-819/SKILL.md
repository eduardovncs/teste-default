---
name: migracao-compe-alfanumerico
description: >
  Migra pipelines Databricks (PySpark, Spark SQL e notebooks) para a transição do código
  dos bancos brasileiros (COMPE) de numérico (INT) para alfanumérico de 3 caracteres
  (STRING, ^[A-Z0-9]{3}$). Use sempre que o usuário mencionar cbco, código de banco, COMPE,
  colunas *_st, convivência col/col_st, schemas *_ing, mudança de INT para STRING em joins,
  filtros, unions ou escritas Delta, ou pedir diagnóstico, perfilamento, plano, aplicação ou
  validação dessa migração — mesmo sem citar "COMPE" explicitamente. Não se aplica a ISPB,
  agência ou número de conta.
compatibility: Databricks (Spark >= 3.1, Python >= 3.10); acesso opcional ao catálogo/metastore.
---

# Migração de Código de Banco (COMPE) Alfanumérico

## Contexto e Regras de Domínio

O código dos bancos brasileiros (COMPE) está migrando de numérico (`INT`) para alfanumérico de 3 caracteres seguindo o padrão `^[A-Z0-9]{3}$`.

- **Tabelas em schemas com sufixo `*_ing`**: passam a ter convivência temporária entre a coluna legada numérica (ex.: `cbco` como `INT`) e uma nova coluna com sufixo `_st` (ex.: `cbco_st` como `STRING`), até a eventual remoção da coluna antiga.
- **Tabelas fora de schemas `*_ing`**: não há garantia de criação de `*_st`; é muito provável que o proprietário altere o tipo da coluna *in-place* de `INT` para `STRING` no futuro.
- **Valores inválidos conhecidos**: códigos `<= 0` (como `0` ou `'000'`), valores numéricos `> 999` (ex.: `1234`, sujeitos a truncamento silencioso por `LPAD`), valores decimais com fração (ex.: `1.5`), valores fora de `^[A-Z0-9]{3}$`, ou divergências entre a coluna numérica e a `*_st`.
- **Escopo negativo**: não migrar outros identificadores bancários como `ispb` (8 dígitos), agência ou número de conta.
- **Atenção a performance**: colunas de código de banco aparecem com frequência em tabelas transacionais volumosas. `JOIN`s em `STRING` consomem mais CPU/memória que `INT`; além disso, aplicar funções (`CAST`, `LPAD`, `COALESCE`, `REGEXP`) sobre colunas usadas em filtros ou gerar *casts* implícitos por tipos divergentes impede *file pruning* / *data skipping* (inclusive em `PARTITIONED BY` e `CLUSTER BY`).

## Convenções e Pré-requisitos

- **Idioma**: responda sempre em português do Brasil.
- **Entradas**: antes de iniciar, identifique onde está o código/notebooks a migrar (repositório local, workspace, formato `.py`/`.ipynb`/`.sql`) e se há acesso ao catálogo/execução SQL.
- **Edição segura**: prefira trabalhar em uma *branch* dedicada ao aplicar alterações. Se o usuário alterar uma decisão de uma etapa anterior, reavalie as etapas dependentes antes de aplicar.

## Fluxo de Execução

Siga as etapas na ordem abaixo — cada etapa depende das confirmações da anterior, e corrigir premissas erradas depois da aplicação gera retrabalho. Para economizar contexto e manter foco, leia o arquivo de cada etapa somente quando for executá-la.

Ao final de cada etapa, apresente o resumo padronizado e aguarde confirmação do usuário antes de avançar.

- **Etapa 1 — Diagnóstico** → Leia e execute `steps/01-diagnostico.md`
- **Etapa 1b — Perfilamento de Dados *(Opcional)*** → Caso o usuário queira inspecionar os dados atuais das tabelas mapeadas após o Diagnóstico, leia e execute `steps/01b-perfilamento.md`
- **Etapa 2 — Perguntas** → Leia e execute `steps/02-perguntas.md`
- **Etapa 3 — Plano de Alteração** → Leia e execute `steps/03-plano.md`
- **Etapa 4 — Aplicação** → Leia e execute `steps/04-aplicacao.md`
- **Etapa 5 — Validação** → Leia e execute `steps/05-validacao.md`

## Referências Técnicas
- `references/normalizacao.md` — Função padrão PySpark (`normalizar_compe`) e exemplos equivalentes em Spark SQL. Leia ao elaborar o plano e ao aplicar as alterações.
