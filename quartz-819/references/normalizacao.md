# Referência: Função de Normalização COMPE Alfanumérico

Use sempre a função padrão `normalizar_compe` abaixo em pipelines **PySpark / Notebooks** (requer Spark ≥ 3.1 para `F.raise_error` e Python ≥ 3.10 para anotações `str | None`; em Python < 3.10, use `Optional[str]`).

Tanto a inspeção de schema (`col_legada`, `col_st` e seus tipos) quanto os `if`s de configuração executam **no driver** ao montar o plano Catalyst — opções desativadas não adicionam custo em tempo de execução.

---

## 1. Função Padrão PySpark (`normalizar_compe`)

Cobre automaticamente os 4 estados do ciclo de vida da tabela sem quebrar o pipeline:
1. **Apenas `col_legada` (`INT`/`DECIMAL`)** — antes de `*_st` existir na ingestão, ou tabela não-`_ing` ainda numérica.
2. **`col_legada` (`INT`/`DECIMAL`) + `col_st` (`STRING`)** — período de convivência em schemas `*_ing`.
3. **Apenas `col_st` (`STRING`)** — após a remoção da coluna antiga em schemas `*_ing`.
4. **Apenas `col_legada` (`STRING`)** — tabela não-`_ing` após migração *in-place* do tipo pelo proprietário (aceita tanto `'1'` → `'001'` quanto `'A01'` → `'A01'`, de forma 100% segura em modo ANSI).

> **Detalhe importante na convivência:** quando `col_st` traz um código válido (ex.: `'A01'`), valores inválidos na coluna legada (como `cbco = 0`, comum quando o legado numérico não consegue representar códigos alfanuméricos) **não** disparam erro, filtro nem falsa divergência — mesmo no modo `sem_validacao`. As ações de `acao_legada_invalida` só se aplicam quando a coluna legada efetivamente precisa ser utilizada.

```python
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType

_REGEX_COMPE = r"^[A-Z0-9]{3}$"


def normalizar_compe(
    df: DataFrame,
    col_legada: str,
    col_st: str | None = None,           # None -> auto-detecta "{col_legada}_st" | "" -> ignora *_st
    col_saida: str | None = None,        # None -> sobrescreve col_legada | "<nome>" -> cria/atualiza outra coluna
    descartar_st: bool = False,          # True -> remove col_st residual (se diferente de col_saida)
    prioridade_conflito: str = "st",     # "st" | "legada" | "null" | "erro"
    sanitizar: bool = True,              # True -> UPPER(TRIM) em colunas STRING | False -> valor bruto
    acao_st_invalida: str = "fallback",  # "fallback" | "null" | "filtrar" | "erro"
    acao_legada_invalida: str = "null",  # "null" | "filtrar" | "erro" | "sem_validacao"
) -> DataFrame:
    """Normaliza colunas de código de banco (COMPE) para STRING no padrão ^[A-Z0-9]{3}$."""
    if prioridade_conflito not in {"st", "legada", "null", "erro"}:
        raise ValueError(f"prioridade_conflito inválida: {prioridade_conflito}")
    if acao_st_invalida not in {"fallback", "null", "filtrar", "erro"}:
        raise ValueError(f"acao_st_invalida inválida: {acao_st_invalida}")
    if acao_legada_invalida not in {"null", "filtrar", "erro", "sem_validacao"}:
        raise ValueError(f"acao_legada_invalida inválida: {acao_legada_invalida}")

    col_st_esperada = f"{col_legada}_st" if col_st is None else col_st
    col_destino = col_saida or col_legada

    cols_lower = {c.lower(): c for c in df.columns}
    leg_real = cols_lower.get(col_legada.lower())
    st_real = cols_lower.get(col_st_esperada.lower()) if col_st_esperada else None

    if not leg_real and not st_real:
        raise ValueError(
            f"Nenhuma das colunas '{col_legada}' ou '{col_st_esperada}' foi encontrada no DataFrame."
        )

    def _finalizar(df_res: DataFrame) -> DataFrame:
        if descartar_st and st_real and st_real.lower() != col_destino.lower():
            return df_res.drop(st_real)
        return df_res

    def _preparar_string(nome_col: str) -> tuple[Column, Column, Column, Column]:
        bruta = F.col(nome_col)
        expr = F.upper(F.trim(bruta)) if sanitizar else bruta
        preenchida = expr.isNotNull() & (expr != "")
        valida = preenchida & expr.rlike(_REGEX_COMPE) & (expr != "000")
        invalida = preenchida & (~valida)
        return expr, preenchida, valida, invalida

    def _preparar_legada_string(nome_col: str) -> tuple[Column, Column, Column, Column]:
        bruta = F.col(nome_col)
        expr = F.upper(F.trim(bruta)) if sanitizar else bruta
        preenchida = expr.isNotNull() & (expr != "")
        lpad_num = F.lpad(expr, 3, "0")
        eh_num = expr.rlike(r"^[0-9]{1,3}$")
        if acao_legada_invalida == "sem_validacao":
            norm = F.when(eh_num, lpad_num).otherwise(expr)
            return norm, preenchida, preenchida, F.lit(False)
        eh_num_valido = eh_num & (lpad_num != "000")
        eh_alfa_valido = expr.rlike(_REGEX_COMPE) & (expr != "000")
        valida = preenchida & (eh_num_valido | eh_alfa_valido)
        invalida = preenchida & (~valida)
        norm = F.when(eh_num_valido, lpad_num).when(eh_alfa_valido, expr)
        return norm, preenchida, valida, invalida

    def _preparar_numerica(nome_col: str) -> tuple[Column, Column, Column]:
        bruta = F.col(nome_col)
        eh_inteiro = bruta == bruta.cast("long")
        as_int_str = bruta.cast("long").cast("string")
        if acao_legada_invalida == "sem_validacao":
            valida = bruta.isNotNull()
            invalida = F.lit(False)
            # Evita truncar valores > 999, negativos ou decimais com fração
            norm = F.when(
                eh_inteiro & (bruta >= 0) & (bruta <= 999),
                F.lpad(as_int_str, 3, "0"),
            ).otherwise(F.when(eh_inteiro, as_int_str).otherwise(bruta.cast("string")))
        else:
            valida = bruta.isNotNull() & eh_inteiro & (bruta >= 1) & (bruta <= 999)
            invalida = bruta.isNotNull() & (~valida)
            norm = F.when(valida, F.lpad(as_int_str, 3, "0"))
        return norm, valida, invalida

    def _resolver_legada_sozinha(nome_col: str) -> DataFrame:
        tipo = df.schema[nome_col].dataType
        if isinstance(tipo, StringType):
            norm, _, _, invalida = _preparar_legada_string(nome_col)
            df_out = df.filter(~invalida) if acao_legada_invalida == "filtrar" else df
            if acao_legada_invalida == "erro":
                col_final = F.when(
                    invalida,
                    F.raise_error(F.concat(F.lit(f"COMPE inválido em {nome_col}: "), F.col(nome_col))),
                ).otherwise(norm)
            else:
                col_final = norm
            return _finalizar(df_out.withColumn(col_destino, col_final))

        if not isinstance(tipo, NumericType):
            raise TypeError(f"Tipo não suportado em '{nome_col}': {tipo.simpleString()}")

        norm, _, invalida = _preparar_numerica(nome_col)
        df_out = df.filter(~invalida) if acao_legada_invalida == "filtrar" else df
        if acao_legada_invalida == "erro":
            col_final = F.when(
                invalida,
                F.raise_error(
                    F.concat(F.lit(f"COMPE numérico inválido em {nome_col}: "), F.col(nome_col).cast("string"))
                ),
            ).otherwise(norm)
        else:
            col_final = norm
        return _finalizar(df_out.withColumn(col_destino, col_final))

    # Caso 1: apenas col_st existe (coluna legada já foi dropada)
    if st_real and not leg_real:
        st_expr, _, st_valida, st_invalida = _preparar_string(st_real)
        df_out = df.filter(~st_invalida) if acao_st_invalida == "filtrar" else df
        if acao_st_invalida == "erro":
            col_final = F.when(
                st_invalida,
                F.raise_error(F.concat(F.lit(f"COMPE inválido em {st_real}: "), F.col(st_real))),
            ).when(st_valida, st_expr)
        else:
            col_final = F.when(st_valida, st_expr)
        return _finalizar(df_out.withColumn(col_destino, col_final))

    # Caso 2: apenas col_legada existe (antes da criação de *_st ou tabela não-_ing)
    if leg_real and not st_real:
        return _resolver_legada_sozinha(leg_real)

    # Caso 3: convivência — ambas col_legada e col_st existem
    st_expr, st_preenchida, st_valida, st_invalida = _preparar_string(st_real)
    leg_tipo = df.schema[leg_real].dataType
    if isinstance(leg_tipo, StringType):
        leg_norm, _, leg_valida, leg_invalida = _preparar_legada_string(leg_real)
    elif isinstance(leg_tipo, NumericType):
        leg_norm, leg_valida, leg_invalida = _preparar_numerica(leg_real)
    else:
        raise TypeError(f"Tipo não suportado em '{leg_real}': {leg_tipo.simpleString()}")

    # Define quando a coluna legada efetivamente será utilizada
    precisa_legada = (~st_preenchida) | (st_invalida if acao_st_invalida == "fallback" else F.lit(False))

    df_out = df
    if acao_st_invalida == "filtrar":
        df_out = df_out.filter(~st_invalida)
    if acao_legada_invalida == "filtrar":
        df_out = df_out.filter(~(precisa_legada & leg_invalida))

    # Expressão para o ramo da legada (quando acionado)
    if acao_legada_invalida == "erro":
        ramo_legada = F.when(
            leg_invalida,
            F.raise_error(
                F.concat(F.lit(f"COMPE legado inválido em {leg_real}: "), F.col(leg_real).cast("string"))
            ),
        ).otherwise(leg_norm)
    else:
        ramo_legada = leg_norm

    # Expressão para col_st preenchida porém inválida
    if acao_st_invalida == "fallback":
        ramo_st_invalida = ramo_legada
    elif acao_st_invalida == "erro":
        ramo_st_invalida = F.raise_error(
            F.concat(F.lit(f"COMPE inválido em {st_real}: "), F.col(st_real))
        )
    else:  # "null" ou "filtrar" (já filtrado acima)
        ramo_st_invalida = F.lit(None).cast("string")

    if prioridade_conflito == "st":
        col_final = (
            F.when(st_invalida, ramo_st_invalida)
            .when(st_valida, st_expr)
            .otherwise(ramo_legada)
        )
    else:
        # Só acusa divergência quando leg_norm é de fato um COMPE válido (evita conflito com '000' em sem_validacao)
        leg_valida_conflito = (
            leg_valida & leg_norm.rlike(_REGEX_COMPE) & (leg_norm != "000")
            if acao_legada_invalida == "sem_validacao"
            else leg_valida
        )
        conflito = st_valida & leg_valida_conflito & (st_expr != leg_norm)
        if prioridade_conflito == "legada":
            ramo_conflito = leg_norm
        elif prioridade_conflito == "null":
            ramo_conflito = F.lit(None).cast("string")
        else:  # "erro"
            ramo_conflito = F.raise_error(
                F.concat(
                    F.lit(f"Divergência COMPE ({leg_real}="),
                    leg_norm,
                    F.lit(f", {st_real}="),
                    st_expr,
                    F.lit(")"),
                )
            )
        col_final = (
            F.when(st_invalida, ramo_st_invalida)
            .when(conflito, ramo_conflito)
            .when(st_valida, st_expr)
            .otherwise(ramo_legada)
        )

    return _finalizar(df_out.withColumn(col_destino, col_final))
```

### Mapeamento rápido: Decisões → Parâmetros

| Decisão | Parâmetro | Valores |
|---|---|---|
| Prioridade em divergência | `prioridade_conflito` | `"st"`, `"legada"`, `"null"`, `"erro"` |
| Sanitização de colunas `STRING` | `sanitizar` | `True` (`UPPER/TRIM`) ou `False` (bruto) |
| Quando `col_st` é inválida (incl. `'000'`) | `acao_st_invalida` | `"fallback"`, `"null"`, `"filtrar"`, `"erro"` |
| Quando `col_legada` é inválida (`<= 0`, `> 999`, fração ou fora do padrão) | `acao_legada_invalida` | `"null"`, `"filtrar"`, `"erro"`, `"sem_validacao"` |
| Coluna de saída (ex.: manter legada e criar `*_st` no destino) | `col_saida` | `None` (sobrescreve `col_legada`) ou `"<nome>"` |
| Remover `col_st` residual após normalizar | `descartar_st` | `False` (mantém) ou `True` (remove) |

---

## 2. Exemplos de Implementação em Spark SQL

> **Limitação do Spark SQL puro:** não é possível verificar dinamicamente se `*_st` existe dentro de um `SELECT`. Só referencie `col_st` em SQL quando a coluna já existir no schema da tabela (confirmado no Diagnóstico). Caso precise suportar a tabela antes da criação de `*_st`, faça a leitura/normalização em PySpark usando `normalizar_compe` e exponha via temp view.

### Exemplo A — Convivência com sanitização, fallback e faixa `1..999` (priorizando `cbco_st`)
*(Requer que `cbco` ainda seja numérico; se houver risco de `cbco` virar `STRING` em modo ANSI, aplique o padrão com `CAST(... AS STRING)` do Exemplo C no ramo da legada.)*

```sql
SELECT
  CASE
    WHEN UPPER(TRIM(cbco_st)) RLIKE '^[A-Z0-9]{3}$'
     AND UPPER(TRIM(cbco_st)) <> '000'
      THEN UPPER(TRIM(cbco_st))
    WHEN cbco BETWEEN 1 AND 999
      THEN LPAD(CAST(CAST(cbco AS BIGINT) AS STRING), 3, '0')
    ELSE NULL
  END AS cbco
FROM catalog.schema_ing.tabela
```

### Exemplo B — Convivência com erro em divergência (`raise_error` nativo)
```sql
WITH base AS (
  SELECT
    CASE
      WHEN UPPER(TRIM(cbco_st)) RLIKE '^[A-Z0-9]{3}$'
       AND UPPER(TRIM(cbco_st)) <> '000'
        THEN UPPER(TRIM(cbco_st))
    END AS st_val,
    CASE
      WHEN cbco BETWEEN 1 AND 999
        THEN LPAD(CAST(CAST(cbco AS BIGINT) AS STRING), 3, '0')
    END AS leg_val
  FROM catalog.schema_ing.tabela
)
SELECT
  CASE
    WHEN st_val IS NOT NULL AND leg_val IS NOT NULL AND st_val <> leg_val
      THEN raise_error(concat('Divergência COMPE: cbco=', leg_val, ', cbco_st=', st_val))
    ELSE COALESCE(st_val, leg_val)
  END AS cbco
FROM base
```

### Exemplo C — Normalização defensiva para tabela fora de `*_ing` (compatível com `INT` atual e futuro `STRING`, seguro em modo ANSI)
```sql
SELECT
  CASE
    WHEN TRIM(CAST(cbco AS STRING)) RLIKE '^[0-9]{1,3}$'
     AND LPAD(TRIM(CAST(cbco AS STRING)), 3, '0') <> '000'
      THEN LPAD(TRIM(CAST(cbco AS STRING)), 3, '0')
    WHEN UPPER(TRIM(CAST(cbco AS STRING))) RLIKE '^[A-Z0-9]{3}$'
     AND UPPER(TRIM(CAST(cbco AS STRING))) <> '000'
      THEN UPPER(TRIM(CAST(cbco AS STRING)))
    ELSE NULL
  END AS cbco
FROM catalog.schema_curated.tabela
```

### Exemplo D — Filtro nativo pré-normalização (preservando *file pruning* / *data skipping*)
```sql
SELECT ...
FROM catalog.schema_ing.tabela
WHERE cbco IN (1, 33) OR cbco_st IN ('001', '033')
```
