"""Reprodutor de bugs (Rodada 4) no spark-lineage em jobs PySpark (.py).

1. Ternário encadeado na mesma linha (`a if c1 else (b if c2 else c)`):
   - Esperado: explorar os 3 destinos (`tb_a`, `tb_b`, `tb_c`).
   - Atual: `visit_IfExp` usa `site_id = f"{file}:{lineno}"` sem `col_offset`;
     os dois ternários colidem no mesmo `site_id` e `tb_b` nunca é capturado.

2. `df.head(None)`:
   - Esperado: retornar uma única `Row` (como `df.head()`), não uma `list`.
   - Atual: `DF_ACTIONS["head"]` devolve `[Row(...)]` quando `n=None` é passado explicitamente.

3. `dbutils.widgets.remove("modo")`:
   - Esperado: remover o valor default do widget `"modo"`.
   - Atual: `remove` cai no `__getattr__` no-op e `widgets.get("modo")` mantém o valor antigo.

4. `pdf = df.toPandas()` mutado in-place entre passadas do `BranchExplorer`:
   - Esperado: `Capturer.pandas()` devolver `.copy()` do cache para não vazar colunas extras entre passadas.
   - Atual: devolve a mesma instância mutável de `pandas.DataFrame`.
"""

df = spark.range(5)

# Bug R4-4: ternário encadeado na mesma linha colide em site_id -> perde dev.tgt.tb_b
modo = dbutils.widgets.get("modo") or "C"
destino = "dev.tgt.tb_a" if modo == "A" else ("dev.tgt.tb_b" if modo == "B" else "dev.tgt.tb_c")
df.write.saveAsTable(destino)

# Bug R4-16: dbutils.widgets.remove não remove o default registrado
dbutils.widgets.text("temp_flag", "ATIVO")
dbutils.widgets.remove("temp_flag")
if dbutils.widgets.get("temp_flag") == "":
    df.write.saveAsTable("dev.tgt.tb_widget_removido")

# Bug R4-17: df.head(None) devolve list em vez de Row única
primeira = df.head(None)
if not isinstance(primeira, list):
    df.write.saveAsTable("dev.tgt.tb_head_none_ok")
