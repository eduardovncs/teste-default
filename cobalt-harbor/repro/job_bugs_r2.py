"""Reprodutor de bugs (Rodada 2) no spark-lineage ao capturar um job PySpark (.py).

Cada bloco representa um padrão válido em PySpark/Databricks que falha no
`spark-lineage capture` / `sources`:

1. `df.write.jdbc(url, table)`:
   - Esperado: destino `"public.tb_vendas"` em `destinations`.
   - Atual: `Capturer._target` pega `a[0]` e registra a URL JDBC
     `"jdbc:postgresql://host:5432/db"` como tabela de destino.

2. `df.write.saveAsTable("`dev`.`tgt`.`tb_crase`")`:
   - Esperado: consulta `--tag destinations --table dev.tgt.tb_crase` retorna o destino.
   - Atual: crases internas são mantidas no destino e `sources._match_table`
     usa `.strip("`")` em vez de `.replace("`", "")`, retornando `[]`.

3. `if cfg.path.endswith(".parquet"):`:
   - Esperado: `BranchExplorer` explora True e False (`dev.tgt.tb_parquet` e `dev.tgt.tb_csv`).
   - Atual: `"path"` está em `_SKIP_ATTRS`, então qualquer atributo `.path` em objetos
     de negócio faz o `if` ser ignorado (`dev.tgt.tb_csv` perdido).

4. `while True:` com `if df.count() > 0: ... break`:
   - Esperado: não entrar em loop infinito ao explorar o ramo `False`.
   - Atual: `BranchExplorer.decide` força `False` em todas as iterações do laço
     (o guarda `iters > 50` abaixo evita travar o terminal, disparando RuntimeError).
"""

from types import SimpleNamespace

df = spark.range(5)

# Bug R2-3: write.jdbc captura a URL JDBC em vez do nome da tabela
df.write.jdbc("jdbc:postgresql://host:5432/db", "public.tb_vendas")

# Bug R2-5: nome de tabela com crases não casa em sources._match_table
df.write.saveAsTable("`dev`.`tgt`.`tb_crase`")

# Bug R2-8b: atributo `.path` em objeto de negócio silencia BranchExplorer
cfg = SimpleNamespace(path="/mnt/dados/entrada.parquet")
if cfg.path.endswith(".parquet"):
    df.write.saveAsTable("dev.tgt.tb_parquet")
else:
    df.write.saveAsTable("dev.tgt.tb_csv")

# Bug R2-8a: laço while com break condicional entra em loop infinito na passada forçada
iters = 0
while True:
    iters += 1
    if iters > 50:
        raise RuntimeError("loop infinito provocado pelo BranchExplorer")
    if df.count() > 0:
        df.write.saveAsTable("dev.tgt.tb_loop_ok")
        break
