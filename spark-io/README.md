# spark-io

Ferramenta estática e read-only para mapear bases de entrada e saída em pipelines Spark/Databricks (`.py`, `.ipynb`, `.sql`), construída como apoio à `skill-compe`.

## Princípios

- **Zero execução do código do usuário:** não executa Python, Spark, MongoDB, APIs ou DML.
- **AST + `sqlglot` + Unity Catalog:** avaliação parcial de strings/constantes, propagação de wrappers internos, dependências de notebooks (`%run`, `dbutils.notebook.run`) e resolução estrutural de prefixos de ambiente (`pr_*`).

## Desenvolvimento e Qualidade

```bash
pip install -e ".[dev]"
poe check
```
