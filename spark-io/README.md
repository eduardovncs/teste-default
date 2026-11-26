# spark-io

Pacote Python e CLI para mapeamento estático e **100% read-only** de bases de entrada e saída em pipelines Spark/Databricks (`.py`, `.ipynb`, `.sql`), construído como apoio à `skill-compe`.

## Principais características

- **Zero execução do código do usuário:** analisa a AST Python e SQL (`sqlglot`) sem rodar Spark, MongoDB, APIs externas ou DML.
- **Independente do nome da variável do Spark:** reconhece leituras e escritas em `spark`, `conector`, `self.spark`, `sessao`, etc. (`read.*`, `readStream.*`, `table`, `write.*`, `writeStream.*`, `writeTo`, `mergeInto`, `DeltaTable`, `.sql(...)`).
- **Avaliação parcial agnóstica ao nome de ambiente:** resolve constantes locais, f-strings, concatenações `+`, `.format()`, dicts/listas literais, loops `for` e ambos os ramos de `if/else`, aplicando `--env-prefix pr` a padrões como `{qualquer_var}_xpto.xpto_ing.tabela`.
- **Wrappers e dependências de notebooks:** propaga funções wrapper multi-hop dentro do repositório, segue `# MAGIC %run` (com proteção contra ciclos), `dbutils.notebook.run` (injetando argumentos em `dbutils.widgets.get`) e sinaliza referências externas (`/Shared/...`, `ler_base(...)`).
- **Leve e Python puro:** requer apenas `Python >= 3.10`, `sqlglot` e `databricks-sdk` (sem `databricks-connect` nem `pyspark`).

## Instalação

### Local (modo editável)

```bash
pip install -e .
```

Com ferramentas de desenvolvimento e qualidade (`pytest`, `ruff`, `ty`, `pydoclint`, `xenon`, `poethepoet`):

```bash
pip install -e ".[dev]"
```

### Direto do GitHub

```bash
pip install "git+https://github.com/eduardovncs/teste-default.git#subdirectory=spark-io"
```

## Como utilizar

### 1. Via CLI (`spark-io` ou `python -m spark_io`)

Analisar um projeto inteiro a partir de um **entrypoint** (recomendado — segue apenas notebooks/módulos alcançáveis):

```bash
spark-io --path ./meu-projeto --entrypoint entrypoint.py --env-prefix pr
```

Analisar todos os arquivos `.py`, `.ipynb` e `.sql` de um diretório:

```bash
spark-io --path ./meu-projeto --env-prefix pr
```

Informar variáveis conhecidas para resolver partes dinâmicas extras (ex.: catálogo inteiro em variável):

```bash
spark-io --path ./meu-projeto --entrypoint job.py --env-prefix pr --var cat=pr_core --var schema=xpto_ing
```

Também pode ser chamado como módulo Python:

```bash
python -m spark_io --path ./meu-projeto --entrypoint job.py
```

#### Opções da CLI

| Opção | Padrão | Descrição |
| --- | --- | --- |
| `--path` *(obrigatório)* | — | Diretório raiz do projeto ou caminho de um arquivo único. |
| `--entrypoint` | `None` | Arquivo inicial relativo a `--path` para guiar a análise estática. |
| `--env-prefix` | `pr` | Prefixo de ambiente aplicado a catálogos no padrão `<*>_<resto>`. |
| `--var CHAVE=VALOR` | — | Variável conhecida para avaliação parcial (pode ser repetida). |

### 2. Como biblioteca Python

```python
from spark_io import CustomWrapperSpec, map_spark_io

resultado = map_spark_io(
    "./meu-projeto",
    entrypoint="entrypoint.py",
    env_prefix="pr",
    variables={"cat": "pr_core"},
    custom_wrappers={
        "ler_base": CustomWrapperSpec(kind="read", target_param_index=0),
        "salvar_base": CustomWrapperSpec(kind="write", target_param_index=1, target_param="destino"),
    },
)

print("Entradas:", [e.target for e in resultado.inputs])
print("Saídas:", [e.target for e in resultado.outputs])
print("JSON completo:", resultado.to_dict())
```

### Estrutura do JSON de saída

```json
{
  "project_root": "/caminho/do/projeto",
  "entrypoint": "entrypoint.py",
  "inputs": [
    {
      "target": "pr_xpto.xpto_ing.tb_clientes",
      "kind": "read",
      "via": "wrapper:ler_tabela",
      "file": "entrypoint.py",
      "line": 14,
      "target_type": "table",
      "dynamic": false
    }
  ],
  "outputs": [
    {
      "target": "pr_xpto.gold.tb_saida",
      "kind": "write",
      "via": "write.saveAsTable",
      "file": "entrypoint.py",
      "line": 28,
      "mode": "overwrite",
      "options": {"overwriteSchema": "true"},
      "target_type": "table",
      "dynamic": false
    }
  ],
  "wrappers": [],
  "dependencies": [],
  "external_dependencies": [],
  "unclassified_table_refs": [],
  "unresolved_dynamic": [],
  "warnings": []
}
```

- **`inputs` / `outputs`**: tabelas e caminhos de entrada/saída mapeados (CTEs e temp views locais são filtradas automaticamente).
- **`wrappers`**: funções definidas no repositório detectadas como wrappers de I/O Spark.
- **`dependencies`**: ligações locais resolvidas (`%run` e `dbutils.notebook.run`, incluindo argumentos de widgets).
- **`external_dependencies`**: referências a notebooks fora do repositório local (ex.: `/Shared/...`, `/Repos/...`).
- **`unclassified_table_refs`**: chamadas a funções externas desconhecidas recebendo strings com formato de tabela (ex.: `ler_base("pr_xpto.xpto_ing.tbl")`).
- **`unresolved_dynamic`**: operações cujo alvo é 100% opaco em tempo estático (ex.: nome vindo de consulta externa em runtime).

## Desenvolvimento e Qualidade

O projeto usa `poethepoet` para rodar toda a esteira (`ruff`, `ty`, `pydoclint`, `xenon` e `pytest`):

```bash
poe check   # format-check + lint + typecheck + docstrings + complexity + test
poe fix     # aplica ruff format e ruff check --fix
poe test    # executa apenas a suíte pytest
```
