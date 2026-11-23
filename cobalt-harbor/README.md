# spark-lineage

CLI e biblioteca Python para captura de planos de execução Spark (sem editar o código do job) e mapeamento de fontes, destinos e colunas.

## Instalação

Na raiz deste repositório, instale o pacote e as dependências listadas em `requirements.txt` / `pyproject.toml`:

```bash
pip install -r requirements.txt
pip install -e .
```

Se você utiliza **Databricks Connect** (que já inclui seu próprio `pyspark`), instale com o extra `databricks`:

```bash
pip install -e ".[databricks]"
```

## Uso no terminal (CLI)

Após instalar, o comando `spark-lineage` (ou `python -m spark_lineage`) fica disponível:

### 0. Diagnosticar o ambiente (`doctor`)

Verifica versão do Python, pacotes instalados (`pyspark`, `databricks-connect`, `delta-spark`, `pandas`), configurações de autenticação do Databricks (`~/.databrickscfg` e variáveis de ambiente), `JAVA_HOME` e, opcionalmente, testa a conexão real com a sessão Spark:

```bash
spark-lineage doctor
spark-lineage doctor --connect --profile DEFAULT
spark-lineage doctor --format json
```

### 1. Capturar planos de execução de um job (`capture`)

```bash
spark-lineage capture \
    --project-root ./meu_projeto \
    --entrypoint jobs/etl_vendas.py \
    --output plans.json \
    --text-dir planos/ \
    --param data_ref=2026-11-01
```

### 2. Mapear fontes, destinos e colunas (`sources`)

```bash
spark-lineage sources \
    --plans plans.json \
    --output sources.json \
    --format md
```

Adicione `--catalog` caso queira consultar tipos e comentários diretamente no Unity Catalog via Databricks Connect.

## Uso como biblioteca Python

```python
from spark_lineage.capture_plans import capture_plans
from spark_lineage import build_sources, render_markdown, run_doctor

# 0. Diagnóstico do ambiente
print(run_doctor(connect=False))

# 1. Captura planos (requer sessão Spark / Databricks Connect)
result = capture_plans("./meu_projeto", "jobs/etl_vendas.py")

# 2. Extrai fontes e destinos (puro Python, sem necessidade de Spark)
doc = build_sources(result)
print(render_markdown(doc))
```

## Testes

```bash
python tests/test_plan_parser.py
# ou com pytest:
pytest
```
