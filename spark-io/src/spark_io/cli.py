"""Interface de linha de comando para o `spark-io`."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from spark_io.mapper import map_spark_io


def _parse_var(raw: str) -> tuple[str, str]:
    """Converte um argumento `chave=valor` em par de strings.

    Args:
        raw: Texto recebido via flag `--var`.

    Returns:
        Tupla `(chave, valor)` normalizada.

    Raises:
        argparse.ArgumentTypeError: Quando o argumento não contém `=`.
    """
    if "=" not in raw:
        msg = f"Formato inválido para --var: '{raw}'. Use chave=valor."
        raise argparse.ArgumentTypeError(msg)
    key, value = raw.split("=", 1)
    return key.strip(), value.strip()


def _build_parser() -> argparse.ArgumentParser:
    """Constrói o parser de argumentos da CLI.

    Returns:
        Instância configurada de `argparse.ArgumentParser`.
    """
    parser = argparse.ArgumentParser(
        prog="spark-io",
        description="Mapeamento estático read-only de entradas e saídas Spark/Databricks.",
    )
    parser.add_argument("--path", required=True, type=Path, help="Diretório raiz ou arquivo a analisar.")
    parser.add_argument("--entrypoint", type=Path, default=None, help="Arquivo entrypoint opcional dentro de --path.")
    parser.add_argument("--env-prefix", default="pr", help="Prefixo de ambiente para catálogos (padrão: pr).")
    parser.add_argument(
        "--var",
        action="append",
        default=[],
        type=_parse_var,
        help="Variável conhecida no formato chave=valor (pode ser repetida).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Executa a CLI do `spark-io` e imprime o resultado em JSON.

    Args:
        argv: Lista opcional de argumentos de linha de comando.

    Returns:
        Código de saída do processo (`0` em sucesso).
    """
    args = _build_parser().parse_args(argv)
    variables = dict(args.var) if args.var else None
    result = map_spark_io(
        args.path,
        entrypoint=args.entrypoint,
        env_prefix=args.env_prefix,
        variables=variables,
    )
    sys.stdout.write(json.dumps(result.to_dict(), ensure_ascii=False, indent=2) + "\n")
    return 0
