#!/usr/bin/env python3
"""Ponto de entrada único baseado em um único artefato autocontido (plans.json / plan.json).

Comandos disponíveis::

    spark-lineage doctor [--connect] [--profile DEFAULT] [--format text|json]
        -> "O ambiente e as bibliotecas estão prontos para rodar?"

    spark-lineage capture --project-root ./proj --entrypoint jobs/job.py --output plan.json
        -> Captura planos, explora ramos if/else, busca schemas no Unity Catalog e consolida tudo em um único plan.json.

    spark-lineage sources --plans plan.json [--tag all|sources|...] [--table TABELA] [--format md|json]
        -> Consulta instantânea e 100% offline em cima do plan.json (não gera arquivos extras por padrão).

    spark-lineage query --plans plan.json --tag destinations [--table TABELA] [--format json|md]
        -> Atalho para consultar tags específicas do plan.json (padrão: --format json).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .compact import compact_plan, render_compact_markdown
from .doctor import render_doctor_text, run_doctor
from .impact import render_impact_markdown, trace_column_impact
from .sources import VALID_TAGS, build_sources, discover_doc_tables, discover_plan_tables, extract_tag, render_markdown

Json = dict[str, Any]


def _fetch_single_table_schema(spark: Any, table: str) -> dict[str, Json] | None:
    try:
        fields = spark.table(table).schema.fields
    except Exception as exc:  # noqa: BLE001
        print(f"[cli] AVISO: não consegui ler o schema de {table}: {exc!r}", file=sys.stderr)
        return None
    return {
        f.name: {
            "type": f.dataType.simpleString(),
            "comment": (f.metadata or {}).get("comment"),
        }
        for f in fields
    }


def fetch_catalog_schema(tables: list[str]) -> dict[str, dict[str, Json] | None]:
    """Busca tipos e comentários de TODAS as colunas via Unity Catalog (exige Databricks Connect).

    Args:
        tables: Lista de nomes completos de tabelas.

    Returns:
        Mapa ``tabela -> {coluna -> {type, comment}}`` (ou ``None`` quando a leitura falha).
    """
    from .capture_plans import get_spark

    spark = get_spark()
    return {t: _fetch_single_table_schema(spark, t) for t in tables}


def _write_optional_output(output_path: str | None, payload: Any, out_text: str, label: str) -> None:
    if not output_path:
        return
    content = json.dumps(payload, indent=2, ensure_ascii=False) if output_path.endswith(".json") else out_text
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(content, encoding="utf-8")
    print(f"[cli] {label}: gravado em {output_path}", file=sys.stderr)


def cmd_sources(ns: argparse.Namespace) -> int:
    """Executa os subcomandos ``sources`` e ``query`` sobre um arquivo ``plan.json``.

    Args:
        ns: Argumentos parseados pelo ``argparse``.

    Returns:
        Código de saída (0).
    """
    result = json.loads(Path(ns.plans).read_text(encoding="utf-8"))
    catalog = result.get("catalog")
    if ns.catalog:
        tables = discover_doc_tables(result)
        catalog = fetch_catalog_schema(tables)
        result = {**result, "catalog": catalog}
    doc = build_sources(result, catalog=catalog)
    payload = extract_tag(result, doc, tag=ns.tag, table=ns.table)

    if ns.format == "md" and ns.tag not in ("catalog", "plans", "column_lineage"):
        view_doc = payload if ns.tag == "all" else {**doc, ns.tag: payload}
        out_text = render_markdown(view_doc, tag=ns.tag)
    else:
        out_text = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"

    _write_optional_output(ns.output, payload, out_text, f"{ns.cmd} (tag={ns.tag})")
    sys.stdout.write(out_text)
    return 0


def cmd_compact(ns: argparse.Namespace) -> int:
    """Executa o subcomando ``compact`` para compactar/filtrar/renderizar um artefato do job."""
    result = json.loads(Path(ns.plans).read_text(encoding="utf-8"))
    tables = [t.strip() for t in (ns.table or []) if t.strip()] or None
    columns: list[str] | None = None
    if ns.columns:
        columns = [c.strip() for raw in ns.columns for c in raw.split(",") if c.strip()] or None
    compact_doc = compact_plan(result, max_hops=ns.max_hops, tables=tables, columns=columns)
    if ns.format == "md":
        out_text = render_compact_markdown(compact_doc)
    else:
        out_text = json.dumps(compact_doc, indent=2, ensure_ascii=False, default=str) + "\n"
    _write_optional_output(ns.output, compact_doc, out_text, "compact")
    sys.stdout.write(out_text)
    return 0


def cmd_doctor(ns: argparse.Namespace) -> int:
    """Executa o subcomando ``doctor`` para diagnóstico do ambiente.

    Args:
        ns: Argumentos parseados pelo ``argparse``.

    Returns:
        Código de saída (0 para ``ok``/``warn``, 1 para ``error``).
    """
    report = run_doctor(connect=ns.connect, profile=ns.profile)
    if ns.format == "json":
        sys.stdout.write(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(render_doctor_text(report))
    return 1 if report["status"] == "error" else 0


def cmd_impact(ns: argparse.Namespace) -> int:
    """Executa o subcomando ``impact`` para rastrear o impacto de uma coluna.

    Args:
        ns: Argumentos parseados pelo ``argparse``.

    Returns:
        Código de saída (0).
    """
    result = json.loads(Path(ns.plans).read_text(encoding="utf-8"))
    report = trace_column_impact(result, table=ns.table, column=ns.column, max_hops=ns.max_hops)
    if ns.format == "md":
        out_text = render_impact_markdown(report)
    else:
        out_text = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    _write_optional_output(ns.output, report, out_text, "impact")
    sys.stdout.write(out_text)
    return 0


def _add_query_args(sp: argparse.ArgumentParser, default_format: str) -> None:
    sp.add_argument("--plans", default="plan.json", help="arquivo único gerado pelo capture (padrão: plan.json)")
    sp.add_argument(
        "--tag",
        choices=VALID_TAGS,
        default="all",
        help="seção específica do plan.json para retornar (padrão: all)",
    )
    sp.add_argument("--table", default=None, help="filtra o resultado por nome (ou sufixo) de tabela")
    sp.add_argument(
        "--format",
        choices=["md", "json"],
        default=default_format,
        help=f"formato no stdout (padrão: {default_format})",
    )
    sp.add_argument(
        "--output",
        default=None,
        help="opcional: grava a saída em arquivo (por padrão só exibe no stdout)",
    )
    sp.add_argument(
        "--catalog",
        action="store_true",
        help="força atualização ao vivo do Unity Catalog (o capture já salva o catalog no plan.json)",
    )


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="spark-lineage", description="CLI de mapeamento de colunas em jobs Spark.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser(
        "capture",
        help="captura planos, ramos e catálogo em um único JSON (use: spark-lineage capture --help)",
    )

    sp_sources = sub.add_parser(
        "sources",
        help="consulta fontes, destinos, fluxos ou qualquer tag do plan.json (offline)",
    )
    _add_query_args(sp_sources, default_format="json")

    sp_query = sub.add_parser(
        "query",
        help="busca uma tag/tabela específica no plan.json (padrão: JSON no stdout)",
    )
    _add_query_args(sp_query, default_format="json")

    sp_imp = sub.add_parser("impact", help="rastreia filtros, joins, transformações e destinos de uma coluna")
    sp_imp.add_argument("--plans", default="plan.json", help="arquivo gerado pelo capture (compactado ou bruto)")
    sp_imp.add_argument("--table", required=True, help="nome completo ou sufixo da tabela de origem")
    sp_imp.add_argument("--column", required=True, help="nome da coluna a rastrear")
    sp_imp.add_argument(
        "--max-hops",
        type=int,
        default=5,
        help="máximo de saltos em tabelas intermediárias/staging (padrão: 5)",
    )
    sp_imp.add_argument("--format", choices=["json", "md"], default="json", help="formato no stdout (padrão: json)")
    sp_imp.add_argument("--output", default=None, help="opcional: grava a saída em arquivo")

    sp_cmp = sub.add_parser(
        "compact",
        help="compacta um plan.json bruto (ou filtra/renderiza um compactado) com fontes, queries e linhagem por coluna",
    )
    sp_cmp.add_argument("--plans", default="plan.json", help="arquivo plan.json (bruto ou já compactado)")
    sp_cmp.add_argument(
        "--table",
        action="append",
        default=[],
        help="filtra por tabela ou sufixo (repetível)",
    )
    sp_cmp.add_argument(
        "--columns",
        action="append",
        default=[],
        help="filtra colunas em column_lineage (separadas por vírgula ou repetível)",
    )
    sp_cmp.add_argument(
        "--max-hops",
        type=int,
        default=5,
        help="máximo de saltos multi-hop por coluna (padrão: 5)",
    )
    sp_cmp.add_argument("--format", choices=["json", "md"], default="json", help="formato no stdout (padrão: json)")
    sp_cmp.add_argument("--output", default=None, help="opcional: grava a saída em arquivo")

    sp_doc = sub.add_parser("doctor", help="verifica bibliotecas instaladas, configuração Databricks e sessão Spark")
    sp_doc.add_argument(
        "--connect",
        action="store_true",
        help="testa a criação real da sessão Spark / Databricks Connect",
    )
    sp_doc.add_argument(
        "--profile",
        default=None,
        help="perfil do ~/.databrickscfg para validar (padrão: DEFAULT ou DATABRICKS_CONFIG_PROFILE)",
    )
    sp_doc.add_argument("--format", choices=["text", "json"], default="text", help="formato da saída")
    return ap


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada principal da CLI ``spark-lineage``.

    Args:
        argv: Lista de argumentos de linha de comando (usa ``sys.argv[1:]`` se ``None``).

    Returns:
        Código de saída do subcomando executado.
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "capture":  # repassa todos os argumentos ao capture_plans (inclui --help)
        from . import capture_plans

        return capture_plans.main(argv[1:])

    ns = _build_parser().parse_args(argv)
    if ns.cmd == "doctor":
        return cmd_doctor(ns)
    if ns.cmd == "impact":
        return cmd_impact(ns)
    if ns.cmd == "compact":
        return cmd_compact(ns)
    return cmd_sources(ns)


if __name__ == "__main__":
    sys.exit(main())
