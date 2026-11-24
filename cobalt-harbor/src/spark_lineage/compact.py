"""Geração e renderização do artefato compactado de linhagem por job (otimizado para LLMs e consultas offline).

Descarta os textos brutos extensos de ``plans[*].sections`` (parsed/analyzed/optimized/physical/formatted)
após pré-calcular:
- fontes, views, destinos, fluxos, ramos explorados, eventos e avisos;
- queries SQL capturadas (``queries``);
- linhagem e impacto multi-hop por coluna (``column_lineage``).

Todos os comandos offline (``sources``, ``query`` e ``impact``) reconhecem este formato
automaticamente sem precisar do ``plan.json`` bruto.
"""

from __future__ import annotations

from typing import Any

from .impact import trace_column_impact
from .sources import (
    Catalog,
    Json,
    _match_table,
    build_sources,
    extract_plan_queries,
    render_markdown,
)

COMPACT_FORMAT = "spark-lineage-compact/v1"


def is_compact_doc(doc: Json) -> bool:
    """Retorna ``True`` se ``doc`` estiver no formato compactado ``spark-lineage-compact/v1``."""
    return isinstance(doc, dict) and str(doc.get("format", "")).startswith("spark-lineage-compact/")


def _matches_any_table(candidate: str | None, tables: list[str] | None) -> bool:
    if not tables:
        return True
    if not candidate:
        return False
    return any(_match_table(candidate, t) or _match_table(t, candidate) for t in tables)


def _filter_queries(queries: list[Json], tables: list[str]) -> list[Json]:
    out: list[Json] = []
    for item in queries:
        if _matches_any_table(item.get("target"), tables):
            out.append(item)
            continue
        q_low = (item.get("query") or "").lower()
        if any(t.replace("`", "").strip().lower() in q_low for t in tables):
            out.append(item)
    return out


def _build_column_lineage(
    raw_result: Json,
    sources: list[Json],
    *,
    max_hops: int = 5,
    tables: list[str] | None = None,
    columns: list[str] | None = None,
) -> dict[str, dict[str, Json]]:
    col_filter = {c.strip().lower() for c in columns if c.strip()} if columns else None
    lineage: dict[str, dict[str, Json]] = {}

    for src in sources:
        tname = src.get("table")
        if not tname or not _matches_any_table(tname, tables):
            continue
        tbl_lineage: dict[str, Json] = {}
        for col in src.get("columns", []):
            cname = col.get("name")
            if not cname:
                continue
            if col.get("read") is False:
                continue
            if col_filter is not None and cname.lower() not in col_filter:
                continue
            rep = trace_column_impact(raw_result, table=tname, column=cname, max_hops=max_hops)
            if not rep["summary"]["found"]:
                continue
            tbl_lineage[cname] = {
                "type": rep["type"],
                "comment": rep["comment"],
                "read_sites": rep["read_sites"],
                "summary": rep["summary"],
                "destinations_affected": rep["destinations_affected"],
                "hops": rep["hops"],
            }
        if tbl_lineage:
            lineage[tname] = tbl_lineage
    return lineage


def compact_plan(
    raw_result: Json,
    *,
    catalog: Catalog | None = None,
    max_hops: int = 5,
    tables: list[str] | None = None,
    columns: list[str] | None = None,
) -> Json:
    """Compacta o resultado de captura em um documento enxuto autocontido (sem ``plans[*].sections``).

    Args:
        raw_result: Documento bruto gerado por ``capture_plans`` (ou já compactado).
        catalog: Catálogo opcional para enriquecer tipos e comentários.
        max_hops: Máximo de saltos multi-hop pré-calculados por coluna.
        tables: Lista opcional de tabelas (ou sufixos) para filtrar o compactado.
        columns: Lista opcional de nomes de colunas para filtrar ``column_lineage``.

    Returns:
        Documento no formato ``spark-lineage-compact/v1``.
    """
    if is_compact_doc(raw_result):
        doc = build_sources(raw_result, catalog=catalog)
        queries = list(raw_result.get("queries") or [])
        lineage = raw_result.get("column_lineage") or {}
        if tables or columns:
            col_filter = {c.strip().lower() for c in columns if c.strip()} if columns else None
            filtered_lineage: dict[str, dict[str, Json]] = {}
            for tname, cmap in lineage.items():
                if not _matches_any_table(tname, tables):
                    continue
                kept = {
                    cn: info
                    for cn, info in cmap.items()
                    if col_filter is None or cn.lower() in col_filter
                }
                if kept:
                    filtered_lineage[tname] = kept
            lineage = filtered_lineage
    else:
        doc = build_sources(raw_result, catalog=catalog)
        queries = extract_plan_queries(raw_result.get("plans", []))
        lineage = _build_column_lineage(
            raw_result,
            doc["sources"],
            max_hops=max_hops,
            tables=tables,
            columns=columns,
        )

    sources = doc["sources"]
    destinations = doc["destinations"]
    flows = doc["flows"]
    views = doc["views"]
    cat_out = catalog if catalog is not None else (raw_result.get("catalog") or {})

    if tables:
        sources = [s for s in sources if _matches_any_table(s.get("table"), tables)]
        destinations = [d for d in destinations if _matches_any_table(d.get("table"), tables)]
        flows = [
            f
            for f in flows
            if any(_matches_any_table(r, tables) for r in f.get("reads", []))
            or any(_matches_any_table(w, tables) for w in f.get("writes", []))
        ]
        queries = _filter_queries(queries, tables)
        cat_out = {k: v for k, v in cat_out.items() if _matches_any_table(k, tables)}

    return {
        "format": COMPACT_FORMAT,
        "project_root": raw_result.get("project_root"),
        "entrypoint": doc.get("entrypoint"),
        "params": raw_result.get("params") or {},
        "settings": raw_result.get("settings") or {},
        "status": raw_result.get("status", doc.get("capture_status")),
        "capture_status": doc.get("capture_status"),
        "error": raw_result.get("error"),
        "plans_analyzed": doc.get("plans_analyzed", 0),
        "sources": sources,
        "views": views,
        "destinations": destinations,
        "flows": flows,
        "branches": raw_result.get("branches") or [],
        "queries": queries,
        "column_lineage": lineage,
        "catalog": cat_out,
        "events": doc.get("events", []),
        "warnings": doc.get("warnings", []),
    }


def _md_queries(queries: list[Json]) -> list[str]:
    if not queries:
        return []
    lines = [f"## Queries SQL capturadas ({len(queries)})"]
    for q in queries:
        tgt = f" -> `{q['target']}`" if q.get("target") else ""
        site = f" @ `{q['call_site']}`" if q.get("call_site") else ""
        br = f" [ramo: {q['branch']}]" if q.get("branch") else ""
        single_line = " ".join((q.get("query") or "").split())
        lines.append(f"- **{q.get('plan', '?')}** `{q.get('action', '')}`{tgt}{site}{br}: `{single_line[:240]}`")
    return lines


def _md_column_lineage(lineage: dict[str, dict[str, Json]]) -> list[str]:
    if not lineage:
        return []
    total_cols = sum(len(cols) for cols in lineage.values())
    lines = [f"## Linhagem por coluna ({total_cols} colunas mapeadas)"]
    for tname, cols in lineage.items():
        lines.append(f"### `{tname}`")
        for cname, info in cols.items():
            typ = f" ({info['type']})" if info.get("type") else ""
            dests = ", ".join(
                f"`{d['table']}` ({', '.join(c['name'] for c in d.get('columns', [])) or 'filtro/join'})"
                for d in info.get("destinations_affected", [])
            ) or "(sem escrita)"
            ops = info.get("summary", {}).get("operations_count", 0)
            hops = info.get("summary", {}).get("hops", 0)
            lines.append(f"- **`{cname}`**{typ} — {hops} hop(s), {ops} op(s) -> {dests}")
            for hop in info.get("hops", []):
                for p in hop.get("plans", []):
                    for f in p.get("filters", []):
                        lines.append(f"  - [Filter {p['plan_id']}] `{f['expression']}`")
                    for j in p.get("joins", []):
                        lines.append(f"  - [Join {j['join_type']} {p['plan_id']}] `{j['condition']}`")
                    for tr in p.get("transformations", []):
                        lines.append(f"  - [{tr['kind']} {p['plan_id']}] `{tr['expression']}`")
    return lines


def render_compact_markdown(compact_doc: Json) -> str:
    """Renderiza um documento compactado como Markdown enxuto pronto para revisão humana ou LLM."""
    base_md = render_markdown(compact_doc, tag="all").rstrip()
    extra_sections: list[str] = [base_md]

    branches = compact_doc.get("branches") or []
    if branches:
        extra_sections.append(render_markdown(compact_doc, tag="branches").rstrip())

    q_lines = _md_queries(compact_doc.get("queries") or [])
    if q_lines:
        extra_sections.append("\n".join(q_lines))

    lin_lines = _md_column_lineage(compact_doc.get("column_lineage") or {})
    if lin_lines:
        extra_sections.append("\n".join(lin_lines))

    return "\n\n".join(s for s in extra_sections if s) + "\n"
