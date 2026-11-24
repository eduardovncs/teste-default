"""Extrai e formata fontes, destinos, fluxos e avisos a partir do plan.json.

Entrada: dict gerado por ``capture_plans`` (chaves: status, error, plans, catalog, branches, events, warnings...).
Saída: dict consolidado (``build_sources``) + renderização em Markdown/JSON por seção
(``render_markdown`` / ``extract_tag``).

Não precisa de Spark nem de conexão (usa o ``catalog`` já embutido no plan.json pelo ``capture``).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from typing import Any

from .plan_parser import (
    is_collapsed_to_empty,
    other_relation_classes,
    parse_merge_targets,
    parse_relations,
    parse_scans,
    parse_views,
)

WRITE_PREFIXES = ("write.", "writeStream.")
VALID_TAGS = (
    "all",
    "sources",
    "destinations",
    "flows",
    "views",
    "branches",
    "queries",
    "column_lineage",
    "events",
    "warnings",
    "catalog",
    "plans",
)
_IN_PLACE_DML_ACTIONS = ("sql:DELETE", "sql:UPDATE")
_SQL_READ_ACTIONS = frozenset({"sql:SELECT", "sql:WITH", "sql:VALUES", "sql:TABLE", "sql:FROM"})
_UNNAMED_MERGE = "<merge sem destino nomeado>"

Json = dict[str, Any]
Catalog = dict[str, Any]


# --------------------------------------------------------------------------- #
# Helpers compartilhados (também usados por impact.py)
# --------------------------------------------------------------------------- #


def _add(lst: list, item: object) -> None:
    if item not in lst:
        lst.append(item)


def site_label(occ: Json) -> str | None:
    """Formata o local de chamada de uma ocorrência como ``arquivo:linha``.

    Args:
        occ: Ocorrência de plano (com ``call_site`` opcional).

    Returns:
        ``"arquivo:linha"`` ou ``None`` se não houver arquivo.
    """
    s = occ.get("call_site") or {}
    return f"{s['file']}:{s['line']}" if s.get("file") else None


def plan_analyzed(plan: Json) -> str:
    """Retorna a seção 'analyzed' (ou 'raw') de um plano capturado.

    Args:
        plan: Plano capturado.

    Returns:
        O texto do plano analisado, ou string vazia.
    """
    secs = plan.get("sections", {})
    return secs.get("analyzed") or secs.get("raw") or ""


def in_place_dml_targets(plan: Json) -> set[str]:
    """Tabelas alteradas in-place (DELETE/UPDATE) por um plano.

    Nesses comandos a própria tabela alvo aparece como Relation dentro de DeleteFromTable/UpdateTable;
    ela é um destino (e não uma fonte lida).

    Args:
        plan: Plano capturado.

    Returns:
        Conjunto com os nomes das tabelas alvo.
    """
    return {
        occ["target"]
        for occ in plan.get("occurrences", [])
        if occ.get("action") in _IN_PLACE_DML_ACTIONS and occ.get("target")
    }


_EXTRA_WRITE_ACTIONS = frozenset({"mergeInto(source)", "delta.merge(source)"})


def is_write_action(action: str) -> bool:
    """Indica se a ação grava dados (writer batch/streaming, merge ou SQL DML/DDL).

    Args:
        action: Nome da ação registrada na ocorrência.

    Returns:
        ``True`` para ações de escrita.
    """
    if action in _SQL_READ_ACTIONS:
        return False
    return action in _EXTRA_WRITE_ACTIONS or action.startswith((*WRITE_PREFIXES, "sql:"))


def _is_real_table(name: str | None) -> bool:
    return bool(name) and not name.startswith("<") and "/" not in name and ":" not in name


def discover_plan_tables(plans: list[Json]) -> list[str]:
    """Lista todas as tabelas reais (fontes e destinos) referenciadas nos planos capturados.

    Args:
        plans: Lista de planos do plan.json.

    Returns:
        Nomes de tabela únicos, na ordem em que aparecem.
    """
    tables: list[str] = []
    for plan in plans:
        analyzed = plan_analyzed(plan)
        candidates = [
            *(rel.get("table") for rel in parse_relations(analyzed)),
            *(mt.get("table") for mt in parse_merge_targets(analyzed)),
            *(occ.get("target") for occ in plan.get("occurrences", [])),
        ]
        for t in candidates:
            if _is_real_table(t):
                _add(tables, t)
    return tables


def discover_doc_tables(doc: Json) -> list[str]:
    """Lista todas as tabelas reais tanto de um ``plan.json`` bruto quanto de um documento compactado."""
    if doc.get("plans"):
        return discover_plan_tables(doc["plans"])
    tables: list[str] = []
    for item in [*(doc.get("sources") or []), *(doc.get("destinations") or [])]:
        t = item.get("table")
        if _is_real_table(t):
            _add(tables, t)
    return tables


def extract_plan_queries(plans: list[Json]) -> list[Json]:
    """Extrai e deduplica todas as queries SQL registradas nas ocorrências dos planos."""
    out: list[Json] = []
    seen: set[tuple[Any, ...]] = set()
    for plan in plans:
        pid = plan.get("id")
        for occ in plan.get("occurrences", []):
            q = ((occ.get("extra") or {}).get("query") or "").strip()
            if not q:
                continue
            site = site_label(occ)
            sig = (pid, occ.get("action"), occ.get("target"), site, q)
            if sig in seen:
                continue
            seen.add(sig)
            item: Json = {
                "plan": pid,
                "action": occ.get("action"),
                "target": occ.get("target"),
                "call_site": site,
                "query": q,
            }
            if occ.get("branch"):
                item["branch"] = occ["branch"]
            out.append(item)
    return out


# --------------------------------------------------------------------------- #
# Consolidação
# --------------------------------------------------------------------------- #


def _new_source(table: str, fmt: str | None) -> Json:
    return {
        "table": table,
        "format": fmt,
        "columns": {},
        "plans": [],
        "action_sites": [],
        "has_scan": False,
        "scanned": set(),
        "filters": [],
        "schema_truncated": False,
        "filters_truncated": False,
        "columns_truncated": False,
        "scan_columns_truncated": False,
    }


def _col_key_ci(columns: dict[str, Json], name: str) -> str:
    if name in columns:
        return name
    low = name.lower()
    for k in columns:
        if k.lower() == low:
            return k
    return name


def _merge_scan(entry: Json, scan: Json) -> None:
    entry["has_scan"] = True
    rs_ci = {k.lower(): v for k, v in scan["read_schema"].items()}
    for c in scan["columns"]:
        ckey = _col_key_ci(entry["columns"], c["name"])
        col = entry["columns"].setdefault(ckey, {"name": ckey, "type": None})
        entry["scanned"].add(c["name"].lower())
        if c["name"] in scan["read_schema"]:
            col["type"] = scan["read_schema"][c["name"]]
        elif c["name"].lower() in rs_ci:
            col["type"] = rs_ci[c["name"].lower()]
    for f in scan["filters"]:
        _add(entry["filters"], f)
    entry["schema_truncated"] |= scan["read_schema_truncated"]
    entry["filters_truncated"] |= scan["filters_truncated"]
    entry["scan_columns_truncated"] |= scan.get("columns_truncated", False)


def _new_destination(table: str | None, **extra: Any) -> Json:
    return {"table": table, "actions": [], "plans": [], "columns": None, "call_sites": [], **extra}


def _merge_columns(cat_dest: Json | None, col_names: list[str]) -> list[Json]:
    table_cat = cat_dest or {}
    return [{"name": n, "type": (table_cat.get(n) or {}).get("type")} for n in col_names]


class _SourcesBuilder:
    # Acumula fontes, destinos, views e fluxos plano a plano.

    def __init__(self, catalog: Catalog | None, read_sites: dict[str, list[str]] | None = None) -> None:
        self.catalog: Catalog = catalog or {}
        self._catalog_ci: dict[str, Json] = {
            k.replace("`", "").strip().lower(): v for k, v in self.catalog.items() if isinstance(v, dict)
        }
        self.sources: dict[str, Json] = {}
        self.views: dict[str, list[str]] = {}
        self.dests: dict[str, Json] = {}
        self.flows: list[Json] = []
        self.collapsed: list[str] = []
        self.merge_without_source: list[tuple[str, str | None]] = []
        self.unparsed: Counter[str] = Counter()
        self._unnamed_paths: list[str] = [k for k in (read_sites or {}) if "/" in k or "\\" in k]
        self._unnamed_sig_map: dict[tuple[str | None, tuple[tuple[str, int | None], ...]], str] = {}

    def _catalog_for(self, table: str | None) -> Json | None:
        if not table:
            return None
        return self.catalog.get(table) or self._catalog_ci.get(table.replace("`", "").strip().lower())

    def _unnamed_source_name(self, rel: Json) -> str:
        col_sig = tuple((c["name"], c.get("id")) for c in rel["columns"])
        sig = (rel.get("format"), col_sig)
        if sig in self._unnamed_sig_map:
            return self._unnamed_sig_map[sig]
        idx = len(self._unnamed_sig_map)
        if idx < len(self._unnamed_paths):
            name = self._unnamed_paths[idx]
        else:
            suffix = f" #{idx + 1}" if idx > 0 else ""
            name = f"<relação sem nome ({rel['format']}){suffix}>"
        self._unnamed_sig_map[sig] = name
        return name

    def add_plan(self, plan: Json) -> None:
        pid = plan["id"]
        secs = plan.get("sections", {})
        analyzed = plan_analyzed(plan)
        physical = secs.get("physical", "")
        rels = parse_relations(analyzed)
        scans = parse_scans(physical, secs.get("formatted", ""))
        in_place = in_place_dml_targets(plan)

        reads = self._add_reads(plan, rels, scans, in_place)
        if rels and not scans and not in_place and is_collapsed_to_empty(physical):
            self.collapsed.append(pid)
        for v in parse_views(analyzed):
            _add(self.views.setdefault(v, []), pid)
        self.unparsed.update(other_relation_classes(analyzed))

        writes: list[str] = []
        self._add_writes(plan, writes)
        self._add_merges(plan, analyzed, writes, has_reads=bool(rels))
        if reads or writes:
            self.flows.append({"plan": pid, "reads": reads, "writes": writes})

    def _register_relation(self, rel: Json, pid: str, plan_sites: list[str], scans: list[Json]) -> str:
        tname = rel["table"] or self._unnamed_source_name(rel)
        skey = tname.lower()
        entry = self.sources.setdefault(skey, _new_source(tname, rel["format"]))
        _add(entry["plans"], pid)
        for ps in plan_sites:
            _add(entry["action_sites"], ps)
        for c in rel["columns"]:
            ckey = _col_key_ci(entry["columns"], c["name"])
            entry["columns"].setdefault(ckey, {"name": ckey, "type": None})
        entry["columns_truncated"] |= rel["columns_truncated"]
        for sc in scans:
            _merge_scan(entry, sc)
        return entry["table"]

    def _add_reads(self, plan: Json, rels: list[Json], scans: list[Json], in_place: set[str]) -> list[str]:
        scans_by_table: dict[str | None, list[Json]] = {}
        for sc in scans:
            key = sc["table"].lower() if isinstance(sc["table"], str) else None
            scans_by_table.setdefault(key, []).append(sc)
        plan_sites = [s for s in map(site_label, plan.get("occurrences", [])) if s]

        effective_rels = rels
        if not effective_rels and scans:
            effective_rels = [
                {
                    "table": sc["table"],
                    "format": sc["format"],
                    "columns": sc["columns"],
                    "columns_truncated": sc.get("columns_truncated", False),
                }
                for sc in scans
                if sc.get("table")
            ]

        reads: list[str] = []
        unnamed_scans = scans_by_table.get(None, [])
        unnamed_idx = 0
        for rel in effective_rels:
            rtbl = rel["table"]
            if rtbl is None:
                rel_ids = {c["id"] for c in rel["columns"] if c.get("id") is not None}
                matched_scans = [
                    sc
                    for sc in unnamed_scans
                    if rel_ids and ({c["id"] for c in sc["columns"] if c.get("id") is not None} & rel_ids)
                ]
                if not matched_scans and unnamed_idx < len(unnamed_scans):
                    sc_cand = unnamed_scans[unnamed_idx]
                    sc_ids = {c["id"] for c in sc_cand["columns"] if c.get("id") is not None}
                    if not (rel_ids and sc_ids):
                        matched_scans = [sc_cand]
                unnamed_idx += 1
                tname = self._register_relation(rel, plan["id"], plan_sites, matched_scans)
                _add(reads, tname)
            elif not any(_match_table(rtbl, t) or _match_table(t, rtbl) for t in in_place):
                tname = self._register_relation(rel, plan["id"], plan_sites, scans_by_table.get(rtbl.lower(), []))
                _add(reads, tname)
        return reads

    def _add_writes(self, plan: Json, writes: list[str]) -> None:
        pid = plan["id"]
        for occ in plan.get("occurrences", []):
            action, target = occ.get("action", ""), occ.get("target")
            site = site_label(occ)
            if target and is_write_action(action):
                d = self.dests.setdefault(target, _new_destination(target))
                _add(d["actions"], action)
                _add(d["plans"], pid)
                if d["columns"] is None and plan.get("output_columns"):
                    d["columns"] = plan["output_columns"]
                _add(writes, target)
            elif action.startswith("sql:") and action not in _SQL_READ_ACTIONS:
                q = (occ.get("extra") or {}).get("query", "")[:160]
                d = self.dests.setdefault(
                    f"<{action}: {q}>", _new_destination(None, actions=[action], plans=[pid], query=q)
                )
            else:
                continue
            if site:
                _add(d["call_sites"], site)

    def _add_merges(self, plan: Json, analyzed: str, writes: list[str], has_reads: bool) -> None:
        pid = plan["id"]
        first_site = next((s for s in map(site_label, plan.get("occurrences", [])) if s), None)
        for mt in parse_merge_targets(analyzed):
            key = mt["table"] or _UNNAMED_MERGE
            d = self.dests.setdefault(key, _new_destination(mt["table"]))
            _add(d["actions"], "delta.merge")
            _add(d["plans"], pid)
            if d["columns"] is None:
                d["columns"] = _merge_columns(self._catalog_for(mt["table"]), mt["columns"])
            if first_site:
                _add(d["call_sites"], first_site)
            _add(writes, key)
            if not has_reads:
                self.merge_without_source.append((pid, mt["table"]))

    def enrich_destinations(self) -> None:
        # Completa tipos/comentários das colunas de destino usando o catalog quando disponível.
        for d in self.dests.values():
            cat_d = self._catalog_for(d.get("table"))
            if cat_d and d.get("columns"):
                _enrich_columns(d["columns"], cat_d)

    def source_list(self, read_sites_map: dict[str, list[str]]) -> list[Json]:
        return [_finalize_source(e, self._catalog_for(e["table"]), read_sites_map) for e in self.sources.values()]


def _catalog_col(cat: Json | None, name: str) -> Json | None:
    if not cat:
        return None
    if name in cat:
        return cat[name]
    low = name.lower()
    for k, v in cat.items():
        if k.lower() == low:
            return v
    return None


def _enrich_columns(columns: list[Json], cat_d: Json) -> None:
    for col in columns:
        cinfo = _catalog_col(cat_d, col["name"])
        if not cinfo:
            continue
        if not col.get("type") and cinfo.get("type"):
            col["type"] = cinfo["type"]
        if cinfo.get("comment"):
            col["comment"] = cinfo["comment"]


def _read_flag(entry: Json, name: str) -> bool | None:
    if name.lower() in entry["scanned"]:
        return True
    if entry["has_scan"] and not entry["scan_columns_truncated"]:
        return False
    return None


def _read_order(col: Json) -> int:
    return {True: 0, None: 1}.get(col["read"], 2)


def _source_column(entry: Json, cat: Json | None, name: str) -> Json:
    col = entry["columns"].get(name, {"name": name, "type": None})
    item: Json = {"name": name, "type": col["type"]}
    cat_col = _catalog_col(cat, name)
    if cat_col is not None:
        item["type"] = cat_col.get("type") or item["type"]
        if cat_col.get("comment"):
            item["comment"] = cat_col["comment"]
    item["read"] = _read_flag(entry, name)
    return item


def _matched_read_sites(table: str, read_sites_map: dict[str, list[str]]) -> list[str]:
    # Locais no código onde a tabela foi lida (spark.table / spark.read.table / spark.sql).
    out: list[str] = []
    t_low = table.lower()
    for rtbl, rsites in read_sites_map.items():
        r_low = rtbl.lower()
        if r_low == t_low or t_low.endswith(f".{r_low}"):
            for rs in rsites:
                _add(out, rs)
    return out


def _types_from(entry: Json, cat: Json | None) -> str:
    if cat:
        return "catalog"
    if entry["schema_truncated"]:
        return "read_schema_partial"
    return "read_schema" if entry["has_scan"] else "none"


def _finalize_source(entry: Json, cat: Json | None, read_sites_map: dict[str, list[str]]) -> Json:
    names = list(entry["columns"])
    if cat:
        seen_lower = {c.lower() for c in names}
        names += [c for c in cat if c.lower() not in seen_lower]
    cols = sorted((_source_column(entry, cat, n) for n in names), key=_read_order)
    return {
        "table": entry["table"],
        "format": entry["format"],
        "plans": entry["plans"],
        "call_sites": _matched_read_sites(entry["table"], read_sites_map) or entry["action_sites"],
        "types_from": _types_from(entry, cat),
        "columns_truncated": entry["columns_truncated"] and not bool(cat),
        "scan_columns_truncated": entry["scan_columns_truncated"],
        "columns": cols,
        "scan_filters": entry["filters"],
        "scan_filters_truncated": entry["filters_truncated"],
    }


# --------------------------------------------------------------------------- #
# Avisos e eventos
# --------------------------------------------------------------------------- #


def _status_warnings(result: Json) -> Iterable[str]:
    if result.get("status") != "ok":
        err = result.get("error") or {}
        yield (
            f"Captura incompleta (status={result.get('status')}): "
            f"{err.get('type')}: {err.get('message')}. Os planos abaixo são parciais."
        )


def _collapsed_warnings(collapsed: list[str]) -> Iterable[str]:
    if collapsed:
        yield (
            f"Planos {', '.join(collapsed)}: o plano físico colapsou para tabela vazia, então "
            "as colunas realmente lidas pelo scan estão indisponíveis neles (provavelmente tabelas vazias "
            "no ambiente de captura)."
        )


def _truncation_warnings(sources: list[Json]) -> Iterable[str]:
    partial = [s["table"] for s in sources if s["types_from"] == "read_schema_partial"]
    if partial:
        yield (
            "Tipos parciais (o Spark truncou o ReadSchema em ~100 caracteres) em: "
            + ", ".join(partial)
            + ". Use --catalog ou recapture com a versão atual do CLI (explain formatted)."
        )
    cut_scan = [s["table"] for s in sources if s["scan_columns_truncated"]]
    if cut_scan:
        yield (
            "Lista de colunas LIDAS no scan foi truncada pelo Spark ('... N more fields') em: "
            + ", ".join(cut_scan)
            + ". Colunas fora das primeiras ~25 aparecem com leitura indeterminada "
            "(mesmo com --catalog, o Unity Catalog só conhece o schema total da tabela). "
            "Recapture com a versão atual do CLI (explain formatted) para obter todas as colunas lidas sem truncamento."
        )
    cut = [s["table"] for s in sources if s["columns_truncated"]]
    if cut:
        yield (
            "Lista completa de colunas da tabela truncada pelo Spark ('... N more fields') em: "
            + ", ".join(cut)
            + ". Faltam colunas não lidas; use --catalog para listar todas as colunas da tabela."
        )


def _parser_warnings(builder: _SourcesBuilder) -> Iterable[str]:
    for pid, table in builder.merge_without_source:
        yield (
            f"{pid}: MERGE em {table or '(destino não nomeado)'}; o plano do MERGE não "
            "expõe as tabelas de origem (veja o DataFrame que alimenta o merge)."
        )
    if builder.unparsed:
        yield "Nós de leitura não interpretados: " + ", ".join(f"{k} x{v}" for k, v in builder.unparsed.items())


def _capture_warnings(result: Json) -> Iterable[str]:
    for w in result.get("capture_warnings", result.get("warnings", [])):
        yield str(w.get("message", w) if isinstance(w, dict) else w)


def _build_warnings(result: Json, builder: _SourcesBuilder, sources: list[Json]) -> list[str]:
    warnings: list[str] = []
    for gen in (
        _status_warnings(result),
        _collapsed_warnings(builder.collapsed),
        _truncation_warnings(sources),
        _parser_warnings(builder),
        _capture_warnings(result),
    ):
        for w in gen:
            _add(warnings, w)
    return warnings


def _normalize_event(e: Json) -> Json:
    if "detail" in e and len(e) <= 4:
        return e
    return {
        "kind": e.get("kind"),
        "detail": e.get("statement_kind") or e.get("name"),
        "query": (e.get("query") or e.get("args") or "")[:160],
    }


def _is_compact_format(result: Json) -> bool:
    return isinstance(result, dict) and str(result.get("format", "")).startswith("spark-lineage-compact/")


def _enrich_compact_with_catalog(sources: list[Json], destinations: list[Json], catalog: Catalog) -> tuple[list[Json], list[Json]]:
    cat_ci = {k.replace("`", "").strip().lower(): v for k, v in catalog.items() if isinstance(v, dict)}

    def lookup(table: str | None) -> Json | None:
        if not table:
            return None
        return catalog.get(table) or cat_ci.get(table.replace("`", "").strip().lower())

    out_sources: list[Json] = []
    for s in sources:
        sc = dict(s)
        cat_s = lookup(s.get("table"))
        cols = [dict(c) for c in s.get("columns", [])]
        if cat_s:
            _enrich_columns(cols, cat_s)
            seen_low = {c["name"].lower() for c in cols}
            for cn, cinfo in cat_s.items():
                if cn.lower() not in seen_low:
                    item: Json = {"name": cn, "type": cinfo.get("type"), "read": None}
                    if cinfo.get("comment"):
                        item["comment"] = cinfo["comment"]
                    cols.append(item)
            sc["types_from"] = "catalog"
            sc["columns_truncated"] = False
        sc["columns"] = cols
        out_sources.append(sc)

    out_dests: list[Json] = []
    for d in destinations:
        dc = dict(d)
        cat_d = lookup(d.get("table"))
        if cat_d and d.get("columns"):
            cols = [dict(c) for c in d["columns"]]
            _enrich_columns(cols, cat_d)
            dc["columns"] = cols
        out_dests.append(dc)
    return out_sources, out_dests


def build_sources(result: Json, catalog: Catalog | None = None) -> Json:
    """Consolida fontes, destinos, views, fluxos, ramos e avisos a partir do resultado da captura.

    Aceita tanto o ``plan.json`` bruto quanto o documento compactado ``spark-lineage-compact/v1``.

    Args:
        result: Documento gerado pelo ``capture`` (bruto ou compactado).
        catalog: Schemas do Unity Catalog; se ``None``, usa ``result['catalog']`` já salvo no arquivo.

    Returns:
        Documento consolidado com ``sources``, ``views``, ``destinations``, ``flows``, ``events`` e ``warnings``.
    """
    if _is_compact_format(result):
        sources = list(result.get("sources") or [])
        dests = list(result.get("destinations") or [])
        if catalog is not None:
            sources, dests = _enrich_compact_with_catalog(sources, dests, catalog)
        return {
            "entrypoint": result.get("entrypoint"),
            "capture_status": result.get("capture_status", result.get("status")),
            "plans_analyzed": result.get("plans_analyzed", 0),
            "sources": sources,
            "views": list(result.get("views") or []),
            "destinations": dests,
            "flows": list(result.get("flows") or []),
            "events": [_normalize_event(e) for e in result.get("events", [])],
            "warnings": list(result.get("warnings") or []),
        }

    if catalog is None:
        catalog = result.get("catalog")
    read_sites = result.get("read_sites") or {}
    builder = _SourcesBuilder(catalog, read_sites=read_sites)
    for plan in result.get("plans", []):
        builder.add_plan(plan)
    builder.enrich_destinations()
    out_sources = builder.source_list(read_sites)

    return {
        "entrypoint": result.get("entrypoint"),
        "capture_status": result.get("status", result.get("capture_status")),
        "plans_analyzed": len(result.get("plans", [])),
        "sources": out_sources,
        "views": [{"name": n, "plans": p} for n, p in builder.views.items()],
        "destinations": list(builder.dests.values()),
        "flows": builder.flows,
        "events": [_normalize_event(e) for e in result.get("events", [])],
        "warnings": _build_warnings(result, builder, out_sources),
    }


# --------------------------------------------------------------------------- #
# Consulta por tag
# --------------------------------------------------------------------------- #


def _match_table(candidate: str | None, table_filter: str) -> bool:
    if not candidate:
        return False
    tf = table_filter.lower().replace("`", "").strip()
    c = candidate.lower().replace("`", "").strip()
    return c == tf or c.endswith(f".{tf}")


def _filter_by(items: list[Json], key: str, table: str) -> list[Json]:
    return [x for x in items if _match_table(x.get(key), table)]


def _filter_flows(flows: list[Json], table: str) -> list[Json]:
    return [
        f
        for f in flows
        if any(_match_table(r, table) for r in f.get("reads", []))
        or any(_match_table(w, table) for w in f.get("writes", []))
    ]


def _tag_all(_result: Json, doc: Json, table: str | None) -> Json:
    data = dict(doc)
    if table:
        data["sources"] = _filter_by(data["sources"], "table", table)
        data["destinations"] = _filter_by(data["destinations"], "table", table)
        data["flows"] = _filter_flows(data["flows"], table)
    return data


def _tag_branches(result: Json, _doc: Json, _table: str | None) -> list[Json]:
    return result.get("branches") or []


def _tag_catalog(result: Json, _doc: Json, table: str | None) -> Catalog:
    cat = result.get("catalog") or {}
    if table:
        return {k: v for k, v in cat.items() if _match_table(k, table)}
    return cat


def _tag_plans(result: Json, doc: Json, table: str | None) -> list[Json]:
    plans = result.get("plans") or []
    if not table:
        return plans
    matched = _filter_by(doc.get("sources", []), "table", table) + _filter_by(
        doc.get("destinations", []), "table", table
    )
    pids = {p for item in matched for p in item.get("plans", [])}
    return [p for p in plans if p.get("id") in pids]


def _tag_queries(result: Json, _doc: Json, table: str | None) -> list[Json]:
    queries = list(result.get("queries") or []) if "queries" in result else extract_plan_queries(result.get("plans", []))
    if not table:
        return queries
    tf = table.lower().replace("`", "").strip()
    return [
        q
        for q in queries
        if _match_table(q.get("target"), table) or tf in (q.get("query") or "").lower()
    ]


def _tag_column_lineage(result: Json, _doc: Json, table: str | None) -> dict[str, Any]:
    lineage = result.get("column_lineage") or {}
    if not table:
        return lineage
    return {k: v for k, v in lineage.items() if _match_table(k, table)}


_TAG_HANDLERS: dict[str, Callable[[Json, Json, str | None], Any]] = {
    "all": _tag_all,
    "branches": _tag_branches,
    "queries": _tag_queries,
    "column_lineage": _tag_column_lineage,
    "catalog": _tag_catalog,
    "plans": _tag_plans,
}

_ITEM_FILTERS: dict[str, Callable[[list[Json], str], list[Json]]] = {
    "sources": lambda items, t: _filter_by(items, "table", t),
    "destinations": lambda items, t: _filter_by(items, "table", t),
    "flows": _filter_flows,
    "views": lambda items, t: _filter_by(items, "name", t),
}


def extract_tag(result: Json, doc: Json, tag: str = "all", table: str | None = None) -> Any:
    """Extrai apenas a seção (``tag``) solicitada do documento consolidado / plan.json.

    Args:
        result: Documento bruto do plan.json.
        doc: Documento consolidado por ``build_sources``.
        tag: Seção desejada (ver ``VALID_TAGS``).
        table: Filtro opcional por nome (ou sufixo) de tabela.

    Returns:
        O conteúdo da seção, filtrado por tabela quando aplicável.
    """
    handler = _TAG_HANDLERS.get(tag)
    if handler is not None:
        return handler(result, doc, table)
    items = doc.get(tag, [])
    item_filter = _ITEM_FILTERS.get(tag)
    if not table or item_filter is None:
        return items
    return item_filter(items, table)


# --------------------------------------------------------------------------- #
# Markdown
# --------------------------------------------------------------------------- #


def _fmt_col(c: Json) -> str:
    t = f":{c['type']}" if c.get("type") else ""
    cm = f" ({c['comment']})" if c.get("comment") else ""
    return f"{c['name']}{t}{cm}"


def _fmt_cols(cols: list[Json]) -> str:
    return ", ".join(map(_fmt_col, cols))


def _md_source_columns(cols: list[Json]) -> list[str]:
    read = [c for c in cols if c["read"] is True]
    if not read:
        return ["- colunas (leitura NÃO confirmada pelo plano físico): " + _fmt_cols(cols)]
    unknown = [c for c in cols if c["read"] is None]
    unused = [c for c in cols if c["read"] is False]
    lines = ["- colunas lidas (confirmadas pelo scan): " + _fmt_cols(read)]
    if unknown:
        lines.append("- colunas com leitura indeterminada (scan truncado): " + _fmt_cols(unknown))
    if unused:
        lines.append("- demais colunas da tabela (não lidas): " + _fmt_cols(unused))
    return lines


def _md_source(s: Json) -> list[str]:
    sites = f" em {', '.join(s['call_sites'])}" if s.get("call_sites") else ""
    lines = [
        f"### `{s['table']}` [{s['format']}]{sites} - planos {', '.join(s['plans'])}",
        *_md_source_columns(s["columns"]),
    ]
    if s["scan_filters"]:
        cut = " (lista truncada pelo Spark)" if s["scan_filters_truncated"] else ""
        lines.append("- filtros no scan: " + "; ".join(s["scan_filters"][:6]) + cut)
    return lines


def _md_sources(doc: Json) -> list[str]:
    lines = [f"## Fontes lidas ({len(doc['sources'])})"]
    for s in doc["sources"]:
        lines += _md_source(s)
    return lines


def _md_views(doc: Json) -> list[str]:
    return ["## Views referenciadas"] + [f"- `{v['name']}` (planos {', '.join(v['plans'])})" for v in doc["views"]]


def _md_destinations(doc: Json) -> list[str]:
    lines = [f"## Destinos ({len(doc['destinations'])})"]
    for d in doc["destinations"]:
        cols = d.get("columns") or []
        ncols = f"{len(cols)} colunas" if cols else "colunas desconhecidas"
        name = f"`{d['table']}`" if d["table"] else f"(SQL) {d.get('query', '')[:60]}"
        lines.append(f"- {name}: {', '.join(d['actions'])} em {', '.join(d['call_sites']) or '?'} - {ncols}")
        if cols:
            lines.append("  - colunas: " + _fmt_cols(cols))
    return lines


def _md_flows(doc: Json) -> list[str]:
    return ["## Fluxos (plano: lê -> escreve)"] + [
        f"- {f['plan']}: {', '.join(f['reads']) or '(sem leituras)'} -> {', '.join(f['writes']) or '(sem escrita)'}"
        for f in doc["flows"]
    ]


def _md_branch(b: Json) -> str:
    vals = ", ".join(f"{v} ({'padrão' if v == b['natural_value'] else 'forçado'})" for v in b["explored_values"])
    return f"- `{b['site']}`: `if {b['condition']}` -> {vals}"


def _md_branches(doc: Json) -> list[str]:
    branches = doc.get("branches", [])
    return [f"## Ramos de decisão explorados ({len(branches)})"] + [_md_branch(b) for b in branches]


def _md_queries(doc: Json) -> list[str]:
    queries = doc.get("queries") or []
    lines = [f"## Queries SQL ({len(queries)})"]
    for q in queries:
        tgt = f" -> `{q['target']}`" if q.get("target") else ""
        site = f" @ `{q['call_site']}`" if q.get("call_site") else ""
        single_line = " ".join((q.get("query") or "").split())
        lines.append(f"- `{q.get('action', '')}`{tgt}{site}: `{single_line[:240]}`")
    return lines


def _md_events(doc: Json) -> list[str]:
    return [
        "## Não executado durante a captura",
        *[f"- {e.get('kind', '')} {e.get('detail', '')}: {e.get('query', '')}" for e in doc["events"]],
    ]


def _md_warnings(doc: Json) -> list[str]:
    return ["## Avisos"] + [f"- {w}" for w in doc["warnings"]]


# (nome, tags que exibem a seção, exige conteúdo quando tag == "all", renderizador)
_MD_SECTIONS: tuple[tuple[str, tuple[str, ...], bool, Callable[[Json], list[str]]], ...] = (
    ("sources", ("all", "sources"), False, _md_sources),
    ("views", ("all", "views"), True, _md_views),
    ("destinations", ("all", "destinations"), False, _md_destinations),
    ("flows", ("all", "flows"), False, _md_flows),
    ("branches", ("branches",), False, _md_branches),
    ("queries", ("queries",), False, _md_queries),
    ("events", ("all", "events"), True, _md_events),
    ("warnings", ("all", "warnings"), True, _md_warnings),
)


def render_markdown(doc: Json, tag: str = "all") -> str:
    """Gera um resumo compacto em Markdown (todas as seções ou apenas a ``tag`` escolhida).

    Args:
        doc: Documento consolidado por ``build_sources``.
        tag: Seção a renderizar (``"all"`` para todas).

    Returns:
        O texto Markdown, terminado em quebra de linha.
    """
    lines: list[str] = []
    if tag == "all":
        lines += [
            f"# Fontes e destinos de `{doc['entrypoint']}`",
            f"Planos analisados: {doc['plans_analyzed']} | captura: {doc['capture_status']}",
            "",
        ]
    for name, tags, needs_content, render in _MD_SECTIONS:
        if tag not in tags or (needs_content and not doc[name] and tag != name):
            continue
        if lines and name != "sources":
            lines.append("")
        lines += render(doc)
    return "\n".join(lines) + "\n"
