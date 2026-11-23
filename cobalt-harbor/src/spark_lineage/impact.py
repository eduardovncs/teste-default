"""Rastreia o impacto e todas as transformações de uma coluna (tabela + coluna).

O rastreio vai de ponta a ponta através dos planos capturados no plan.json (100% offline).

Como funciona:
    1. Localiza a coluna na ``Relation`` de origem dentro do plano ``analyzed`` e captura seu Expression ID (#1234).
    2. Percorre a árvore do plano de baixo para cima (post-order), propagando o rastreio por:
       Filter, Join, Project (derivações/casts/CASE WHEN), Aggregate, Window, Union (posicional),
       CTERelationDef/CTERelationRef, Sort, Deduplicate.
    3. Identifica todas as colunas de saída afetadas e seus destinos de escrita.
    4. Encadeia automaticamente através de tabelas intermediárias (staging), repetindo o rastreio
       nos planos que leem as colunas gravadas na etapa anterior.
"""

from __future__ import annotations

import re
from collections import deque
from collections.abc import Callable, Iterator
from typing import Any

from .plan_parser import parse_merge_targets, parse_relations, split_top_level, strip_ids
from .sources import build_sources, in_place_dml_targets, is_write_action, plan_analyzed, site_label

_TREE_BRANCH_RE = re.compile(r"^([\s:|]*?)([+:]-\s+)(.*)$")
_ATTR_ID_RE = re.compile(r"(`[^`]+`|[\w]+)#(\d+)L?")
_ALIAS_RE = re.compile(r"^(?P<rhs>.+?)\s+AS\s+(?:`(?P<alias_bt>[^`]+)`|(?P<alias>[^#\s`,\]]+))#(?P<id>\d+)L?$", re.S)
_CTE_DEF_RE = re.compile(r"^CTERelationDef\s+(\d+)")
_CTE_REF_RE = re.compile(r"^CTERelationRef\s+(\d+),\s*\w+,\s*\[(?P<cols>.*)\]")
_JOIN_RE = re.compile(r"^Join\s+(?P<jtype>[^,\s]+(?:\([^)]*\))?)(?:,\s*(?P<cond>.+))?$")
_CODEGEN_PREFIX_RE = re.compile(r"^\*\(\d+\)\s+")
_SUBQUERY_REF_RE = re.compile(r"\b(?:scalar-subquery|list|exists)#(\d+)")

Json = dict[str, Any]
Attr = tuple[str, int | None]


def _match_table(candidate: str | None, table_query: str) -> bool:
    if not candidate:
        return False
    q = table_query.replace("`", "").strip().lower()
    c = candidate.replace("`", "").strip().lower()
    return c == q or c.endswith(f".{q}")


def _extract_bracket_groups(text: str) -> list[str]:
    # Extrai todos os blocos de nível superior delimitados por `[...]` em uma linha de operador,
    # ignorando colchetes dentro de strings `'...'` e identificadores entre crases.
    groups: list[str] = []
    depth = 0
    start = -1
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch in ("'", "`"):
            quote = ch
            i += 1
            while i < n:
                c = text[i]
                if c == "\\" and quote != "`" and i + 1 < n:
                    i += 2
                    continue
                i += 1
                if c == quote:
                    if i < n and text[i] == quote:
                        i += 1
                        continue
                    break
            continue
        if ch == "[":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0 and start >= 0:
                groups.append(text[start + 1 : i])
                start = -1
        i += 1
    return groups


def _extract_ids(text: str) -> list[tuple[str, int]]:
    # Retorna [(nome, id), ...] para todas as referências `col#123` encontradas no texto.
    out: list[tuple[str, int]] = []
    seen: set[int] = set()
    for m in _ATTR_ID_RE.finditer(text):
        cid = int(m.group(2))
        if cid not in seen:
            seen.add(cid)
            out.append((m.group(1).replace("`", ""), cid))
    return out


def _alias_name(m: re.Match[str]) -> str:
    return m.group("alias_bt") or m.group("alias")


def _parse_output_attr(s: str) -> Attr:
    m_alias = _ALIAS_RE.match(s)
    if m_alias:
        return _alias_name(m_alias), int(m_alias.group("id"))
    # Atributo simples: col#123 ou `col`#123L
    name, sep, rest = s.rpartition("#")
    digits = rest.rstrip("L")
    if sep and digits.isdigit():
        return name.replace("`", "").strip(), int(digits)
    return strip_ids(s), None


def _parse_output_attrs(expr_list_str: str) -> list[Attr]:
    # Extrai [(nome_saida, id_saida), ...] de uma lista de expressões/atributos de saída.
    items = (item.strip() for item in split_top_level(expr_list_str))
    return [_parse_output_attr(s) for s in items if s and not s.startswith("...")]


def _aliases(expr_list_str: str) -> Iterator[tuple[str, str, int]]:
    # Itera sobre as expressões `rhs AS alias#id` de uma lista de expressões.
    for expr in split_top_level(expr_list_str):
        m_al = _ALIAS_RE.match(expr.strip())
        if m_al:
            yield m_al.group("rhs").strip(), _alias_name(m_al), int(m_al.group("id"))


# --------------------------------------------------------------------------- #
# Árvore do plano
# --------------------------------------------------------------------------- #


class _PlanNode:
    __slots__ = ("children", "depth", "op", "output_attrs", "text")

    def __init__(self, op: str, text: str, depth: int) -> None:
        self.op = op
        self.text = text
        self.depth = depth
        self.children: list[_PlanNode] = []
        self.output_attrs: list[Attr] = []


def _schema_header_lines(lines: list[str]) -> int:
    # Quantas linhas iniciais pular: a 1ª linha do analyzed costuma ser o schema ("col1: string, ...").
    if len(lines) > 1 and not _TREE_BRANCH_RE.match(lines[0]):
        first_tok = lines[0].strip().split()[0]
        if ":" in first_tok:
            return 1
    return 0


def _node_from_line(raw: str) -> _PlanNode:
    m = _TREE_BRANCH_RE.match(raw)
    if m:
        depth = len(m.group(1)) + len(m.group(2))
        body = m.group(3).strip()
    else:
        depth = 0
        body = raw.strip()
    clean_body = _CODEGEN_PREFIX_RE.sub("", body)
    op = clean_body.split()[0].split("[")[0].rstrip("(") if clean_body else ""
    return _PlanNode(op=op, text=body, depth=depth)


def _parse_plan_tree(analyzed: str) -> _PlanNode | None:
    # Reconstrói a árvore hierárquica de operadores a partir da seção `analyzed` do Spark.
    lines = [ln for ln in analyzed.splitlines() if ln.strip()]
    if not lines:
        return None
    nodes = [_node_from_line(raw) for raw in lines[_schema_header_lines(lines) :]]
    root = nodes[0]
    stack = [root]
    for node in nodes[1:]:
        while len(stack) > 1 and stack[-1].depth >= node.depth:
            stack.pop()
        stack[-1].children.append(node)
        stack.append(node)
    return root


def _post_order(root: _PlanNode) -> list[_PlanNode]:
    # Nós em post-order (filhos da esquerda para a direita antes do pai), sem recursão.
    order: list[_PlanNode] = []
    stack = [root]
    while stack:
        node = stack.pop()
        order.append(node)
        stack.extend(node.children)
    order.reverse()
    return order


def _node_output_attrs(node: _PlanNode) -> list[Attr]:
    # Obtém a lista de atributos de saída de um nó (propagando através de nós transparentes).
    cur: _PlanNode | None = node
    while cur is not None:
        if cur.output_attrs:
            return cur.output_attrs
        cur = cur.children[-1] if cur.children else None
    return []


def _first_child_attrs(node: _PlanNode) -> list[Attr]:
    return _node_output_attrs(node.children[-1]) if node.children else []


def _is_pure_passthrough(rhs: str, alias: str) -> bool:
    # True se rhs for apenas o próprio atributo sem transformação (ex.: col#1 ou VALOR#1 AS valor#2).
    return strip_ids(rhs).replace("`", "").strip().lower() == alias.lower()


def _is_pure_cast(expr: str) -> bool:
    s = strip_ids(expr).strip()
    if not s.lower().startswith("cast(") or not s.endswith(")"):
        return False
    depth = 0
    for i, ch in enumerate(s[4:], start=4):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i == len(s) - 1
    return False


def _transform(kind: str, inputs: list[str], output: str | None, expression: str) -> Json:
    return {"kind": kind, "input_columns": inputs, "output_column": output, "expression": expression}


def _project_kind(rhs: str, alias: str, used: list[str]) -> str:
    if _is_pure_passthrough(rhs, used[0]) and alias.lower() != used[0].lower():
        return "rename"
    if _is_pure_cast(rhs) and len(used) == 1:
        return "cast"
    return "derive"


# --------------------------------------------------------------------------- #
# Rastreio em um plano
# --------------------------------------------------------------------------- #


class _ColumnTracer:
    # Propaga o rastreio de uma coluna pelos operadores de um plano analisado.

    def __init__(self, source_table: str, source_column: str) -> None:
        self.source_table = source_table
        self.target_col = source_column.strip().lower()
        self.tracked: dict[int, Json] = {}
        self.cte_outputs: dict[int, list[Attr]] = {}
        self.filters: list[Json] = []
        self.joins: list[Json] = []
        self.transformations: list[Json] = []
        self._seen_steps: set[tuple] = set()
        self._handlers: dict[str, Callable[[_PlanNode], None]] = {
            "Relation": self._relation,
            "RelationV2": self._relation,
            "CTERelationDef": self._cte_def,
            "CTERelationRef": self._cte_ref,
            "Union": self._union,
            "Filter": self._filter,
            "Join": self._join,
            "Project": self._project,
            "Aggregate": self._aggregate,
            "Window": self._window,
            "Generate": self._generate,
            "Expand": self._expand,
            "Except": self._set_op,
            "Intersect": self._set_op,
            "Deduplicate": self._dedup_or_sort,
            "Sort": self._dedup_or_sort,
            "AppendData": self._insert_v2,
            "OverwriteByExpression": self._insert_v2,
            "OverwritePartitionsDynamic": self._insert_v2,
        }

    # ---- utilitários ---- #
    def _add_step(self, bucket: list[Json], sig: tuple, item: Json) -> None:
        if sig not in self._seen_steps:
            self._seen_steps.add(sig)
            bucket.append(item)

    def _used(self, text: str) -> list[str]:
        out: list[str] = []
        for name, cid in _extract_ids(text):
            if cid in self.tracked:
                lbl = self.tracked[cid]["name"] if name in ("subquery", "list", "exists") else name
                out.append(lbl)
        return out

    def _propagate_subqueries(self, node: _PlanNode) -> None:
        if len(node.children) < 2:
            return
        sq_ids = [int(x) for x in _SUBQUERY_REF_RE.findall(node.text)]
        for sq_id, sub_ch in zip(sq_ids, node.children[:-1], strict=False):
            for name, cid in _node_output_attrs(sub_ch):
                if cid in self.tracked:
                    self._track(sq_id, name, direct=False, via=self.tracked[cid]["name"])
                    break

    def _track(self, cid: int | None, name: str, direct: bool, via: str | None) -> None:
        if cid is not None and cid not in self.tracked:
            self.tracked[cid] = {"name": name, "direct": direct, "via": via}

    def run(self, root: _PlanNode) -> None:
        for node in _post_order(root):
            handler = self._handlers.get(node.op)
            if handler is None and node.op.endswith("Scan") and "LocalTableScan" not in node.op and "InMemory" not in node.op:
                handler = self._relation
            (handler or self._passthrough)(node)

    # ---- operadores ---- #
    def _relation(self, node: _PlanNode) -> None:
        rels = parse_relations(node.text)
        if not rels:
            return
        rel = rels[0]
        node.output_attrs = [(c["name"], c["id"]) for c in rel["columns"]]
        if not (_match_table(rel["table"], self.source_table) or _match_table(self.source_table, rel["table"])):
            return
        for c in rel["columns"]:
            if c["name"].lower() == self.target_col and c["id"] is not None:
                self.tracked[c["id"]] = {"name": c["name"], "direct": True, "via": None}

    def _cte_def(self, node: _PlanNode) -> None:
        m_cte = _CTE_DEF_RE.match(node.text)
        if m_cte and node.children:
            cid = int(m_cte.group(1))
            self.cte_outputs[cid] = _node_output_attrs(node.children[0])
            node.output_attrs = self.cte_outputs[cid]

    def _cte_ref(self, node: _PlanNode) -> None:
        m_ref = _CTE_REF_RE.match(node.text)
        if not m_ref:
            return
        ref_attrs = _parse_output_attrs(m_ref.group("cols"))
        node.output_attrs = ref_attrs
        def_attrs = self.cte_outputs.get(int(m_ref.group(1)), [])
        for (r_name, r_id), (_, d_id) in zip(ref_attrs, def_attrs, strict=False):
            if d_id in self.tracked:
                src = self.tracked[d_id]
                self._track(r_id, r_name, direct=src["direct"], via=src["name"])

    def _union(self, node: _PlanNode) -> None:
        if not node.children:
            return
        first_attrs = _node_output_attrs(node.children[0])
        node.output_attrs = first_attrs
        for ch in node.children[1:]:
            for (c_name, c_id), (f_name, f_id) in zip(_node_output_attrs(ch), first_attrs, strict=False):
                if c_id in self.tracked:
                    self._union_column(c_name, c_id, f_name, f_id)

    def _union_column(self, c_name: str, c_id: int, f_name: str, f_id: int | None) -> None:
        same_name = c_name.lower() == f_name.lower()
        self._track(f_id, f_name, direct=self.tracked[c_id]["direct"] and same_name, via=c_name)
        if not same_name:
            item = _transform("union", [c_name], f_name, f"{c_name} -> {f_name} (Union)")
            self._add_step(self.transformations, ("union", c_name, f_name), item)

    def _filter(self, node: _PlanNode) -> None:
        self._propagate_subqueries(node)
        cond = node.text[len("Filter") :].strip()
        used = self._used(cond)
        if used:
            clean_cond = strip_ids(cond)
            self._add_step(self.filters, ("filter", clean_cond), {"expression": clean_cond, "columns_used": used})
        if node.children:
            node.output_attrs = _node_output_attrs(node.children[-1])

    def _join(self, node: _PlanNode) -> None:
        m_j = _JOIN_RE.match(node.text)
        jtype = m_j.group("jtype") if m_j else "Inner"
        cond = (m_j.group("cond") or "").strip() if m_j else ""
        used = self._used(cond)
        if used:
            clean_cond = strip_ids(cond)
            item = {"join_type": jtype, "condition": clean_cond, "columns_used": used}
            self._add_step(self.joins, ("join", jtype, clean_cond), item)
        jlow = jtype.lower()
        if jlow in ("leftanti", "leftsemi"):
            node.output_attrs = _node_output_attrs(node.children[0]) if node.children else []
        elif jlow.startswith("existencejoin("):
            left_attrs = _node_output_attrs(node.children[0]) if node.children else []
            inner = jtype[jtype.find("(") + 1 : jtype.rfind(")")]
            extra_attrs = _parse_output_attrs(inner) if inner else []
            if used:
                for e_name, e_id in extra_attrs:
                    self._track(e_id, e_name, direct=False, via=used[0])
            node.output_attrs = left_attrs + extra_attrs
        else:
            # Saída de um Join concatena os atributos dos dois lados
            node.output_attrs = [attr for ch in node.children for attr in _node_output_attrs(ch)]

    def _project(self, node: _PlanNode) -> None:
        self._propagate_subqueries(node)
        groups = _extract_bracket_groups(node.text)
        if not groups:
            return
        node.output_attrs = _parse_output_attrs(groups[0])
        for rhs, alias, aid in _aliases(groups[0]):
            used = self._used(rhs)
            if not used:
                continue
            pure = _is_pure_passthrough(rhs, alias)
            kind = _project_kind(rhs, alias, used)
            direct = (pure or kind in ("rename", "cast")) and all(
                self.tracked[cid]["direct"] for _, cid in _extract_ids(rhs) if cid in self.tracked
            )
            self._track(aid, alias, direct=direct, via=used[0])
            if not pure:
                clean_rhs = strip_ids(rhs)
                item = _transform(kind, used, alias, f"{clean_rhs} AS {alias}")
                self._add_step(self.transformations, ("project", alias, clean_rhs), item)

    def _aggregate(self, node: _PlanNode) -> None:
        self._propagate_subqueries(node)
        groups = _extract_bracket_groups(node.text)
        if not groups:
            return
        grp_str, agg_str = ("", groups[0]) if len(groups) == 1 else (groups[0], groups[1])
        grp_used = self._used(grp_str)
        if grp_used:
            clean_grp = strip_ids(grp_str)
            item = _transform("group_by", grp_used, None, f"GROUP BY [{clean_grp}]")
            self._add_step(self.transformations, ("group_by", clean_grp), item)
        node.output_attrs = _parse_output_attrs(agg_str)
        for rhs, alias, aid in _aliases(agg_str):
            used = self._used(rhs)
            if not used:
                continue
            self._track(aid, alias, direct=False, via=used[0])
            if not _is_pure_passthrough(rhs, alias):
                clean_rhs = strip_ids(rhs)
                item = _transform("aggregate", used, alias, f"{clean_rhs} AS {alias}")
                self._add_step(self.transformations, ("aggregate", alias, clean_rhs), item)

    def _window(self, node: _PlanNode) -> None:
        groups = _extract_bracket_groups(node.text)
        win_attrs: list[Attr] = []
        if groups:
            spec_used = [name for g in groups[1:3] for name in self._used(g)]
            for rhs, alias, aid in _aliases(groups[0]):
                win_attrs.append((alias, aid))
                all_used = list(dict.fromkeys(self._used(rhs) + spec_used))
                if all_used:
                    self._track(aid, alias, direct=False, via=all_used[0])
                    clean_rhs = strip_ids(rhs)
                    item = _transform("window", all_used, alias, f"{clean_rhs} AS {alias}")
                    self._add_step(self.transformations, ("window", alias, clean_rhs), item)
        node.output_attrs = _first_child_attrs(node) + win_attrs

    def _dedup_or_sort(self, node: _PlanNode) -> None:
        groups = _extract_bracket_groups(node.text)
        used = self._used(groups[0]) if groups else []
        if used:
            op, clean = node.op, strip_ids(groups[0])
            item = _transform(op.lower(), used, None, f"{op} [{clean}]")
            self._add_step(self.transformations, (op.lower(), clean), item)
        if node.children:
            node.output_attrs = _node_output_attrs(node.children[0])

    def _insert_v2(self, node: _PlanNode) -> None:
        if not node.children:
            return
        child_attrs = _node_output_attrs(node.children[0])
        groups = _extract_bracket_groups(node.text)
        if not groups:
            node.output_attrs = child_attrs
            return
        target_attrs = _parse_output_attrs(groups[0])
        mapped_out: list[Attr] = []
        for idx, (t_name, t_id) in enumerate(target_attrs):
            if idx < len(child_attrs):
                c_name, c_id = child_attrs[idx]
                if c_id in self.tracked:
                    use_id = t_id if t_id is not None else c_id
                    direct = self.tracked[c_id]["direct"] and (c_name.lower() == t_name.lower())
                    self.tracked[use_id] = {"name": t_name, "direct": direct, "via": c_name}
                    if c_name.lower() != t_name.lower():
                        item = _transform("rename", [c_name], t_name, f"{c_name} -> {t_name} (INSERT)")
                        self._add_step(self.transformations, ("insert_map", c_name, t_name), item)
                    mapped_out.append((t_name, use_id))
                else:
                    mapped_out.append((t_name, t_id))
        node.output_attrs = mapped_out

    def _generate(self, node: _PlanNode) -> None:
        child_attrs = _first_child_attrs(node)
        groups = _extract_bracket_groups(node.text)
        gen_attrs = _parse_output_attrs(groups[-1]) if groups else []
        node.output_attrs = child_attrs + gen_attrs
        body = node.text[len("Generate") :].strip()
        gen_parts = split_top_level(body)
        gen_expr = gen_parts[0] if gen_parts else ""
        used = self._used(gen_expr)
        if used:
            clean_expr = strip_ids(gen_expr)
            for g_name, g_id in gen_attrs:
                self._track(g_id, g_name, direct=False, via=used[0])
                item = _transform("derive", used, g_name, clean_expr)
                self._add_step(self.transformations, ("generate", g_name, clean_expr), item)

    def _expand(self, node: _PlanNode) -> None:
        groups = _extract_bracket_groups(node.text)
        if len(groups) < 2:
            self._passthrough(node)
            return
        projs_str, out_str = groups[0], groups[1]
        out_attrs = _parse_output_attrs(out_str)
        node.output_attrs = out_attrs
        for proj_str in _extract_bracket_groups(projs_str):
            for idx, expr in enumerate(split_top_level(proj_str)):
                if idx >= len(out_attrs):
                    continue
                used = self._used(expr)
                if not used:
                    continue
                o_name, o_id = out_attrs[idx]
                same_name = _is_pure_passthrough(expr, o_name)
                direct = same_name and all(
                    self.tracked[cid]["direct"] for _, cid in _extract_ids(expr) if cid in self.tracked
                )
                self._track(o_id, o_name, direct=direct, via=used[0])

    def _set_op(self, node: _PlanNode) -> None:
        if node.children:
            node.output_attrs = _node_output_attrs(node.children[0])
        if len(node.children) > 1:
            right_used = [name for name, cid in _node_output_attrs(node.children[1]) if cid in self.tracked]
            if right_used:
                op_low = node.op.lower()
                item = _transform(op_low, right_used, None, f"{node.op} (right: {', '.join(right_used)})")
                self._add_step(self.transformations, (op_low, tuple(right_used)), item)

    @staticmethod
    def _passthrough(node: _PlanNode) -> None:
        if node.children:
            node.output_attrs = _node_output_attrs(node.children[-1])


def _affected_outputs(root: _PlanNode, tracked: dict[int, Json]) -> list[Json]:
    out: list[Json] = []
    seen: set[str] = set()
    for o_name, o_id in _node_output_attrs(root):
        if o_id in tracked and o_name not in seen:
            seen.add(o_name)
            info = tracked[o_id]
            out.append({"name": o_name, "direct": info["direct"], "via": info["via"]})
    return out


def _plan_destinations(plan: Json, analyzed: str) -> list[Json]:
    destinations = [
        {"table": occ["target"], "action": occ.get("action", ""), "call_site": site_label(occ)}
        for occ in plan.get("occurrences", [])
        if occ.get("target") and is_write_action(occ.get("action", ""))
    ]
    destinations += [
        {"table": mt["table"], "action": "delta.merge", "call_site": None}
        for mt in parse_merge_targets(analyzed)
        if mt["table"]
    ]
    return destinations


def _trace_single_plan(plan: Json, source_table: str, source_column: str) -> Json | None:
    # Rastreia `(source_table, source_column)` dentro de um único plano Spark.
    analyzed = plan_analyzed(plan)
    if not analyzed:
        return None
    # Não trata comandos DELETE/UPDATE in-place na própria tabela como leitura downstream
    if any(_match_table(t, source_table) or _match_table(source_table, t) for t in in_place_dml_targets(plan)):
        return None
    root = _parse_plan_tree(analyzed)
    if root is None:
        return None

    tracer = _ColumnTracer(source_table, source_column)
    tracer.run(root)
    if not tracer.tracked:
        return None
    return {
        "plan_id": plan["id"],
        "filters": tracer.filters,
        "joins": tracer.joins,
        "transformations": tracer.transformations,
        "output_columns_affected": _affected_outputs(root, tracer.tracked),
        "destinations": _plan_destinations(plan, analyzed),
    }


# --------------------------------------------------------------------------- #
# Rastreio multi-etapas
# --------------------------------------------------------------------------- #


def _resolve_source(doc: Json, table: str) -> tuple[str, Json | None]:
    # Resolve o nome completo da tabela de origem (permite passar só o sufixo, ex.: tb_transacao_pix).
    for s in doc.get("sources", []):
        if _match_table(s.get("table"), table):
            return s["table"], s
    return table, None


def _resolve_column(source_meta: Json | None, cat_table: Json | None, column: str) -> tuple[Json | None, str]:
    # Metadados da coluna (tipo/comentário) e seu nome com a grafia original.
    wanted = column.strip().lower()
    if source_meta:
        for c in source_meta.get("columns", []):
            if c["name"].lower() == wanted:
                return c, c["name"]
    elif cat_table:
        for cname, cinfo in cat_table.items():
            if cname.lower() == wanted:
                return {"name": cname, **(cinfo or {})}, cname
    return None, column


class _ImpactSearch:
    # Busca em largura (tabela, coluna) através dos planos, encadeando destinos intermediários.

    def __init__(self, plans: list[Json], max_hops: int) -> None:
        self.plans = plans
        self.max_hops = max_hops
        self.visited: set[tuple[str, str]] = set()
        self.hops: list[Json] = []
        self.dest_summary: dict[str, Json] = {}
        self.plans_seen: set[str] = set()
        self.total_ops = 0

    def run(self, table: str, column: str) -> None:
        queue: deque[tuple[int, str, str]] = deque([(1, table, column)])
        while queue:
            hop_num, cur_table, cur_col = queue.popleft()
            key = (cur_table.lower(), cur_col.lower())
            if key in self.visited or hop_num > self.max_hops:
                continue
            self.visited.add(key)
            traces = (_trace_single_plan(plan, cur_table, cur_col) for plan in self.plans)
            hop_plans = [tr for tr in traces if tr is not None]
            for tr in hop_plans:
                queue.extend(self._register(tr, hop_num))
            if hop_plans:
                self.hops.append(
                    {"hop": hop_num, "source_table": cur_table, "source_column": cur_col, "plans": hop_plans}
                )

    def _register(self, tr: Json, hop_num: int) -> list[tuple[int, str, str]]:
        # Contabiliza um plano rastreado e devolve as próximas etapas (tabelas staging).
        self.plans_seen.add(tr["plan_id"])
        self.total_ops += len(tr["filters"]) + len(tr["joins"]) + len(tr["transformations"])
        outputs = tr["output_columns_affected"]
        next_steps: list[tuple[int, str, str]] = []
        for d in tr["destinations"]:
            self._merge_destination(d, outputs)
            next_steps += [
                (hop_num + 1, d["table"], oc["name"])
                for oc in outputs
                if (d["table"].lower(), oc["name"].lower()) not in self.visited
            ]
        return next_steps

    def _merge_destination(self, d: Json, outputs: list[Json]) -> None:
        key = d["table"].replace("`", "").strip().lower()
        entry = self.dest_summary.setdefault(
            key, {"table": d["table"], "actions": [], "call_sites": [], "columns": []}
        )
        if d["action"] not in entry["actions"]:
            entry["actions"].append(d["action"])
        if d["call_site"] and d["call_site"] not in entry["call_sites"]:
            entry["call_sites"].append(d["call_site"])
        known = {x["name"].lower() for x in entry["columns"]}
        for oc in outputs:
            if oc["name"].lower() not in known:
                known.add(oc["name"].lower())
                entry["columns"].append(oc)


def trace_column_impact(result: Json, table: str, column: str, max_hops: int = 5) -> Json:
    """Rastreia uma coluna ``(table, column)`` de ponta a ponta por todos os planos do plan.json.

    Encadeia automaticamente através de tabelas intermediárias (staging).

    Args:
        result: Documento gerado pelo ``capture`` (plan.json).
        table: Nome completo ou sufixo da tabela de origem.
        column: Nome da coluna a rastrear (sem diferenciar maiúsculas).
        max_hops: Máximo de saltos através de tabelas intermediárias.

    Returns:
        Relatório com resumo, destinos afetados e o detalhamento de cada etapa.
    """
    doc = build_sources(result)
    catalog = result.get("catalog") or {}
    resolved_table, source_meta = _resolve_source(doc, table)
    rt_low = resolved_table.replace("`", "").strip().lower()
    cat_tbl = catalog.get(resolved_table) or next(
        (v for k, v in catalog.items() if k.replace("`", "").strip().lower() == rt_low),
        None,
    )
    col_meta, column = _resolve_column(source_meta, cat_tbl, column)

    search = _ImpactSearch(result.get("plans", []), max_hops)
    search.run(resolved_table, column)
    meta = col_meta or {}
    return {
        "entrypoint": result.get("entrypoint"),
        "table": resolved_table,
        "column": column,
        "type": meta.get("type"),
        "comment": meta.get("comment"),
        "read_sites": (source_meta or {}).get("call_sites", []),
        "summary": {
            "found": bool(search.hops),
            "hops": len(search.hops),
            "plans_affected": len(search.plans_seen),
            "operations_count": search.total_ops,
            "destinations_affected": len(search.dest_summary),
        },
        "destinations_affected": list(search.dest_summary.values()),
        "hops": search.hops,
    }


# --------------------------------------------------------------------------- #
# Markdown
# --------------------------------------------------------------------------- #


def _md_plan(p: Json) -> list[str]:
    dests = ", ".join(f"`{d['table']}` ({d['action']})" for d in p["destinations"]) or "(sem escrita)"
    lines = [f"### Plano `{p['plan_id']}` -> {dests}"]
    lines += [f"- **[Filter]** `{f['expression']}`" for f in p["filters"]]
    lines += [f"- **[Join {j['join_type']}]** `{j['condition']}`" for j in p["joins"]]
    lines += [f"- **[{tr['kind']}]** `{tr['expression']}`" for tr in p["transformations"]]
    if p["output_columns_affected"]:
        outs = ", ".join(
            f"`{c['name']}`" + ("" if c["direct"] else " (derivada)") for c in p["output_columns_affected"]
        )
        lines.append(f"- **Colunas de saída afetadas**: {outs}")
    return lines


def _md_destinations_summary(destinations: list[Json]) -> list[str]:
    if not destinations:
        return []
    lines = [f"## Resumo de destinos afetados ({len(destinations)})"]
    for d in destinations:
        cols = ", ".join(f"`{c['name']}`" for c in d["columns"]) or "(nenhuma coluna direta)"
        sites = ", ".join(d["call_sites"]) or "?"
        lines.append(f"- `{d['table']}` ({', '.join(d['actions'])} em {sites}): {cols}")
    return lines


def render_impact_markdown(report: Json) -> str:
    """Gera o resumo em Markdown do impacto de uma coluna.

    Args:
        report: Relatório produzido por ``trace_column_impact``.

    Returns:
        O texto Markdown, terminado em quebra de linha.
    """
    t_str = f":{report['type']}" if report.get("type") else ""
    cm_str = f" ({report['comment']})" if report.get("comment") else ""
    lines = [
        f"# Impacto de `{report['table']}.{report['column']}`{t_str}{cm_str}",
        f"Entrypoint: `{report['entrypoint']}` | Planos afetados: {report['summary']['plans_affected']} "
        f"| Destinos afetados: {report['summary']['destinations_affected']}",
        "",
    ]
    if report.get("read_sites"):
        lines += [f"- leitura no código: {', '.join(report['read_sites'])}", ""]

    if not report["hops"]:
        lines.append("Nenhuma leitura ou transformação encontrada para esta coluna nos planos capturados.")
        return "\n".join(lines) + "\n"

    for h in report["hops"]:
        lines.append(f"## Etapa {h['hop']}: `{h['source_table']}.{h['source_column']}`")
        for p in h["plans"]:
            lines += _md_plan(p)
        lines.append("")
    lines += _md_destinations_summary(report["destinations_affected"])
    return "\n".join(lines) + "\n"
