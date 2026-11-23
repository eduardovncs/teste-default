"""Parsing PURO (sem Spark, sem rede) do texto de planos de execução.

Recebe strings (as seções 'analyzed' / 'physical' geradas pelo ``capture_plans``) e
devolve estruturas Python. Não tem estado nem efeitos colaterais, então serve para o CLI,
para um servidor MCP ou para um orquestrador futuro, e é testável com texto de exemplo.

Formatos cobertos (vistos em planos reais do Databricks/Photon)::

    analyzed:  "Relation catalogo.schema.tabela[col#1,col2#2L,...] parquet"
               "View (`vw_nome`, [col#1, ...])"
               "MergeIntoCommandEdge SubqueryAlias s, SubqueryAlias t, `c`.`s`.`t`, Delta[...], ..."
    physical:  "PhotonScan parquet catalogo.schema.tabela[col#1,...] DataFilters: [...], ...
                ReadSchema: struct<col:string,vl:decimal(18,2)>"   (ou FileScan, no Spark aberto)

Limitações conhecidas:
    - Só reconhece o nó "Relation". Outras classes (LogicalRelation, DataSourceV2Relation,
      LocalRelation...) são apenas contadas por ``other_relation_classes``.
    - O plano físico pode colapsar para "LocalTableScan <empty>" (tabelas vazias no ambiente
      de captura): nesse caso não há ReadSchema e os tipos ficam indisponíveis.
    - O plano do MERGE não expõe a origem do merge, só o destino e as colunas atribuídas.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

_ID_RE = re.compile(r"#\d+L?")
_REL_RE = re.compile(r"\bRelation\s+(?:(?P<table>[^\s\[]+)\s*)?\[(?P<cols>[^\]]*)\]\s*(?P<fmt>[\w.\-]+)?")
_REL_V2_RE = re.compile(r"\bRelationV2\[(?P<cols>[^\]]*)\](?:\s+(?P<table>[^\s,\[]+))?")
_OTHER_REL_RE = re.compile(r"\b(\w+Relation\w*)\b")
_VIEW_RE = re.compile(r"\bView \((?P<name>[^\[]+?),\s*\[")
_SCAN_RE = re.compile(r"(?P<op>\b\w*Scan\w*)\s+(?P<fmt>[\w.\-]+)\s*(?:(?P<table>[^\s\[]+)\s*)?\[(?P<cols>[^\]]*)\]")
_MERGE_CMD_RE = re.compile(r"MergeIntoCommand\w*\s+(?P<before>.*?),\s*Delta\[")
_ASSIGN_RE = re.compile(r"`([^`]+)`\s*=\s")
_FILTER_KEYS = ("DataFilters", "PartitionFilters", "PushedFilters", "RequiredDataFilters")
_FMT_SCAN_HDR_RE = re.compile(
    r"^\(\d+\)\s+(?P<op>\w*Scan\w*)\s+(?P<arg1>[^\s\[]+)(?:\s+(?P<arg2>[^\s\[]+))?(?:\s*\[[^\]]+\])?\s*$"
)
_FMT_NODE_HDR_RE = re.compile(r"^\(\d+\)\s+")
_FMT_OUTPUT_RE = re.compile(r"^Output(?:\s*\[\d+\])?:\s*\[(?P<cols>.*)\]\s*$")
_NEXT_KEY_RE = re.compile(r", [A-Z][A-Za-z]+: ")
_MORE_FIELDS_RE = re.compile(r"^\.\.\.\s*\d+ more fields?$")
_IGNORED_REL_CLASSES = frozenset({"CTERelationDef", "CTERelationRef", "RelationV2", "InMemoryRelation"})
_IGNORED_SCAN_FMTS = frozenset({"OneRowRelation", "ExistingRDD"})
_INSERT_V2_RE = re.compile(
    r"\b(?:AppendData|OverwriteByExpression|OverwritePartitionsDynamic)\s+RelationV2\[(?P<cols>[^\]]*)\]"
)
_TRUNCATION_MARK = "..."

Scan = dict[str, Any]


# --------------------------------------------------------------------------- #
# Primitivas
# --------------------------------------------------------------------------- #


def strip_ids(text: str) -> str:
    """Remove os IDs de expressão do Spark: 'cbco_orig#14731L' -> 'cbco_orig'.

    Args:
        text: Trecho de plano com referências ``nome#id``.

    Returns:
        O texto sem os sufixos ``#id``.
    """
    return _ID_RE.sub("", text)


def normalize_table(name: str) -> str:
    """Normaliza nomes de tabela: '`dev`.`schema`.`tabela`' -> 'dev.schema.tabela'.

    Args:
        name: Nome da tabela, possivelmente com crases.

    Returns:
        O nome sem crases e sem espaços nas pontas.
    """
    return name.replace("`", "").strip()


def _optional_table(raw: str | None) -> str | None:
    return normalize_table(raw) if raw else None


def split_top_level(text: str, angle: bool = False) -> list[str]:
    """Divide por vírgula respeitando (), [], {} (e <> se ``angle=True``, para tipos Spark) e literais ``'...'``."""
    opens = "([{<" if angle else "([{"
    closes = ")]}>" if angle else ")]}"
    parts: list[str] = []
    depth = 0
    cur: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch in ("'", "`"):
            quote = ch
            cur.append(ch)
            i += 1
            while i < n:
                c = text[i]
                if c == "\\" and quote != "`" and i + 1 < n:
                    cur.append(text[i : i + 2])
                    i += 2
                    continue
                cur.append(c)
                i += 1
                if c == quote:
                    if i < n and text[i] == quote:
                        cur.append(text[i])
                        i += 1
                        continue
                    break
            continue
        if ch in opens:
            depth += 1
        elif ch in closes:
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
        i += 1
    tail = "".join(cur).strip()
    if tail:
        parts.append(tail)
    return parts


def _is_ignored_scan(op: str, fmt: str | None) -> bool:
    return "LocalTableScan" in op or "InMemoryTableScan" in op or (fmt or "") in _IGNORED_SCAN_FMTS


def _parse_attr(tok: str) -> dict[str, Any]:
    name, sep, rest = tok.rpartition("#")
    digits = rest.rstrip("L")
    if sep and name and digits.isdigit():
        return {"name": name.strip().strip("`"), "id": int(digits)}
    return {"name": tok.strip().strip("`"), "id": None}


def _parse_attrs(cols: str) -> tuple[list[dict[str, Any]], bool]:
    # 'a#1, b#2L' -> ([{'name': 'a', 'id': 1}, {'name': 'b', 'id': 2}], truncado?).
    out: list[dict[str, Any]] = []
    truncated = False
    for tok in split_top_level(cols):
        if _MORE_FIELDS_RE.match(tok.strip()):
            truncated = True
        else:
            out.append(_parse_attr(tok))
    return out, truncated


# --------------------------------------------------------------------------- #
# Plano analisado
# --------------------------------------------------------------------------- #


def parse_relations(analyzed: str) -> list[dict[str, Any]]:
    """Extrai os nós 'Relation' e 'RelationV2' (leitura) do plano analisado: tabela, formato e TODAS as colunas.

    Args:
        analyzed: Seção 'analyzed' do explain.

    Returns:
        Uma entrada por Relation/RelationV2, com ``table``, ``format``, ``columns`` e ``columns_truncated``.
    """
    out = []
    for line in analyzed.splitlines():
        m = _REL_RE.search(line)
        if m:
            cols, truncated = _parse_attrs(m.group("cols"))
            out.append(
                {
                    "table": _optional_table(m.group("table")),
                    "format": m.group("fmt"),
                    "columns": cols,
                    "columns_truncated": truncated,
                }
            )
            continue
        if not _INSERT_V2_RE.search(line):
            m2 = _REL_V2_RE.search(line)
            if m2:
                cols, truncated = _parse_attrs(m2.group("cols"))
                out.append(
                    {
                        "table": _optional_table(m2.group("table")),
                        "format": None,
                        "columns": cols,
                        "columns_truncated": truncated,
                    }
                )
                continue
        m_scan = _SCAN_RE.search(line)
        if m_scan and not _is_ignored_scan(m_scan.group("op"), m_scan.group("fmt")):
            cols, truncated = _parse_attrs(m_scan.group("cols"))
            out.append(
                {
                    "table": _optional_table(m_scan.group("table")),
                    "format": m_scan.group("fmt"),
                    "columns": cols,
                    "columns_truncated": truncated,
                }
            )
    return out


def other_relation_classes(analyzed: str) -> Counter[str]:
    """Conta classes '*Relation' que o parser NÃO interpreta (LocalRelation, LogicalRelation...).

    Args:
        analyzed: Seção 'analyzed' do explain.

    Returns:
        Contagem por nome de classe.
    """
    return Counter(
        cls
        for line in analyzed.splitlines()
        for cls in _OTHER_REL_RE.findall(line)
        if cls != "Relation" and cls not in _IGNORED_REL_CLASSES
    )


def parse_views(analyzed: str) -> list[str]:
    """Lista os nomes das views referenciadas (o plano analisado já as expande em Relations).

    Args:
        analyzed: Seção 'analyzed' do explain.

    Returns:
        Nomes normalizados das views, na ordem em que aparecem.
    """
    return [normalize_table(m.group("name")) for m in filter(None, map(_VIEW_RE.search, analyzed.splitlines()))]


def parse_merge_targets(analyzed: str) -> list[dict[str, Any]]:
    """Extrai destino e colunas atribuídas de comandos MERGE (MergeIntoCommand*).

    Args:
        analyzed: Seção 'analyzed' (ou 'raw') do explain.

    Returns:
        Uma entrada por MERGE, com ``table`` e ``columns``.
    """
    out = []
    for line in analyzed.splitlines():
        m = _MERGE_CMD_RE.search(line)
        if m:
            parts = split_top_level(m.group("before"))
            last = parts[-1].strip() if parts else ""
            raw_table = last if (last and " " not in last and "[" not in last) else None
            cols = list(dict.fromkeys(_ASSIGN_RE.findall(line)))
            out.append({"table": _optional_table(raw_table), "columns": cols})
    return out


def parse_insert_target_columns(analyzed: str) -> list[dict[str, Any]] | None:
    """Extrai as colunas da tabela de destino em planos SQL de INSERT (AppendData/Overwrite* RelationV2[...]).

    Args:
        analyzed: Seção 'analyzed' do explain.

    Returns:
        Lista de dicionários ``{"name": ..., "type": None}`` ou ``None``.
    """
    for line in analyzed.splitlines():
        m = _INSERT_V2_RE.search(line)
        if m:
            cols, _ = _parse_attrs(m.group("cols"))
            if cols:
                return [{"name": c["name"], "type": None} for c in cols]
    return None


# --------------------------------------------------------------------------- #
# Plano físico
# --------------------------------------------------------------------------- #
# O Spark trunca CADA valor de metadado do scan em 100 caracteres (spark.sql.maxMetadataStringLength),
# terminando em "...". Por isso cada valor é delimitado pela PRÓXIMA chave (", Chave: "), e valores
# cortados são sinalizados em vez de interpretados como se estivessem completos.


def _find_key(line: str, key: str) -> int:
    m = re.search(rf"(?<![A-Za-z]){key}: ", line)
    return m.end() if m else -1


def _is_balanced(text: str, angle: bool = False) -> bool:
    opens = "([{<" if angle else "([{"
    closes = ")]}>" if angle else ")]}"
    depth = 0
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
        if ch in opens:
            depth += 1
        elif ch in closes:
            depth -= 1
        i += 1
    return depth == 0


def _metadata_value(line: str, key: str, angle: bool = False) -> tuple[str | None, bool]:
    # Valor bruto de 'Chave: valor' e se foi truncado pelo Spark (termina em '...').
    start = _find_key(line, key)
    if start < 0:
        return None, False
    chunk = line[start:]
    for m in _NEXT_KEY_RE.finditer(line, start):
        candidate = line[start : m.start()]
        if candidate.endswith(_TRUNCATION_MARK) or _is_balanced(candidate, angle):
            chunk = candidate
            break
    return chunk, chunk.endswith(_TRUNCATION_MARK)


def _bracket_items(
    value: str,
    prefix: str,
    suffix: str,
    truncated: bool,
    angle: bool = False,
) -> tuple[list[str], bool]:
    # Itens de 'prefix ... suffix' e se o último (cortado pelo Spark) foi descartado.
    body = value[len(prefix) :]
    if truncated:
        body = body[: -len(_TRUNCATION_MARK)]
    elif body.endswith(suffix):
        body = body[: -len(suffix)]
    items = split_top_level(body, angle=angle)
    dropped = truncated and bool(items)
    return (items[:-1] if dropped else items), dropped


def _split_schema_field(field: str) -> tuple[str, str] | None:
    in_bt = False
    for i, ch in enumerate(field):
        if ch == "`":
            in_bt = not in_bt
        elif ch == ":" and not in_bt:
            return field[:i].strip().strip("`"), field[i + 1 :].strip()
    return None


def parse_read_schema(line: str) -> tuple[dict[str, str], bool]:
    """Interpreta 'ReadSchema: struct<a:int,b:decimal(18,2)>' -> ({'a': 'int', 'b': 'decimal(18,2)'}, truncado?).

    Se truncado, só os campos COMPLETOS são devolvidos (o último, cortado, é descartado).

    Args:
        line: Linha do plano físico (ou do explain formatted) contendo ``ReadSchema:``.

    Returns:
        Mapa coluna -> tipo e um indicador de truncamento.
    """
    value, truncated = _metadata_value(line, "ReadSchema", angle=True)
    if not value or not value.startswith("struct<"):
        return {}, False
    out: dict[str, str] = {}
    fields, _ = _bracket_items(value, "struct<", ">", truncated, angle=True)
    for field in fields:
        if _MORE_FIELDS_RE.match(field):
            truncated = True
            continue
        parsed = _split_schema_field(field)
        if parsed is not None:
            name, typ = parsed
            out[name] = typ
    return out, truncated


def _parse_filters(line: str) -> tuple[list[str], bool]:
    found: list[str] = []
    truncated = False
    for key in _FILTER_KEYS:
        value, trunc = _metadata_value(line, key)
        if not value or not value.startswith("["):
            continue
        items, dropped = _bracket_items(value, "[", "]", trunc)
        truncated |= dropped
        found.extend(_clean_filters(items, found))
    return found, truncated


def _clean_filters(items: list[str], known: list[str]) -> list[str]:
    # Remove IDs dos filtros e descarta vazios, marcadores de truncamento e repetidos.
    out: list[str] = []
    for item in items:
        clean = strip_ids(item)
        if clean and not _MORE_FIELDS_RE.match(clean) and clean not in known and clean not in out:
            out.append(clean)
    return out


def _new_scan(table: str | None, fmt: str) -> Scan:
    return {
        "table": _optional_table(table),
        "format": fmt,
        "columns": [],
        "columns_truncated": False,
        "read_schema": {},
        "read_schema_truncated": False,
        "filters": [],
        "filters_truncated": False,
    }


def _apply_formatted_detail(cur: Scan, line: str) -> None:
    # Preenche colunas, ReadSchema ou filtros do scan atual a partir de uma linha de detalhe.
    m_out = _FMT_OUTPUT_RE.match(line)
    if m_out:
        cur["columns"], cur["columns_truncated"] = _parse_attrs(m_out.group("cols"))
    elif line.startswith("ReadSchema:"):
        cur["read_schema"], cur["read_schema_truncated"] = parse_read_schema(line)
    elif line.startswith(tuple(f"{k}:" for k in _FILTER_KEYS)):
        filts, trunc = _parse_filters(line)
        cur["filters"].extend(f for f in filts if f not in cur["filters"])
        cur["filters_truncated"] |= trunc


def _formatted_header(line: str) -> tuple[bool, Scan | None]:
    # Classifica a linha: (é cabeçalho de nó/seção?, novo scan se for um scan relevante).
    m_hdr = _FMT_SCAN_HDR_RE.match(line)
    if m_hdr:
        op = m_hdr.group("op")
        arg1 = m_hdr.group("arg1")
        arg2 = m_hdr.group("arg2")
        if _is_ignored_scan(op, arg1):
            return True, None
        if op == "BatchScan" and arg2 is None:
            return True, _new_scan(arg1, "v2")
        return True, _new_scan(arg2, arg1)
    is_header = not line or bool(_FMT_NODE_HDR_RE.match(line)) or line.startswith("==")
    return is_header, None


def parse_formatted_scans(formatted: str) -> list[Scan]:
    """Extrai os scans do plano ``explain(mode='formatted')``: sem truncamento de Output, ReadSchema ou filtros.

    Args:
        formatted: Texto do explain em modo formatted.

    Returns:
        Um dicionário por scan (tabela, formato, colunas, ReadSchema e filtros).
    """
    out: list[Scan] = []
    cur: Scan | None = None
    for raw_line in formatted.splitlines():
        line = raw_line.strip()
        is_header, new_scan = _formatted_header(line)
        if is_header:
            if cur is not None:
                out.append(cur)
            cur = new_scan
        elif cur is not None:
            _apply_formatted_detail(cur, line)
    if cur is not None:
        out.append(cur)
    return out


def _parse_physical_scan(line: str) -> Scan | None:
    m = _SCAN_RE.search(line)
    if not m or _is_ignored_scan(m.group("op"), m.group("fmt")):
        return None
    scan = _new_scan(m.group("table"), m.group("fmt"))
    scan["columns"], scan["columns_truncated"] = _parse_attrs(m.group("cols"))
    scan["read_schema"], scan["read_schema_truncated"] = parse_read_schema(line)
    scan["filters"], scan["filters_truncated"] = _parse_filters(line)
    return scan


def parse_scans(physical: str, formatted: str = "") -> list[Scan]:
    """Extrai os scans do plano físico: tabela, colunas LIDAS (após poda), tipos (ReadSchema) e filtros.

    Se ``formatted`` estiver disponível (gerado por ``explain(mode='formatted')``), usa-o por não truncar.

    Args:
        physical: Seção 'physical' do explain estendido.
        formatted: Texto opcional do explain em modo formatted.

    Returns:
        Um dicionário por scan encontrado.
    """
    fmt_scans = parse_formatted_scans(formatted) if formatted else []
    if fmt_scans:
        return fmt_scans
    return [scan for scan in map(_parse_physical_scan, physical.splitlines()) if scan is not None]


def is_collapsed_to_empty(physical: str) -> bool:
    """Indica se o plano físico virou 'LocalTableScan <empty>' (otimizador viu tabelas vazias).

    Args:
        physical: Seção 'physical' do explain.

    Returns:
        ``True`` se o plano colapsou para uma relação vazia.
    """
    return "LocalTableScan <empty>" in physical
