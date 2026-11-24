#!/usr/bin/env python3
r"""Captura os planos de execução Spark de um job SEM editar o código dele.

Como funciona:
    1. Faz monkey-patching das *actions* (show, collect, count, write.*, spark.sql de DML...).
    2. Instrumenta os ``if/else`` de negócio dos módulos do projeto via AST em memória.
    3. Executa o entrypoint do job (1ª passada com o fluxo padrão + passadas adicionais
       invertendo automaticamente os ramos ``if/else`` não visitados).
    4. Quando o job chama uma action, o wrapper captura ``df.explain(extended=True)``
       (que só ANALISA, não executa), devolve um valor substituto e segue em frente.
    5. Ao final, devolve/grava um JSON com os planos deduplicados + onde cada um foi
       disparado (arquivo, linha, tabela de destino e ramo de decisão).

Uso (CLI)::

    spark-lineage capture --project-root ./meu_projeto --entrypoint jobs/etl_vendas.py \\
        --output plans.json --text-dir planos/ --param data_ref=2026-11-01

Uso (Python)::

    from spark_lineage.capture_plans import capture_plans

    resultado = capture_plans("./meu_projeto", "jobs/etl_vendas.py")

Importante (segurança):
    Os wrappers evitam gravações, mas um caminho não previsto (biblioteca auxiliar, API
    de baixo nível) ainda pode gravar. Rode com uma credencial SOMENTE LEITURA.
"""

from __future__ import annotations

import argparse
import ast
import builtins
import concurrent.futures
import contextlib
import datetime as dt
import hashlib
import importlib
import importlib.abc
import importlib.machinery
import inspect
import io
import json
import os
import re
import sys
import threading
import tokenize
import traceback
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path
from types import CodeType, ModuleType
from typing import Any

Json = dict[str, Any]

# --------------------------------------------------------------------------- #
# Utilitários
# --------------------------------------------------------------------------- #

_SECTION_RE = re.compile(r"^== (.+?) ==[ \t]*$", re.M)
_SECTION_KEYS = {
    "parsed logical plan": "parsed",
    "analyzed logical plan": "analyzed",
    "optimized logical plan": "optimized",
    "physical plan": "physical",
}
_PLAN_NORM_ID_RE = re.compile(r"#\d+L?")
_PLAN_NORM_CTE_RE = re.compile(r"\b(CTERelation(?:Def|Ref))\s+\d+")
_PLAN_NORM_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\.\d+")


def _normalize_plan_for_hash(text: str) -> str:
    # Remove IDs efêmeros do Catalyst (#123, CTERelationDef 12) e timestamps com microssegundos.
    s = _PLAN_NORM_ID_RE.sub("", text)
    s = _PLAN_NORM_CTE_RE.sub(r"\1", s)
    return _PLAN_NORM_TS_RE.sub("<TS>", s)


def _plan_hash(text: str) -> str:
    normalized = _normalize_plan_for_hash(text).encode()
    return hashlib.sha1(normalized, usedforsecurity=False).hexdigest()[:12]


def explain_text(df: Any) -> str:
    """Obtém o texto do explain estendido (não executa o job; usa só API pública).

    Args:
        df: DataFrame Spark.

    Returns:
        Saída textual de ``df.explain(extended=True)``.
    """
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(extended=True)
    return buf.getvalue()


def explain_formatted(df: Any) -> str:
    """Obtém o texto do explain em modo formatted (não trunca Output, ReadSchema nem filtros no Serverless).

    Args:
        df: DataFrame Spark.

    Returns:
        Saída textual de ``df.explain(mode="formatted")``.
    """
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()


def split_sections(text: str) -> dict[str, str]:
    """Quebra o texto do explain em parsed / analyzed / optimized / physical.

    Args:
        text: Saída bruta do ``explain``.

    Returns:
        Mapa com as seções encontradas (ou ``{"raw": ...}`` quando não há cabeçalhos).
    """
    parts = _SECTION_RE.split(text)  # [antes, nome1, corpo1, nome2, corpo2, ...]
    out: dict[str, str] = {}
    for i in range(1, len(parts) - 1, 2):
        name = parts[i].strip().lower()
        out[_SECTION_KEYS.get(name, name)] = parts[i + 1].strip()
    if not out:
        out["raw"] = text.strip()
    return out


def _scalar_defaults(types_mod: Any) -> tuple[tuple[Any, Any], ...]:
    char_types = (
        types_mod.StringType,
        getattr(types_mod, "CharType", ()),
        getattr(types_mod, "VarcharType", ()),
    )
    ts_types = (types_mod.TimestampType, getattr(types_mod, "TimestampNTZType", ()))
    return (
        ((types_mod.ByteType, types_mod.ShortType, types_mod.IntegerType, types_mod.LongType), 0),
        ((types_mod.FloatType, types_mod.DoubleType), 0.0),
        (types_mod.DecimalType, Decimal(0)),
        (types_mod.BooleanType, False),
        (char_types, "0"),
        (types_mod.BinaryType, b""),
        (types_mod.DateType, dt.date(1970, 1, 1)),
        (ts_types, dt.datetime(1970, 1, 1)),
    )


def _neutral_value(dtype: Any) -> Any:
    # Valor neutro para um tipo Spark (usado nas linhas sintéticas).
    from pyspark.sql import Row, types

    for cls_group, default in _scalar_defaults(types):
        if isinstance(dtype, cls_group):
            return default
    if isinstance(dtype, types.ArrayType):
        return []
    if isinstance(dtype, types.MapType):
        return {}
    if isinstance(dtype, types.StructType):
        names = [f.name for f in dtype.fields]
        return Row(*names)(*[_neutral_value(f.dataType) for f in dtype.fields])
    return None


class _Stub:
    """Objeto que aceita qualquer atributo/chamada (ex.: StreamingQuery, dbutils.fs)."""

    def __getattr__(self, name: str) -> _Stub:
        return self

    def __call__(self, *a: Any, **k: Any) -> _Stub:
        return self

    def __iter__(self) -> Iterator[Any]:
        return iter(())

    def __bool__(self) -> bool:
        return True


class _JobExit(Exception):  # noqa: N818 - sinal de controle de fluxo (como SystemExit), não erro
    """Sinaliza dbutils.notebook.exit(): término normal do job."""


class _Widgets:
    def __init__(self, params: dict[str, str]) -> None:
        self._base_params = dict(params)
        self._tls = threading.local()

    @property
    def _p(self) -> dict[str, str]:
        p = getattr(self._tls, "p", None)
        if p is None:
            p = dict(self._base_params)
            self._tls.p = p
        return p

    @_p.setter
    def _p(self, value: dict[str, str]) -> None:
        self._tls.p = value

    @property
    def _defaults(self) -> dict[str, str]:
        d = getattr(self._tls, "defaults", None)
        if d is None:
            d = {}
            self._tls.defaults = d
        return d

    @_defaults.setter
    def _defaults(self, value: dict[str, str]) -> None:
        self._tls.defaults = value

    def reset_thread_state(self, params: dict[str, str]) -> None:
        self._tls.p = dict(params)
        self._tls.defaults = {}

    def text(self, name: str, defaultValue: str = "", label: str | None = None) -> None:  # noqa: N803
        _ = label
        self._defaults[name] = str(defaultValue)

    def dropdown(
        self,
        name: str,
        defaultValue: str = "",  # noqa: N803
        choices: Any = None,
        label: str | None = None,
    ) -> None:
        _ = choices, label
        self._defaults[name] = str(defaultValue)

    def combobox(
        self,
        name: str,
        defaultValue: str = "",  # noqa: N803
        choices: Any = None,
        label: str | None = None,
    ) -> None:
        _ = choices, label
        self._defaults[name] = str(defaultValue)

    def multiselect(
        self,
        name: str,
        defaultValue: str = "",  # noqa: N803
        choices: Any = None,
        label: str | None = None,
    ) -> None:
        _ = choices, label
        self._defaults[name] = str(defaultValue)

    def get(self, name: str) -> str:
        if name in self._p:
            return self._p[name]
        return self._defaults.get(name, "")

    def getArgument(self, name: str, defaultValue: str = "") -> str:  # noqa: N802, N803
        if name in self._p:
            return self._p[name]
        return self._defaults.get(name, str(defaultValue))

    def getAll(self) -> dict[str, str]:  # noqa: N802 - nome da API oficial do dbutils.widgets
        return {**self._defaults, **self._p}

    def remove(self, name: str) -> None:
        self._defaults.pop(name, None)
        self._p.pop(name, None)

    def removeAll(self) -> None:  # noqa: N802 - nome da API oficial do dbutils.widgets
        self._defaults.clear()
        self._p.clear()

    def __getattr__(self, name: str) -> Callable[..., None]:
        return lambda *a, **k: None


class _Secrets:
    def get(self, scope: str | None = None, key: str | None = None) -> str:
        return ""

    def __getattr__(self, name: str) -> Callable[..., str]:
        return lambda *a, **k: ""


class _Notebook:
    def __init__(self, runner: Callable[..., str] | None = None) -> None:
        self._runner = runner

    def exit(self, value: Any = "") -> None:
        raise _JobExit(value)

    def run(
        self,
        path: str,
        timeout_seconds: int = 0,
        arguments: dict[str, Any] | None = None,
        *_a: Any,
        **_kw: Any,
    ) -> str:
        _ = timeout_seconds
        if self._runner is not None:
            return self._runner(path, arguments or {})
        return ""

    def __getattr__(self, name: str) -> _Stub:
        return _Stub()


class DBUtilsStub:
    """Substituto em memória para ``dbutils`` durante a captura de planos."""

    def __init__(self, params: dict[str, str], notebook_runner: Callable[..., str] | None = None) -> None:
        """Inicializa os submódulos ``widgets``, ``secrets``, ``notebook``, ``fs`` e ``jobs``.

        Args:
            params: Mapa de parâmetros devolvidos por ``dbutils.widgets.get``.
            notebook_runner: Callback opcional para executar ``dbutils.notebook.run(...)``.
        """
        self.widgets = _Widgets(params)
        self.secrets = _Secrets()
        self.notebook = _Notebook(runner=notebook_runner)
        self.fs = _Stub()
        self.jobs = _Stub()


# --------------------------------------------------------------------------- #
# Classificação de SQL (spark.sql executa DDL/DML imediatamente!)
# --------------------------------------------------------------------------- #

_READ_KW = frozenset(
    {
        "SELECT",
        "WITH",
        "VALUES",
        "TABLE",
        "FROM",
        "SHOW",
        "DESCRIBE",
        "DESC",
        "EXPLAIN",
        "USE",
        "SET",
        "LIST",
    }
)
_EXPLAINABLE_KW = frozenset({"INSERT", "MERGE", "UPDATE", "DELETE", "CREATE", "REPLACE"})
_CTE_DML_KW = frozenset({"INSERT", "MERGE", "UPDATE", "DELETE"})
_TEMP_VIEW_RE = re.compile(r"(CREATE|REPLACE)\s+(OR\s+REPLACE\s+)?(GLOBAL\s+)?(TEMP|TEMPORARY)\s+VIEW\b", re.I)
_SQL_INSERT_DIR_RE = re.compile(
    r"^\s*INSERT\s+OVERWRITE\s+(?:LOCAL\s+)?DIRECTORY\s+(?:'([^']+)'|\"([^\"]+)\")",
    re.I | re.S,
)
_SQL_TARGET_RE = re.compile(
    r"^\s*"
    r"(?:"
    r"DELETE(?:\s+FROM)?|"
    r"UPDATE|"
    r"MERGE(?:\s+WITH\s+SCHEMA\s+EVOLUTION)?\s+INTO|"
    r"INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?|"
    r"(?:CREATE|REPLACE)(?:\s+OR\s+(?:REPLACE|REFRESH))?"
    r"(?:\s+(?:EXTERNAL|TEMPORARY|TEMP|GLOBAL\s+TEMP|GLOBAL\s+TEMPORARY|MATERIALIZED|STREAMING))*"
    r"\s+(?:TABLE|VIEW)(?:\s+IF\s+NOT\s+EXISTS)?"
    r")\s+(?!TABLE\b)(?!IF\b)(?!DIRECTORY\b)(?!LOCAL\b)([`\"'\w.\-]+)",
    re.I | re.S,
)
_SQL_INSERT_TARGET_RE = re.compile(
    r"\bINSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?\s+(?!TABLE\b)(?!IF\b)(?!DIRECTORY\b)(?!LOCAL\b)([`\"'\w.\-]+)",
    re.I | re.S,
)
_SQL_FROM_RE = re.compile(r"\b(FROM|JOIN|USING)\s+([`\"'\w.\-]+)", re.I)
_SQL_EXTRACT_TRIM_RE = re.compile(r"\b(?:EXTRACT|TRIM)\s*\([^()]*\)", re.I)
_SQL_NON_TABLES = frozenset({"SELECT", "LATERAL", "VALUES", "UNNEST", "RANGE", "EXPLODE", "ON", "SET", "WHERE"})
_SQL_USING_FORMATS = frozenset(
    {"DELTA", "PARQUET", "CSV", "JSON", "ORC", "ICEBERG", "AVRO", "TEXT", "JDBC", "HIVE", "LIBSVM", "BINARYFILE", "XML"}
)
_QUOTE_CHARS = str.maketrans("", "", "`\"'")
_SQL_DOLLAR_PARAM_RE = re.compile(r"\$\{(?:var\.|bundle\.)?([A-Za-z_]\w*)\}")
_SQL_COLON_PARAM_RE = re.compile(r"(?<![:\w])\:([A-Za-z_]\w*)\b")
_CELL_SEP_RE = re.compile(r"^\s*(?:#|--)\s*COMMAND\s+-+\s*$")
_PY_MAGIC_RE = re.compile(r"^(?P<indent>[ \t]*)#\s*MAGIC(?:\s(?P<body>.*))?$")
_SQL_MAGIC_RE = re.compile(r"^\s*--\s*MAGIC(?:\s(?P<body>.*))?$")


def _strip_sql_comments(query: str, *, blank_strings: bool = False) -> str:
    """Remove comentários SQL (``--`` e ``/* ... */``) respeitando literais e identificadores entre aspas."""
    out: list[str] = []
    i = 0
    n = len(query)
    while i < n:
        ch = query[i]
        nxt = query[i + 1] if i + 1 < n else ""
        if ch in ("'", '"', "`"):
            quote = ch
            blank_q = blank_strings and quote in ("'", '"')
            out.append(ch)
            i += 1
            while i < n:
                c = query[i]
                if c == "\\" and quote != "`" and i + 1 < n:
                    out.append("  " if blank_q else query[i : i + 2])
                    i += 2
                    continue
                if c == quote:
                    if i + 1 < n and query[i + 1] == quote:
                        out.append("  " if blank_q else query[i : i + 2])
                        i += 2
                        continue
                    out.append(c)
                    i += 1
                    break
                out.append(" " if blank_q else c)
                i += 1
            continue
        if ch == "-" and nxt == "-":
            i += 2
            while i < n and query[i] != "\n":
                i += 1
            out.append(" ")
            continue
        if ch == "/" and nxt == "*":
            i += 2
            while i + 1 < n and not (query[i] == "*" and query[i + 1] == "/"):
                i += 1
            i = min(n, i + 2)
            out.append(" ")
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _strip_leading_cte(s: str) -> str:
    """Se ``s`` começar com ``WITH ...``, devolve o trecho a partir do comando principal no nível 0."""
    if not re.match(r"^\s*WITH\b", s, re.I):
        return s
    i = 0
    n = len(s)
    depth = 0
    saw_paren = False
    while i < n:
        ch = s[i]
        if ch in ("'", '"', "`"):
            quote = ch
            i += 1
            while i < n:
                if s[i] == "\\" and quote != "`" and i + 1 < n:
                    i += 2
                    continue
                if s[i] == quote:
                    if i + 1 < n and s[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if ch == "(":
            depth += 1
            saw_paren = True
            i += 1
            continue
        if ch == ")" and depth > 0:
            depth -= 1
            i += 1
            if depth == 0 and saw_paren:
                j = i
                while j < n and s[j].isspace():
                    j += 1
                if j >= n:
                    break
                if s[j] == ",":
                    i = j + 1
                    continue
                m_kw = re.match(r"[A-Za-z]+", s[j:])
                if m_kw and m_kw.group(0).upper() == "AS":
                    i = j + 2
                    continue
                return s[j:]
            continue
        i += 1
    return s


def _top_level_sql(s: str) -> str:
    """Retorna apenas os trechos de ``s`` no nível de parênteses 0 (substituindo subqueries por espaços)."""
    out: list[str] = []
    depth = 0
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if ch in ("'", '"', "`"):
            quote = ch
            out.append(ch if depth == 0 else " ")
            i += 1
            while i < n:
                c = s[i]
                if c == "\\" and quote != "`" and i + 1 < n:
                    out.append(s[i : i + 2] if depth == 0 else "  ")
                    i += 2
                    continue
                if c == quote:
                    if i + 1 < n and s[i + 1] == quote:
                        out.append(s[i : i + 2] if depth == 0 else "  ")
                        i += 2
                        continue
                    out.append(c if depth == 0 else " ")
                    i += 1
                    break
                out.append(c if depth == 0 else " ")
                i += 1
            continue
        if ch == "(":
            depth += 1
            out.append(" ")
        elif ch == ")":
            depth = max(0, depth - 1)
            out.append(" ")
        elif depth == 0:
            out.append(ch)
        else:
            out.append(" ")
        i += 1
    return "".join(out)


_CACHE_TABLE_AS_RE = re.compile(
    r"^(?:\s|--[^\n]*\n|/\*.*?\*/)*CACHE\s+(?:LAZY\s+)?TABLE\s+(?P<name>`[^`]+`|[\w.]+)\s*"
    r"(?:OPTIONS\s*\([^)]*\)\s*)?(?:AS\s+)?(?P<body>(?:SELECT|WITH|VALUES|TABLE|FROM|\().+)$",
    re.I | re.S,
)
_CREATE_TEMP_VIEW_NO_REPLACE_RE = re.compile(
    r"^((?:\s|--[^\n]*\n|/\*.*?\*/)*CREATE\s+)(?!OR\s+REPLACE\b)"
    r"((?:GLOBAL\s+)?TEMP(?:ORARY)?\s+VIEW\b)(?:\s+IF\s+NOT\s+EXISTS\b)?",
    re.I | re.S,
)


def classify_sql(query: str) -> tuple[str, str]:
    """Classifica uma query SQL em modo de interceptação e palavra-chave principal.

    Args:
        query: Comando SQL passado para ``spark.sql``.

    Returns:
        Tupla ``(modo, palavra_chave)``, onde ``modo`` é ``'pass'``, ``'explain'`` ou ``'block'``.
    """
    s = _strip_sql_comments(query, blank_strings=True).strip().lstrip("(").strip()
    m = re.match(r"[A-Za-z]+", s)
    kw = m.group(0).upper() if m else ""
    if _TEMP_VIEW_RE.match(s):
        return "pass", "CREATE_TEMP_VIEW"  # só metadado, e o job normalmente depende dela
    if _CACHE_TABLE_AS_RE.match(s):
        return "pass", "CACHE_TABLE_AS"  # reescrito para CREATE OR REPLACE TEMP VIEW sem materializar cache
    if kw == "WITH":
        main_s = _strip_leading_cte(s).lstrip("(").strip()
        m_main = re.match(r"[A-Za-z]+", main_s)
        main_kw = m_main.group(0).upper() if m_main else ""
        if main_kw in _CTE_DML_KW:
            return "explain", main_kw
        if main_kw == "FROM" and _SQL_INSERT_TARGET_RE.search(_top_level_sql(main_s)):
            return "explain", "INSERT"
        return "pass", "WITH"
    if kw == "FROM" and _SQL_INSERT_TARGET_RE.search(_top_level_sql(s)):
        return "explain", "INSERT"
    if kw in _READ_KW:
        return "pass", kw
    if kw in _EXPLAINABLE_KW:
        return "explain", kw
    return "block", kw or "UNKNOWN"


def extract_sql_targets(query: str) -> list[str]:
    """Extrai todos os nomes de tabelas alvo em comandos DML/DDL (incluindo multi-table ``FROM ... INSERT INTO``).

    Args:
        query: Comando SQL.

    Returns:
        Lista de tabelas alvo normalizadas (sem aspas/crases), preservando ordem e sem duplicatas.
    """
    raw_main = _strip_leading_cte(_strip_sql_comments(query, blank_strings=False).strip()).strip()
    m_dir = _SQL_INSERT_DIR_RE.match(raw_main)
    if m_dir:
        path = (m_dir.group(1) or m_dir.group(2) or "").strip()
        return [path] if path else []
    s = _strip_sql_comments(query, blank_strings=True).strip()
    main_s = _strip_leading_cte(s).strip()
    m = _SQL_TARGET_RE.match(main_s)
    if m:
        tok = m.group(1).translate(_QUOTE_CHARS).strip()
        return [tok] if tok else []
    if re.match(r"^FROM\b", main_s, re.I):
        targets: list[str] = []
        for m_ins in _SQL_INSERT_TARGET_RE.finditer(_top_level_sql(main_s)):
            tok = m_ins.group(1).translate(_QUOTE_CHARS).strip()
            if tok and tok not in targets:
                targets.append(tok)
        return targets
    return []


def extract_sql_target(query: str) -> str | None:
    """Extrai o nome da tabela alvo em comandos DML/DDL (DELETE FROM, INSERT INTO, MERGE INTO, UPDATE, CREATE VIEW).

    Args:
        query: Comando SQL.

    Returns:
        Nome da tabela sem aspas/crases, ou ``None`` se não identificado.
    """
    targets = extract_sql_targets(query)
    return targets[0] if targets else None


def extract_sql_sources(query: str) -> list[str]:
    """Extrai nomes de tabelas referenciadas em cláusulas FROM / JOIN de uma query SQL.

    Args:
        query: Comando SQL.

    Returns:
        Lista de tabelas únicas na ordem em que aparecem.
    """
    s = _strip_sql_comments(query, blank_strings=True)
    s = _SQL_EXTRACT_TRIM_RE.sub(" ", s)
    out: list[str] = []
    for m in _SQL_FROM_RE.finditer(s):
        kw = m.group(1).upper()
        tok = m.group(2).translate(_QUOTE_CHARS).strip()
        if not tok or tok.upper() in _SQL_NON_TABLES or tok.startswith("("):
            continue
        if kw == "USING" and tok.upper() in _SQL_USING_FORMATS:
            continue
        if tok not in out:
            out.append(tok)
    return out


def substitute_sql_params(query: str, params: dict[str, str]) -> str:
    """Substitui parâmetros ``${chave}`` e ``:chave`` em queries SQL usando ``params``.

    Parâmetros ``:chave`` dentro de literais de string, identificadores entre aspas ou comentários
    são preservados intactos.

    Args:
        query: Comando SQL original.
        params: Mapa de parâmetros ativos (CLI ``--param`` + defaults de ``dbutils.widgets``).

    Returns:
        Comando SQL com os parâmetros conhecidos substituídos.
    """
    if not params:
        return query
    text = _SQL_DOLLAR_PARAM_RE.sub(lambda m: params.get(m.group(1), m.group(0)), query)

    def _sub_colon(chunk: str) -> str:
        return _SQL_COLON_PARAM_RE.sub(
            lambda m: ("'" + params[m.group(1)].replace("'", "''") + "'") if m.group(1) in params else m.group(0),
            chunk,
        )

    out: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if ch in ("'", '"', "`"):
            if buf:
                out.append(_sub_colon("".join(buf)))
                buf = []
            quote = ch
            start = i
            i += 1
            while i < n:
                if text[i] == "\\" and quote != "`" and i + 1 < n:
                    i += 2
                    continue
                if text[i] == quote:
                    if i + 1 < n and text[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            out.append(text[start:i])
            continue
        if ch == "-" and nxt == "-":
            if buf:
                out.append(_sub_colon("".join(buf)))
                buf = []
            start = i
            i += 2
            while i < n and text[i] != "\n":
                i += 1
            out.append(text[start:i])
            continue
        if ch == "/" and nxt == "*":
            if buf:
                out.append(_sub_colon("".join(buf)))
                buf = []
            start = i
            i += 2
            while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
                i += 1
            i = min(n, i + 2)
            out.append(text[start:i])
            continue
        buf.append(ch)
        i += 1
    if buf:
        out.append(_sub_colon("".join(buf)))
    return "".join(out)


def _split_sql_statements(indexed_lines: list[tuple[int, str]]) -> list[tuple[int, str]]:
    """Divide linhas SQL ``[(idx_0based, texto), ...]`` em comandos individuais por ``;`` e fim de célula.

    Respeita strings (``'...'``, ``"..."``, crases) e comentários (``--`` e ``/* ... */``),
    devolvendo ``[(linha_inicial_0based, sql_limpo), ...]``.
    """
    statements: list[tuple[int, str]] = []
    cur_chars: list[str] = []
    cur_start_line: int | None = None
    in_quote: str | None = None
    in_block_comment = False

    def flush() -> None:
        nonlocal cur_chars, cur_start_line
        raw = "".join(cur_chars).strip()
        cur_chars = []
        start = cur_start_line
        cur_start_line = None
        if start is not None and raw and _strip_sql_comments(raw).strip():
            statements.append((start, raw))

    for line_idx, line in indexed_lines:
        if not in_quote and not in_block_comment and _CELL_SEP_RE.match(line):
            flush()
            continue
        i = 0
        n = len(line)

        while i < n:
            ch = line[i]
            nxt = line[i + 1] if i + 1 < n else ""
            if in_block_comment:
                cur_chars.append(ch)
                if ch == "*" and nxt == "/":
                    cur_chars.append(nxt)
                    in_block_comment = False
                    i += 2
                    continue
                i += 1
                continue
            if in_quote:
                cur_chars.append(ch)
                if ch == "\\" and in_quote != "`" and i + 1 < n:
                    cur_chars.append(nxt)
                    i += 2
                    continue
                if ch == in_quote:
                    if nxt == in_quote:  # escape ''
                        cur_chars.append(nxt)
                        i += 2
                        continue
                    in_quote = None
                i += 1
                continue
            if ch == "-" and nxt == "-":
                if cur_start_line is not None:
                    cur_chars.append(line[i:])
                break
            if ch == "/" and nxt == "*":
                in_block_comment = True
                cur_chars.append("/*")
                i += 2
                continue
            if ch in ("'", '"', "`"):
                if cur_start_line is None:
                    cur_start_line = line_idx
                in_quote = ch
                cur_chars.append(ch)
                i += 1
                continue
            if ch == ";":
                flush()
                i += 1
                continue
            if cur_start_line is None and not ch.isspace():
                cur_start_line = line_idx
            cur_chars.append(ch)
            i += 1
        if cur_chars:
            cur_chars.append("\n")
    flush()
    return statements


def _emit_stmt_at_line(out_lines: list[str], line_idx: int, code_stmt: str, indent: str = "") -> None:
    existing = out_lines[line_idx].strip()
    if existing and not existing.startswith("#"):
        out_lines[line_idx] = f"{out_lines[line_idx]}; {code_stmt}"
    else:
        out_lines[line_idx] = f"{indent}{code_stmt}"


_TRANSPARENT_PY_MAGICS = ("%time", "%timeit", "%capture")


def _process_magic_block(out_lines: list[str], block: list[tuple[int, str, str]]) -> None:
    """Converte um bloco de linhas ``# MAGIC`` consecutivas em chamadas Python mantendo a numeração de linhas."""
    for idx, indent, _ in block:
        out_lines[idx] = f"{indent}#"

    non_empty = [(idx, indent, body) for idx, indent, body in block if body.strip()]
    if not non_empty:
        return
    first_idx, first_indent, first_body = non_empty[0]
    s = first_body.strip()
    if s.startswith("%%"):
        s = s[1:]  # normaliza cell magics padrão do Jupyter (%%sql, %%python, %%md, %%sh, %%bash, %%time)

    if s.startswith("%md"):
        if first_indent:
            out_lines[first_idx] = f"{first_indent}pass"
        return
    if s.startswith("%run"):
        arg = s[len("%run") :].strip()
        _emit_stmt_at_line(out_lines, first_idx, f"__spark_lineage_run__({arg!r}, globals())", first_indent)
        return
    for magic in ("%pip", "%sh", "%bash", "%fs", "%scala", "%r"):
        if s == magic or s.startswith(f"{magic} "):
            full_cmd = "\n".join(b for _, _, b in non_empty)
            _emit_stmt_at_line(
                out_lines,
                first_idx,
                f"__spark_lineage_magic__({magic[1:]!r}, {full_cmd!r})",
                first_indent,
            )
            return
    if s == "%python" or s.startswith("%python "):
        inline_py = s[len("%python") :].strip()
        if inline_py:
            out_lines[first_idx] = f"{first_indent}{inline_py}"
        for idx, indent, body in block:
            if idx > first_idx:
                out_lines[idx] = f"{indent}{body}"
        return
    if any(s == m or s.startswith(f"{m} ") for m in _TRANSPARENT_PY_MAGICS):
        is_cell_magic = first_body.strip().startswith("%%")
        if not is_cell_magic:
            inline_py = ""
            if s.startswith("%time "):
                inline_py = s[len("%time") :].strip()
            elif s.startswith("%timeit "):
                inline_py = re.sub(r"^(?:-[a-zA-Z]+(?:\s+\d+)?\s+)+", "", s[len("%timeit") :].strip()).strip()
            if inline_py:
                out_lines[first_idx] = f"{first_indent}{inline_py}"
        for idx, indent, body in block:
            if idx > first_idx:
                out_lines[idx] = f"{indent}{body}"
        return
    if s == "%sql" or s.startswith("%sql "):
        sql_lines: list[tuple[int, str]] = []
        inline_sql = s[len("%sql") :].strip()
        if inline_sql:
            sql_lines.append((first_idx, inline_sql))
        for idx, _, body in block:
            if idx > first_idx:
                sql_lines.append((idx, body))
        for stmt_line, stmt_txt in _split_sql_statements(sql_lines):
            _emit_stmt_at_line(out_lines, stmt_line, f"__spark_lineage_sql__({stmt_txt!r})", first_indent)


def _preprocess_py_notebook(src: str) -> str:
    if "# MAGIC" not in src and "#MAGIC" not in src:
        return src
    lines = src.splitlines()
    out_lines = list(lines)
    block: list[tuple[int, str, str]] = []

    for idx, line in enumerate(lines):
        if _CELL_SEP_RE.match(line):
            if block:
                _process_magic_block(out_lines, block)
                block = []
            continue
        m = _PY_MAGIC_RE.match(line)
        if m:
            block.append((idx, m.group("indent") or "", m.group("body") or ""))
        elif block:
            _process_magic_block(out_lines, block)
            block = []
    if block:
        _process_magic_block(out_lines, block)
    return "\n".join(out_lines) + ("\n" if src.endswith("\n") else "")


def _preprocess_sql_source(src: str) -> str:
    """Converte um arquivo ``.sql`` (ou notebook SQL do Databricks) em código Python linha-a-linha."""
    lines = src.splitlines()
    out_lines = ["#" for _ in lines]
    cells: list[list[tuple[int, str]]] = [[]]
    for idx, line in enumerate(lines):
        if _CELL_SEP_RE.match(line):
            cells.append([])
        else:
            cells[-1].append((idx, line))

    for cell in cells:
        if not cell:
            continue
        magic_lines = [(idx, _SQL_MAGIC_RE.match(ln)) for idx, ln in cell]
        only_magic = [(idx, "", m.group("body") or "") for idx, m in magic_lines if m is not None]
        if only_magic:
            first_non_empty = next((b.strip() for _, _, b in only_magic if b.strip()), "")
            if first_non_empty.startswith(
                ("%md", "%run", "%pip", "%sh", "%bash", "%fs", "%scala", "%r", "%python", "%sql", "%%")
            ):
                _process_magic_block(out_lines, only_magic)
                continue
        sql_chunk: list[tuple[int, str]] = []
        for idx, ln in cell:
            s = ln.strip()
            if s.startswith("%run "):
                if sql_chunk:
                    for s_line, s_txt in _split_sql_statements(sql_chunk):
                        _emit_stmt_at_line(out_lines, s_line, f"__spark_lineage_sql__({s_txt!r})")
                    sql_chunk = []
                arg = s[len("%run") :].strip()
                _emit_stmt_at_line(out_lines, idx, f"__spark_lineage_run__({arg!r}, globals())")
            else:
                sql_chunk.append((idx, ln))
        if sql_chunk:
            for s_line, s_txt in _split_sql_statements(sql_chunk):
                _emit_stmt_at_line(out_lines, s_line, f"__spark_lineage_sql__({s_txt!r})")
    return "\n".join(out_lines) + "\n"


_IPYNB_BLOCK_MAGICS = ("%sql", "%md", "%sh", "%bash", "%scala", "%r", "%fs", "%python")


def _update_triple_quote_state(line: str, active_quote: str | None) -> str | None:
    """Rastreia abertura/fechamento de strings multilinha ``\"\"\"`` / ``'''`` para não alterar ``!`` dentro delas."""
    i = 0
    n = len(line)
    while i < n:
        ch = line[i]
        if active_quote is not None:
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if line[i : i + 3] == active_quote:
                active_quote = None
                i += 3
                continue
            i += 1
        else:
            if ch == "#":
                break
            if line[i : i + 3] in ('"""', "'''"):
                active_quote = line[i : i + 3]
                i += 3
                continue
            if ch in ('"', "'"):
                q = ch
                i += 1
                while i < n:
                    if line[i] == "\\" and i + 1 < n:
                        i += 2
                        continue
                    if line[i] == q:
                        i += 1
                        break
                    i += 1
                continue
            i += 1
    return active_quote


_IPYNB_LINE_MAGIC_RE = re.compile(r"^%%?[A-Za-z_]")


def _preprocess_ipynb_source(src: str) -> str:
    """Converte um notebook ``.ipynb`` em script Python com células ``# MAGIC`` e ``# COMMAND``."""
    nb = json.loads(src)
    py_cells: list[str] = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        raw_lines = cell.get("source", [])
        text = "".join(raw_lines) if isinstance(raw_lines, list) else str(raw_lines)
        lines = text.splitlines()
        first_idx = next(
            (i for i, ln in enumerate(lines) if ln.strip() and not ln.lstrip().startswith("#")),
            None,
        )
        first = lines[first_idx].strip() if first_idx is not None else ""
        is_block_magic = first.startswith("%%") or any(
            first == m or first.startswith(f"{m} ") for m in _IPYNB_BLOCK_MAGICS
        )
        if is_block_magic and first_idx is not None:
            prefixed = list(lines[:first_idx]) + [f"# MAGIC {ln}" for ln in lines[first_idx:]]
            py_cells.append("\n".join(prefixed))
        else:
            converted: list[str] = []
            prev_was_magic = False
            triple_q: str | None = None
            for ln in lines:
                stripped = ln.lstrip()
                if triple_q is None and _IPYNB_LINE_MAGIC_RE.match(stripped):
                    if prev_was_magic:
                        converted.append("# COMMAND ----------")
                    indent = ln[: len(ln) - len(stripped)]
                    converted.append(f"{indent}# MAGIC {stripped}")
                    prev_was_magic = True
                elif triple_q is None and stripped.startswith("!") and not stripped.startswith("!="):
                    if prev_was_magic:
                        converted.append("# COMMAND ----------")
                    indent = ln[: len(ln) - len(stripped)]
                    converted.append(f"{indent}# MAGIC %sh {stripped[1:].strip()}")
                    prev_was_magic = True
                else:
                    converted.append(ln)
                    prev_was_magic = False
                    triple_q = _update_triple_quote_state(ln, triple_q)
            py_cells.append("\n".join(converted))
    return _preprocess_py_notebook("\n# COMMAND ----------\n".join(py_cells))


def preprocess_notebook_source(src: str, filepath: str) -> str:
    """Pré-processa arquivos ``.py`` (com ``# MAGIC``), ``.sql`` e ``.ipynb`` em código Python executável.

    Args:
        src: Conteúdo bruto do arquivo.
        filepath: Caminho do arquivo (usado para detectar a extensão ``.sql`` / ``.ipynb`` / ``.py``).

    Returns:
        Código Python pronto para ``ast.parse`` com numeração de linhas preservada.
    """
    low = filepath.lower()
    if low.endswith(".sql"):
        return _preprocess_sql_source(src)
    if low.endswith(".ipynb"):
        return _preprocess_ipynb_source(src)
    return _preprocess_py_notebook(src)


# --------------------------------------------------------------------------- #
# Exploração automática de ramos (AST Branch Flipping)
# --------------------------------------------------------------------------- #

_SKIP_NAMES = frozenset({"__name__", "TYPE_CHECKING", "importlib"})
_SYS_SKIP_ATTRS = frozenset({"path", "modules", "version_info", "platform"})


def _is_none_comparison(node: ast.Compare) -> bool:
    has_is = any(isinstance(op, (ast.Is, ast.IsNot)) for op in node.ops)
    has_none = any(isinstance(c, ast.Constant) and c.value is None for c in (node.left, *node.comparators))
    return has_is and has_none


def _is_technical_guard(child: ast.AST) -> bool:
    if isinstance(child, ast.Name):
        return child.id in _SKIP_NAMES
    if isinstance(child, ast.Attribute):
        return (
            isinstance(child.value, ast.Name)
            and child.value.id == "sys"
            and child.attr in _SYS_SKIP_ATTRS
        )
    if isinstance(child, ast.Compare):
        return _is_none_comparison(child)
    return False


def _should_instrument_if(test_node: ast.AST) -> bool:
    # Filtra guardas técnicos (__name__ == '__main__', sys.path, x is None) e mantém decisões de negócio.
    return not any(_is_technical_guard(child) for child in ast.walk(test_node))


def _has_udf_decorator(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    return any("udf" in ast.unparse(dec).lower() for dec in node.decorator_list)


_SPARK_HINT_PARAM_NAMES = frozenset(
    {"spark", "sc", "df", "dataframe", "batch_df", "dbutils", "sqlcontext", "delta_table", "session"}
)
_SPARK_HINT_TYPE_TOKENS = ("dataframe", "sparksession", "column", "deltatable", "catalog")
_SPARK_DISQUALIFY_NAMES = frozenset(
    {
        "spark",
        "sc",
        "sqlContext",
        "dbutils",
        "DeltaTable",
        "SparkSession",
        "__spark_lineage_sql__",
        "__spark_lineage_run__",
        "__spark_lineage_magic__",
    }
)
_SAFE_SINK_DECORATORS = frozenset({"staticmethod", "classmethod"})
_SAFE_SINK_BUILTINS = frozenset(
    {
        "print",
        "len",
        "str",
        "repr",
        "int",
        "float",
        "bool",
        "type",
        "isinstance",
        "issubclass",
        "format",
        "round",
        "min",
        "max",
        "sum",
        "sorted",
        "enumerate",
        "zip",
        "range",
        "list",
        "dict",
        "set",
        "tuple",
        "abs",
        "any",
        "all",
        "chr",
        "ord",
        "id",
        "hasattr",
        "getattr",
        "pprint",
    }
)
_SAFE_LOG_METHODS = frozenset(
    {
        "debug",
        "info",
        "warning",
        "warn",
        "error",
        "critical",
        "exception",
        "fatal",
        "log",
        "flush",
        "pprint",
        "pformat",
        "format_exc",
        "print_exc",
        "getLogger",
        "basicConfig",
        "isEnabledFor",
        "setLevel",
    }
)
_SAFE_PURE_METHODS = frozenset(
    {
        "format",
        "join",
        "strip",
        "lstrip",
        "rstrip",
        "lower",
        "upper",
        "title",
        "capitalize",
        "replace",
        "split",
        "rsplit",
        "splitlines",
        "startswith",
        "endswith",
        "encode",
        "decode",
        "ljust",
        "rjust",
        "center",
        "zfill",
        "find",
        "rfind",
        "index",
        "isoformat",
        "strftime",
        "keys",
        "values",
        "items",
        "get",
        "copy",
    }
)
_SAFE_MODULE_METHODS: dict[str, frozenset[str]] = {
    "datetime": frozenset({"now", "utcnow", "today", "fromtimestamp", "strftime", "isoformat"}),
    "time": frozenset({"time", "monotonic", "perf_counter", "strftime", "localtime", "gmtime"}),
    "json": frozenset({"dumps"}),
    "os": frozenset({"getenv"}),
    "environ": frozenset({"get"}),
}


def _dotted_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _is_safe_assign_target(target: ast.AST) -> bool:
    if isinstance(target, ast.Name):
        return True
    if isinstance(target, (ast.Tuple, ast.List)):
        return all(_is_safe_assign_target(elt) for elt in target.elts)
    return False


def _is_logger_receiver(recv: ast.AST) -> bool:
    dotted = _dotted_name(recv).lower()
    if dotted in ("logging", "warnings", "pprint", "traceback", "sys.stdout", "sys.stderr", "stdout", "stderr"):
        return True
    if isinstance(recv, ast.Call):
        return _dotted_name(recv.func).lower() == "logging.getlogger"
    tail = dotted.rsplit(".", 1)[-1] if dotted else ""
    return bool(tail) and ("log" in tail or tail in ("console", "stdout", "stderr"))


def _is_safe_sink_call(call: ast.Call, safe_local_sinks: set[str]) -> bool:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id in _SAFE_SINK_BUILTINS or func.id in safe_local_sinks
    if isinstance(func, ast.Attribute):
        method = func.attr
        recv_dotted = _dotted_name(func.value).lower()
        recv_tail = recv_dotted.rsplit(".", 1)[-1] if recv_dotted else ""
        if method == "write":
            return recv_dotted in ("sys.stdout", "sys.stderr", "stdout", "stderr")
        if method in _SAFE_LOG_METHODS and _is_logger_receiver(func.value):
            return True
        if method in _SAFE_PURE_METHODS:
            return True
        if recv_tail in _SAFE_MODULE_METHODS and method in _SAFE_MODULE_METHODS[recv_tail]:
            return True
        return False
    return False


def _has_spark_param_or_annotation(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    args = fn.args
    all_args = [*args.posonlyargs, *args.args, *args.kwonlyargs]
    if args.vararg:
        all_args.append(args.vararg)
    if args.kwarg:
        all_args.append(args.kwarg)
    for a in all_args:
        if a.arg.lower() in _SPARK_HINT_PARAM_NAMES:
            return True
        if a.annotation is not None:
            ann = ast.unparse(a.annotation).lower()
            if any(tok in ann for tok in _SPARK_HINT_TYPE_TOKENS):
                return True
    return False


def _is_non_spark_sink_function(
    fn: ast.FunctionDef | ast.AsyncFunctionDef,
    safe_local_sinks: set[str] | None = None,
) -> bool:
    """Retorna ``True`` se ``fn`` for uma função sink pura (sem retorno com valor, sem mutação externa e sem Spark)."""
    local_sinks = safe_local_sinks or set()
    for dec in fn.decorator_list:
        if _dotted_name(dec) not in _SAFE_SINK_DECORATORS:
            return False
    if _has_spark_param_or_annotation(fn):
        return False

    for stmt in fn.body:
        for child in ast.walk(stmt):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Yield, ast.YieldFrom)):
                return False
            if isinstance(child, (ast.Global, ast.Nonlocal)):
                return False
            if isinstance(child, ast.Return) and child.value is not None:
                return False
            if isinstance(child, ast.Import):
                if any(a.name.split(".")[0] in ("pyspark", "delta") for a in child.names):
                    return False
            elif isinstance(child, ast.ImportFrom):
                if (child.module or "").split(".")[0] in ("pyspark", "delta"):
                    return False
            elif isinstance(child, ast.Assign):
                if not all(_is_safe_assign_target(t) for t in child.targets):
                    return False
            elif isinstance(child, (ast.AnnAssign, ast.AugAssign)):
                if not _is_safe_assign_target(child.target):
                    return False
            elif isinstance(child, ast.Delete):
                if not all(_is_safe_assign_target(t) for t in child.targets):
                    return False
            elif isinstance(child, ast.Name):
                if child.id in _SPARK_DISQUALIFY_NAMES:
                    return False
            elif isinstance(child, ast.Call):
                if not _is_safe_sink_call(child, local_sinks):
                    return False
    return True


def _collect_sink_functions(module: ast.Module) -> set[str]:
    """Identifica funções sink no módulo (incluindo sinks que chamam outros sinks do mesmo módulo)."""
    top_funcs = [n for n in module.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    sinks: set[str] = set()
    changed = True
    while changed:
        changed = False
        for fn in top_funcs:
            if fn.name not in sinks and _is_non_spark_sink_function(fn, sinks):
                sinks.add(fn.name)
                changed = True
    return sinks


class _BranchTransformer(ast.NodeTransformer):
    # Envolve condições de `if` de negócio com `__spark_lineage_branch__(site, cond_str, expr)`.

    def __init__(self, rel_file: str) -> None:
        self.rel_file = rel_file
        self._safe_local_sinks: set[str] = set()

    def visit_Module(self, node: ast.Module) -> ast.Module:
        self._safe_local_sinks = _collect_sink_functions(node)
        self.generic_visit(node)
        return node

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:
        if _has_udf_decorator(node) or _is_non_spark_sink_function(node, self._safe_local_sinks):
            return node
        return self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AST:
        if _has_udf_decorator(node) or _is_non_spark_sink_function(node, self._safe_local_sinks):
            return node
        return self.generic_visit(node)

    def visit_If(self, node: ast.If) -> ast.If:
        self.generic_visit(node)
        if not _should_instrument_if(node.test):
            return node
        # Pula guardas puros de validação (`if cond: raise ...` sem `else`), pois forçá-los só aborta a execução.
        if not node.orelse and len(node.body) == 1 and isinstance(node.body[0], ast.Raise):
            return node
        site_id = f"{self.rel_file}:{node.lineno}"
        cond_str = ast.unparse(node.test)
        call = ast.Call(
            func=ast.Name(id="__spark_lineage_branch__", ctx=ast.Load()),
            args=[
                ast.Constant(value=site_id),
                ast.Constant(value=cond_str),
                node.test,
            ],
            keywords=[],
        )
        node.test = ast.copy_location(call, node.test)
        return node

    def visit_IfExp(self, node: ast.IfExp) -> ast.IfExp:
        self.generic_visit(node)
        if not _should_instrument_if(node.test):
            return node
        site_id = f"{self.rel_file}:{node.lineno}:{node.col_offset}"
        cond_str = ast.unparse(node.test)
        call = ast.Call(
            func=ast.Name(id="__spark_lineage_branch__", ctx=ast.Load()),
            args=[
                ast.Constant(value=site_id),
                ast.Constant(value=cond_str),
                node.test,
            ],
            keywords=[],
        )
        node.test = ast.copy_location(call, node.test)
        return node


class _BranchLoader(importlib.abc.Loader):
    def __init__(self, origin: str, code: CodeType) -> None:
        self.origin = origin
        self.code = code

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType | None:
        return None

    def exec_module(self, module: ModuleType) -> None:
        module.__file__ = self.origin
        exec(self.code, module.__dict__)


def _is_project_file(abspath: str, root_prefix: str, tool_file: str) -> bool:
    if abspath == tool_file or not abspath.startswith(root_prefix):
        return False
    return "site-packages" not in abspath and f"{os.sep}.venv{os.sep}" not in abspath


class _BranchMetaPathFinder(importlib.abc.MetaPathFinder):
    # Intercepta imports de arquivos .py dentro da raiz do projeto para instrumentar seus `if/else`.

    def __init__(self, root: Path, root_prefix: str, tool_file: str) -> None:
        self.root = root
        self.root_prefix = root_prefix
        self.tool_file = tool_file
        self._code_cache: dict[str, CodeType] = {}
        self._lock = threading.Lock()
        self._tls = threading.local()

    def compile_file(self, filepath: str) -> CodeType:
        abspath = os.path.abspath(filepath)
        with self._lock:
            cached = self._code_cache.get(abspath)
        if cached is not None:
            return cached
        try:
            with tokenize.open(abspath) as f:
                raw_src = f.read()
        except Exception:  # noqa: BLE001
            raw_src = Path(abspath).read_text(encoding="utf-8")
        src = preprocess_notebook_source(raw_src, abspath)
        rel = os.path.relpath(abspath, self.root)
        tree = ast.parse(src, filename=abspath)
        tree = _BranchTransformer(rel).visit(tree)
        ast.fix_missing_locations(tree)
        compiled = compile(tree, filename=abspath, mode="exec")
        with self._lock:
            self._code_cache[abspath] = compiled
        return compiled

    def find_spec(
        self,
        fullname: str,
        path: Any,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if getattr(self._tls, "in_find", False):
            return None
        self._tls.in_find = True
        try:
            spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
        finally:
            self._tls.in_find = False
        if spec is None or not spec.origin or not spec.origin.endswith(".py"):
            return spec
        origin = os.path.abspath(spec.origin)
        if not _is_project_file(origin, self.root_prefix, self.tool_file):
            return spec
        try:
            code = self.compile_file(origin)
        except Exception:  # noqa: BLE001
            return spec
        spec.loader = _BranchLoader(origin, code)
        return spec


class BranchExplorer:
    """Rastreia decisões ``if/else`` executadas e agenda passadas extras (inclusive em paralelo) para ramos não visitados."""

    def __init__(self, enabled: bool = True) -> None:
        """Inicializa o explorador de ramos.

        Args:
            enabled: Se ``True``, habilita o rastreamento e a inversão automática de ramos.
        """
        self.enabled = enabled
        self.branches: dict[str, Json] = {}
        self.order: list[str] = []
        self._scheduled: set[tuple[str, bool]] = set()
        self._lock = threading.RLock()
        self._tls = threading.local()
        self.start_pass(0)

    def _state(self) -> Json:
        st = getattr(self._tls, "state", None)
        if st is None:
            st = {
                "pass_index": 0,
                "target_site": None,
                "target_value": None,
                "planned_forces": {},
                "has_flipped": True,
                "active_branch_label": None,
                "current_forces_taken": {},
                "forced_this_pass": set(),
            }
            self._tls.state = st
        return st

    @property
    def pass_index(self) -> int:
        return self._state()["pass_index"]

    @pass_index.setter
    def pass_index(self, val: int) -> None:
        self._state()["pass_index"] = val

    @property
    def target_site(self) -> str | None:
        return self._state()["target_site"]

    @target_site.setter
    def target_site(self, val: str | None) -> None:
        self._state()["target_site"] = val

    @property
    def target_value(self) -> bool | None:
        return self._state()["target_value"]

    @target_value.setter
    def target_value(self, val: bool | None) -> None:
        self._state()["target_value"] = val

    @property
    def planned_forces(self) -> dict[str, bool]:
        return self._state()["planned_forces"]

    @planned_forces.setter
    def planned_forces(self, val: dict[str, bool]) -> None:
        self._state()["planned_forces"] = val

    @property
    def has_flipped(self) -> bool:
        return self._state()["has_flipped"]

    @has_flipped.setter
    def has_flipped(self, val: bool) -> None:
        self._state()["has_flipped"] = val

    @property
    def active_branch_label(self) -> str | None:
        return self._state()["active_branch_label"]

    @active_branch_label.setter
    def active_branch_label(self, val: str | None) -> None:
        self._state()["active_branch_label"] = val

    def start_pass(
        self,
        pass_index: int,
        target_site: str | None = None,
        target_value: bool | None = None,
        planned_forces: dict[str, bool] | None = None,
    ) -> None:
        """Inicia uma nova passada de execução do job (estado isolado na thread atual).

        Args:
            pass_index: Índice da passada (0 para a execução natural).
            target_site: Identificador ``arquivo:linha`` do ramo que esta passada pretende inverter.
            target_value: Valor booleano forçado no ``target_site``.
            planned_forces: Mapa completo de decisões forçadas para alcançar o ``target_site``.
        """
        self._tls.state = {
            "pass_index": pass_index,
            "target_site": target_site,
            "target_value": target_value,
            "planned_forces": dict(planned_forces or {}),
            "has_flipped": pass_index == 0,
            "active_branch_label": None,
            "current_forces_taken": {},
            "forced_this_pass": set(),
        }

    def _ensure_branch(self, site_id: str, cond_str: str, actual: bool, st: Json) -> Json:
        info = self.branches.get(site_id)
        if info is None:
            info = {
                "site": site_id,
                "condition": cond_str,
                "natural_value": actual if st["pass_index"] == 0 else None,
                "visited": set(),
                "required_forces": dict(st["current_forces_taken"]),
                "status_by_value": {},
            }
            self.branches[site_id] = info
            self.order.append(site_id)
        return info

    def decide(self, site_id: str, cond_str: str, value: Any) -> bool:
        """Avalia (ou força) o resultado de um ``if`` instrumentado.

        Args:
            site_id: Localização ``arquivo:linha`` do ``if``.
            cond_str: Expressão da condição em texto.
            value: Valor original avaliado em tempo de execução.

        Returns:
            O booleano efetivo a ser seguido pelo ``if``.
        """
        actual = bool(value)
        if not self.enabled:
            return actual
        st = self._state()
        with self._lock:
            info = self._ensure_branch(site_id, cond_str, actual, st)
            if site_id in st["planned_forces"] and site_id not in st["forced_this_pass"]:
                st["forced_this_pass"].add(site_id)
                chosen = st["planned_forces"][site_id]
                st["current_forces_taken"][site_id] = chosen
                info["visited"].add(chosen)
                if site_id == st["target_site"] and not st["has_flipped"]:
                    st["has_flipped"] = True
                    st["active_branch_label"] = f"{site_id} ({cond_str} -> {chosen})"
                return chosen
            info["visited"].add(actual)
            info["status_by_value"].setdefault(str(actual), "ok")
            return actual

    def mark_target_status(self, status_msg: str) -> None:
        """Registra o status final do ramo forçado na passada atual.

        Args:
            status_msg: Mensagem de status (ex.: ``"ok"`` ou descrição de erro).
        """
        st = self._state()
        target_site = st["target_site"]
        target_value = st["target_value"]
        with self._lock:
            if target_site and target_site in self.branches:
                info = self.branches[target_site]
                if target_value is not None:
                    info["visited"].add(target_value)
                info["status_by_value"][str(target_value)] = status_msg

    def next_unvisited(self) -> tuple[str, bool, str, dict[str, bool]] | None:
        """Retorna o próximo ramo ainda não visitado, ou ``None`` se todos foram explorados.

        Returns:
            Tupla ``(site_id, valor_forçado, condição, forces_necessários)`` ou ``None``.
        """
        with self._lock:
            for site_id in self.order:
                info = self.branches[site_id]
                for candidate in (True, False):
                    if candidate not in info["visited"] and (site_id, candidate) not in self._scheduled:
                        forces = {**info["required_forces"], site_id: candidate}
                        return site_id, candidate, info["condition"], forces
            return None

    def next_unvisited_batch(self, max_items: int) -> list[tuple[str, bool, str, dict[str, bool]]]:
        """Reserva atomicamente até ``max_items`` ramos ainda não visitados para execução paralela."""
        if max_items <= 0:
            return []
        batch: list[tuple[str, bool, str, dict[str, bool]]] = []
        with self._lock:
            for site_id in self.order:
                info = self.branches[site_id]
                for candidate in (True, False):
                    if candidate not in info["visited"] and (site_id, candidate) not in self._scheduled:
                        self._scheduled.add((site_id, candidate))
                        forces = {**info["required_forces"], site_id: candidate}
                        batch.append((site_id, candidate, info["condition"], forces))
                        if len(batch) >= max_items:
                            return batch
        return batch

    def summary(self) -> list[Json]:
        """Gera o resumo dos ramos descobertos e explorados.

        Returns:
            Lista de dicionários com condição, valor natural, valores explorados e status.
        """
        out: list[Json] = []
        for site_id in self.order:
            info = self.branches[site_id]
            nat = info["natural_value"]
            out.append(
                {
                    "site": info["site"],
                    "condition": info["condition"],
                    "natural_value": nat,
                    "explored_values": sorted(info["visited"], key=lambda x: (x is not nat,)),
                    "status": info["status_by_value"],
                }
            )
        return out


# --------------------------------------------------------------------------- #
# Captura
# --------------------------------------------------------------------------- #


def _as_dataframe(obj: Any, owner: Any = None) -> Any:
    # Garante um objeto com `.explain()` (reconstrói ConnectDataFrame quando `writer._df` é LogicalPlan).
    if hasattr(obj, "explain"):
        return obj
    session = next(
        (getattr(owner, attr, None) for attr in ("_spark", "_session", "_sparkSession") if getattr(owner, attr, None)),
        None,
    )
    if session is None:
        raise AttributeError(f"{type(obj).__name__} não é DataFrame e não achei a sessão no writer")
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

    return ConnectDataFrame(obj, session)


def _writer_source(writer: Any) -> Any:
    # DataFrame/plano de origem de um writer (batch, V2 ou streaming).
    src = getattr(writer, "_df", None)
    if src is None:  # DataStreamWriter do Spark Connect guarda o plano em _write_stream
        src = getattr(getattr(writer, "_write_stream", None), "_child", None)
    return src


def _normalize_target(val: str | None) -> str | None:
    if not isinstance(val, str):
        return None
    cleaned = val.replace("`", "").strip()
    return cleaned or None


def _writer_table(writer: Any) -> str | None:
    # Nome da tabela de destino de um DataFrameWriterV2 (o atributo varia por versão).
    name = getattr(writer, "_table_name", None) or getattr(writer, "_table", None)
    if name is None:
        name = getattr(getattr(writer, "_write", None), "table_name", None)
    return _normalize_target(name)


def _extract_df_columns(df: Any) -> list[dict[str, str]] | None:
    try:
        return [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields]
    except Exception:  # noqa: BLE001
        return None


def _attach_formatted_plan(df: Any, sections: dict[str, str]) -> None:
    try:
        fmt_txt = explain_formatted(df).strip()
        if fmt_txt:
            sections["formatted"] = fmt_txt
    except Exception:  # noqa: BLE001
        pass


class Capturer:
    """Acumula planos capturados, eventos bloqueados, avisos e locais de leitura durante a execução."""

    def __init__(
        self,
        root: Path,
        collect_mode: str,
        count_value: int,
        params: dict[str, str],
        explorer: BranchExplorer | None = None,
    ) -> None:
        """Inicializa o estado de captura para uma execução do job.

        Args:
            root: Diretório raiz do projeto.
            collect_mode: ``"synthetic"`` ou ``"empty"``.
            count_value: Valor devolvido por ``df.count()``.
            params: Parâmetros de widget do ``dbutils``.
            explorer: Explorador de ramos ``if/else`` ativo.
        """
        self.root = root
        self.root_prefix = str(root) + os.sep
        self.collect_mode = collect_mode
        self.count_value = count_value
        self.params = params
        self.explorer = explorer or BranchExplorer(enabled=False)
        self.plans: dict[str, Json] = {}
        self.events: list[Json] = []
        self.warnings: list[Json] = []
        self.read_sites: dict[str, list[str]] = {}
        self._tool_file = os.path.abspath(__file__)
        self._lock = threading.RLock()
        self._tls = threading.local()
        self._row_cache: dict[tuple[Any, ...], list[Any]] = {}
        self._pandas_cache: dict[tuple[Any, ...], Any] = {}
        self._table_exists_cache: dict[str, bool] = {}
        self._ast_cache: dict[str, ast.Module | None] = {}

    @property
    def written_tables(self) -> dict[str, Any]:
        wt = getattr(self._tls, "written_tables", None)
        if wt is None:
            wt = {}
            self._tls.written_tables = wt
        return wt

    @written_tables.setter
    def written_tables(self, value: dict[str, Any]) -> None:
        self._tls.written_tables = value

    @property
    def written_paths(self) -> dict[str, Any]:
        wp = getattr(self._tls, "written_paths", None)
        if wp is None:
            wp = {}
            self._tls.written_paths = wp
        return wp

    @written_paths.setter
    def written_paths(self, value: dict[str, Any]) -> None:
        self._tls.written_paths = value

    def record_written_table(self, table: str | None, df: Any) -> None:
        """Registra uma tabela gravada (interceptada) para que ``tableExists`` e ``spark.table`` a enxerguem."""
        norm = _normalize_target(table)
        if norm and df is not None:
            self.written_tables[norm.lower()] = df

    def get_written_table(self, table: str | None) -> Any | None:
        norm = _normalize_target(table)
        return self.written_tables.get(norm.lower()) if norm else None

    @staticmethod
    def _norm_path(path: str | None) -> str | None:
        if not isinstance(path, str):
            return None
        cleaned = path.strip().rstrip("/\\")
        return cleaned.lower() if cleaned else None

    def record_written_path(self, path: str | None, df: Any) -> None:
        """Registra um caminho de arquivo gravado (interceptado) para re-leituras posteriores no mesmo job."""
        key = self._norm_path(path)
        if key and df is not None:
            self.written_paths[key] = df

    def get_written_path(self, path: str | None) -> Any | None:
        key = self._norm_path(path)
        return self.written_paths.get(key) if key else None

    def call_site(self) -> Json | None:
        """Localiza onde no código do projeto a action atual foi chamada.

        Returns:
            Dicionário com ``file``, ``line``, ``function``, ``code`` e ``call_chain``, ou ``None``.
        """
        chain: list[Json] = [
            {
                "file": os.path.relpath(os.path.abspath(fr.filename), self.root),
                "line": fr.lineno,
                "function": fr.name,
                "code": (fr.line or "").strip(),
            }
            for fr in traceback.extract_stack()
            if _is_project_file(os.path.abspath(fr.filename), self.root_prefix, self._tool_file)
        ]
        if not chain:
            return None
        return {**chain[-1], "call_chain": chain[-8:]}

    @staticmethod
    def _chain_key(site: Json | None) -> tuple[Any, ...]:
        if not site:
            return ()
        return tuple((c.get("file"), c.get("line")) for c in site.get("call_chain", [site]))

    @staticmethod
    def _target(a: tuple[Any, ...], kw: dict[str, Any], method: str = "") -> str | None:
        if method == "jdbc":
            if isinstance(kw.get("table"), str):
                return _normalize_target(kw["table"])
            if len(a) >= 2 and isinstance(a[1], str):
                return _normalize_target(a[1])
            return None
        if a and isinstance(a[0], str):
            return _normalize_target(a[0])
        for k in ("path", "name", "tableName", "table"):
            val = kw.get(k)
            if isinstance(val, str):
                return _normalize_target(val)
        return None

    def capture(  # noqa: PLR0913, PLR0917
        self,
        df: Any,
        action: str,
        a: tuple[Any, ...] = (),
        kw: dict[str, Any] | None = None,
        target: str | None = None,
        extra: Json | None = None,
        owner: Any = None,
    ) -> None:
        """Captura o plano de execução de ``df`` para a ``action`` interceptada.

        Args:
            df: DataFrame ou LogicalPlan.
            action: Nome da ação interceptada (ex.: ``"collect"``, ``"write.saveAsTable"``).
            a: Argumentos posicionais originais da ação.
            kw: Argumentos nomeados originais da ação.
            target: Tabela ou caminho de destino, quando aplicável.
            extra: Metadados extras (como query SQL ou condição de merge).
            owner: Objeto writer dono do plano (para recuperar a sessão no Spark Connect).
        """
        if not self.explorer.has_flipped:
            return
        site = self.call_site()
        resolved_target = target if target is not None else self._target(a, kw or {})
        try:
            df = _as_dataframe(df, owner)
            sections = split_sections(explain_text(df))
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                self.warnings.append(
                    {
                        "action": action,
                        "call_site": site,
                        "message": f"falha ao obter o plano ({type(df).__name__}): {exc!r}",
                    }
                )
            return
        # Para planos novos, captura também `mode='formatted'` para evitar truncamento no Serverless.
        base = sections.get("analyzed") or sections.get("raw") or ""
        with self._lock:
            is_new = _plan_hash(base) not in self.plans
        if is_new:
            _attach_formatted_plan(df, sections)
        self.add_plan(sections, action, resolved_target, _extract_df_columns(df), site, extra)

    def _occurrence_sig(self, action: Any, target: Any, site: Json | None, extra: Json | None) -> tuple[Any, ...]:
        return (action, target, self._chain_key(site), (extra or {}).get("query"))

    def add_plan(
        self,
        sections: dict[str, str],
        action: str,
        target: str | None,
        columns: list[dict[str, str]] | None,
        site: Json | None,
        extra: Json | None = None,
    ) -> None:
        """Registra um plano já separado em seções, deduplicando por hash e ocorrência.

        Args:
            sections: Seções do explain (parsed, analyzed, optimized, physical, formatted).
            action: Nome da ação que disparou o plano.
            target: Tabela ou caminho de destino.
            columns: Colunas de saída do DataFrame.
            site: Local de chamada no código do projeto.
            extra: Metadados adicionais da ocorrência.
        """
        base = sections.get("analyzed") or sections.get("raw") or ""
        key = _plan_hash(base)
        branch_label = self.explorer.active_branch_label
        with self._lock:
            plan: Json | None = self.plans.get(key)
            if plan is None:
                plan = {
                    "id": f"P{len(self.plans) + 1:03d}",
                    "hash": key,
                    "output_columns": columns,
                    "sections": sections,
                    "occurrences": [],
                }
                self.plans[key] = plan

            occurrences: list[Json] = plan["occurrences"]
            new_sig = self._occurrence_sig(action, target, site, extra)
            if any(
                self._occurrence_sig(ex.get("action"), ex.get("target"), ex.get("call_site"), ex.get("extra"))
                == new_sig
                for ex in occurrences
            ):
                return

            occ: Json = {"action": action, "target": target, "call_site": site}
            if branch_label:
                occ["branch"] = branch_label
            if extra:
                occ["extra"] = extra
            occurrences.append(occ)

    def event(self, kind: str, **info: Any) -> None:
        """Registra um evento interceptado que não gera plano (ex.: DDL bloqueado, operação Delta).

        Args:
            kind: Categoria do evento.
            **info: Detalhes adicionais do evento.
        """
        if not self.explorer.has_flipped:
            return
        site = self.call_site()
        branch_label = self.explorer.active_branch_label
        new_sig = (kind, self._chain_key(site), info.get("statement_kind"), info.get("name"), info.get("query"))
        with self._lock:
            for existing in self.events:
                ex_sig = (
                    existing.get("kind"),
                    self._chain_key(existing.get("call_site")),
                    existing.get("statement_kind"),
                    existing.get("name"),
                    existing.get("query"),
                )
                if ex_sig == new_sig:
                    return
            ev: Json = {"kind": kind, "call_site": site, **info}
            if branch_label:
                ev["branch"] = branch_label
            self.events.append(ev)

    def warn(self, message: str) -> None:
        """Adiciona um aviso associado ao ponto atual de chamada no projeto.

        Args:
            message: Texto descritivo do aviso.
        """
        if self.explorer.has_flipped:
            site = self.call_site()
            with self._lock:
                self.warnings.append({"call_site": site, "message": message})

    def record_read(self, table: str | None) -> None:
        """Registra ``arquivo:linha`` onde o código do projeto iniciou a leitura de uma tabela.

        Args:
            table: Nome da tabela ou caminho lido.
        """
        norm = (table or "").replace("`", "").strip()
        site = self.call_site() if norm else None
        if not site or not site.get("file"):
            return
        loc = f"{site['file']}:{site['line']}"
        with self._lock:
            lst = self.read_sites.setdefault(norm, [])
            if loc not in lst:
                lst.append(loc)

    # ---- valores substitutos ---- #
    @staticmethod
    def _row(df: Any) -> Any:
        try:
            from pyspark.sql import Row

            names = [f.name for f in df.schema.fields]
            return Row(*names)(*[_neutral_value(f.dataType) for f in df.schema.fields])
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _schema_key(df: Any) -> tuple[tuple[str, str], ...]:
        try:
            return tuple((f.name, f.dataType.simpleString()) for f in df.schema.fields)
        except Exception:  # noqa: BLE001
            return ()

    def rows(self, df: Any) -> list[Any]:
        """Devolve a lista de linhas substitutas para ``collect()`` / ``take()`` / ``head()``.

        Args:
            df: DataFrame Spark.

        Returns:
            Lista com 1 ``Row`` sintética (modo ``synthetic``) ou lista vazia (modo ``empty``).
        """
        if self.collect_mode == "empty":
            return []
        chain = self._chain_key(self.call_site())
        ck = (chain, self._schema_key(df)) if chain else ()
        if not self.explorer.has_flipped:
            with self._lock:
                if ck in self._row_cache:
                    return list(self._row_cache[ck])
        row = self._row(df)
        res = [row] if row is not None else []
        if ck:
            with self._lock:
                self._row_cache[ck] = list(res)
        return res

    def first_row(self, df: Any) -> Any:
        """Devolve a primeira linha substituta para ``first()`` / ``head()``.

        Args:
            df: DataFrame Spark.

        Returns:
            Uma ``Row`` sintética ou ``None``.
        """
        r = self.rows(df)
        return r[0] if r else None

    def pandas(self, df: Any) -> Any:
        """Devolve um ``pandas.DataFrame`` substituto para ``toPandas()``.

        Args:
            df: DataFrame Spark.

        Returns:
            DataFrame pandas sintético com o mesmo schema, ou ``None`` em caso de erro.
        """
        chain = self._chain_key(self.call_site())
        ck = (chain, self._schema_key(df)) if chain else ()
        if not self.explorer.has_flipped:
            with self._lock:
                if ck in self._pandas_cache:
                    return self._pandas_cache[ck].copy()
        try:
            import pandas as pd

            cols: Any = [f.name for f in df.schema.fields]
            res = pd.DataFrame([list(r) for r in self.rows(df)], columns=cols)
        except Exception:  # noqa: BLE001
            return None
        if ck:
            with self._lock:
                self._pandas_cache[ck] = res.copy()
        return res

    @staticmethod
    def quantiles(a: tuple[Any, ...], k: dict[str, Any]) -> Any:
        """Valor substituto de ``approxQuantile(col, probabilities, relativeError)``.

        Args:
            a: Argumentos posicionais originais.
            k: Argumentos nomeados originais.

        Returns:
            Lista de zeros (``col`` único) ou lista de listas (``col`` lista/tupla).
        """
        col = a[0] if a else k.get("col")
        probs = a[1] if len(a) > 1 else k.get("probabilities", [])
        row = [0.0] * len(probs)
        return [list(row) for _ in col] if isinstance(col, (list, tuple)) else row

    @staticmethod
    def arrow(df: Any) -> Any:
        """Devolve uma ``pyarrow.Table`` vazia com as colunas de ``df`` para ``toArrow()``.

        Args:
            df: DataFrame Spark.

        Returns:
            Tabela Arrow vazia, ou ``None`` se o pyarrow não estiver disponível.
        """
        try:
            import pyarrow as pa

            return pa.table({f.name: [] for f in df.schema.fields})
        except Exception:  # noqa: BLE001
            return None


class Patcher:
    """Gerencia monkey-patches temporários em classes do PySpark / Delta e sua restauração."""

    def __init__(self) -> None:
        """Inicializa o registro de atributos substituídos."""
        self.saved: list[tuple[type, str, bool, Any]] = []
        self.orig: dict[tuple[type, str], Any] = {}

    def patch(self, cls: type, name: str, factory: Callable[[Any], Any], create: bool = False) -> None:
        """Substitui o atributo ``name`` de ``cls`` por ``factory(original)``.

        Args:
            cls: Classe alvo.
            name: Nome do método/atributo.
            factory: Função que recebe o atributo original e devolve o substituto.
            create: Se ``True``, cria o atributo mesmo que ele não exista na classe.
        """
        has_attr = hasattr(cls, name)
        if not has_attr and not create:
            return
        original = getattr(cls, name, None)
        if has_attr:
            self.orig[(cls, name)] = original
        # descritor cru: restaurar o 'original' de getattr transformaria static/classmethod em função comum
        self.saved.append((cls, name, name in cls.__dict__, cls.__dict__.get(name)))
        setattr(cls, name, factory(original))

    def original(self, cls: type, name: str) -> Any:
        """Recupera a implementação original de ``name`` na hierarquia MRO de ``cls``.

        Args:
            cls: Classe do objeto.
            name: Nome do método.

        Returns:
            O método original antes do patch.
        """
        for k in cls.__mro__:
            if (k, name) in self.orig:
                return self.orig[(k, name)]
        return getattr(cls, name)

    def restore(self) -> None:
        """Reverte todos os patches aplicados na ordem inversa."""
        for cls, name, own, original in reversed(self.saved):
            with contextlib.suppress(Exception):
                if own:
                    setattr(cls, name, original)
                else:
                    delattr(cls, name)
        self.saved.clear()


def _import_class(mod: str, attr: str) -> type | None:
    try:
        return getattr(importlib.import_module(mod), attr)
    except Exception:  # noqa: BLE001
        return None


def _resolve(candidates: list[tuple[str, str]]) -> list[type]:
    seen: set[int] = set()
    out: list[type] = []
    for mod, attr in candidates:
        cls = _import_class(mod, attr)
        if cls is not None and id(cls) not in seen:
            seen.add(id(cls))
            out.append(cls)
    return out


def _both(attr: str, *modules: str) -> list[tuple[str, str]]:
    return [(m, attr) for m in modules]


DF_CLASSES = _resolve(
    _both("DataFrame", "pyspark.sql.classic.dataframe", "pyspark.sql.connect.dataframe", "pyspark.sql.dataframe")
)
WRITER_CLASSES = _resolve(
    _both(
        "DataFrameWriter",
        "pyspark.sql.classic.readwriter",
        "pyspark.sql.connect.readwriter",
        "pyspark.sql.readwriter",
    )
)
WRITER_V2_CLASSES = _resolve(
    _both(
        "DataFrameWriterV2",
        "pyspark.sql.classic.readwriter",
        "pyspark.sql.connect.readwriter",
        "pyspark.sql.readwriter",
    )
)
STREAM_WRITER_CLASSES = _resolve(
    _both(
        "DataStreamWriter",
        "pyspark.sql.classic.streaming",
        "pyspark.sql.connect.streaming.readwriter",
        "pyspark.sql.streaming.readwriter",
        "pyspark.sql.streaming",
    )
)
SESSION_CLASSES = _resolve(
    _both("SparkSession", "pyspark.sql.classic.session", "pyspark.sql.connect.session", "pyspark.sql.session")
)
READER_CLASSES = _resolve(
    _both(
        "DataFrameReader",
        "pyspark.sql.classic.readwriter",
        "pyspark.sql.connect.readwriter",
        "pyspark.sql.readwriter",
    )
)
STREAM_READER_CLASSES = _resolve(
    _both(
        "DataStreamReader",
        "pyspark.sql.classic.streaming",
        "pyspark.sql.connect.streaming.readwriter",
        "pyspark.sql.streaming.readwriter",
        "pyspark.sql.streaming",
    )
)
READER_PATH_METHODS = ["load", "parquet", "csv", "json", "orc", "text", "jdbc", "xml"]
MERGE_WRITER_CLASSES = _resolve(
    _both("MergeIntoWriter", "pyspark.sql.classic.merge", "pyspark.sql.connect.merge", "pyspark.sql.merge")
)
CATALOG_CLASSES = _resolve(
    _both("Catalog", "pyspark.sql.classic.catalog", "pyspark.sql.connect.catalog", "pyspark.sql.catalog")
)
CLIENT_CLASSES = _resolve([("pyspark.sql.connect.client.core", "SparkConnectClient")])

# Comandos do Spark Connect que podem passar durante a captura (não gravam dados nem alteram tabelas).
# Todo o resto (write_operation, merge_into_table_command, extension = comandos do Delta...) é bloqueado.
_ALLOWED_COMMANDS = frozenset(
    {
        "register_function",
        "register_table_function",
        "register_data_source",
        "create_dataframe_view",
        "sql_command",
        "get_resources_command",
        "create_resource_profile_command",
        "remove_cached_remote_relation_command",
        "streaming_query_command",
        "streaming_query_manager_command",
        "streaming_query_listener_bus_command",
    }
)

# ação -> função que devolve o valor substituto
DF_ACTIONS: dict[str, Callable[..., Any]] = {
    "show": lambda c, df, a, k: None,
    "display": lambda c, df, a, k: None,
    "collect": lambda c, df, a, k: c.rows(df),
    "count": lambda c, df, a, k: c.count_value,
    "isEmpty": lambda c, df, a, k: c.count_value == 0,
    "first": lambda c, df, a, k: c.first_row(df),
    "head": lambda c, df, a, k: (
        c.first_row(df)
        if (a[0] if a else k.get("n")) is None
        else c.rows(df)[: (a[0] if a else k["n"])]
    ),
    "take": lambda c, df, a, k: c.rows(df)[: (a[0] if a else k.get("num", 0))],
    "tail": lambda c, df, a, k: c.rows(df)[: (a[0] if a else k.get("num", 0))],
    "toPandas": lambda c, df, a, k: c.pandas(df),
    "toLocalIterator": lambda c, df, a, k: iter(c.rows(df)),
    "foreach": lambda c, df, a, k: None,
    "foreachPartition": lambda c, df, a, k: None,
    "checkpoint": lambda c, df, a, k: df,
    "localCheckpoint": lambda c, df, a, k: df,
    "approxQuantile": lambda c, df, a, k: c.quantiles(a, k),
    "corr": lambda c, df, a, k: 0.0,
    "cov": lambda c, df, a, k: 0.0,
    "toArrow": lambda c, df, a, k: c.arrow(df),
}

WRITER_METHODS = ["save", "saveAsTable", "insertInto", "parquet", "csv", "json", "orc", "text", "jdbc", "xml"]
WRITER_V2_METHODS = ["create", "replace", "createOrReplace", "append", "overwrite", "overwritePartitions"]
STREAM_WRITER_METHODS = ["start", "toTable"]


def _make_df_action_factory(cap: Capturer, name: str) -> Callable[[Any], Any]:
    def factory(_original: Any) -> Callable[..., Any]:
        def wrapper(df: Any, *a: Any, **kw: Any) -> Any:
            cap.capture(df, name, a, kw)
            return DF_ACTIONS[name](cap, df, a, kw)

        wrapper.__name__ = name
        return wrapper

    return factory


class _StorageLevelNone:
    useDisk = False
    useMemory = False
    useOffHeap = False
    deserialized = False
    replication = 1

    def __bool__(self) -> bool:
        return False


def _storage_level_none() -> Any:
    try:
        from pyspark import StorageLevel

        return StorageLevel.NONE
    except Exception:  # noqa: BLE001
        return _StorageLevelNone()


def _make_df_noop_self_factory(name: str) -> Callable[[Any], Any]:
    def factory(_original: Any) -> Callable[..., Any]:
        def wrapper(df: Any, *_a: Any, **_kw: Any) -> Any:
            return df

        wrapper.__name__ = name
        return wrapper

    return factory


def _make_create_temp_view_factory(replace_method: str = "createOrReplaceTempView") -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def factory(original: Callable[..., Any]) -> Callable[..., Any]:
        def create_temp_view(df: Any, *a: Any, **kw: Any) -> Any:
            if hasattr(df, replace_method):
                return getattr(df, replace_method)(*a, **kw)
            return original(df, *a, **kw)

        return create_temp_view

    return factory


def _patch_dataframes(p: Patcher, cap: Capturer) -> None:
    for cls in DF_CLASSES:
        for name in DF_ACTIONS:
            p.patch(cls, name, _make_df_action_factory(cap, name), create=(name == "display"))
        for name in ("cache", "persist", "unpersist"):
            p.patch(cls, name, _make_df_noop_self_factory(name))
        p.patch(cls, "is_cached", lambda _orig: property(lambda _self: False))
        p.patch(cls, "storageLevel", lambda _orig: property(lambda _self: _storage_level_none()))
        p.patch(cls, "createTempView", _make_create_temp_view_factory("createOrReplaceTempView"))
        p.patch(cls, "createGlobalTempView", _make_create_temp_view_factory("createOrReplaceGlobalTempView"))


def _writer_option_target(writer: Any) -> str | None:
    """Extrai ``path``/``dbtable``/``table`` das opções acumuladas no writer (Classic ou Spark Connect)."""
    opt_sources = [getattr(writer, "_options", None)]
    for attr in ("_write", "_write_proto"):
        op = getattr(writer, attr, None)
        if op is not None:
            opt_sources.append(getattr(op, "options", None))
    for opts in opt_sources:
        if opts is None or not hasattr(opts, "items"):
            continue
        if hasattr(opts, "get"):
            for k in ("path", "dbtable", "table", "tableName", "name"):
                val = opts.get(k)
                if isinstance(val, str) and val.strip():
                    return _normalize_target(val)
        for k, val in opts.items():
            if isinstance(k, str) and k.lower() in ("path", "dbtable", "table", "tablename") and isinstance(val, str):
                if val.strip():
                    return _normalize_target(val)
    return None


def _make_writer_factory(
    cap: Capturer,
    name: str,
    ret: Callable[[], Any],
    v2: bool = False,
    prefix: str = "write",
) -> Callable[[Any], Any]:
    def factory(_original: Any) -> Callable[..., Any]:
        def wrapper(writer: Any, *a: Any, **kw: Any) -> Any:
            df = _writer_source(writer)
            target = (
                _writer_table(writer)
                if v2
                else (cap._target(a, kw, method=name) or _writer_option_target(writer))
            )
            if df is None:
                cap.warn(f"{prefix}.{name}: não encontrei o DataFrame de origem no writer")
            else:
                df_obj = df
                with contextlib.suppress(Exception):
                    df_obj = _as_dataframe(df, writer)
                if target and (v2 or name in ("saveAsTable", "insertInto", "toTable")):
                    cap.record_written_table(target, df_obj)
                elif target and name in ("save", "csv", "parquet", "json", "orc", "text", "xml", "start"):
                    cap.record_written_path(target, df_obj)
                fb = (
                    getattr(writer, "_spark_lineage_foreach_batch", None)
                    or getattr(writer, "_foreach_batch_fn", None)
                    if prefix == "writeStream" and name == "start"
                    else None
                )
                if callable(fb):
                    try:
                        fb(df_obj, 0)
                    except Exception as exc:  # noqa: BLE001
                        cap.warn(f"writeStream.foreachBatch: erro ao executar callback: {exc!r}")
                else:
                    cap.capture(df, f"{prefix}.{name}", a, kw, target=target, owner=writer)
            return ret()

        wrapper.__name__ = name
        return wrapper

    return factory


def _make_foreach_batch_factory() -> Callable[[Any], Any]:
    def factory(_original: Any) -> Callable[..., Any]:
        def foreach_batch(writer: Any, func: Any) -> Any:
            writer._spark_lineage_foreach_batch = func
            return writer

        return foreach_batch

    return factory


def _patch_writers(p: Patcher, cap: Capturer) -> None:
    for cls in WRITER_CLASSES:
        for name in WRITER_METHODS:
            p.patch(cls, name, _make_writer_factory(cap, name, lambda: None))
    for cls in WRITER_V2_CLASSES:
        for name in WRITER_V2_METHODS:
            p.patch(cls, name, _make_writer_factory(cap, name, lambda: None, v2=True))
    for cls in STREAM_WRITER_CLASSES:
        p.patch(cls, "foreachBatch", _make_foreach_batch_factory())
        for name in STREAM_WRITER_METHODS:
            p.patch(cls, name, _make_writer_factory(cap, name, _Stub, prefix="writeStream"))


def _explain_sql_dml(
    p: Patcher,
    cap: Capturer,
    exec_explain: Callable[[str], Any],
    query: str,
    kind: str,
) -> None:
    from .plan_parser import parse_insert_target_columns

    for tbl in extract_sql_sources(query):
        cap.record_read(tbl)
    try:
        df = exec_explain("EXPLAIN EXTENDED " + query)
        rows = p.original(type(df), "collect")(df)
        text = "\n".join(str(r[0]) for r in rows)
        sections = split_sections(text)
        base = sections.get("analyzed") or sections.get("raw") or ""
        if _plan_hash(base) not in cap.plans:
            with contextlib.suppress(Exception):
                df_fmt = exec_explain("EXPLAIN FORMATTED " + query)
                rows_fmt = p.original(type(df_fmt), "collect")(df_fmt)
                fmt_txt = "\n".join(str(r[0]) for r in rows_fmt).strip()
                if fmt_txt:
                    sections["formatted"] = fmt_txt
        targets = extract_sql_targets(query)
        columns = parse_insert_target_columns(base)
        site = cap.call_site()
        extra_meta = {"query": query[:2000]}
        for target in targets or [None]:
            cap.add_plan(sections, f"sql:{kind}", target, columns, site, extra_meta)
    except Exception as exc:  # noqa: BLE001
        cap.event("sql_not_explained", statement_kind=kind, query=query[:500], error=repr(exc))


def _coerce_str_list(val: Any) -> list[str]:
    if isinstance(val, str):
        return [val]
    if isinstance(val, (list, tuple)):
        return [x for x in val if isinstance(x, str)]
    return []


def _reader_targets(reader: Any, a: tuple[Any, ...], kw: dict[str, Any], method: str = "") -> list[str]:
    if method == "jdbc":
        first = kw.get("table") or (a[1] if len(a) >= 2 else None)
    else:
        first = a[0] if a else kw.get("path") or kw.get("paths") or kw.get("table")
    targets = _coerce_str_list(first)
    if targets:
        return targets
    opts = getattr(reader, "_options", None) or {}
    if hasattr(opts, "items"):
        found: list[str] = []
        for k, val in opts.items():
            if isinstance(k, str) and k.lower() in ("path", "dbtable", "table", "tablename") and isinstance(val, str):
                if val.strip() and val not in found:
                    found.append(val)
        return found
    return []


def _rewrite_safe_temp_view_sql(query: str, kind: str) -> str:
    clean = _strip_sql_comments(query)
    if kind == "CACHE_TABLE_AS":
        m = _CACHE_TABLE_AS_RE.match(clean)
        if m:
            return f"CREATE OR REPLACE TEMP VIEW {m.group('name')} AS {m.group('body')}"
    if kind == "CREATE_TEMP_VIEW":
        rewritten = _CREATE_TEMP_VIEW_NO_REPLACE_RE.sub(r"\1OR REPLACE \2", clean, count=1)
        if rewritten != clean:
            return rewritten
    return query


def _make_sql_factory(p: Patcher, cap: Capturer) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def sql_factory(original: Callable[..., Any]) -> Callable[..., Any]:
        def sql(session: Any, *a: Any, **kw: Any) -> Any:
            query = str(a[0] if a else kw.get("sqlQuery", ""))
            mode, kind = classify_sql(query)
            if mode == "pass":
                for tbl in extract_sql_sources(query):
                    cap.record_read(tbl)
                rewritten = _rewrite_safe_temp_view_sql(query, kind)
                if rewritten != query:
                    rest_a = a[1:]
                    rest_kw = {k: v for k, v in kw.items() if k != "sqlQuery"}
                    return original(session, rewritten, *rest_a, **rest_kw)
                return original(session, *a, **kw)
            if not cap.explorer.has_flipped:
                return original(session, "SELECT 1 AS _blocked WHERE 1=0")
            if mode == "explain":
                rest_a = a[1:]
                rest_kw = {k: v for k, v in kw.items() if k != "sqlQuery"}
                _explain_sql_dml(p, cap, lambda q: original(session, q, *rest_a, **rest_kw), query, kind)
            else:
                cap.event("sql_blocked", statement_kind=kind, query=query[:500])
            return original(session, "SELECT 1 AS _blocked WHERE 1=0")

        return sql

    return sql_factory


def _make_table_read_factory(cap: Capturer) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def table_read_factory(original: Callable[..., Any]) -> Callable[..., Any]:
        def table(obj: Any, table_name: Any, *a: Any, **kw: Any) -> Any:
            if isinstance(table_name, str):
                cap.record_read(table_name)
                written_df = cap.get_written_table(table_name)
                if written_df is not None:
                    return written_df
            return original(obj, table_name, *a, **kw)

        return table

    return table_read_factory


_DF_KNOWN_ATTRS = frozenset(
    {
        "select",
        "selectExpr",
        "filter",
        "where",
        "withColumn",
        "withColumns",
        "withColumnRenamed",
        "withColumnsRenamed",
        "drop",
        "dropDuplicates",
        "drop_duplicates",
        "distinct",
        "join",
        "crossJoin",
        "union",
        "unionAll",
        "unionByName",
        "intersect",
        "intersectAll",
        "subtract",
        "exceptAll",
        "groupBy",
        "groupby",
        "rollup",
        "cube",
        "agg",
        "orderBy",
        "sort",
        "sortWithinPartitions",
        "limit",
        "offset",
        "alias",
        "repartition",
        "coalesce",
        "fillna",
        "dropna",
        "na",
        "stat",
        "write",
        "writeTo",
        "writeStream",
        "schema",
        "dtypes",
        "columns",
        "cache",
        "persist",
        "unpersist",
        "is_cached",
        "storageLevel",
        "checkpoint",
        "localCheckpoint",
        "createTempView",
        "createOrReplaceTempView",
        "createGlobalTempView",
        "createOrReplaceGlobalTempView",
        "collect",
        "count",
        "isEmpty",
        "first",
        "head",
        "take",
        "tail",
        "show",
        "display",
        "toPandas",
        "toLocalIterator",
        "toArrow",
        "toDF",
        "explain",
        "printSchema",
        "describe",
        "summary",
        "foreach",
        "foreachPartition",
        "sample",
        "sampleBy",
        "hint",
        "transform",
        "melt",
        "unpivot",
        "transpose",
        "mergeInto",
    }
)
_COL_LIST_METHODS = frozenset(
    {"select", "drop", "groupBy", "groupby", "orderBy", "sort", "dropDuplicates", "drop_duplicates"}
)
_VALID_COL_RE = re.compile(r"^[A-Za-z_][\w\-.]*$")


def _is_valid_inferred_col(name: str) -> bool:
    s = name.strip()
    if not s or s == "*" or "/" in s or "\\" in s or " " in s:
        return False
    return bool(_VALID_COL_RE.match(s))


def _extract_str_constants(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        out: list[str] = []
        for elt in node.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                out.append(elt.value)
        return out
    return []


def _extract_col_calls(node: ast.AST) -> list[str]:
    out: list[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            tail = _dotted_name(child.func).rsplit(".", 1)[-1]
            if tail in ("col", "column") and child.args:
                out.extend(_extract_str_constants(child.args[0]))
    return out


def _get_parsed_file_ast(cap: Capturer, rel_file: str) -> ast.Module | None:
    abspath = str((cap.root / rel_file).resolve())
    with cap._lock:
        if abspath in cap._ast_cache:
            return cap._ast_cache[abspath]
    tree: ast.Module | None = None
    try:
        raw_src = Path(abspath).read_text(encoding="utf-8")
        pre = preprocess_notebook_source(raw_src, abspath)
        tree = ast.parse(pre, filename=abspath)
    except Exception:  # noqa: BLE001
        tree = None
    with cap._lock:
        cap._ast_cache[abspath] = tree
    return tree


def _enclosing_stmt_list(tree: ast.Module, target_line: int) -> list[ast.stmt]:
    best: list[ast.stmt] = tree.body
    best_span = 10**9
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            start = getattr(node, "lineno", 0)
            end = getattr(node, "end_lineno", start) or start
            if start <= target_line <= end and (end - start) < best_span:
                best = node.body
                best_span = end - start
    return best


def _extract_sql_string_literal(node: ast.AST) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return ""


def _infer_sql_view_columns(stmts: list[ast.stmt], view_names: set[str]) -> list[str]:
    if not view_names:
        return []
    cols: list[str] = []
    for stmt in stmts:
        for child in ast.walk(stmt):
            if not isinstance(child, ast.Call):
                continue
            if _dotted_name(child.func).rsplit(".", 1)[-1] != "sql" or not child.args:
                continue
            sql_txt = _extract_sql_string_literal(child.args[0])
            if not sql_txt:
                continue
            for vw in view_names:
                m_al = re.search(rf"\b{re.escape(vw)}\b(?:\s+AS)?\s+([A-Za-z_]\w*)\b", sql_txt, re.I)
                aliases = {vw}
                if m_al and m_al.group(1).upper() not in ("ON", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL"):
                    aliases.add(m_al.group(1))
                for al in aliases:
                    for m_col in re.finditer(rf"\b{re.escape(al)}\.(`[^`]+`|[A-Za-z_]\w*)", sql_txt):
                        cname = m_col.group(1).strip("`")
                        if _is_valid_inferred_col(cname) and cname not in cols:
                            cols.append(cname)
    return cols


def _infer_reader_columns_from_ast(cap: Capturer, site: Json | None) -> list[str]:
    """Infere nomes de colunas esperadas de um reader que falhou analisando os usos da variável na AST."""
    if not site or not site.get("file") or not site.get("line"):
        return []
    tree = _get_parsed_file_ast(cap, str(site["file"]))
    if tree is None:
        return []

    target_line = int(site["line"])
    stmts = _enclosing_stmt_list(tree, target_line)
    tracked_vars: set[str] = set()
    read_stmt_idx = 0
    for idx, stmt in enumerate(stmts):
        start = getattr(stmt, "lineno", 0)
        end = getattr(stmt, "end_lineno", start) or start
        if start <= target_line <= end:
            read_stmt_idx = idx
            if isinstance(stmt, ast.Assign):
                for tgt in stmt.targets:
                    if isinstance(tgt, ast.Name):
                        tracked_vars.add(tgt.id)
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                tracked_vars.add(stmt.target.id)
            break

    cols: list[str] = []

    def add_col(raw: str) -> None:
        c = raw.strip().strip("`")
        if _is_valid_inferred_col(c) and c not in cols:
            cols.append(c)

    def expr_uses_tracked(expr: ast.AST) -> bool:
        return any(isinstance(n, ast.Name) and n.id in tracked_vars for n in ast.walk(expr))

    def collect_from_call(call: ast.Call, is_target_receiver: bool) -> None:
        if not isinstance(call.func, ast.Attribute):
            return
        mname = call.func.attr
        if is_target_receiver:
            if mname in _COL_LIST_METHODS:
                for arg in call.args:
                    for c in _extract_str_constants(arg):
                        add_col(c)
                    for c in _extract_col_calls(arg):
                        add_col(c)
            elif mname == "withColumnRenamed" and call.args:
                for c in _extract_str_constants(call.args[0]):
                    add_col(c)
            elif mname in ("filter", "where", "withColumn", "withColumns", "agg"):
                for arg in call.args:
                    for c in _extract_col_calls(arg):
                        add_col(c)
                for kw_arg in call.keywords:
                    for c in _extract_col_calls(kw_arg.value):
                        add_col(c)
        if mname == "join":
            other_is_tracked = bool(call.args and expr_uses_tracked(call.args[0]))
            if is_target_receiver or other_is_tracked:
                on_node = next((k.value for k in call.keywords if k.arg == "on"), None)
                if on_node is None and len(call.args) >= 2:
                    on_node = call.args[1]
                if on_node is not None:
                    for c in _extract_str_constants(on_node):
                        add_col(c)
                    for c in _extract_col_calls(on_node):
                        add_col(c)

    relevant_stmts = stmts[read_stmt_idx:]
    view_names: set[str] = set()

    # Se não houve atribuição (chamada encadeada `spark.read.csv(...).select(...)`), inspeciona o próprio statement.
    if not tracked_vars and relevant_stmts:
        for child in ast.walk(relevant_stmts[0]):
            if isinstance(child, ast.Call):
                collect_from_call(child, is_target_receiver=True)
        return cols

    for stmt in relevant_stmts:
        if isinstance(stmt, ast.Assign) and expr_uses_tracked(stmt.value):
            for tgt in stmt.targets:
                if isinstance(tgt, ast.Name):
                    tracked_vars.add(tgt.id)
        for child in ast.walk(stmt):
            if isinstance(child, ast.Subscript) and isinstance(child.value, ast.Name) and child.value.id in tracked_vars:
                for c in _extract_str_constants(child.slice):
                    add_col(c)
            elif isinstance(child, ast.Attribute) and isinstance(child.value, ast.Name) and child.value.id in tracked_vars:
                if child.attr not in _DF_KNOWN_ATTRS and not child.attr.startswith("_"):
                    add_col(child.attr)
            elif isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                is_recv = expr_uses_tracked(child.func.value)
                collect_from_call(child, is_target_receiver=is_recv)
                if is_recv and child.func.attr in ("createOrReplaceTempView", "createTempView") and child.args:
                    view_names.update(_extract_str_constants(child.args[0]))

    for c in _infer_sql_view_columns(relevant_stmts, view_names):
        add_col(c)
    return cols


def _reader_spark_session(reader: Any) -> Any | None:
    for attr in ("_spark", "_session", "sparkSession"):
        sess = getattr(reader, attr, None)
        if sess is not None and hasattr(sess, "sql"):
            return sess
    with contextlib.suppress(Exception):
        return get_spark()
    return None


def _reader_explicit_schema(reader: Any, kw: dict[str, Any]) -> Any | None:
    for cand in (kw.get("schema"), getattr(reader, "_spark_lineage_schema", None), getattr(reader, "_schema", None)):
        if cand is not None:
            return cand
    return None


def _schema_to_sql_cols(schema: Any) -> list[tuple[str, str]]:
    if hasattr(schema, "fields"):
        return [(f.name, f.dataType.simpleString()) for f in schema.fields]
    if isinstance(schema, str):
        out: list[tuple[str, str]] = []
        for part in schema.split(","):
            toks = part.strip().split(None, 1)
            if len(toks) == 2:
                out.append((toks[0].strip("`"), toks[1].strip()))
        return out
    return []


def _build_synthetic_reader_df(spark: Any, explicit_schema: Any | None, inferred_cols: list[str]) -> tuple[Any, list[str]]:
    if explicit_schema is not None:
        schema_cols = _schema_to_sql_cols(explicit_schema)
        if hasattr(spark, "createDataFrame"):
            with contextlib.suppress(Exception):
                return spark.createDataFrame([], explicit_schema), [c for c, _ in schema_cols]
        if schema_cols:
            proj = ", ".join(f"CAST(NULL AS {typ}) AS `{name.replace('`', '')}`" for name, typ in schema_cols)
            return spark.sql(f"SELECT {proj} WHERE 1=0"), [c for c, _ in schema_cols]
    col_names = inferred_cols or ["_c0"]
    proj = ", ".join(f"CAST(NULL AS STRING) AS `{c.replace('`', '')}`" for c in col_names)
    return spark.sql(f"SELECT {proj} WHERE 1=0"), col_names


def _make_reader_schema_factory() -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def factory(original: Callable[..., Any]) -> Callable[..., Any]:
        def schema(reader: Any, schema_arg: Any, *a: Any, **kw: Any) -> Any:
            reader._spark_lineage_schema = schema_arg
            return original(reader, schema_arg, *a, **kw)

        return schema

    return factory


def _make_reader_path_factory(cap: Capturer, method: str = "") -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def reader_path_factory(original: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(reader: Any, *a: Any, **kw: Any) -> Any:
            targets = _reader_targets(reader, a, kw, method=method)
            for t in targets:
                cap.record_read(t)
            for t in targets:
                written_df = cap.get_written_path(t)
                if written_df is not None:
                    return written_df
            try:
                return original(reader, *a, **kw)
            except Exception as exc:  # noqa: BLE001
                spark = _reader_spark_session(reader)
                if spark is None:
                    raise
                explicit_schema = _reader_explicit_schema(reader, kw)
                inferred_cols = [] if explicit_schema is not None else _infer_reader_columns_from_ast(cap, cap.call_site())
                try:
                    synth_df, used_cols = _build_synthetic_reader_df(spark, explicit_schema, inferred_cols)
                except Exception:  # noqa: BLE001
                    raise exc from None
                target_label = targets[0] if targets else "<path>"
                cap.event(
                    "reader_fallback",
                    name=f"read.{method}",
                    target=target_label,
                    columns=used_cols,
                    error=repr(exc)[:300],
                )
                cap.warn(
                    f"read.{method}({target_label!r}) falhou ({type(exc).__name__}: {exc}); "
                    f"usando DataFrame sintético vazio com colunas {used_cols}"
                )
                return synth_df

        return wrapper

    return reader_path_factory


def _patch_sql_and_readers(p: Patcher, cap: Capturer) -> None:
    sql_factory = _make_sql_factory(p, cap)
    table_read_factory = _make_table_read_factory(cap)
    reader_schema_factory = _make_reader_schema_factory()

    for cls in SESSION_CLASSES:
        p.patch(cls, "sql", sql_factory)
        p.patch(cls, "table", table_read_factory)
        p.patch(cls, "stop", _make_blocked_factory(cap, "session_stop_blocked", "SparkSession.stop", lambda: None))
    for cls in (*READER_CLASSES, *STREAM_READER_CLASSES):
        p.patch(cls, "table", table_read_factory)
        p.patch(cls, "schema", reader_schema_factory)
        for mname in READER_PATH_METHODS:
            p.patch(cls, mname, _make_reader_path_factory(cap, method=mname))


def _delta_table_target(table: Any) -> str | None:
    """Extrai nome da tabela ou caminho de uma instância ``DeltaTable`` (Classic ou Spark Connect)."""
    for obj in (table, getattr(table, "_dt", None)):
        if obj is None:
            continue
        for attr in ("_tableOrViewName", "_table_or_view_name", "_table_name", "_path", "table_name", "path"):
            val = getattr(obj, attr, None)
            if isinstance(val, str) and val.strip():
                return _normalize_target(val)
    return None


def _make_delta_merge_factory(cap: Capturer) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def merge_factory(original: Callable[..., Any]) -> Callable[..., Any]:
        def merge(table: Any, source: Any, condition: Any, *a: Any, **kw: Any) -> Any:
            cap.capture(
                source,
                "delta.merge(source)",
                target=_delta_table_target(table),
                extra={"condition": str(condition)[:500]},
            )
            return original(table, source, condition, *a, **kw)

        return merge

    return merge_factory


def _make_delta_event_factory(cap: Capturer, label: str) -> Callable[[Any], Any]:
    def factory(_original: Any) -> Callable[..., Any]:
        def wrapper(_obj: Any, *a: Any, **_kw: Any) -> Any:
            cap.event("delta_operation", name=label, args=repr(a)[:500])
            return _Stub()  # clone()/vacuum()/builders devolvem objetos que o job pode encadear

        return wrapper

    return factory


# No Delta Connect, estas operações vão direto para execute_command (não passam por toPandas)
# e EXECUTARIAM de verdade se não fossem substituídas.
_DELTA_TABLE_OPS = (
    "delete",
    "update",
    "vacuum",
    "restoreToVersion",
    "restoreToTimestamp",
    "convertToDelta",
    "generate",
    "upgradeTableProtocol",
    "addFeatureSupport",
    "dropFeatureSupport",
    "clone",
    "cloneAtVersion",
    "cloneAtTimestamp",
)


def _patch_delta(p: Patcher, cap: Capturer) -> None:
    # Delta (melhor esforço): captura o plano da origem do MERGE e bloqueia operações destrutivas.
    # Cobre as classes clássicas (delta.tables) e as do Delta Connect (delta.connect.tables).
    for modname in ("delta.tables", "delta.connect.tables"):
        try:
            dtm = importlib.import_module(modname)
        except Exception:  # noqa: BLE001
            continue
        if hasattr(dtm, "DeltaTable"):
            p.patch(dtm.DeltaTable, "merge", _make_delta_merge_factory(cap))
            for n in _DELTA_TABLE_OPS:
                p.patch(dtm.DeltaTable, n, _make_delta_event_factory(cap, f"DeltaTable.{n}"))
        if hasattr(dtm, "DeltaMergeBuilder"):
            p.patch(dtm.DeltaMergeBuilder, "execute", _make_delta_event_factory(cap, "DeltaMergeBuilder.execute"))
        if hasattr(dtm, "DeltaTableBuilder"):  # create / createIfNotExists / replace / createOrReplace
            p.patch(dtm.DeltaTableBuilder, "execute", _make_delta_event_factory(cap, "DeltaTableBuilder.execute"))
        if hasattr(dtm, "DeltaOptimizeBuilder"):
            for n in ("executeCompaction", "executeZOrderBy"):
                p.patch(dtm.DeltaOptimizeBuilder, n, _make_delta_event_factory(cap, f"DeltaOptimizeBuilder.{n}"))


def _make_merge_into_factory(cap: Capturer) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def factory(original: Callable[..., Any]) -> Callable[..., Any]:
        def merge_into(df: Any, table: Any, condition: Any, *a: Any, **kw: Any) -> Any:
            cap.capture(
                df,
                "mergeInto(source)",
                target=_normalize_target(table) if isinstance(table, str) else None,
                extra={"condition": str(condition)[:500]},
            )
            return original(df, table, condition, *a, **kw)

        return merge_into

    return factory


def _make_blocked_factory(
    cap: Capturer, kind: str, label: str, ret: Callable[[], Any] = _Stub
) -> Callable[[Any], Callable[..., Any]]:
    def factory(_original: Any) -> Callable[..., Any]:
        def wrapper(_obj: Any, *a: Any, **_kw: Any) -> Any:
            cap.event(kind, name=label, args=repr(a)[:500])
            return ret()

        return wrapper

    return factory


def _make_table_exists_factory(cap: Capturer) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def factory(original: Callable[..., Any]) -> Callable[..., Any]:
        def table_exists(catalog_obj: Any, *a: Any, **kw: Any) -> bool:
            table_name = a[0] if a else kw.get("tableName")
            db_name = a[1] if len(a) >= 2 else kw.get("dbName")
            if not isinstance(table_name, str):
                return False
            full_name = f"{db_name}.{table_name}" if isinstance(db_name, str) and db_name.strip() else table_name
            norm = _normalize_target(full_name)
            if not norm or norm.startswith(".") or norm.endswith(".") or ".." in norm:
                cap.warn(f"catalog.tableExists({full_name!r}): nome de tabela malformado, assumindo False")
                return False
            key = norm.lower()
            if key in cap.written_tables:
                return True
            with cap._lock:
                if key in cap._table_exists_cache:
                    return cap._table_exists_cache[key]
            try:
                res = bool(original(catalog_obj, full_name))
            except Exception as exc:  # noqa: BLE001
                cap.warn(f"catalog.tableExists({full_name!r}) falhou: {exc!r}")
                res = False
            with cap._lock:
                cap._table_exists_cache[key] = res
            return res

        return table_exists

    return factory


def _patch_merge_and_catalog(p: Patcher, cap: Capturer) -> None:
    # df.mergeInto(...) (Spark 4): captura o plano da origem; o .merge() executaria o comando, então é bloqueado.
    for cls in DF_CLASSES:
        p.patch(cls, "mergeInto", _make_merge_into_factory(cap))
    for cls in MERGE_WRITER_CLASSES:
        p.patch(cls, "merge", _make_blocked_factory(cap, "merge_into_blocked", "MergeIntoWriter.merge", lambda: None))
    # DDL e operações de cache pela API de catálogo.
    for cls in CATALOG_CLASSES:
        for n in ("createTable", "createExternalTable", "recoverPartitions"):
            p.patch(cls, n, _make_blocked_factory(cap, "catalog_blocked", f"Catalog.{n}"))
        for n in ("cacheTable", "uncacheTable", "clearCache", "refreshTable", "refreshByPath", "dropGlobalTempView"):
            p.patch(cls, n, _make_blocked_factory(cap, "catalog_blocked", f"Catalog.{n}", lambda: None))
        p.patch(cls, "isCached", _make_blocked_factory(cap, "catalog_blocked", "Catalog.isCached", lambda: False))
        p.patch(cls, "tableExists", _make_table_exists_factory(cap))


def _empty_command_result(original: Callable[..., Any]) -> tuple[Any, ...]:
    """Retorno vazio compatível com a assinatura de ``execute_command`` da versão instalada."""
    try:
        ann = str(inspect.signature(original).return_annotation)
    except (TypeError, ValueError):
        ann = ""
    # Spark 4 devolve (dados, propriedades, ExecutionInfo); versões anteriores, (dados, propriedades).
    return (None, {}, None) if "ExecutionInfo" in ann else (None, {})


def _make_command_guard_factory(cap: Capturer) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def factory(original: Callable[..., Any]) -> Callable[..., Any]:
        empty = _empty_command_result(original)

        def execute_command(client: Any, command: Any, *a: Any, **kw: Any) -> Any:
            try:
                kind = command.WhichOneof("command_type")
            except Exception:  # noqa: BLE001
                kind = None
            if kind in _ALLOWED_COMMANDS:
                return original(client, command, *a, **kw)
            cap.event("blocked_command", name=str(kind))
            return empty

        return execute_command

    return factory


def _patch_command_guard(p: Patcher, cap: Capturer) -> None:
    """Rede de segurança: ``execute_command`` é o ponto único por onde passa todo comando do Spark Connect.

    O que escapar dos wrappers específicos é bloqueado aqui e registrado em ``events`` (com arquivo:linha).
    """
    for cls in CLIENT_CLASSES:
        p.patch(cls, "execute_command", _make_command_guard_factory(cap))


def install_patches(cap: Capturer) -> Patcher:
    """Instala todos os monkey-patches de captura em DataFrame, Writers, SparkSession, Readers, Delta e catálogo.

    Inclui a trava de comandos do Spark Connect (``execute_command``) como rede de segurança.

    Args:
        cap: Instância do ``Capturer`` que receberá os planos e eventos.

    Returns:
        O ``Patcher`` com os métodos originais salvos (para ``restore()`` no ``finally``).
    """
    p = Patcher()
    _patch_dataframes(p, cap)
    _patch_writers(p, cap)
    _patch_sql_and_readers(p, cap)
    _patch_delta(p, cap)
    _patch_merge_and_catalog(p, cap)
    _patch_command_guard(p, cap)
    return p


# --------------------------------------------------------------------------- #
# Execução do entrypoint
# --------------------------------------------------------------------------- #


def get_spark() -> Any:
    """Obtém ou cria a sessão Spark (priorizando Databricks Connect sobre PySpark local).

    Returns:
        Sessão Spark ativa.

    Raises:
        RuntimeError: Se nem o Databricks Connect nem o PySpark local conseguirem iniciar sessão.
    """
    errors: list[str] = []
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except Exception as exc:  # noqa: BLE001
        errors.append(f"databricks.connect: {exc!r}")
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    except Exception as exc:  # noqa: BLE001
        errors.append(f"pyspark local: {exc!r}")
    else:
        print("[capture_plans] AVISO: usando Spark LOCAL, não o Databricks Connect", file=sys.stderr)
        return spark
    raise RuntimeError("Não consegui criar a sessão Spark:\n  " + "\n  ".join(errors))


_get_spark = get_spark


def _resolve_entrypoint(root: Path, entrypoint: str) -> Path:
    ep = Path(entrypoint)
    candidates = [ep] if ep.is_absolute() else [root / ep, Path.cwd() / ep]
    for c in candidates:
        if c.is_file():
            return c.resolve()
    raise FileNotFoundError(f"entrypoint não encontrado: {entrypoint} (raiz: {root})")


_NOTEBOOK_EXTS = ("", ".py", ".sql", ".ipynb")
_RUN_PARAM_RE = re.compile(r"\$([A-Za-z_]\w*)\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|(\S+))")


def _strip_inline_hash_comment(text: str) -> str:
    in_q: str | None = None
    for i, ch in enumerate(text):
        if in_q:
            if ch == in_q:
                in_q = None
        elif ch in ('"', "'"):
            in_q = ch
        elif ch == "#":
            return text[:i]
    return text


def _parse_magic_run_args(raw_arg: str) -> tuple[str, dict[str, str]]:
    """Separa o caminho do notebook e eventuais parâmetros ``$chave=valor`` de um ``%run``."""
    cleaned = _strip_inline_hash_comment(raw_arg)
    params: dict[str, str] = {}
    for m in _RUN_PARAM_RE.finditer(cleaned):
        val = m.group(2) if m.group(2) is not None else (m.group(3) if m.group(3) is not None else m.group(4) or "")
        params[m.group(1)] = val
    path_part = _RUN_PARAM_RE.sub("", cleaned).strip().strip("\"'")
    return path_part, params


def _resolve_notebook_target(raw_path: str, caller_file: str | None, root: Path) -> Path | None:
    """Localiza um notebook referenciado por ``%run`` ou ``dbutils.notebook.run`` dentro do projeto."""
    cleaned = raw_path.strip().strip("\"'")
    if not cleaned:
        return None
    base_dirs: list[Path] = []
    if caller_file:
        base_dirs.append(Path(caller_file).resolve().parent)
    base_dirs.append(root)
    rel_clean = cleaned.lstrip("/\\")
    for base in base_dirs:
        for ext in _NOTEBOOK_EXTS:
            cand = (base / f"{cleaned}{ext}") if not cleaned.startswith(("/", "\\")) else (base / f"{rel_clean}{ext}")
            if cand.is_file():
                return cand.resolve()
    return None


def _clear_project_modules(root_prefix: str, tool_file: str) -> None:
    # Remove de sys.modules os módulos do projeto para que reexecuções passem por código limpo.
    for mod_name, mod in list(sys.modules.items()):
        f = getattr(mod, "__file__", None)
        if f and _is_project_file(os.path.abspath(f), root_prefix, tool_file):
            sys.modules.pop(mod_name, None)


def _exec_entrypoint(ep: Path, finder: _BranchMetaPathFinder, init_globals: Json) -> None:
    code = finder.compile_file(str(ep))
    ns = {
        **init_globals,
        "__name__": "__main__",
        "__file__": str(ep),
        "__package__": None,
        "__cached__": None,
    }
    exec(code, ns)


def _read_table_schema(spark: Any, table: str) -> dict[str, Json] | None:
    try:
        fields = spark.table(table).schema.fields
    except Exception as exc:  # noqa: BLE001
        print(f"[capture_plans] AVISO: não consegui ler o schema de {table} no catálogo: {exc!r}", file=sys.stderr)
        return None
    return {
        f.name: {
            "type": f.dataType.simpleString(),
            "comment": (f.metadata or {}).get("comment"),
        }
        for f in fields
    }


def _fetch_catalog_for_plans(spark: Any, plans: list[Json]) -> dict[str, dict[str, Json] | None]:
    # Busca tipos e comentários de todas as tabelas referenciadas nos planos usando a sessão Spark já aberta.
    from .sources import discover_plan_tables

    return {t: _read_table_schema(spark, t) for t in discover_plan_tables(plans)}


_EXPLAIN_SPARK_CONFS = (
    ("spark.sql.maxMetadataStringLength", "100000"),
    ("spark.sql.debug.maxToStringFields", "1000"),
)


def _set_explain_conf(spark: Any, cap: Capturer, key: str, val: str) -> bool:
    # Tenta ajustar uma conf do Spark; devolve True se foi bloqueada por Serverless/Shared.
    try:
        spark.conf.set(key, val)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if "CONFIG_NOT_AVAILABLE" in msg or "42K0I" in msg:
            return True
        cap.warnings.append({"message": f"não consegui ajustar {key} ({exc!r}); os planos podem vir truncados"})
    return False


def _configure_spark_explain(spark: Any, cap: Capturer) -> None:
    # Amplia os limites de string do explain quando o compute permite.
    blocked_confs = [key for key, val in _EXPLAIN_SPARK_CONFS if _set_explain_conf(spark, cap, key, val)]
    if blocked_confs:
        aviso = (
            f"configurações {', '.join(blocked_confs)} não são permitidas neste compute "
            "(Databricks Serverless/Shared); usando `explain(mode='formatted')` + Unity Catalog "
            "para evitar truncamento de colunas e tipos."
        )
        print(f"[capture_plans] AVISO: {aviso}", file=sys.stderr)
        cap.warnings.append({"message": aviso})


def _run_first_pass(
    ep: Path,
    finder: _BranchMetaPathFinder,
    init_globals: Json,
    cap: Capturer,
) -> tuple[str, Json | None]:
    cap.explorer.start_pass(0)
    _clear_project_modules(cap.root_prefix, cap._tool_file)
    try:
        _exec_entrypoint(ep, finder, init_globals)
    except _JobExit:
        pass
    except SystemExit as exc:
        if exc.code not in (None, 0):
            return "error", {"type": "SystemExit", "message": str(exc.code)}
    except Exception as exc:  # noqa: BLE001
        return "error", {"type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()}
    return "ok", None


def _execute_forced_pass(
    ep: Path,
    finder: _BranchMetaPathFinder,
    init_globals: Json,
    cap: Capturer,
    branch_target: tuple[str, str, bool],
) -> None:
    site_id, cond_str, forced_val = branch_target
    try:
        _exec_entrypoint(ep, finder, init_globals)
        cap.explorer.mark_target_status("ok")
    except (_JobExit, SystemExit):
        cap.explorer.mark_target_status("ok")
    except Exception as exc:  # noqa: BLE001
        cap.explorer.mark_target_status(f"error: {type(exc).__name__}: {exc}")
        m_site = re.match(r"^(.*?):(\d+)(?::\d+)?$", site_id)
        file_part, line_num = (m_site.group(1), int(m_site.group(2))) if m_site else (site_id, 0)
        with cap._lock:
            cap.warnings.append(
                {
                    "call_site": {"file": file_part, "line": line_num},
                    "message": f"ramo forçado {site_id} ({cond_str} -> {forced_val}) "
                    f"interrompido com {type(exc).__name__}: {exc}",
                }
            )


def _run_branch_passes(  # noqa: PLR0913, PLR0917
    ep: Path,
    args: list[str] | None,
    finder: _BranchMetaPathFinder,
    init_globals: Json,
    cap: Capturer,
    max_branch_passes: int,
    branch_workers: int = 4,
) -> None:
    if max_branch_passes <= 0:
        return

    workers = max(1, branch_workers)
    if workers == 1:
        for extra_pass in range(1, max_branch_passes + 1):
            nxt = cap.explorer.next_unvisited()
            if nxt is None:
                break
            site_id, forced_val, cond_str, planned_forces = nxt
            print(
                f"[capture_plans] explorando ramo adicional ({extra_pass}): "
                f"{site_id} -> if ({cond_str}) = {forced_val}",
                file=sys.stderr,
            )
            cap.explorer.start_pass(
                extra_pass, target_site=site_id, target_value=forced_val, planned_forces=planned_forces
            )
            cap.written_tables.clear()
            cap.written_paths.clear()
            dbutils_obj = init_globals.get("dbutils")
            if dbutils_obj is not None and hasattr(dbutils_obj, "widgets"):
                dbutils_obj.widgets.reset_thread_state(cap.params)
            _clear_project_modules(cap.root_prefix, cap._tool_file)
            sys.argv = [str(ep), *(args or [])]
            _execute_forced_pass(ep, finder, init_globals, cap, (site_id, cond_str, forced_val))
        return

    sys.argv = [str(ep), *(args or [])]

    def _run_worker(
        pass_idx: int,
        site_id: str,
        forced_val: bool,
        cond_str: str,
        planned_forces: dict[str, bool],
    ) -> None:
        cap.explorer.start_pass(pass_idx, target_site=site_id, target_value=forced_val, planned_forces=planned_forces)
        cap.written_tables.clear()
        cap.written_paths.clear()
        dbutils_obj = init_globals.get("dbutils")
        if dbutils_obj is not None and hasattr(dbutils_obj, "widgets"):
            dbutils_obj.widgets.reset_thread_state(cap.params)
        _execute_forced_pass(ep, finder, init_globals, cap, (site_id, cond_str, forced_val))

    dispatched = 0
    in_flight: set[concurrent.futures.Future[None]] = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        while True:
            capacity = min(workers - len(in_flight), max_branch_passes - dispatched)
            if capacity > 0:
                batch = cap.explorer.next_unvisited_batch(capacity)
                for site_id, forced_val, cond_str, planned_forces in batch:
                    dispatched += 1
                    print(
                        f"[capture_plans] explorando ramo adicional ({dispatched}): "
                        f"{site_id} -> if ({cond_str}) = {forced_val}",
                        file=sys.stderr,
                    )
                    in_flight.add(pool.submit(_run_worker, dispatched, site_id, forced_val, cond_str, planned_forces))
            if not in_flight:
                break
            done, in_flight = concurrent.futures.wait(in_flight, return_when=concurrent.futures.FIRST_COMPLETED)
            for fut in done:
                fut.result()


def _restore_builtins(old_builtins: dict[str, Any]) -> None:
    for n, v in old_builtins.items():
        if v is None:
            if hasattr(builtins, n):
                delattr(builtins, n)
        else:
            setattr(builtins, n, v)


def _maybe_fetch_catalog(
    spark: Any,
    cap: Capturer,
    plans_list: list[Json],
    fetch_catalog: bool,
) -> dict[str, dict[str, Json] | None]:
    if not (fetch_catalog and plans_list):
        return {}
    try:
        active_spark = spark or get_spark()
        return _fetch_catalog_for_plans(active_spark, plans_list)
    except Exception as exc:  # noqa: BLE001
        cap.warnings.append({"message": f"não consegui consultar o Unity Catalog: {exc!r}"})
        return {}


def capture_plans(  # noqa: PLR0913, PLR0917
    project_root: str,
    entrypoint: str,
    args: list[str] | None = None,
    params: dict[str, str] | None = None,
    collect_mode: str = "synthetic",
    count_value: int = 1,
    inject_spark: bool = True,
    explore_branches: bool = True,
    max_branch_passes: int = 10,
    fetch_catalog: bool = True,
    branch_workers: int = 4,
) -> Json:
    """Executa o entrypoint com as actions interceptadas e devolve o documento completo.

    Args:
        project_root: Raiz do projeto (adicionada ao ``sys.path`` e usada como ``cwd``).
        entrypoint: Caminho do script principal do job (relativo à raiz ou absoluto).
        args: Argumentos extras para ``sys.argv``.
        params: Parâmetros devolvidos por ``dbutils.widgets.get``.
        collect_mode: ``"synthetic"`` (1 linha sintética) ou ``"empty"`` (lista vazia).
        count_value: Valor inteiro devolvido por ``df.count()``.
        inject_spark: Se ``True``, injeta ``spark`` no namespace global do entrypoint.
        explore_branches: Se ``True``, reexecuta o job invertendo ramos ``if/else`` não visitados.
        max_branch_passes: Limite de passadas extras para exploração de ramos.
        fetch_catalog: Se ``True``, consulta tipos e comentários das tabelas no Unity Catalog ao final.
        branch_workers: Número de threads paralelas para explorar ramos (padrão: 4; ``1`` para sequencial).

    Returns:
        Dicionário autocontido com planos, catálogo, fontes, destinos, fluxos, ramos, eventos e avisos.
    """
    from .sources import build_sources

    root = Path(project_root).resolve()
    ep = _resolve_entrypoint(root, entrypoint)
    params = params or {}
    explorer = BranchExplorer(enabled=explore_branches)
    cap = Capturer(root, collect_mode, count_value, params, explorer=explorer)
    finder = _BranchMetaPathFinder(root, cap.root_prefix, cap._tool_file)

    old_cwd, old_path, old_argv = os.getcwd(), list(sys.path), list(sys.argv)
    builtin_names = (
        "display",
        "dbutils",
        "__spark_lineage_branch__",
        "__spark_lineage_sql__",
        "__spark_lineage_run__",
        "__spark_lineage_magic__",
    )
    old_builtins = {n: getattr(builtins, n, None) for n in builtin_names}
    spark: Any = None
    patcher = install_patches(cap)
    sys.meta_path.insert(0, finder)

    try:
        os.chdir(root)
        sys.path.insert(0, str(root))
        sys.argv = [str(ep), *(args or [])]

        def display(obj: Any = None, *_a: Any, **_kw: Any) -> None:
            if obj is not None and hasattr(obj, "explain"):
                cap.capture(obj, "display")

        init_globals: Json = {"display": display}

        nb_tls = threading.local()

        def _active_notebooks() -> set[Path]:
            active = getattr(nb_tls, "active", None)
            if active is None:
                active = {ep.resolve()}
                nb_tls.active = active
            return active

        def run_child_notebook(path_str: str, child_args: dict[str, Any]) -> str:
            site = cap.call_site()
            caller_f = str(root / site["file"]) if site and site.get("file") else str(ep)
            target_nb = _resolve_notebook_target(path_str, caller_f, root)
            if target_nb is None:
                cap.event("notebook_run_not_found", target=path_str, arguments=child_args)
                cap.warn(f"dbutils.notebook.run: notebook não encontrado localmente: {path_str}")
                return ""
            resolved = target_nb.resolve()
            active_notebooks = _active_notebooks()
            if resolved in active_notebooks:
                cap.event("notebook_run_cycle", target=str(target_nb))
                cap.warn(f"dbutils.notebook.run: ciclo detectado ao chamar {target_nb.name}; ignorado")
                return ""
            prev_params = dict(dbutils.widgets._p)
            prev_defaults = dict(dbutils.widgets._defaults)
            dbutils.widgets._p.update({str(k): str(v) for k, v in child_args.items()})
            active_notebooks.add(resolved)
            try:
                _exec_entrypoint(target_nb, finder, init_globals)
            except _JobExit as exc:
                return str(exc.args[0]) if exc.args else ""
            finally:
                active_notebooks.discard(resolved)
                dbutils.widgets._p = prev_params
                dbutils.widgets._defaults = prev_defaults
            return ""

        dbutils = DBUtilsStub(params, notebook_runner=run_child_notebook)
        init_globals["dbutils"] = dbutils

        def exec_magic_run(raw_arg: str, caller_globals: Json) -> None:
            target_str, run_params = _parse_magic_run_args(raw_arg)
            caller_f = caller_globals.get("__file__") or str(ep)
            target_nb = _resolve_notebook_target(target_str, caller_f, root)
            if target_nb is None:
                cap.event("magic_run_not_found", target=target_str)
                cap.warn(f"%run: notebook não encontrado localmente: {target_str}")
                return
            resolved = target_nb.resolve()
            active_notebooks = _active_notebooks()
            if resolved in active_notebooks:
                cap.event("magic_run_cycle", target=str(target_nb))
                cap.warn(f"%run: ciclo detectado ao chamar {target_nb.name}; ignorado")
                return
            prev_params = dict(dbutils.widgets._p)
            dbutils.widgets._p.update(run_params)
            code = finder.compile_file(str(target_nb))
            old_file = caller_globals.get("__file__")
            caller_globals["__file__"] = str(target_nb)
            active_notebooks.add(resolved)
            try:
                exec(code, caller_globals)
            except _JobExit:
                pass
            finally:
                active_notebooks.discard(resolved)
                dbutils.widgets._p = prev_params
                if old_file is not None:
                    caller_globals["__file__"] = old_file

        def exec_magic_sql(raw_query: str) -> Any:
            nonlocal spark
            active_spark = spark or init_globals.get("spark") or get_spark()
            query = substitute_sql_params(raw_query, dbutils.widgets.getAll())
            mode, kind = classify_sql(query)
            df = active_spark.sql(query)
            if mode == "pass" and kind in ("SELECT", "WITH", "VALUES", "TABLE", "FROM"):
                cap.capture(df, "sql:SELECT", extra={"query": query[:2000]})
            return df

        def exec_other_magic(kind: str, content: str) -> None:
            cap.event(f"magic_{kind}", query=content[:500])

        builtins.__dict__.update(
            {
                "display": display,
                "dbutils": dbutils,
                "__spark_lineage_branch__": explorer.decide,
                "__spark_lineage_sql__": exec_magic_sql,
                "__spark_lineage_run__": exec_magic_run,
                "__spark_lineage_magic__": exec_other_magic,
            }
        )
        if inject_spark:
            spark = get_spark()
            init_globals["spark"] = spark
            _configure_spark_explain(spark, cap)

        status, error = _run_first_pass(ep, finder, init_globals, cap)
        if explore_branches and status == "ok":
            _run_branch_passes(
                ep, args, finder, init_globals, cap, max_branch_passes, branch_workers=branch_workers
            )
    finally:
        if finder in sys.meta_path:
            sys.meta_path.remove(finder)
        patcher.restore()
        os.chdir(old_cwd)
        sys.path[:] = old_path
        sys.argv[:] = old_argv
        _restore_builtins(old_builtins)

    plans_list = list(cap.plans.values())
    catalog_data = _maybe_fetch_catalog(spark, cap, plans_list, fetch_catalog)
    ep_rel = str(ep.relative_to(root)) if root in ep.parents else str(ep)
    raw_result: Json = {
        "project_root": str(root),
        "entrypoint": ep_rel,
        "params": params,
        "settings": {
            "collect_mode": collect_mode,
            "count_value": count_value,
            "explore_branches": explore_branches,
            "branch_workers": branch_workers,
            "fetch_catalog": fetch_catalog,
        },
        "status": status,
        "error": error,
        "plans": plans_list,
        "catalog": catalog_data,
        "read_sites": cap.read_sites,
        "branches": explorer.summary(),
        "events": cap.events,
        "capture_warnings": cap.warnings,
    }
    consolidated = build_sources(raw_result, catalog=catalog_data)
    return {
        **raw_result,
        "sources": consolidated["sources"],
        "views": consolidated["views"],
        "destinations": consolidated["destinations"],
        "flows": consolidated["flows"],
        "warnings": consolidated["warnings"],
    }


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _write_text_dir(result: Json, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for plan in result["plans"]:
        lines = [f"# {plan['id']} (hash {plan['hash']})", "", "## Ocorrências"]
        for o in plan["occurrences"]:
            s = o.get("call_site") or {}
            br = f"  [ramo: {o['branch']}]" if o.get("branch") else ""
            lines.append(f"- {o['action']} -> {o.get('target')}  @ {s.get('file')}:{s.get('line')}{br}")
        if plan.get("output_columns"):
            lines += ["", "## Colunas de saída", *[f"- {c['name']}: {c['type']}" for c in plan["output_columns"]]]
        for name, body in plan["sections"].items():
            lines += ["", f"## {name}", body]
        (out_dir / f"{plan['id']}.txt").write_text("\n".join(lines), encoding="utf-8")


def _build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="spark-lineage capture",
        description="Captura planos de execução Spark de um job.",
    )
    ap.add_argument("--project-root", required=True, help="raiz do projeto (vai para sys.path e cwd)")
    ap.add_argument("--entrypoint", required=True, help="arquivo do job (relativo à raiz ou absoluto)")
    ap.add_argument("--output", default="plan.json", help="JSON único de saída (padrão: plan.json)")
    ap.add_argument("--text-dir", help="se informado, grava também um .txt por plano")
    ap.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="CHAVE=VALOR",
        help="parâmetro para dbutils.widgets.get (repetível)",
    )
    ap.add_argument("--arg", action="append", default=[], help="argumento para sys.argv (repetível)")
    ap.add_argument(
        "--collect-mode",
        choices=["synthetic", "empty"],
        default="synthetic",
        help="collect/first/take devolvem 1 linha sintética (padrão) ou lista vazia",
    )
    ap.add_argument("--count-value", type=int, default=1, help="valor devolvido por count() (padrão: 1)")
    ap.add_argument(
        "--no-inject-spark",
        action="store_true",
        help="não injetar a variável global 'spark' (o job cria a própria sessão)",
    )
    ap.add_argument(
        "--no-explore-branches",
        action="store_true",
        help="desativa a exploração automática de ramos if/else não visitados",
    )
    ap.add_argument(
        "--max-branch-passes",
        type=int,
        default=10,
        help="máximo de passadas extras para explorar ramos if/else (padrão: 10)",
    )
    ap.add_argument(
        "--branch-workers",
        type=int,
        default=4,
        help="número de threads paralelas para explorar ramos if/else (padrão: 4; use 1 para sequencial)",
    )
    ap.add_argument(
        "--no-catalog",
        action="store_true",
        help="não consultar schemas/comentários das tabelas no Unity Catalog ao final da captura",
    )
    ap.add_argument(
        "--raw",
        action="store_true",
        help="grava o JSON bruto (com sections completos dos planos) em --output em vez do formato compactado padrão",
    )
    ap.add_argument(
        "--raw-output",
        default=None,
        help="opcional: grava também o JSON bruto completo em um arquivo separado (para depuração)",
    )
    return ap


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada CLI do subcomando ``spark-lineage capture``.

    Por padrão grava o artefato **compactado** (``spark-lineage-compact/v1``, sem ``plans[*].sections``)
    em ``--output``, com fontes, destinos, fluxos, ramos, queries SQL e linhagem por coluna já pré-calculados.

    Args:
        argv: Lista de argumentos de linha de comando (usa ``sys.argv[1:]`` se ``None``).

    Returns:
        Código de saída (0 para sucesso, 1 para erro durante a execução do job).
    """
    from .compact import compact_plan

    ns = _build_arg_parser().parse_args(argv)
    params = dict(p.split("=", 1) for p in ns.param if "=" in p)
    result = capture_plans(
        ns.project_root,
        ns.entrypoint,
        args=ns.arg,
        params=params,
        collect_mode=ns.collect_mode,
        count_value=ns.count_value,
        inject_spark=not ns.no_inject_spark,
        explore_branches=not ns.no_explore_branches,
        max_branch_passes=ns.max_branch_passes,
        fetch_catalog=not ns.no_catalog,
        branch_workers=ns.branch_workers,
    )

    payload = result if ns.raw else compact_plan(result)
    out_path = Path(ns.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    if ns.raw_output:
        raw_path = Path(ns.raw_output)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(
            json.dumps(result, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
    if ns.text_dir:
        _write_text_dir(result, Path(ns.text_dir))

    n_occ = sum(len(p["occurrences"]) for p in result["plans"])
    n_br = len(result.get("branches", []))
    n_src = len(result.get("sources", []))
    n_dst = len(result.get("destinations", []))
    mode_lbl = "bruto" if ns.raw else "compacto"
    print(
        f"[capture_plans] status={result['status']} ({mode_lbl}) planos={len(result['plans'])} "
        f"ocorrências={n_occ} fontes={n_src} destinos={n_dst} ramos={n_br} "
        f"eventos={len(result['events'])} avisos={len(result['warnings'])} -> {ns.output}",
        file=sys.stderr,
    )
    if result["error"]:
        print(
            f"[capture_plans] erro no job: {result['error']['type']}: {result['error']['message']}",
            file=sys.stderr,
        )
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
