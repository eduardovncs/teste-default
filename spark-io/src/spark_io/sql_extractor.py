"""Extração determinística de tabelas de entrada e saída em SQL usando `sqlglot`."""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from spark_io.partial_eval import WILDCARD, apply_env_prefix

_SQL_WILDCARD_MARKER = "__spark_io_wildcard__"
_DML_ACTIONS: tuple[tuple[type[exp.Expr], str], ...] = (
    (exp.Insert, "sql:INSERT"),
    (exp.Merge, "sql:MERGE"),
    (exp.Update, "sql:UPDATE"),
    (exp.Delete, "sql:DELETE"),
)


@dataclass(frozen=True)
class SqlTableHit:
    """Referência de tabela extraída de uma instrução SQL.

    Args:
        table: Nome completo (ou parcial com `<*>`) da tabela extraída.
        kind: Direção da operação (`"read"` ou `"write"`).
        via: Ação SQL correspondente (ex.: `"sql:SELECT"`, `"sql:INSERT"`, `"sql:MERGE"`).
        dynamic: Indica se o nome contém `<*>` não resolvido.
    """

    table: str
    kind: str
    via: str
    dynamic: bool


@dataclass
class SqlExtractionResult:
    """Resultado consolidado da análise de um bloco SQL.

    Args:
        hits: Referências de leitura e escrita extraídas.
        created_temp_views: Nomes (minúsculos) de temp views criadas pelo bloco SQL.
        warnings: Avisos de parse quando alguma instrução SQL não puder ser interpretada.
    """

    hits: list[SqlTableHit] = field(default_factory=list)
    created_temp_views: set[str] = field(default_factory=set)
    warnings: list[str] = field(default_factory=list)


def _format_table_name(table_exp: exp.Table, env_prefix: str) -> tuple[str, bool]:
    """Constrói o nome normalizado de um nó `exp.Table` restaurando wildcards.

    Args:
        table_exp: Nó de tabela produzido pelo `sqlglot`.
        env_prefix: Prefixo padrão de ambiente para resolução de `<*>_<resto>`.

    Returns:
        Tupla `(nome_resolvido, eh_dinamico)`.
    """
    parts = [part for part in (table_exp.catalog, table_exp.db, table_exp.name) if part]
    raw = ".".join(parts).replace(_SQL_WILDCARD_MARKER, WILDCARD)
    return apply_env_prefix(raw, env_prefix)


def _unwrap_table(node: exp.Expr | None) -> exp.Table | None:
    """Extrai o `exp.Table` direto ou encapsulado em `exp.Schema`.

    Args:
        node: Nó alvo de DML/DDL no `sqlglot`.

    Returns:
        Instância de `exp.Table` ou `None`.
    """
    unwrapped = node.this if isinstance(node, exp.Schema) else node
    return unwrapped if isinstance(unwrapped, exp.Table) else None


def _extract_create_target(stmt: exp.Create) -> tuple[exp.Table | None, str, bool]:
    """Identifica o alvo e se a instrução `CREATE` cria uma view temporária.

    Args:
        stmt: Nó `exp.Create` do `sqlglot`.

    Returns:
        Tupla `(tabela_alvo, nome_acao, eh_temp_view)`.
    """
    kind = (stmt.args.get("kind") or "").upper()
    props = stmt.args.get("properties")
    has_temp_prop = isinstance(props, exp.Properties) and any(
        isinstance(prop, exp.TemporaryProperty) for prop in props.expressions
    )
    is_temp = has_temp_prop or "TEMP" in kind
    return _unwrap_table(stmt.this), f"sql:CREATE_{kind or 'TABLE'}", is_temp and "VIEW" in kind


def _extract_write_target(stmt: exp.Expr) -> tuple[exp.Table | None, str, bool]:
    """Identifica o alvo de escrita de uma instrução DML/DDL no `sqlglot`.

    Args:
        stmt: Expressão raiz da instrução SQL.

    Returns:
        Tupla `(tabela_alvo, nome_acao, eh_temp_view)`.
    """
    if isinstance(stmt, exp.Create):
        return _extract_create_target(stmt)
    for stmt_cls, action in _DML_ACTIONS:
        if isinstance(stmt, stmt_cls):
            return _unwrap_table(stmt.this), action, False
    return None, "sql:SELECT", False


def _record_target_table(
    target_node: exp.Table | None,
    action: str,
    is_temp_view: bool,
    env_prefix: str,
    known_temp_views: set[str],
    result: SqlExtractionResult,
) -> None:
    """Registra tabela de destino ou temp view criada pela instrução SQL.

    Args:
        target_node: Nó de tabela alvo, se houver.
        action: Nome da ação SQL (ex.: `"sql:INSERT"`).
        is_temp_view: Indica se o alvo é uma view temporária.
        env_prefix: Prefixo padrão de ambiente (`pr`).
        known_temp_views: Conjunto mutável de temp views conhecidas.
        result: Acumulador de saída.
    """
    if target_node is None:
        return
    target_name, is_dyn = _format_table_name(target_node, env_prefix)
    if not target_name:
        return
    if is_temp_view:
        known_temp_views.add(target_name.lower())
        result.created_temp_views.add(target_name.lower())
    else:
        result.hits.append(SqlTableHit(table=target_name, kind="write", via=action, dynamic=is_dyn))


def _record_read_tables(
    stmt: exp.Expr,
    target_node: exp.Table | None,
    env_prefix: str,
    excluded_names: set[str],
    result: SqlExtractionResult,
) -> None:
    """Registra tabelas de leitura de uma instrução SQL ignorando CTEs e temp views.

    Args:
        stmt: Expressão SQL parseada.
        target_node: Nó de tabela de escrita a ignorar.
        env_prefix: Prefixo padrão de ambiente (`pr`).
        excluded_names: Conjunto (minúsculo) de CTEs e temp views a ignorar.
        result: Acumulador de saída.
    """
    for tbl in stmt.find_all(exp.Table):
        if tbl is target_node:
            continue
        name, is_dyn = _format_table_name(tbl, env_prefix)
        if name and name.lower() not in excluded_names:
            result.hits.append(SqlTableHit(table=name, kind="read", via="sql:SELECT", dynamic=is_dyn))


def _collect_statement_hits(
    stmt: exp.Expr,
    env_prefix: str,
    known_temp_views: set[str],
    result: SqlExtractionResult,
) -> None:
    """Extrai leituras e escritas de uma única instrução SQL parseada.

    Args:
        stmt: Expressão SQL já parseada pelo `sqlglot`.
        env_prefix: Prefixo padrão de ambiente (`pr`).
        known_temp_views: Conjunto (minúsculo) de temp views conhecidas até este ponto.
        result: Acumulador de saída.
    """
    cte_names = {cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE) if cte.alias_or_name}
    target_node, action, is_temp_view = _extract_write_target(stmt)
    _record_target_table(target_node, action, is_temp_view, env_prefix, known_temp_views, result)
    _record_read_tables(stmt, target_node, env_prefix, cte_names | known_temp_views, result)


def extract_sql_io(
    sql: str,
    *,
    env_prefix: str = "pr",
    temp_views: set[str] | None = None,
) -> SqlExtractionResult:
    """Analisa um bloco SQL Databricks via `sqlglot` e retorna tabelas lidas e gravadas.

    Args:
        sql: Código SQL bruto (pode conter `<*>` oriundo de avaliação parcial).
        env_prefix: Prefixo padrão de ambiente para resolução de catálogos `<*>_<resto>`.
        temp_views: Conjunto opcional de nomes de views temporárias já criadas no contexto.

    Returns:
        `SqlExtractionResult` com hits de leitura/escrita, temp views criadas e avisos.
    """
    result = SqlExtractionResult()
    cleaned = sql.strip()
    if not cleaned:
        return result

    sanitized = cleaned.replace(WILDCARD, _SQL_WILDCARD_MARKER)
    known_views = {v.lower() for v in (temp_views or set())}

    try:
        statements = sqlglot.parse(sanitized, read="databricks")
    except ParseError as exc:
        result.warnings.append(f"Falha ao parsear SQL via sqlglot: {exc}")
        return result

    for stmt in statements:
        if stmt is not None:
            _collect_statement_hits(stmt, env_prefix, known_views, result)

    return result
