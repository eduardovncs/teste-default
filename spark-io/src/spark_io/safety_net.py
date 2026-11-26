"""Rede de segurança exaustiva (`ast.walk`) garantindo zero falsos negativos silenciosos."""

from __future__ import annotations

import ast

from spark_io.chain_analyzer import READ_TERMINALS, WRITE_TERMINALS, unwind_chain
from spark_io.models import UnresolvedDynamic, UnresolvedKind

_DIRECT_WRITE_METHODS = frozenset({"writeTo", "mergeInto", "saveAsTable", "insertInto", "toTable"})
_ANCHOR_RULES: tuple[tuple[str, UnresolvedKind, frozenset[str]], ...] = (
    ("read", "read", READ_TERMINALS),
    ("readStream", "read", READ_TERMINALS),
    ("write", "write", WRITE_TERMINALS),
    ("writeStream", "write", WRITE_TERMINALS),
)


def _classify_anchor_chain(attrs: list[str], last_attr: str) -> tuple[UnresolvedKind, str] | None:
    """Classifica cadeias contendo âncoras `.read`/`.readStream`/`.write`/`.writeStream`.

    Args:
        attrs: Lista de nomes de atributos da cadeia.
        last_attr: Nome do método terminal chamado.

    Returns:
        Tupla `(kind, nome_chamada)` ou `None`.
    """
    for anchor, kind, terminals in _ANCHOR_RULES:
        if anchor in attrs and last_attr in terminals:
            return kind, f"{anchor}.{last_attr}"
    return None


def _is_delta_factory(root: ast.expr, first_attr: str) -> bool:
    """Verifica se a chamada é `DeltaTable.forName` ou `DeltaTable.forPath`.

    Args:
        root: Nó raiz da cadeia desenrolada.
        first_attr: Primeiro atributo após a raiz.

    Returns:
        `True` se corresponder a uma fábrica `DeltaTable`.
    """
    return isinstance(root, ast.Name) and root.id == "DeltaTable" and first_attr in ("forName", "forPath")


def _classify_direct_spark_call(
    root: ast.expr,
    first_attr: str,
    last_attr: str,
    has_args: bool,
) -> tuple[UnresolvedKind, str] | None:
    """Classifica chamadas diretas Spark/Delta sem âncora `.read`/`.write`.

    Args:
        root: Nó raiz da cadeia desenrolada.
        first_attr: Primeiro atributo após a raiz.
        last_attr: Método terminal invocado.
        has_args: Indica se a chamada recebeu argumentos posicionais ou nomeados.

    Returns:
        Tupla `(kind, nome_chamada)` ou `None`.
    """
    if last_attr in _DIRECT_WRITE_METHODS:
        return "write", last_attr
    if last_attr in ("table", "sql") and has_args:
        return "read", last_attr
    if _is_delta_factory(root, first_attr):
        return "read", f"DeltaTable.{first_attr}"
    return None


def _classify_spark_call(call: ast.Call) -> tuple[UnresolvedKind, str] | None:
    """Identifica se um `ast.Call` arbitrário corresponde a uma chamada Spark de leitura/escrita.

    Args:
        call: Nó `ast.Call` inspecionado.

    Returns:
        Tupla `(kind, nome_chamada)` ou `None`.
    """
    root, steps = unwind_chain(call)
    if not steps or steps[-1].call is None:
        return None
    attrs = [s.attr for s in steps]
    last_attr = steps[-1].attr

    anchored = _classify_anchor_chain(attrs, last_attr)
    if anchored is not None:
        return anchored
    return _classify_direct_spark_call(root, steps[0].attr, last_attr, bool(call.args or call.keywords))


def _store_outermost(
    bucket: dict[tuple[int, str], UnresolvedDynamic],
    candidate: UnresolvedDynamic,
) -> None:
    """Mantém apenas a expressão mais externa para um mesmo par `(linha, kind)`.

    Args:
        bucket: Dicionário acumulador indexado por `(linha, kind)`.
        candidate: Novo item `UnresolvedDynamic` candidato.
    """
    key = (candidate.line, candidate.kind)
    existing = bucket.get(key)
    if existing is None or len(candidate.expr) > len(existing.expr):
        bucket[key] = candidate


def audit_missed_io(
    node: ast.AST,
    rel_file: str,
    covered_lines: set[int],
    ignored_lines: set[int],
) -> list[UnresolvedDynamic]:
    """Varre exaustivamente `node` com `ast.walk` e reporta qualquer chamada Spark não coberta.

    Args:
        node: Árvore AST (módulo ou função/classe) a auditar.
        rel_file: Caminho relativo do arquivo de origem.
        covered_lines: Linhas já contabilizadas pela Camada 1.
        ignored_lines: Linhas de chamadas parametrizadas dentro de wrappers já rastreados.

    Returns:
        Lista de `UnresolvedDynamic` para chamadas Spark que escaparam da Camada 1.
    """
    skip = covered_lines | ignored_lines
    found_by_key: dict[tuple[int, str], UnresolvedDynamic] = {}

    for child in ast.walk(node):
        if not isinstance(child, ast.Call) or child.lineno in skip:
            continue
        classified = _classify_spark_call(child)
        if classified is None:
            continue
        kind, label = classified
        _store_outermost(
            found_by_key,
            UnresolvedDynamic(
                file=rel_file,
                line=child.lineno,
                call=label,
                kind=kind,
                expr=ast.unparse(child),
            ),
        )

    return list(found_by_key.values())
