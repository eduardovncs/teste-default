"""Avaliação parcial leve de expressões Python via AST (sem execução de código)."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field

WILDCARD = "<*>"
_IDENT_RE = re.compile(r"^(?:<\*>|[A-Za-z_][A-Za-z0-9_]*|<\*>_[A-Za-z0-9_]+|[A-Za-z0-9_]+_<\*>)$")
_NUMERIC_ONLY_RE = re.compile(r"^\d+$")
_SQL_ENV_PREFIX_RE = re.compile(r"<\*>_([A-Za-z0-9_]+(?:\.[A-Za-z0-9_<>]+)+)")
_FORBIDDEN_TABLE_CHARS = frozenset({" ", "\t", "\n", "/", "\\", ":"})


@dataclass
class EnvScope:
    """Mini-ambiente simbólico de constantes literais conhecidas no escopo atual.

    Args:
        strings: Variáveis resolvidas para string literal ou template com `<*>`.
        dicts: Dicionários literais de strings (`nome_var -> {chave: valor}`).
        lists: Listas ou tuplas literais de strings (`nome_var -> [itens]`).
        widgets: Valores de `dbutils.widgets.get(...)` injetados por `dbutils.notebook.run`.
        user_vars: Variáveis fornecidas explicitamente pelo chamador (têm precedência sobre `<*>`).
        delta_tables: Mapeamento de variáveis `DeltaTable` para `(target, dynamic, raw_expr)`.
    """

    strings: dict[str, str] = field(default_factory=dict)
    dicts: dict[str, dict[str, str]] = field(default_factory=dict)
    lists: dict[str, list[str]] = field(default_factory=dict)
    widgets: dict[str, str] = field(default_factory=dict)
    user_vars: dict[str, str] = field(default_factory=dict)
    delta_tables: dict[str, tuple[str, bool, str | None]] = field(default_factory=dict)

    def copy(self) -> EnvScope:
        """Cria uma cópia rasa independente do escopo atual.

        Returns:
            Nova instância de `EnvScope` com dicionários copiados.
        """
        return EnvScope(
            strings=dict(self.strings),
            dicts={k: dict(v) for k, v in self.dicts.items()},
            lists={k: list(v) for k, v in self.lists.items()},
            widgets=dict(self.widgets),
            user_vars=dict(self.user_vars),
            delta_tables=dict(self.delta_tables),
        )


def _collapse_wildcards(text: str) -> str:
    """Agrupa ocorrências consecutivas de `<*>` em um único marcador.

    Args:
        text: Template textual resultante da avaliação parcial.

    Returns:
        String normalizada sem `<*><*>` redundantes.
    """
    while f"{WILDCARD}{WILDCARD}" in text:
        text = text.replace(f"{WILDCARD}{WILDCARD}", WILDCARD)
    return text


def apply_env_prefix(value: str, env_prefix: str) -> tuple[str, bool]:
    """Aplica o prefixo de ambiente (`pr`) quando o catálogo inicia com `<*>_`.

    Args:
        value: Nome ou template de tabela/SQL avaliado parcialmente.
        env_prefix: Prefixo padrão de ambiente (ex.: `"pr"`).

    Returns:
        Tupla `(valor_resolvido, ainda_dinamico)`.
    """
    cleaned = _collapse_wildcards(value.strip())
    if not cleaned or cleaned == WILDCARD:
        return cleaned, True

    resolved = cleaned
    if env_prefix:
        parts = resolved.split(".")
        if len(parts) >= 2 and parts[0].startswith(f"{WILDCARD}_"):
            parts[0] = f"{env_prefix}_{parts[0][len(WILDCARD) + 1 :]}"
            resolved = ".".join(parts)
        elif " " in resolved or "\n" in resolved:
            resolved = _SQL_ENV_PREFIX_RE.sub(rf"{env_prefix}_\1", resolved)

    return resolved, (WILDCARD in resolved)


def _apply_format_args(template: str, pos_vals: list[str], kw_vals: dict[str, str]) -> str:
    """Aplica substituições posicionais e nomeadas em uma string de template `.format()`.

    Args:
        template: String de formato base.
        pos_vals: Valores posicionais parcialmente avaliados.
        kw_vals: Valores nomeados parcialmente avaliados.

    Returns:
        String resultante após substituições.
    """
    result = template
    for idx, val in enumerate(pos_vals):
        result = result.replace("{}", val, 1).replace(f"{{{idx}}}", val)
    for key, val in kw_vals.items():
        result = result.replace(f"{{{key}}}", val)
    return _collapse_wildcards(result)


def _eval_format_call(call: ast.Call, env: EnvScope) -> str:
    """Avalia parcialmente chamadas `"...".format(*args, **kwargs)`.

    Args:
        call: Nó `ast.Call` representando `.format(...)`.
        env: Escopo simbólico atual.

    Returns:
        String formatada parcialmente (usando `<*>` para argumentos desconhecidos).
    """
    if not isinstance(call.func, ast.Attribute):
        return WILDCARD
    template = eval_str_expr(call.func.value, env)
    if template == WILDCARD:
        return WILDCARD
    pos_vals = [eval_str_expr(arg, env) for arg in call.args]
    kw_vals = {kw.arg: eval_str_expr(kw.value, env) for kw in call.keywords if kw.arg}
    return _apply_format_args(template, pos_vals, kw_vals)


def _eval_widget_get(call: ast.Call, env: EnvScope) -> str | None:
    """Resolve `dbutils.widgets.get("chave")` usando os widgets injetados no escopo.

    Args:
        call: Nó `ast.Call` candidato.
        env: Escopo simbólico atual.

    Returns:
        Valor do widget (ou `<*>` se não injetado), ou `None` se não for `widgets.get`.
    """
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr != "get":
        return None
    if not isinstance(func.value, ast.Attribute) or func.value.attr != "widgets":
        return None
    if not call.args:
        return WILDCARD
    key = eval_str_expr(call.args[0], env)
    if WILDCARD in key:
        return WILDCARD
    return env.widgets.get(key, env.user_vars.get(key, WILDCARD))


def _eval_call_expr(node: ast.Call, env: EnvScope) -> str:
    """Avalia chamadas suportadas em expressões de string (`widgets.get` e `.format`).

    Args:
        node: Nó `ast.Call`.
        env: Escopo simbólico atual.

    Returns:
        String parcialmente avaliada ou `<*>`.
    """
    widget_val = _eval_widget_get(node, env)
    if widget_val is not None:
        return widget_val
    if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        return _eval_format_call(node, env)
    return WILDCARD


def _eval_fstring_part(part: ast.expr, env: EnvScope) -> str:
    """Avalia um componente individual de `ast.JoinedStr`.

    Args:
        part: Nó filho de `ast.JoinedStr`.
        env: Escopo simbólico atual.

    Returns:
        Texto literal ou resultado parcial da expressão interpolada.
    """
    if isinstance(part, ast.Constant) and isinstance(part.value, str):
        return part.value
    if isinstance(part, ast.FormattedValue):
        return eval_str_expr(part.value, env)
    return ""


def _eval_subscript(node: ast.Subscript, env: EnvScope) -> str:
    """Avalia acessos literais `CFG["chave"]` em dicionários conhecidos no escopo.

    Args:
        node: Nó `ast.Subscript`.
        env: Escopo simbólico atual.

    Returns:
        Valor resolvido do dicionário ou `<*>`.
    """
    if isinstance(node.value, ast.Name) and node.value.id in env.dicts:
        key = eval_str_expr(node.slice, env)
        if WILDCARD not in key:
            return env.dicts[node.value.id].get(key, WILDCARD)
    return WILDCARD


def _eval_composite_str(node: ast.expr, env: EnvScope) -> str:
    """Avalia f-strings, concatenações `+`, subscripts e chamadas suportadas.

    Args:
        node: Expressão AST composta.
        env: Escopo simbólico atual.

    Returns:
        String avaliada parcialmente ou `<*>`.
    """
    if isinstance(node, ast.JoinedStr):
        return _collapse_wildcards("".join(_eval_fstring_part(p, env) for p in node.values))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _collapse_wildcards(eval_str_expr(node.left, env) + eval_str_expr(node.right, env))
    if isinstance(node, ast.Subscript):
        return _eval_subscript(node, env)
    if isinstance(node, ast.Call):
        return _eval_call_expr(node, env)
    return WILDCARD


def eval_str_expr(node: ast.expr, env: EnvScope) -> str:
    """Avalia parcialmente uma expressão Python para string ou template com `<*>`.

    Args:
        node: Expressão AST a avaliar.
        env: Escopo simbólico de constantes e widgets.

    Returns:
        String literal resolvida ou template contendo `<*>`.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return env.strings.get(node.id, env.user_vars.get(node.id, WILDCARD))
    return _eval_composite_str(node, env)


def eval_str_list(node: ast.expr, env: EnvScope) -> list[str] | None:
    """Resolve uma lista/tupla literal de strings ou variável que aponta para ela.

    Args:
        node: Expressão AST iterável.
        env: Escopo simbólico atual.

    Returns:
        Lista de strings resolvidas ou `None` quando o iterável não é conhecido estaticamente.
    """
    if isinstance(node, ast.Name) and node.id in env.lists:
        return list(env.lists[node.id])
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return [eval_str_expr(elt, env) for elt in node.elts]
    return None


def eval_str_dict(node: ast.expr, env: EnvScope) -> dict[str, str] | None:
    """Resolve um dicionário literal `{chave: valor}` para pares de strings.

    Args:
        node: Expressão AST candidata a dicionário.
        env: Escopo simbólico atual.

    Returns:
        Dicionário de strings ou `None` se não for um dicionário literal.
    """
    if isinstance(node, ast.Name) and node.id in env.dicts:
        return dict(env.dicts[node.id])
    if not isinstance(node, ast.Dict):
        return None
    out: dict[str, str] = {}
    for key_node, val_node in zip(node.keys, node.values, strict=False):
        if key_node is None:
            continue
        key = eval_str_expr(key_node, env)
        if WILDCARD not in key:
            out[key] = eval_str_expr(val_node, env)
    return out


def bind_assignment(target: ast.expr, value: ast.expr, env: EnvScope) -> None:
    """Atualiza o `EnvScope` a partir de uma atribuição simples `x = expr`.

    Args:
        target: Alvo da atribuição AST.
        value: Expressão do lado direito.
        env: Escopo simbólico a atualizar no local.
    """
    if not isinstance(target, ast.Name):
        return
    name = target.id

    dict_val = eval_str_dict(value, env)
    if dict_val is not None:
        env.dicts[name] = dict_val
        return

    list_val = eval_str_list(value, env)
    if list_val is not None:
        env.lists[name] = list_val
        return

    str_val = eval_str_expr(value, env)
    env.strings[name] = env.user_vars[name] if str_val == WILDCARD and name in env.user_vars else str_val


def _has_informative_segments(parts: list[str]) -> bool:
    """Garante que os segmentos não sejam todos numéricos nem todos wildcards.

    Args:
        parts: Segmentos separados por ponto.

    Returns:
        `True` se houver ao menos um segmento não numérico e um não wildcard.
    """
    return any(not _NUMERIC_ONLY_RE.match(p) for p in parts) and any(p != WILDCARD for p in parts)


def _is_valid_table_parts(parts: list[str]) -> bool:
    """Valida se os segmentos separados por `.` formam identificadores de tabela válidos.

    Args:
        parts: Lista de segmentos de um nome qualificado de tabela.

    Returns:
        `True` se todos os segmentos forem identificadores válidos e não apenas números/wildcards.
    """
    if len(parts) not in (2, 3):
        return False
    if len(parts) == 2 and not parts[0].lower().endswith("_ing"):
        return False
    return _has_informative_segments(parts) and all(_IDENT_RE.match(p) for p in parts)


def is_table_pattern(value: str) -> bool:
    """Verifica se uma string ou template segue o formato de tabela Unity Catalog.

    Args:
        value: String já avaliada parcialmente.

    Returns:
        `True` quando o valor tem formato compatível com `catalogo.schema.tabela` ou `*_ing.tabela`.
    """
    text = value.strip()
    if not text or any(ch in _FORBIDDEN_TABLE_CHARS for ch in text):
        return False
    return _is_valid_table_parts(text.split("."))
