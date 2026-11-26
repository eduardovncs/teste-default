"""Avaliação parcial leve de expressões Python via AST (sem execução de código)."""

from __future__ import annotations

import ast
import copy
import re
from dataclasses import dataclass, field
from typing import Any

from spark_io.models import IoKind

WILDCARD = "<*>"
_IDENT_RE = re.compile(r"^(?:<\*>|[A-Za-z_][A-Za-z0-9_]*|<\*>_[A-Za-z0-9_]+|[A-Za-z0-9_]+_<\*>)$")
_NUMERIC_ONLY_RE = re.compile(r"^\d+$")
_SQL_ENV_PREFIX_RE = re.compile(r"<\*>_([A-Za-z0-9_]+(?:\.[A-Za-z0-9_<>]+)+)")
_FORBIDDEN_TABLE_CHARS = frozenset({" ", "\t", "\n", "/", "\\", ":"})
_STR_METHODS = frozenset({"strip", "lstrip", "rstrip", "lower", "upper"})


@dataclass(frozen=True)
class FuncSummary:
    """Resumo estático de uma função auxiliar pura (retorno constante ou pass-through).

    Args:
        constant: String literal constante retornada pela função, quando aplicável.
        passthrough_param_index: Índice posicional do parâmetro retornado intacto/sanitizado.
        passthrough_param_name: Nome do parâmetro retornado intacto/sanitizado.
    """

    constant: str | None = None
    passthrough_param_index: int | None = None
    passthrough_param_name: str | None = None


@dataclass(frozen=True)
class BuilderState:
    """Estado acumulado de um DataFrameReader/DataFrameWriter em variável intermediária.

    Args:
        kind: Direção do builder (`"read"` ou `"write"`).
        anchor: Nome da âncora original (`"read"`, `"readStream"`, `"write"` ou `"writeStream"`).
        receiver: Expressão do objeto raiz que originou o builder.
        data_format: Formato acumulado via `.format(...)`.
        mode: Modo de escrita acumulado via `.mode(...)`.
        options: Opções acumuladas via `.option(...)` / `.options(...)`.
    """

    kind: IoKind
    anchor: str
    receiver: str | None = None
    data_format: str | None = None
    mode: str | None = None
    options: dict[str, str] = field(default_factory=dict)


@dataclass
class EnvScope:
    """Mini-ambiente simbólico de constantes literais conhecidas no escopo atual.

    Args:
        strings: Variáveis resolvidas para string literal ou template com `<*>`.
        dicts: Dicionários literais (planos ou aninhados) acessíveis por chave.
        lists: Listas ou tuplas literais de strings (`nome_var -> [itens]`).
        widgets: Valores de `dbutils.widgets.get(...)` injetados por `dbutils.notebook.run`.
        user_vars: Variáveis fornecidas explicitamente pelo chamador (têm precedência sobre `<*>`).
        delta_tables: Mapeamento de variáveis `DeltaTable` para `(target, dynamic, raw_expr)`.
        builders: Variáveis que armazenam `DataFrameReader` / `DataFrameWriter` intermediários.
        imported_symbols: Símbolos importados localmente (`alias -> (modulo, nome_original)`).
        imported_modules: Módulos locais importados (`alias -> modulo`).
        env_prefix: Prefixo padrão de ambiente (`pr`) usado como fallback em dicts por ambiente.
        func_summaries: Resumos estáticos de funções auxiliares do repositório.
    """

    strings: dict[str, str] = field(default_factory=dict)
    dicts: dict[str, dict[str, Any]] = field(default_factory=dict)
    lists: dict[str, list[str]] = field(default_factory=dict)
    widgets: dict[str, str] = field(default_factory=dict)
    user_vars: dict[str, str] = field(default_factory=dict)
    delta_tables: dict[str, tuple[str, bool, str | None]] = field(default_factory=dict)
    builders: dict[str, BuilderState] = field(default_factory=dict)
    imported_symbols: dict[str, tuple[str, str]] = field(default_factory=dict)
    imported_modules: dict[str, str] = field(default_factory=dict)
    env_prefix: str = ""
    func_summaries: dict[str, FuncSummary] = field(default_factory=dict)

    def copy(self) -> EnvScope:
        """Cria uma cópia independente do escopo atual.

        Returns:
            Nova instância de `EnvScope` com estruturas mutáveis copiadas.
        """
        return EnvScope(
            strings=dict(self.strings),
            dicts=copy.deepcopy(self.dicts),
            lists={k: list(v) for k, v in self.lists.items()},
            widgets=dict(self.widgets),
            user_vars=dict(self.user_vars),
            delta_tables=dict(self.delta_tables),
            builders=dict(self.builders),
            imported_symbols=dict(self.imported_symbols),
            imported_modules=dict(self.imported_modules),
            env_prefix=self.env_prefix,
            func_summaries=self.func_summaries,
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


def _eval_str_method(call: ast.Call, env: EnvScope) -> str | None:
    """Avalia métodos simples de normalização de string (`.strip`, `.lower`, `.upper`).

    Args:
        call: Nó `ast.Call` candidato.
        env: Escopo simbólico atual.

    Returns:
        String transformada ou `None` quando não for método suportado.
    """
    if not isinstance(call.func, ast.Attribute) or call.func.attr not in _STR_METHODS:
        return None
    base = eval_str_expr(call.func.value, env)
    if base == WILDCARD:
        return WILDCARD
    method = getattr(base, call.func.attr)
    return str(method())


def _simple_callee_name(call: ast.Call) -> str | None:
    """Extrai o nome simples de uma função ou método invocado em `ast.Call`.

    Args:
        call: Nó `ast.Call`.

    Returns:
        Identificador simples ou `None`.
    """
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _pick_summary_arg(call: ast.Call, summary: FuncSummary) -> ast.expr | None:
    """Seleciona o argumento propagado por uma função pass-through.

    Args:
        call: Chamada AST da função auxiliar.
        summary: Resumo estático da função.

    Returns:
        Nó AST do argumento propagado ou `None`.
    """
    if summary.passthrough_param_name:
        for kw in call.keywords:
            if kw.arg == summary.passthrough_param_name:
                return kw.value
    idx = summary.passthrough_param_index
    return call.args[idx] if idx is not None and len(call.args) > idx else None


def _eval_summary_call(call: ast.Call, env: EnvScope) -> str | None:
    """Resolve chamadas a funções auxiliares puras registradas em `env.func_summaries`.

    Args:
        call: Nó `ast.Call` candidato.
        env: Escopo simbólico atual.

    Returns:
        Valor resolvido pela função auxiliar ou `None` quando não registrada.
    """
    callee = _simple_callee_name(call)
    summary = env.func_summaries.get(callee) if callee else None
    if summary is None:
        return None
    if summary.constant is not None:
        return summary.constant
    arg_node = _pick_summary_arg(call, summary)
    return eval_str_expr(arg_node, env) if arg_node is not None else WILDCARD


def _eval_call_expr(node: ast.Call, env: EnvScope) -> str:
    """Avalia chamadas suportadas em expressões de string (`widgets.get`, `.format`, helpers).

    Args:
        node: Nó `ast.Call`.
        env: Escopo simbólico atual.

    Returns:
        String parcialmente avaliada ou `<*>`.
    """
    for evaluator in (_eval_widget_get, _eval_str_method, _eval_summary_call):
        resolved = evaluator(node, env)
        if resolved is not None:
            return resolved
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


def _lookup_dict_key(mapping: dict[str, Any], key: str, env_prefix: str) -> Any:
    """Busca `key` em `mapping`, usando `env_prefix` como fallback quando `key` é dinâmico.

    Args:
        mapping: Dicionário avaliado estaticamente.
        key: Chave avaliada parcialmente.
        env_prefix: Prefixo padrão de ambiente (`pr`).

    Returns:
        Valor encontrado no dicionário ou `None`.
    """
    if WILDCARD not in key:
        return mapping.get(key)
    if not env_prefix:
        return None
    if env_prefix in mapping:
        return mapping[env_prefix]
    lower_prefix = env_prefix.lower()
    for map_key, map_val in mapping.items():
        if map_key.lower() == lower_prefix:
            return map_val
    return None


def _eval_container_expr(node: ast.expr, env: EnvScope) -> Any:
    """Resolve acessos recursivos em dicionários aninhados (ex.: `TABLES[env]["gsal"]`).

    Args:
        node: Expressão AST de nome ou subscript.
        env: Escopo simbólico atual.

    Returns:
        String, dicionário aninhado, lista ou `None`.
    """
    if isinstance(node, ast.Name):
        return env.dicts.get(node.id, env.lists.get(node.id, env.strings.get(node.id)))
    if isinstance(node, ast.Subscript):
        container = _eval_container_expr(node.value, env)
        if isinstance(container, dict):
            key = eval_str_expr(node.slice, env)
            return _lookup_dict_key(container, key, env.env_prefix)
    return None


def _eval_subscript(node: ast.Subscript, env: EnvScope) -> str:
    """Avalia acessos em dicionários literais (planos ou aninhados) no escopo.

    Args:
        node: Nó `ast.Subscript`.
        env: Escopo simbólico atual.

    Returns:
        String resolvida ou `<*>`.
    """
    val = _eval_container_expr(node, env)
    return val if isinstance(val, str) else WILDCARD


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


def _eval_dict_value(node: ast.expr, env: EnvScope) -> Any:
    """Avalia um valor de dicionário suportando dicts aninhados, listas ou strings.

    Args:
        node: Expressão AST de valor no dicionário.
        env: Escopo simbólico atual.

    Returns:
        Dicionário aninhado, lista ou string parcialmente avaliada.
    """
    nested = eval_nested_dict(node, env)
    if nested is not None:
        return nested
    list_val = eval_str_list(node, env)
    return list_val if list_val is not None else eval_str_expr(node, env)


def eval_nested_dict(node: ast.expr, env: EnvScope) -> dict[str, Any] | None:
    """Resolve um dicionário literal (plano ou aninhado) para estrutura Python.

    Args:
        node: Expressão AST candidata a dicionário.
        env: Escopo simbólico atual.

    Returns:
        Dicionário resolvido ou `None` quando não for um dicionário conhecido.
    """
    if isinstance(node, ast.Name) and node.id in env.dicts:
        return copy.deepcopy(env.dicts[node.id])
    if not isinstance(node, ast.Dict):
        return None
    out: dict[str, Any] = {}
    for key_node, val_node in zip(node.keys, node.values, strict=False):
        if key_node is None:
            continue
        key = eval_str_expr(key_node, env)
        if WILDCARD not in key:
            out[key] = _eval_dict_value(val_node, env)
    return out


def eval_str_dict(node: ast.expr, env: EnvScope) -> dict[str, str] | None:
    """Resolve um dicionário plano `{chave: valor}` para pares de strings.

    Args:
        node: Expressão AST candidata a dicionário.
        env: Escopo simbólico atual.

    Returns:
        Dicionário de strings ou `None` se não for um dicionário literal.
    """
    nested = eval_nested_dict(node, env)
    if nested is None:
        return None
    return {k: v for k, v in nested.items() if isinstance(v, str)}


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

    dict_val = eval_nested_dict(value, env)
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
