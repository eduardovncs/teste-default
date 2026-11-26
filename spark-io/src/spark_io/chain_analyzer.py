"""Análise estática de cadeias de chamadas Spark/Delta/Databricks na AST Python."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field

from spark_io.models import IoKind, TargetType
from spark_io.partial_eval import (
    WILDCARD,
    BuilderState,
    EnvScope,
    apply_env_prefix,
    eval_str_dict,
    eval_str_expr,
    eval_str_list,
)

_READ_TERMINALS = frozenset({"table", "load", "parquet", "csv", "json", "orc", "delta", "jdbc", "text"})
_WRITE_TERMINALS = frozenset(
    {"saveAsTable", "insertInto", "save", "parquet", "csv", "json", "orc", "delta", "jdbc", "toTable", "start"}
)
_TABLE_TERMINALS = frozenset({"table", "saveAsTable", "insertInto", "toTable"})
_WRITE_TO_TERMINALS = frozenset({"create", "append", "overwrite", "overwritePartitions", "replace", "createOrReplace"})
_DELTA_WRITE_METHODS = frozenset({"merge", "update", "delete"})
_TEMP_VIEW_METHODS = frozenset({"createOrReplaceTempView", "createTempView", "createGlobalTempView"})
_TARGET_KWARGS = ("path", "name", "tableName", "table")
_ANCHOR_SPECS: tuple[tuple[str, IoKind, frozenset[str]], ...] = (
    ("read", "read", _READ_TERMINALS),
    ("readStream", "read", _READ_TERMINALS),
    ("write", "write", _WRITE_TERMINALS),
    ("writeStream", "write", _WRITE_TERMINALS),
)


@dataclass(frozen=True)
class ChainStep:
    """Um passo de atributo ou chamada dentro de uma cadeia `a.b().c()`.

    Args:
        attr: Nome do atributo acessado.
        call: Nó `ast.Call` associado quando o atributo é invocado, ou `None`.
    """

    attr: str
    call: ast.Call | None = None


@dataclass(frozen=True)
class PrimitiveMatch:
    """Primitiva de I/O Spark/Delta detectada em uma chamada AST.

    Args:
        kind: Direção (`"read"` ou `"write"`).
        via: Nome padronizado da primitiva (ex.: `"read.table"`, `"write.saveAsTable"`).
        target_node: Nó AST do argumento de alvo, quando presente na chamada.
        target_value: Alvo principal já avaliado (ou `<*>` quando opaco).
        dynamic: Indica se o alvo principal ainda contém `<*>`.
        extra_targets: Alvos adicionais quando uma lista de caminhos é passada (ex.: `.parquet([a, b])`).
        target_type: Tipo do alvo (`"table"`, `"path"` ou `"unknown"`).
        receiver: Expressão textual do objeto receptor raiz.
        data_format: Formato de dados coletado via `.format(...)`.
        mode: Modo de escrita coletado via `.mode(...)`.
        options: Opções coletadas via `.option(...)` / `.options(...)`.
        raw_expr: Expressão fonte original do alvo quando não literal.
    """

    kind: IoKind
    via: str
    target_node: ast.expr | None
    target_value: str
    dynamic: bool
    extra_targets: tuple[str, ...] = ()
    target_type: TargetType = "table"
    receiver: str | None = None
    data_format: str | None = None
    mode: str | None = None
    options: dict[str, str] = field(default_factory=dict)
    raw_expr: str | None = None


@dataclass(frozen=True)
class NotebookRunMatch:
    """Chamada `dbutils.notebook.run(...)` extraída da AST.

    Args:
        target: Caminho resolvido (ou `<*>`) do notebook filho.
        dynamic: Indica se o caminho contém `<*>`.
        raw_expr: Expressão original do caminho.
        arguments: Argumentos literais passados ao notebook filho.
        line: Linha da chamada.
    """

    target: str
    dynamic: bool
    raw_expr: str
    arguments: dict[str, str]
    line: int


@dataclass
class _ChainOpts:
    """Opções acumuladas ao longo de uma cadeia `.format().mode().option()`."""

    data_format: str | None = None
    mode: str | None = None
    options: dict[str, str] = field(default_factory=dict)


def unwind_chain(node: ast.expr) -> tuple[ast.expr, list[ChainStep]]:
    """Desenrola uma cadeia de atributos/chamadas em `(raiz, passos)`.

    Args:
        node: Expressão AST a desenrolar.

    Returns:
        Tupla contendo o nó raiz e a lista ordenada de `ChainStep`.
    """
    steps: list[ChainStep] = []
    cur: ast.expr = node
    while True:
        if isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute):
            steps.append(ChainStep(attr=cur.func.attr, call=cur))
            cur = cur.func.value
        elif isinstance(cur, ast.Attribute):
            steps.append(ChainStep(attr=cur.attr, call=None))
            cur = cur.value
        else:
            break
    steps.reverse()
    return cur, steps


def _expr_text(node: ast.expr | None) -> str | None:
    """Serializa uma expressão AST não literal em texto legível.

    Args:
        node: Nó AST de expressão ou `None`.

    Returns:
        Texto via `ast.unparse`, ou `None` para literais string e `None`.
    """
    if node is None or (isinstance(node, ast.Constant) and isinstance(node.value, str)):
        return None
    return ast.unparse(node)


def _extract_call_arg(call: ast.Call, index: int = 0, kw_names: tuple[str, ...] = _TARGET_KWARGS) -> ast.expr | None:
    """Obtém argumento posicional ou nomeado de uma chamada AST.

    Args:
        call: Nó `ast.Call`.
        index: Posição do argumento esperado.
        kw_names: Nomes alternativos aceitos como keyword argument.

    Returns:
        Expressão AST encontrada ou `None`.
    """
    if len(call.args) > index:
        return call.args[index]
    for kw in call.keywords:
        if kw.arg in kw_names:
            return kw.value
    return None


def _merge_options_kwargs(call: ast.Call, env: EnvScope, out: dict[str, str]) -> None:
    """Extrai pares chave/valor de `.options(k=v)` ou `.options({...})`.

    Args:
        call: Chamada `.options(...)`.
        env: Escopo simbólico atual.
        out: Dicionário destino das opções.
    """
    if call.args:
        parsed = eval_str_dict(call.args[0], env)
        if parsed:
            out.update(parsed)
    for kw in call.keywords:
        if kw.arg:
            out[kw.arg] = eval_str_expr(kw.value, env)


def _clean_arg_str(call: ast.Call, index: int, env: EnvScope) -> str | None:
    """Avalia um argumento posicional de `call` retornando `None` se contiver `<*>`.

    Args:
        call: Nó `ast.Call`.
        index: Posição do argumento.
        env: Escopo simbólico atual.

    Returns:
        String limpa sem wildcard ou `None`.
    """
    if len(call.args) <= index:
        return None
    val = eval_str_expr(call.args[index], env)
    return None if WILDCARD in val else val


def _apply_chain_option_step(step: ChainStep, env: EnvScope, collected: _ChainOpts) -> None:
    """Aplica um passo `.format`, `.mode`, `.option` ou `.options` ao acumulador.

    Args:
        step: Passo da cadeia.
        env: Escopo simbólico atual.
        collected: Acumulador `_ChainOpts`.
    """
    call = step.call
    if call is None:
        return
    if step.attr == "format":
        collected.data_format = _clean_arg_str(call, 0, env) or collected.data_format
    elif step.attr == "mode":
        collected.mode = _clean_arg_str(call, 0, env) or collected.mode
    elif step.attr == "option" and len(call.args) >= 2:
        key = _clean_arg_str(call, 0, env)
        if key is not None:
            collected.options[key] = eval_str_expr(call.args[1], env)
    elif step.attr == "options":
        _merge_options_kwargs(call, env, collected.options)


def _collect_chain_options(steps: list[ChainStep], env: EnvScope) -> _ChainOpts:
    """Coleta `.format(...)`, `.mode(...)`, `.option(...)` e `.options(...)` na cadeia.

    Args:
        steps: Passos da cadeia desenrolada.
        env: Escopo simbólico atual.

    Returns:
        Instância de `_ChainOpts` preenchida.
    """
    collected = _ChainOpts()
    for step in steps:
        _apply_chain_option_step(step, env, collected)
    return collected


def _merge_builder_opts(base: BuilderState, extra: _ChainOpts) -> _ChainOpts:
    """Combina opções já acumuladas em um `BuilderState` com novas opções da cadeia.

    Args:
        base: Estado anterior do builder.
        extra: Novas opções coletadas na linha atual.

    Returns:
        `_ChainOpts` mesclado com precedência para `extra`.
    """
    merged_opts = dict(base.options)
    merged_opts.update(extra.options)
    return _ChainOpts(
        data_format=extra.data_format or base.data_format,
        mode=extra.mode or base.mode,
        options=merged_opts,
    )


def extract_builder_state(expr: ast.expr, env: EnvScope) -> BuilderState | None:
    """Detecta cadeias incompletas de reader/writer atribuídas a variáveis intermediárias.

    Args:
        expr: Expressão do lado direito de uma atribuição (ex.: `conector.read.format("parquet")`).
        env: Escopo simbólico atual.

    Returns:
        `BuilderState` atualizado ou `None` quando a expressão não for builder incompleto.
    """
    root, steps = unwind_chain(expr)
    if not steps:
        return None
    last_attr = steps[-1].attr

    if isinstance(root, ast.Name) and root.id in env.builders:
        base = env.builders[root.id]
        terminals = _READ_TERMINALS if base.kind == "read" else _WRITE_TERMINALS
        if steps[-1].call is not None and last_attr in terminals:
            return None
        merged = _merge_builder_opts(base, _collect_chain_options(steps, env))
        return BuilderState(
            kind=base.kind,
            anchor=base.anchor,
            receiver=base.receiver,
            data_format=merged.data_format,
            mode=merged.mode,
            options=merged.options,
        )

    return _extract_fresh_builder(root, steps, env)


def _extract_fresh_builder(root: ast.expr, steps: list[ChainStep], env: EnvScope) -> BuilderState | None:
    """Detecta o início de um builder `*.read.*` ou `*.write.*` sem chamada terminal.

    Args:
        root: Nó raiz da cadeia.
        steps: Passos desenrolados da cadeia.
        env: Escopo simbólico atual.

    Returns:
        Novo `BuilderState` ou `None`.
    """
    attrs = [s.attr for s in steps]
    last_is_call = steps[-1].call is not None
    last_attr = steps[-1].attr
    for anchor, kind, terminals in _ANCHOR_SPECS:
        if anchor not in attrs or (last_is_call and last_attr in terminals):
            continue
        idx = attrs.index(anchor)
        opts = _collect_chain_options(steps[idx + 1 :], env)
        root_text = ast.unparse(root)
        receiver = root_text if idx == 0 else f"{root_text}.{'.'.join(attrs[:idx])}"
        return BuilderState(
            kind=kind,
            anchor=anchor,
            receiver=receiver,
            data_format=opts.data_format,
            mode=opts.mode,
            options=opts.options,
        )
    return None


def _resolve_target_from_options(opts: _ChainOpts, env_prefix: str) -> tuple[str, bool, TargetType]:
    """Busca alvo alternativo nas opções `.option("path"/"dbtable"/"table", ...)`.

    Args:
        opts: Opções coletadas da cadeia.
        env_prefix: Prefixo padrão de ambiente (`pr`).

    Returns:
        Tupla `(target, dynamic, target_type)`.
    """
    for opt_key, t_type in (("path", "path"), ("dbtable", "table"), ("table", "table")):
        for key, val in opts.options.items():
            if key.lower() == opt_key:
                resolved, is_dyn = apply_env_prefix(val, env_prefix)
                return resolved, is_dyn, t_type
    return WILDCARD, True, "unknown"


def _resolve_target_and_type(
    arg_node: ast.expr | None,
    terminal: str,
    opts: _ChainOpts,
    env: EnvScope,
    env_prefix: str,
) -> tuple[str, tuple[str, ...], bool, TargetType, str | None]:
    """Resolve alvo(s), flag dinâmica, tipo e expressão original de um terminal read/write.

    Args:
        arg_node: Nó AST do argumento direto do terminal, se houver.
        terminal: Nome do método terminal (ex.: `"saveAsTable"`, `"load"`).
        opts: Opções coletadas na cadeia.
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente (`pr`).

    Returns:
        Tupla `(target_value, extra_targets, dynamic, target_type, raw_expr)`.
    """
    if arg_node is None:
        target, is_dyn, t_type = _resolve_target_from_options(opts, env_prefix)
        return target, (), is_dyn, t_type, None

    list_vals = eval_str_list(arg_node, env)
    if list_vals:
        resolved_items = [apply_env_prefix(item, env_prefix)[0] for item in list_vals]
        first = resolved_items[0]
        return first, tuple(resolved_items[1:]), (WILDCARD in first), "path", _expr_text(arg_node)

    raw_val = eval_str_expr(arg_node, env)
    resolved, is_dyn = apply_env_prefix(raw_val, env_prefix)
    target_type: TargetType = "table" if terminal in _TABLE_TERMINALS else "path"
    if resolved.startswith("/") or "://" in resolved:
        target_type = "path"
    return resolved, (), is_dyn, target_type, _expr_text(arg_node)


def _match_builder_terminal(
    root: ast.expr,
    steps: list[ChainStep],
    env: EnvScope,
    env_prefix: str,
) -> PrimitiveMatch | None:
    """Detecta chamada terminal sobre uma variável builder rastreada (ex.: `reader.load()`).

    Args:
        root: Nó raiz da cadeia.
        steps: Passos da cadeia.
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente (`pr`).

    Returns:
        `PrimitiveMatch` correspondente ou `None`.
    """
    if not isinstance(root, ast.Name) or root.id not in env.builders or not steps or steps[-1].call is None:
        return None
    base = env.builders[root.id]
    terminals = _READ_TERMINALS if base.kind == "read" else _WRITE_TERMINALS
    last_attr = steps[-1].attr
    if last_attr not in terminals:
        return None
    opts = _merge_builder_opts(base, _collect_chain_options(steps, env))
    arg_node = _extract_call_arg(steps[-1].call)
    target, extras, is_dyn, t_type, raw_expr = _resolve_target_and_type(arg_node, last_attr, opts, env, env_prefix)
    return PrimitiveMatch(
        kind=base.kind,
        via=f"{base.anchor}.{last_attr}",
        target_node=arg_node,
        target_value=target,
        extra_targets=extras,
        dynamic=is_dyn,
        target_type=t_type,
        receiver=base.receiver or root.id,
        data_format=opts.data_format,
        mode=opts.mode,
        options=opts.options,
        raw_expr=raw_expr,
    )


def _format_receiver(root: ast.expr, attrs: list[str], idx: int) -> str:
    """Formata o texto do objeto receptor antes da âncora `.read`/`.write`.

    Args:
        root: Nó raiz da cadeia.
        attrs: Lista de atributos da cadeia.
        idx: Índice da âncora `.read`/`.write`.

    Returns:
        Expressão textual do receptor.
    """
    root_text = ast.unparse(root)
    return root_text if idx == 0 else f"{root_text}.{'.'.join(attrs[:idx])}"


def _build_anchored_match(
    root: ast.expr,
    steps: list[ChainStep],
    anchor: str,
    kind: IoKind,
    env: EnvScope,
    env_prefix: str,
) -> PrimitiveMatch:
    """Constrói um `PrimitiveMatch` a partir de uma cadeia ancorada em `.read`/`.write`.

    Args:
        root: Nó raiz da cadeia.
        steps: Passos da cadeia desenrolada.
        anchor: Nome da âncora encontrada (`"read"`, `"write"`, etc.).
        kind: Direção (`"read"` ou `"write"`).
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente (`pr`).

    Returns:
        Instância preenchida de `PrimitiveMatch`.
    """
    attrs = [s.attr for s in steps]
    idx = attrs.index(anchor)
    last_step = steps[-1]
    opts = _collect_chain_options(steps[idx + 1 :], env)
    arg_node = _extract_call_arg(last_step.call) if last_step.call is not None else None
    target, extras, is_dyn, t_type, raw_expr = _resolve_target_and_type(arg_node, last_step.attr, opts, env, env_prefix)
    return PrimitiveMatch(
        kind=kind,
        via=f"{anchor}.{last_step.attr}",
        target_node=arg_node,
        target_value=target,
        extra_targets=extras,
        dynamic=is_dyn,
        target_type=t_type,
        receiver=_format_receiver(root, attrs, idx),
        data_format=opts.data_format,
        mode=opts.mode,
        options=opts.options,
        raw_expr=raw_expr,
    )


def _match_read_or_write_chain(
    root: ast.expr,
    steps: list[ChainStep],
    env: EnvScope,
    env_prefix: str,
) -> PrimitiveMatch | None:
    """Detecta cadeias `*.read.*`, `*.readStream.*`, `*.write.*`, `*.writeStream.*` e builders.

    Args:
        root: Nó raiz da cadeia.
        steps: Passos da cadeia desenrolada.
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente.

    Returns:
        `PrimitiveMatch` correspondente ou `None`.
    """
    builder_hit = _match_builder_terminal(root, steps, env, env_prefix)
    if builder_hit is not None or not steps or steps[-1].call is None:
        return builder_hit

    attrs = [s.attr for s in steps]
    last_attr = steps[-1].attr
    for anchor, kind, terminals in _ANCHOR_SPECS:
        if anchor in attrs and last_attr in terminals:
            return _build_anchored_match(root, steps, anchor, kind, env, env_prefix)
    return None


def _build_table_write_match(
    root: ast.expr,
    arg_node: ast.expr | None,
    via: str,
    mode: str | None,
    env: EnvScope,
    env_prefix: str,
) -> PrimitiveMatch:
    """Constrói um `PrimitiveMatch` de escrita em tabela (`writeTo` / `mergeInto`).

    Args:
        root: Nó raiz da cadeia.
        arg_node: Nó AST do argumento de tabela.
        via: Identificador da primitiva.
        mode: Modo de escrita associado.
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente.

    Returns:
        Instância preenchida de `PrimitiveMatch`.
    """
    raw_val = eval_str_expr(arg_node, env) if arg_node is not None else WILDCARD
    resolved, is_dyn = apply_env_prefix(raw_val, env_prefix)
    return PrimitiveMatch(
        kind="write",
        via=via,
        target_node=arg_node,
        target_value=resolved,
        dynamic=is_dyn,
        target_type="table",
        receiver=ast.unparse(root),
        mode=mode,
        raw_expr=_expr_text(arg_node),
    )


def _match_write_to_or_merge_into(
    root: ast.expr,
    steps: list[ChainStep],
    env: EnvScope,
    env_prefix: str,
) -> PrimitiveMatch | None:
    """Detecta cadeias `df.writeTo(alvo).*` e `df.mergeInto(alvo, ...).*`.

    Args:
        root: Nó raiz da cadeia.
        steps: Passos da cadeia.
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente.

    Returns:
        `PrimitiveMatch` de escrita ou `None`.
    """
    for idx, step in enumerate(steps):
        if step.call is None:
            continue
        if step.attr == "writeTo":
            has_term = idx < len(steps) - 1 and steps[-1].attr in _WRITE_TO_TERMINALS
            terminal = steps[-1].attr if has_term else "writeTo"
            mode = terminal if has_term else None
            return _build_table_write_match(
                root, _extract_call_arg(step.call), f"writeTo.{terminal}", mode, env, env_prefix
            )
        if step.attr == "mergeInto":
            return _build_table_write_match(root, _extract_call_arg(step.call), "mergeInto", "merge", env, env_prefix)
    return None


def _delta_operation(attrs: list[str]) -> tuple[IoKind, str, str | None] | None:
    """Identifica se uma cadeia Delta executa escrita (`merge`/`update`/`delete`) ou `toDF`.

    Args:
        attrs: Nomes de atributos na cadeia.

    Returns:
        Tupla `(kind, op_name, mode)` ou `None`.
    """
    write_op = next((m for m in _DELTA_WRITE_METHODS if m in attrs), None)
    if write_op is not None:
        return "write", write_op, write_op
    if "toDF" in attrs:
        return "read", "toDF", None
    return None


def _match_delta_table_chain(
    root: ast.expr,
    steps: list[ChainStep],
    env: EnvScope,
    env_prefix: str,
) -> PrimitiveMatch | None:
    """Detecta operações em `DeltaTable` (inline ou via variável rastreada).

    Args:
        root: Nó raiz da cadeia.
        steps: Passos da cadeia.
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente.

    Returns:
        `PrimitiveMatch` de escrita/leitura Delta ou `None`.
    """
    op = _delta_operation([s.attr for s in steps])
    if op is None:
        return None
    kind, op_name, mode = op

    if isinstance(root, ast.Name) and root.id in env.delta_tables:
        target, is_dyn, raw_expr = env.delta_tables[root.id]
        t_type: TargetType = "path" if target.startswith("/") else "table"
        return PrimitiveMatch(
            kind=kind,
            via=f"delta.{op_name}",
            target_node=None,
            target_value=target,
            dynamic=is_dyn,
            target_type=t_type,
            receiver=root.id,
            mode=mode,
            raw_expr=raw_expr,
        )

    inline_delta = extract_delta_binding_from_steps(root, steps, env, env_prefix)
    if inline_delta is None:
        return None
    target, is_dyn, raw_expr, arg_node, t_type = inline_delta
    return PrimitiveMatch(
        kind=kind,
        via=f"delta.{op_name}",
        target_node=arg_node,
        target_value=target,
        dynamic=is_dyn,
        target_type=t_type,
        receiver="DeltaTable",
        mode=mode,
        raw_expr=raw_expr,
    )


def extract_delta_binding_from_steps(
    root: ast.expr,
    steps: list[ChainStep],
    env: EnvScope,
    env_prefix: str,
) -> tuple[str, bool, str | None, ast.expr | None, TargetType] | None:
    """Extrai alvo de `DeltaTable.forName(spark, tbl)` ou `DeltaTable.forPath(spark, path)`.

    Args:
        root: Nó raiz da expressão.
        steps: Passos da cadeia desenrolada.
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente.

    Returns:
        Tupla `(target, dynamic, raw_expr, arg_node, target_type)` ou `None`.
    """
    is_delta = isinstance(root, ast.Name) and root.id == "DeltaTable"
    if not is_delta or not steps or steps[0].call is None or steps[0].attr not in ("forName", "forPath"):
        return None
    call = steps[0].call
    arg_node = _extract_call_arg(call, index=1, kw_names=("tableOrViewName", "path", "name"))
    if arg_node is None and len(call.args) == 1:
        arg_node = call.args[0]
    raw_val = eval_str_expr(arg_node, env) if arg_node is not None else WILDCARD
    resolved, is_dyn = apply_env_prefix(raw_val, env_prefix)
    t_type: TargetType = "path" if steps[0].attr == "forPath" else "table"
    return resolved, is_dyn, _expr_text(arg_node), arg_node, t_type


def match_spark_primitive(call: ast.Call, env: EnvScope, env_prefix: str) -> PrimitiveMatch | None:
    """Identifica se uma chamada AST corresponde a uma primitiva Spark/Delta de I/O.

    Args:
        call: Nó `ast.Call` a inspecionar.
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente (`pr`).

    Returns:
        `PrimitiveMatch` quando reconhecida, ou `None`.
    """
    root, steps = unwind_chain(call)
    if not steps:
        return None

    for matcher in (_match_read_or_write_chain, _match_write_to_or_merge_into, _match_delta_table_chain):
        matched = matcher(root, steps, env, env_prefix)
        if matched is not None:
            return matched

    if len(steps) == 1 and steps[0].attr == "table" and steps[0].call is not None:
        arg_node = _extract_call_arg(steps[0].call)
        if arg_node is not None:
            raw_val = eval_str_expr(arg_node, env)
            resolved, is_dyn = apply_env_prefix(raw_val, env_prefix)
            return PrimitiveMatch(
                kind="read",
                via="table",
                target_node=arg_node,
                target_value=resolved,
                dynamic=is_dyn,
                target_type="table",
                receiver=ast.unparse(root),
                raw_expr=_expr_text(arg_node),
            )
    return None


def match_sql_call(call: ast.Call, env: EnvScope, env_prefix: str) -> str | None:
    """Identifica chamadas `*.sql(query)` e devolve a query parcialmente avaliada.

    Args:
        call: Nó `ast.Call` candidato.
        env: Escopo simbólico atual.
        env_prefix: Prefixo padrão de ambiente (`pr`).

    Returns:
        Query SQL avaliada (com prefixo aplicado) ou `None` se não for `.sql(...)`.
    """
    if not isinstance(call.func, ast.Attribute) or call.func.attr != "sql":
        return None
    arg = _extract_call_arg(call, index=0, kw_names=("sqlQuery", "query"))
    if arg is None:
        return None
    raw = eval_str_expr(arg, env)
    if raw == WILDCARD:
        return None
    resolved, _ = apply_env_prefix(raw, env_prefix)
    return resolved


def match_temp_view_call(call: ast.Call, env: EnvScope) -> str | None:
    """Detecta chamadas `createOrReplaceTempView` / `createTempView` / `createGlobalTempView`.

    Args:
        call: Nó `ast.Call` candidato.
        env: Escopo simbólico atual.

    Returns:
        Nome em minúsculas da view temporária criada, ou `None`.
    """
    if not isinstance(call.func, ast.Attribute) or call.func.attr not in _TEMP_VIEW_METHODS or not call.args:
        return None
    name = eval_str_expr(call.args[0], env)
    return None if WILDCARD in name or not name else name.lower()


def match_notebook_run_call(call: ast.Call, env: EnvScope) -> NotebookRunMatch | None:
    """Detecta chamadas `dbutils.notebook.run(path, timeout, arguments)`.

    Args:
        call: Nó `ast.Call` candidato.
        env: Escopo simbólico atual.

    Returns:
        Instância de `NotebookRunMatch` ou `None`.
    """
    _, steps = unwind_chain(call)
    attrs = [s.attr for s in steps]
    if len(attrs) < 2 or attrs[-2:] != ["notebook", "run"]:
        return None
    path_node = _extract_call_arg(call, index=0, kw_names=("path",))
    if path_node is None:
        return None
    target = eval_str_expr(path_node, env)
    args_node = _extract_call_arg(call, index=2, kw_names=("arguments",))
    arguments = eval_str_dict(args_node, env) if args_node is not None else None
    return NotebookRunMatch(
        target=target,
        dynamic=WILDCARD in target,
        raw_expr=ast.unparse(path_node),
        arguments=arguments or {},
        line=call.lineno,
    )
