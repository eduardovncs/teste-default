"""Orquestrador principal do mapeador estático de entradas e saídas Spark."""

from __future__ import annotations

import ast
import copy
from dataclasses import dataclass, field
from pathlib import Path

from spark_io.chain_analyzer import (
    NotebookRunMatch,
    PrimitiveMatch,
    extract_builder_state,
    extract_delta_binding_from_steps,
    match_notebook_run_call,
    match_spark_primitive,
    match_sql_call,
    match_temp_view_call,
    unwind_chain,
)
from spark_io.models import (
    CustomWrapperSpec,
    DependencyEdge,
    ExternalDependency,
    IoEntry,
    IoKind,
    MapResult,
    UnclassifiedTableRef,
    UnresolvedDynamic,
)
from spark_io.partial_eval import (
    WILDCARD,
    EnvScope,
    FuncSummary,
    apply_env_prefix,
    bind_assignment,
    eval_nested_dict,
    eval_str_expr,
    eval_str_list,
    is_table_pattern,
)
from spark_io.safety_net import audit_missed_io
from spark_io.source_loader import ParsedDocument, load_document
from spark_io.sql_extractor import extract_sql_io
from spark_io.wrapper_finder import InternalWrapper, ModuleAst, discover_func_summaries, discover_wrappers

_SUPPORTED_SUFFIXES = frozenset({".py", ".ipynb", ".sql"})
_IGNORED_DIRS = frozenset({".venv", ".git", "__pycache__", ".pytest_cache", ".ruff_cache", "node_modules"})
_IGNORED_CALL_NAMES = frozenset(
    {
        "getLogger",
        "import_module",
        "getattr",
        "setattr",
        "hasattr",
        "isinstance",
        "print",
        "Path",
        "MongoClient",
        "insert_one",
        "insert_many",
        "update_one",
        "update_many",
        "replace_one",
        "bulk_write",
        "get",
        "post",
        "put",
        "delete",
        "alias",
        "col",
        "select",
        "filter",
        "where",
        "join",
        "withColumn",
        "withColumnRenamed",
        "cast",
        "option",
        "options",
        "format",
        "mode",
        "createOrReplaceTempView",
        "createTempView",
        "createGlobalTempView",
    }
)


@dataclass
class _MapperContext:
    """Estado compartilhado de uma execução de `map_spark_io`."""

    project_root: Path
    env_prefix: str
    user_vars: dict[str, str]
    custom_wrappers: dict[str, CustomWrapperSpec]
    result: MapResult
    docs: dict[Path, ParsedDocument] = field(default_factory=dict)
    trees: dict[Path, ast.Module] = field(default_factory=dict)
    wrappers: dict[str, InternalWrapper] = field(default_factory=dict)
    func_summaries: dict[str, FuncSummary] = field(default_factory=dict)
    visited_files: set[Path] = field(default_factory=set)
    visited_calls: set[tuple[Path, str, tuple[tuple[str, str], ...]]] = field(default_factory=set)
    local_func_defs: dict[Path, dict[str, ast.FunctionDef | ast.AsyncFunctionDef]] = field(default_factory=dict)
    called_local_funcs: dict[Path, set[str]] = field(default_factory=dict)
    analyzed_wrappers: set[str] = field(default_factory=set)
    covered_lines: dict[str, set[int]] = field(default_factory=dict)
    seen_io: set[tuple[str, str, str, int]] = field(default_factory=set)
    seen_refs: set[tuple[str, str, str, int]] = field(default_factory=set)
    seen_unresolved: set[tuple[str, int, str]] = field(default_factory=set)


@dataclass(frozen=True)
class _ResolvedWrapper:
    """Especificação unificada de wrapper interno ou customizado."""

    kind: IoKind
    param_idx: int | None
    param_name: str | None
    default_mode: str | None
    default_format: str | None
    default_options: dict[str, str]


def _new_env(ctx: _MapperContext, *, widgets: dict[str, str] | None = None) -> EnvScope:
    """Cria um novo `EnvScope` herdando variáveis, prefixo de ambiente e resumos de funções.

    Args:
        ctx: Contexto compartilhado do mapeador.
        widgets: Argumentos opcionais de widgets injetados por `dbutils.notebook.run`.

    Returns:
        Nova instância configurada de `EnvScope`.
    """
    return EnvScope(
        widgets=dict(widgets or {}),
        user_vars=dict(ctx.user_vars),
        env_prefix=ctx.env_prefix,
        func_summaries=ctx.func_summaries,
    )


def _mark_covered(ctx: _MapperContext, rel_file: str, line: int) -> None:
    """Marca uma linha de arquivo como já contabilizada pela Camada 1.

    Args:
        ctx: Contexto compartilhado do mapeador.
        rel_file: Caminho relativo do arquivo.
        line: Número da linha (1-based).
    """
    ctx.covered_lines.setdefault(rel_file, set()).add(line)


def _record_unresolved(ctx: _MapperContext, item: UnresolvedDynamic) -> None:
    """Registra uma expressão dinâmica não resolvida sem duplicar por `(file, line, kind)`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        item: Instância de `UnresolvedDynamic`.
    """
    key = (item.file, item.line, item.kind)
    if key in ctx.seen_unresolved:
        return
    ctx.seen_unresolved.add(key)
    _mark_covered(ctx, item.file, item.line)
    ctx.result.unresolved_dynamic.append(item)


def _rel_str(root: Path, file_path: Path) -> str:
    """Retorna o caminho relativo normalizado com barras `/`.

    Args:
        root: Diretório raiz do projeto.
        file_path: Caminho absoluto do arquivo.

    Returns:
        Caminho relativo usando `/`.
    """
    try:
        return file_path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return file_path.name


def _resolve_notebook_path(root: Path, current_file: Path, raw_target: str) -> Path | None:
    """Resolve caminho de `%run` ou `dbutils.notebook.run` para arquivo local.

    Args:
        root: Diretório raiz do projeto.
        current_file: Arquivo onde a referência foi declarada.
        raw_target: Caminho alvo informado no código.

    Returns:
        Caminho absoluto resolvido se existir localmente, ou `None` se externo.
    """
    cleaned = raw_target.strip()
    if not cleaned:
        return None
    base = root if cleaned.startswith("/") else current_file.parent
    rel_part = cleaned.lstrip("/") if cleaned.startswith("/") else cleaned
    candidate = (base / rel_part).resolve()

    if candidate.is_file():
        return candidate
    for ext in (".py", ".ipynb", ".sql"):
        with_ext = candidate.with_suffix(ext)
        if with_ext.is_file():
            return with_ext
    return None


def _resolve_import_module(root: Path, module_name: str) -> Path | None:
    """Resolve um módulo Python importado (`src.lib`) para arquivo dentro de `root`.

    Args:
        root: Diretório raiz do projeto.
        module_name: Nome pontuado do módulo (ex.: `"src.config"`).

    Returns:
        Caminho do arquivo `.py` local correspondente, ou `None` se externo.
    """
    parts = [p for p in module_name.split(".") if p]
    if not parts:
        return None
    rel = Path(*parts)
    py_file = (root / rel).with_suffix(".py")
    if py_file.is_file():
        return py_file.resolve()
    pkg_init = root / rel / "__init__.py"
    return pkg_init.resolve() if pkg_init.is_file() else None


def _load_and_parse(ctx: _MapperContext, file_path: Path) -> tuple[ParsedDocument, ast.Module | None]:
    """Carrega e faz parse AST de um arquivo com cache no contexto.

    Args:
        ctx: Contexto compartilhado do mapeador.
        file_path: Caminho absoluto do arquivo.

    Returns:
        Tupla `(ParsedDocument, ast.Module | None)`.
    """
    resolved = file_path.resolve()
    if resolved not in ctx.docs:
        doc = load_document(resolved)
        ctx.docs[resolved] = doc
        ctx.result.warnings.extend(doc.warnings)
        if doc.python_source.strip():
            try:
                ctx.trees[resolved] = ast.parse(doc.python_source, filename=str(resolved))
            except SyntaxError as exc:
                ctx.result.warnings.append(f"SyntaxError ao analisar {_rel_str(ctx.project_root, resolved)}: {exc}")
    return ctx.docs[resolved], ctx.trees.get(resolved)


def _enqueue_if_new(target: Path | None, seen: set[Path], queue: list[Path]) -> None:
    """Adiciona um caminho à fila de descoberta quando ainda não visitado.

    Args:
        target: Caminho candidato ou `None`.
        seen: Conjunto de caminhos já visitados.
        queue: Fila de caminhos pendentes.
    """
    if target is not None and target not in seen:
        queue.append(target)


def _enqueue_import_node(ctx: _MapperContext, node: ast.AST, seen: set[Path], queue: list[Path]) -> None:
    """Enfileira módulos locais referenciados por `import` ou `from ... import`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        node: Nó AST candidato.
        seen: Conjunto de arquivos já visitados.
        queue: Fila de arquivos pendentes.
    """
    if isinstance(node, ast.ImportFrom) and node.module:
        _enqueue_if_new(_resolve_import_module(ctx.project_root, node.module), seen, queue)
        for alias in node.names:
            _enqueue_if_new(_resolve_import_module(ctx.project_root, f"{node.module}.{alias.name}"), seen, queue)
    elif isinstance(node, ast.Import):
        for alias in node.names:
            _enqueue_if_new(_resolve_import_module(ctx.project_root, alias.name), seen, queue)


def _enqueue_ast_references(
    ctx: _MapperContext,
    cur: Path,
    tree: ast.Module,
    seen: set[Path],
    queue: list[Path],
) -> None:
    """Enfileira módulos importados e notebooks chamados em `dbutils.notebook.run`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        cur: Arquivo atual.
        tree: AST do arquivo atual.
        seen: Conjunto de arquivos já enfileirados.
        queue: Fila de arquivos pendentes.
    """
    blank_env = EnvScope(user_vars=ctx.user_vars)
    for node in ast.walk(tree):
        _enqueue_import_node(ctx, node, seen, queue)
        if isinstance(node, ast.Call):
            nb_run = match_notebook_run_call(node, blank_env)
            if nb_run and not nb_run.dynamic:
                _enqueue_if_new(_resolve_notebook_path(ctx.project_root, cur, nb_run.target), seen, queue)


def _collect_reachable_modules(ctx: _MapperContext, start_files: list[Path]) -> list[ModuleAst]:
    """Descobre todos os arquivos Python alcançáveis para propagar wrappers multi-hop.

    Args:
        ctx: Contexto compartilhado do mapeador.
        start_files: Arquivos iniciais (entrypoint ou todos os arquivos do diretório).

    Returns:
        Lista de `ModuleAst` alcançáveis a partir de `start_files`.
    """
    queue = [f.resolve() for f in start_files]
    seen: set[Path] = set()
    modules: list[ModuleAst] = []

    while queue:
        cur = queue.pop(0)
        if cur in seen or not cur.is_file():
            continue
        seen.add(cur)
        doc, tree = _load_and_parse(ctx, cur)
        for directive in doc.run_directives:
            _enqueue_if_new(_resolve_notebook_path(ctx.project_root, cur, directive.target), seen, queue)
        if tree is None:
            continue
        modules.append(ModuleAst(rel_path=_rel_str(ctx.project_root, cur), tree=tree))
        _enqueue_ast_references(ctx, cur, tree, seen, queue)

    return modules


def _record_io(ctx: _MapperContext, entry: IoEntry) -> None:
    """Registra uma leitura ou escrita evitando duplicatas exatas.

    Args:
        ctx: Contexto compartilhado do mapeador.
        entry: Entrada de I/O a adicionar.
    """
    _mark_covered(ctx, entry.file, entry.line)
    key = (entry.kind, entry.target, entry.file, entry.line)
    if key in ctx.seen_io:
        return
    ctx.seen_io.add(key)
    target_list = ctx.result.inputs if entry.kind == "read" else ctx.result.outputs
    target_list.append(entry)


def _record_sql_block(
    ctx: _MapperContext,
    sql: str,
    rel_file: str,
    line: int,
    temp_views: set[str],
) -> None:
    """Extrai e registra entradas e saídas de um bloco SQL via `sqlglot`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        sql: Texto SQL a analisar.
        rel_file: Caminho relativo do arquivo de origem.
        line: Linha inicial do bloco SQL.
        temp_views: Conjunto mutável de temp views conhecidas no escopo atual.
    """
    _mark_covered(ctx, rel_file, line)
    extracted = extract_sql_io(sql, env_prefix=ctx.env_prefix, temp_views=temp_views)
    temp_views.update(extracted.created_temp_views)
    ctx.result.warnings.extend(extracted.warnings)
    for hit in extracted.hits:
        kind: IoKind = "write" if hit.kind == "write" else "read"
        if hit.table == WILDCARD:
            _record_unresolved(ctx, UnresolvedDynamic(file=rel_file, line=line, call=hit.via, kind=kind, expr=sql))
            continue
        _record_io(
            ctx,
            IoEntry(
                target=hit.table,
                kind=kind,
                via=hit.via,
                file=rel_file,
                line=line,
                dynamic=hit.dynamic,
                target_type="table",
            ),
        )


def _collect_module_env(ctx: _MapperContext, tree: ast.Module) -> EnvScope:
    """Extrai constantes literais de nível superior de um módulo Python.

    Args:
        ctx: Contexto compartilhado do mapeador.
        tree: AST do módulo.

    Returns:
        `EnvScope` contendo constantes literais do módulo.
    """
    mod_env = _new_env(ctx)
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
            bind_assignment(stmt.targets[0], stmt.value, mod_env)
        elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            bind_assignment(stmt.target, stmt.value, mod_env)
    return mod_env


def _copy_env_subset(src: EnvScope, dst: EnvScope, names: set[str] | None) -> None:
    """Copia constantes selecionadas de `src` para `dst`.

    Args:
        src: Escopo de origem.
        dst: Escopo de destino.
        names: Nomes permitidos, ou `None` para importar tudo.
    """
    for key, val in src.strings.items():
        if names is None or key in names:
            dst.strings[key] = val
    for key, val in src.dicts.items():
        if names is None or key in names:
            dst.dicts[key] = copy.deepcopy(val)
    for key, val in src.lists.items():
        if names is None or key in names:
            dst.lists[key] = list(val)


def _handle_import_from(ctx: _MapperContext, stmt: ast.ImportFrom, env: EnvScope) -> None:
    """Carrega constantes e registra símbolos importados de módulos locais.

    Args:
        ctx: Contexto compartilhado do mapeador.
        stmt: Declaração `ast.ImportFrom`.
        env: Escopo simbólico destino.
    """
    if not stmt.module:
        return
    mod_path = _resolve_import_module(ctx.project_root, stmt.module)
    if mod_path is None:
        return
    _, tree = _load_and_parse(ctx, mod_path)
    if tree is None:
        return
    wanted = None if any(a.name == "*" for a in stmt.names) else {a.name for a in stmt.names}
    _copy_env_subset(_collect_module_env(ctx, tree), env, wanted)
    for alias in stmt.names:
        if alias.name != "*":
            local_name = alias.asname or alias.name
            env.imported_symbols[local_name] = (stmt.module, alias.name)


def _handle_plain_import(ctx: _MapperContext, stmt: ast.Import, env: EnvScope) -> None:
    """Registra módulos locais importados via `import mod [as alias]`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        stmt: Declaração `ast.Import`.
        env: Escopo simbólico destino.
    """
    for alias in stmt.names:
        if _resolve_import_module(ctx.project_root, alias.name) is not None:
            local_name = alias.asname or alias.name
            env.imported_modules[local_name] = alias.name


def _handle_primitive_match(
    ctx: _MapperContext,
    prim: PrimitiveMatch,
    rel_file: str,
    line: int,
) -> None:
    """Registra uma primitiva Spark/Delta encontrada (ou `unresolved_dynamic` se opaca).

    Args:
        ctx: Contexto compartilhado do mapeador.
        prim: Primitiva identificada pelo `chain_analyzer`.
        rel_file: Caminho relativo do arquivo atual.
        line: Linha da chamada.
    """
    if prim.target_value == WILDCARD:
        _record_unresolved(
            ctx,
            UnresolvedDynamic(
                file=rel_file,
                line=line,
                call=prim.via,
                kind=prim.kind,
                expr=prim.raw_expr or WILDCARD,
            ),
        )
        return
    for target in (prim.target_value, *prim.extra_targets):
        _record_io(
            ctx,
            IoEntry(
                target=target,
                kind=prim.kind,
                via=prim.via,
                file=rel_file,
                line=line,
                receiver=prim.receiver,
                data_format=prim.data_format,
                mode=prim.mode,
                options=dict(prim.options),
                target_type=prim.target_type,
                dynamic=(WILDCARD in target),
                raw_expr=prim.raw_expr,
            ),
        )


def _resolve_wrapper_spec(ctx: _MapperContext, callee: str) -> _ResolvedWrapper | None:
    """Busca especificação de wrapper interno ou customizado pelo nome da função.

    Args:
        ctx: Contexto compartilhado do mapeador.
        callee: Nome simples da função chamada.

    Returns:
        `_ResolvedWrapper` unificado ou `None`.
    """
    internal = ctx.wrappers.get(callee)
    if internal is not None:
        kind: IoKind = "write" if internal.info.kind == "write" else "read"
        return _ResolvedWrapper(
            kind=kind,
            param_idx=internal.info.target_param_index,
            param_name=internal.info.target_param,
            default_mode=internal.default_mode,
            default_format=internal.default_format,
            default_options=dict(internal.default_options),
        )
    custom = ctx.custom_wrappers.get(callee)
    if custom is None:
        return None
    return _ResolvedWrapper(
        kind=custom.kind,
        param_idx=custom.target_param_index,
        param_name=custom.target_param,
        default_mode=None,
        default_format=None,
        default_options={},
    )


def _pick_wrapper_arg(call: ast.Call, param_idx: int | None, param_name: str | None) -> ast.expr | None:
    """Seleciona o argumento de alvo de uma chamada de wrapper.

    Args:
        call: Nó `ast.Call`.
        param_idx: Índice posicional esperado.
        param_name: Nome do parâmetro esperado.

    Returns:
        Expressão AST do argumento ou `None`.
    """
    if param_name:
        for kw in call.keywords:
            if kw.arg == param_name:
                return kw.value
    if param_idx is not None and len(call.args) > param_idx:
        return call.args[param_idx]
    return None


def _extract_kw_str(call: ast.Call, names: tuple[str, ...], env: EnvScope) -> str | None:
    """Extrai valor string de um keyword argument opcional (ex.: `modo="append"`).

    Args:
        call: Nó `ast.Call`.
        names: Nomes de keyword aceitos.
        env: Escopo simbólico atual.

    Returns:
        Valor string resolvido ou `None`.
    """
    for kw in call.keywords:
        if kw.arg in names:
            val = eval_str_expr(kw.value, env)
            if WILDCARD not in val:
                return val
    return None


def _analyze_wrapper_fixed_io(ctx: _MapperContext, wrapper: InternalWrapper) -> None:
    """Analisa uma vez o corpo de um wrapper chamado para capturar tabelas fixas internas.

    Args:
        ctx: Contexto compartilhado do mapeador.
        wrapper: Wrapper interno efetivamente invocado.
    """
    if wrapper.info.name in ctx.analyzed_wrappers or wrapper.fn_node is None:
        return
    ctx.analyzed_wrappers.add(wrapper.info.name)
    wrapper_path = (ctx.project_root / wrapper.info.file).resolve()
    _, tree = _load_and_parse(ctx, wrapper_path)
    base_env = _collect_module_env(ctx, tree) if tree is not None else _new_env(ctx)
    temp_views: set[str] = set()
    for stmt in wrapper.fn_node.body:
        if stmt.lineno in wrapper.param_io_lines:
            continue
        _walk_single_statement(ctx, stmt, wrapper_path, base_env, temp_views)


def _handle_wrapper_call(
    ctx: _MapperContext,
    call: ast.Call,
    callee: str,
    env: EnvScope,
    rel_file: str,
) -> bool:
    """Resolve chamadas a wrappers internos do repo ou `custom_wrappers`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        call: Nó `ast.Call`.
        callee: Nome simples da função chamada.
        env: Escopo simbólico atual.
        rel_file: Caminho relativo do arquivo atual.

    Returns:
        `True` se a chamada foi tratada como wrapper conhecido.
    """
    spec = _resolve_wrapper_spec(ctx, callee)
    if spec is None:
        return False

    internal = ctx.wrappers.get(callee)
    if internal is not None:
        _analyze_wrapper_fixed_io(ctx, internal)

    arg_node = _pick_wrapper_arg(call, spec.param_idx, spec.param_name)
    if arg_node is None:
        return True
    resolved, is_dyn = apply_env_prefix(eval_str_expr(arg_node, env), ctx.env_prefix)
    raw_expr = None if isinstance(arg_node, ast.Constant) else ast.unparse(arg_node)

    if resolved == WILDCARD:
        _record_unresolved(
            ctx,
            UnresolvedDynamic(file=rel_file, line=call.lineno, call=callee, kind=spec.kind, expr=raw_expr or WILDCARD),
        )
        return True

    call_mode = _extract_kw_str(call, ("mode", "modo"), env) or spec.default_mode
    _record_io(
        ctx,
        IoEntry(
            target=resolved,
            kind=spec.kind,
            via=f"wrapper:{callee}",
            file=rel_file,
            line=call.lineno,
            data_format=spec.default_format,
            mode=call_mode,
            options=dict(spec.default_options),
            target_type="path" if resolved.startswith("/") else "table",
            dynamic=is_dyn,
            raw_expr=raw_expr,
        ),
    )
    return True


def _resolve_imported_target(call: ast.Call, env: EnvScope) -> tuple[str, str] | None:
    """Identifica se `call` invoca uma função/classe importada de módulo local.

    Args:
        call: Nó `ast.Call`.
        env: Escopo simbólico atual.

    Returns:
        Tupla `(nome_modulo, nome_simbolo)` ou `None`.
    """
    if isinstance(call.func, ast.Name) and call.func.id in env.imported_symbols:
        return env.imported_symbols[call.func.id]
    if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name):
        mod_name = env.imported_modules.get(call.func.value.id)
        if mod_name is not None:
            return mod_name, call.func.attr
    return None


def _bind_arg_value(param_name: str, arg_node: ast.expr, caller_env: EnvScope, callee_env: EnvScope) -> str:
    """Avalia um argumento no escopo chamador e vincula ao parâmetro no escopo chamado.

    Args:
        param_name: Nome do parâmetro na função chamada.
        arg_node: Expressão AST passada na chamada.
        caller_env: Escopo simbólico do chamador.
        callee_env: Escopo simbólico da função chamada.

    Returns:
        Representação resumida em string para compor a assinatura da chamada.
    """
    nested = eval_nested_dict(arg_node, caller_env)
    if nested is not None:
        callee_env.dicts[param_name] = nested
        return "<dict>"
    list_val = eval_str_list(arg_node, caller_env)
    if list_val is not None:
        callee_env.lists[param_name] = list_val
        return ",".join(list_val)
    str_val = eval_str_expr(arg_node, caller_env)
    callee_env.strings[param_name] = str_val
    return str_val


def _bind_default_args(
    fn: ast.FunctionDef | ast.AsyncFunctionDef,
    callee_env: EnvScope,
    sig: dict[str, str],
) -> None:
    """Vincula defaults literais de string de uma função ao escopo do callee.

    Args:
        fn: Definição da função chamada.
        callee_env: Escopo simbólico da função chamada.
        sig: Dicionário acumulador da assinatura da chamada.
    """
    offset = len(fn.args.args) - len(fn.args.defaults)
    for idx, default_node in enumerate(fn.args.defaults):
        if isinstance(default_node, ast.Constant) and isinstance(default_node.value, str):
            p_name = fn.args.args[offset + idx].arg
            callee_env.strings[p_name] = default_node.value
            sig[p_name] = default_node.value


def _bind_call_arguments(
    fn: ast.FunctionDef | ast.AsyncFunctionDef,
    call: ast.Call,
    caller_env: EnvScope,
    callee_env: EnvScope,
) -> tuple[tuple[str, str], ...]:
    """Vincula defaults, argumentos posicionais e nomeados da chamada ao escopo do callee.

    Args:
        fn: Definição da função chamada.
        call: Nó `ast.Call` no ponto da chamada.
        caller_env: Escopo simbólico do chamador.
        callee_env: Escopo simbólico da função chamada.

    Returns:
        Tupla ordenada `(param, resumo)` identificando esta invocação.
    """
    params = [a.arg for a in fn.args.args]
    if params and params[0] in ("self", "cls"):
        params = params[1:]

    sig: dict[str, str] = {}
    _bind_default_args(fn, callee_env, sig)

    for idx, arg_node in enumerate(call.args):
        if idx < len(params):
            sig[params[idx]] = _bind_arg_value(params[idx], arg_node, caller_env, callee_env)

    for kw in call.keywords:
        if kw.arg:
            sig[kw.arg] = _bind_arg_value(kw.arg, kw.value, caller_env, callee_env)

    return tuple(sorted(sig.items()))


def _execute_symbol_body(
    ctx: _MapperContext,
    mod_path: Path,
    target_node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
    call: ast.Call,
    callee_env: EnvScope,
    caller_env: EnvScope,
) -> None:
    """Percorre o corpo de uma função ou classe chamada vinculando argumentos com guarda de ciclo.

    Args:
        ctx: Contexto compartilhado do mapeador.
        mod_path: Arquivo onde o símbolo está definido.
        target_node: Definição AST da função ou classe.
        call: Chamada AST no ponto de invocação.
        callee_env: Escopo base da função/classe chamada.
        caller_env: Escopo do chamador para avaliação dos argumentos.
    """
    bound_sig: tuple[tuple[str, str], ...] = ()
    if isinstance(target_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        bound_sig = _bind_call_arguments(target_node, call, caller_env, callee_env)

    key = (mod_path, target_node.name, bound_sig)
    if key in ctx.visited_calls:
        return
    ctx.visited_calls.add(key)
    temp_views: set[str] = set()
    _walk_statements(ctx, target_node.body, mod_path, callee_env, temp_views)
    _run_safety_audit(ctx, target_node, _rel_str(ctx.project_root, mod_path))


def _analyze_imported_symbol(
    ctx: _MapperContext,
    mod_name: str,
    sym_name: str,
    call: ast.Call,
    caller_env: EnvScope,
) -> bool:
    """Analisa estaticamente apenas a função ou classe importada de um módulo local.

    Args:
        ctx: Contexto compartilhado do mapeador.
        mod_name: Nome pontuado do módulo local.
        sym_name: Nome da função ou classe invocada.
        call: Nó `ast.Call` no ponto da chamada.
        caller_env: Escopo simbólico do chamador.

    Returns:
        `True` quando o símbolo local foi encontrado e processado.
    """
    mod_path = _resolve_import_module(ctx.project_root, mod_name)
    if mod_path is None:
        return False
    _, tree = _load_and_parse(ctx, mod_path)
    if tree is None:
        return False

    target_node = next(
        (
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name == sym_name
        ),
        None,
    )
    if target_node is None:
        return False

    _execute_symbol_body(ctx, mod_path, target_node, call, _collect_module_env(ctx, tree), caller_env)
    return True


def _analyze_local_function_call(
    ctx: _MapperContext,
    cur_file: Path,
    callee: str,
    call: ast.Call,
    caller_env: EnvScope,
) -> bool:
    """Analisa uma função definida no mesmo arquivo vinculando os argumentos passados.

    Args:
        ctx: Contexto compartilhado do mapeador.
        cur_file: Arquivo atual.
        callee: Nome simples da função chamada.
        call: Nó `ast.Call`.
        caller_env: Escopo simbólico do chamador.

    Returns:
        `True` quando uma função local elegível foi analisada.
    """
    if callee in ctx.func_summaries:
        return False
    fn_node = ctx.local_func_defs.get(cur_file.resolve(), {}).get(callee)
    if fn_node is None:
        return False
    _execute_symbol_body(ctx, cur_file.resolve(), fn_node, call, caller_env.copy(), caller_env)
    return True


def _check_unclassified_table_ref(
    ctx: _MapperContext,
    call: ast.Call,
    callee: str,
    env: EnvScope,
    rel_file: str,
) -> None:
    """Detecta funções desconhecidas recebendo strings com padrão de tabela Unity Catalog.

    Args:
        ctx: Contexto compartilhado do mapeador.
        call: Chamada AST não classificada como primitiva nem wrapper.
        callee: Nome simples da função ou método chamado.
        env: Escopo simbólico atual.
        rel_file: Caminho relativo do arquivo atual.
    """
    if callee in _IGNORED_CALL_NAMES or callee in ctx.func_summaries:
        return
    call_name = ast.unparse(call.func)
    for arg in (*call.args, *(kw.value for kw in call.keywords)):
        resolved, is_dyn = apply_env_prefix(eval_str_expr(arg, env), ctx.env_prefix)
        if not is_table_pattern(resolved):
            continue
        key = (call_name, resolved, rel_file, call.lineno)
        if key in ctx.seen_refs:
            continue
        ctx.seen_refs.add(key)
        _mark_covered(ctx, rel_file, call.lineno)
        raw_expr = None if isinstance(arg, ast.Constant) else ast.unparse(arg)
        ctx.result.unclassified_table_refs.append(
            UnclassifiedTableRef(
                file=rel_file,
                line=call.lineno,
                call=call_name,
                target=resolved,
                dynamic=is_dyn,
                raw_expr=raw_expr,
            )
        )


def _handle_notebook_run(
    ctx: _MapperContext,
    nb_run: NotebookRunMatch,
    cur_file: Path,
    rel_file: str,
) -> None:
    """Resolve e analisa recursivamente um notebook filho chamado via `dbutils.notebook.run`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        nb_run: Dados extraídos da chamada `dbutils.notebook.run`.
        cur_file: Arquivo chamador atual.
        rel_file: Caminho relativo do arquivo chamador.
    """
    if nb_run.dynamic:
        _record_unresolved(
            ctx,
            UnresolvedDynamic(
                file=rel_file,
                line=nb_run.line,
                call="dbutils.notebook.run",
                kind="dependency",
                expr=nb_run.raw_expr,
            ),
        )
        return

    target_path = _resolve_notebook_path(ctx.project_root, cur_file, nb_run.target)
    if target_path is None:
        ctx.result.external_dependencies.append(
            ExternalDependency(
                caller_file=rel_file,
                target_path=nb_run.target,
                line=nb_run.line,
                via="dbutils.notebook.run",
            )
        )
        return

    ctx.result.dependencies.append(
        DependencyEdge(
            caller_file=rel_file,
            target_file=_rel_str(ctx.project_root, target_path),
            line=nb_run.line,
            via="dbutils.notebook.run",
            arguments=dict(nb_run.arguments),
        )
    )
    child_env = _new_env(ctx, widgets=nb_run.arguments)
    _analyze_file(ctx, target_path, child_env, temp_views=set())


def _callee_name(call: ast.Call) -> str:
    """Retorna o nome simples da função ou método invocado em `ast.Call`.

    Args:
        call: Nó `ast.Call`.

    Returns:
        Nome do identificador ou string vazia.
    """
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ""


def _process_direct_call(
    ctx: _MapperContext,
    call: ast.Call,
    cur_file: Path,
    rel_file: str,
    env: EnvScope,
    temp_views: set[str],
) -> bool:
    """Classifica chamadas de infraestrutura Spark (temp view, SQL, notebook.run, primitiva).

    Args:
        ctx: Contexto compartilhado do mapeador.
        call: Nó `ast.Call`.
        cur_file: Caminho absoluto do arquivo atual.
        rel_file: Caminho relativo do arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views no escopo atual.

    Returns:
        `True` se a chamada foi consumida.
    """
    view_name = match_temp_view_call(call, env)
    if view_name is not None:
        temp_views.add(view_name)
        return True

    sql_text = match_sql_call(call, env, ctx.env_prefix)
    if sql_text is not None:
        _record_sql_block(ctx, sql_text, rel_file, call.lineno, temp_views)
        return True

    nb_run = match_notebook_run_call(call, env)
    if nb_run is not None:
        _handle_notebook_run(ctx, nb_run, cur_file, rel_file)
        return True

    prim = match_spark_primitive(call, env, ctx.env_prefix)
    if prim is not None:
        _handle_primitive_match(ctx, prim, rel_file, call.lineno)
        return True
    return False


def _process_single_call(
    ctx: _MapperContext,
    call: ast.Call,
    cur_file: Path,
    rel_file: str,
    env: EnvScope,
    temp_views: set[str],
) -> bool:
    """Classifica uma única chamada AST (Spark, wrapper, função local importada ou ref).

    Args:
        ctx: Contexto compartilhado do mapeador.
        call: Nó `ast.Call`.
        cur_file: Caminho absoluto do arquivo atual.
        rel_file: Caminho relativo do arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views no escopo atual.

    Returns:
        `True` se a chamada inteira foi consumida e não precisa descer nos filhos.
    """
    if _process_direct_call(ctx, call, cur_file, rel_file, env, temp_views):
        return True

    callee = _callee_name(call)
    if callee and _handle_wrapper_call(ctx, call, callee, env, rel_file):
        return True

    imported = _resolve_imported_target(call, env)
    if imported is not None and _analyze_imported_symbol(ctx, imported[0], imported[1], call, env):
        return True

    if callee and _analyze_local_function_call(ctx, cur_file, callee, call, env):
        return True

    if callee:
        _check_unclassified_table_ref(ctx, call, callee, env, rel_file)
    return False


def _inspect_expression_calls(
    ctx: _MapperContext,
    expr: ast.expr,
    cur_file: Path,
    env: EnvScope,
    temp_views: set[str],
) -> None:
    """Inspeciona uma expressão AST sem duplicar sub-chamadas de uma cadeia já reconhecida.

    Args:
        ctx: Contexto compartilhado do mapeador.
        expr: Expressão AST a analisar.
        cur_file: Arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views no escopo atual.
    """
    rel_file = _rel_str(ctx.project_root, cur_file)
    if isinstance(expr, ast.Call) and _process_single_call(ctx, expr, cur_file, rel_file, env, temp_views):
        return
    for child in ast.iter_child_nodes(expr):
        if isinstance(child, ast.expr):
            _inspect_expression_calls(ctx, child, cur_file, env, temp_views)


def _walk_statements(
    ctx: _MapperContext,
    stmts: list[ast.stmt],
    cur_file: Path,
    env: EnvScope,
    temp_views: set[str],
) -> None:
    """Percorre uma lista de instruções Python mantendo avaliação parcial e ramos.

    Args:
        ctx: Contexto compartilhado do mapeador.
        stmts: Lista de statements AST.
        cur_file: Arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views conhecidas.
    """
    for stmt in stmts:
        _walk_single_statement(ctx, stmt, cur_file, env, temp_views)


def _walk_control_flow(
    ctx: _MapperContext,
    stmt: ast.stmt,
    cur_file: Path,
    env: EnvScope,
    temp_views: set[str],
) -> None:
    """Percorre fluxos de controle (`for`, `while`, `if/else`, `with`, `try`, `match`).

    Args:
        ctx: Contexto compartilhado do mapeador.
        stmt: Instrução de controle de fluxo AST.
        cur_file: Arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views conhecidas.
    """
    if isinstance(stmt, ast.For):
        _inspect_expression_calls(ctx, stmt.iter, cur_file, env, temp_views)
        _handle_for_loop(ctx, stmt, cur_file, env, temp_views)
    elif isinstance(stmt, (ast.While, ast.If)):
        _inspect_expression_calls(ctx, stmt.test, cur_file, env, temp_views)
        _walk_statements(ctx, stmt.body, cur_file, env.copy(), temp_views)
        _walk_statements(ctx, stmt.orelse, cur_file, env.copy(), temp_views)
    elif isinstance(stmt, (ast.With, ast.AsyncWith)):
        for item in stmt.items:
            _inspect_expression_calls(ctx, item.context_expr, cur_file, env, temp_views)
        _walk_statements(ctx, stmt.body, cur_file, env, temp_views)
    elif isinstance(stmt, ast.Try):
        _walk_try_statement(ctx, stmt, cur_file, env, temp_views)
    elif isinstance(stmt, ast.Match):
        _inspect_expression_calls(ctx, stmt.subject, cur_file, env, temp_views)
        for case in stmt.cases:
            _walk_statements(ctx, case.body, cur_file, env.copy(), temp_views)


def _walk_compound_statement(
    ctx: _MapperContext,
    stmt: ast.stmt,
    cur_file: Path,
    env: EnvScope,
    temp_views: set[str],
) -> None:
    """Percorre definições (`class`, `def`, `async def`) e fluxos de controle.

    Args:
        ctx: Contexto compartilhado do mapeador.
        stmt: Instrução composta AST.
        cur_file: Arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views conhecidas.
    """
    if isinstance(stmt, ast.ClassDef):
        _walk_statements(ctx, stmt.body, cur_file, env.copy(), temp_views)
    elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
        deferred = ctx.called_local_funcs.get(cur_file.resolve(), set())
        if stmt.name not in ctx.wrappers and stmt.name not in deferred:
            _walk_statements(ctx, stmt.body, cur_file, env.copy(), temp_views)
    else:
        _walk_control_flow(ctx, stmt, cur_file, env, temp_views)


def _walk_try_statement(
    ctx: _MapperContext,
    stmt: ast.Try,
    cur_file: Path,
    env: EnvScope,
    temp_views: set[str],
) -> None:
    """Percorre blocos `try`/`except`/`else`/`finally`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        stmt: Instrução `ast.Try`.
        cur_file: Arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views conhecidas.
    """
    _walk_statements(ctx, stmt.body, cur_file, env, temp_views)
    for handler in stmt.handlers:
        _walk_statements(ctx, handler.body, cur_file, env.copy(), temp_views)
    _walk_statements(ctx, stmt.orelse, cur_file, env, temp_views)
    _walk_statements(ctx, stmt.finalbody, cur_file, env, temp_views)


def _walk_single_statement(
    ctx: _MapperContext,
    stmt: ast.stmt,
    cur_file: Path,
    env: EnvScope,
    temp_views: set[str],
) -> None:
    """Processa uma única instrução AST delegando blocos compostos.

    Args:
        ctx: Contexto compartilhado do mapeador.
        stmt: Instrução AST.
        cur_file: Arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views conhecidas.
    """
    if isinstance(stmt, ast.ImportFrom):
        _handle_import_from(ctx, stmt, env)
    elif isinstance(stmt, ast.Import):
        _handle_plain_import(ctx, stmt, env)
    elif isinstance(stmt, ast.Assign):
        _handle_assign(ctx, stmt, cur_file, env, temp_views)
    elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
        bind_assignment(stmt.target, stmt.value, env)
        _inspect_expression_calls(ctx, stmt.value, cur_file, env, temp_views)
    elif isinstance(stmt, (ast.Expr, ast.Return)) and stmt.value is not None:
        _inspect_expression_calls(ctx, stmt.value, cur_file, env, temp_views)
    else:
        _walk_compound_statement(ctx, stmt, cur_file, env, temp_views)


def _handle_assign(
    ctx: _MapperContext,
    stmt: ast.Assign,
    cur_file: Path,
    env: EnvScope,
    temp_views: set[str],
) -> None:
    """Processa atribuição atualizando constantes, `DeltaTable`, builders e chamadas I/O.

    Args:
        ctx: Contexto compartilhado do mapeador.
        stmt: Instrução `ast.Assign`.
        cur_file: Arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views conhecidas.
    """
    rel_file = _rel_str(ctx.project_root, cur_file)
    if len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
        var_name = stmt.targets[0].id
        root, steps = unwind_chain(stmt.value)
        delta_bind = extract_delta_binding_from_steps(root, steps, env, ctx.env_prefix)
        if delta_bind is not None:
            target, is_dyn, raw_expr, _, _ = delta_bind
            env.delta_tables[var_name] = (target, is_dyn, raw_expr)
            _mark_covered(ctx, rel_file, stmt.lineno)
            return
        builder = extract_builder_state(stmt.value, env)
        if builder is not None:
            env.builders[var_name] = builder
            _mark_covered(ctx, rel_file, stmt.lineno)
            return
        bind_assignment(stmt.targets[0], stmt.value, env)
    _inspect_expression_calls(ctx, stmt.value, cur_file, env, temp_views)


def _handle_for_loop(
    ctx: _MapperContext,
    stmt: ast.For,
    cur_file: Path,
    env: EnvScope,
    temp_views: set[str],
) -> None:
    """Desenrola laços `for` sobre listas literais ou percorre o corpo uma vez com `<*>`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        stmt: Instrução `ast.For`.
        cur_file: Arquivo atual.
        env: Escopo simbólico atual.
        temp_views: Conjunto mutável de temp views conhecidas.
    """
    items = eval_str_list(stmt.iter, env)
    if items is not None and isinstance(stmt.target, ast.Name):
        for item in items:
            loop_env = env.copy()
            loop_env.strings[stmt.target.id] = item
            _walk_statements(ctx, stmt.body, cur_file, loop_env, temp_views)
        return
    loop_env = env.copy()
    if isinstance(stmt.target, ast.Name):
        loop_env.strings[stmt.target.id] = WILDCARD
    _walk_statements(ctx, stmt.body, cur_file, loop_env, temp_views)


def _process_run_directives(
    ctx: _MapperContext,
    doc: ParsedDocument,
    cur_file: Path,
    env: EnvScope,
    temp_views: set[str],
) -> None:
    """Processa diretivas `%run` compartilhando o escopo global do notebook incluído.

    Args:
        ctx: Contexto compartilhado do mapeador.
        doc: Documento carregado do notebook atual.
        cur_file: Caminho absoluto do arquivo atual.
        env: Escopo simbólico compartilhado.
        temp_views: Conjunto compartilhado de temp views.
    """
    rel_file = _rel_str(ctx.project_root, cur_file)
    for directive in doc.run_directives:
        target_path = _resolve_notebook_path(ctx.project_root, cur_file, directive.target)
        if target_path is None:
            ctx.result.external_dependencies.append(
                ExternalDependency(
                    caller_file=rel_file,
                    target_path=directive.target,
                    line=directive.line,
                    via="%run",
                )
            )
            continue
        ctx.result.dependencies.append(
            DependencyEdge(
                caller_file=rel_file,
                target_file=_rel_str(ctx.project_root, target_path),
                line=directive.line,
                via="%run",
            )
        )
        _analyze_file(ctx, target_path, env, temp_views)


def _wrapper_ignored_lines(ctx: _MapperContext, rel_file: str) -> set[int]:
    """Coleta linhas de chamadas parametrizadas dentro de wrappers definidos em `rel_file`.

    Args:
        ctx: Contexto compartilhado do mapeador.
        rel_file: Caminho relativo do arquivo auditado.

    Returns:
        Conjunto de números de linha cobertos pelos wrappers do arquivo.
    """
    ignored: set[int] = set()
    for wrapper in ctx.wrappers.values():
        if wrapper.info.file != rel_file:
            continue
        ignored.update(wrapper.param_io_lines)
        if wrapper.info.name not in ctx.analyzed_wrappers and wrapper.fn_node is not None:
            ignored.update(getattr(node, "lineno", 0) for node in ast.walk(wrapper.fn_node) if hasattr(node, "lineno"))
    return ignored


def _run_safety_audit(ctx: _MapperContext, node: ast.AST, rel_file: str) -> None:
    """Executa a Camada 2 (`ast.walk`) para garantir que nenhuma leitura/escrita escape.

    Args:
        ctx: Contexto compartilhado do mapeador.
        node: Nó AST (módulo ou função/classe) a auditar.
        rel_file: Caminho relativo do arquivo de origem.
    """
    covered = ctx.covered_lines.get(rel_file, set())
    ignored = _wrapper_ignored_lines(ctx, rel_file)
    for missed in audit_missed_io(node, rel_file, covered, ignored):
        _record_unresolved(ctx, missed)


def _index_local_functions(ctx: _MapperContext, resolved: Path, tree: ast.Module) -> None:
    """Indexa funções definidas em `tree` e marca as parametrizadas chamadas localmente.

    Args:
        ctx: Contexto compartilhado do mapeador.
        resolved: Caminho absoluto do arquivo.
        tree: AST do módulo.
    """
    defs: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {
        stmt.name: stmt for stmt in tree.body if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    ctx.local_func_defs[resolved] = defs
    called_names = {
        node.func.id for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    ctx.called_local_funcs[resolved] = {
        name for name, fn in defs.items() if name in called_names and bool(fn.args.args)
    }


def _analyze_file(ctx: _MapperContext, file_path: Path, env: EnvScope, temp_views: set[str]) -> None:
    """Analisa estaticamente um único arquivo (Camada 1 + Camada 2 + blocos SQL).

    Args:
        ctx: Contexto compartilhado do mapeador.
        file_path: Caminho absoluto do arquivo.
        env: Escopo simbólico ativo.
        temp_views: Conjunto mutável de temp views conhecidas.
    """
    resolved = file_path.resolve()
    if resolved in ctx.visited_files or not resolved.is_file():
        return
    ctx.visited_files.add(resolved)

    doc, tree = _load_and_parse(ctx, resolved)
    rel_file = _rel_str(ctx.project_root, resolved)
    _process_run_directives(ctx, doc, resolved, env, temp_views)

    if tree is not None:
        _index_local_functions(ctx, resolved, tree)
        _walk_statements(ctx, tree.body, resolved, env, temp_views)
        _run_safety_audit(ctx, tree, rel_file)

    for block in doc.sql_blocks:
        _record_sql_block(ctx, block.sql, rel_file, block.line, temp_views)


def _discover_project_files(root: Path) -> list[Path]:
    """Lista todos os arquivos `.py`, `.ipynb` e `.sql` dentro de um diretório.

    Args:
        root: Diretório raiz a varrer.

    Returns:
        Lista ordenada de caminhos absolutos suportados.
    """
    files: list[Path] = []
    for item in sorted(root.rglob("*")):
        if not item.is_file() or item.suffix.lower() not in _SUPPORTED_SUFFIXES:
            continue
        if any(part in _IGNORED_DIRS or part.endswith(".egg-info") for part in item.relative_to(root).parts):
            continue
        files.append(item.resolve())
    return files


def _resolve_start_files(path: str | Path, entrypoint: str | Path | None) -> tuple[Path, list[Path], str | None]:
    """Determina raiz do projeto, lista inicial de arquivos e rótulo do entrypoint.

    Args:
        path: Diretório raiz ou arquivo único.
        entrypoint: Entrypoint opcional relativo a `path`.

    Returns:
        Tupla `(project_root, start_files, entry_label)`.

    Raises:
        FileNotFoundError: Quando `path` ou `entrypoint` não existe no disco.
    """
    target_path = Path(path).resolve()
    if not target_path.exists():
        msg = f"Caminho não encontrado: {target_path}"
        raise FileNotFoundError(msg)
    if target_path.is_file():
        return target_path.parent, [target_path], target_path.name
    if entrypoint is not None:
        entry_path = (target_path / entrypoint).resolve()
        if not entry_path.is_file():
            msg = f"Entrypoint não encontrado: {entry_path}"
            raise FileNotFoundError(msg)
        return target_path, [entry_path], _rel_str(target_path, entry_path)
    return target_path, _discover_project_files(target_path), None


def map_spark_io(
    path: str | Path,
    *,
    entrypoint: str | Path | None = None,
    env_prefix: str = "pr",
    variables: dict[str, str] | None = None,
    custom_wrappers: dict[str, CustomWrapperSpec] | None = None,
) -> MapResult:
    """Mapeia estaticamente bases de entrada e saída a partir de um projeto ou entrypoint.

    Args:
        path: Diretório raiz do projeto ou arquivo único a analisar.
        entrypoint: Arquivo inicial opcional dentro de `path` para guiar o grafo de execução estático.
        env_prefix: Prefixo padrão de ambiente aplicado a catálogos no formato `<*>_<resto>`.
        variables: Mapa opcional de variáveis conhecidas para substituição na avaliação parcial.
        custom_wrappers: Especificações opcionais de wrappers externos conhecidos.

    Returns:
        Instância de `MapResult` contendo entradas, saídas, wrappers e dependências encontradas.
    """
    project_root, start_files, entry_label = _resolve_start_files(path, entrypoint)
    ctx = _MapperContext(
        project_root=project_root,
        env_prefix=env_prefix,
        user_vars=dict(variables or {}),
        custom_wrappers=dict(custom_wrappers or {}),
        result=MapResult(project_root=project_root.as_posix(), entrypoint=entry_label),
    )

    reachable_modules = _collect_reachable_modules(ctx, start_files)
    raw_summaries = discover_func_summaries(reachable_modules)
    ctx.wrappers = discover_wrappers(reachable_modules, func_summaries=raw_summaries)
    ctx.func_summaries = {k: v for k, v in raw_summaries.items() if k not in ctx.wrappers}
    ctx.result.wrappers = [w.info for w in ctx.wrappers.values()]

    for start_file in start_files:
        _analyze_file(ctx, start_file, _new_env(ctx), temp_views=set())

    return ctx.result
