"""Descoberta estática de funções wrapper de I/O Spark e resumos de funções auxiliares."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field

from spark_io.chain_analyzer import PrimitiveMatch, match_spark_primitive
from spark_io.models import IoKind, WrapperInfo, WrapperKind
from spark_io.partial_eval import EnvScope, FuncSummary

ParamOrigin = tuple[str, int]
_STR_PASSTHROUGH_METHODS = frozenset({"strip", "lstrip", "rstrip", "lower", "upper"})


@dataclass
class InternalWrapper:
    """Metadados internos de uma função wrapper descoberta no repositório.

    Args:
        info: Metadados públicos (`WrapperInfo`).
        default_mode: Modo de escrita literal padrão observado dentro do wrapper.
        default_format: Formato de dados literal observado dentro do wrapper.
        default_options: Opções literais observadas dentro do wrapper.
    """

    info: WrapperInfo
    default_mode: str | None = None
    default_format: str | None = None
    default_options: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ModuleAst:
    """Módulo Python já parseado com metadados de caminho relativo.

    Args:
        rel_path: Caminho relativo do módulo (com separador `/`).
        tree: Árvore sintática `ast.Module`.
    """

    rel_path: str
    tree: ast.Module


@dataclass
class _WrapperAccum:
    """Acumulador de evidências de I/O dentro do corpo de uma função candidata."""

    kinds: set[IoKind] = field(default_factory=set)
    wraps: list[str] = field(default_factory=list)
    target_param: str | None = None
    target_idx: int | None = None
    default_mode: str | None = None
    default_format: str | None = None
    default_options: dict[str, str] = field(default_factory=dict)

    def add_primitive(self, prim: PrimitiveMatch, p_name: str, p_idx: int) -> None:
        """Registra evidência oriunda de uma primitiva Spark/Delta.

        Args:
            prim: Primitiva Spark/Delta encontrada dentro da função.
            p_name: Nome do parâmetro da função propagado como alvo.
            p_idx: Índice posicional do parâmetro propagado como alvo.
        """
        self.kinds.add(prim.kind)
        self.wraps.append(prim.via)
        self.target_param, self.target_idx = p_name, p_idx
        self.default_mode = self.default_mode or prim.mode
        self.default_format = self.default_format or prim.data_format
        self.default_options.update(prim.options)

    def add_inner_wrapper(self, inner: InternalWrapper, p_name: str, p_idx: int) -> None:
        """Registra evidência oriunda de uma chamada a outro wrapper já descoberto.

        Args:
            inner: Wrapper interno invocado pela função.
            p_name: Nome do parâmetro da função propagado como alvo.
            p_idx: Índice posicional do parâmetro propagado como alvo.
        """
        inner_kind: IoKind = "write" if inner.info.kind == "write" else "read"
        self.kinds.add(inner_kind)
        self.wraps.append(f"wrapper:{inner.info.name}")
        self.target_param, self.target_idx = p_name, p_idx
        self.default_mode = self.default_mode or inner.default_mode
        self.default_format = self.default_format or inner.default_format
        self.default_options.update(inner.default_options)


def _param_index_map(fn: ast.FunctionDef) -> dict[str, int]:
    """Mapeia nomes de parâmetros posicionais de uma função para seus índices.

    Args:
        fn: Definição de função AST.

    Returns:
        Dicionário `{nome_parametro: indice}` (ignorando `self`/`cls`).
    """
    params = [arg.arg for arg in fn.args.args]
    if params and params[0] in ("self", "cls"):
        params = params[1:]
    return {name: idx for idx, name in enumerate(params)}


def _default_string_params(fn: ast.FunctionDef) -> dict[str, str]:
    """Extrai valores default literais de string nos parâmetros de uma função.

    Args:
        fn: Definição de função AST.

    Returns:
        Dicionário `{nome_param: valor_default}`.
    """
    defaults = fn.args.defaults
    offset = len(fn.args.args) - len(defaults)
    return {
        fn.args.args[offset + idx].arg: node.value
        for idx, node in enumerate(defaults)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }


def _iter_direct_nodes(fn: ast.FunctionDef) -> list[ast.AST]:
    """Percorre os nós AST do corpo de `fn` sem entrar em funções aninhadas.

    Args:
        fn: Definição de função AST.

    Returns:
        Lista ordenada de nós AST pertencentes diretamente a `fn`.
    """
    stack: list[ast.AST] = list(reversed(fn.body))
    out: list[ast.AST] = []
    while stack:
        cur = stack.pop()
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        out.append(cur)
        stack.extend(reversed(list(ast.iter_child_nodes(cur))))
    return out


def _call_origin(
    call: ast.Call,
    lineage: dict[str, ParamOrigin],
    summaries: dict[str, FuncSummary],
) -> ParamOrigin | None:
    """Resolve se uma chamada propaga um parâmetro (`.strip()`, pass-through ou validador 1-arg).

    Args:
        call: Nó `ast.Call`.
        lineage: Linhagem atual de variáveis locais.
        summaries: Resumos conhecidos de funções auxiliares.

    Returns:
        `ParamOrigin` propagado ou `None`.
    """
    if isinstance(call.func, ast.Attribute) and call.func.attr in _STR_PASSTHROUGH_METHODS:
        return _propagate_origin(call.func.value, lineage, summaries)

    callee = _extract_callee_name(call)
    summary = summaries.get(callee) if callee else None
    if summary is not None:
        if summary.constant is not None:
            return None
        arg = _extract_wrapper_target_arg(call, summary.passthrough_param_index, summary.passthrough_param_name)
        return _propagate_origin(arg, lineage, summaries)

    if len(call.args) == 1 and not isinstance(call.args[0], ast.Subscript):
        return _propagate_origin(call.args[0], lineage, summaries)
    return None


def _unique_origin(
    nodes: list[ast.expr],
    lineage: dict[str, ParamOrigin],
    summaries: dict[str, FuncSummary],
) -> ParamOrigin | None:
    """Retorna a única origem de parâmetro encontrada entre múltiplos nós AST.

    Args:
        nodes: Lista de expressões AST filhas.
        lineage: Mapa de variáveis locais para `(nome_param, indice_param)`.
        summaries: Resumos de funções auxiliares conhecidas.

    Returns:
        `ParamOrigin` único ou `None`.
    """
    origins = {origin for item in nodes if (origin := _propagate_origin(item, lineage, summaries)) is not None}
    return next(iter(origins)) if len(origins) == 1 else None


def _propagate_origin(
    node: ast.expr | None,
    lineage: dict[str, ParamOrigin],
    summaries: dict[str, FuncSummary],
) -> ParamOrigin | None:
    """Identifica se `node` deriva diretamente de um único parâmetro (sem vazar chaves de dict).

    Args:
        node: Expressão AST inspecionada.
        lineage: Mapa de variáveis locais para `(nome_param, indice_param)`.
        summaries: Resumos de funções auxiliares conhecidas.

    Returns:
        Tupla `(nome_param, indice_param)` ou `None`.
    """
    if node is None or isinstance(node, ast.Subscript):
        return None
    if isinstance(node, ast.Name):
        return lineage.get(node.id)
    if isinstance(node, ast.Call):
        return _call_origin(node, lineage, summaries)
    if isinstance(node, ast.JoinedStr):
        parts = [p.value for p in node.values if isinstance(p, ast.FormattedValue)]
        return _unique_origin(parts, lineage, summaries)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _unique_origin([node.left, node.right], lineage, summaries)
    return None


def _extract_assign_pair(node: ast.AST) -> tuple[str, ast.expr] | None:
    """Extrai `(nome_variavel, expr_valor)` de atribuições simples.

    Args:
        node: Nó AST candidato.

    Returns:
        Tupla `(nome, valor)` ou `None`.
    """
    if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
        return node.targets[0].id, node.value
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
        return node.target.id, node.value
    return None


def _build_param_lineage(
    fn: ast.FunctionDef,
    param_map: dict[str, int],
    summaries: dict[str, FuncSummary],
) -> dict[str, ParamOrigin]:
    """Propaga variáveis locais derivadas de parâmetros (ex.: `table_fqn = _validate(table)`).

    Args:
        fn: Definição de função AST.
        param_map: Mapa inicial de parâmetros posicionais da função.
        summaries: Resumos de funções auxiliares já conhecidos.

    Returns:
        Dicionário `{nome_variavel: (nome_parametro, indice_parametro)}`.
    """
    lineage: dict[str, ParamOrigin] = {name: (name, idx) for name, idx in param_map.items()}
    for node in _iter_direct_nodes(fn):
        pair = _extract_assign_pair(node)
        if pair is None:
            continue
        origin = _propagate_origin(pair[1], lineage, summaries)
        if origin is not None:
            lineage[pair[0]] = origin
    return lineage


def _combine_kinds(kinds: set[IoKind]) -> WrapperKind:
    """Combina direções observadas dentro de um wrapper em `WrapperKind`.

    Args:
        kinds: Conjunto não vazio de `"read"` e/ou `"write"`.

    Returns:
        `"read"`, `"write"` ou `"read_write"`.
    """
    if "read" in kinds and "write" in kinds:
        return "read_write"
    return "write" if "write" in kinds else "read"


def _extract_callee_name(call: ast.Call) -> str | None:
    """Retorna o nome simples da função ou método chamado em `ast.Call`.

    Args:
        call: Nó `ast.Call`.

    Returns:
        Nome do identificador ou `None`.
    """
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _extract_wrapper_target_arg(
    call: ast.Call,
    param_index: int | None,
    param_name: str | None,
) -> ast.expr | None:
    """Extrai o argumento correspondente ao alvo numa chamada de wrapper.

    Args:
        call: Chamada AST do wrapper.
        param_index: Índice posicional esperado do alvo.
        param_name: Nome do parâmetro esperado do alvo.

    Returns:
        Nó AST do argumento alvo ou `None`.
    """
    if param_name is not None:
        for kw in call.keywords:
            if kw.arg == param_name:
                return kw.value
    if param_index is not None and len(call.args) > param_index:
        return call.args[param_index]
    return None


def _match_known_wrapper_call(
    call: ast.Call,
    known: dict[str, InternalWrapper],
    lineage: dict[str, ParamOrigin],
    summaries: dict[str, FuncSummary],
) -> tuple[InternalWrapper, str, int] | None:
    """Verifica se uma chamada invoca um wrapper já conhecido propagando um parâmetro.

    Args:
        call: Chamada AST dentro do corpo da função candidata.
        known: Mapa de wrappers já identificados.
        lineage: Linhagem de variáveis locais até os parâmetros da função externa.
        summaries: Resumos de funções auxiliares conhecidas.

    Returns:
        Tupla `(wrapper_interno, nome_param, indice_param)` ou `None`.
    """
    callee = _extract_callee_name(call)
    if callee is None or callee not in known:
        return None
    inner = known[callee]
    arg_node = _extract_wrapper_target_arg(call, inner.info.target_param_index, inner.info.target_param)
    origin = _propagate_origin(arg_node, lineage, summaries)
    return None if origin is None else (inner, origin[0], origin[1])


@dataclass(frozen=True)
class _InspectCtx:
    """Contexto de inspeção de chamadas dentro de uma função candidata a wrapper."""

    env: EnvScope
    lineage: dict[str, ParamOrigin]
    known: dict[str, InternalWrapper]
    summaries: dict[str, FuncSummary]


def _record_call_in_accum(call: ast.Call, ictx: _InspectCtx, accum: _WrapperAccum) -> None:
    """Avalia uma chamada dentro do corpo de uma função e atualiza o acumulador.

    Args:
        call: Nó `ast.Call` inspecionado.
        ictx: Contexto de inspeção da função candidata.
        accum: Acumulador de evidências.
    """
    prim = match_spark_primitive(call, ictx.env, env_prefix="")
    if prim is not None:
        origin = _propagate_origin(prim.target_node, ictx.lineage, ictx.summaries)
        if origin is not None:
            accum.add_primitive(prim, origin[0], origin[1])
        return

    wrapped = _match_known_wrapper_call(call, ictx.known, ictx.lineage, ictx.summaries)
    if wrapped is not None:
        inner, p_name, p_idx = wrapped
        accum.add_inner_wrapper(inner, p_name, p_idx)


def _inspect_function_body(
    fn: ast.FunctionDef,
    rel_path: str,
    known: dict[str, InternalWrapper],
    summaries: dict[str, FuncSummary],
) -> InternalWrapper | None:
    """Verifica se uma função encapsula primitivas Spark ou wrappers já conhecidos.

    Args:
        fn: Nó `ast.FunctionDef` a inspecionar.
        rel_path: Caminho relativo do arquivo onde a função está definida.
        known: Mapa de wrappers já descobertos até a iteração atual.
        summaries: Resumos de funções auxiliares puras.

    Returns:
        `InternalWrapper` quando a função encapsula I/O, ou `None`.
    """
    lineage = _build_param_lineage(fn, _param_index_map(fn), summaries)
    ictx = _InspectCtx(
        env=EnvScope(strings=_default_string_params(fn)),
        lineage=lineage,
        known=known,
        summaries=summaries,
    )
    accum = _WrapperAccum()

    for node in _iter_direct_nodes(fn):
        if isinstance(node, ast.Call):
            _record_call_in_accum(node, ictx, accum)

    if not accum.kinds:
        return None

    return InternalWrapper(
        info=WrapperInfo(
            name=fn.name,
            file=rel_path,
            line=fn.lineno,
            kind=_combine_kinds(accum.kinds),
            wraps=list(dict.fromkeys(accum.wraps)),
            target_param=accum.target_param,
            target_param_index=accum.target_idx,
        ),
        default_mode=accum.default_mode,
        default_format=accum.default_format,
        default_options=accum.default_options,
    )


def discover_wrappers(
    modules: list[ModuleAst],
    *,
    func_summaries: dict[str, FuncSummary] | None = None,
) -> dict[str, InternalWrapper]:
    """Descobre todos os wrappers internos em um conjunto de módulos (ponto fixo multi-hop).

    Args:
        modules: Lista de módulos Python já parseados.
        func_summaries: Resumos opcionais de funções puras já descobertos.

    Returns:
        Dicionário `{nome_funcao: InternalWrapper}` com todos os wrappers identificados.
    """
    summaries = func_summaries or {}
    functions: list[tuple[str, ast.FunctionDef]] = [
        (mod.rel_path, node) for mod in modules for node in ast.walk(mod.tree) if isinstance(node, ast.FunctionDef)
    ]

    known: dict[str, InternalWrapper] = {}
    for _ in range(len(functions) + 1):
        changed = False
        for rel_path, fn in functions:
            if fn.name in known:
                continue
            found = _inspect_function_body(fn, rel_path, known, summaries)
            if found is not None:
                known[fn.name] = found
                changed = True
        if not changed:
            break
    return known


def _constant_return_value(return_vals: list[ast.expr]) -> str | None:
    """Retorna a string constante se todos os `return` da função devolverem o mesmo literal.

    Args:
        return_vals: Expressões retornadas pela função.

    Returns:
        String literal única ou `None`.
    """
    values: set[str] = set()
    for expr in return_vals:
        if not (isinstance(expr, ast.Constant) and isinstance(expr.value, str)):
            return None
        values.add(expr.value)
    return next(iter(values)) if len(values) == 1 else None


def _summarize_function(fn: ast.FunctionDef) -> FuncSummary | None:
    """Resume uma função pura quando retorna string constante ou propaga um parâmetro.

    Args:
        fn: Definição de função AST.

    Returns:
        `FuncSummary` quando constante ou pass-through, ou `None`.
    """
    return_vals = [n.value for n in _iter_direct_nodes(fn) if isinstance(n, ast.Return) and n.value is not None]
    if not return_vals:
        return None

    const_val = _constant_return_value(return_vals)
    if const_val is not None:
        return FuncSummary(constant=const_val)

    lineage = _build_param_lineage(fn, _param_index_map(fn), {})
    origins = {_propagate_origin(v, lineage, {}) for v in return_vals}
    origin = next(iter(origins)) if len(origins) == 1 else None
    if origin is None:
        return None
    return FuncSummary(passthrough_param_name=origin[0], passthrough_param_index=origin[1])


def discover_func_summaries(
    modules: list[ModuleAst],
    *,
    exclude: set[str] | None = None,
) -> dict[str, FuncSummary]:
    """Descobre funções puras de retorno constante ou pass-through nos módulos alcançáveis.

    Args:
        modules: Lista de módulos Python já parseados.
        exclude: Nomes de funções a ignorar (ex.: wrappers de I/O já classificados).

    Returns:
        Dicionário `{nome_funcao: FuncSummary}`.
    """
    ignored = exclude or set()
    summaries: dict[str, FuncSummary] = {}
    for mod in modules:
        for node in ast.walk(mod.tree):
            if not isinstance(node, ast.FunctionDef) or node.name in ignored:
                continue
            summary = _summarize_function(node)
            if summary is not None:
                summaries[node.name] = summary
    return summaries
