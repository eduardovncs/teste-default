"""Descoberta estática de funções wrapper de I/O Spark com propagação multi-hop."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field

from spark_io.chain_analyzer import PrimitiveMatch, match_spark_primitive
from spark_io.models import IoKind, WrapperInfo, WrapperKind
from spark_io.partial_eval import EnvScope


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


def _param_ref(node: ast.expr | None, param_map: dict[str, int]) -> tuple[str | None, int | None]:
    """Identifica se uma expressão referencia diretamente um parâmetro da função.

    Args:
        node: Expressão AST do argumento passado internamente.
        param_map: Mapa de parâmetros da função externa.

    Returns:
        Tupla `(nome_param, indice_param)` ou `(None, None)`.
    """
    if isinstance(node, ast.Name) and node.id in param_map:
        return node.id, param_map[node.id]
    return None, None


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
    param_map: dict[str, int],
) -> tuple[InternalWrapper, str, int] | None:
    """Verifica se uma chamada invoca um wrapper já conhecido propagando um parâmetro.

    Args:
        call: Chamada AST dentro do corpo da função candidata.
        known: Mapa de wrappers já identificados.
        param_map: Mapa de parâmetros da função externa.

    Returns:
        Tupla `(wrapper_interno, nome_param, indice_param)` ou `None`.
    """
    callee = (
        call.func.id
        if isinstance(call.func, ast.Name)
        else (call.func.attr if isinstance(call.func, ast.Attribute) else None)
    )
    if callee is None or callee not in known:
        return None
    inner = known[callee]
    arg_node = _extract_wrapper_target_arg(call, inner.info.target_param_index, inner.info.target_param)
    p_name, p_idx = _param_ref(arg_node, param_map)
    return None if p_name is None or p_idx is None else (inner, p_name, p_idx)


def _record_call_in_accum(
    call: ast.Call,
    env: EnvScope,
    param_map: dict[str, int],
    known: dict[str, InternalWrapper],
    accum: _WrapperAccum,
) -> None:
    """Avalia uma chamada dentro do corpo de uma função e atualiza o acumulador.

    Args:
        call: Nó `ast.Call` inspecionado.
        env: Escopo simbólico local da função.
        param_map: Mapa de parâmetros da função.
        known: Wrappers já identificados.
        accum: Acumulador de evidências.
    """
    prim = match_spark_primitive(call, env, env_prefix="")
    if prim is not None:
        p_name, p_idx = _param_ref(prim.target_node, param_map)
        if p_name is not None and p_idx is not None:
            accum.add_primitive(prim, p_name, p_idx)
        return

    wrapped = _match_known_wrapper_call(call, known, param_map)
    if wrapped is not None:
        inner, p_name, p_idx = wrapped
        accum.add_inner_wrapper(inner, p_name, p_idx)


def _inspect_function_body(
    fn: ast.FunctionDef,
    rel_path: str,
    known: dict[str, InternalWrapper],
) -> InternalWrapper | None:
    """Verifica se uma função encapsula primitivas Spark ou wrappers já conhecidos.

    Args:
        fn: Nó `ast.FunctionDef` a inspecionar.
        rel_path: Caminho relativo do arquivo onde a função está definida.
        known: Mapa de wrappers já descobertos até a iteração atual.

    Returns:
        `InternalWrapper` quando a função encapsula I/O, ou `None`.
    """
    param_map = _param_index_map(fn)
    env = EnvScope(strings=_default_string_params(fn))
    accum = _WrapperAccum()

    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            _record_call_in_accum(node, env, param_map, known, accum)

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


def discover_wrappers(modules: list[ModuleAst]) -> dict[str, InternalWrapper]:
    """Descobre todos os wrappers internos em um conjunto de módulos (ponto fixo multi-hop).

    Args:
        modules: Lista de módulos Python já parseados.

    Returns:
        Dicionário `{nome_funcao: InternalWrapper}` com todos os wrappers identificados.
    """
    functions: list[tuple[str, ast.FunctionDef]] = [
        (mod.rel_path, node) for mod in modules for node in ast.walk(mod.tree) if isinstance(node, ast.FunctionDef)
    ]

    known: dict[str, InternalWrapper] = {}
    for _ in range(len(functions) + 1):
        changed = False
        for rel_path, fn in functions:
            if fn.name in known:
                continue
            found = _inspect_function_body(fn, rel_path, known)
            if found is not None:
                known[fn.name] = found
                changed = True
        if not changed:
            break
    return known
