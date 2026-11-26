"""Modelos de dados tipados para o mapeamento estático de entradas e saídas Spark."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

IoKind = Literal["read", "write"]
WrapperKind = Literal["read", "write", "read_write"]
TargetType = Literal["table", "path", "unknown"]
DependencyVia = Literal["%run", "dbutils.notebook.run", "import"]
ExternalVia = Literal["%run", "dbutils.notebook.run"]
UnresolvedKind = Literal["read", "write", "dependency", "unknown"]


@dataclass(frozen=True)
class CustomWrapperSpec:
    """Especificação de um wrapper externo conhecido para leitura ou escrita.

    Args:
        kind: Classificação do wrapper (`"read"` ou `"write"`).
        target_param_index: Índice posicional do argumento com o nome da tabela ou caminho.
        target_param: Nome opcional do parâmetro nomeado correspondente ao alvo.
    """

    kind: IoKind
    target_param_index: int = 0
    target_param: str | None = None


@dataclass(frozen=True)
class IoEntry:
    """Representa uma operação de leitura ou escrita encontrada estaticamente.

    Args:
        target: Nome da tabela ou caminho resolvido (ou template com `<*>` se parcial).
        kind: Direção da operação (`"read"` ou `"write"`).
        via: Primitiva ou wrapper utilizado (ex.: `"read.table"`, `"wrapper:ler_tabela"`).
        file: Caminho relativo do arquivo onde a operação ocorre.
        line: Número da linha (1-based) no arquivo de origem.
        receiver: Expressão do objeto receptor quando aplicável (ex.: `"conector"`, `"spark"`).
        data_format: Formato de dados identificado (ex.: `"delta"`, `"parquet"`, `"csv"`).
        mode: Modo de escrita identificado (ex.: `"overwrite"`, `"append"`, `"merge"`).
        options: Opções relevantes capturadas na chamada (ex.: `overwriteSchema`).
        target_type: Classificação do alvo (`"table"`, `"path"` ou `"unknown"`).
        dynamic: Indica se o alvo ainda contém trecho dinâmico `<*>` não resolvido.
        raw_expr: Expressão original no código quando derivada de variável ou f-string.
    """

    target: str
    kind: IoKind
    via: str
    file: str
    line: int
    receiver: str | None = None
    data_format: str | None = None
    mode: str | None = None
    options: dict[str, str] = field(default_factory=dict)
    target_type: TargetType = "table"
    dynamic: bool = False
    raw_expr: str | None = None


@dataclass(frozen=True)
class WrapperInfo:
    """Função definida no repositório identificada como wrapper de I/O Spark.

    Args:
        name: Nome qualificado ou simples da função wrapper.
        file: Caminho relativo do arquivo onde a função está definida.
        line: Número da linha da definição.
        kind: Classificação (`"read"`, `"write"` ou `"read_write"`).
        wraps: Lista de primitivas ou wrappers encapsulados pela função.
        target_param: Nome do parâmetro propagado para o alvo de I/O, quando identificado.
        target_param_index: Posição do parâmetro propagado para o alvo, quando identificada.
    """

    name: str
    file: str
    line: int
    kind: WrapperKind
    wraps: list[str] = field(default_factory=list)
    target_param: str | None = None
    target_param_index: int | None = None


@dataclass(frozen=True)
class DependencyEdge:
    """Dependência resolvida entre dois arquivos/notebooks do projeto.

    Args:
        caller_file: Arquivo que declara a dependência.
        target_file: Arquivo alvo resolvido dentro do projeto.
        line: Número da linha onde a dependência ocorre.
        via: Mecanismo de inclusão (`"%run"`, `"dbutils.notebook.run"` ou `"import"`).
        arguments: Argumentos literais passados ao notebook filho, quando aplicável.
    """

    caller_file: str
    target_file: str
    line: int
    via: DependencyVia
    arguments: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ExternalDependency:
    """Dependência de notebook fora da raiz local analisada (ex.: `/Shared/...`).

    Args:
        caller_file: Arquivo que declara a dependência.
        target_path: Caminho referenciado fora do projeto local.
        line: Número da linha da referência.
        via: Mecanismo usado (`"%run"` ou `"dbutils.notebook.run"`).
    """

    caller_file: str
    target_path: str
    line: int
    via: ExternalVia


@dataclass(frozen=True)
class UnclassifiedTableRef:
    """Chamada a função desconhecida recebendo um argumento com padrão de tabela.

    Args:
        file: Arquivo onde a chamada ocorre.
        line: Número da linha da chamada.
        call: Nome da função ou método chamado (ex.: `"ler_base"`).
        target: Nome ou template de tabela extraído do argumento.
        dynamic: Indica se o alvo ainda contém trecho dinâmico `<*>`.
        raw_expr: Expressão original no código quando aplicável.
    """

    file: str
    line: int
    call: str
    target: str
    dynamic: bool = False
    raw_expr: str | None = None


@dataclass(frozen=True)
class UnresolvedDynamic:
    """Referência de I/O ou dependência cujo alvo não pôde ser resolvido estaticamente.

    Args:
        file: Arquivo onde a expressão ocorre.
        line: Número da linha da expressão.
        call: Primitiva, wrapper ou mecanismo de dependência chamado.
        kind: Categoria da operação não resolvida.
        expr: Representação textual da expressão dinâmica original.
    """

    file: str
    line: int
    call: str
    kind: UnresolvedKind
    expr: str


@dataclass
class MapResult:
    """Resultado consolidado do mapeamento estático de entradas e saídas.

    Args:
        project_root: Raiz do projeto analisado.
        entrypoint: Arquivo inicial analisado, ou `None` quando o diretório inteiro foi varrido.
        inputs: Operações de leitura mapeadas.
        outputs: Operações de escrita mapeadas.
        wrappers: Wrappers de I/O descobertos no repositório.
        dependencies: Dependências locais resolvidas entre arquivos/notebooks.
        external_dependencies: Dependências de notebooks fora do repositório local.
        unclassified_table_refs: Referências a tabelas passadas para funções desconhecidas.
        unresolved_dynamic: Expressões dinâmicas impossíveis de resolver estaticamente.
        warnings: Avisos não fatais coletados durante o mapeamento.
    """

    project_root: str
    entrypoint: str | None = None
    inputs: list[IoEntry] = field(default_factory=list)
    outputs: list[IoEntry] = field(default_factory=list)
    wrappers: list[WrapperInfo] = field(default_factory=list)
    dependencies: list[DependencyEdge] = field(default_factory=list)
    external_dependencies: list[ExternalDependency] = field(default_factory=list)
    unclassified_table_refs: list[UnclassifiedTableRef] = field(default_factory=list)
    unresolved_dynamic: list[UnresolvedDynamic] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serializa o resultado completo para dicionário compatível com JSON.

        Returns:
            Dicionário com todos os campos do mapeamento.
        """
        return asdict(self)
