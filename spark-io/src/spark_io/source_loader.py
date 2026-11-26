"""Carregamento estático de fontes `.py` (incluindo notebooks Databricks), `.ipynb` e `.sql`."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

_MAGIC_PREFIX = "# MAGIC"
_SQL_MAGIC_RE = re.compile(r"^\s*%sql\b(.*)$", re.IGNORECASE)
_RUN_MAGIC_RE = re.compile(r"^\s*%run\s+(.+)$", re.IGNORECASE)
_CELL_SQL_RE = re.compile(r"^\s*%%sql\b(.*)$", re.IGNORECASE)
_OTHER_MAGIC_RE = re.compile(r"^\s*(?:%%?[A-Za-z_][A-Za-z0-9_]*|!\s*\S)")


@dataclass(frozen=True)
class SqlBlock:
    """Bloco SQL extraído de um arquivo `.sql`, célula `%sql` ou `%%sql`.

    Args:
        sql: Conteúdo SQL bruto do bloco.
        line: Linha inicial (1-based) no arquivo de origem.
    """

    sql: str
    line: int


@dataclass(frozen=True)
class RunDirective:
    """Diretiva `%run` encontrada em um notebook Databricks ou Jupyter.

    Args:
        target: Caminho alvo informado na diretiva `%run`.
        line: Linha (1-based) onde a diretiva foi declarada.
    """

    target: str
    line: int


@dataclass
class ParsedDocument:
    """Representação unificada de um arquivo carregado estaticamente.

    Args:
        path: Caminho absoluto do arquivo carregado.
        python_source: Código Python sanitizado com preservação de linhas quando aplicável.
        sql_blocks: Lista de blocos SQL extraídos de células/arquivos SQL.
        run_directives: Lista de diretivas `%run` encontradas.
        warnings: Avisos não fatais gerados durante a leitura do arquivo.
    """

    path: Path
    python_source: str = ""
    sql_blocks: list[SqlBlock] = field(default_factory=list)
    run_directives: list[RunDirective] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _strip_magic_prefix(line: str) -> str | None:
    """Remove o prefixo `# MAGIC` de uma linha Databricks, se presente.

    Args:
        line: Linha original do arquivo `.py`.

    Returns:
        Texto interno após `# MAGIC` ou `None` quando a linha não é mágica.
    """
    stripped = line.lstrip()
    if not stripped.startswith(_MAGIC_PREFIX):
        return None
    rest = stripped[len(_MAGIC_PREFIX) :]
    return rest[1:] if rest.startswith(" ") else rest


def _clean_run_target(raw_target: str) -> str:
    """Normaliza o caminho alvo de uma diretiva `%run`.

    Args:
        raw_target: Texto após `%run`.

    Returns:
        Caminho limpo sem comentários inline ou aspas externas.
    """
    no_comment = raw_target.split("#", 1)[0].strip()
    if len(no_comment) >= 2 and no_comment[0] == no_comment[-1] and no_comment[0] in {'"', "'"}:
        return no_comment[1:-1].strip()
    return no_comment.split()[0] if no_comment else ""


@dataclass
class _MagicState:
    """Estado mutável do parser de linhas `# MAGIC` Databricks."""

    doc: ParsedDocument
    py_lines: list[str] = field(default_factory=list)
    sql_lines: list[str] = field(default_factory=list)
    mode: str = "python"
    sql_start: int = 0

    def flush_sql(self) -> None:
        """Descarrega o bloco SQL acumulado para o documento, se não vazio."""
        if not self.sql_lines:
            return
        sql_text = "\n".join(self.sql_lines).strip()
        if sql_text:
            self.doc.sql_blocks.append(SqlBlock(sql=sql_text, line=self.sql_start))
        self.sql_lines.clear()


def _parse_databricks_py(path: Path, text: str) -> ParsedDocument:
    """Separa código Python, blocos `# MAGIC %sql` e `# MAGIC %run` em um `.py`.

    Args:
        path: Caminho do arquivo `.py`.
        text: Conteúdo textual completo do arquivo.

    Returns:
        Documento estruturado preservando numeração de linhas Python.
    """
    state = _MagicState(doc=ParsedDocument(path=path))
    for idx, line in enumerate(text.splitlines(), start=1):
        magic = _strip_magic_prefix(line)
        if magic is None:
            state.flush_sql()
            state.mode = "python"
            state.py_lines.append(line)
            continue
        _process_magic_line(magic, idx, state)

    state.flush_sql()
    state.doc.python_source = "\n".join(state.py_lines)
    return state.doc


def _process_magic_line(magic: str, line_no: int, state: _MagicState) -> None:
    """Processa uma linha `# MAGIC` atualizando o estado do parser Databricks.

    Args:
        magic: Conteúdo após `# MAGIC`.
        line_no: Número da linha atual (1-based).
        state: Estado mutável do parser Databricks.
    """
    run_match = _RUN_MAGIC_RE.match(magic)
    if run_match:
        target = _clean_run_target(run_match.group(1))
        if target:
            state.doc.run_directives.append(RunDirective(target=target, line=line_no))
        state.py_lines.append("")
        state.mode = "ignore"
        return

    sql_match = _SQL_MAGIC_RE.match(magic)
    if sql_match:
        inline = sql_match.group(1).strip()
        if inline:
            state.sql_lines.append(inline)
        state.py_lines.append("")
        state.mode = "sql"
        state.sql_start = line_no
        return

    stripped = magic.strip().lower()
    if stripped == "%python":
        state.py_lines.append("")
        state.mode = "python"
        return
    if stripped.startswith("%"):
        state.py_lines.append("")
        state.mode = "ignore"
        return

    if state.mode == "sql":
        state.sql_lines.append(magic)
        state.py_lines.append("")
    elif state.mode == "python":
        state.py_lines.append(magic)
    else:
        state.py_lines.append("")


def _parse_ipynb_line(line: str, cur_line: int, doc: ParsedDocument) -> str:
    """Sanitiza uma linha de célula Jupyter extraindo `%run` e `%sql` inline.

    Args:
        line: Linha original da célula.
        cur_line: Número sintético da linha no notebook.
        doc: Documento destino para diretivas e blocos SQL.

    Returns:
        Linha Python sanitizada (ou string vazia para magics).
    """
    run_match = _RUN_MAGIC_RE.match(line)
    if run_match:
        target = _clean_run_target(run_match.group(1))
        if target:
            doc.run_directives.append(RunDirective(target=target, line=cur_line))
        return ""
    sql_match = _SQL_MAGIC_RE.match(line)
    if sql_match:
        inline = sql_match.group(1).strip()
        if inline:
            doc.sql_blocks.append(SqlBlock(sql=inline, line=cur_line))
        return ""
    return "" if _OTHER_MAGIC_RE.match(line) else line


def _parse_ipynb_cell(cell_source: str, base_line: int, doc: ParsedDocument) -> list[str]:
    """Processa uma célula de código Jupyter separando Python, `%%sql` e `%run`.

    Args:
        cell_source: Código-fonte da célula.
        base_line: Linha sintética inicial para referência.
        doc: Documento destino para blocos SQL e diretivas `%run`.

    Returns:
        Linhas Python sanitizadas da célula.
    """
    lines = cell_source.splitlines()
    if not lines:
        return []

    cell_sql = _CELL_SQL_RE.match(lines[0])
    if cell_sql:
        body = [cell_sql.group(1).strip(), *lines[1:]]
        sql_text = "\n".join(part for part in body if part).strip()
        if sql_text:
            doc.sql_blocks.append(SqlBlock(sql=sql_text, line=base_line))
        return ["" for _ in lines]

    if lines[0].lstrip().startswith("%%"):
        return ["" for _ in lines]

    return [_parse_ipynb_line(line, base_line + offset, doc) for offset, line in enumerate(lines)]


def _parse_ipynb(path: Path, text: str) -> ParsedDocument:
    """Carrega um notebook Jupyter `.ipynb` extraindo Python, SQL e `%run`.

    Args:
        path: Caminho do arquivo `.ipynb`.
        text: Conteúdo JSON do notebook.

    Returns:
        Documento unificado para análise estática.
    """
    doc = ParsedDocument(path=path)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        doc.warnings.append(f"JSON inválido em {path.name}: {exc}")
        return doc

    py_lines: list[str] = []
    for cell in payload.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        raw_source = cell.get("source", "")
        cell_text = "".join(raw_source) if isinstance(raw_source, list) else str(raw_source)
        base_line = len(py_lines) + 1
        py_lines.extend(_parse_ipynb_cell(cell_text, base_line, doc))
        py_lines.append("")

    doc.python_source = "\n".join(py_lines)
    return doc


def load_document(path: Path) -> ParsedDocument:
    """Lê um arquivo `.py`, `.ipynb` ou `.sql` de forma estática.

    Args:
        path: Caminho absoluto do arquivo a carregar.

    Returns:
        Instância de `ParsedDocument` com código Python, blocos SQL e diretivas `%run`.
    """
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    suffix = path.suffix.lower()
    if suffix == ".sql":
        sql_text = text.strip()
        blocks = [SqlBlock(sql=sql_text, line=1)] if sql_text else []
        return ParsedDocument(path=path, sql_blocks=blocks)
    if suffix == ".ipynb":
        return _parse_ipynb(path, text)
    return _parse_databricks_py(path, text)
