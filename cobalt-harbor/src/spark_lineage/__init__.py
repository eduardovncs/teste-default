"""Pacote para captura e análise de planos de execução Spark."""

from __future__ import annotations

__version__ = "0.1.0"

from .doctor import render_doctor_text, run_doctor
from .impact import render_impact_markdown, trace_column_impact
from .plan_parser import (
    is_collapsed_to_empty,
    normalize_table,
    other_relation_classes,
    parse_formatted_scans,
    parse_merge_targets,
    parse_read_schema,
    parse_relations,
    parse_scans,
    parse_views,
    split_top_level,
    strip_ids,
)
from .sources import VALID_TAGS, build_sources, discover_plan_tables, extract_tag, render_markdown

__all__ = [
    "VALID_TAGS",
    "__version__",
    "build_sources",
    "discover_plan_tables",
    "extract_tag",
    "is_collapsed_to_empty",
    "normalize_table",
    "other_relation_classes",
    "parse_formatted_scans",
    "parse_merge_targets",
    "parse_read_schema",
    "parse_relations",
    "parse_scans",
    "parse_views",
    "render_doctor_text",
    "render_impact_markdown",
    "render_markdown",
    "run_doctor",
    "split_top_level",
    "strip_ids",
    "trace_column_impact",
]
