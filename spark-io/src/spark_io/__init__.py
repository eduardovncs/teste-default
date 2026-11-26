"""Pacote público do `spark_io` para mapeamento estático de entradas e saídas Spark."""

from spark_io.mapper import map_spark_io
from spark_io.models import (
    CustomWrapperSpec,
    DependencyEdge,
    ExternalDependency,
    IoEntry,
    MapResult,
    UnclassifiedTableRef,
    UnresolvedDynamic,
    WrapperInfo,
)

__all__ = [
    "CustomWrapperSpec",
    "DependencyEdge",
    "ExternalDependency",
    "IoEntry",
    "MapResult",
    "UnclassifiedTableRef",
    "UnresolvedDynamic",
    "WrapperInfo",
    "map_spark_io",
]
