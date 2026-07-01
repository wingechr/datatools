"""Abstract classes / interfaces, types"""

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import ParamSpec, TypeAlias, TypeVar

FunParams = ParamSpec("FunParams")
FunResult = TypeVar("FunResult")
SubCls = TypeVar("SubCls")

Json: TypeAlias = str | int | float | bool | None | list["Json"] | dict[str, "Json"]
StrPath = Path | str
UID = str
ByteData = bytes
MetadataAttribute = str
MetadataValue: TypeAlias = Json
MetadataPairs: TypeAlias = (
    Mapping[MetadataAttribute, MetadataValue]
    | Iterable[tuple[MetadataAttribute, MetadataValue]]
)
