"""Abstract classes / interfaces, types"""

from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any, Literal, ParamSpec, TypeAlias, TypeVar

FunParams = ParamSpec("FunParams")
FunResult = TypeVar("FunResult")
SubCls = TypeVar("SubCls")

Json: TypeAlias = str | int | float | bool | None | list["Json"] | dict[str, "Json"]
StrPath = Path | str
Name = str
ByteData = bytes
MetadataAttribute = str
MetadataValue: TypeAlias = Json
MetadataPairs: TypeAlias = (
    Mapping[MetadataAttribute, MetadataValue]
    | Iterable[tuple[MetadataAttribute, MetadataValue]]
)
FunHashsum = Callable[..., str]
FunToBytes = Callable[[Any], bytes]
FunFromBytes = Callable[[bytes], Any]

# any name, must not collide with input parameters
SINGLE_OUTPUT_PARAM_NAME = "__output"
HTTP_METHOD = Literal["GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"]
