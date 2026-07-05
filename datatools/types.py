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


# NS_DCT = Namespace("http://purl.org/dc/terms/")
# NS_PROV = Namespace("https://www.w3.org/TR/prov-o/#")
# NS_XSD = Namespace("http://www.w3.org/2001/XMLSchema#")
# NS_FNO = Namespace("https://fno.io/spec/#")
# NS_SDO = Namespace("https://schema.org/")
# NS_FOAF = Namespace("http://xmlns.com/foaf/")
# https://www.w3.org/TR/vocab-dcat-3/

# g = Graph()
# g.bind("dcterms", NS_DCT)
# g.bind("prov", NS_PROV)
# g.bind("xsd", NS_XSD)
#
# raise Exception(NS_XSD.xyz.fragment)

PROP_DESCRIPTION = "description"  # http://purl.org/dc/terms/description
PROP_SAVED_WITH = "savedWith"
PROP_GENERATED_BY = "wasGeneratedBy"  # https://www.w3.org/TR/prov-o/#wasGeneratedBy
PROP_DATETIME = "created"  # http://purl.org/dc/terms/created or http://www.w3.org/ns/prov#generatedAtTime # noqa: E501
PROP_CREATOR = "creator"  # http://purl.org/dc/terms/creator
PROP_FUNCTION = "function"  # TODO
PROP_LOADED_WITH = "loadedWith"  # TODO
PROP_PARAMETER = "parameter"  # TODO
PROP_PARAMETER_NAME = "name"  # https://schema.org/name
PROP_PARAMETER_VALUE = "@value"
PROP_SIZE = "sizeBytes"
PROP_FILE = "file"
PROP_HASHSUM = "hashsum"
PROP_JOB = "job"
