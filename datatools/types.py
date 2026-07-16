"""Abstract classes / interfaces, types"""

from collections.abc import Callable, Iterable, Mapping
from functools import cache
import io
from io import BufferedReader, BufferedWriter
from pathlib import Path
from typing import (
    Any,
    ClassVar,
    Generic,
    Literal,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
)

from rdflib import Namespace, URIRef

# class ReadableByteBuffer(Protocol):  # noqa: D101
#    def read(self, __n: int = ...) -> bytes: ...  # noqa: D102
#    def readline(self) -> bytes: ...  # noqa: D102

ReadableByteBuffer = BufferedReader
WritableBuffer = BufferedWriter


class FunFromReadableByteBuffer(Protocol):  # noqa: D101
    def __call__(self, __fp: ReadableByteBuffer, *args: Any, **kwargs: Any) -> Any: ...  # noqa: D102, E501


class FunToWritableBuffer(Protocol):  # noqa: D101
    # can also write str
    def __call__(  # noqa: D102
        self, __data: Any, __fp: BufferedWriter, *args: Any, **kwargs: Any
    ) -> Any: ...


FunParams = ParamSpec("FunParams")
FunResult = TypeVar("FunResult")
SubCls = TypeVar("SubCls")
T = TypeVar("T")


Json: TypeAlias = str | int | float | bool | None | list["Json"] | dict[str, "Json"]
StrPath = Path | str
StrBytes = str | bytes
ByteData = bytes | Iterable[bytes] | ReadableByteBuffer
Name = str
MetadataAttribute = str
MetadataValue: TypeAlias = Json
MetadataPairs: TypeAlias = (
    Mapping[MetadataAttribute, MetadataValue]
    | Iterable[tuple[MetadataAttribute, MetadataValue]]
)
FunHashsum = Callable[..., str]


# any name, must be a valid parameter name
# # but not collide with input parameters
SINGLE_OUTPUT_PARAM_NAME = "MAIN"
HTTP_METHOD = Literal["GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"]
DEFAULT_CHUNK_SIZE = io.DEFAULT_BUFFER_SIZE  # 8192 bytes currently

# https://www.w3.org/TR/vocab-dcat-3/

# g = Graph()
# g.bind("dcterms", NS_DCT)
# g.bind("prov", NS_PROV)
# g.bind("xsd", NS_XSD)
#
# raise Exception(NS_XSD.xyz.fragment)


class MyEnum(Generic[T]):
    """TODO"""

    _value_type: ClassVar[type]

    @classmethod
    @cache
    def as_dict(cls) -> dict[str, T]:
        """TODO"""
        return {k: v for k, v in cls.__dict__.items() if isinstance(v, cls._value_type)}

    @classmethod
    @cache
    def get(cls, key: str) -> T:
        """TODO"""
        return cls.as_dict()[key]


ns = Namespace("http://purl.org/dataschema/datatools#")


class Namespaces(MyEnum[Namespace]):
    """TODO

    Example:


    >>> Namespaces.get_prefix("http://www.w3.org/2001/XMLSchema")
    Traceback (most recent call last):
    ...
    KeyError:

    >>> Namespaces.get_prefix("http://www.w3.org/2001/XMLSchema#")
    'xsd'

    >>> Namespaces.get("xsd")
    Namespace('http://www.w3.org/2001/XMLSchema#')

    """

    _value_type = Namespace

    # https://www.w3.org/TR/vocab-dcat-3/#normative-namespaces

    # adms = Namespace("http://www.w3.org/ns/adms#")
    # dc = Namespace("http://purl.org/dc/elements/1.1/")
    # dcat = Namespace("http://www.w3.org/ns/dcat#")
    # dcterms = Namespace("http://purl.org/dc/terms/")
    # dctype = Namespace("http://purl.org/dc/dcmitype/")
    # foaf = Namespace("http://xmlns.com/foaf/0.1/")
    # locn = Namespace("http://www.w3.org/ns/locn#")
    # odrl = Namespace("http://www.w3.org/ns/odrl/2/")
    # owl = Namespace("http://www.w3.org/2002/07/owl#")
    # prov = Namespace("http://www.w3.org/ns/prov#")
    # rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    # rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    # skos = Namespace("http://www.w3.org/2004/02/skos/core#")
    # spdx = Namespace("http://spdx.org/rdf/terms#")
    # time = Namespace("http://www.w3.org/2006/time#")
    # vcard = Namespace("http://www.w3.org/2006/vcard/ns#")
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    # dcat itself:
    # dcat = Namespace("http://www.w3.org/ns/dcat#")

    # other
    # fno = Namespace("https://fno.io/spec/#")
    # sdo = Namespace("https://schema.org/")
    wingechr = ns

    @classmethod
    @cache
    def get_prefix(cls, ns_uri: str) -> str:
        """TODO"""
        for k, v in cls.as_dict().items():
            if ns_uri == v:
                return k
        raise KeyError(ns_uri)


class ExtURIRef:
    """TODO"""

    def __init__(self, uriref: URIRef):
        self.uriref = uriref

    @property
    def label(self) -> str:
        """TODO"""
        return str(self.uriref).replace("#", "/").split("/")[-1]


class URIRefs(MyEnum[ExtURIRef]):
    """TODO"""

    _value_type = ExtURIRef

    FileResource = ExtURIRef(ns["FileResource"])
    Function = ExtURIRef(ns["Function"])
    CreationEvent = ExtURIRef(ns["CreationEvent"])
    Serialization = ExtURIRef(ns["Serialization"])
    Deserialization = ExtURIRef(ns["Deserialization"])
    LiteralParameter = ExtURIRef(ns["LiteralParameter"])
    Message = ExtURIRef(ns["Message"])

    # FileResource -> CreationEvent
    createdBy = ExtURIRef(ns["createdBy"])
    # (qualifies createdBy) FileResource -> Serialization
    serializedWith = ExtURIRef(ns["serializedWith"])
    # Serialization --> CreationEvent
    event = ExtURIRef(ns["event"])  # not used yet
    # [Serialization, Deserialization, CreationEvent] --> Function
    usedFunction = ExtURIRef(ns["usedFunction"])
    # [Serialization, Deserialization, CreationEvent] --> [Deserialization, LiteralParameter] # noqa: E501
    usedInput = ExtURIRef(ns["usedInput"])

    # [Serialization, Deserialization, LiteralParameter] --> xsd:string
    roleName = ExtURIRef(ns["roleName"])
    value = ExtURIRef(ns["value"])
    # CreationEvent --> xsd:string
    taskId = ExtURIRef(ns["taskId"])
    # CreationEvent --> xsd:string(date)
    datetime = ExtURIRef(ns["datetime"])
    # CreationEvent --> uri
    creator = ExtURIRef(ns["creator"])

    # Function --> xsd:String
    description = ExtURIRef(ns["description"])

    # FileResource -> value
    name = ExtURIRef(ns["name"])  # unique name in storage - or use path?
    bytes = ExtURIRef(ns["bytes"])
    hash = ExtURIRef(ns["hash"])
    format = ExtURIRef(ns["format"])  # not used yet,e.g. "csv"
    mediatype = ExtURIRef(ns["mediatype"])  # not used yet,e.g. "text/csv"
    encoding = ExtURIRef(ns["encoding"])  # not used yet,e.g. "utf-8"

    message = ExtURIRef(ns["message"])


# use http, becaus most others vocabs do it too
RDF_CONTEXT = {"@vocab": "http://purl.org/dataschema/datatools#"} | {
    k: str(v) for k, v in Namespaces.as_dict().items()
}

JSON_SCHEMA_FILE_RESOURCE = (
    # use https, otherwise vscode has trouble loading the forwarded url
    "https://purl.org/dataschema/datatools/FileResource-0.0.0.schema.json"
)

LOCKFILE_SUFFIX = ".__lock"
TEMPFILE_SUFFIX = ".__temp"
