"""Abstract classes / interfaces, types"""

from collections.abc import Callable, Iterable, Mapping
from functools import cache
from pathlib import Path
import re
from typing import Any, ClassVar, Generic, Literal, ParamSpec, TypeAlias, TypeVar

from rdflib import Namespace, URIRef

FunParams = ParamSpec("FunParams")
FunResult = TypeVar("FunResult")
SubCls = TypeVar("SubCls")
T = TypeVar("T")

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
    def get(cls, prefix: str) -> T:
        """TODO"""
        return cls.as_dict()[prefix]


class Namespaces(MyEnum[Namespace]):
    """TODO"""

    _value_type = Namespace

    # https://www.w3.org/TR/vocab-dcat-3/#normative-namespaces
    # adms = Namespace("http://www.w3.org/ns/adms#")
    # dc = Namespace("http://purl.org/dc/elements/1.1/")
    dcat = Namespace("http://www.w3.org/ns/dcat#")
    dcterms = Namespace("http://purl.org/dc/terms/")
    # dctype = Namespace("http://purl.org/dc/dcmitype/")
    # foaf = Namespace("http://xmlns.com/foaf/0.1/")
    # locn = Namespace("http://www.w3.org/ns/locn#")
    # odrl = Namespace("http://www.w3.org/ns/odrl/2/")
    # owl = Namespace("http://www.w3.org/2002/07/owl#")
    prov = Namespace("http://www.w3.org/ns/prov#")
    rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    # rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    # skos = Namespace("http://www.w3.org/2004/02/skos/core#")
    spdx = Namespace("http://spdx.org/rdf/terms#")
    # time = Namespace("http://www.w3.org/2006/time#")
    # vcard = Namespace("http://www.w3.org/2006/vcard/ns#")
    # xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    # dcat itself:
    dcat = Namespace("http://www.w3.org/ns/dcat#")

    # other
    # fno = Namespace("https://fno.io/spec/#")
    # sdo = Namespace("https://schema.org/")

    @classmethod
    @cache
    def get_prefix(cls, ns_uri: str) -> str:
        """TODO"""
        for k, v in cls.as_dict().items():
            if ns_uri == v:
                return k
        raise KeyError(ns_uri)


class MyUriRef:
    """TODO"""

    def __init__(self, uri: str | URIRef | None, name: str | None = None):
        if not uri:
            prefix, qname = None, None
        else:
            uri = str(uri)
            if m := re.match(r"^([^:/#]+):([^:/#]+)$", uri):
                # e.g. "dct:description"
                prefix, qname = m.groups()
                # check that it exists
                Namespaces.get(prefix)
            elif m := re.match(r"^(.*[/#])([^/#]+)$", uri):
                # e.g. "http://purl.org/dc/terms/description"
                ns_uri, qname = m.groups()
                prefix = Namespaces.get_prefix(ns_uri)
            else:
                raise NotImplementedError(uri)
        self._prefix = prefix
        self._qname = qname
        name = name or qname
        if not name:
            raise ValueError("no name")
        self.name: str = name

    @property
    def prefix_name(self) -> str | None:
        """TODO"""
        if self._prefix and self._qname:
            return f"{self._prefix}:{self._qname}"

    def __str__(self) -> str:
        return self.name


class RdfProperties(MyEnum[MyUriRef]):
    """TODO"""

    _value_type = MyUriRef

    DESCRIPTION = MyUriRef("dcterms:description")
    GENERATED_BY = MyUriRef("prov:wasGeneratedBy")
    SAVED_WITH = MyUriRef("prov:hadPlan")
    DATETIME = MyUriRef("dcterms:issued")
    CREATOR = MyUriRef("dcterms:creator")
    FUNCTION = MyUriRef("prov:hadPlan")
    LOADED_WITH = MyUriRef("prov:hadPlan")
    PARAMETER = MyUriRef("prov:used")
    PARAMETER_NAME = MyUriRef("dcat:hadRole")
    SIZE = MyUriRef("dcat:byteSize")
    FILE = MyUriRef("dcat:distribution")
    HASHSUM = MyUriRef("spdx:checksum")
    IDENTIFIER = MyUriRef("dcterms:identifier")
    PARAMETER_VALUE = MyUriRef("rdf:value")


class RdfClasses(MyEnum[MyUriRef]):
    """TODO"""

    _value_type = MyUriRef

    FUNCTION = MyUriRef("prov:Plan", name="Function")
    ACTIVITY = MyUriRef("prov:Activity", name="Activity")
    OUTPUT = MyUriRef("prov:Entity", name="Output")
    INPUT = MyUriRef("prov:Entity", name="Input")
    FILE = MyUriRef("dcat:Distribution", name="File")


RDF_CONTEXT = (
    {k: str(v) for k, v in Namespaces.as_dict().items()}
    | {u.name: u.prefix_name for u in RdfProperties.as_dict().values() if u.prefix_name}
    | {u.name: u.prefix_name for u in RdfClasses.as_dict().values() if u.prefix_name}
)
