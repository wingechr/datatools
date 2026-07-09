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
    rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    # skos = Namespace("http://www.w3.org/2004/02/skos/core#")
    spdx = Namespace("http://spdx.org/rdf/terms#")
    # time = Namespace("http://www.w3.org/2006/time#")
    # vcard = Namespace("http://www.w3.org/2006/vcard/ns#")
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
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
    """TODO

    Example:


    >>> str(MyUriRef("dcat:Dataset"))
    'Dataset'
    >>> str(MyUriRef("dcat:Dataset", name="MyDataset"))
    'MyDataset'
    >>> str(MyUriRef("http://www.w3.org/ns/dcat#Dataset"))
    'Dataset'
    >>> MyUriRef("urn:something")
    Traceback (most recent call last):
    ...
    KeyError:
    >>> MyUriRef("http://something/something")
    Traceback (most recent call last):
    ...
    KeyError:
    >>> MyUriRef("something")
    Traceback (most recent call last):
    ...
    NotImplementedError:

    """

    def __init__(
        self, uri: str | URIRef, name: str | None = None, type_range: str | None = None
    ):
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
        self.name: str = name or qname
        # check range
        if type_range:
            URIRef(type_range)

        self.type_range = type_range

    @property
    def prefix_name(self) -> str:
        """TODO"""
        return f"{self._prefix}:{self._qname}"

    def __str__(self) -> str:
        return self.name


class RdfProperties(MyEnum[MyUriRef]):
    """TODO"""

    _value_type = MyUriRef

    DESCRIPTION = MyUriRef("dcterms:description", type_range="xsd:string")
    GENERATED_BY = MyUriRef("prov:wasGeneratedBy")
    SAVED_WITH = MyUriRef("prov:qualifiedGeneration")
    ACTIVITY = MyUriRef("prov:activity")
    DATETIME = MyUriRef("prov:endedAtTime", type_range="xsd:dateTime")
    CREATOR = MyUriRef("prov:wasAssociatedWith")
    FUNCTION = MyUriRef("prov:used")
    PARAMETER = MyUriRef("prov:used")
    LABEL = MyUriRef("rdfs:label")
    SIZE = MyUriRef("dcat:byteSize", name="bytes", type_range="xsd:nonNegativeInteger")
    HASH = MyUriRef("spdx:checksum", name="hash")
    HASHSUM = MyUriRef("spdx:checksumValue", type_range="xsd:string")
    HASHALGO = MyUriRef(
        "spdx:algorithm",
        type_range="@id",  # looks weird, but is correct
    )
    NAME_TITLE = MyUriRef("dcterms:title", name="name", type_range="xsd:string")
    TASK_IDENTIFIER = MyUriRef(
        "dcterms:identifier", name="jobHash", type_range="xsd:string"
    )
    PARAMETER_VALUE = MyUriRef("rdf:value", type_range=None)
    ASSOCIATION = MyUriRef("prov:qualifiedAssociation")
    PLAN = MyUriRef("prov:hadPlan")


class RdfClasses(MyEnum[MyUriRef]):
    """TODO"""

    _value_type = MyUriRef

    FUNCTION = MyUriRef("prov:Plan", name="Function")
    ACTIVITY = MyUriRef("prov:Activity", name="Activity")
    INPUT_OUTPUT_FILE = MyUriRef("prov:Entity", name="Entity")
    FILE = MyUriRef("dcat:Distribution", name="File")
    SERIALIZE = MyUriRef("prov:Generation")
    PERSON = MyUriRef("prov:Person")
    HASH = MyUriRef("spdx:Checksum")
    ASSOCIATION = MyUriRef("prov:Association")


RDF_CONTEXT = (
    {k: str(v) for k, v in Namespaces.as_dict().items()}
    | {
        u.name: {"@id": u.prefix_name}
        | ({"@type": u.type_range} if u.type_range else {})
        for u in RdfProperties.as_dict().values()
        if u.prefix_name
    }
    | {u.name: u.prefix_name for u in RdfClasses.as_dict().values() if u.prefix_name}
)


class RDFTerm:
    @property


class RDFClass(RDFTerm):
    pass

class RDFProperty(RDFTerm):
    def __init__(self)
