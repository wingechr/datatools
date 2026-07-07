"""Abstract classes / interfaces, types"""

from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
import re
from typing import Any, Literal, ParamSpec, TypeAlias, TypeVar

from rdflib import Namespace, URIRef

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


# https://www.w3.org/TR/vocab-dcat-3/

# g = Graph()
# g.bind("dcterms", NS_DCT)
# g.bind("prov", NS_PROV)
# g.bind("xsd", NS_XSD)
#
# raise Exception(NS_XSD.xyz.fragment)


class Namespaces:
    """TODO"""

    # https://www.w3.org/TR/vocab-dcat-3/#normative-namespaces
    adms = Namespace("http://www.w3.org/ns/adms#")
    dc = Namespace("http://purl.org/dc/elements/1.1/")
    dcat = Namespace("http://www.w3.org/ns/dcat#")
    dcterms = Namespace("http://purl.org/dc/terms/")
    dctype = Namespace("http://purl.org/dc/dcmitype/")
    foaf = Namespace("http://xmlns.com/foaf/0.1/")
    locn = Namespace("http://www.w3.org/ns/locn#")
    odrl = Namespace("http://www.w3.org/ns/odrl/2/")
    owl = Namespace("http://www.w3.org/2002/07/owl#")
    prov = Namespace("http://www.w3.org/ns/prov#")
    rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    skos = Namespace("http://www.w3.org/2004/02/skos/core#")
    spdx = Namespace("http://spdx.org/rdf/terms#")
    time = Namespace("http://www.w3.org/2006/time#")
    vcard = Namespace("http://www.w3.org/2006/vcard/ns#")
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    # dcat itself:
    dcat = Namespace("http://www.w3.org/ns/dcat#")

    # other
    fno = Namespace("https://fno.io/spec/#")
    sdo = Namespace("https://schema.org/")

    @classmethod
    def get_prefix(cls, ns_uri: str) -> str:
        """TODO"""
        for k, v in Namespaces.__dict__.items():
            if not isinstance(v, Namespace):
                continue
            if str(v) == ns_uri:
                return k
        raise KeyError(ns_uri)

    @classmethod
    def get(cls, prefix: str) -> Namespace:
        """TODO"""
        return getattr(cls, prefix)


class Property:
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

    def __str__(self) -> str:
        return self.name


class Properties:
    """TODO"""

    DESCRIPTION = Property("dcterms:description")
    GENERATED_BY = Property("prov:wasGeneratedBy")
    SAVED_WITH = Property("prov:hadPlan")
    DATETIME = Property("dcterms:issued")
    CREATOR = Property("dcterms:creator")
    FUNCTION = Property("prov:hadPlan")
    LOADED_WITH = Property("prov:hadPlan")
    PARAMETER = Property("prov:used")
    PARAMETER_NAME = Property("dcat:hadRole")
    SIZE = Property("dcat:byteSize")
    FILE = Property("dcat:distribution")
    HASHSUM = Property("spdx:checksum")
    JOB = Property("dcterms:identifier")
    PARAMETER_VALUE = Property(None, name="@value")


RDF_CONTEXT = {}
