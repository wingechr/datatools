import os
import re
import subprocess as sp
from collections import namedtuple
from dataclasses import dataclass

from rdflib import RDFS, Graph, Namespace, URIRef

ns = Namespace("urn:todo/")

# RDF.type
# RDFS.domain
# RDFS.range
# RDFS.label


def get_name_from_uri(uri: str) -> str:
    # last part of uri
    return re.sub("[:/#]", "/", uri).rstrip("/").split("/")[-1]


def create_class(name: str, propertties: list[str]) -> type:
    return namedtuple(name, propertties)


class MyGraph:
    def __init__(self):
        self.rdf_graph = Graph()

    def add(self, triple: tuple) -> None:
        self.rdf_graph.add(triple)

    def get_label(self, uriref: URIRef) -> str:
        try:
            return str(next(self.rdf_graph.objects(uriref, RDFS.label)))
        except StopIteration:
            return get_name_from_uri(uriref)

    def get_classes(self) -> set[URIRef]:
        return set(
            self.rdf_graph.objects(subject=None, predicate=RDFS.domain, unique=True)
        ) | set(self.rdf_graph.objects(subject=None, predicate=RDFS.range, unique=True))

    def to_jsonld(self, path: str) -> None:
        self.rdf_graph.serialize(path, format="json-ld")

    def create_class_graph(self) -> dict[str, dict[str, list[str]]]:
        ur_classes = self.get_classes()
        result = {}
        for ur_class in ur_classes:
            name_class = self.get_label(ur_class)
            result[name_class] = {}
            for ur_prop in self.rdf_graph.subjects(
                object=ur_class, predicate=RDFS.domain, unique=True
            ):
                name_prop = self.get_label(ur_prop)
                result[name_class][name_prop] = []
                for ur_class_type in self.rdf_graph.objects(
                    subject=ur_prop, predicate=RDFS.range, unique=True
                ):
                    if ur_class_type in ur_classes:
                        name_class_type = self.get_label(ur_class_type)
                        result[name_class][name_prop].append(name_class_type)

        return result


def define_graph() -> MyGraph:

    # define properties and classes via domain and ranges of properties
    g = MyGraph()

    g.add((ns.function, RDFS.domain, ns.Process))
    g.add((ns.function, RDFS.range, ns.Function))

    g.add((ns.input, RDFS.domain, ns.Process))
    g.add((ns.input, RDFS.range, ns.Input))

    g.add((ns.process, RDFS.range, ns.Process))
    g.add((ns.process, RDFS.domain, ns.Output))

    g.add((ns.context, RDFS.domain, ns.Process))
    g.add((ns.context, RDFS.range, ns.Context))

    g.add((ns.role, RDFS.domain, ns.Output))
    g.add((ns.encoder, RDFS.domain, ns.Output))

    g.add((ns.created, RDFS.domain, ns.Resource))
    g.add((ns.created, RDFS.range, ns.Output))

    g.add((ns.role, RDFS.domain, ns.Input))
    g.add((ns.resource, RDFS.domain, ns.Input))
    g.add((ns.resource, RDFS.range, ns.Resource))
    g.add((ns.decoder, RDFS.domain, ns.Input))

    g.add((ns.decoder, RDFS.range, ns.Function))
    g.add((ns.encoder, RDFS.range, ns.Function))

    g.add((ns.description, RDFS.domain, ns.Function))

    g.add((ns.fileType, RDFS.domain, ns.Resource))
    g.add((ns.schema, RDFS.domain, ns.Resource))
    # g.add((ns.dataType, RDFS.domain, ns.Input))
    # g.add((ns.dataType, RDFS.domain, ns.Output))
    # g.add((ns.schema, RDFS.domain, ns.Input))
    # g.add((ns.schema, RDFS.domain, ns.Output))

    g.add((ns.inputSchema, RDFS.domain, ns.Function))
    g.add((ns.outputSchema, RDFS.domain, ns.Function))
    g.add((ns.inputType, RDFS.domain, ns.Function))
    g.add((ns.outputType, RDFS.domain, ns.Function))

    g.add((ns.inputSchema, RDFS.range, ns.Schema))
    g.add((ns.outputSchema, RDFS.range, ns.Schema))
    g.add((ns.schema, RDFS.range, ns.Schema))

    g.add((ns.inputType, RDFS.range, ns.Type))
    g.add((ns.outputType, RDFS.range, ns.Type))
    g.add((ns.dataType, RDFS.range, ns.Type))
    g.add((ns.fileType, RDFS.range, ns.Type))

    # g.add((ns.Function, RDFS.label, Literal("blablabla")))

    return g


@dataclass
class ClassDiagram:
    classes: dict[str, dict[str, list[str]]]

    def _get_str_header(self) -> str:
        return ""

    def _get_str_footer(self) -> str:
        return ""

    def _get_str_class(self, name: str, name_props: list[str]) -> str:
        raise NotImplementedError()

    def _get_str_property(
        self, name_cls: str, name_prop: str, name_cls_type: str
    ) -> str:
        raise NotImplementedError()

    def get_str(self) -> str:
        result = ""
        result += self._get_str_header() + "\n"
        for name, properties in self.classes.items():
            result += "\n" + self._get_str_class(name, properties)

        result += "\n"

        for name, properties in self.classes.items():
            for prop, names_to in properties.items():
                for name_to in names_to:
                    result += "\n" + self._get_str_property(name, prop, name_to)

        result += "\n" + self._get_str_footer()

        return result


class ClassDiagramMermaid(ClassDiagram):
    def _get_str_header(self) -> str:
        return "```mermaid\nclassDiagram"

    def _get_str_footer(self) -> str:
        return "```"

    def _get_str_class(self, name: str, name_props: list[str]) -> str:
        name_props = "\n    " + "\n    ".join(name_props)
        return "  class %s {%s\n  }\n" % (name, name_props)

    def _get_str_property(
        self, name_cls: str, name_prop: str, name_cls_type: str
    ) -> str:
        return f"  {name_cls} --> {name_cls_type}"


class ClassDiagramGraphviz(ClassDiagram):
    use_ports = False

    def _get_str_header(self) -> str:
        return "digraph  {"

    def _get_str_footer(self) -> str:
        return "}"

    def _get_str_class(self, name: str, name_props: list[str]) -> str:
        rows_props = "".join(
            f'<tr><td align="left" port="{n}">{n}</td></tr>' for n in name_props
        )
        if rows_props:
            # internal table without border, each property is one row
            rows_props = f'<tr><td><table border="0" cellborder="0" cellspacing="0">{rows_props}</table></td></tr>'  # noqa

        # table for outline and separation between title and properties
        label = f'<table border="0" cellborder="1" cellspacing="0" cellpadding="4"><tr><td><b>{name}</b></td></tr>{rows_props}</table>'  # noqa
        return f"{name} [shape=plain, label=<{label}>]"

    def _get_str_property(
        self, name_cls: str, name_prop: str, name_cls_type: str
    ) -> str:
        edge_style_association = "[arrowhead=vee style=dashed]"
        if self.use_ports:
            return (
                f"{name_cls}:{name_prop} -> {name_cls_type} {edge_style_association};"
            )
        else:
            return f"{name_cls} -> {name_cls_type} {edge_style_association};"

    def get_png(self) -> bytes:
        code = self.get_str()

        dotfile = "_tmp_dotfile.dot"

        with open(dotfile, "w", encoding="utf-8") as file:
            file.write(code)

        pngfile = dotfile + ".png"

        cmd = ["dot", dotfile, "-T", "png", "-o", pngfile]

        # TODO: dot is dot.bat, not dot.exe, so we need shell=True
        # but still does not work with \ in absolute paths
        sp.Popen(cmd, shell=True).communicate()

        with open(pngfile, "rb") as file:
            data = file.read()

        os.remove(dotfile)
        os.remove(pngfile)

        return data


g = define_graph()
cg = g.create_class_graph()

bytes_png = ClassDiagramGraphviz(cg).get_png()
markdown = ClassDiagramMermaid(cg).get_str()

with open("a.png", "wb") as file:
    file.write(bytes_png)

with open("a.md", "w", encoding="utf-8") as file:
    file.write(markdown)

# sys.stdout.buffer.write(bytes_png)
