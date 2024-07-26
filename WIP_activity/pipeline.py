import abc
import argparse
import datetime
import hashlib
import json
from typing import Callable, Dict, List, Union
from urllib.parse import urlparse

from rdflib import RDF, Literal, Namespace, URIRef


def is_valid_uri(uri):
    try:
        result = urlparse(uri)
        assert all([result.scheme])
        return True
    except Exception:
        return False


# regular function
def function(input_1: object, input_2) -> object:
    return {"output_1": sum(input_2) * input_1, "output_2": sum(input_2) + input_1}


def as_rdf(value):
    if isinstance(value, str) and is_valid_uri(value):
        return URIRef(value)
    else:
        return Literal(value)


def parse_context_list(context: List[str]) -> dict:
    result = {}
    for line in context or []:
        key, value = line.split("=")
        key = key.strip()
        value = value.strip()
        assert key not in result
        result[key] = value
    return result


class Parameter(abc.ABC):
    def __init__(self, name: str) -> None:
        self.name = name
        self.value = None

    def bind_value(self, value: str) -> None:
        self.value = value

    def as_rdf(self) -> Union[Literal, URIRef]:
        # TODO
        return URIRef("file://" + self.value.replace("\\", "/"))


class Function:
    def __init__(self, function: Callable) -> None:
        self.function = function

    def as_rdf(self) -> Union[Literal, URIRef]:
        # TODO
        return URIRef("function://" + self.function.__name__)

    def __call__(self, **kwargs):
        return self.function(**kwargs)


class InputParameter(Parameter):
    def load(self) -> object:
        raise NotImplementedError


class OutputParameter(Parameter):
    def _get(self, data: object) -> object:
        return data

    def _save(self, data: object, location: str) -> None:
        raise NotImplementedError()

    def save(self, data: object) -> None:
        data = self._get(data)
        location = self.value
        self._save(data=data, location=location)


class JsonOutputParameterMixin:
    def _save(self, data: object, location: str) -> None:
        data_s = json.dumps(data, indent=2, ensure_ascii=False)
        with open(location, "w", encoding="utf-8") as file:
            file.write(data_s)


class PartialOutputParameter(OutputParameter):
    def __init__(self, name: str, key: str = None) -> None:
        super().__init__(name=name)
        self.key = key or self.name

    def _get(self, data: object):
        return data[self.key]


class PartialJsonOutputParameter(JsonOutputParameterMixin, PartialOutputParameter):
    pass


class LiteralInputParameter(InputParameter):
    def load(self):
        try:
            return json.loads(self.value)
        except ValueError:
            return self.value

    def as_rdf(self) -> Literal:
        return Literal(self.value)


class JsonInputParameter(InputParameter):
    def load(self) -> object:
        filepath = self.value
        with open(filepath, encoding="utf-8") as file:
            return json.load(file)


class Activity:
    def __init__(self, function: Callable, parameters: List[Parameter] = None):
        self.function = Function(function)
        self.parameters = {}
        self.argument_parser = argparse.ArgumentParser()

        for p in parameters or []:
            self.add_parameter(p)

    def _filter_parameters(self, cls) -> Dict[str, InputParameter]:
        return {n: p for n, p in self.parameters.items() if isinstance(p, cls)}

    @property
    def input_parameters(self) -> Dict[str, InputParameter]:
        return self._filter_parameters(InputParameter)

    @property
    def output_parameters(self) -> Dict[str, OutputParameter]:
        return self._filter_parameters(OutputParameter)

    def argparse(self) -> dict:
        # add non-data arguments
        self.argument_parser.add_argument("--context", "-c", nargs="*")

        kwargs = vars(self.argument_parser.parse_args())
        kwargs = {k.replace("-", "_"): v for k, v in kwargs.items()}
        return kwargs

    def decorate(self, func: Callable) -> Callable:
        def func_(**kwargs):
            return func(**kwargs)

        return func_

    def add_parameter(self, parameter: Parameter) -> None:
        name = parameter.name
        assert name not in self.parameters
        assert not name.startswith("_")
        self.parameters[name] = parameter
        argname = name.replace("_", "-")
        self.argument_parser.add_argument(argname)

    def get_job_signature(self):
        return (
            str(self.function.as_rdf()),
            {n: str(p.as_rdf()) for n, p in sorted(self.input_parameters.items())},
        )

    def get_job_id(self, job) -> str:
        data_b = json.dumps(job, ensure_ascii=False).encode()
        return hashlib.md5(data_b).hexdigest()

    def bind_parameter_values(self, kwargs: Dict[str, str]) -> None:
        assert set(kwargs) == set(self.parameters)
        for n, p in self.parameters.items():
            value = kwargs[n]
            p.bind_value(value)

    def load_input_data(self) -> dict:
        return {n: p.load() for n, p in self.input_parameters.items()}

    def save_output_data(self, output_data: object) -> None:
        for p in self.output_parameters.values():
            p.save(output_data)

    def get_activity_rdf(self) -> URIRef:
        # TODO: something something name, date, job_id
        job_id = self.get_job_id(self.get_job_signature())
        fun_name = self.function.function.__name__
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        return URIRef(f"activity://{fun_name}_{timestamp}_{job_id}")

    def create_metadata(self, _context=None) -> list:
        activity_uri = self.get_activity_rdf()
        ns = Namespace("http://todo/")
        triples = []

        # activity type, function,job,...
        triples.append((activity_uri, RDF.type, ns.activity))
        triples.append((activity_uri, ns.function, self.function.as_rdf))
        triples.append(
            (activity_uri, ns.jobId, Literal(self.get_job_id(self.get_job_signature())))
        )
        # add context
        for key, val in (_context or {}).items():
            key = URIRef(key)  # must be uri
            val = as_rdf(val)
            triples.append((activity_uri, key, val))
            print((activity_uri, key, val))

        # TODO: add more generated metadata from job execution

        # create uris for input and output relationships
        for n, p in self.input_parameters.items():
            uri = activity_uri + "#" + n
            triples.append((activity_uri, ns.input, uri))
            triples.append((uri, ns.name, Literal(n)))
            triples.append((uri, ns.value, p.as_rdf()))
            triples.append((uri, RDF.type, ns.activityInput))
            # TODO: add more generated metadata from input deserialization?

        for n, p in self.output_parameters.items():
            uri = activity_uri + "#" + n
            triples.append((activity_uri, ns.output, uri))
            triples.append((uri, ns.name, Literal(n)))
            triples.append((uri, ns.value, p.as_rdf()))
            triples.append((uri, RDF.type, ns.activityOutput))
            # TODO: add more generated metadata from output generation / serialization

        return triples

    def store_metadata(self, _context=None):
        metadata_triples = self.create_metadata(_context=_context)
        # TODO: store in triplestore, store with output(s)
        for spo in metadata_triples:
            pass

    def __call__(self, _context: dict = None, **kwargs: Dict[str, str]) -> None:
        self.bind_parameter_values(kwargs)

        input_data = self.load_input_data()
        # TODO: optionally validate input_data with input_data_schema

        output_data = self.function(**input_data)
        # TODO: optionally validate output_data with output_data_schema

        self.save_output_data(output_data)

        self.store_metadata(_context=_context)

    def main(self):
        # get string parameters from user (sys.argv)
        kwargs = self.argparse()

        # pop non-data arguments (e.g. logging)
        context = parse_context_list(kwargs.pop("context"))

        self(_context=context, **kwargs)


activity = Activity(
    function=function,
    parameters=[
        LiteralInputParameter("input_1"),
        JsonInputParameter("input_2"),
        PartialJsonOutputParameter("output_1"),
        PartialJsonOutputParameter("output_2"),
    ],
)


activity.main()

# OR

activity(
    input_1="1",
    input_2="input-resource1.json",
    output_1="output_resource1.json",
    output_2="output_resource2.json",
    _context={},
)
