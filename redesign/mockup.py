import datetime
import inspect
import json
import logging
import os
import re
from typing import Any, Callable, Union, get_type_hints

from utils import (
    RoleType,
    as_io_dict,
    dict_add,
    get_default_filetype,
    get_type_name,
    get_value_type,
    infer_converter,
    infer_loader,
    is_data_uri,
    is_uri,
    split_args_kwargs,
)

ResourceOrStrType = Union["AbstractResource", str]
ResourcesType = Union[ResourceOrStrType, dict[RoleType, ResourceOrStrType]]


class AbstractResource:

    @classmethod
    def as_resource(
        cls, x: ResourceOrStrType, storage: "Storage" = None
    ) -> "AbstractResource":
        if isinstance(x, AbstractResource):
            return x
        if isinstance(x, str) and is_uri(x):
            if not storage:
                raise Exception("Storage not defined")
            if is_data_uri(x):
                path = x.replace("data://", "")
                return Resource(storage, path)
            else:
                return RemoteResource(storage, x)
        else:
            return LiteralResource(x)

    def get_bytes(self) -> bytes:
        raise NotImplementedError()

    def set_bytes(self, bdata: bytes) -> None:
        raise NotImplementedError()

    def get_data(self, dtype=None, **kwargs) -> Any:
        decoder = self.infer_decoder(dtype)
        bdata = self.get_bytes()
        data = decoder(bdata, **kwargs)
        return data

    def set_data(self, data: Any, dtype=None, **kwargs) -> None:
        encoder = self.infer_encoder(dtype)
        bdata = encoder(data, **kwargs)
        return self.set_bytes(bdata)

    def set_metadata(self, metadata: dict) -> None:
        raise NotImplementedError()

    def get_metadata(self) -> dict:
        raise NotImplementedError()

    def infer_decoder(self, dtype=None) -> Callable:
        raise NotImplementedError()

    def infer_encoder(self, dtype=None) -> Callable:
        raise NotImplementedError()

    def exists(self) -> bool:
        raise NotImplementedError()

    def to_str(self) -> str:
        raise NotImplementedError()


class LiteralResource(AbstractResource):
    def __init__(self, sdata: str):
        if not isinstance(sdata, str):
            logging.warning("literal data is not str")
            sdata = "" if sdata is None else str(sdata)
        self._sdata = sdata
        self._bdata = sdata.encode()

    def infer_decoder(self, dtype=None) -> Callable:
        # uses dtype constructor
        return lambda bdata: dtype(bdata.decode())

    def get_bytes(self) -> bytes:
        return self._bdata

    def __str__(self) -> str:
        return f"LiteralResource({self._sdata})"

    def get_metadata(self) -> dict:
        return {"@value": self._sdata}

    def to_str(self) -> str:
        return self._sdata


class Resource(AbstractResource):
    def __init__(self, storage: "Storage", path: str):
        self.storage = storage
        self.path = path

    @property
    def path_metadata(self) -> str:
        return self.path + ".metadata.json"

    @property
    def filetype(self) -> str:
        return self.path.split(".")[-1]

    @property
    def uri(self) -> str:
        return f"data://{self.path}"

    def get_bytes(self):
        with open(self.path, "rb") as file:
            return file.read()

    def set_bytes(self, data: bytes) -> bytes:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "wb") as file:
            file.write(data)

    def set_metadata(self, metadata: dict) -> None:
        metadata_s = json.dumps(metadata, indent=2, ensure_ascii=False)
        metadata_b = metadata_s.encode()
        with open(self.path_metadata, "wb") as file:
            file.write(metadata_b)

    def infer_decoder(self, dtype=None) -> Callable:
        # TODO: arguemnts from metadata
        return Function(infer_converter(self.filetype, dtype))

    def infer_encoder(self, dtype=None) -> Callable:
        # TODO: arguemnts from metadata
        return Function(infer_converter(dtype, self.filetype))

    def get_metadata(self) -> dict:
        return {"@id": self.path}

    def __str__(self) -> str:
        return f"Resource({self.path})"

    def exists(self) -> bool:
        return os.path.exists(self.path)

    def to_str(self) -> str:
        return self.uri


class RemoteResource(Resource):
    def __init__(self, storage: "Storage", source_uri: str, path: str = None):
        self.storage = storage
        self.source_uri = source_uri
        self.path = path or storage.create_path_from_id(source_uri)

    def infer_loader(self) -> Callable:
        return infer_loader(self.source_uri)

    def download(self):
        process = Process(
            function=self.infer_loader(),
            inputs=LiteralResource(self.source_uri),
            outputs=self,
        )
        process.run()

    def get_bytes(self):
        if not self.exists():
            self.download()
        return super().get_bytes()

    def __str__(self) -> str:
        return f"RemoteResource({self.source_uri}, {self.path})"

    def to_str(self) -> str:
        return self.source_uri


class Function:
    def __init__(
        self,
        function: Callable,
        id: str = None,
        name: str = None,
        description: str = None,
        input_types: dict[str, type] = None,
        output_type: type = None,
        **kwargs: dict,
    ):
        self.function = function
        self.kwargs = kwargs
        self.description = description or self.infer_function_description(function)
        self.name = name or self.infer_function_name(function)
        self.input_types = input_types or self.infer_function_input_schema(function)
        self.output_type = output_type or self.infer_function_output_type(function)
        self.id = id or self._create_id()

    def get_input_type(self, role: str = None):
        if role is None:
            # must be first role
            role = list(self.input_types.keys())[0]
        return self.input_types[role]

    def get_output_type(self, role: str = None):
        if role is None:
            return self.output_type
        else:
            return get_value_type(self.output_type)

    def _create_id(self) -> str:
        return self.infer_function_id(self.function)

    def __call__(self, *args, **kwargs) -> Any:
        return self.function(*args, **kwargs, **self.kwargs)

    @classmethod
    def infer_function_id(cls, function: Callable) -> str:
        return "urn:function/" + cls.infer_function_name(function)

    @classmethod
    def as_function(cls, function: Union[Callable, "Function"]) -> "Function":
        if not isinstance(function, Function):
            function = Function(function=function)
        return function

    @classmethod
    def infer_function_name(cls, function: Callable) -> str:
        return function.__name__

    @classmethod
    def infer_function_description(cls, function: Callable) -> str:
        return function.__doc__

    @classmethod
    def infer_function_input_schema(cls, function: Callable) -> dict[str, type]:
        try:
            sig = inspect.signature(function)
        except ValueError:
            # fails for some builtins, like str
            return {None: None}

        type_hints = get_type_hints(function)

        return {
            param.name: type_hints.get(param.name, None)
            for param in sig.parameters.values()
        }

    @classmethod
    def infer_function_output_type(cls, function: Callable) -> type:
        type_hints = get_type_hints(function)
        return type_hints.get("return", None)

    def get_metadata(self) -> dict:
        metadata = {"@id": self.id}
        if self.kwargs:
            metadata["kwargs"] = self.kwargs
        metadata["name"] = self.name
        metadata["description"] = self.description
        # metadata["inputTypes"] = {
        #    k: get_type_name(v) for k, v in self.input_types.items()
        # }
        # metadata["outputType"] = get_type_name(self.output_type)

        return metadata


class Storage:
    def __init__(self, location: str):
        self.location = location

    def resource(self, path: str):
        return Resource(storage=self, path=path)

    def create_path_from_id(self, id: str, suffix=None) -> str:
        # TODO
        path = re.sub("[^a-zA-Z0-9_.-]+", "/", id).strip("/")
        return path + (suffix or "")


class Process:
    def __init__(
        self,
        function: Union[Function, Callable],
        inputs: ResourcesType = None,
        outputs: ResourcesType = None,
        context: dict = None,
        default_storage: "Storage" = None,
    ):
        self.function: Function = Function.as_function(function)

        self.inputs: dict[str, AbstractResource] = {
            role: AbstractResource.as_resource(x, storage=default_storage)
            for role, x in as_io_dict(inputs).items()
        }

        if not outputs:
            if not default_storage:
                raise Exception("default_storage missing")
            # auto generate
            role = None
            output_type = self.function.get_output_type(role)
            filetype = get_default_filetype(output_type)
            output_id = self._get_part_id(self._create_id(), "output")
            path = default_storage.create_path_from_id(output_id, suffix=f".{filetype}")
            res = default_storage.resource(path=path)
            outputs = {role: res}
        self.outputs: dict[str, Resource] = {
            role: AbstractResource.as_resource(x, storage=default_storage)
            for role, x in as_io_dict(outputs).items()
        }

        print(self.inputs)
        print(self.outputs)

        self.context: Any = context or {}
        self.default_storage = default_storage

    def create_metadata_run(self) -> dict:
        return {"timestamp": datetime.datetime.now().isoformat()}

    @classmethod
    def _get_part_id(cls, id: str, inp_outp: str, role: str = None) -> str:
        pid = f"{id}#{inp_outp}"
        if role:
            pid = f"{pid}/{role}"
        return pid

    def run(self, id=None) -> None:

        if any(resource.exists() for resource in self.outputs.values()):
            raise Exception("some outputs already exist")

        metadata = self.create_metadata_run() | self.context

        id = id or self._create_id()
        metadata["input"] = []

        # process all inputs, combine with role into dict
        # role can be name, position (int), or None (All)
        args_kwargs: dict[RoleType, Any] = {}
        for role, resource in self.inputs.items():
            input_type = self.function.get_input_type(role)
            data = resource.get_data(input_type)
            dict_add(args_kwargs, role, data)
            metadata["input"].append(
                {
                    "@id": self._get_part_id(id, "input", role),
                    "role": role,
                    "resource": resource.get_metadata(),
                    "type": get_type_name(input_type),
                }
            )

        args, kwargs = split_args_kwargs(args_kwargs)

        # call function
        result = self.function(*args, **kwargs)
        metadata["function"] = self.function.get_metadata()

        # process output
        for role, resource in self.outputs.items():
            output_type = self.function.get_output_type(role)
            if role is None:
                data = result
            else:
                data = result[role]

            resource.set_data(data, output_type)
            metadata["@id"] = self._get_part_id(id, "output", role)
            metadata["type"] = get_type_name(output_type)
            resource.set_metadata(metadata)

    def _create_id(self) -> str:
        return (
            self.function.id
            + "?"
            + "&".join(
                f"{role}={res.to_str()}" for role, res in self.inputs.items()
            )  # TODO: handle role=None
        )
