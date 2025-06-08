import datetime
import inspect
import json
import logging
from typing import Any, Callable, Union, get_type_hints
from utils import (
    is_uri,
    csv_to_df,
    df_to_csv,
    get_type_name,
    get_value_type,
    split_args_kwargs,
    dict_add,
    as_io_dict,
    RoleType,
    get_default_filetype,
)


class AbstractResource:

    @classmethod
    def as_resource(cls, x, storage: "Storage" = None) -> "AbstractResource":
        if isinstance(x, AbstractResource):
            return x
        if isinstance(x, str) and is_uri(x):
            if not storage:
                raise Exception("Storage not defined")
            return Resource(storage, x)
        else:
            return LiteralResource(x)


class Resource(AbstractResource):
    def __init__(self, storage: "Storage", id: str, filetype: str = None):
        self.storage = storage
        self.id = id
        self.path = self.id.replace("data://", "")
        self.filetype = filetype

    @property
    def path_metadata(self) -> str:
        return self.path + ".metadata.json"

    def read_bytes(self):
        with open(self.path, "rb") as file:
            return file.read()

    def save_bytes(self, data: bytes) -> bytes:
        with open(self.path, "wb") as file:
            file.write(data)

    def save_metadata(self, metadata: Any) -> bytes:
        metadata_s = json.dumps(metadata, indent=2, ensure_ascii=False)
        metadata_b = metadata_s.encode()
        with open(self.path_metadata, "wb") as file:
            file.write(metadata_b)

    def infer_decoder(self, dtype=None) -> Callable:
        print(f"infer_converter({self.filetype}, {dtype})")
        return Function(csv_to_df, index="key")

    def infer_encoder(self, dtype=None) -> Callable:
        print(f"infer_converter({dtype}, {self.filetype})")
        return Function(df_to_csv, index=True)

    def __str__(self):
        return self.id


class LiteralResource(AbstractResource):
    def __init__(self, data: str):
        self.data = data

    def infer_decoder(self, dtype=None) -> Callable:
        print(f"infer_converter(str, {dtype})")
        return float

    def infer_encoder(self, dtype=None) -> Callable:
        print(f"infer_converter({dtype}, str)")
        return str

    def __str__(self):
        return self.data


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
            role = next(self.input_types)
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
    def infer_function_id(cls, function: Callable):
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

    def get_metadata(self):
        metadata = {"@id": self.id}
        if self.kwargs:
            metadata["kwargs"] = self.kwargs
        metadata["name"] = self.name
        metadata["description"] = self.description
        metadata["inputTypes"] = {
            k: get_type_name(v) for k, v in self.input_types.items()
        }
        metadata["outputType"] = get_type_name(self.output_type)

        return metadata


class Input:
    def __init__(
        self,
        process: "Process",
        resource: Resource,
        function: Function,
        role: RoleType = None,
    ):
        self.role: RoleType = role
        self.function = function
        self.resource = resource
        self.process = process
        self.id = self._create_id()

    def load(self) -> Any:
        data_bytes = self.resource.read_bytes()
        data = self.function(data_bytes)
        return data

    def _create_id(self) -> str:
        id_input = f"{self.process.id}/input"
        if self.role is None:
            return id_input
        else:
            return f"{id_input}/{self.role}"

    def get_metadata(self):
        metadata = {"@id": self.id}
        if self.role is not None:
            metadata["role"] = self.role
        # NOTE: dont copy complete resource metadata
        if self.resource:
            # use key "data"
            metadata["data"] = {"@id": self.resource.id}

        # metadata["function"] = self.function.get_metadata()
        # only get id and description,not the whole thinf
        metadata["decoder"] = {"@id": self.function.id}

        return metadata


class LiteralInput(Input):
    def __init__(
        self,
        process: "Process",
        data: str,
        function: Function,
        role: RoleType = None,
    ):
        super().__init__(
            process=process,
            resource=None,
            function=function,
            role=role,
        )
        self.data = data

    def load(self) -> Any:
        data = self.function(self.data)
        return data

    def get_metadata(self):
        metadata = super().get_metadata()
        # overwrite from resource
        metadata["data"] = {"@value": self.data}

        return metadata


class Output:
    def __init__(
        self,
        process: "Process",
        resource: Resource,
        function: Function,
        role: RoleType = None,
    ):
        self.role: RoleType = role
        self.function = function
        self.resource = resource
        self.process = process
        self.id = self._create_id()

    def get_data(self, data: Any) -> Any:
        if self.role is None:
            return data
        return data[self.role]

    def handle(self, data: Any) -> None:
        data = self.get_data(data)
        data_bytes = self.function(data)

        metadata_output = self.get_metadata()
        metadata_resource = {"created": metadata_output, "filetype": "TODO"}

        self.resource.save_bytes(data_bytes)
        self.resource.save_metadata(metadata_resource)

    def _create_id(self) -> str:
        id_output = f"{self.process.id}/output"
        if self.role is None:
            return id_output
        else:
            return f"{id_output}/{self.role}"

    def get_metadata(self):
        metadata = {"@id": self.id}
        metadata["encoder"] = self.function.get_metadata()
        metadata["process"] = self.process.get_metadata()
        if self.role is not None:
            metadata["role"] = self.role
        return metadata


class Storage:
    def __init__(self, location: str):
        self.location = location

    def resource(self, id: str):
        return Resource(storage=self, id=id)


class Process:
    def __init__(
        self,
        function: Union[Function, Callable],
        id: str = None,
        inputs: Union[AbstractResource, dict[str, AbstractResource]] = None,
        outputs: Union[AbstractResource, dict[str, AbstractResource]] = None,
        context: Any = None,
        default_storage: "Storage" = None,
    ):
        self.function: Function = Function.as_function(function)
        input_resources = {
            role: AbstractResource.as_resource(x, storage=default_storage)
            for role, x in as_io_dict(inputs).items()
        }

        self.id = id or self._create_id(input_resources)

        self.inputs: dict[str, Input] = {}
        for role, resource in input_resources.items():
            input_type = self.function.get_input_type(role)
            if isinstance(resource, LiteralResource):
                inp = LiteralInput(
                    process=self,
                    data=resource.data,
                    function=Function.as_function(resource.infer_decoder(input_type)),
                    role=role,
                )
            else:
                inp = Input(
                    process=self,
                    resource=resource,
                    function=Function.as_function(resource.infer_decoder(input_type)),
                    role=role,
                )

            self.inputs[role] = inp

        self.outputs: dict[str, Output] = {}

        if not outputs:
            # auto generate
            role = None
            output_type = self.function.get_output_type(role)
            filetype = get_default_filetype(output_type)
            res = "data://d2.csv"

            outputs = {role: res}

        for role, x in as_io_dict(outputs).items():
            output_type = self.function.get_output_type(role)
            x = AbstractResource.as_resource(x, storage=default_storage)
            outp = Output(
                process=self,
                role=role,
                resource=x,
                function=Function.as_function(x.infer_encoder(output_type)),
            )

            self.outputs[role] = outp

        self.context: Any = context
        self._metadata_run = None
        self.default_storage = default_storage

    def check(self):
        # check input/output types of function/function
        input_types_names_ordered = list(self.function.input_types)

        args_kwargs: dict[RoleType, Any] = {}
        for input in self.inputs.values():
            dict_add(args_kwargs, input.role, input)

        args, kwargs = split_args_kwargs(args_kwargs)
        for i, arg in enumerate(args):
            dict_add(kwargs, input_types_names_ordered[i], arg)

        all_keys = set(self.function.input_types) | set(kwargs)
        for key in all_keys:
            type_expected = get_type_name(self.function.input_types.get(key))
            input: Input = kwargs.get(key)
            if input:
                type_got = get_type_name(input.function.output_type)
            else:
                type_got = None
            if type_expected is None:
                logging.warning("Unexpected input for %s", key)
            elif type_got is None:
                logging.warning("Missing input for %s", key)
            elif type_expected != type_got and (
                type_expected != "Any" and type_got != "Any"
            ):
                logging.warning(
                    "input type %s does not match epexted %s for %s",
                    type_got,
                    type_expected,
                    key,
                )

        type_expected = get_type_name(self.function.output_type)
        for output in self.outputs.values():
            type_got = get_type_name(list(output.function.input_types.values())[0])
            if type_expected != type_got and (
                type_expected != "Any" and type_got != "Any"
            ):
                logging.warning(
                    "output type %s does not match epexted %s for %s",
                    type_got,
                    type_expected,
                    output.role,
                )

    def create_metadata_run(self):
        self._metadata_run = {"timestamp": datetime.datetime.now().isoformat()}

    def run(self) -> None:

        self.check()
        self.create_metadata_run()

        # process all inputs, combine with role into dict
        # role can be name, position (int), or None (All)
        args_kwargs: dict[RoleType, Any] = {}
        for input in self.inputs.values():
            data = input.load()
            dict_add(args_kwargs, input.role, data)

        args, kwargs = split_args_kwargs(args_kwargs)

        # call function
        result = self.function(*args, **kwargs)

        # process output
        for output in self.outputs.values():
            output.handle(result)

    def _create_id(self, resource: dict[str, AbstractResource]) -> str:
        return (
            self.function.id
            + "?"
            + "&".join(f"{role}={res}" for role, res in resource.items())
        )

    def get_metadata(self):
        metadata = {"@id": self.id}
        metadata["run"] = self._metadata_run
        metadata["context"] = self.context
        metadata["function"] = self.function.get_metadata()
        metadata["inputs"] = [input.get_metadata() for input in self.inputs.values()]

        return metadata
