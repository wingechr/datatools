import datetime
import inspect
import json
import logging
from io import BytesIO, TextIOWrapper
from typing import Any, Callable, Union, get_type_hints, get_args

import pandas as pd

RoleType = Union[str, int, None]


def get_value_type(dtype: type) -> type:
    # dict[Any, int] -> int
    # list[int] -> int
    return get_args(dtype)[-1]


def str_is_uri(s: str) -> bool:
    return ":" in s


def is_data_uri(uri: str) -> bool:
    return uri.startswith("data://")


def as_resource(x, storage: "Storage" = None) -> "AbstractResource":
    if isinstance(x, AbstractResource):
        return x
    if isinstance(x, str) and str_is_uri(x):
        if not storage:
            raise Exception("Storage not defined")
        return Resource(storage, x)
    else:
        return LiteralResource(x)


def dict_add(dct: dict, key: Any, value: Any) -> None:
    assert key not in dct
    dct[key] = value


def split_args_kwargs(
    data: dict[RoleType, Any], assert_args_range: bool = True
) -> tuple[tuple[Any], dict[str, Any]]:
    args_d = {}
    kwargs = {}
    if None in data:  # primitive
        assert set(data) == {None}
        args = [data[None]]
    else:
        for k, v in data.items():
            if isinstance(k, int):
                args_d[k] = v
            elif isinstance(k, str):
                kwargs[k] = v
            else:
                raise TypeError(k)
        if assert_args_range:
            assert set(args_d) == set(range(len(args_d)))
        # fill missing positionals with None
        max_idx = max(args_d) + 1 if args_d else 0
        args = [args_d[i] for i in range(max_idx)]
    return args, kwargs


def get_type_name(cls: type) -> str:
    if cls is None:
        return "Any"
    return f"{cls.__module__}.{cls.__qualname__}"


def as_io_dict(x) -> dict:
    if x is None:
        return {}
    if not isinstance(x, dict):
        return {None: x}
    return x


class Component:
    def __init__(self, id: str = None):
        self._id = id

    @property
    def id(self) -> str:
        # if id doesnot exist: generate,
        # but only once (dont change again)
        if not self._id:
            self._id = self._create_id()
        return self._id

    def _create_id(self) -> str:
        raise NotImplementedError()

    def get_metadata(self) -> dict:
        return {"@id": self.id}


class AbstractResource:
    pass


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


class LiteralResource(AbstractResource):
    def __init__(self, data: str):
        self.data = data

    def infer_decoder(self, dtype=None) -> Callable:
        print(f"infer_converter(str, {dtype})")
        return float

    def infer_encoder(self, dtype=None) -> Callable:
        print(f"infer_converter({dtype}, str)")
        return str


class Function(Component):
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
        super().__init__(id=id)
        self.function = function
        self.kwargs = kwargs
        self.description = description or self.infer_function_description(function)
        self.name = name or self.infer_function_name(function)
        self.input_types = input_types or self.infer_function_input_schema(function)
        self.output_type = output_type or self.infer_function_output_type(function)

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
        metadata = super().get_metadata()
        if self.kwargs:
            metadata["kwargs"] = self.kwargs
        metadata["name"] = self.name
        metadata["description"] = self.description
        metadata["inputTypes"] = {
            k: get_type_name(v) for k, v in self.input_types.items()
        }
        metadata["outputType"] = get_type_name(self.output_type)

        return metadata


class Input(Component):
    def __init__(
        self,
        process: "Process",
        resource: Resource,
        function: Function,
        role: RoleType = None,
    ):
        super().__init__()
        self.role: RoleType = role
        self.function = function
        self.resource = resource
        self.process = process

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
        metadata = super().get_metadata()
        if self.role is not None:
            metadata["role"] = self.role
        # NOTE: dont copy complete resource metadata
        if self.resource:
            # use key "data"
            metadata["data"] = {"@id": self.resource.id}
        metadata["function"] = self.function.get_metadata()

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


class Output(Component):
    def __init__(
        self,
        process: "Process",
        resource: Resource,
        function: Function,
        role: RoleType = None,
    ):
        super().__init__()
        self.role: RoleType = role
        self.function = function
        self.resource = resource
        self.process = process

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
        metadata = super().get_metadata()
        metadata["function"] = self.function.get_metadata()
        metadata["process"] = self.process.get_metadata()
        if self.role is not None:
            metadata["role"] = self.role
        return metadata


class Storage:
    def resource(self, id: str):
        return Resource(storage=self, id=id)


class Process(Component):
    def __init__(
        self,
        function: Union[Function, Callable],
        id: str = None,
        inputs: Union[AbstractResource, dict[str, AbstractResource]] = None,
        outputs: Union[AbstractResource, dict[str, AbstractResource]] = None,
        context: Any = None,
        default_storage: "Storage" = None,
    ):
        super().__init__(id=id)
        self.function: Function = Function.as_function(function)

        self.inputs: dict[str, Input] = {}
        for role, x in as_io_dict(inputs).items():
            input_type = self.function.get_input_type(role)
            x = as_resource(x, storage=default_storage)
            if isinstance(x, LiteralResource):
                inp = LiteralInput(
                    process=self,
                    data=x.data,
                    function=Function.as_function(x.infer_decoder(input_type)),
                    role=role,
                )
            else:
                inp = Input(
                    process=self,
                    resource=x,
                    function=Function.as_function(x.infer_decoder(input_type)),
                    role=role,
                )

            self.inputs[role] = inp

        self.outputs: dict[str, Output] = {}
        for role, x in as_io_dict(outputs).items():
            output_type = self.function.get_output_type(role)
            x = as_resource(x, storage=default_storage)
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

    def _create_id(self) -> str:
        return "urn:process/1"

    def get_metadata(self):
        metadata = super().get_metadata()
        metadata["run"] = self._metadata_run
        metadata["context"] = self.context
        metadata["function"] = self.function.get_metadata()
        metadata["inputs"] = [input.get_metadata() for input in self.inputs.values()]

        return metadata


def dfmult(df: pd.DataFrame, factor: float) -> pd.DataFrame:
    return df * factor


def csv_to_df(
    data: bytes, encoding="utf-8", index=None, **read_csv_kwargs
) -> pd.DataFrame:
    buf = TextIOWrapper(BytesIO(data), encoding=encoding)
    df = pd.read_csv(buf, **read_csv_kwargs)
    if index:
        df = df.set_index(index)
    return df


def df_to_csv(
    data: pd.DataFrame, encoding="utf-8", index=False, **to_csv_kwargs
) -> bytes:
    buf = BytesIO()
    data.to_csv(buf, index=index, encoding=encoding, **to_csv_kwargs)
    buf.seek(0)
    data_bytes = buf.read()
    return data_bytes


st = Storage()


proc = Process(
    function=dfmult,
    context={"project": "test"},
    inputs={
        "df": "data://d1.csv",
        "factor": "10",
    },
    outputs="data://d2.csv",
    default_storage=st,
)

proc.run()
