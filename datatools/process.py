import hashlib
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Optional, Union, cast

from datatools.base import (
    FUNCTION_URI_PREFIX,
    PROCESS_URI_PREFIX,
    Metadata,
    ParameterKey,
    ParamterTypes,
    ProcessException,
    Type,
)
from datatools.converter import Converter
from datatools.storage import Resource, Storage
from datatools.utils import (
    copy_signature,
    get_args_kwargs_from_dict,
    get_function_datatype,
    get_function_description,
    get_function_filepath,
    get_function_parameters_datatypes,
    get_git_info,
    get_git_root,
    get_now,
    get_suffix,
    get_user_w_host,
    get_value_type,
)

__all__ = ["Function"]


def constant_as_function(value: Any) -> Callable:
    def fun():
        return value

    return fun


def get_function_uri(function: Callable) -> str:
    # get git info
    filepath = get_function_filepath(function)
    git_root = get_git_root(filepath)
    # find git repository
    git_info = get_git_info(git_root)
    path = Path(filepath).relative_to(git_root).as_posix()
    name = function.__name__
    return f"{FUNCTION_URI_PREFIX}%(origin)s/%(commit)s/{path}:{name}" % git_info


class Function:
    """can be used as decorator around functions"""

    def __init__(
        self,
        function: Callable,
        uri: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parameters_types: Optional[ParamterTypes] = None,
        result_type: Optional[Type] = None,
    ):

        self.function: Callable = function
        self.uri: str = uri or get_function_uri(function)
        self.name: str = name or self.function.__name__
        self.description: Optional[str] = description or get_function_description(
            function
        )
        self.parameters_types: ParamterTypes = (
            parameters_types or get_function_parameters_datatypes(function)
        )
        self.result_type: Type = result_type or get_function_datatype(function)

        # set signature to underlying function (@pproperty is not working here)
        copy_signature(self, self.function)

    def __call__(self, *args, **kwargs):
        """Call the underlying function."""
        return self.function(*args, **kwargs)

    @classmethod
    def wrap(cls) -> Callable:
        def decorator(function) -> Function:
            if isinstance(function, Function):
                return function
            return Function(function=function)

        return decorator

    def get_input_type(self, key: ParameterKey) -> Type:
        param = self.get_parameter_name(key)
        parameters_types = self.parameters_types or {}
        return parameters_types[param]

    def get_parameter_name(self, key: ParameterKey) -> str:
        if key is None:
            key = 0
        if isinstance(key, int):
            parameters_types = list(self.parameters_types or {})
            key = parameters_types[key]
        return cast(str, key)

    def process(self, *input_args: Any, **input_kwargs: Any) -> "Process":
        # combine args / kwargs
        input_args_kwargs = {
            key: input for key, input in enumerate(input_args)
        } | input_kwargs

        inputs = {
            cast(ParameterKey, key): Input.wrap(
                input=input, type_to=self.get_input_type(key)
            )
            for key, input in input_args_kwargs.items()
        }
        return Process(function=self, inputs=inputs)

    @cached_property
    def metadata(self) -> Metadata:
        """Metadata about the function."""
        return {
            "@id": self.uri,
            "@type": "Function",
            "name": self.name,
            "description": self.description,
            # "parameters_types": self.parameters_types,
            "datatype": self.result_type,
        }


class Input:

    def __init__(self, read_input: Callable, type_to: Type, source_uri_or_literal: str):
        self.read_input: Callable = read_input
        self.type_to: Type = type_to
        self.source_uri_or_literal: str = source_uri_or_literal

    def __call__(self) -> Any:
        return self.read_input()

    @classmethod
    def wrap(cls, input: Any, type_to: Type = None) -> "Input":
        if not isinstance(input, Input):
            source_uri_or_literal = None
            if isinstance(input, Resource):
                read_input = input.get_loader(type_to=type_to)
                source_uri_or_literal = input.name
            elif not isinstance(input, Callable):
                # literal: convert now
                source_uri_or_literal = str(input)
                input_converted = Converter.convert_to(input, type_to=type_to)

                def read_input():
                    return input_converted

            else:
                # generic callable
                read_input = input
                source_uri_or_literal = ""  # TODO
            if type_to is None:
                raise TypeError(
                    "Input type could not be detected. "
                    f"Either create Input manually or add type hints: {input}"
                )
            input = Input(
                read_input=read_input,
                type_to=type_to,
                source_uri_or_literal=source_uri_or_literal,
            )

        return input

    @cached_property
    def metadata(self) -> Metadata:
        """Metadata about the function."""
        return {
            "source": self.source_uri_or_literal,
            "datatype": self.type_to,
        }


class Output:
    def __init__(
        self,
        uri: str,
        handle_output_data_metadata: Callable,
        type_from: Type,
        result_object: Any = None,
    ):
        self.uri = uri
        self.handle_output_data_metadata = handle_output_data_metadata
        self.type_from = type_from
        self.result_object = result_object

    def __call__(self, data: Any, metadata: Any) -> None:
        # write output function must be take data and metadata
        self.handle_output_data_metadata(data, metadata)
        return self.result_object

    @classmethod
    def wrap(
        cls,
        process: "Process",
        key: ParameterKey,
        output_uri: str,
        output: Any,
        type_from: Type = None,
    ) -> "Output":
        if not isinstance(output, Output):
            result_object = None
            if isinstance(output, Resource):
                result_object = output
                handle_output_data_metadata = output.get_dumper(type_from=type_from)
            elif isinstance(output, Storage):
                # use output.uri as resource name (storage will modify it)
                # NOTE: we need to determine file type / extension.
                # until we find a better solution, we use ths storages default
                storage = output
                # generate a unique (file)-name automatically
                process_uri_hash = hashlib.md5(process.uri.encode()).hexdigest()
                suffix = get_suffix(process.uri) or storage.default_filetype

                # TODO: maybe get process_uri hash, not output uri hash
                p_key = "" if key is None else "/key"

                name = f"process/{process_uri_hash}/output{p_key}{suffix}"
                resource = output.resource(name=name)
                handle_output_data_metadata = resource.get_dumper(type_from=type_from)
                result_object = resource
            elif isinstance(output, Callable):
                handle_output_data_metadata = output
            else:
                raise TypeError(output)

            if type_from is None:
                raise TypeError(
                    "Output type could not be detected. "
                    f"Either create Output manually or add type hints: {output}"
                )
            output = Output(
                uri=output_uri,
                handle_output_data_metadata=handle_output_data_metadata,
                type_from=type_from,
                result_object=result_object,
            )
        return output


def get_process_uri(
    function: Function,
    inputs: dict[ParameterKey, Input],
) -> str:
    uri = function.uri
    params = "&".join(
        f"{name}={input.source_uri_or_literal}" for name, input in inputs.items()
    )
    uri = uri.replace(FUNCTION_URI_PREFIX, PROCESS_URI_PREFIX) + f"?{params}"
    return uri


class Process:

    def __init__(
        self,
        function: Function,
        inputs: dict[ParameterKey, Input],
        uri: Optional[str] = None,
        context: Optional[Metadata] = None,
    ):
        self.function = function
        self.inputs = inputs
        self.uri = uri or get_process_uri(
            function,
            inputs,
        )
        self.context: Metadata = context or {}

    def __call__(self, *output_args, **output_kwargs) -> Union[dict, Any]:
        """Run the process."""

        # first: prepare outputs
        output_args_kwargs = dict(enumerate(output_args)) | output_kwargs
        type_from = self.function.result_type
        # if only single arg: make it as None == single output
        if set(output_args_kwargs) == {0}:
            output_args_kwargs = {None: output_args_kwargs[0]}
        else:
            type_from = get_value_type(type_from)

        if None in output_args_kwargs:
            if not set(output_args_kwargs) == {None}:
                raise ProcessException(
                    "Output specification conflict: single output but multiple defined"
                )

        outputs = {
            key: Output.wrap(
                process=self,
                key=key,
                output_uri=f"{self.uri}#output" + ("" if key is None else f"/{key}"),
                output=output,
                type_from=type_from,
            )
            for key, output in output_args_kwargs.items()
        }

        # read input values
        data = {key: input() for key, input in self.inputs.items()}

        # map to function arguments
        args, kwargs = get_args_kwargs_from_dict(data)

        # call original functions
        result = self.function(*args, **kwargs)

        # create process metadata # TODO
        dynmic_metadata = {"datetime": get_now(), "user": get_user_w_host()}
        metadata = self.metadata | dynmic_metadata

        results = {}
        for key, output in outputs.items():
            metadata = {
                "@id": output.uri,
                "createdBy": metadata,
                "datatype": output.type_from,
            }
            partial_result = result if key is None else result[key]
            # write output function must be take data and metadata
            results[key] = output(partial_result, metadata)

        if None in output_args_kwargs:
            results = results[None]

        return results

    @classmethod
    def from_uri(cls, uri: str) -> "Process":
        handler = Converter.convert_to(uri, Callable)
        function = Function(function=handler)
        process = function.process(uri)
        return process

    @cached_property
    def metadata(self) -> Metadata:
        """Metadata about the function."""
        return {
            "@id": self.uri,
            "@type": "Process",
            "function": self.function.metadata,
            "input": [
                input.metadata
                | {
                    "role": self.function.get_parameter_name(key),
                    "@id": f"{self.uri}#input" + ("" if key is None else f"/{key}"),
                    "@type": "input",
                }
                for key, input in self.inputs.items()
            ],
        } | (self.context or {})
