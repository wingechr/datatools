from dataclasses import dataclass
from functools import cached_property
from typing import Any, Callable, Optional, cast

from datatools.classes import ParameterKey, Type
from datatools.converter import Converter
from datatools.storage import Resource
from datatools.utils import (
    copy_signature,
    get_args_kwargs_from_dict,
    get_parameters_types,
    get_result_type,
    get_value_type,
)

__all__ = ["Function"]


def constant_as_function(value: Any) -> Callable:
    def fun():
        return value

    return fun


@dataclass(frozen=True)
class Function:
    """can be used as decorator around functions"""

    function: Callable
    name: Optional[str] = None
    description: Optional[str] = None
    parameters_types: Optional[dict[str, Type]] = None
    result_type: Optional[Type] = None

    def __call__(self, *args, **kwargs):
        """Call the underlying function."""
        return self.function(*args, **kwargs)

    def __post_init__(self):

        # set signature to underlying function (@pproperty is not working here)
        copy_signature(self, self.function)

        if self.parameters_types is None:
            object.__setattr__(
                self, "parameters_types", get_parameters_types(self.function)
            )
        if self.result_type is None:
            object.__setattr__(self, "result_type", get_result_type(self.function))
        if self.name is None:
            object.__setattr__(self, "name", self.function.__name__)
        if self.description is None:
            object.__setattr__(self, "description", self.function.__doc__)

        if self.result_type is None:
            raise TypeError(
                "Function result type could not be detected. "
                f"Either create Function manually or add type hints: {self.function}"
            )
        if any(x is None for x in (self.parameters_types or {}).values()):
            raise TypeError(
                "Function parameter types could not be detected. "
                "Either create Function manually "
                f"or add type hints: {self.parameters_types}"
            )

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
        return key

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
    def metadata(self) -> dict[str, Any]:
        """Metadata about the function."""
        return {
            "name": self.name,
            "description": self.description,
            "parameters_types": self.parameters_types,
            "result_type": self.result_type,
        }


@dataclass(frozen=True)
class Input:

    read_input: Any
    type_to: Type

    def __call__(self) -> Any:
        return self.read_input()

    @classmethod
    def wrap(cls, input: Any, type_to: Type = None) -> "Input":
        if not isinstance(input, Input):
            if isinstance(input, Resource):
                read_input = input.get_loader(type_to=type_to)
            elif not isinstance(input, Callable):
                # literal: convert now
                input_converted = Converter.convert_to(input, type_to=type_to)

                def read_input():
                    return input_converted

            else:
                # generic callable
                read_input = input
            if type_to is None:
                raise TypeError(
                    "Input type could not be detected. "
                    f"Either create Input manually or add type hints: {input}"
                )
            input = Input(read_input=read_input, type_to=type_to)
        return input


@dataclass(frozen=True)
class Output:

    handle_output_data_metadata: Callable
    type_from: Type

    def __call__(self, data: Any, metadata: Any) -> None:
        # write output function must be take data and metadata
        return self.handle_output_data_metadata(data, metadata)

    @classmethod
    def wrap(cls, output: Any, type_from: Type = None) -> "Output":
        if not isinstance(output, Output):
            if isinstance(output, Resource):
                handle_output_data_metadata = output.get_dumper(type_from=type_from)
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
                handle_output_data_metadata=handle_output_data_metadata,
                type_from=type_from,
            )
        return output


@dataclass(frozen=True)
class Process:
    function: Function
    inputs: dict[ParameterKey, Input]

    def __call__(self, *output_args, **output_kwargs) -> None:
        """Run the process."""

        # first: prepare outputs
        output_args_kwargs = dict(enumerate(output_args)) | output_kwargs
        type_from = self.function.result_type
        # if only single arg: make it as None == single output
        if set(output_args_kwargs) == {0}:
            output_args_kwargs = {None: output_args_kwargs[0]}
        else:
            type_from = get_value_type(type_from)

        outputs = {
            key: Output.wrap(output=output, type_from=type_from)
            for key, output in output_args_kwargs.items()
        }

        # read input values
        data = {key: input() for key, input in self.inputs.items()}

        # map to function arguments
        args, kwargs = get_args_kwargs_from_dict(data)

        # call original functions
        result = self.function(*args, **kwargs)

        # create process metadata # TODO
        metadata = self.function.metadata

        for key, output in outputs.items():
            partial_result = result if key is None else result[key]
            # write output function must be take data and metadata
            output(partial_result, metadata)
