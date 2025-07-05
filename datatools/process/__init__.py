from dataclasses import dataclass
from typing import Any, Callable, Union

__all__ = ["Process", "ProcessException"]

Key = Union[None, int, str]
KeyDict = dict[Key, Any]
KeyAny = Union[None, Any, list[Any], KeyDict, tuple[list[Any], dict[str, Any]]]
Type = Any


def infer_converter(type_from: Type, type_to: Type) -> Callable:
    # TODO
    return lambda x: x


def infer_from_bytes(type: Type) -> Callable:
    return infer_converter(bytes, type)


def infer_to_bytes(type: Type) -> Callable:
    return infer_converter(type, bytes)


@dataclass(frozen=True)
class Function:
    """can be used as decorator aroundfunctions"""

    function: Callable

    def __call__(self, *args, **kwargs) -> Any:
        return self.function(*args, **kwargs)

    @classmethod
    def as_function(cls, function: Union["Function", Any]) -> "Function":
        if isinstance(function, Function):
            return function
        return Function(function=function)

    def get_input_type(self, key: Key) -> Type:
        # TODO
        pass

    def get_output_type(self, key: Key) -> Type:
        # TODO
        pass


@dataclass(frozen=True)
class Input:

    read_input: Any

    def __call__(self) -> Any:
        return self.read_input()

    @classmethod
    def as_input(cls, input: Union["Input", Any], type_to: Type) -> "Input":
        if isinstance(input, Input):
            return input

        from_bytes = infer_from_bytes(type_to)

        def read_input():
            bdata = input()
            data = from_bytes(bdata)
            return data

        return Input(read_input)


@dataclass(frozen=True)
class Output:

    handle_output_data_metadata: Any

    def __call__(self, data: Any, metadata: Any) -> None:
        # write output function must be take data and metadata
        return self.handle_output_data_metadata(data, metadata)

    @classmethod
    def as_output(cls, output: Union["Output", Any], type_from: Type) -> "Output":
        if isinstance(output, Output):
            return output

        to_bytes = infer_to_bytes(type_from)

        def handle_output_data_metadata(data: Any, metadata: Any):
            bdata = to_bytes(data)
            return output(bdata, metadata)

        return Output(handle_output_data_metadata)


class ProcessException(Exception):
    pass


def any_to_dict(
    items: KeyAny,
) -> dict[Key, Any]:
    if items is None:
        return {}
    elif isinstance(items, tuple):
        args, kwargs = items
        return dict(list(enumerate(args)) + list(kwargs.items()))
    elif isinstance(items, list):
        return dict(enumerate(items))
    elif isinstance(items, dict):
        return items
    else:
        return {None: items}


def get_args_kwargs_from_dict(
    data: dict[Key, Any],
) -> tuple[list[Any], dict[str, Any]]:
    args_d = {}
    kwargs = {}
    if None in data:  # primitive: must be the only one
        args = [data[None]]
    else:
        for k, v in data.items():
            if isinstance(k, int):
                args_d[k] = v
            elif isinstance(k, str):
                kwargs[k] = v
            else:
                raise TypeError(k)
        if args_d:
            # fill missing positionals with None
            args = [args_d.get(i, None) for i in range(max(args_d) + 1)]
        else:
            args = []

    return args, kwargs


@dataclass(frozen=True)
class Process:
    function: Callable
    inputs: KeyAny = None
    outputs: KeyAny = None

    def __post_init__(self) -> None:
        # change attributes:
        # wrap / ensure proper classes
        # save changes: must use __setattr__ because we use frozen=True
        function = Function.as_function(self.function)
        inputs = {
            key: Input.as_input(input, function.get_input_type(key))
            for key, input in any_to_dict(self.inputs).items()
        }
        outputs = {
            key: Output.as_output(output, function.get_output_type(key))
            for key, output in any_to_dict(self.outputs).items()
        }

        object.__setattr__(self, "function", function)
        object.__setattr__(self, "inputs", inputs)
        object.__setattr__(self, "outputs", outputs)

    def __call__(self) -> None:
        """Run the process."""

        # read input values
        inputs = any_to_dict(self.inputs)
        data = {key: read_input() for key, read_input in inputs.items()}

        # map to function arguments
        args, kwargs = get_args_kwargs_from_dict(data)

        # call original functions
        result = self.function(*args, **kwargs)

        # create process metadata # TODO
        metadata = {}

        # map to outputs and save
        outputs = any_to_dict(self.outputs)
        for key, write_output in outputs.items():
            partial_result = result if key is None else result[key]
            # write output function must be take data and metadata
            write_output(partial_result, metadata)
