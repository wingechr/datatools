"""Abstract classes / interfaces, types"""

from abc import ABC, abstractmethod
from collections.abc import Callable
import inspect
from inspect import Parameter
from typing import Any


def function_get_defaults(func: Callable):
    """TODO"""
    sig = inspect.signature(func)
    return {
        name: param.default
        for name, param in sig.parameters.items()
        if param.default is not inspect._empty
    }


def function_has_varargs(func: Callable) -> bool:
    """TODO"""
    sig = inspect.signature(func)
    has_args = any(p.kind == Parameter.VAR_POSITIONAL for p in sig.parameters.values())
    has_kwargs = any(p.kind == Parameter.VAR_KEYWORD for p in sig.parameters.values())
    return has_args or has_kwargs


def function_get_regular_params(func: Callable) -> list[str]:
    """TODO"""
    if function_has_varargs(func):
        raise TypeError("Function cannot have *args or **kwargs")
    sig = inspect.signature(func)
    return list(sig.parameters)


class Job(ABC):
    """TODO."""

    @abstractmethod
    def _todo(self): ...

    def __init__(self):
        self.input_names: list[str]
        self.input_parsers: dict[str, Callable[[Any], Any] | None]
        self.input_identifiers: dict[str, Callable[[Any], str] | None]
        self.input_values: dict[str, Any]
        self.function: Callable
        self.function_identifier: Callable[[Callable], str]
        self.output_handler: Callable[[Any], None]

    def __call__(self) -> None:
        """TODO"""
        # load all inputs
        input_data = self._parse_inputs(self.input_values, self.input_parsers)
        output_data = self.function(input_data)
        self.output_handler(output_data)

    def _parse_inputs(
        self, values: dict[str, Any], parsers: dict[str, Callable | None]
    ) -> dict[str, Any]:
        data = {}
        for name in self.input_names:
            value = values.get(name)
            parser = parsers.get(name)
            if parser:
                value = parser(value)
                data[name] = value
        return data

    # def _create_hash(self):
    #    input_ids = self._parse_inputs(self.input_values, self.input_identifiers)
    #    input_parser_ids = self._parse_inputs(self.input_values, self.input_parsers)
    #    function_id = self.function_identifier(self.function)
