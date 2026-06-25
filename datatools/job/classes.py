"""Abstract classes / interfaces, types"""

from collections.abc import Callable
from dataclasses import dataclass
import logging
from typing import Any, Generic

from datatools.types import FunParams, FunResult
from datatools.utils import (
    function_get_defaults,
    function_get_regular_params,
    function_has_varargs,
    names_get_argument_dict,
)


class FunctionWrapper(Generic[FunParams, FunResult]):
    """TODO"""

    def __init__(
        self,
        fun: Callable[FunParams, FunResult],
        **params,
    ):
        if function_has_varargs(fun):
            logging.warning("Dont wrap a function with *args / **kwargs")
        self.fun = fun
        self.fun_defaults = function_get_defaults(fun)
        self.fun_parameter_names = function_get_regular_params(fun)

    def __call__(self, *args: FunParams.args, **kwargs: FunParams.kwargs) -> FunResult:  # noqa
        return self.fun(*args, **kwargs)

    @classmethod
    def wrap(
        cls,
        **params,
    ):
        """TODO"""

        def decorator(fun):
            return FunctionWrapper(
                fun,
                **params,
            )

        return decorator

    @classmethod
    def assert_wrapped(cls, function: Callable) -> "FunctionWrapper":
        """TODO"""
        if isinstance(function, FunctionWrapper):
            return function
        return FunctionWrapper(function)

    def get_function_id(self) -> str:
        """TODO"""
        return self.fun.__name__

    def get_argument_dict(self, *args, **kwargs) -> dict[str, Any]:
        """TODO"""
        return names_get_argument_dict(self.fun_parameter_names, *args, **kwargs)


@dataclass
class InputHandler:
    """TODO"""

    handle: Callable[[Any], Any]
    name: str = "input"


@dataclass
class OutputHandler:
    """TODO"""

    handle: Callable[[Any, Any], None]
    name: str = "output"


@dataclass
class OutputConvertHandler(OutputHandler):
    """TODO"""

    handle: Callable[[Any], Any]


def make_job(
    function: Callable,
    input_readers: dict[str, Callable[[Any], Any]],
    output_writers: dict[str, Callable[[Any, str], Any]],
):
    """TODO"""

    wrapped_function = FunctionWrapper.assert_wrapped(function)

    # parameter the input funcion expects
    input_parameter_names = wrapped_function.fun_parameter_names
    output_parameter_names = list(output_writers)
    invalid_output_parameter_names = set(output_writers) & set(input_parameter_names)
    if invalid_output_parameter_names:
        raise Exception("Invalid output parameters: %s", invalid_output_parameter_names)

    parameter_names = output_parameter_names + input_parameter_names

    additional_input_parameter_names = set(input_readers) - set(input_parameter_names)
    if additional_input_parameter_names:
        raise Exception(
            "Mismatched names between input handlers and argument names: %s",
            additional_input_parameter_names,
        )

    # find unmapped parameters that have defaults
    unhandled_input_parameter_names = set(input_parameter_names) - set(input_readers)
    defaults = {
        k: v
        for k, v in wrapped_function.fun_defaults.items()
        if k in unhandled_input_parameter_names
    }
    logging.error("unmapped_defaults: %s", defaults)

    def job_fun(*args, **kwargs):
        logging.error("orig_fun_parameter_names: %s", input_parameter_names)

        # add missing defaults:
        kwargs = defaults | kwargs
        param_values = names_get_argument_dict(parameter_names, *args, **kwargs)
        # we want to keep these param_values for meta data
        logging.error("param_values: %s", param_values)

        input_param_values = {}
        for param in input_parameter_names:
            value = param_values[param]
            if param in input_readers:
                value = input_readers[param](value)
            input_param_values[param] = value

        logging.error("param_values input_param_values: %s", input_param_values)

        # call function
        result = function(**input_param_values)

        for param, write in output_writers.items():
            name = param_values[param]
            write(result, name)

    return job_fun
