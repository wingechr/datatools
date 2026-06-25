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

    def job_fun(*args, **kwargs):
        # logging.error("orig_fun_parameter_names: %s", input_parameter_names)

        output_uids, input_params = get_job_parameters(
            function=function,
            output_parameter_names=list(output_writers),
            args=args,
            kwargs=kwargs,
        )

        additional_input_parameter_names = set(input_readers) - set(input_params)
        if additional_input_parameter_names:
            raise Exception(
                "Mismatched names between input handlers and argument names: %s",
                additional_input_parameter_names,
            )

        # logging.error("input_params: %s", input_params)

        updated_input_values = {
            p: read(input_params[p]) for p, read in input_readers.items()
        }

        input_param_values = input_params | updated_input_values

        # logging.error("param_values input_param_values: %s", input_param_values)

        # call function
        result = function(**input_param_values)

        for param, write in output_writers.items():
            name = output_uids[param]
            write(result, name)

    return job_fun


def get_job_parameters(
    function: Callable,
    output_parameter_names: list[str],
    args: tuple[Any],
    kwargs: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """TODO"""

    # checks
    wrapped_function = FunctionWrapper.assert_wrapped(function)

    # parameter the input funcion expects
    input_parameter_names = wrapped_function.fun_parameter_names

    invalid_output_parameter_names = set(output_parameter_names) & set(
        input_parameter_names
    )
    if invalid_output_parameter_names:
        raise Exception("Invalid output parameters: %s", invalid_output_parameter_names)

    input_parameter_names = wrapped_function.fun_parameter_names
    defaults = {
        k: v
        for k, v in wrapped_function.fun_defaults.items()
        if k in input_parameter_names
    }

    # add missing defaults:
    parameter_names = output_parameter_names + input_parameter_names
    kwargs = defaults | kwargs
    param_values = names_get_argument_dict(parameter_names, *args, **kwargs)
    # we want to keep these param_values for meta data
    logging.error("param_values: %s", param_values)
    output_values = {p: param_values[p] for p in output_parameter_names}
    input_values = {p: param_values[p] for p in input_parameter_names}
    return output_values, input_values
