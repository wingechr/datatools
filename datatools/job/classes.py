"""Abstract classes / interfaces, types"""

from collections.abc import Callable
from dataclasses import dataclass
import logging
from typing import Any, Generic

from datatools.types import FunParams, FunResult
from datatools.utils import (
    assert_unique,
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
    name_mapped: str | None = None  # name in inner function

    @property
    def _name_mapped(self) -> str:
        return self.name_mapped or self.name


@dataclass
class OutputHandler:
    """TODO"""

    handle: Callable[[Any, Any], None]
    name: str = "output"
    split: Callable[[Any], Any] | None = None

    @property
    def _split(self) -> Callable[[Any], Any]:
        def use_all(x):
            return x

        return self.split or use_all


@dataclass
class OutputConvertHandler(OutputHandler):
    """TODO"""

    handle: Callable[[Any], Any]


def make_job(
    function: Callable,
    input_handlers: list[InputHandler],
    output_handlers: list[OutputHandler],
):
    """TODO"""

    wrapped_function = FunctionWrapper.assert_wrapped(function)

    # parameter the input funcion expects
    orig_fun_parameter_names = wrapped_function.fun_parameter_names
    new_fun_parameter_names_output = []
    for oh in output_handlers:
        new_fun_parameter_names_output.append(oh.name)

    assert_unique(h._name_mapped for h in input_handlers)
    input_handlers_by_name_mapped = {h._name_mapped: h for h in input_handlers}

    assert_unique(h.name for h in input_handlers)
    input_handlers_by_name = {h.name: h for h in input_handlers}
    unused_input_handlers_names = set(input_handlers_by_name_mapped) - set(
        orig_fun_parameter_names
    )
    if unused_input_handlers_names:
        raise Exception(
            "Mismatched names between input handlers and argument names: %s",
            unused_input_handlers_names,
        )

    new_fun_parameter_names_input = []
    for name in orig_fun_parameter_names:
        name_ = (
            input_handlers_by_name_mapped[name].name
            if name in input_handlers_by_name_mapped
            else name
        )
        new_fun_parameter_names_input.append(name_)

    # output first (but does not have to be)
    new_fun_parameter_names = (
        new_fun_parameter_names_output + new_fun_parameter_names_input
    )
    assert_unique(new_fun_parameter_names)

    def job_fun(*args, **kwargs) -> None:
        logging.error("orig_fun_parameter_names: %s", orig_fun_parameter_names)
        logging.error("new_fun_parameter_names: %s", new_fun_parameter_names)

        param_values = names_get_argument_dict(new_fun_parameter_names, *args, **kwargs)
        logging.error("param_values: %s", param_values)

        param_values_input_mapped = {}
        for name in new_fun_parameter_names_input:
            value = param_values[name]
            handler = input_handlers_by_name.get(name)
            if handler:
                name_mapped = handler._name_mapped
                value = handler.handle(value)
            else:
                name_mapped = name
            param_values_input_mapped[name_mapped] = value

        logging.error("param_values_input_mapped: %s", param_values_input_mapped)

        # call function
        result = function(**param_values_input_mapped)
        for oh in output_handlers:
            value = oh._split(result)
            name_ = param_values[oh.name]
            logging.error("output %s %s %s", oh.name, name_, value)
            oh.handle(value, name_)

    return job_fun
