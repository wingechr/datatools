"""Abstract classes / interfaces, types"""

from collections.abc import Callable
import hashlib
import json
import logging
import pickle
from typing import Generic, ParamSpec, TypeVar

from datatools.types import Json
from datatools.utils import (
    function_get_argument_dict,
    function_has_varargs,
)

P = ParamSpec("P")
R = TypeVar("R")


class FunctionWrapper(Generic[P, R]):
    """TODO"""

    def __init__(
        self,
        fun: Callable[P, R],
        output_to_bytes: Callable[[R], bytes] | None = None,
        output_from_bytes: Callable[[bytes], R] | None = None,
        **params,
    ):
        if function_has_varargs(fun):
            logging.warning("Dont wrap a function with *args / **kwargs")
        self.fun = fun
        # self.fun_defaults = function_get_defaults(fun)
        # self.fun_parameter_names = function_get_regular_params(fun)
        self.output_to_bytes: Callable[[R], bytes] = output_to_bytes or pickle.dumps
        self.output_from_bytes: Callable[[bytes], R] = output_from_bytes or pickle.loads

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:  # noqa
        return self.fun(*args, **kwargs)

    @classmethod
    def wrap(
        cls,
        output_to_bytes: Callable[[R], bytes] | None = None,
        output_from_bytes: Callable[[bytes], R] | None = None,
        **params,
    ):
        """TODO"""

        def decorator(fun):
            return FunctionWrapper(
                fun,
                output_to_bytes=output_to_bytes,
                output_from_bytes=output_from_bytes,
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

    def get_unique_hash_data(self, *args, **kwargs) -> Json:
        """TODO"""
        parameter: Json = function_get_argument_dict(self.fun, *args, **kwargs)  # type:ignore # noqa
        function_id: Json = self.get_function_id()  # type:ignore
        return {
            "function": function_id,
            "parameter": parameter,
        }  # type:ignore

    @classmethod
    def get_unique_hash(cls, hash_data: Json) -> str:
        """TODO"""
        hash_data_s = json.dumps(
            hash_data, ensure_ascii=False, indent=0, sort_keys=True
        )
        hash_data_b = hash_data_s.encode("utf-8")
        hashsum = hashlib.md5(hash_data_b).hexdigest()  # noqa:S324
        return hashsum
