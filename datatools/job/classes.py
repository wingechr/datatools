"""Abstract classes / interfaces, types"""

from collections.abc import Callable
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Generic

from datatools.types import FunParams, FunResult
from datatools.utils import (
    function_get_defaults,
    function_get_regular_params,
    function_has_varargs,
    names_get_argument_dict,
    pickle_dump_to_path,
    pickle_load_from_path,
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


class Job:
    """TODO"""

    def __init__(
        self,
        function: Callable,
        output_writers: dict[str, Callable[[Any, str], Any]],
        input_readers: dict[str, Callable[[Any], Any]] | None = None,
    ):
        self.function = FunctionWrapper.assert_wrapped(function)
        self.output_writers = output_writers
        self.input_readers = input_readers or {}

        self.output_parameter_names = list(self.output_writers)
        self.input_parameter_names = self.function.fun_parameter_names
        self.parameter_names = self.output_parameter_names + self.input_parameter_names
        self.defaults = {
            k: v
            for k, v in self.function.fun_defaults.items()
            if k in self.input_parameter_names
        }

        # checks

        additional_input_parameter_names = set(self.input_readers) - set(
            self.input_parameter_names
        )
        if additional_input_parameter_names:
            raise Exception(
                "Mismatched names between input handlers and argument names: %s",
                additional_input_parameter_names,
            )

        # parameter the input funcion expects
        invalid_output_parameter_names = set(self.output_parameter_names) & set(
            self.input_parameter_names
        )
        if invalid_output_parameter_names:
            raise Exception(
                "Invalid output parameters: %s", invalid_output_parameter_names
            )

    def __call__(self, *args, **kwargs):
        """TODO"""
        # logging.error("orig_fun_parameter_names: %s", input_parameter_names)

        output_uids, input_params = self.get_job_parameters(*args, **kwargs)

        # logging.error("input_params: %s", input_params)

        updated_input_values = {
            p: read(input_params[p]) for p, read in self.input_readers.items()
        }

        input_param_values = input_params | updated_input_values

        # logging.error("param_values input_param_values: %s", input_param_values)

        # call function
        result = self.function(**input_param_values)

        for param, write in self.output_writers.items():
            name = output_uids[param]
            write(result, name)

    def get_job_parameters(
        self,
        *args,
        **kwargs,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """TODO"""

        # add missing defaults:
        kwargs = self.defaults | kwargs

        param_values = names_get_argument_dict(self.parameter_names, *args, **kwargs)

        # we want to keep these param_values for meta data
        # logging.error("param_values: %s", param_values)
        output_values = {p: param_values[p] for p in self.output_parameter_names}
        input_values = {p: param_values[p] for p in self.input_parameter_names}
        return output_values, input_values


class Cache(Generic[FunParams, FunResult]):
    """TODO"""

    __output_name__ = "__output"

    def __init__(
        self,
        function: Callable[FunParams, FunResult],
        get_id: Callable,
        dump: Callable = pickle_dump_to_path,
        load: Callable = pickle_load_from_path,
        exists: Callable = lambda p: p.exists(),
    ):
        self.get_id = get_id
        self.dump = dump
        self.load = load
        self.exists = exists
        self.function = FunctionWrapper.assert_wrapped(function)

        self.job = Job(
            function=self.function, output_writers={self.__output_name__: self.dump}
        )

    def __call__(self, *args: FunParams.args, **kwargs: FunParams.kwargs) -> FunResult:  # noqa
        uid = self.get_id(self.job, *args, **kwargs)
        if not self.exists(uid):
            self.job(uid)
        return self.load(uid)

    @classmethod
    def cache(
        cls,
        get_id: Callable,
        dump: Callable = pickle_dump_to_path,
        load: Callable = pickle_load_from_path,
        exists: Callable = lambda p: p.exists(),
    ):
        """TODO"""

        def decorator(function):
            return Cache(function, get_id=get_id, dump=dump, load=load, exists=exists)

        return decorator


def make_file_cache_get_path(location: str | Path = "__cache__", suffix=".pickle"):
    """TODO"""
    location = Path(location)

    def get_id(job: Job, *args, **kwargs):
        _ouput_uids, input_params = job.get_job_parameters(*args, **kwargs)
        function_id = job.function.get_function_id()
        hash_data = {"function": function_id, "parameters": input_params}

        hash_data_s = json.dumps(
            hash_data, ensure_ascii=False, indent=0, sort_keys=True
        )
        hash_data_b = hash_data_s.encode("utf-8")
        hashsum = hashlib.md5(hash_data_b).hexdigest()  # noqa:S324
        path = location / hashsum[:2] / hashsum[2:4] / f"{hashsum}{suffix}"
        return path

    return get_id
