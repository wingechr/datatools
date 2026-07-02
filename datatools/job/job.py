"""Abstract classes / interfaces, types"""

from collections.abc import Callable
import logging
from typing import Any, Generic

from datatools.types import FunHashsum, FunParams, FunResult, Json
from datatools.utils import (
    function_get_defaults,
    function_get_regular_params,
    get_function_description,
    get_md5_hash,
    names_get_argument_dict,
)


class FunctionWrapper(Generic[FunParams, FunResult]):
    """TODO"""

    def __init__(
        self,
        fun: Callable[FunParams, FunResult],
        function_id: str | None = None,
        description: str | None = None,
        **params,
    ):
        # TODO: is this still a problem?
        # if function_has_varargs(fun):
        #    logging.warning("Dont wrap a function with *args / **kwargs")

        self.fun = fun
        self.fun_defaults = function_get_defaults(fun)
        self.fun_parameter_names = function_get_regular_params(fun)
        self.function_id: str = function_id or fun.__name__
        self.description = (
            get_function_description(fun) if description is None else description
        )

    def __call__(self, *args: FunParams.args, **kwargs: FunParams.kwargs) -> FunResult:  # noqa
        return self.fun(*args, **kwargs)

    @classmethod
    def wrap(
        cls,
        function_id: str | None = None,
        description: str | None = None,
        **params,
    ):
        """TODO"""

        def decorator(fun):
            return FunctionWrapper(
                fun,
                function_id=function_id,
                description=description,
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
        return self.function_id


def get_job_input_parameters(job: "Job", *args, **kwargs) -> dict:
    """TODO"""
    # logging.error((args, kwargs))
    _output_params, input_params = job.get_job_parameters(
        *job.output_parameter_names, *args, **kwargs
    )
    return input_params


def default_get_hash_data(job: "Job", input_params: dict) -> Json:
    """TODO"""
    function_id = job.function.get_function_id()
    return {"function": function_id, "parameters": input_params}


def default_get_job_hashsum(job: "Job", *args, **kwargs) -> str:
    """TODO"""
    input_params = get_job_input_parameters(job, *args, **kwargs)
    hash_data = default_get_hash_data(job, input_params)
    hashsum = get_md5_hash(hash_data)
    return hashsum


class Job:
    """TODO"""

    def __init__(
        self,
        function: Callable,
        output_writers: dict[str, Callable[[Any, str], Any]],
        input_readers: dict[str, Callable[[Any], Any]] | None = None,
        check_done: Callable[..., bool] | None = None,
        get_job_hashsum: FunHashsum = default_get_job_hashsum,
    ):
        self.function = FunctionWrapper.assert_wrapped(function)
        self.output_writers = output_writers
        self.input_readers = input_readers or {}
        self.check_done = check_done

        self.output_parameter_names = list(self.output_writers)
        self.input_parameter_names = self.function.fun_parameter_names
        self.parameter_names = self.output_parameter_names + self.input_parameter_names
        self.defaults = {
            k: v
            for k, v in self.function.fun_defaults.items()
            if k in self.input_parameter_names
        }
        self._get_job_hashsum = get_job_hashsum

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

    def get_job_hashsum(self, *args, **kwargs) -> str:
        """TODO

        FIXME: create unit tests - why dont i have to pass output args like
        the same way as in __call__?
        """
        return self._get_job_hashsum(self, *args, **kwargs)  # self is first arg (job)

    def __call__(self, *args, **kwargs):
        """TODO"""
        # logging.error("orig_fun_parameter_names: %s", input_parameter_names)

        output_uids, input_params = self.get_job_parameters(*args, **kwargs)

        if self.check_done and self.check_done(**output_uids):
            logging.info("Already done, %s", self)
            return

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

        param_values = names_get_argument_dict(
            self.parameter_names, self.defaults, *args, **kwargs
        )

        # we want to keep these param_values for meta data
        # logging.error("param_values: %s", param_values)
        output_values = {p: param_values[p] for p in self.output_parameter_names}
        input_values = {p: param_values[p] for p in self.input_parameter_names}
        return output_values, input_values

    def __str__(self) -> str:
        inp = ", ".join(self.input_parameter_names)
        outp = ", ".join(self.output_parameter_names)
        return f"Job({inp}) -> ({outp})"
