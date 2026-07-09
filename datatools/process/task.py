"""Abstract classes / interfaces, types

Concepts

Functions get various input parameters (of different types) and return an output.

The callable AnnotatedFunction does not change the signature or behaviour of
the Function in any way but provides additional metadata
(especially a unique id and description).

A Task has a changed signature from its base wrapped Function.
One or more output arguments are attached to the front, and the types of some of the
imputs have changed.
These will be usually file names where output is stored or input is read from.
The Task has been created with suitable reading/writing functions to convert from
and to bytes for these.
Calling the Task does not return result (except maybe a success status).

A Job is a Task where at least the input parameters are provided.
But the Task has not been executed yet. This can be used to create a unique
hash for caching from function id and input values.

An Activity is the process of running the Job.
It also creates some context metadata (like timestamp) when it wus run.
It must have a unique id as well, but maybe we just use a random UID, or alternatively
the Task's id plus some hashed context data.


"""

from collections.abc import Callable
import logging
from typing import Any, Generic

from datatools.types import FunHashsum, FunParams, FunResult, Json, URIRefs as u
from datatools.utils import (
    function_get_defaults,
    function_get_regular_params,
    get_deterministic_uuid5_from_data,
    get_function_description,
    get_function_id,
    names_get_argument_dict,
)


class AnnotatedFunction(Generic[FunParams, FunResult]):
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
        self.function_id: str = function_id or get_function_id(fun)
        self.description = (
            get_function_description(fun) if description is None else description
        )

    def __call__(self, *args: FunParams.args, **kwargs: FunParams.kwargs) -> FunResult:  # noqa
        return self.fun(*args, **kwargs)

    def get_metadata(self) -> dict[str, Json]:
        """TODO"""
        return {
            # "@type": u.Function.label,
            "@id": f"function:{self.function_id}",
            u.description.label: self.description,
        }

    @classmethod
    def wrap(
        cls,
        function_id: str | None = None,
        description: str | None = None,
        **params,
    ):
        """TODO"""

        def decorator(fun):
            return AnnotatedFunction(
                fun,
                function_id=function_id,
                description=description,
                **params,
            )

        return decorator

    @classmethod
    def assert_wrapped(cls, function: Callable) -> "AnnotatedFunction":
        """TODO"""
        if isinstance(function, AnnotatedFunction):
            return function
        return AnnotatedFunction(function)


def get_task_input_parameters(task: "Task", *args, **kwargs) -> dict:
    """TODO"""
    # logging.error((args, kwargs))
    _output_params, input_params = task.get_input_output_parameters(
        *task.output_parameter_names, *args, **kwargs
    )
    return input_params


def default_get_hash_data(task: "Task", input_params: dict) -> Json:
    """TODO"""
    function_id = task.function.function_id
    return {"function": function_id, "parameters": input_params}


def default_get_task_uuid(task: "Task", *args, **kwargs) -> str:
    """TODO"""
    input_params = get_task_input_parameters(task, *args, **kwargs)
    hash_data = default_get_hash_data(task, input_params)
    hashsum = get_deterministic_uuid5_from_data(hash_data)
    return hashsum


class Job:
    """TODO"""

    def __init__(
        self,
        output_names: dict,
        output_writers: dict,
        input_readers: dict,
        input_params: dict,
        function: Callable,
        check_done: Callable | None,
    ):
        self.output_names = output_names
        self.output_writers = output_writers
        self.input_readers = input_readers
        self.input_params = input_params
        self.function = function
        self.check_done = check_done

    def __call__(self):
        """TODO"""
        if self.check_done and self.check_done(**self.output_names):
            logging.info("Already done, %s", self)
            return

        updated_input_values = {
            p: read(self.input_params[p]) for p, read in self.input_readers.items()
        }

        input_param_values = self.input_params | updated_input_values

        # call function
        result = self.function(**input_param_values)

        for param, write in self.output_writers.items():
            name = self.output_names[param]
            write(result, name)


class Task:
    """TODO"""

    def __init__(
        self,
        function: Callable,
        output_writers: dict[str, Callable[[Any, str], Any]],
        input_readers: dict[str, Callable[[Any], Any]] | None = None,
        check_done: Callable[..., bool] | None = None,
        get_task_uuid: FunHashsum = default_get_task_uuid,
    ):
        self.function = AnnotatedFunction.assert_wrapped(function)
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
        self._get_task_uuid = get_task_uuid

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

    def get_task_uuid(self, *args, **kwargs) -> str:
        """TODO

        FIXME: create unit tests - why dont i have to pass output args like
        the same way as in __call__?
        """
        return self._get_task_uuid(self, *args, **kwargs)  # self is first arg (task)

    def create_job(self, *args, **kwargs) -> Job:
        """TODO"""
        output_names, input_params = self.get_input_output_parameters(*args, **kwargs)
        job = Job(
            output_names=output_names,
            output_writers=self.output_writers,
            input_readers=self.input_readers,
            input_params=input_params,
            function=self.function,
            check_done=self.check_done,
        )
        return job

    def __call__(self, *args, **kwargs):
        """TODO"""
        job = self.create_job(*args, **kwargs)
        job()

    def get_input_output_parameters(
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
