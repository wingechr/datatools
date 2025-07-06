import datetime
import importlib.util
import inspect
import logging
import sys
from functools import cache as _cache
from functools import update_wrapper
from pathlib import Path
from typing import Callable, Union, get_args

import jsonpath_ng

from datatools.classes import Any, ParameterKey, Type


def cache(func: Callable) -> Callable:
    """Update cache decorator that preserves metadata.

    Parameters
    ----------
    func : Callable
        original function

    Returns
    -------
    Callable
        cache decorated function
    """

    cached_func = _cache(func)
    return update_wrapper(cached_func, func)


@_cache
def get_type_name(cls: Type) -> str:
    if cls is None:
        return "Any"
    if isinstance(cls, str):
        return cls
    if cls.__module__ == "typing":
        return str(cls)

    # remove leading underscore from module name
    modulename = str(cls.__module__).lstrip("_")
    classname = cls.__qualname__

    return f"{modulename}.{classname}"


@_cache
def get_filetype_from_filename(filename: Union[Path, str]) -> Type:
    """returns something like .txt"""
    suffix = str(filename).split(".")[-1]
    return f".{suffix}"


def get_args_kwargs_from_dict(
    data: dict[ParameterKey, Any],
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


@_cache
def get_value_type(dtype: Type) -> Type:
    # dict[Any, int] -> int
    # list[int] -> int
    return get_args(dtype)[-1]


def get_result_type(function: Callable) -> Type:
    sig = inspect.signature(function)
    return_type = sig.return_annotation
    if return_type == inspect._empty:
        return_type = None
    return return_type


def get_parameters_types(function: Callable) -> dict[str, Any]:
    sig = inspect.signature(function)
    # hints = get_type_hints(function)  # does not work with my decorated classes
    # parameter_types = {
    #    name: hints.get(name, None) for name, param in sig.parameters.items()
    # }
    parameter_types = {name: param.annotation for name, param in sig.parameters.items()}
    return parameter_types


def get_keyword_only_parameters_types(
    function: Callable, min_idx: int = 0
) -> list[str]:
    parameters = inspect.signature(function).parameters
    return [
        name
        for idx, (name, param) in enumerate(parameters.items())
        if idx >= min_idx
        and param.kind
        in {inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
    ]


def is_type_class(x) -> bool:
    """Check if x is a type."""
    # return isinstance(x, type)
    if inspect.isclass(x):
        return True
    # special case: typping classes are not real classes
    if type(x).__module__ == "typing":
        return True
    return False


def json_serialize(x):
    if isinstance(x, datetime.datetime):
        return x.strftime("%Y-%m-%dT%H:%M:%S%z")
    elif isinstance(x, datetime.date):
        return x.strftime("%Y-%m-%d")
    elif isinstance(x, datetime.time):
        return x.strftime("%H:%M:%S")
    elif is_type_class(x):
        return get_type_name(x)
    else:
        raise NotImplementedError(type(x))


def jsonpath_update(data: dict, key: str, val: Any) -> None:
    key_pattern = jsonpath_ng.parse(key)
    # NOTE: for some reason, update_or_create in jsonpath_ng  does not
    # work with types that cannot be serialized to JSON
    try:
        val = json_serialize(val)
    except NotImplementedError:
        pass
    key_pattern.update_or_create(data, val)


def jsonpath_get(data: dict, key: str) -> Any:
    key_pattern = jsonpath_ng.parse(key)
    match = key_pattern.find(data)
    result = [x.value for x in match]
    # TODO: we always get a list (multiple matches),
    # but most of the time, we want only one
    if len(result) == 0:
        result = None
    elif len(result) == 1:
        result = result[0]
    else:
        logging.info("multiple results in metadata found for %s", key)

    return result


def import_module_from_path(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module '{name}' from '{filepath}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def copy_signature(self: object, other: Callable) -> None:
    object.__setattr__(self, "__signature__", inspect.signature(other))
    object.__setattr__(self, "__name__", other.__name__)
    object.__setattr__(self, "__doc__", other.__doc__)


def passthrough(x: Any) -> Any:
    return x
