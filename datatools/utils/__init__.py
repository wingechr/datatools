import datetime
import inspect
import logging
from functools import cache as _cache
from functools import update_wrapper
from pathlib import Path
from typing import Callable, Union, get_args, get_type_hints

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


def get_type_name(cls: type) -> str:
    if cls is None:
        return "Any"
    return f"{cls.__module__}.{cls.__qualname__}"


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
    hints = get_type_hints(function)
    parameter_types = {name: hints.get(name, None) for name in sig.parameters}
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


def json_serialize(x):
    if isinstance(x, datetime.datetime):
        return x.strftime("%Y-%m-%dT%H:%M:%S%z")
    elif isinstance(x, datetime.date):
        return x.strftime("%Y-%m-%d")
    elif isinstance(x, datetime.time):
        return x.strftime("%H:%M:%S")
    elif isinstance(x, type):
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
