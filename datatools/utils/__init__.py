import inspect
from functools import cache as _cache
from functools import update_wrapper
from pathlib import Path
from typing import Callable, Union, get_args, get_type_hints

from datatools.classes import Any, Key, Type


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
    data: dict[Key, Any],
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
    # if return_type == inspect._empty:
    #    pass
    return return_type


def get_parameters_types(function: Callable) -> dict[str, Any]:
    sig = inspect.signature(function)
    hints = get_type_hints(function)
    parameter_types = {name: hints.get(name, None) for name in sig.parameters}
    return parameter_types
