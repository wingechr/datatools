"""Type conversion"""

import functools
from typing import Any, Callable

__all__ = ["register_converter", "get_converter", "Type", "ConverterException"]

Type = Any


converters: dict[tuple[Type, Type], Callable] = {}


class ConverterException(Exception):
    pass


def register_converter(type_from: Type, type_to: Type):
    def decorator(fun):
        @functools.wraps(fun)
        def _fun(*args, **kwargs):
            return fun(*args, **kwargs)

        # register
        # TODO: warn when overwriting?
        converters[(type_from, type_to)] = _fun

        return _fun

    return decorator


def get_converter(type_from: Type, type_to: Type) -> Callable:
    return converters[(type_from, type_to)]
