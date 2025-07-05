"""Type conversion"""

import functools
from dataclasses import dataclass
from typing import Any, Callable, ClassVar

__all__ = ["Converter", "Type", "ConverterException"]

Type = Any


class ConverterException(Exception):
    pass


@dataclass(frozen=True)
class Converter:
    _converters: ClassVar[dict[tuple[Type, Type], Callable]] = {}
    function: Callable

    @classmethod
    def get(cls, type_from: Type, type_to: Type) -> Callable:
        return cls._converters[(type_from, type_to)]

    @classmethod
    def register(cls, type_from: Type, type_to: Type):
        def decorator(function) -> Converter:
            converter = Converter(function=function)
            cls._converters[(type_from, type_to)] = converter
            return converter

        return decorator

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)
