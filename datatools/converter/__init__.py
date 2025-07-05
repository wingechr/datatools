"""Type conversion"""

from dataclasses import dataclass
from typing import Any, Callable, ClassVar

from datatools.classes import Type, UnknownType

__all__ = ["Converter"]


@dataclass(frozen=True)
class Converter:
    _converters: ClassVar[dict[tuple[Type, Type], Callable]] = {}
    function: Callable

    @classmethod
    def get(cls, type_from: Type, type_to: Type) -> Callable:
        if type_from == type_to:
            return lambda x: x
        return cls._converters[(type_from, type_to)]

    @classmethod
    def register(cls, type_from: Type, type_to: Type) -> Callable:
        def decorator(function) -> Converter:
            converter = Converter(function=function)
            cls._converters[(type_from, type_to)] = converter
            return converter

        return decorator

    @classmethod
    def convert_return(cls, type_to: Type, type_from: Type = UnknownType) -> Callable:
        if type_from == UnknownType:
            # get converter after function returned result
            def decorator(function) -> Callable:
                def decorated_function(*args, **kwargs):
                    result = function(*args, **kwargs)
                    type_from = type(result)
                    converter = Converter.get(type_from, type_to)
                    return converter(result)

                return decorated_function

        else:
            # get converter bofore function returned result
            def decorator(function) -> Callable:
                converter = Converter.get(type_from, type_to)

                def decorated_function(*args, **kwargs):
                    result = function(*args, **kwargs)
                    return converter(result)

                return decorated_function

        return decorator

    @classmethod
    def convert_to(cls, data: Type, type_to: Type) -> Any:
        type_from = type(data)
        convert = cls.get(type_from, type_to)
        return convert(data)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs)
