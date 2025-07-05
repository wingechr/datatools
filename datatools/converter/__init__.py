"""Type conversion"""

import json
from dataclasses import dataclass
from io import BytesIO, IOBase
from itertools import product
from typing import Any, Callable, ClassVar, Union

from datatools.classes import Type
from datatools.utils import get_type_name, json_serialize

__all__ = ["Converter"]


def clean_type(dtype: Type) -> Type:
    if isinstance(dtype, type):
        return get_type_name(dtype)
    return dtype


def get_cleaned_type_list(types: Union[Type, list[Type]]) -> list[Type]:
    if not isinstance(types, list):
        types = [types]
    types = [clean_type(x) for x in types]
    return types


@dataclass(frozen=True)
class Converter:
    _converters: ClassVar[dict[tuple[Type, Type], Callable]] = {}
    function: Callable

    @classmethod
    def get(cls, type_from: Type, type_to: Type) -> Callable:
        type_from = clean_type(type_from)
        type_to = clean_type(type_to)
        if type_from == type_to:
            return lambda x: x
        return cls._converters[(type_from, type_to)]

    @classmethod
    def register(
        cls,
        type_from: Union[Type, list[Type]],
        type_to: Union[Type, list[Type]],
    ) -> Callable:
        type_from = get_cleaned_type_list(type_from)
        type_to = get_cleaned_type_list(type_to)

        def decorator(function) -> Converter:
            converter = Converter(function=function)
            for tf, tt in product(type_from, type_to):
                cls._converters[(tf, tt)] = converter
            return converter

        return decorator

    @classmethod
    def convert_return(cls, type_to: Type, type_from: Type = None) -> Callable:
        if type_from is None:
            # get converter after function returned result
            def decorator(function) -> Callable:
                def decorated_function(*args, **kwargs):
                    result = function(*args, **kwargs)
                    type_from = get_type_name(type(result))
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
    def convert_to(cls, data: Type, type_to: Type = None) -> Any:
        type_from = get_type_name(type(data))
        convert = Converter.get(type_from, type_to)
        return convert(data)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs)


# register some default converters

json_types = [get_type_name(x) for x in [list, dict]]


@Converter.register(json_types, ".json")
def json_dump(data: object) -> BytesIO:
    return BytesIO(
        json.dumps(
            data, indent=2, ensure_ascii=False, sort_keys=False, default=json_serialize
        ).encode()
    )


@Converter.register(".json", json_types)
def json_load(buffer: IOBase) -> object:
    return json.load(buffer)
