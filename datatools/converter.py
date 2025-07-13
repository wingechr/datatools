"""Type conversion"""

import json
import logging
import pickle
from io import BufferedIOBase, BytesIO, TextIOWrapper
from itertools import product
from typing import Any, Callable, ClassVar, Union
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import requests

from datatools.base import PARAM_SQL_QUERY, OptionalStr, Type
from datatools.utils import (
    copy_signature,
    filepath_from_uri,
    get_function_datatype,
    get_function_parameters_datatypes,
    get_type_name,
    json_serialize,
    passthrough,
)

__all__ = ["Converter"]


def clean_type(dtype: Type) -> OptionalStr:
    if isinstance(dtype, type):
        return get_type_name(dtype)
    return dtype


def get_cleaned_type_list(types: Union[Type, list[Type]]) -> list[OptionalStr]:
    if not isinstance(types, list):
        types = [types]
    return [clean_type(x) for x in types]


class Converter:
    _converters: ClassVar[dict[tuple[Union[str, None], Union[str, None]], Callable]] = (
        {}
    )

    def __init__(self, function: Callable):
        self.function = function
        copy_signature(self, self.function)

    @classmethod
    def get(cls, type_from: Type, type_to: Type) -> Callable:
        type_from = clean_type(type_from)
        type_to = clean_type(type_to)
        if type_from == type_to:
            return passthrough
        return cls._converters[(type_from, type_to)]

    @classmethod
    def register(
        cls,
        type_from: Union[Type, list[Type]],
        type_to: Union[Type, list[Type]],
    ) -> Callable:
        types_from = get_cleaned_type_list(type_from)
        types_to = get_cleaned_type_list(type_to)

        def decorator(function) -> Converter:
            converter = Converter(function=function)
            for tf_tt in product(types_from, types_to):
                if tf_tt in cls._converters:
                    logging.warning("Overwriting exising Converter %s, %s", *tf_tt)
                else:
                    logging.debug("Registering Converter %s, %s", *tf_tt)
                cls._converters[tf_tt] = converter
            return converter

        return decorator

    @classmethod
    def autoregister(
        cls,
        function: Callable,
    ) -> Callable:
        type_from = list(get_function_parameters_datatypes(function).values())[0]
        type_to = get_function_datatype(function)
        return cls.register(type_from=type_from, type_to=type_to)(function)

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
    def convert_to(cls, data: Type, type_to: Type = None, **kwargs) -> Any:
        type_from = get_type_name(type(data))
        convert = Converter.get(type_from, type_to)
        return convert(data, **kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs)

    def __get__(self, instance, owner):
        # Support instance methods
        return self.__class__(self.function.__get__(instance, owner))


# register some default converters

json_types: list[Type] = [get_type_name(x) for x in [list, dict]]
pickle_types: list[Type] = [get_type_name(x) for x in [list, dict, pd.DataFrame]]
sql_protocols: list[Type] = ["sqlite:"]


@Converter.register(json_types, ".json")
def json_dump(data: object, encoding="utf-8") -> BufferedIOBase:
    return BytesIO(
        json.dumps(
            data, indent=2, ensure_ascii=False, sort_keys=False, default=json_serialize
        ).encode(encoding=encoding)
    )


@Converter.register(".json", json_types)
def json_load(buffer: BytesIO, encoding="utf-8") -> object:
    with TextIOWrapper(buffer, encoding=encoding) as text_buffer:
        return json.load(text_buffer)


@Converter.register(pickle_types, ".pickle")
def pickle_dump(data: object) -> BufferedIOBase:
    return BytesIO(pickle.dumps(data))


@Converter.register(".pickle", pickle_types)
def pickle_load(buffer: BufferedIOBase) -> object:
    return pickle.load(buffer)


@Converter.register(["https:", "http:"], None)
def download(url: str, headers: Union[dict, None] = None) -> BufferedIOBase:
    """Download content from a URL and return it as a BufferedIOBase object."""
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error for bad responses
    return BytesIO(response.content)


@Converter.register("file:", None)
def filecopy(url: str) -> BufferedIOBase:
    """Copy file."""
    path = filepath_from_uri(url)
    return path.open("rb")


@Converter.register(pd.DataFrame, ".json")
def dataframe_to_json(df: pd.DataFrame) -> BufferedIOBase:
    # buf = BytesIO()
    data = df.to_dict(orient="records")
    return json_dump(data)


@Converter.register(".json", pd.DataFrame)
def json_to_dataframe(buffer: BufferedIOBase) -> pd.DataFrame:
    return pd.read_json(buffer)


@Converter.register(".csv", pd.DataFrame)
def csv_to_dataframe(buffer: BufferedIOBase) -> pd.DataFrame:
    return pd.read_csv(buffer)


@Converter.register(".xlsx", pd.DataFrame)
def xlsx_to_dataframe(buffer: BufferedIOBase) -> pd.DataFrame:
    return pd.read_excel(buffer)


@Converter.register(sql_protocols, None)
def sql_download(uri: str) -> pd.DataFrame:
    """Copy data from sql database"""
    query = parse_qs(urlsplit(uri).query)
    sql_query = query.get(PARAM_SQL_QUERY)[0]
    df = pd.read_sql(sql_query, uri)
    return df


@Converter.autoregister
def get_handler(url: str) -> Callable:
    scheme = url.split(":")[0]
    return Converter.get(f"{scheme}:", None)
