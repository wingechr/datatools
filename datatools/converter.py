"""Type conversion"""

import json
import logging
import pickle
from io import BufferedIOBase, BytesIO, TextIOWrapper
from itertools import product
from typing import Any, Callable, ClassVar, Optional, Union
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import requests

from datatools.base import (
    PARAM_SQL_QUERY,
    MetadataDict,
    OptionalStr,
    ParameterKey,
    Type,
    UriHandlerType,
)
from datatools.utils import (
    copy_signature,
    filepath_from_uri,
    get_function_datatype,
    get_function_name,
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
    _converters: ClassVar[
        dict[tuple[Union[str, None], Union[str, None]], Callable[..., Any]]
    ] = {}

    def __init__(self, function: Callable[..., Any]):
        self.function = function
        copy_signature(self, self.function)

    @classmethod
    def get(cls, type_from: Type, type_to: Type) -> Callable[..., Any]:
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
    ) -> Callable[..., Any]:
        types_from = get_cleaned_type_list(type_from)
        types_to = get_cleaned_type_list(type_to)

        def decorator(function: Callable[..., Any]) -> Converter:
            converter = Converter(function=function)
            for tf_tt in product(types_from, types_to):
                if tf_tt in cls._converters:
                    logging.warning(
                        "Overwriting exising Converter %s, %s (%s, %s)",
                        *tf_tt,
                        get_function_name(cls._converters[tf_tt]),
                        get_function_name(converter),
                    )
                else:
                    logging.debug("Registering Converter %s, %s", *tf_tt)
                cls._converters[tf_tt] = converter
            return converter

        return decorator

    @classmethod
    def register_uri_handler(
        cls,
        scheme_from: Union[Type, list[Type]],
    ) -> Callable[..., Any]:
        return cls.register(scheme_from, UriHandlerType)

    @classmethod
    def autoregister(
        cls,
        function: Callable[..., Any],
    ) -> Callable[..., Any]:
        type_from = list(get_function_parameters_datatypes(function).values())[0]
        type_to = get_function_datatype(function)
        return cls.register(type_from=type_from, type_to=type_to)(function)

    @classmethod
    def convert_return(
        cls, type_to: Type, type_from: Type = None
    ) -> Callable[..., Any]:
        if type_from is None:
            # get converter after function returned result
            def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
                def decorated_function(*args, **kwargs):
                    result = function(*args, **kwargs)
                    type_from = get_type_name(type(result))
                    converter = Converter.get(type_from, type_to)
                    return converter(result)

                return decorated_function

        else:
            # get converter bofore function returned result
            def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
                converter = Converter.get(type_from, type_to)

                def decorated_function(*args: Any, **kwargs: Any):
                    result = function(*args, **kwargs)
                    return converter(result)

                return decorated_function

        return decorator

    @classmethod
    def convert_to(cls, data: Type, type_to: Type = None, **kwargs: Any) -> Any:
        type_from = get_type_name(type(data))
        convert = Converter.get(type_from, type_to)
        return convert(data, **kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs)

    def __get__(self, instance: Any, owner: Any):
        # Support instance methods
        return self.__class__(self.function.__get__(instance, owner))


# register some default converters

json_types: list[Type] = [get_type_name(x) for x in [list, dict]]
pickle_types: list[Type] = [get_type_name(x) for x in [list, dict, pd.DataFrame]]
sql_protocols: list[Type] = ["sqlite:"]


@Converter.register(json_types, ".json")
def json_dump(data: object, encoding: str = "utf-8") -> BufferedIOBase:
    return BytesIO(
        json.dumps(
            data, indent=2, ensure_ascii=False, sort_keys=False, default=json_serialize
        ).encode(encoding=encoding)
    )


@Converter.register(".json", json_types)
def json_load(buffer: BytesIO, encoding: str = "utf-8") -> object:
    with TextIOWrapper(buffer, encoding=encoding) as text_buffer:
        return json.load(text_buffer)


@Converter.register(pickle_types, ".pickle")
def pickle_dump(data: object) -> BufferedIOBase:
    return BytesIO(pickle.dumps(data))


@Converter.register(".pickle", pickle_types)
def pickle_load(buffer: BufferedIOBase) -> object:
    return pickle.load(buffer)


@Converter.register_uri_handler(["https:", "http:"])
def download(url: str, headers: Union[dict[Any, Any], None] = None) -> BufferedIOBase:
    """Download content from a URL and return it as a BufferedIOBase object."""
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error for bad responses
    return BytesIO(response.content)


@Converter.register_uri_handler("file:")
def filecopy(url: str) -> BufferedIOBase:
    """Copy file."""
    path = filepath_from_uri(url)
    return path.open("rb")


@Converter.register(pd.DataFrame, ".json")
def dataframe_to_json(df: pd.DataFrame) -> BufferedIOBase:
    # buf = BytesIO()
    data = df.to_dict(orient="records")  # type: ignore
    return json_dump(data)


@Converter.register(".json", pd.DataFrame)
def json_to_dataframe(buffer: BufferedIOBase, encoding: str = "utf-8") -> pd.DataFrame:
    with TextIOWrapper(buffer, encoding=encoding) as text_buffer:  # type: ignore
        return pd.read_json(text_buffer)  # type:ignore


@Converter.register(".csv", pd.DataFrame)
def csv_to_dataframe(
    buffer: BufferedIOBase,
    encoding: str = "utf-8",
    index_col: Optional[list[ParameterKey]] = None,
) -> pd.DataFrame:
    with TextIOWrapper(buffer, encoding=encoding) as text_buffer:  # type: ignore
        df = pd.read_csv(text_buffer, index_col=index_col)  # type:ignore
    return df


@Converter.register(".xlsx", pd.DataFrame)
def xlsx_to_dataframe(buffer: BufferedIOBase) -> pd.DataFrame:
    with buffer:
        return pd.read_excel(buffer)  # type:ignore


@Converter.register_uri_handler(sql_protocols)
def sql_download(uri: str) -> pd.DataFrame:
    """Copy data from sql database"""
    query = parse_qs(urlsplit(uri).query)
    sql_query = query.get(PARAM_SQL_QUERY)
    if not sql_query:
        raise ValueError("Missing sql query")
    sql_query = sql_query[0]
    df = pd.read_sql(sql_query, uri)  # type:ignore
    return df


@Converter.autoregister
def get_handler(url: str) -> Callable[..., Any]:
    scheme = url.split(":")[0]
    return Converter.get(f"{scheme}:", None)


@Converter.register(pd.DataFrame, MetadataDict)
def inspect_df(df: pd.DataFrame) -> MetadataDict:
    index_col: list[str] = [c if c is not None else 0 for c in df.index.names]

    return {
        "columns": df.columns.tolist(),
        # "dtypes": df.dtypes.to_dict(),
        "shape": df.shape,
        # index_col: important for loader
        "index_col": index_col,
    }


@Converter.register(pd.DataFrame, ".csv")
def df_to_csv(df: pd.DataFrame, encoding: str = "utf-8", index: bool = True):
    buf = BytesIO()
    # TODO: if index = True but has no names: generate names,
    # otherwise they will be renamed when loading (e.g. Unnamed: 0)
    df.to_csv(buf, encoding=encoding, index=index)
    buf.seek(0)
    return buf
