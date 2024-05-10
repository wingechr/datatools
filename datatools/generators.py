import abc
import logging
import os
import re
from typing import Any, Callable, Dict, Tuple, Type
from urllib.parse import parse_qs, unquote, urlencode, urlsplit, urlunsplit

import sqlalchemy as sa

from .classes import RegistryAbstractBase
from .constants import PARAM_SQL_QUERY
from .utils import (
    BytesIteratorBuffer,
    get_default_media_data_type,
    get_sql_table_schema,
    normalize_sql_query,
    remove_auth_from_uri_or_path,
    sa_create_engine,
    uri_to_data_path,
    uri_to_filepath_abs,
)


class AbstractDataGenerator(RegistryAbstractBase):
    _subclasses = {}  # overwrite from BaseClass
    create_kwargs = []

    @classmethod
    def _is_class_for(cls, data_source: Any) -> bool:
        return False

    @classmethod
    def get_instance(cls, data_source: Any) -> "AbstractDataGenerator":
        subclass = cls._get_class(data_source=data_source)
        return subclass(data_source=data_source)

    def __init__(self, data_source: Any) -> None:
        self._data_source = data_source

    @abc.abstractmethod
    def create_name(self) -> str:
        ...

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        return get_default_media_data_type(name=name)

    @abc.abstractmethod
    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        ...


class FunctionDataGenerator(AbstractDataGenerator):
    @classmethod
    def _is_class_for(cls, data_source: Any) -> bool:
        return isinstance(data_source, Callable)

    def create_name(self) -> str:
        # return fnuction name
        function = self._data_source
        return function.__name__

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        media_type, _data_type = get_default_media_data_type(name=name)
        return media_type, object

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        data = self._data_source()
        metadata = {}
        return data, metadata


class HttpDataGenerator(AbstractDataGenerator):
    @classmethod
    def _is_class_for(cls, data_source: Any) -> bool:
        return isinstance(data_source, str) and re.match(r"^https?://", data_source)

    def create_name(self) -> str:
        return uri_to_data_path(self._data_source)

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        # this can be all kinds of things.
        # SHOULD be determined from suffix, but that will not always work
        return get_default_media_data_type(name=name)

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        import requests

        res = requests.get(self._data_source, stream=True)
        res.raise_for_status()
        print(res.headers)
        # max_bytes = int(res.headers["Content-Length"])
        chunk_size = 1024
        bytes_iter = res.iter_content(chunk_size=chunk_size)
        data = BytesIteratorBuffer(bytes_iter=bytes_iter)

        metadata = dict({"media_type": res.headers["Content-Type"]})
        return data, metadata


class FileDataGenerator(AbstractDataGenerator):
    @classmethod
    def _is_class_for(cls, data_source: Any) -> bool:
        return isinstance(data_source, str) and re.match(r"^file://", data_source)

    def create_name(self) -> str:
        # only use filename
        return os.path.basename(self._data_source)

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        # this can be all kinds of things.
        # SHOULD be determined from suffix, but that will not always work
        return get_default_media_data_type(name=name)

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        file_path = uri_to_filepath_abs(self._data_source)
        data = open(file_path, "rb")
        metadata = {}
        return data, metadata


class SqlpDataGenerator(AbstractDataGenerator):
    @classmethod
    def _is_class_for(cls, data_source: Any) -> bool:
        return isinstance(data_source, str) and re.match(
            r"^[^/]*sql[^/]*://", data_source
        )

    def create_name(self) -> str:
        return uri_to_data_path(self._data_source)

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        # get default media type from name (suffix)
        media_type, _data_type = get_default_media_data_type(name)
        # per default: save as csv?
        return (media_type, list)

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        uri = self._data_source
        metadata = {}
        metadata["source.path"] = remove_auth_from_uri_or_path(uri)
        url = urlsplit(uri)

        # pop sql query
        query_dict = parse_qs(url.query)
        sql_query = query_dict.pop(PARAM_SQL_QUERY)[0]
        sql_query = unquote(sql_query)
        sql_query = normalize_sql_query(sql_query)

        metadata["source.query"] = sql_query

        # usually, netloc is empty, and so urlunsplit()
        # drops the "//"" at the beginning
        path = url.path if url.netloc else "//" + url.path
        # doseq: if False: encode arrays differently
        query_str = urlencode(query_dict, doseq=True)

        connection_string = urlunsplit([url.scheme, url.netloc, path, query_str, None])
        logging.debug(f"Connect: {connection_string}")
        eng = sa_create_engine(connection_string)
        with eng.connect() as con:
            with con:
                logging.debug(f"Exceute: {sql_query}")
                res = con.execute(sa.text(sql_query))
                data_schema = get_sql_table_schema(res.cursor)
                logging.debug(f"Schema: {data_schema}")
                data = [rec._asdict() for rec in res.fetchall()]
                logging.debug(f"Rows: {len(data)}")
        # make sure everything is closed
        eng.dispose()

        metadata["schema"] = data_schema

        return data, metadata
