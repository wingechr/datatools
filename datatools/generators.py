import abc
import hashlib
import json
import logging
import os
import re
from typing import Any, Callable, Dict, Tuple, Type
from urllib.parse import parse_qs, unquote, urlencode, urlsplit, urlunsplit

import sqlalchemy as sa

from .classes import RegistryAbstractBase
from .constants import PARAM_SQL_QUERY, STORAGE_SCHEME
from .utils import (
    BytesIteratorBuffer,
    get_default_media_data_type_by_name,
    get_default_suffix,
    get_function_info,
    get_sql_table_schema,
    json_serialize,
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
    def _is_class_for(cls, source: Any) -> bool:
        return False

    @classmethod
    def get_instance(cls, source: Any) -> "AbstractDataGenerator":
        subclass = cls._get_class(source=source)
        return subclass(source=source)

    def __init__(self, source: Any) -> None:
        self._source = source

    @abc.abstractmethod
    def create_name(self) -> str: ...

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        return get_default_media_data_type_by_name(name=name)

    @abc.abstractmethod
    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]: ...


class ResourceGenerator(AbstractDataGenerator):
    """Already existing resource

    Parameters
    ----------
    AbstractDataGenerator : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """

    _name_prefix = STORAGE_SCHEME + "://"

    @classmethod
    def _is_class_for(cls, source: Any) -> bool:
        return isinstance(source, str) and source.startswith(cls._name_prefix)

    def create_name(self) -> str:
        # remove scheme
        len_prefix = len(self._name_prefix)
        return self._source[len_prefix:]

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        raise NotImplementedError()

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        # TODO: if original source was a string: maybe we can get it from there?
        raise NotImplementedError()


class FunctionDataGenerator(AbstractDataGenerator):
    # TODO: what would be the default media type
    # for an arbitrary object? ==> pickle

    @classmethod
    def _is_class_for(cls, source: Any) -> bool:
        return isinstance(source, Callable)

    def create_name(self) -> str:
        # create hash of job
        func = self._source
        func_info = get_function_info(func)
        function_name = func_info["name"]

        job_descriptor_obj = {
            "function": function_name,
            "kwargs": func_info["kwargs"],
        }
        job_descriptor_bytes = json.dumps(
            job_descriptor_obj,
            sort_keys=True,
            ensure_ascii=False,
            default=json_serialize,
        ).encode()
        job_descriptor_hash = hashlib.md5(job_descriptor_bytes).hexdigest()
        media_type, _data_type = self.get_media_data_type(name=None)
        suffix = get_default_suffix(media_type=media_type) or ""

        return f"{function_name}_{job_descriptor_hash}{suffix}"

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        return "application/x-pickle", object

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        func = self._source
        data = func()
        func_info = get_function_info(func)
        metadata = {"method": func_info}
        return data, metadata


class HttpDataGenerator(AbstractDataGenerator):
    @classmethod
    def _is_class_for(cls, source: Any) -> bool:
        return isinstance(source, str) and re.match(r"^https?://", source)

    def create_name(self) -> str:
        return uri_to_data_path(self._source)

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        # this can be all kinds of things.
        # SHOULD be determined from suffix, but that will not always work
        return get_default_media_data_type_by_name(name=name)

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        import requests

        res = requests.get(self._source, stream=True)
        res.raise_for_status()
        # max_bytes = int(res.headers["Content-Length"])
        chunk_size = 1024
        bytes_iter = res.iter_content(chunk_size=chunk_size)
        data = BytesIteratorBuffer(bytes_iter=bytes_iter)

        metadata = dict({"media_type": res.headers["Content-Type"]})
        return data, metadata


class FileDataGenerator(AbstractDataGenerator):
    @classmethod
    def _is_class_for(cls, source: Any) -> bool:
        return isinstance(source, str) and re.match(r"^file://", source)

    def create_name(self) -> str:
        # only use filename
        return os.path.basename(self._source)

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        # this can be all kinds of things.
        # SHOULD be determined from suffix, but that will not always work
        return get_default_media_data_type_by_name(name=name)

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        file_path = uri_to_filepath_abs(self._source)
        data = open(file_path, "rb")
        metadata = {}
        return data, metadata


class SqlpDataGenerator(AbstractDataGenerator):
    @classmethod
    def _is_class_for(cls, source: Any) -> bool:
        return isinstance(source, str) and re.match(r"^[^/]*sql[^/]*://", source)

    def create_name(self) -> str:
        return uri_to_data_path(self._source)

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        # get default media type from name (suffix)
        media_type, _data_type = get_default_media_data_type_by_name(name)
        # per default: save as csv?
        return (media_type, list)

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        uri = self._source
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
