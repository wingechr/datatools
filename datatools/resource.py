import logging
import pickle
import re
from io import BufferedReader, BytesIO
from typing import Iterable, Tuple
from urllib.parse import parse_qs, unquote, urlencode, urlsplit, urlunsplit

import requests
import sqlalchemy as sa

from . import storage
from .constants import DEFAULT_MEDIA_TYPE, PARAM_SQL_QUERY
from .exceptions import DataExists
from .utils import (
    as_uri,
    get_sql_table_schema,
    normalize_sql_query,
    parse_content_type,
    remove_auth_from_uri_or_path,
    uri_to_filepath_abs,
)


def default_storage():
    return storage.Storage()


class UriResource(storage.StorageResource):
    def __init__(self, uri: str, name: str = None, storage: "storage.Storage" = None):
        self.__uri = as_uri(uri)
        super().__init__(storage=storage or default_storage(), name=name or self.__uri)

        url_parts = urlsplit(self.__uri)
        self.__scheme = url_parts.scheme
        self.__netloc = url_parts.netloc
        self.__path = url_parts.path
        self.__query = parse_qs(url_parts.query)
        self.__fragment = url_parts.fragment

    def __str__(self):
        return (
            f"Resource(uri='{self.uri}', name='{self.name}', storage='{self.storage}')"
        )

    @property
    def scheme(self):
        return self.__scheme

    @property
    def netloc(self):
        return self.__netloc

    @property
    def path(self):
        return self.__path

    @property
    def query(self):
        return self.__query

    @property
    def fragment(self):
        return self.__fragment

    @property
    def uri(self):
        return self.__uri

    def __read_source(self) -> Tuple[BufferedReader, dict]:
        metadata = {}
        metadata["source.path"] = remove_auth_from_uri_or_path(self.uri)

        # protocol routing
        if self.scheme == "file":
            file_path = uri_to_filepath_abs(self.uri)
            logging.debug(f"OPEN: {file_path}")
            data = open(file_path, "rb")

        elif self.scheme in ["http", "https"]:
            # because we want to encode auth headers
            # in the uri, we place it in the netloc
            # part before the @host (instead of the basic auth)
            # if it's basic auth, the pattern is `user:pass`
            # if it's a header, it's header=value
            match_header = re.match("^([^=@]+)=([^=@]+)@(.+)$", self.netloc)
            headers = {}
            if match_header:
                h_name, h_val, netloc = match_header.groups()
                h_name = unquote(h_name)
                h_val = unquote(h_val)
                headers[h_name] = h_val
                logging.debug("Stripping auth header from uri")

            else:
                netloc = self.netloc

            # TODO: is self.query encoded properly automatically?

            query = urlencode(self.query, doseq=True) if self.query else None
            url = urlunsplit([self.scheme, netloc, self.path, query, None])
            logging.debug(f"OPEN: {url}")
            res = requests.get(url, stream=True, headers=headers)

            res.raise_for_status()
            content_type = res.headers.get("Content-Type")
            if content_type:
                _meta = parse_content_type(content_type)
                metadata.update(_meta)
                logging.info(_meta)

            data = res.raw

        elif "sql" in self.scheme:
            # pop sql query
            query_dict = self.query
            sql_query = query_dict.pop(PARAM_SQL_QUERY)[0]
            sql_query = unquote(sql_query)
            sql_query = normalize_sql_query(sql_query)

            metadata["source.query"] = sql_query

            # usually, netloc is empty, and so urlunsplit()
            # drops the "//"" at the beginning
            path = self.path if self.netloc else "//" + self.path
            # doseq: if False: encode arrays differently
            query_str = urlencode(query_dict, doseq=True)

            connection_string = urlunsplit(
                [self.scheme, self.netloc, path, query_str, None]
            )
            logging.debug(f"Connect: {connection_string}")
            eng = sa.create_engine(connection_string)
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

            data = pickle.dumps(data)
            data = BytesIO(data)

            metadata["schema"] = data_schema
            metadata["mediatype"] = DEFAULT_MEDIA_TYPE

        else:
            raise NotImplementedError(self.scheme)

        return data, metadata

    def write(self, data: BufferedReader | bytes | Iterable, exist_ok=False) -> None:
        raise NotImplementedError("use save() instead")

    def save(self, exist_ok=False) -> None:
        if self.exists():
            if not exist_ok:
                raise DataExists(self)
            return

        data, metadata = self.__read_source()
        super().write(data=data, exist_ok=exist_ok)
        self.metadata.update(metadata)

    def open(self) -> BufferedReader:
        # make sure that data exists in storage
        self.save(exist_ok=True)
        return super().open()
