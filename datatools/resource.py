import logging
import os
import re
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd
import requests

from datatools.utils.json import dumpb as json_dumpb
from datatools.utils.json import dumps as json_dumps
from datatools.utils.json import loadb as json_loadb
from datatools.utils.json import loads as json_loads

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


def is_uri(uri_or_path: str) -> bool:
    """at least two lowercase characters before ':'"""
    return re.match("^([a-z]{2,}):", uri_or_path)


def get_scheme(uri: str) -> str:
    return uri.split(":")[0]


def parse_query(query: str) -> dict:
    return parse_qs(query)


def get_single(query: dict, key: str, default: str = None) -> str:
    vals = query.get(key, [default])
    if len(vals) != 1:
        raise ValueError("must specify one of %s" % key)
    return vals[0]


def path_to_uri(path):
    path = path.replace("\\", "/")
    # if absolute Windows path with drive
    # file:///c:/path
    if re.match(r"^[a-zA-Z]:", path):
        path = "///" + path
    # UNIX abs path:
    # file:///path
    elif re.match(r"^/[^/]", path):
        path = "//" + path
    # Windows UNC network path OR relative path:
    # do not change
    return "file:" + path


def uri_to_path(uri):
    url = urlparse(uri)
    path = unquote(url.path)
    if re.match("^/[A-Za-z]:", path):
        path = path[1:]  # remove slash
    if url.netloc:
        # windows unc
        path = "//" + url.netloc + path
    return path


class Resource:
    def read_bytes(self):
        raise NotImplementedError()

    def read_text(self):
        data_bytes = self.read_bytes()
        return data_bytes.decode()

    def read_json(self):
        data_bytes = self.read_bytes()
        return json_loadb(data_bytes)

    def write_bytes(self, byte_data: bytes, overwrite=False):
        raise NotImplementedError()

    def write_text(self, text_data: str, overwrite=False):
        data_bytes = text_data.encode()
        return self.write_bytes(data_bytes, overwrite=overwrite)

    def write_json(self, data: object, overwrite=False):
        data_bytes = json_dumpb(data)
        return self.write_bytes(data_bytes, overwrite=overwrite)

    def write_resource(self, resource: object, overwrite=False):
        data_bytes = resource.read_bytes()
        return self.write_bytes(data_bytes, overwrite=overwrite)


class FileResource(Resource):
    __slots__ = ["__path"]

    def __init__(self, path):
        self.__path = path

    @property
    def path(self):
        return self.__path

    @property
    def filepath(self):
        return os.path.abspath(self.path)

    def read_bytes(self):
        with open(self.filepath, "rb") as file:
            return file.read()

    def exists(self):
        return os.path.exists(self.filepath)

    def write_bytes(self, byte_data: bytes, overwrite=False):
        if not overwrite and self.exists():
            raise FileExistsError(self.filepath)
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        try:
            with open(self.filepath, "wb") as file:
                file.write(byte_data)
        except Exception:
            # cleanup
            if self.exists():
                try:
                    os.remove(self.filepath)
                except Exception:
                    pass
            raise


class SqlResource(Resource):
    __slots__ = ["__query", "__uri"]

    def __init__(self, uri):
        # remove query
        uri, query = uri.split("?")
        query = parse_query(query)
        self.__query = query
        self.__uri = uri

    def query_get_single(self, key):
        return get_single(self.__query, key)

    @property
    def uri(self):
        return self.__uri

    def read_bytes(self):
        data = self.read_json(resource)
        return json_dumpb(data)

    def read_text(self):
        data = self.read_json(resource)
        return json_dumps(data)

    def read_json(self):
        sql = self.query_get_single("sql")
        table = self.query_get_single("table")
        if sql:
            df = pd.read_sql(sql, self.uri)
        elif table:
            df = pd.read_sql(table, self.uri)
        else:
            raise ValueError("table or sql")

        data = df.to_dict(orient="records")
        return data

    def write_bytes(self, byte_data: bytes, overwrite=False):
        data = json_loadb(byte_data)
        return self.write_json(data, overwrite=overwrite)

    def write_text(self, text_data: str, overwrite=False):
        data = json_loads(text_data)
        return self.write_json(data, overwrite=overwrite)

    def write_json(self, data: object, overwrite=False):
        table = self.query_get_single("table")
        assert table
        schema = self.query_get_single("schema")
        df = pd.DataFrame(data)
        if_exists = "append" if overwrite else "fail"  # TODO: replace?
        df.to_sql(
            table,
            self.uri,
            schema=schema,
            index=False,
            if_exists=if_exists,
            method="multi",
        )


class HttpResource(Resource):
    def __init__(self, uri):
        self.__uri = uri

    @property
    def uri(self):
        return self.__uri

    def read_bytes(self):
        resp = requests.get(self.uri)
        resp.raise_for_status()
        return resp.content

    def write_bytes(self, byte_data: bytes, overwrite=False):
        if not overwrite:
            raise NotImplementedError("overwrite not yet defined for http")
        # todo: content type header?
        resp = requests.post(self.uri, byte_data)
        resp.raise_for_status()


def resource(uri_or_path: str) -> Resource:
    if not is_uri(uri_or_path):
        # it's a path
        scheme = "file"
    else:
        scheme = get_scheme(uri_or_path)

    if scheme == "file":
        cls = FileResource
    elif scheme.startswith("http"):
        cls = HttpResource
    elif "sql" in scheme:
        cls = SqlResource
    else:
        raise NotImplementedError(scheme)

    return cls(uri_or_path)
