import logging
import os
import re
from abc import ABC, abstractmethod
from urllib.parse import parse_qs, unquote, urlparse, urlsplit

import pandas as pd
import requests
import requests_cache
from sqlalchemy import create_engine, inspect

from .utils.byte import hash, validate_hash
from .utils.collection import FrozenUniqueMap, UniqueMap
from .utils.datetime import fmt_datetime_tz, now
from .utils.env import get_user
from .utils.json import dumpb as json_dumpb
from .utils.json import dumps as json_dumps
from .utils.json import infer_table_schema
from .utils.json import load as json_load
from .utils.json import loadb as json_loadb
from .utils.json import validate_json_schema, validate_table_schema

requests_cache.install_cache("datatools_schema_cache", backend="sqlite", use_temp=True)


def is_uri(uri_or_path: str) -> bool:
    """at least two lowercase characters before ':'"""
    return re.match("^([a-z+]{2,}):", uri_or_path)


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


class Report(FrozenUniqueMap):
    def __init__(self, data_bytes, hash_method="sha256", metadata=None):
        hashsum = hash(data_bytes, method=hash_method)
        metadata = metadata or {}
        data = {
            "hash": f"{hash_method}:{hashsum}",
            "size": len(data_bytes),
            "user": get_user(),
            "timestamp": fmt_datetime_tz(now()),
        }
        super().__init__(list(data.items()) + list(metadata.items()))

    def to_dict(self):
        return dict(self.items())

    def __str__(self):
        return json_dumps(self.to_dict())


class Location(ABC):
    __slots__ = ["__uri"]

    def __init__(self, uri):
        self.__uri = uri

    @classmethod
    def _to_json(cls, x):
        if isinstance(x, bytes):
            return json_loadb(x)
        elif isinstance(x, Location):
            return cls._to_json(x.read())
        return x

    @classmethod
    def _to_bytes(cls, x):
        if isinstance(x, bytes):
            return x
        elif isinstance(x, Location):
            return cls._to_bytes(x.read())
        return json_dumpb(x)

    @property
    def uri(self):
        return self.__uri

    @property
    def supports_metadata(self):
        return False

    def read(
        self,
        as_json=False,
        bytes_hash: str = None,
        json_schema: str | dict | bool = None,
        table_schema: dict = None,
    ) -> bytes | object:
        data = self._read()
        if not as_json or bytes_hash:
            data_bytes = self._to_bytes(data)

        if bytes_hash is not None:
            validate_hash(data_bytes, bytes_hash)

        if (json_schema or table_schema) and not as_json:
            raise Exception("json_schema or table_schema require as_json")

        if as_json:
            data = self._to_json(data)
            data_json = data

            if json_schema is not None:
                validate_json_schema(data_json, json_schema)
            if table_schema is not None:
                if table_schema is True:
                    table_schema = infer_table_schema(data_json)
                else:
                    validate_table_schema(data_json, table_schema)

        else:
            if json_schema:
                raise Exception("cannot use json validate on bytes")
            if table_schema:
                raise Exception("cannot use data validate on bytes")
            data = data_bytes

        return data

    def write(
        self,
        data: bytes | object,
        overwrite=False,
        bytes_hash: str = None,
        json_schema: str | dict | bool = None,
        table_schema: str | dict | bool = None,
        metadata: dict = None,
    ) -> Report:
        if metadata and not self.supports_metadata:
            raise NotImplementedError("Location does not support writing of metadata")
        if not metadata and self.supports_metadata:
            metadata = {}

        if bytes_hash is not None:
            data_bytes = self._to_bytes(data)
            validate_hash(data_bytes, bytes_hash)

        if json_schema or table_schema:
            data_json = self._to_json(data)
            if json_schema is not None:
                validate_json_schema(data_json, json_schema)
            if table_schema is not None:
                if table_schema is True:
                    table_schema = infer_table_schema(data_json)
                else:
                    validate_table_schema(data_json, table_schema)

        kwargs = {}

        if metadata is not None:
            kwargs["metadata"] = metadata
            if table_schema:
                metadata["schema"] = table_schema

        result = self._write(data, overwrite=overwrite, **kwargs)

        if isinstance(result, Report):
            return result
        result = self._to_bytes(result)
        return Report(result)

    @abstractmethod
    def _read(self) -> bytes | object:
        raise NotImplementedError()

    @abstractmethod
    def _write(self, data: bytes | object, overwrite=False) -> bytes | Report:
        raise NotImplementedError()

    def __str__(self):
        return self.uri


class FileLocation(Location):
    __slots__ = ["__path"]

    def __init__(self, path):
        path = path.replace("\\", "/")
        # TODO: relative uri
        uri = f"file:///{path}"
        super().__init__(uri=uri)
        self.__path = path

    @property
    def path(self):
        return self.__path

    @property
    def filepath(self):
        return os.path.abspath(self.path)

    def exists(self):
        return os.path.exists(self.filepath)

    def _read(self) -> bytes | object:
        with open(self.filepath, "rb") as file:
            data = file.read()
        return data

    def _write(self, data: bytes | object, overwrite=False) -> bytes:
        # prepare folder
        if not overwrite and self.exists():
            raise FileExistsError(self.filepath)
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)

        data = self._to_bytes(data)

        try:
            with open(self.filepath, "wb") as file:
                file.write(data)
        except Exception:
            # cleanup
            if self.exists():
                try:
                    os.remove(self.filepath)
                except Exception:
                    pass
            raise

        return data


class SqlLocation(Location):
    __slots__ = ["__sql", "__engine"]

    def __init__(self, uri):
        # remove query
        uri, sql = uri.split("#")
        sql = unquote(sql)
        super().__init__(uri=uri)
        self.__sql = sql
        self.__engine = create_engine(uri)

    @property
    def table(self):
        if "." in self.__sql:
            _, table = self.__sql.split(".")
            return table
        else:
            return self.__sql

    @property
    def schema(self):
        if "." in self.__sql:
            schema, _ = self.__sql.split(".")
            return schema
        else:
            return None

    @property
    def database(self):
        # TODO: not working on pyodbc
        database = self.__engine.url.database

        if not database and "?odbc_connect=" in self.uri:
            # TODO: global functions to parse uri
            uri = urlsplit(self.uri)
            query = parse_qs(uri.query)
            odbc = query["odbc_connect"][0]
            odbc_kwargs = {}
            for kv in odbc.split(";"):
                k, v = kv.split("=")
                odbc_kwargs[k.lower().strip()] = v.strip()
            database = odbc_kwargs.get("database")

        return database

    def _read(self) -> bytes | object:
        logging.debug(self.__sql)
        df = pd.read_sql(self.__sql, self.__engine)
        data = df.to_dict(orient="records")
        return data

    def _write(self, data: bytes | object, overwrite=False) -> bytes:
        table = self.table
        schema = self.schema
        data_bytes = self._to_bytes(data)
        data = self._to_json(data)
        df = pd.DataFrame(data)
        if_exists = "append" if overwrite else "fail"  # TODO: replace?

        if schema:
            logging.debug(f"upload to {schema}.{table}, if_exists={if_exists}")
        else:
            logging.debug(f"upload to {table}, if_exists={if_exists}")

        # method = "multi" # TODO: check if this works
        method = None

        df.to_sql(
            table,
            self.__engine,
            schema=schema,
            index=False,
            if_exists=if_exists,
            method=method,
        )
        metadata = {"rows": len(df)}

        return Report(data_bytes, metadata=metadata)

    def connection(self):
        return self.__engine.begin()

    @property
    def column_names(self):
        meta = self.read_metadata()
        return [f["name"] for f in meta["schema"]["fields"]]

    def read_metadata(self) -> object:
        # TODO: check if i's table
        # TODO: get other metadata
        table = self.table
        schema = self.schema

        insp = inspect(self.__engine)
        columns = insp.get_columns(table, schema=schema)

        metadata = {
            "schema": {
                "fields": [{"name": c["name"], "type": str(c["type"])} for c in columns]
            }
        }
        return metadata


class HttpLocation(Location):
    def __init__(self, uri):
        super().__init__(uri=uri)

    def _read(self) -> bytes | object:
        resp = requests.get(self.uri)
        resp.raise_for_status()
        return resp.content

    def _write(self, data: bytes | object, overwrite=False) -> bytes:
        if not overwrite:
            raise NotImplementedError("check existence not yet defined for http")
        # todo: content type header?
        data = self._to_bytes(data)
        resp = requests.post(self.uri, data)
        resp.raise_for_status()
        return data


class DatapackageResourceLocation(Location):
    __slots__ = ["__base_path", "__name"]

    def __init__(self, path):
        # __resource_name is like fragment
        base_path, resource_name = path.split("#")
        base_path.replace("\\", "/").rstrip("/")
        uri = f"dpr://{base_path}#{resource_name}"  # TODO
        super().__init__(uri=uri)

        self.__base_path = base_path
        self.__name = resource_name

    @property
    def supports_metadata(self):
        return True

    @property
    def base_path(self):
        return self.__base_path

    @property
    def name(self):
        return self.__name

    @property
    def datapackage_json_path(self):
        return self.__base_path + "/datapackage.json"

    def get_path(self, resource: dict) -> str:
        path = resource["path"]
        assert path.startswith("data/")
        path = self.__base_path + "/" + path
        return path

    def _read(self) -> bytes | object:
        # TODO: read metadata as well?
        package = self.__load_datapackage_json()
        idx = self.__get_resource_index_by_name(package, self.name)
        resource = package["resources"][idx]
        if "data" in resource:
            return resource["data"]
        else:
            path = self.get_path(resource)
            return FileLocation(path).read()

    def read_metadata(self) -> object:
        package = self.__load_datapackage_json()
        idx = self.__get_resource_index_by_name(package, self.name)
        resource = package["resources"][idx]
        # TODO: remove data/path?
        return resource

    def _write(self, data: bytes | object, overwrite=False, metadata=None) -> Report:

        package = self.__load_datapackage_json()
        resources = package["resources"]

        try:
            resource_idx = self.__get_resource_index_by_name(package, self.name)
        except KeyError:
            resource_idx = None

        if resource_idx is None:
            resource_idx = len(resources)
            resources.append({})  # add dummy, will be filled later
        else:  # existing
            if not overwrite:
                raise FileExistsError(str(self))
            resource = resources[resource_idx]
            # if path exists: delete
            if "path" in resource:
                path = self.get_path(resource)
                if not os.path.isfile(path):
                    logging.warning("referenced file does not exist: %s", path)
                else:
                    os.remove(path)
        resource = UniqueMap([("name", self.name), ("path", "data/" + self.name)])

        path = self.get_path(resource)
        report = FileLocation(path).write(data, overwrite=False)
        resource["hash"] = report["hash"]
        resource["size"] = report["size"]
        resource["changed"] = {"user": report["user"], "timestamp": report["timestamp"]}

        if metadata:
            for k, v in metadata.items():
                resource[k] = v

        resources[resource_idx] = dict(resource.items())

        # update new id of package: hash over name + hash of resources
        # TODO: what if has doed not exist
        package_hash_data = [{"name": r["name"], "hash": r["hash"]} for r in resources]
        package_hash_data = self._to_bytes(package_hash_data)
        package_report = Report(package_hash_data)
        package["hash"] = package_report["hash"]
        package["changed"] = {"user": report["user"], "timestamp": report["timestamp"]}

        # write index
        FileLocation(self.datapackage_json_path).write(package, overwrite=True)

        return report

    def __load_datapackage_json(self):
        # ensure base_dir exists
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)
        if not os.path.exists(self.datapackage_json_path):
            name = os.path.basename(self.base_path)
            data = {"name": name, "resources": []}
        else:
            data = json_load(self.datapackage_json_path)
        return data

    def __get_resource_index_by_name(self, package, name) -> int:
        for idx, res in enumerate(package["resources"]):
            if res["name"] == name:
                return idx
        raise KeyError(name)


class MemoryLocation(Location):
    __slots__ = ["__data"]

    def __init__(self, data=None):
        self.__data = data

    def _read(self) -> bytes | object:
        return self.__data

    def _write(self, data: bytes | object, overwrite=False) -> bytes:
        if self.__data is not None and not overwrite:
            raise Exception("read only")
        self.__data = data
        return self.__data


def location(uri_or_path: str) -> Location:
    if not is_uri(uri_or_path):
        # it's a path
        if "#" in uri_or_path:
            scheme = "dpr"  # its a datapackage resource
        else:
            scheme = "file"
    else:
        scheme = get_scheme(uri_or_path)

    if scheme == "file":
        cls = FileLocation

    elif scheme == "dpr":  # FIXME: for file and dpr: parse uri
        cls = DatapackageResourceLocation
    elif scheme.startswith("http"):
        cls = HttpLocation
    elif "sql" in scheme:
        cls = SqlLocation
    else:
        raise NotImplementedError(scheme)

    return cls(uri_or_path)
