import logging
import os
import re
from abc import ABC, abstractmethod
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd
import requests

from datatools.utils.byte import hash, validate_hash
from datatools.utils.collection import FrozenUniqueMap
from datatools.utils.datetime import fmt_datetime_tz, now
from datatools.utils.env import get_user
from datatools.utils.json import dumpb as json_dumpb
from datatools.utils.json import dumps as json_dumps
from datatools.utils.json import load as json_load
from datatools.utils.json import loadb as json_loadb
from datatools.utils.json import validate_dataschema, validate_jsonschema

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


def to_json(x):
    if isinstance(x, bytes):
        return json_loadb(x)
    elif isinstance(x, Resource):
        return to_json(x.read())
    return x


def to_bytes(x):
    if isinstance(x, bytes):
        return x
    elif isinstance(x, Resource):
        return to_bytes(x.read())
    return json_dumpb(x)


class Report(FrozenUniqueMap):
    def __init__(self, data_bytes, hash_method="sha256"):
        hashsum = hash(data_bytes, method=hash_method)
        data = {
            "hash": f"{hash_method}:{hashsum}",
            "size": len(data_bytes),
            "user": get_user(),
            "timestamp": fmt_datetime_tz(now()),
        }
        super().__init__(data.items())

    def to_dict(self):
        return dict(self.items())

    def __str__(self):
        return json_dumps(self.to_dict())


class Resource(ABC):
    def read(
        self,
        as_json=False,
        validate_bytes_hash: str = None,
        validate_json_schema: str | dict | bool = None,
        validate_data_schema: dict = None,
    ) -> bytes | object:
        data = self._read()
        if not as_json or validate_bytes_hash:
            data_bytes = to_bytes(data)

        if validate_bytes_hash:
            validate_hash(data_bytes, validate_bytes_hash)

        if as_json:
            data = to_json(data)
            if validate_json_schema:
                validate_jsonschema(data, validate_json_schema)
            if validate_data_schema:
                validate_dataschema(data, validate_data_schema)

        else:
            if validate_json_schema:
                raise Exception("cannot use json validate on bytes")
            if validate_data_schema:
                raise Exception("cannot use data validate on bytes")
            data = data_bytes

        return data

    def write(self, data: bytes | object, overwrite=False) -> Report:
        result = self._write(data, overwrite=overwrite)
        if isinstance(result, Report):
            return result
        result = to_bytes(result)
        return Report(result)

    @abstractmethod
    def _read(self) -> bytes | object:
        raise NotImplementedError()

    @abstractmethod
    def _write(self, data: bytes | object, overwrite=False) -> bytes | Report:
        raise NotImplementedError()


class FileResource(Resource):
    __slots__ = ["__path"]

    def __init__(self, path):
        path = path.replace("\\", "/")
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

        data = to_bytes(data)

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

    def _read(self) -> bytes | object:
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

    def _write(self, data: bytes | object, overwrite=False) -> bytes:
        table = self.query_get_single("table")
        assert table
        schema = self.query_get_single("schema")
        data_bytes = to_bytes(data)  # check if it can be serialized
        data = to_json(data)
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
        return data_bytes


class HttpResource(Resource):
    def __init__(self, uri):
        self.__uri = uri

    @property
    def uri(self):
        return self.__uri

    def _read(self) -> bytes | object:
        resp = requests.get(self.uri)
        resp.raise_for_status()
        return resp.content

    def _write(self, data: bytes | object, overwrite=False) -> bytes:
        if not overwrite:
            raise NotImplementedError("check existence not yet defined for http")
        # todo: content type header?
        data = to_bytes(data)
        resp = requests.post(self.uri, data)
        resp.raise_for_status()
        return data


class DatapackageResource(Resource):
    __slots__ = ["__base_path", "__name"]

    def __init__(self, path):
        # __resource_name is like fragment
        base_path, resource_name = path.split("#")
        self.__base_path = base_path.replace("\\", "/").rstrip("/")
        self.__name = resource_name

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
        package = self.__load_datapackage_json()
        idx = self.__get_resource_index_by_name(package, self.name)
        resource = package["resources"][idx]
        if "data" in resource:
            return resource["data"]
        else:
            path = self.get_path(resource)
            return FileResource(path).read()

    def _write(self, data: bytes | object, overwrite=False) -> Report:
        package = self.__load_datapackage_json()
        resources = package["resources"]
        err_on_exist = not overwrite
        resource_idx = self.__get_resource_index_by_name(
            package, self.name, err_on_exist=err_on_exist
        )
        if resource_idx is None:
            resource_idx = len(resources)
            resources.append({})  # add dummy, will be filled later
        else:  # existing and overwrite is ok
            resource = resources[resource_idx]
            # if path exists: delete
            if "path" in resource:
                path = self.get_path(resource)
                if not os.path.isfile(path):
                    logging.warning("referenced file does not exist: %s", path)
                else:
                    os.remove(path)
        resource = {"name": self.name, "path": "data/" + self.name}
        resources[resource_idx] = resource
        path = self.get_path(resource)
        report = FileResource(path).write(data, overwrite=False)
        resource["hash"] = report["hash"]
        resource["size"] = report["size"]
        resource["changed"] = {"user": report["user"], "timestamp": report["timestamp"]}

        # update new id of package: hash over name + hash of resources
        # TODO: what if has doed not exist
        package_hash_data = [{"name": r["name"], "hash": r["hash"]} for r in resources]
        package_hash_data = to_bytes(package_hash_data)
        package_report = Report(package_hash_data)
        package["hash"] = package_report["hash"]
        package["changed"] = {"user": report["user"], "timestamp": report["timestamp"]}

        # write index
        FileResource(self.datapackage_json_path).write(package, overwrite=True)

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

    def __get_resource_index_by_name(self, package, name, err_on_exist=False) -> int:
        for idx, res in enumerate(package["resources"]):
            if res["name"] == name:
                if err_on_exist:
                    raise KeyError(name)
                return idx
        if not err_on_exist:
            raise KeyError(name)
        return None


class MemoryResource(Resource):
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


def resource(uri_or_path: str) -> Resource:
    if not is_uri(uri_or_path):
        # it's a path
        if "#" in uri_or_path:
            scheme = "dpr"  # its a datapackage resource
        else:
            scheme = "file"
    else:
        scheme = get_scheme(uri_or_path)

    if scheme == "file":
        cls = FileResource

    elif scheme == "dpr":  # FIXME: for file and dpr: parse uri
        cls = DatapackageResource
    elif scheme.startswith("http"):
        cls = HttpResource
    elif "sql" in scheme:
        cls = SqlResource
    else:
        raise NotImplementedError(scheme)

    return cls(uri_or_path)
