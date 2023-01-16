import getpass
import hashlib
import json
import logging  # noqa
import os
import re
import shutil
import socket
import subprocess as sp
import tempfile
from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import date, datetime, time, timezone
from io import BufferedReader, BytesIO
from os.path import (
    abspath,
    dirname,
    exists,
    getsize,
    isdir,
    isfile,
    join,
    realpath,
    relpath,
)
from pathlib import Path
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWRITE
from tempfile import mkdtemp
from typing import List
from urllib.parse import parse_qs, unquote, unquote_plus, urlparse, urlsplit

import chardet
import frictionless
import inflection
import jsonref
import jsonschema
import pandas as pd
import pyodbc
import pytz
import requests
import sqlalchemy as sa
import tzlocal
import unidecode
from appdirs import AppDirs

# import requests_cache
# requests_cache.install_cache("datatools_schema_cache", backend="sqlite", use_temp=True) # noqa

__version__ = "0.0.1"


SCHEMA_SUFFIX = ".schema.json"
DEFAULT_CHUNK_SIZE = 2**24
DEFAULT_ENCODING = "utf-8"
FMT_DATETIME_TZ = "%Y-%m-%dT%H:%M:%S%z"
FMT_DATETIME = "%Y-%m-%dT%H:%M:%S"
FMT_DATE = "%Y-%m-%d"
FMT_TIME = "%H:%M:%S"

dialects = ["sqlite", "postgresql", "mysql", "mssql"]


app_dirs = AppDirs("datatools", "datatools")


def bytes_hash(byte_data, method="sha256") -> str:
    iterator = Iterator(byte_data, hash_method=method)
    iterator.read()
    return iterator.get_current_hash()


def validate(byte_data, method, hashsum):
    data_hashsum = bytes_hash(byte_data, method=method)
    if hashsum != data_hashsum:
        raise Exception(
            "Validation Error: expected %s does not match %s", hashsum, data_hashsum
        )


def validate_hash(byte_data, hash):
    method, hashsum = hash.split(":")
    return validate(byte_data, method, hashsum)


class Iterator:
    __slots__ = [
        "data_stream",
        "hasher",
        "size",
        "max_size",
        "chunk_size",
        "iter_buffer",
    ]

    def __init__(self, data_stream, chunk_size=None, max_size=None, hash_method=None):
        if hash_method:
            self.hasher = getattr(hashlib, hash_method)()
        else:
            self.hasher = None
        self.size = 0
        self.max_size = max_size
        self.chunk_size = chunk_size or DEFAULT_CHUNK_SIZE

        self.iter_buffer = None

        # logging.debug("%s", type(data_stream))
        if isinstance(data_stream, str):
            # logging.debug("opening file: %s", data_stream)
            self.data_stream = open(data_stream, "rb")
        elif isinstance(data_stream, bytes):
            # logging.debug("buffer data: %s")
            self.data_stream = BytesIO(data_stream)
        elif isinstance(data_stream, (BytesIO, BufferedReader)):
            self.data_stream = data_stream
        else:
            self.data_stream = iter(data_stream)
            self.iter_buffer = b""

    def __iter__(self):
        return self

    def __del__(self):
        try:
            # self.data_stream.__exit__(None, None, None)
            self.data_stream.close()
        except Exception:
            pass

    def __next__(self):
        chunk = self.read(self.chunk_size)
        if not chunk:
            raise StopIteration()
        return chunk

    def read(self, size=-1):
        if self.max_size and self.size + size > self.max_size:
            size = self.max_size - self.size

        if self.iter_buffer is None:
            # not an iterator, so read
            chunk = self.data_stream.read(size)
        else:
            for chunk in self.data_stream:
                self.iter_buffer += chunk
                if size > -1 and len(self.iter_buffer) >= size:
                    break
            if size > -1:
                chunk = self.iter_buffer[:size]
                self.iter_buffer = self.iter_buffer[size:]
            else:
                chunk = self.iter_buffer
                self.iter_buffer = b""

        self.size += len(chunk)
        # logging.debug("read %d bytes (max_size=%d)", len(chunk), size)
        if self.hasher:
            self.hasher.update(chunk)
        return chunk

    def get_current_hash(self):
        if self.hasher:
            return self.hasher.hexdigest()
        else:
            raise Exception("no hasher")

    def get_current_size(self):
        return self.size


def detect_encoding(byte_data, size=None):
    if size > 0:
        byte_data = byte_data[:size]
    return chardet.detect(byte_data)["encoding"]


class FileCache:
    __slots__ = ["base_dir"]

    hash_method = "sha256"

    def __init__(self, base_dir=None):
        if not base_dir:
            base_dir = mkdtemp(prefix="datatools.cache.")
        self.base_dir = abspath(base_dir)
        # logging.debug("cache dir: %s", self.base_dir)
        makedirs(self.base_dir)

    def _get_path_from_id(
        self,
        id,
    ):
        return str(id)

    def _get_path_exists(
        self,
        id,
    ):
        path = self._get_path_from_id(id)
        path = self._validate_path(path)
        return path, isfile(path)

    def _validate_path(self, path):
        path = join(self.base_dir, path)
        path = abspath(path)
        if not path.startswith(self.base_dir):
            raise Exception("invalid path")
        if isdir(path):
            raise Exception("invalid path")
        makedirs(dirname(path), exist_ok=True)
        return path

    def __contains__(self, id):
        _, exists = self._get_path_exists(id)
        return exists

    def __getitem__(self, id):
        path, exists = self._get_path_exists(id)
        if not exists:
            raise KeyError(id)
        return Iterator(path)

    def __setitem__(self, id, data):
        path, exists = self._get_path_exists(id)

        it = Iterator(data, hash_method=self.hash_method)
        byte_data = it.read()
        if exists:
            with Iterator(path, hash_method=self.hash_method) as it_old:
                it_old.read()
                if it_old.get_current_hash() != it.get_current_hash():
                    raise Exception("hash changed")

            # make_file_writable(path)
            return  # do not overwrite

        with open(path, "wb") as file:
            file.write(byte_data)
        make_file_readlonly(path)


import logging  # noqa


class FrozenUniqueMap:
    """immutable, ordered map of unique hashables mapping to variables"""

    __slots__ = ["__values", "__indices", "__keys"]

    def __init__(self, items):

        self.__indices = {}  # key -> idx
        self.__keys = []
        self.__values = []

        for key, val in items:
            self._setitem(key, val)

    def _setitem(self, key, val):
        if key in self.__indices:  # ensure uniqueness
            raise KeyError(f"{key} not unique")
        idx = len(self.__indices)
        self.__indices[key] = idx
        self.__keys.append(key)
        self.__values.append(val)

    def __len__(self):
        return len(self.__keys)

    def __getitem__(self, key):
        idx = self.__indices[key]
        return self.__values[idx]

    def __contains__(self, key):
        return key in self.__indices

    def keys(self):
        return iter(self.__keys)

    def values(self):
        return iter(self.__values)

    def items(self):
        return zip(self.__keys, self.__values)

    def index(self, key):
        return self.__indices[key]


class UniqueMap(FrozenUniqueMap):
    """ordered map of unique hashables mapping to variables"""

    def __setitem__(self, key, val):
        return self._setitem(key, val)


def odbc_drivers() -> List[str]:
    return pyodbc.drivers()


def guess_odbc_driver(name):
    for dr in odbc_drivers():
        if name.lower() in dr.lower():
            return dr.lower()
    raise ValueError(name)


def get_odbc_connectionstring(**kwargs):
    constr = ";".join("%s=%s" % (k, v) for k, v in kwargs.items() if v is not None)
    return constr


def get_uri(dialect, path, driver=None):
    if driver:
        dialect = f"{dialect}+{driver}"
    uri = f"{dialect}://{path}"
    return uri


def get_uri_odbc(dialect, odbc_driver=None, **obbc_kwargs):
    cs = get_odbc_connectionstring(driver=odbc_driver, **obbc_kwargs)
    path = f"/?odbc_connect={cs}"
    return get_uri(dialect, path, driver="pyodbc")


def get_uri_odbc_access(database):
    odbc_driver = guess_odbc_driver("microsoft access")
    return get_uri_odbc("access", odbc_driver=odbc_driver, dbq=database)


def get_uri_odbc_sqlite(database):
    odbc_driver = guess_odbc_driver("sqlite3")
    return get_uri_odbc("sqlite", odbc_driver=odbc_driver, database=database)


def get_uri_odbc_sqlserver(server, database=None):
    odbc_driver = guess_odbc_driver("sql server")
    return get_uri_odbc(
        "mssql", odbc_driver=odbc_driver, server=server, database=database
    )


def get_uri_sqlite(database=None):
    if not database:  # memory
        path = ""
    else:
        path = database
        if not path.startswith("/"):
            path = "/" + path
    return get_uri(dialect="sqlite", path=path)


def create_mock_engine(dialect_name, executor):
    dialect = sa.create_engine(dialect_name + "://").dialect

    def _executor(sql, *args, **kwargs):
        executor(str(sql.compile(dialect=dialect)))

    return sa.create_mock_engine(dialect_name + "://", _executor)


def reflect_tables(uri):
    eng = sa.create_engine(uri)
    # eng.connect() # test
    meta = sa.MetaData()
    meta.reflect(eng)  # load all tables definitions
    return meta


def get_timezone_utc():
    return pytz.utc


def _get_timezone_local():
    """e.g.  DstTzInfo 'Europe/Berlin' LMT+0:53:00 STD"""
    return tzlocal.get_localzone()


def get_current_timezone_offset() -> timezone:
    """e.g.  DstTzInfo 'Europe/Berlin' LMT+0:53:00 STD"""
    tz_local = _get_timezone_local()
    now = datetime.now()
    return timezone(tz_local.utcoffset(now))


def to_timezone(dt, tz):
    if dt.tzinfo:  # convert
        return dt.astimezone(tz)
    else:
        return dt.replace(tzinfo=tz)


def now():
    return to_timezone(datetime.now(), get_current_timezone_offset())


def utcnow():
    return to_timezone(datetime.utcnow(), get_timezone_utc())


def fmt_date(dt):
    return dt.strftime(FMT_DATE)


def fmt_time(dt):
    return dt.strftime(FMT_TIME)


def fmt_datetime(dt):
    return dt.strftime(FMT_DATETIME)


def fmt_datetime_tz(dt):
    """the regular strftime does not add a colon in the offset!"""
    result = dt.strftime(FMT_DATETIME_TZ)
    result = result[:-2] + ":" + result[-2:]
    return result


def serialize(x):
    if isinstance(x, datetime):
        if x.tzinfo:
            return fmt_datetime_tz(x)
        else:
            return fmt_datetime(x)
    elif isinstance(x, date):
        return fmt_date(x)
    elif isinstance(x, time):
        return fmt_time(x)
    else:
        raise NotImplementedError(type(x))


def get_user():
    """Return current user name"""
    return getpass.getuser()


def get_host():
    """Return current host name"""
    return socket.gethostname()


def get_user_host():
    return "%s@%s" % (get_user(), get_host())


def get_git_commit(cwd=None):
    proc = sp.Popen(["git", "rev-parse", "HEAD"], cwd=cwd, stdout=sp.PIPE)
    stdout, _ = proc.communicate()
    assert proc.returncode == 0
    return stdout.decode().strip()


def make_file_readlonly(file_path):
    os.chmod(file_path, S_IREAD | S_IRGRP | S_IROTH)


def make_file_writable(file_path):
    os.chmod(file_path, S_IWRITE)


def get_size(file_path) -> int:
    return getsize(file_path)


def normpath(path):
    result = path.replace("\\", "/")  # windows -> normal
    result = result.lstrip("./")
    return result


def get_file_path_uri(file_path):
    file_path = Path(realpath(file_path))
    uri = file_path.as_uri()
    return uri


def assert_slash_end(path):
    if not path.endswith("/"):
        path += "/"
    return path


def walk_rel(start, filter=None):
    for rt, _ds, fs in os.walk(start):
        rt_rel = relpath(rt, start)
        for f in fs:
            file_path_rel = normpath(join(rt_rel, f))
            if not filter or filter(file_path_rel):
                yield file_path_rel
            else:
                logging.debug(f"SKIPPING: {file_path_rel}")


def copy_uri(source_uri, target_path, overwrite=False):
    source_path = urlparse(source_uri).path
    if source_path.startswith("/./"):  # relative path
        source_path = source_path.lstrip("/./")
    copy(source_path, target_path, overwrite=overwrite)


def makedirs(path, exist_ok=True):
    if isdir(path):
        return
    logging.debug(f"MAKEDIR {path}")
    os.makedirs(path, exist_ok=exist_ok)


def copy(source_file_path, target_file_path, overwrite=False):
    assert_not_exist(target_file_path, overwrite=overwrite)
    makedirs(dirname(target_file_path), exist_ok=True)
    logging.debug(f"COPY {source_file_path} ==> {target_file_path}")
    shutil.copy(source_file_path, target_file_path)


def move(source_file_path, target_file_path, overwrite=False):
    assert_not_exist(target_file_path, overwrite=overwrite)
    makedirs(dirname(target_file_path), exist_ok=True)
    logging.debug(f"MOVE {source_file_path} ==> {target_file_path}")
    shutil.move(source_file_path, target_file_path)


def assert_not_exist(target_file_path, overwrite=False):
    if not exists(target_file_path):
        return
    if not overwrite:
        logging.error(f"File exists: {target_file_path}")
        raise FileExistsError(f"File exists: {target_file_path}")
    logging.debug(f"RM {target_file_path}")
    os.remove(target_file_path)


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
    elif isinstance(x, Location):
        return to_json(x.read())
    return x


def to_bytes(x):
    if isinstance(x, bytes):
        return x
    elif isinstance(x, Location):
        return to_bytes(x.read())
    return json_dumpb(x)


class Report(FrozenUniqueMap):
    def __init__(self, data_bytes, hash_method="sha256"):
        hashsum = bytes_hash(data_bytes, method=hash_method)
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


class Location(ABC):
    __slots__ = ["__uri"]

    def __init__(self, uri):
        self.__uri = uri

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
            data_bytes = to_bytes(data)

        if bytes_hash is not None:
            validate_hash(data_bytes, bytes_hash)

        if (json_schema or table_schema) and not as_json:
            raise Exception("json_schema or table_schema require as_json")

        if as_json:
            data = to_json(data)

            if json_schema is not None:
                validate_json_schema(data, json_schema)
            if table_schema is not None:
                validate_resource_schema(data, table_schema)

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
        table_schema: dict = None,
        metadata: dict = None,
    ) -> Report:
        if metadata and not self.supports_metadata:
            raise NotImplementedError("Location does not support writing of metadata")

        if bytes_hash is not None:
            data_bytes = to_bytes(data)
            validate_hash(data_bytes, bytes_hash)

        if json_schema or table_schema:
            data_json = to_json(data)
            if json_schema is not None:
                validate_json_schema(data_json, json_schema)
            if table_schema is not None:
                validate_resource_schema(data_json, table_schema)

        kwargs = {}
        if metadata:
            kwargs["metadata"] = metadata

        result = self._write(data, overwrite=overwrite, **kwargs)

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


class SqlLocation(Location):
    __slots__ = ["__query"]

    def __init__(self, uri):
        # remove query
        uri, query = uri.split("?")
        super().__init__(uri=uri)
        query = parse_query(query)
        self.__query = query

    def query_get_single(self, key):
        return get_single(self.__query, key)

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
        data = to_bytes(data)
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
        package = self.__load_datapackage_json()
        idx = self.__get_resource_index_by_name(package, self.name)
        resource = package["resources"][idx]
        if "data" in resource:
            return resource["data"]
        else:
            path = self.get_path(resource)
            return FileLocation(path).read()

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
        package_hash_data = to_bytes(package_hash_data)
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


def load_schema(uri):
    return location(uri).read(as_json=True)


def validate_json_schema(data, schema: str | dict | bool = True) -> object:
    if schema is True:
        schema = data["$schema"]
    if isinstance(schema, str):
        schema = load_schema(schema)

    jsonschema.validate(data, schema)
    logging.debug("Validation ok")

    return data


def validate_resource_schema(data, schema):
    if not schema:
        raise Exception("no schema")
    validate_resource({"data": data, "schema": schema})
    return data


def guess_data_schema(data):
    res = validate_resource({"data": data})
    tasks = res["tasks"]
    assert len(tasks) == 1
    schema = tasks[0]["resource"]["schema"]
    return schema


def validate_resource(resource):
    frictionless.Resource(descriptor=resource)
    res = frictionless.validate_resource(resource)
    if not res["valid"]:
        raise Exception(res)
    return res


class SchemaValidator:
    __slots__ = ["schema", "validator"]

    def __init__(self, schema):
        if isinstance(schema, str):
            schema = load_schema(schema)

        validator_cls = jsonschema.validators.validator_for(schema)
        # check if schema is valid
        validator_cls.check_schema(schema)
        self.validator = validator_cls(schema)

        self.schema = schema

    def validate(self, json):
        return self.validator.validate(json)


def bytes_load(filepath: str) -> bytes:
    with open(filepath, "rb") as file:
        return file.read()


def text_load(filepath: str, encoding="utf-8") -> str:
    datab = bytes_load(filepath=filepath)
    data = datab.decode(encoding=encoding)
    return data


def bytes_dump(filepath: str, data: bytes, overwrite=False):
    if os.path.isfile(filepath):
        if not overwrite:
            raise FileExistsError(os.path.abspath(filepath))
    else:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "wb") as file:
        return file.write(data)


def text_dump(filepath: str, data: str, overwrite=False):
    datab = data.encode()
    return bytes_dump(filepath=filepath, data=datab, overwrite=overwrite)


def json_loads(data: str):
    return json.loads(data)


def json_load(filepath: str, encoding="utf-8"):
    data_text = text_load(filepath, encoding=encoding)
    return json_loads(data_text)


def json_dumps(data, indent=2, sort_keys=True, ensure_ascii=False) -> str:
    return json.dumps(
        data, indent=indent, sort_keys=sort_keys, ensure_ascii=ensure_ascii
    )


def json_dump(
    data, filepath: str, indent=2, sort_keys=True, ensure_ascii=False, overwrite=False
):
    data_text = json_dumps(
        data, indent=indent, sort_keys=sort_keys, ensure_ascii=ensure_ascii
    )
    return text_dump(filepath, data_text, overwrite=overwrite)


def json_loadb(bytes_data: bytes, encoding: str = DEFAULT_ENCODING) -> object:
    str_data = bytes_data.decode(encoding=encoding)
    return json_loads(str_data)


def json_dumpb(data: object) -> bytes:
    return json_dumps(data).encode()


class NamedClosedTemporaryFile:
    def __init__(self, suffix=None, prefix=None, dir=None):
        self.suffix = suffix
        self.prefix = prefix
        self.dir = dir
        self.filepath = None

    def __enter__(self):
        file = tempfile.NamedTemporaryFile(
            dir=self.dir, suffix=self.suffix, prefix=self.prefix, delete=False
        )
        self.filepath = file.name
        file.close()
        assert os.path.isfile(self.filepath)
        return self.filepath

    def __exit__(self, *args):
        os.remove(self.filepath)


def text_normalize(name, allowed_chars="a-z0-9", sep="_"):

    name = unquote_plus(name)

    # manual replacements for german
    for cin, cout in [
        ("ä", "ae"),
        ("ö", "oe"),
        ("ü", "ue"),
        ("Ä", "Ae"),
        ("Ö", "Oe"),
        ("Ü", "Ue"),
        ("ß", "ss"),
    ]:
        name = name.replace(cin, cout)

    # maske ascii
    name = unidecode.unidecode(name)

    name = inflection.underscore(name)

    # lower case and remove all blocks of invalid characters
    name = name.lower()
    name = re.sub("[^" + allowed_chars + "]+", sep, name)
    name = name.rstrip(sep)

    return name


class JsonStore:
    def __init__(self, cache_location=None):
        cache_location = cache_location or app_dirs + "/jsonstore"
        logging.debug(os.path.abspath(cache_location))
        #: location for persitant storage
        self._cache_location = cache_location
        #: in memory cache
        self._cache = {}

    def __getitem__(self, key):
        if key not in self._cache:
            self._cache[key] = self._get(key)
        data = self._cache[key]
        data = deepcopy(data)
        return data

    def _get(self, key):
        """load from"""
        data = {"$ref": key}
        data_s = json.dumps(data)
        data = jsonref.loads(data_s, loader=self._load)
        return data

    def _load(self, path):
        local_path = self._get_local_path(path)
        logging.debug(f"loading from {local_path}")
        if not os.path.isfile(local_path):
            data = self._download(path)
            json_dump(data, local_path)
        data = json_load(local_path)
        return data

    def _download(self, url):
        logging.debug(f"downloading from {url}")
        schema = requests.get(url).json()
        return schema

    def _get_local_path(self, key):
        path = urlsplit(key)
        local_path = self._cache_location + "/" + (path.hostname or "") + path.path
        local_path = os.path.realpath(local_path)
        return local_path


def json_recursion(modify_value, obj):
    if isinstance(obj, dict):
        return dict((k, json_recursion(modify_value, v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return [json_recursion(modify_value, v) for v in obj]
    else:
        return modify_value(obj)


class JsonSchemaValidator:
    def __init__(self, schema):
        validator_cls = jsonschema.validators.validator_for(schema)
        validator_cls.check_schema(schema)
        self.validator = validator_cls(schema)

    def __call__(self, instance):
        self.validator.validate(instance)


class DatapackageDescriptor:
    def __init__(self, path):
        self.path = path


# --------------------------------------------------------------------------------
# functions for cli scripts
# --------------------------------------------------------------------------------


def cli_validate(file_path):
    raise Exception(file_path)
    logging.info(file_path)
