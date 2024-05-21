import atexit
import base64
import csv
import datetime
import functools
import hashlib
import importlib
import inspect
import io
import json
import logging
import os
import pickle
import re
import shutil
import socket
import stat
import subprocess
import sys
import time
from contextlib import ExitStack as _ExitStack
from io import BufferedReader, IOBase
from pathlib import Path
from typing import Any, Callable, Iterable, List, Tuple, Type, Union
from urllib.parse import quote, unquote, unquote_plus, urlsplit

import chardet
import jsonpath_ng
import numpy as np
import pandas as pd
import requests
import simplejson
import sqlalchemy as sa
import sqlparse
import tzlocal
import unidecode

from .constants import (
    ANONYMOUS_USER,
    DATE_FMT,
    DATETIMETZ_FMT,
    DEFAULT_BUFFER_SIZE,
    FILEMOD_WRITE,
    HASH_METHODS,
    LOCALHOST,
    PARAM_SQL_QUERY,
    TIME_FMT,
)


def get_pandas_version() -> tuple:
    return tuple(int(x) for x in pd.__version__.split("."))


class ExitStack(_ExitStack):
    __singleton_instance = None

    def __new__(cls, *args, **kwargs):
        if not cls.__singleton_instance:
            cls.__singleton_instance = super().__new__(cls, *args, **kwargs)

            # register __exit__ on normal exit
            atexit.register(cls.__singleton_instance.__exit__, None, None, None)

            # register on unhandled Exception exit
            sys.excepthook = cls.__singleton_instance.__exit__

        return cls.__singleton_instance

    def __exit__(self, exc_cls, exc_inst, exc_trace):
        # do regular cleanup
        super().__exit__(exc_cls, exc_inst, exc_trace)
        if exc_inst:
            raise exc_inst


def normalize_sql_query(query: str) -> str:
    """
    Prettify an SQL query.

    Args:
        query (str): The SQL query to be prettified.

    Returns:
        str: The prettified SQL query.
    """
    query = sqlparse.format(
        query,
        reindent=False,
        keyword_case="upper",
        strip_comments=True,
        strip_whitespace=True,
    )
    return query


def get_free_port() -> int:
    """Get a free port by binding to port 0 and releasing it.

    Returns:
        int: free port
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((LOCALHOST, 0))
    _, port = sock.getsockname()
    sock.close()
    return port


def wait_for_server(url, timeout_s=30) -> None:
    """Wait for server to be responsive"""
    wait_s = 0.5

    time_start = time.time()
    while True:
        try:
            requests.head(url)
            logging.debug("Server is online: %s", url)
            return True
        except requests.exceptions.ConnectionError:
            pass

        time_waited = time.time() - time_start
        logging.debug("checking server (%s): %s", time_waited, url)
        if timeout_s is not None and time_waited >= timeout_s:
            break
        time.sleep(wait_s)
    raise Exception("Timeout")


def uri_to_data_path(uri: str) -> str:
    url_parts = urlsplit(uri)

    # start with netloc
    path = url_parts.netloc or ""
    if path:
        path = remove_port_from_url_netloc(path)
        path = remove_auth_from_url_netloc(path)

    # add actual path
    path = path + (url_parts.path or "")

    # add fragment
    path = path + (url_parts.fragment or "")

    path = unquote_plus(path)

    return path


def clean_name(name: str) -> str:
    name = name.lower()
    name = unquote(name)
    for cin, cout in [
        ("ä", "ae"),
        ("ö", "oe"),
        ("ü", "ue"),
        ("ß", "ss"),
    ]:
        name = name.replace(cin, cout)
    name = unidecode.unidecode(name)
    name = re.sub(r":", "", name)
    name = re.sub(r"[^a-z0-9]+", " ", name)
    name = name.strip()
    name = re.sub(r"\s+", "_", name)
    return name


def get_resource_path_name(path: str) -> str:
    """should be all lowercase ascii
    * uri: remove query

    """
    _path = path

    path = path.lower()
    path = unquote(path)
    for cin, cout in [
        ("ä", "ae"),
        ("ö", "oe"),
        ("ü", "ue"),
        ("ß", "ss"),
    ]:
        path = path.replace(cin, cout)
    path = unidecode.unidecode(path)
    path = re.sub(r":", "", path)
    path = re.sub(r"[^a-z0-9/_.\-]+", " ", path)
    path = path.strip()
    path = re.sub(r"\s+", "_", path)
    path = re.sub(r"_+", "_", path)
    path = re.sub(r"/+", "/", path)
    path = path.strip("/")

    if not path:
        raise ValueError(_path)

    return path


def get_query_arg(kwargs: dict, key: str, default=None) -> str:
    """query args only come as lists, we want single value"""
    values = kwargs.get(key)
    if not values:
        return default
    if len(values) > 1:
        logging.warning("multiple values defined")
    value = values[0]
    return unquote_plus(value)


def get_hostname() -> str:
    return socket.gethostname()


def get_fqhostname() -> str:
    """fully qualified hostname (with domain)"""
    return socket.getfqdn()


def get_username() -> str:
    # getpass.getuser() does not always work
    return os.environ.get("USERNAME") or os.environ.get("USER") or ANONYMOUS_USER


def get_user_w_host() -> str:
    return f"{get_username()}@{get_fqhostname()}"


def get_now() -> datetime.datetime:
    # my local timezone, e.g.  DstTzInfo 'Europe/Berlin' LMT+0:53:00 STD
    tz_local = tzlocal.get_localzone()
    # timezone as current utc offset (does not know about dst),
    # e.g. datetime.timezone(datetime.timedelta(seconds=3600))
    now = datetime.datetime.now()
    # convert unaware datetime to proper timezone...
    now_tz = now.replace(tzinfo=tz_local)
    return now_tz


def get_now_str() -> str:
    now = get_now()
    now_str = now.strftime(DATETIMETZ_FMT)
    # add ":" in offset
    now_str = re.sub("([+-][0-9]{2})([0-9]{2})$", r"\1:\2", now_str)
    return now_str


def platform_is_windows() -> bool:
    # os.name: 'posix', 'nt', 'java'
    return os.name == "nt"


def platform_is_unix() -> bool:
    return not platform_is_windows()


def make_file_readonly(file_path: str) -> None:
    """Note: in WIndows, this also prevents delete
    but not in Linux
    """
    current_permissions = os.stat(file_path).st_mode
    readonly_permissions = current_permissions & ~FILEMOD_WRITE
    os.chmod(file_path, readonly_permissions)


def is_file_readonly(file_path: str) -> None:
    current_permissions = os.stat(file_path).st_mode
    return not (current_permissions & FILEMOD_WRITE)


def make_file_writable(file_path: str) -> None:
    current_permissions = os.stat(file_path).st_mode
    readonly_permissions = current_permissions | FILEMOD_WRITE
    os.chmod(file_path, readonly_permissions)


def is_uri(source: str):
    return bool(re.match(".+://", source))


def as_uri(source: str) -> str:
    if not is_uri(source):
        # assume local path
        # uri must be absolute path
        filepath_abs = Path(source).absolute()
        uri = filepath_abs_to_uri(filepath_abs=filepath_abs)
    else:
        uri = source
    return uri


def filepath_abs_to_uri(filepath_abs: Path) -> str:
    """
    Args:
        abspath(Path): must be already absolute path!
    """
    uri = filepath_abs.as_uri()
    url_parts = urlsplit(uri)

    if not url_parts.netloc:
        url_parts = url_parts._replace(netloc=get_hostname())

    uri = url_parts.geturl()  # unsplit

    # we dont want it quoted
    uri = unquote(uri)

    return uri


def uri_to_filepath_abs(uri: str) -> str:
    url = urlsplit(uri)

    # if url.scheme != "file":
    #    raise Exception(f"Not a file path: {uri}")

    is_local = url.netloc == get_hostname()
    is_win = re.match("/[a-zA-Z]:/", url.path) or (not is_local)

    filepath_abs = url.path
    if is_win:
        if is_local:
            # remove starting /
            filepath_abs = filepath_abs.lstrip("/")

        else:  # unc share
            filepath_abs = f"//{url.netloc}{filepath_abs}"
        filepath_abs = filepath_abs.replace("/", "\\")
    else:  # posix
        if not is_local:
            raise NotImplementedError(f"unc share in posix: {uri}")
        pass

    return filepath_abs


def remove_auth_from_uri_or_path(uri_or_path):
    """remove username:password@ from uri"""
    if not is_uri(uri_or_path):
        return uri_or_path
    url_parts = urlsplit(uri_or_path)
    if url_parts.netloc:
        url_parts = url_parts._replace(
            netloc=remove_auth_from_url_netloc(url_parts.netloc)
        )
    if not url_parts.netloc:
        # usually, netloc is empty, and so geturl() drops the "//"" at the beginning
        url_parts = url_parts._replace(path="//" + url_parts.path)
    return url_parts.geturl()


def remove_auth_from_url_netloc(url_netloc: str) -> str:
    """

    url_path(str): path part of url
    """
    return re.sub("[^/@]+@", "", url_netloc)


def remove_port_from_url_netloc(url_netloc: str) -> str:
    """

    url_path(str): path part of url
    """
    return re.sub(":[0-9]+$", "", url_netloc)


def parse_cli_metadata(metadata_key_vals: List[str]) -> dict:
    """cli: list of key=value"""
    metadata = {}
    for key_value in metadata_key_vals:
        parts = key_value.split("=")
        key = parts[0]
        value = "=".join(parts[1:])
        key = key.strip()
        value = value.strip()
        try:
            value = json.loads(value)
        except Exception:
            pass
        metadata[key] = value
    return metadata


def parse_content_type(ctype: str) -> dict:
    result = {}
    parts = [x.strip() for x in ctype.split(";")]
    result["mediatype"] = parts[0]
    for key_value in parts[1:]:
        try:
            key, value = key_value.split("=")
            key = key.strip()
            key = {"charset": "encoding"}.get(key, key)
            value = value.strip()
            result[key] = value
        except Exception:
            logging.warning("cannot parse %s", key_value)

    return result


def json_serialize(x):
    if isinstance(x, datetime.datetime):
        return x.strftime(DATETIMETZ_FMT)
    elif isinstance(x, datetime.date):
        return x.strftime(DATE_FMT)
    elif isinstance(x, datetime.time):
        return x.strftime(TIME_FMT)
    elif isinstance(x, np.bool_):
        return bool(x)
    elif np.issubdtype(type(x), np.integer):
        return int(x)
    elif np.issubdtype(type(x), np.floating):
        return float(x)
    elif x == inspect._empty or pd.isna(x):
        # various NULL types
        return None
    elif isinstance(x, pd.Series):
        # classname
        x = x.astype("object")
        x = x.where(pd.notnull(x), None)
        return dict(x)
    elif inspect.isclass(x):
        # classname
        return x.__name__
    elif np.issubdtype(type(x), bytes):
        # bytes to str:
        return base64.b64encode(x).decode("utf-8")
    else:
        raise NotImplementedError(f"{x.__class__}: {x}")


def as_byte_iterator(data: Union[bytes, Iterable, BufferedReader]) -> Iterable[bytes]:
    if isinstance(data, bytes):
        yield data
    elif isinstance(data, BufferedReader):
        while True:
            chunk = data.read(DEFAULT_BUFFER_SIZE)
            logging.debug("read %s bytes", len(chunk))
            if not chunk:
                break
            yield chunk
        try:
            data.close()
        except Exception as exc:
            logging.warning("could not close BufferedReader: %s", exc)
    elif isinstance(data, Iterable):
        yield from data
    else:
        raise NotImplementedError(type(data))


def detect_encoding(sample_data: bytes) -> str:
    result = chardet.detect(sample_data)
    if result["confidence"] < 1:
        logging.warning("Chardet encoding detection < 100%: %s", result)
    return result["encoding"]


def detect_csv_dialect(sample_data: str) -> dict:
    dialect = csv.Sniffer().sniff(sample_data)
    dialect_dict = dict(
        (k, v)
        for k, v in dialect.__dict__.items()
        if k
        in [
            "lineterminator",
            "quoting",
            "doublequote",
            "delimiter",
            "quotechar",
            "skipinitialspace",
        ]
    )
    return dialect_dict


def get_sql_table_schema(cursor):
    fields = [
        {"name": name, "data_type": data_type, "is_nullable": is_nullable}
        for (
            name,
            data_type,
            _display_size,
            _internal_size,
            _precision,
            _scale,
            is_nullable,
        ) in cursor.description
    ]
    return {"fields": fields}


def get_df_table_schema(df: pd.DataFrame):
    fields = []
    for cname in df.columns:
        fields.append(
            {
                "name": cname,
                "data_type": df[cname].dtype.name,
                "is_nullable": (df[cname].isna() | df[cname].isnull()).any(),
            }
        )
    return {"fields": fields}


def delete_file(filepath: str) -> str:
    if not os.path.exists(filepath):
        return
    logging.debug("deleting %s", filepath)
    make_file_writable(file_path=filepath)
    os.remove(filepath)


class ByteSerializer:
    mediatype = None
    suffix = None

    def dumps(data: object, **kwargs) -> bytes:
        raise NotImplementedError()

    def loads(data: bytes, **kwargs) -> object:
        raise NotImplementedError()


class CsvSerializer(ByteSerializer):
    mediatype = "text/csv"
    suffix = ".csv"

    def dumps(
        self,
        data: object,
        encoding="utf-8",
        sep=",",
        lineterminator="\n",
        **kwargs,
    ) -> bytes:
        buf = io.BytesIO()
        to_csv_kwargs = {
            "index": False,
            "encoding": encoding,
            "sep": sep,
            "lineterminator": lineterminator,
        }
        if get_pandas_version() < (1, 5):
            to_csv_kwargs["line_terminator"] = to_csv_kwargs.pop("lineterminator")

        pd.DataFrame(data, **kwargs).to_csv(
            buf,
            **to_csv_kwargs,
            **kwargs,
        )

        bdata = buf.getvalue()
        return bdata

    def loads(self, data: bytes, encoding="utf-8", sep=",", **kwargs) -> object:
        buf = io.BytesIO(data)
        return pd.read_csv(buf, encoding=encoding, sep=sep, **kwargs)


class JsonSerializer(ByteSerializer):
    mediatype = "application/json"
    suffix = ".json"

    def dumps(self, data: object, **kwargs) -> bytes:
        return json_dumps(data, **kwargs).encode()

    def loads(self, data: bytes, **kwargs) -> object:
        return json.loads(data, **kwargs)


class PickleSerializer(ByteSerializer):
    mediatype = "application/x-pickle"
    suffix = ".pickle"

    def dumps(self, data: object, **kwargs) -> bytes:
        return pickle.dumps(data, **kwargs)

    def loads(self, data: bytes, **kwargs) -> object:
        return pickle.loads(data, **kwargs)


def get_sql_uri(
    connection_string_uri: str, sql_query: str, fragment_name: str = None
) -> str:
    sql_query = normalize_sql_query(sql_query)

    if "?" in connection_string_uri:
        connection_string_uri += "&"
    else:
        connection_string_uri += "?"
    uri = connection_string_uri + PARAM_SQL_QUERY + "=" + quote(sql_query)
    if fragment_name:
        uri = f"{uri}#{fragment_name}"
    return uri


def get_suffix(path: str) -> str:
    suffix = ""
    while True:
        path, ext = os.path.splitext(path)
        if not ext:
            break
        suffix = ext + suffix
    return suffix


def get_byte_serializer(suffix: str) -> ByteSerializer:
    if suffix == ".json":
        return JsonSerializer()
    elif suffix == ".csv":
        return CsvSerializer()
    elif suffix in (".pickle", ".pkl"):
        return PickleSerializer()
    else:
        raise KeyError(suffix)


def df_to_values(df: pd.DataFrame) -> list:
    """get values from DataFrame, replace nans with None

    Parameters
    ----------
    df : pd.DataFrame
        data

    Returns
    -------
    list
        list of dicts
    """
    df = df.astype(object)  # None values need object type
    df = df.where(~df.isna(), other=None)
    data = df.to_dict(orient="records")
    return data


def get_err_message(err: Exception) -> str:
    try:
        return err["message"]
    except Exception:
        pass

    try:
        return err.message
    except Exception:
        pass

    return str(err)


def sa_create_engine(connection_string: str) -> sa.Engine:
    kwargs = {}
    # on sql server: special argument, otherwise reflect does not work
    if connection_string.startswith("mssql+"):
        kwargs["use_setinputsizes"] = False
    engine = sa.create_engine(connection_string, echo=False, **kwargs)
    return engine


def get_connection_string_uri_mssql_pyodbc(server, database="master"):
    return f"mssql+pyodbc://?odbc_connect=driver=sql server;server={server};database={database}"  # noqa


class BytesIteratorBuffer(IOBase):
    def __init__(self, bytes_iter) -> None:
        self.bytes_iter = bytes_iter

        self.buffer = b""
        self.bytes = 0

    def read(self, n=None):
        # read more data

        while True:
            if n is not None and n <= self.bytes:
                # if bytesare specified and we have enough data
                break
            try:
                chunk = next(self.bytes_iter)
            except StopIteration:
                break
            self.buffer += chunk
            self.bytes += len(chunk)

        # how many do we return
        n = self.bytes if n is None else min(n, self.bytes)
        # split buffer
        data = self.buffer[:n]
        self.buffer = self.buffer[n:]
        return data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class ByteBufferWrapper:
    def __init__(self, buffer):
        self.buffer = buffer

        self.bytes = None
        self.hashers = {}

    def __enter__(self):
        self.buffer.__enter__()
        self.bytes = 0
        for method in HASH_METHODS:
            self.hashers[method] = getattr(hashlib, method)()

        return self

    def __exit__(self, *args):
        self.buffer.__exit__(*args)

    def read(self, n=None):
        chunk = self.buffer.read(n)

        self.bytes += len(chunk)
        for hasher in self.hashers.values():
            hasher.update(chunk)

        return chunk


def get_default_media_data_type_by_name(name: str) -> Tuple[str, Type]:
    # defaults, only dependent on name (suffix)
    if re.match(r"^.*\.json$", name):
        return ("application/json", object)
    elif re.match(r"^.*\.(pkl|pickle)$", name):
        return ("application/x-pickle", object)
    elif re.match(r"^.*\.(csv)$", name):
        return ("text/csv", list)
    # default: binary
    return ("application/octet-stream", bytes)


def get_default_suffix(media_type: str) -> str:
    return {
        "application/json": ".json",
        "application/x-pickle": ".pickle",
        "text/csv": ".csv",
    }.get(media_type)


def get_git_info(repo_path: str) -> dict:
    """get git branch,commit and is clean status
    from a local git repository, given as a path"""

    def run_git_command(command, cwd):
        result = subprocess.run(
            command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if result.returncode != 0:
            logging.warning(result.stderr.strip())
            return None
        return result.stdout.strip()

    # Get the current branch name
    branch = run_git_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], repo_path)

    # Get the latest commit hash
    commit = run_git_command(["git", "rev-parse", "HEAD"], repo_path)

    # Get the origin remote URL
    origin_url = run_git_command(
        ["git", "config", "--get", "remote.origin.url"], repo_path
    )

    # Check if the repository is clean
    status = run_git_command(["git", "status", "--porcelain"], repo_path)
    is_clean = None if status is None else len(status) == 0
    if not is_clean:
        logging.warning("git repo is not clean")

    return {
        "branch": branch,
        "commit": commit,
        "is_clean": is_clean,
        "origin": origin_url,
    }


def get_function_info(obj: Callable) -> dict:
    if isinstance(obj, functools.partial):
        func = obj.func
        bound_args = obj.args
        # copy: do not change the original bound arguments
        bound_kwargs = obj.keywords.copy()
    else:
        func = obj
        bound_args = ()
        bound_kwargs = {}

    # get closure/context
    if func.__closure__:
        for pname, cell in zip(func.__code__.co_freevars, func.__closure__):
            # ignore callables
            # in decorated function, the base function is one of these parameters
            # and there is no way of separating it from function arguments that
            # are also functions.
            # But we want serializable data only anyways, so drop all
            value = cell.cell_contents
            if isinstance(value, Callable):
                continue
            bound_kwargs[pname] = value

    # get bound parameters
    kwargs = bound_kwargs
    for i, param in enumerate(inspect.signature(func).parameters.values()):
        # TODO: use param.kind  # inspect.Parameter.POSITIONAL_OR_KEYWORD | POSITIONAL_ONLY | KEYWORD_ONLY # noqa
        if param.name in kwargs:
            continue
        elif i < len(bound_args):  # map args => named kwargs
            value = bound_args[i]
        else:  # default value
            value = param.default

        kwargs[param.name] = value

    # only return json serializable values, otherwise
    # a string of the data class
    def _serialize(x):
        try:
            return json.loads(json_dumps(x))
        except Exception:
            return f"{type(x)}"

    kwargs = {k: _serialize(v) for k, v in kwargs.items()}

    func_name = func.__name__
    doc = inspect.cleandoc(inspect.getdoc(func) or "")
    file = inspect.getfile(func)

    # get module version (or parent module version)
    version = None
    try:
        mod_path = inspect.getmodule(func).__name__.split(".")
        while mod_path and not version:
            mod_name = ".".join(mod_path)
            mod = importlib.import_module(mod_name)
            try:
                version = getattr(mod, "__version__")
                version = f"{mod_name} {version}"
            except AttributeError:
                pass
            mod_path = mod_path[:-1]
    except Exception:
        pass

    # get git info
    git_info = None
    # find git repository
    for path in Path(file).parents:
        if (path / ".git").exists():
            try:
                git_info = get_git_info(str(path))
            except Exception:
                pass
            break

    result = {
        "name": func_name,
        "kwargs": kwargs,
        "doc": doc,
        "file": file,
        "version": version,
        "git": git_info,
    }
    return result


def rmtree_readonly(path: str) -> None:
    """Delete recursively (inckl. readonly)"""

    def delete_rw(action, name, exc):
        """action if shutil rm fails"""
        os.chmod(name, stat.S_IWRITE)
        os.remove(name)

    shutil.rmtree(path, onerror=delete_rw)


def is_callable(obj: Any) -> bool:
    return isinstance(obj, Callable)


def get_sqlite_connection_string(location=None) -> str:
    if location is None or location == ":memory:":
        result = "sqlite:///:memory:"
    else:
        result = as_uri(location)
        # replace scheme and host
        result = re.sub("^file://[^/]*/", "sqlite:///", result)

    return result


def get_sqlite_query_uri(
    location: str = None, sql_query: str = None, fragment_name: str = None
) -> str:
    cs = get_sqlite_connection_string(location=location)
    return get_sql_uri(
        connection_string_uri=cs, sql_query=sql_query, fragment_name=fragment_name
    )


def json_dumps(*args, default=json_serialize, **kwargs):
    return simplejson.dumps(
        *args,
        **kwargs,
        default=default,
        # simplejson: If ignore_nan is true (default: False), then out of range float
        # values (nan, inf, -inf) will be serialized as null in compliance with
        # the ECMA-262 specification. If true, this will override allow_nan.
        ignore_nan=True,
    )


def jsonpath_update(data: dict, key: str, val: Any) -> None:
    key_pattern = jsonpath_ng.parse(key)
    key_pattern.update_or_create(data, val)


def jsonpath_get(data: dict, key) -> Any:
    key_pattern = jsonpath_ng.parse(key)
    match = key_pattern.find(data)
    return match
