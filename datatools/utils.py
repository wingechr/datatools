import base64
import csv
import datetime
import functools
import importlib
import importlib.util
import inspect
import json
import logging
import math
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import time
from io import BufferedReader
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union, get_args
from urllib.parse import quote, unquote, unquote_plus, urlsplit

import chardet
import jsonpath_ng
import numpy as np
import pandas as pd
import requests
import sqlalchemy as sa
import sqlparse
import tzlocal
import unidecode
from jsonpath_ng import jsonpath
from pyodbc import Cursor

from datatools.base import (
    ANONYMOUS_USER,
    DATE_FMT,
    DATETIMETZ_FMT,
    DEFAULT_BUFFER_SIZE,
    FILEMOD_WRITE,
    LOCALHOST,
    PARAM_SQL_QUERY,
    TIME_FMT,
    ParameterKey,
    StrPath,
    Type,
)


def cache(func: Callable[..., Any]) -> Callable[..., Any]:
    """Update cache decorator that preserves metadata.

    Parameters
    ----------
    func : Callable
        original function

    Returns
    -------
    Callable
        cache decorated function
    """

    result = func
    result = functools.cache(func)
    # none of these work for IDE intellisense:
    result = functools.wraps(func)(result)
    # copy_signature(result, func)
    return result


@cache
def get_type_name(cls: Type) -> str:
    if cls is None:
        return "Any"
    if isinstance(cls, str):
        return cls
    if cls.__module__ == "typing":
        return str(cls)

    # remove leading underscore from module name
    modulename = str(cls.__module__).lstrip("_")
    classname = cls.__qualname__

    return f"{modulename}.{classname}"


@cache
def get_filetype_from_filename(filename: Union[Path, str]) -> Type:
    """returns something like .txt"""
    suffix = str(filename).split(".")[-1]
    return f".{suffix}"


def get_args_kwargs_from_dict(
    data: dict[ParameterKey, Any],
) -> tuple[list[Any], dict[str, Any]]:
    args_d: dict[int, Any] = {}
    kwargs: dict[str, Any] = {}
    if None in data:  # primitive: must be the only one
        args = [data[None]]
    else:
        for k, v in data.items():
            if isinstance(k, int):
                args_d[k] = v
            elif isinstance(k, str):
                kwargs[k] = v
            else:
                raise TypeError(k)
        if args_d:
            # fill missing positionals with None
            args = [args_d.get(i, None) for i in range(max(args_d) + 1)]
        else:
            args = []

    return args, kwargs


@cache
def get_value_type(dtype: Type) -> Type:
    # dict[Any, int] -> int
    # list[int] -> int
    return get_args(dtype)[-1]


def get_function_datatype(function: Callable[..., Any]) -> Type:
    sig = inspect.signature(function)
    return_type = sig.return_annotation
    if not return_type:
        return_type = None
    return return_type


def get_function_parameters_datatypes(function: Callable[..., Any]) -> dict[str, Any]:
    sig = inspect.signature(function)
    # hints = get_type_hints(function)  # does not work with my decorated classes
    # parameter_types = {
    #    name: hints.get(name, None) for name, param in sig.parameters.items()
    # }
    parameter_types = {name: param.annotation for name, param in sig.parameters.items()}
    return parameter_types


def get_keyword_only_parameters_types(
    function: Callable[..., Any], min_idx: int = 0
) -> list[str]:
    parameters = inspect.signature(function).parameters
    return [
        name
        for idx, (name, param) in enumerate(parameters.items())
        if idx >= min_idx
        and param.kind
        in {inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
    ]


def is_type_class(x: Any) -> bool:
    """Check if x is a type."""
    # return isinstance(x, type)
    if inspect.isclass(x):
        return True
    # special case: typping classes are not real classes
    if type(x).__module__ in {"typing", "types"}:
        return True
    return False


def isna(x: Any) -> bool:
    if x is None:
        return True
    elif isinstance(x, float):
        if math.isnan(x):
            return True
        elif math.isinf(x):
            return True

    return False


def json_serialize(x: Any) -> Any:
    if isinstance(x, datetime.datetime):
        return x.strftime(DATETIMETZ_FMT)
    elif isinstance(x, datetime.date):
        return x.strftime(DATE_FMT)
    elif isinstance(x, datetime.time):
        return x.strftime(TIME_FMT)
    elif isna(x):
        return None
    elif isinstance(x, np.bool_):
        return bool(x)
    elif np.issubdtype(type(x), np.integer):  # type:ignore
        return int(x)
    elif np.issubdtype(type(x), np.floating):  # type:ignore
        return float(x)
    elif is_type_class(x):
        return get_type_name(x)
    elif np.issubdtype(type(x), bytes):  # type:ignore
        # bytes to str:
        return base64.b64encode(x).decode("utf-8")
    else:
        raise NotImplementedError(f"{x.__class__}: {x}")


def jsonpath_update(data: dict[str, Any], key: str, val: Any) -> None:
    key_pattern: jsonpath.Fields = jsonpath_ng.parse(key)  # type: ignore
    # NOTE: for some reason, update_or_create in jsonpath_ng  does not
    # work with types that cannot be serialized to JSON
    try:
        val = json_serialize(val)
    except NotImplementedError:
        pass
    key_pattern.update_or_create(data, val)  # type: ignore


def jsonpath_get(data: dict[Any, Any], key: str) -> Union[Any, list[Any]]:
    key_pattern: jsonpath.Fields = jsonpath_ng.parse(key)  # type: ignore
    match: list[Any] = key_pattern.find(data)  # type: ignore
    values: list[Any] = [x.value for x in match]  # type: ignore
    # TODO: we always get a list (multiple matches),
    # but most of the time, we want only one
    if len(values) == 0:
        result = None
    elif len(values) == 1:
        result = values[0]
    else:
        logging.info("multiple results in metadata found for %s", key)
        result = values

    return result


def import_module_from_path(name: str, filepath: StrPath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module '{name}' from '{filepath}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def copy_signature(self: object, other: Callable[..., Any]) -> None:

    setattr(self, "__signature__", inspect.signature(other))
    setattr(self, "__name__", get_function_name(other))
    setattr(self, "__doc__", get_function_description(other))
    setattr(self, "__file__", get_function_filepath(other))
    setattr(self, "__annotations__", other.__annotations__)


def passthrough(x: Any) -> Any:
    return x


# ========================================================================================
# TODO: old, partially unused functions
# ========================================================================================


def get_pandas_version() -> tuple[int, ...]:
    return tuple(int(x) for x in pd.__version__.split("."))


@cache
def normalize_sql_query(query: str) -> str:
    """
    Prettify an SQL query.

    Args:
        query (str): The SQL query to be prettified.

    Returns:
        str: The prettified SQL query.
    """
    query = sqlparse.format(  # type:ignore
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


def wait_for_server(url: str, timeout_s: Optional[int] = 30) -> None:
    """Wait for server to be responsive"""
    wait_s = 0.5

    time_start = time.time()
    while True:
        try:
            requests.head(url)  # type: ignore
            logging.debug("Server is online: %s", url)
            return
        except requests.exceptions.ConnectionError:
            pass

        time_waited = time.time() - time_start
        logging.debug("checking server (%s): %s", time_waited, url)
        if timeout_s is not None and time_waited >= timeout_s:
            break
        time.sleep(wait_s)
    raise Exception("Timeout")


@cache
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


@cache
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


def get_query_arg(kwargs: dict[str, list[str]], key: str, default: str = "") -> str:
    """query args only come as lists, we want single value"""
    values = kwargs.get(key)
    if not values:
        return default
    if len(values) > 1:
        logging.warning("multiple values defined")
    value = values[0]
    return unquote_plus(value)


@cache
def get_hostname() -> str:
    return socket.gethostname()


@cache
def get_fqhostname() -> str:
    """fully qualified hostname (with domain)"""
    return socket.getfqdn()


@cache
def get_username() -> str:
    # getpass.getuser() does not always work
    return os.environ.get("USERNAME") or os.environ.get("USER") or ANONYMOUS_USER


@cache
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


@cache
def platform_is_windows() -> bool:
    # os.name: 'posix', 'nt', 'java'
    return os.name == "nt"


@cache
def platform_is_unix() -> bool:
    return not platform_is_windows()


def make_file_readonly(file_path: str) -> None:
    """Note: in WIndows, this also prevents delete
    but not in Linux
    """
    current_permissions = os.stat(file_path).st_mode
    readonly_permissions = current_permissions & ~FILEMOD_WRITE
    os.chmod(file_path, readonly_permissions)


def is_file_readonly(file_path: str) -> bool:
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


def remove_auth_from_uri_or_path(uri_or_path: str) -> str:
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


def parse_cli_metadata(metadata_key_vals: List[str]) -> dict[str, Any]:
    """cli: list of key=value"""
    metadata: dict[str, Any] = {}
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


def parse_content_type(ctype: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
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


def as_byte_iterator(
    data: Union[bytes, Iterable[bytes], BufferedReader, Any],
) -> Iterable[bytes]:
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
        yield from data  # type: ignore
    else:
        raise NotImplementedError(type(data))  # type: ignore


def detect_encoding(sample_data: bytes) -> str:
    result = chardet.detect(sample_data)
    if result["confidence"] < 1:
        logging.warning("Chardet encoding detection < 100%: %s", result)
    return str(result["encoding"])


def detect_csv_dialect(sample_data: str) -> dict[str, Any]:
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


def get_sql_table_schema(cursor: Cursor) -> dict[str, Any]:
    fields: list[dict[str, Any]] = [
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


def get_df_table_schema(df: pd.DataFrame) -> dict[str, Any]:
    fields: list[dict[str, Any]] = []
    for cname in df.columns:
        fields.append(
            {
                "name": cname,
                "data_type": df[cname].dtype.name,
                "is_nullable": (df[cname].isna() | df[cname].isnull()).any(),
            }
        )
    return {"fields": fields}


def delete_file(filepath: str) -> None:
    if not os.path.exists(filepath):
        return
    logging.debug("deleting %s", filepath)
    make_file_writable(file_path=filepath)
    os.remove(filepath)


def get_sql_uri(
    connection_string_uri: str, sql_query: str, fragment_name: Union[str, None] = None
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
    path = re.sub("[^a-z0-9-.]+", "/", path).split("/")[-1]
    return re.sub("^[^.]*", "", path)


def df_to_values(df: pd.DataFrame) -> list[Any]:
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
    df = df.astype(object)  # None values need object type # type:ignore
    df = df.where(~df.isna(), other=None)  # type:ignore
    data = df.to_dict(orient="records")  # type:ignore
    return data


def get_err_message(err: Exception) -> str:
    try:
        return err["message"]  # type:ignore
    except Exception:
        pass

    try:
        return err.message  # type:ignore
    except Exception:
        pass

    return str(err)


def sa_create_engine(connection_string: str) -> sa.Engine:
    kwargs = {}
    # on sql server: special argument, otherwise reflect does not work
    if connection_string.startswith("mssql+"):
        kwargs["use_setinputsizes"] = False
    engine = sa.create_engine(connection_string, echo=False, **kwargs)  # type:ignore
    return engine


def get_connection_string_uri_mssql_pyodbc(server: str, database: str = "master"):
    return f"mssql+pyodbc://?odbc_connect=driver=sql server;server={server};database={database}"  # noqa


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
    }.get(media_type, "")


@cache  # should not change during run
def get_git_info(repo_path: StrPath) -> dict[str, Any]:
    """get git branch,commit and is clean status
    from a local git repository, given as a path"""

    def run_git_command(command: list[str], cwd: StrPath):
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
        "origin": origin_url,
    }


def get_function_description(function: Callable[..., Any]) -> Union[str, None]:
    return function.__doc__


def get_function_name(function: Callable[..., Any]) -> Union[str, None]:
    return function.__name__


def get_module_version(func: Callable[..., Any]) -> Union[str, None]:
    # get module version (or parent module version)
    version = None
    try:
        mod = inspect.getmodule(func)
        mod_path = mod.__name__.split(".")  # type: ignore
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


def get_function_filepath(function: Callable[..., Any]) -> str:
    try:
        return getattr(function, "__file__")
    except AttributeError:
        return inspect.getfile(function)


def get_git_root(filepath: StrPath) -> Path:
    return next(path for path in Path(filepath).parents if (path / ".git").exists())


def rmtree_readonly(path: str) -> None:
    """Delete recursively (inckl. readonly)"""

    def delete_rw(action: Any, name: str, exc: Any):
        """action if shutil rm fails"""
        os.chmod(name, stat.S_IWRITE)
        os.remove(name)

    shutil.rmtree(path, onerror=delete_rw)


def is_callable(obj: Any) -> bool:
    return isinstance(obj, Callable)


def get_sqlite_connection_string(location: Optional[str] = None) -> str:
    if location is None or location == ":memory:":
        result = "sqlite:///:memory:"
    else:
        result = as_uri(location)
        # replace scheme and host
        result = re.sub("^file://[^/]*/", "sqlite:///", result)

    return result


def filepath_from_uri(file_uri: str) -> Path:
    # TODO: handle windows UNC paths
    parts = urlsplit(file_uri)
    path = parts.path
    # in windows (with drive name): drop leading path
    if re.match("^/[^/]+:", path):
        path = path.lstrip("/")
    return Path(path)


def get_sqlite_query_uri(
    location: Union[str, None] = None,
    sql_query: Union[str, None] = None,
    fragment_name: Union[str, None] = None,
) -> str:
    cs = get_sqlite_connection_string(location=location)
    return get_sql_uri(
        connection_string_uri=cs, sql_query=sql_query or "", fragment_name=fragment_name
    )


def get_uri_scheme(uri: str) -> str:
    return uri.split(":")[0] + ":"
