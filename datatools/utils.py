"""TODO"""

import codecs
from collections.abc import Callable, Iterable, Iterator
import csv
import datetime
from functools import cache, partial
import hashlib
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import importlib
import inspect
from inspect import Parameter, Signature
import io
from io import BufferedReader
import json
import logging
import math
import os
from pathlib import Path
import re
import site
import socket
import subprocess
import sys
import tempfile
from threading import Thread
import time
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast
from urllib.parse import unquote, urlparse, urlsplit
from urllib.request import url2pathname
import uuid

import chardet
from filelock import FileLock
import frictionless
import httpx
import jsonpath_ng
import jsonpath_ng.ext
import jsonschema
import jsonschema.validators
import numpy as np
import pandas as pd
from pydantic import BaseModel
import sqlalchemy as sa
import sqlparse
from typing_extensions import override
import tzlocal

from datatools.types import (
    DEFAULT_CHUNK_SIZE,
    LOCKFILE_SUFFIX,
    TEMPFILE_SUFFIX,
    ByteData,
    Json,
    StrPath,
    SubCls,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import CursorResult
    from sqlalchemy.engine.row import Row


DATETIMETZ_FMT = "%Y-%m-%dT%H:%M:%S%z"
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H:%M:%S"
ANONYMOUS_USER = "ANONYMOUS"


class TextFile:
    """TODO"""

    def __init__(
        self,
        path: str | Path,
        encoding="utf-8",
        errors: Literal["strict", "replace", "ignore"] = "strict",
        ensure_ascii=False,
        sort_keys=False,
        indent=2,
    ):
        self.path = Path(path)
        self.encoding = encoding
        self.errors = errors
        self.ensure_ascii = ensure_ascii
        self.sort_keys = sort_keys
        self.indent = indent

    def exists(self) -> bool:
        """TODO"""
        return self.path.exists()

    def load_bytes(self) -> bytes:
        """TODO"""
        logging.debug("Reading %s", self.path)
        with self.path.open("rb") as file:
            return file.read()

    def load_str(self) -> str:
        """TODO"""
        data_b = self.load_bytes()
        data_s = data_b.decode(encoding=self.encoding, errors=self.errors)
        return data_s

    def load_json(self) -> Any:
        """TODO"""
        data_s = self.load_str()
        return json_loads(data_s)

    def dump_bytes(self, data: bytes) -> None:
        """TODO"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        logging.debug("Writing %s", self.path)
        with self.path.open("wb") as file:
            file.write(data)

    def dump_str(self, data: str) -> None:
        """TODO"""
        data_b = data.encode(encoding=self.encoding, errors=self.errors)
        self.dump_bytes(data_b)

    def dump_json(self, data: Any) -> None:
        """TODO"""
        data_s = json_dumps(
            data,
            ensure_ascii=self.ensure_ascii,
            sort_keys=self.sort_keys,
            indent=self.indent,
        )
        self.dump_str(data_s)


def json_serialize(x: Any) -> Json:
    """TODO

    Example:

    >>> import json
    >>> import numpy as np
    >>> import datetime
    >>> from pydantic import BaseModel
    >>> json.dumps(datetime.date(2001,2,3), default=json_serialize)
    '"2001-02-03"'
    >>> json.dumps(datetime.time(4,5,6), default=json_serialize)
    '"04:05:06"'
    >>> json.dumps(datetime.datetime(2001,2,3,4,5,6), default=json_serialize)
    '"2001-02-03T04:05:06"'
    >>> json.dumps(np.nan, allow_nan=True, default=json_serialize)
    'NaN'
    >>> repr(json_serialize(np.nan))
    'None'
    >>> json.dumps(float('nan'), allow_nan=True, default=json_serialize)
    'NaN'
    >>> repr(json_serialize(float('nan')))
    'None'
    >>> json.dumps(np.int64(0), default=json_serialize)
    '0'
    >>> json_serialize(np.float64(0.5))
    0.5
    >>> json.dumps(np.bool(0), default=json_serialize)
    'false'
    >>> json.dumps(object(), default=json_serialize)
    Traceback (most recent call last):
    ...
    NotImplementedError:
    >>> class Test(BaseModel):
    ...    value: int
    >>> json.dumps(Test(value='10'), default=json_serialize)
    '{"value": 10}'


    """
    if isinstance(x, datetime.datetime):
        return x.strftime(DATETIMETZ_FMT)
    elif isinstance(x, datetime.date):
        return x.strftime(DATE_FMT)
    elif isinstance(x, datetime.time):
        return x.strftime(TIME_FMT)
    elif isna(x):
        # FIXME: when using json.dumps(default=json_serialize), nan will NOT
        # be forwarded to this function
        return None
    elif isinstance(x, np.bool_):
        return bool(x)
    elif np.issubdtype(type(x), np.integer):  # type:ignore
        return int(x)
    elif np.issubdtype(type(x), np.floating):  # type:ignore
        return float(x)
    elif isinstance(x, BaseModel):
        return x.model_dump(mode="json")
    else:
        raise NotImplementedError(f"{x.__class__}: {x}")


def str_dumpb(
    text: str,
    encoding="utf-8",
    errors: Literal["strict", "replace", "ignore"] = "strict",
) -> bytes:
    """TODO"""
    return text.encode(encoding=encoding, errors=errors)


def str_load(
    data: bytes,
    encoding="utf-8",
    errors: Literal["strict", "replace", "ignore"] = "strict",
) -> str:
    """TODO"""
    return data.decode(encoding=encoding, errors=errors)


def json_dumps(
    data: Any,
    ensure_ascii: bool = False,
    sort_keys: bool = False,
    indent: int = 2,
    default: Callable | None = json_serialize,
) -> str:
    """TODO"""
    return json.dumps(
        data,
        ensure_ascii=ensure_ascii,
        sort_keys=sort_keys,
        indent=indent,
        default=default,
    )


def json_loads(text: str) -> Json:
    """TODO"""
    return json.loads(text)


def json_loadb(
    data: ByteData,
    encoding="utf-8",
    errors: Literal["strict", "replace", "ignore"] = "strict",
) -> Json:
    """TODO"""
    # TODO: streaming
    bdata = as_bytes(as_byte_iterable(data))
    text = str_load(bdata, encoding=encoding, errors=errors)
    return json_loads(text)


def json_dumpb(
    data: Any,
    ensure_ascii: bool = False,
    sort_keys: bool = False,
    indent: int = 2,
    encoding="utf-8",
    errors: Literal["strict", "replace", "ignore"] = "strict",
) -> bytes:
    """TODO"""
    text = json_dumps(
        data,
        ensure_ascii=ensure_ascii,
        sort_keys=sort_keys,
        indent=indent,
    )
    return str_dumpb(text, encoding=encoding, errors=errors)


def parse_cmd_vals(arguments: list[str]) -> dict[str, Json]:
    """TODO"""
    items = [kv.split("=", 1) for kv in arguments]
    return {k: try_parse_json_str(v) for k, v in items}


def get_free_port() -> int:
    """TODO"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))  # 0 = let the OS choose
        port = s.getsockname()[1]
    return port


def file_uri_to_path(uri: str) -> Path:
    """TODO

    Example:

    >>> file_uri_to_path("file:///absolue/path").as_posix()
    '/absolue/path'
    >>> file_uri_to_path("file:///./relative/path").relative_to(os.getcwd()).as_posix()
    'relative/path'
    >>> file_uri_to_path("file://host/path").as_posix()
    Traceback (most recent call last):
        ...
    NotImplementedError:

    """
    parts = urlparse(uri)
    if parts.netloc:
        raise NotImplementedError(uri)
    elif parts.path.startswith("/./") or parts.path.startswith("/../"):
        # relative path (not standard file:// schema)
        path_s = Path(os.getcwd()).as_posix() + parts.path
    else:
        path_s = url2pathname(unquote(parts.path))
    path = Path(path_s)

    return path


def reverse_prints(
    stdout_data: Iterable[bytes],
    encoding: str = "utf-8",
    errors: str = "replace",
) -> Iterable[str]:
    r"""Streaming decode byte chunks into lines.

    Example:

    >>> list(reverse_prints([b'partial', b'line\nline2\r\npartial', b'line']))
    ['partialline', 'line2', 'partialline']

    """

    def strip_oel(x: str) -> str:
        return x.rstrip("\r\n")

    decoder = codecs.getincrementaldecoder(encoding)(errors)
    buffer = ""
    for chunk in stdout_data:
        buffer += decoder.decode(chunk)
        while (idx := buffer.find("\n")) != -1:
            yield strip_oel(buffer[: idx + 1])
            buffer = buffer[idx + 1 :]
    buffer += decoder.decode(b"", final=True)  # flush trailing partial sequence
    if buffer:
        yield strip_oel(buffer)  # last line, even without trailing \n


def try_parse_json_str(s: str) -> Any:
    """TODO

    Example:

    >>> try_parse_json_str("s")
    's'
    >>> try_parse_json_str('"s"')
    's'
    >>> try_parse_json_str(1)
    1

    """
    try:
        return json_loads(s)
    except Exception:
        return s


def is_file_uri_or_path(x: str | Path) -> bool:
    """TODO

    Example:

    >>> is_file_uri_or_path(Path("."))
    True
    >>> is_file_uri_or_path("file:///path")
    True
    >>> is_file_uri_or_path("http:///path")
    False

    """
    if isinstance(x, Path):
        return True
    return bool(re.match(r"file://", x)) or "://" not in x


def uri_or_path_to_path(x: str | Path) -> Path:
    """TODO

    Example:
    >>> from pathlib import Path
    >>> uri_or_path_to_path(Path(".")).as_posix()
    '.'
    >>> uri_or_path_to_path(".").resolve().relative_to(os.getcwd()).as_posix()
    '.'
    >>> uri_or_path_to_path("file:///./").relative_to(os.getcwd()).as_posix()
    '.'

    """
    if isinstance(x, Path):
        return x
    elif re.match(r"file://", x):
        return file_uri_to_path(x)
    else:
        return Path(x)


def function_get_defaults(func: Callable):
    """TODO"""
    sig = inspect.signature(func)
    return {
        name: param.default
        for name, param in sig.parameters.items()
        if param.default is not inspect._empty
    }


def function_has_varargs(func: Callable) -> bool:
    """TODO

    >>> def f():
    ...     pass
    >>> function_has_varargs(f)
    False
    >>> def f(a, b=1):
    ...     pass
    >>> function_has_varargs(f)
    False
    >>> def f(a, *args):
    ...     pass
    >>> function_has_varargs(f)
    True
    >>> def f(a, **kwargs):
    ...     pass
    >>> function_has_varargs(f)
    True
    """
    sig = inspect.signature(func)
    has_args = any(p.kind == Parameter.VAR_POSITIONAL for p in sig.parameters.values())
    has_kwargs = any(p.kind == Parameter.VAR_KEYWORD for p in sig.parameters.values())
    return has_args or has_kwargs


def function_get_regular_params(func: Callable) -> list[str]:
    """TODO"""
    # is this now possible?
    # if function_has_varargs(func):
    #     raise TypeError("Function cannot have *args or **kwargs")

    sig = inspect.signature(func)
    return list(sig.parameters)


def names_get_argument_dict(
    params: list[str], defaults: dict, *args, **kwargs
) -> dict[str, Any]:
    """TODO"""

    # we need to add defaults, otherwise sig.bind fails
    sig = Signature(
        [
            Parameter(name, Parameter.POSITIONAL_OR_KEYWORD, default=defaults.get(name))
            for name in params
        ]
    )

    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()

    result = bound.arguments

    # logging.error(("names_get_argument_dict", sig.parameters, args, kwargs, result))

    return result


def iter_subclasses(cls: type[SubCls]) -> Iterable[type[SubCls]]:
    """TODO"""
    yield cls
    for subcls in cls.__subclasses__():
        yield from iter_subclasses(subcls)


def subclasses_by_name(cls: type[SubCls]) -> dict[str, type[SubCls]]:
    """TODO"""
    return {c.__name__: c for c in list(iter_subclasses(cls))}


def get_sha256_hash(hash_data: Json) -> str:
    """TODO

    Example:

    >>> get_sha256_hash({"key": 10})
    '63fc351b588eec4fad18ef579b3c42c83d6638e0dc4a55f4772ff8a61455630d'

    """
    hash_data_s = json_dumps(
        hash_data, ensure_ascii=False, indent=0, sort_keys=True, default=json_serialize
    )
    hash_data_b = hash_data_s.encode("utf-8")
    hashsum = hashlib.sha256(hash_data_b).hexdigest()  # noqa:S324
    # logging.error("%s %s", hashsum, hash_data)
    return hashsum


def assert_unique(iterable: Iterable):
    """TODO

    Example:

    >>> assert_unique(range(10))
    >>> assert_unique(iter([1, 2, 1]))
    Traceback (most recent call last):
    ...
    KeyError:

    """
    uq = set()
    for x in iterable:
        if x in uq:
            raise KeyError(f"Duplicate key: {x}")
        uq.add(x)


def identity(x):
    """TODO"""
    return x


def jsonpath_update(data: dict[str, Json], key: str, val: Json) -> None:
    """TODO"""
    path = jsonpath_ng.ext.parse(key)
    path.update_or_create(data, val)


def jsonpath_get(data: dict[str, Json], key: str) -> list[Json]:
    """TODO

    Example:

    >>> jsonpath_get({"a": {"b": 1}}, "$.a.b")
    [1]
    >>> jsonpath_get({"a": {"b": 1}}, "a.b")
    [1]
    >>> jsonpath_get({"a": [{"b": 1}, {"b": 2}]}, "a[0].b")
    [1]
    >>> jsonpath_get({"a": [{"b": 1}, {"b": 2}]}, "a[*].b")
    [1, 2]
    >>> jsonpath_get({"a": [{"b": 1}, {"b": 2}]}, 'a[?(b > 1)].b')
    [2]

    """
    path = jsonpath_ng.ext.parse(key)
    match = path.find(data)
    values = [x.value for x in match]
    return values


def isna(x: Any) -> bool:
    """TODO

    Example:

    >>> import pandas as pd
    >>> import numpy as np
    >>> isna(0)
    False
    >>> isna("")
    False
    >>> isna(float("nan"))
    True
    >>> isna(float("inf"))
    True
    >>> isna(None)
    True
    >>> isna(pd.NA)
    True
    >>> isna(np.nan)
    True

    """
    return bool(
        x is None
        or isinstance(x, float)
        and (math.isnan(x) or math.isinf(x))
        or pd.isna(x)
    )


def get_now() -> datetime.datetime:
    """TODO"""
    # my local timezone, e.g.  DstTzInfo 'Europe/Berlin' LMT+0:53:00 STD
    tz_local = tzlocal.get_localzone()
    # timezone as current utc offset (does not know about dst),
    # e.g. datetime.timezone(datetime.timedelta(seconds=3600))
    now = datetime.datetime.now()
    # convert unaware datetime to proper timezone...
    now_tz = now.replace(tzinfo=tz_local)
    return now_tz


def get_now_str() -> str:
    """TODO"""
    now = get_now()
    now_str = now.strftime(DATETIMETZ_FMT)
    # add ":" in offset
    now_str = re.sub("([+-][0-9]{2})([0-9]{2})$", r"\1:\2", now_str)
    return now_str


@cache
def get_fqhostname() -> str:
    """fully qualified hostname (with domain)"""
    return socket.getfqdn()


@cache
def get_username() -> str:
    """TODO"""
    # getpass.getuser() does not always work
    return os.environ.get("USERNAME") or os.environ.get("USER") or ANONYMOUS_USER


@cache
def get_user_w_host() -> str:
    """TODO"""
    return f"{get_username()}@{get_fqhostname()}"


def start_http_server(
    directory: str | Path = ".", port: int | None = None, host: str = "127.0.0.1"
) -> str:
    """TODO"""
    port = port or get_free_port()
    server = ThreadingHTTPServer(
        (host, port),
        partial(SimpleHTTPRequestHandler, directory=directory),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://{host}:{port}"
    return url


def remove_credentials_from_netloc(netloc: str) -> str:
    """TODO

    Example:

    >>> remove_credentials_from_netloc('name:pw@example.com:80')
    'example.com:80'
    >>> remove_credentials_from_netloc('example.com:80')
    'example.com:80'

    """
    return re.sub("[^/@]+@", "", netloc)


def remove_port_from_netloc(netloc: str) -> str:
    """TODO

    Example:

    >>> remove_port_from_netloc('name:pw@example.com:80')
    'name:pw@example.com'
    >>> remove_port_from_netloc('name:pw@example.com')
    'name:pw@example.com'

    """
    if m := re.match(r"^(.*):[0-9]+$", netloc):
        netloc = m.groups()[0]
    return netloc


def get_name_from_uri(uri: str) -> str:
    """TODO"""
    parts = urlsplit(uri)
    netloc = remove_credentials_from_netloc(parts.netloc)
    netloc = remove_port_from_netloc(parts.netloc)
    path = parts.path
    name = f"{netloc.strip('/')}/{path.strip('/')}"
    name = name.strip("/")
    return name


@cache
def normalize_sql_query(query: str) -> str:
    """TODO

    Prettify an SQL query.

    Args:
        query (str): The SQL query to be prettified.

    Returns:
        str: The prettified SQL query.

    Example:

    >>> normalize_sql_query("select  1 as a;")
    'SELECT 1 AS a;'

    """
    query = sqlparse.format(  # type:ignore
        query,
        reindent=False,
        keyword_case="upper",
        strip_comments=True,
        strip_whitespace=True,
    )
    return query


def detect_encoding(sample_data: bytes) -> str:
    """TODO

    Example:

    >>> b = "Ünicöde".encode(encoding="windows-1252")
    >>> detect_encoding(b).lower()
    'windows-1252'

    """
    result = chardet.detect(sample_data)
    return str(result["encoding"])


def detect_csv_dialect(sample_data: str) -> dict[str, Any]:
    r"""TODO

    Example:

    >>> d = detect_csv_dialect('A;B;C\n1;2;3')
    >>> d["delimiter"]
    ';'


    """
    dialect = csv.Sniffer().sniff(sample_data)
    dialect_dict = {
        k: v
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
    }
    return dialect_dict


def get_sql_table_schema(result: "CursorResult") -> dict[str, Any]:
    """TODO

    Example:

    >>> import sqlalchemy as sa
    >>> eng = sa.create_engine('sqlite:///:memory:')
    >>> res = eng.connect().execute(sa.text(
    ... "select 1 as a, 'x' as b UNION select 2 as a, NULL as b"))
    >>> [x["name"] for x in get_sql_table_schema(res)["fields"]]
    ['a', 'b']

    """
    if result.cursor is None:
        raise Exception("Query returned nothing")  # pragma: no cover

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
        ) in result.cursor.description
    ]
    return {"fields": fields}


def get_df_table_schema(df: pd.DataFrame) -> dict[str, Any]:
    """TODO

    >>> from pandas import DataFrame
    >>> df = DataFrame([{"f1": 1, "f2": 2.2}, {"f1": 2}])
    >>> get_df_table_schema(df)
    {'fields': [{'name': 'f1', 'data_type': 'int64', 'is_nullable': False}, {'name': 'f2', 'data_type': 'float64', 'is_nullable': True}]}

    """  # noqa W501
    fields: list[dict[str, Any]] = []
    for cname in df.columns:
        fields.append(
            {
                "name": cname,
                "data_type": df[cname].dtype.name,
                "is_nullable": bool((df[cname].isna() | df[cname].isnull()).any()),
            }
        )
    return {"fields": fields}


@cache  # should not change during run
def get_git_info(repo_path: StrPath) -> dict[str, Any]:
    """TODO"""

    """get git branch,commit and is clean status
    from a local git repository, given as a path"""

    def run_git_command(command: list[str], cwd: StrPath):
        result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
        if result.returncode != 0:
            logging.warning(result.stderr.strip())  # pragma: no cover
            return None  # pragma: no cover
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


def get_function_description(function: Callable) -> str | None:
    """TODO"""
    return function.__doc__


def get_function_name(function: Callable) -> str:
    """TODO

    Example:

    >>> get_function_name(get_function_name)
    'get_function_name'


    """
    return function.__name__ or str(function)


def get_module(func: Callable) -> str | None:
    """TODO

    Example:

    >>> get_module(get_module)
    'datatools.utils'

    >>> get_module(open)
    'io'

    """
    mod = inspect.getmodule(func)
    if mod is not None:
        mod = mod.__name__
        # system mods sometimes start with "_"
        mod = mod.lstrip("_")

    return mod


def get_module_version(func: Callable) -> str | None:
    """TODO

    Example:

    >>> from datatools import __version__
    >>> v = get_module_version(get_module_version)
    >>> v == __version__
    True


    """
    # get module version (or parent module version)
    version = None
    try:
        mod = inspect.getmodule(func)
        mod_path = mod.__name__.split(".")  # type: ignore
        while mod_path and not version:
            mod_name = ".".join(mod_path)
            mod = importlib.import_module(mod_name)
            try:
                version = mod.__version__
                return version
            except AttributeError:
                pass
            mod_path = mod_path[:-1]
    except Exception:  # noqa: S110 # pragma: no cover
        pass  # pragma: no cover


def get_function_filepath(function: Callable) -> Path:
    """TODO"""
    try:
        return function.__file__
    except AttributeError:
        return Path(inspect.getfile(function))


def get_git_root(filepath: StrPath) -> Path:
    """TODO"""
    return next(path for path in Path(filepath).parents if (path / ".git").exists())


def get_function_git_id(fun: Callable) -> str:
    """TODO

    Example:


    >>> import re
    >>> name = get_function_git_id(get_function_git_id)
    >>> bool(re.match(
    ... r'^(git@|https://)github.com[:/]wingechr/datatools[./].*/datatools/utils.py',
    ... name)) or name
    True

    """
    filepath = get_function_filepath(fun).absolute()
    if any(str(filepath).startswith(p) for p in site.getsitepackages()):
        raise Exception("is site-package")

    git_root = get_git_root(filepath)
    git_info = get_git_info(git_root)
    if not git_info:
        raise Exception("git not found")  # pragma: no cover

    path = filepath.relative_to(git_root).as_posix()
    return f"{git_info['origin']}/{git_info['commit']}/{path}:{fun.__name__}"


def get_function_module_id(fun: Callable) -> str:
    """TODO

    Example:

    >>> get_function_module_id(get_function_module_id)
    'datatools.utils:get_function_module_id'

    """
    mod = get_module(fun)
    return f"{mod}:{fun.__name__}"


def is_lambda(fun: Callable):
    """TODO

    Example:

    >>> is_lambda(is_lambda)
    False
    >>> is_lambda(lambda x: x)
    True

    """
    return getattr(fun, "__name__", None) == "<lambda>"


def get_function_id(fun: Callable) -> str:
    """Example:

    >>> get_function_id(get_function_id).endswith('get_function_id')
    True
    >>> get_function_id(lambda x:x)
    '<lambda>'
    >>> from pandas import DataFrame
    >>> get_function_id(DataFrame).replace(".core.frame:", ":") # older pandas
    'pandas:DataFrame'
    >>> get_function_id(open)
    'io:open'

    """
    if is_lambda(fun):
        return "<lambda>"
    for get_id in [get_function_git_id, get_function_module_id, get_function_name]:
        try:
            return get_id(fun)
        except Exception:  # noqa: S112
            continue
    return str(fun)  # pragma: no cover # fallback


def wait_for_url(url: str, timeout_s=30):
    """TODO

    Example:

    >>> port = get_free_port()
    >>> wait_for_url(f"http://localhost:{port}", timeout_s=0.1)
    Traceback (most recent call last):
    ...
    TimeoutError:

    """
    t_start = time.time()

    while True:
        timeout_left = timeout_s + t_start - time.time()
        if timeout_left <= 0:
            raise TimeoutError()
        try:
            httpx.head(url, timeout=timeout_left)
            break
        except Exception:  # noqa: S112
            continue


def http_get_stream(
    uri: str, chunk_size: int = DEFAULT_CHUNK_SIZE, **options
) -> Iterable[bytes]:
    """TODO"""
    resp = httpx.get(uri, follow_redirects=True)
    resp.raise_for_status()
    yield from resp.iter_bytes(chunk_size=chunk_size)


def read_file_uri_stream(
    uri: str, chunk_size: int = DEFAULT_CHUNK_SIZE, **options
) -> Iterable[bytes]:
    """TODO"""
    path = uri_or_path_to_path(uri).resolve()
    with path.open("rb") as file:
        yield from buffer_to_byte_iterable(file, chunk_size=chunk_size)


def query_sql(uri: str, query: str, **options) -> Iterable["Row"]:
    """TODO"""
    eng = sa.create_engine(uri)
    with eng.connect() as con:
        resp = con.execute(sa.text(query))
        data = resp.fetchall()

    return data


def wrap_exception(
    function: Callable[[], None], debug: bool = True
):  # pragma: no cover - only called in __main__
    """TODO"""
    try:
        # your logic here
        function()
    except Exception as e:
        if debug:
            logging.exception(e)  # includes stack trace
        else:
            logging.error(e)
        sys.exit(1)


def sql_query_result_to_csv_bytes(data: Iterable["Row"], **options) -> Iterable[bytes]:
    """TODO"""
    df = pd.DataFrame(data)
    data_s = df.to_csv(index=False, lineterminator="\n")
    data_b = data_s.encode()
    # for now, we just return the whole thing.
    # since df i in memory anyways
    return [data_b]


def get_deterministic_uuid5(data: str) -> str:
    """TODO

    Example:

    >>> get_deterministic_uuid5("test")
    'da5b8893-d6ca-5c1c-9a9c-91f40a2a3649'

    """
    # NOTE: .hex() returns without the dashes, str() with dashes
    return str(uuid.uuid5(uuid.NAMESPACE_URL, data))


def get_deterministic_uuid5_from_data(data: Json) -> str:
    """TODO

    Example:

    >>> get_deterministic_uuid5_from_data({"value": 10})
    '88bc6da5-9229-5d47-ab0c-005c5f04030b'

    """
    data_s = json_dumps(data, indent=0, sort_keys=True)
    return get_deterministic_uuid5(data_s)


def validate_resource(resource_descriptor):
    """TODO"""
    res = frictionless.Resource(resource_descriptor)
    rep = res.validate()

    if rep.stats["errors"]:
        errors = []
        for task in rep.tasks:
            for err in task.errors:
                errors.append(err.message)

        err_str = "\n".join(errors)
        # logging.error(err_str)
        raise ValueError(err_str)


def get_jsonschema_validator(schema):
    """Return validator instance for schema.

    Example:

    >>> schema = {"type": "object", "properties":
    ...   {"id": {"type": "integer"}}, "required": [ "id" ]}
    >>> validator = get_jsonschema_validator(schema)
    >>> validator({})
    Traceback (most recent call last):
        ...
    ValueError: 'id' is a required property ...

    >>> validator({"id": "a"})
    Traceback (most recent call last):
        ...
    ValueError: 'a' is not of type 'integer' ...

    >>> validator({"id": 1})

    """

    if isinstance(schema, str):
        # FIXME: use webcache
        resp = httpx.get(schema, follow_redirects=True)
        resp.raise_for_status()
        schema = resp.json()

    validator_cls = jsonschema.validators.validator_for(schema)
    # check if schema is valid
    validator_cls.check_schema(schema)
    validator = validator_cls(schema)

    def validator_function(instance):
        errors = []
        for err in validator.iter_errors(instance):
            # path in data structure where error occurs
            path = "$" + "/".join(str(x) for x in err.absolute_path)
            errors.append(f"{err.message} in {path}")
        if errors:
            err_str = "\n".join(errors)
            # logging.error(err_str)
            raise ValueError(err_str)

    return validator_function


IterType = TypeVar("IterType")
Accumulator = TypeVar("Accumulator")
Value = TypeVar("Value")


class CollectStatsIterator(Generic[IterType, Accumulator, Value]):
    """pass"""

    def __init__(
        self,
        iterator: Iterable[IterType],
        initial_value: Accumulator,
        update_value: Callable[[Accumulator, IterType], Accumulator],
    ):
        self._source = iterator
        self._update_value = update_value
        self._value: Accumulator = initial_value

    def __iter__(self) -> Iterator[IterType]:
        self._iter = iter(self._source)
        return self

    def __next__(self) -> IterType:
        item = next(self._iter)
        self._value = self._update_value(self._value, item)
        return item

    @property
    def value(self) -> Value:
        """TODO"""
        return self._value  # type:ignore


class CollectStatsIteratorSize(CollectStatsIterator[bytes, int, int]):
    """TODO"""

    def __init__(
        self,
        iterator: Iterable[bytes],
    ):
        def update(acc: int, v: bytes) -> int:
            return acc + len(v)

        super().__init__(iterator, initial_value=0, update_value=update)


class CollectStatsIteratorHash(CollectStatsIterator[bytes, Any, str]):
    """TODO"""

    def __init__(self, iterator: Iterable[bytes], algorithm: Literal["md5", "sha256"]):
        accumulator = getattr(hashlib, algorithm)()

        def update(acc, v: bytes):
            acc.update(v)
            return acc

        super().__init__(iterator, initial_value=accumulator, update_value=update)

    @property
    def value(self) -> str:
        """TODO"""
        return self._value.hexdigest()


def as_byte_iterable(
    data: ByteData, chunk_size_if_buffer: int = DEFAULT_CHUNK_SIZE
) -> Iterable[bytes]:
    """TODO"""
    if isinstance(data, bytes):
        yield data
    elif isinstance(data, BufferedReader):
        yield from buffer_to_byte_iterable(data, chunk_size=chunk_size_if_buffer)
    elif isinstance(data, Iterable):
        yield from cast(Iterable[bytes], data)
    else:
        raise TypeError(f"Unexpected type: {type(data)}")


def as_bytes(data: Iterable[bytes]) -> bytes:
    """TODO"""
    return b"".join(c for c in data)


class IterableStream(io.RawIOBase):
    """Convert bytes iterator in read only buffer with lazy consumption"""

    def __init__(self, iterable: Iterable[bytes]):
        self._iter = iter(iterable)
        self._leftover = b""

    @override
    def readable(self) -> bool:
        return True

    @override
    def readinto(self, b: bytearray) -> int:
        if not self._leftover:
            try:
                self._leftover = next(self._iter)
            except StopIteration:
                return 0  # EOF
        n = len(b)
        chunk, self._leftover = self._leftover[:n], self._leftover[n:]
        b[: len(chunk)] = chunk
        return len(chunk)


def byte_iterable_as_buffer(iterable: Iterable[bytes]) -> BufferedReader:
    """TODO"""
    return BufferedReader(IterableStream(iterable))  # type:ignore


def buffer_to_byte_iterable(
    buf: BufferedReader, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> Iterator[bytes]:
    """TODO"""
    while chunk := buf.read(chunk_size):
        yield chunk


def write_bytes_locked(
    path: Path,
    data: Iterable[bytes],
    timeout: float = 30,
    lockfile_suffix: str = LOCKFILE_SUFFIX,
    tempfile_suffix: str = TEMPFILE_SUFFIX,
) -> None:
    """Write bytes to `path` atomically, guarded by a cross-platform file lock."""
    lock_path = path.with_name(path.name + lockfile_suffix)

    # raises filelock.Timeout if it can't acquire in time
    with FileLock(lock_path, timeout=timeout):
        fd, tmp_path = tempfile.mkstemp(
            dir=path.parent, prefix=path.name + ".", suffix=tempfile_suffix
        )
        # try:
        with os.fdopen(fd, "wb") as tmp_file:
            for chunk in data:
                tmp_file.write(chunk)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(tmp_path, path)  # atomic on both POSIX and Windows (Py 3.3+)
        # # we actually want to keep the tmp file in case we want to use the data
        # except BaseException:
        #     if os.path.exists(tmp_path):
        #         os.unlink(tmp_path)
        #     raise


def get_item_or_first(x):
    """TODO

    Example:

    >>> get_item_or_first(1)
    1
    >>> get_item_or_first([1])
    1
    >>> repr(get_item_or_first([]))
    'None'



    """
    if isinstance(x, list):
        if not x:
            return None
        return x[0]
    return x
