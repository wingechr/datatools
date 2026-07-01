"""TODO"""

from collections.abc import Callable, Iterable
import csv
import datetime
from functools import cache, partial
import hashlib
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import importlib
import importlib.util
import inspect
from inspect import Parameter, Signature
import json
import logging
import math
import os
from pathlib import Path
import pickle
import re
import socket
import subprocess
import subprocess as sp
import sys
from threading import Thread
from typing import Any, Literal
from urllib.parse import unquote, urlparse, urlsplit
from urllib.request import url2pathname

import chardet
import jsonpath_ng
import numpy as np
import pandas as pd
from pyodbc import Cursor
import sqlparse
import tzlocal

from datatools.types import Json, StrPath, SubCls, SubprocessStatus

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
        return json.loads(data_s)

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
        data_s = json.dumps(
            data,
            ensure_ascii=self.ensure_ascii,
            sort_keys=self.sort_keys,
            indent=self.indent,
        )
        self.dump_str(data_s)


def find_subclass(base_cls, name: str):
    """TODO"""
    for cls in base_cls.__subclasses__():
        if cls.__name__ == name:
            return cls
        found = find_subclass(cls, name)
        if found:
            return found
    return None


def wrap_exception(function: Callable[[], None], debug: bool = True):
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
    """TODO"""
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


def reverse_prints(stdout_data: bytes) -> list[str]:
    """TODO"""
    text = stdout_data.decode(sys.stdout.encoding, errors="replace")
    lines = text.splitlines(keepends=False)[::-1]
    return lines


def try_parse_json_str(s: str) -> Any:
    """TODO"""
    try:
        return json.loads(s)
    except Exception:
        return s


def is_file_uri_or_path(x: str | Path) -> bool:
    """TODO"""
    if isinstance(x, Path):
        return True
    return bool(re.match(r"file://", x)) or "://" not in x


def uri_or_path_to_path(x: str | Path) -> Path:
    """TODO"""
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
    """TODO"""
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


def function_get_argument_dict(f: Callable, *args, **kwargs) -> dict[str, Any]:
    """TODO"""
    sig = inspect.signature(f)
    bound = sig.bind(*args, **kwargs)  # or bind_partial()
    bound.apply_defaults()
    return bound.arguments


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


def get_md5_hash(hash_data: Json) -> str:
    """TODO"""
    hash_data_s = json.dumps(hash_data, ensure_ascii=False, indent=0, sort_keys=True)
    hash_data_b = hash_data_s.encode("utf-8")
    hashsum = hashlib.md5(hash_data_b).hexdigest()  # noqa:S324
    # logging.error("%s %s", hashsum, hash_data)
    return hashsum


def assert_unique(iterable: Iterable):
    """TODO"""
    uq = set()
    for x in iterable:
        if x in uq:
            raise KeyError("Duplicate key: %s", x)
        uq.add(x)


def pickle_dump_to_path(data: Any, path: Path) -> None:
    """TODO"""
    path.parent.mkdir(exist_ok=True, parents=True)
    with path.open("wb") as file:
        return pickle.dump(data, file)


def pickle_load_from_path(path: Path) -> Any:
    """TODO"""
    with path.open("rb") as file:
        return pickle.load(file)  # noqa:S301


def identity(x):
    """TODO"""
    return x


def call_script(
    script: Path | str, args: list[str], data: bytes | None = None
) -> tuple[bytes, bytes]:
    """Call python script."""
    cmd = [sys.executable, str(script)] + list(args)
    logging.debug(cmd)
    pop = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE)
    stdout, stderr = pop.communicate(data)
    if pop.returncode:
        raise SubprocessStatus(pop.returncode)
    return stdout, stderr


def jsonpath_update(data: dict[str, Json], key: str, val: Json) -> None:
    """TODO"""
    path = jsonpath_ng.parse(key)
    path.update_or_create(data, val)


def jsonpath_get(data: dict[str, Json], key: str) -> list[Json]:
    """TODO"""
    path = jsonpath_ng.parse(key)
    match = path.find(data)
    values = [x.value for x in match]
    return values


def isna(x: Any) -> bool:
    """TODO"""
    return bool(x is None or isinstance(x, float) and (math.isnan(x) or math.isinf(x)))


def json_serialize(x: Any) -> Json:
    """TODO"""
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
    else:
        raise NotImplementedError(f"{x.__class__}: {x}")


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
def get_hostname() -> str:
    """TODO"""
    return socket.gethostname()


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


def json_dumps_for_print(data: Json) -> str:
    """TODO"""
    return json.dumps(data, ensure_ascii=False)


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
    """FIXME: not implemented yet"""
    return re.sub("[^/@]+@", "", netloc)


def remove_port_from_netloc(netloc: str) -> str:
    """FIXME: not implemented yet"""
    if m := re.match(r"^(.*):[0-9]+$", netloc):
        netloc = m.groups()[0]
    return netloc


def get_uid_from_uri(uri: str) -> str:
    """TODO"""
    parts = urlsplit(uri)
    netloc = remove_credentials_from_netloc(parts.netloc)
    netloc = remove_port_from_netloc(parts.netloc)
    path = parts.path
    name = f"{netloc.strip('/')}/{path.strip('/')}"
    name = name.strip("/")
    return name


def import_module_from_path(name: str, filepath: StrPath):
    """TODO"""
    spec = importlib.util.spec_from_file_location(name, filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module '{name}' from '{filepath}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@cache
def normalize_sql_query(query: str) -> str:
    """TODO"""
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


def detect_encoding(sample_data: bytes) -> str:
    """TODO"""
    result = chardet.detect(sample_data)
    if result["confidence"] < 1:
        logging.warning("Chardet encoding detection < 100%: %s", result)
    return str(result["encoding"])


def detect_csv_dialect(sample_data: str) -> dict[str, Any]:
    """TODO"""
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


def get_sql_table_schema(cursor: Cursor) -> dict[str, Any]:
    """TODO"""
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
    """TODO"""
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


@cache  # should not change during run
def get_git_info(repo_path: StrPath) -> dict[str, Any]:
    """TODO"""

    """get git branch,commit and is clean status
    from a local git repository, given as a path"""

    def run_git_command(command: list[str], cwd: StrPath):
        result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
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


def get_function_description(function: Callable[..., Any]) -> str | None:
    """TODO"""
    return function.__doc__


def get_function_name(function: Callable[..., Any]) -> str:
    """TODO"""
    return function.__name__ or str(function)


def get_module(func: Callable[..., Any]) -> str | None:
    """TODO"""
    mod = inspect.getmodule(func)
    return mod.__name__ if mod else None


def get_module_version(func: Callable[..., Any]) -> str | None:
    """TODO"""
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
                version = f"{mod_name} {version}"
            except AttributeError:
                pass
            mod_path = mod_path[:-1]
    except Exception:  # noqa: S110
        pass


def get_function_filepath(function: Callable[..., Any]) -> Path:
    """TODO"""
    try:
        return function.__file__
    except AttributeError:
        return Path(inspect.getfile(function))


def get_git_root(filepath: StrPath) -> Path:
    """TODO"""
    return next(path for path in Path(filepath).parents if (path / ".git").exists())


def get_function_git_id(fun: Callable) -> str:
    """TODO"""
    filepath = get_function_filepath(fun)
    git_root = get_git_root(filepath)
    git_info = get_git_info(git_root)
    path = filepath.relative_to(git_root)
    return f"{git_info['origin']}/{git_info['commit']}/{path}:{fun.__name__}"


def get_function_module_id(fun: Callable) -> str:
    """TODO"""
    mod = get_module(fun)
    return f"{mod}:{fun.__name__}"


def get_function_id(fun: Callable) -> str:
    """TODO"""
    try:
        return get_function_git_id(fun)
    except Exception:  # noqa: S110
        pass

    try:
        return get_function_module_id(fun)
    except Exception:  # noqa: S110
        pass

    return get_function_name(fun)
