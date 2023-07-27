import atexit
import datetime
import json
import logging
import os
import re
import socket
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from urllib.parse import unquote, unquote_plus, urlsplit

import appdirs
import requests
import sqlparse
import tzlocal
import unidecode

from .exceptions import InvalidPath

DATETIMETZ_FMT = "%Y-%m-%dT%H:%M:%S%z"
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H:%M:%S"

FILEMOD_WRITE = 0o222
ANONYMOUS_USER = "Anonymous"
LOCALHOST = "localhost"


# global exit stack
exit_stack = ExitStack()
# register at error
sys.excepthook = exit_stack.__exit__
# also register on regular exit
atexit.register(exit_stack.__exit__, None, None, None)


def normalize_sql_query(query: str) -> str:
    """
    Prettify an SQL query.

    Args:
        query (str): The SQL query to be prettified.

    Returns:
        str: The prettified SQL query.
    """
    return sqlparse.format(
        query,
        reindent=False,
        keyword_case="upper",
        strip_comments=True,
        strip_whitespace=True,
    )


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
            res = requests.head(url)
            if res.ok:
                return True
            else:
                raise Exception("HEAD failed")
        except requests.exceptions.ConnectionError:
            pass

        time_waited = time.time() - time_start
        logging.debug(f"checking server ({time_waited}): {url}")
        if timeout_s is not None and time_waited >= timeout_s:
            break
        time.sleep(wait_s)
    raise Exception("Timeout")


def uri_to_path(uri: str) -> str:
    url_parts = urlsplit(uri)
    if url_parts.scheme == "https":
        url_parts = url_parts._replace(scheme="http")
    if url_parts.netloc:
        nl = url_parts.netloc
        nl = remove_port_from_url_netloc(nl)
        nl = remove_auth_from_url_netloc(nl)
        url_parts = url_parts._replace(netloc=nl)
    # remove query params
    url_parts = url_parts._replace(query=None)
    uri = url_parts.geturl()
    # remove fragment separator
    uri = uri.replace("#", "")
    path = uri
    return path


def normalize_path(path: str) -> str:
    """should be all lowercase ascii"""
    _path = path  # save original
    if is_uri(path):
        path = uri_to_path(uri=path)

    path = unquote_plus(path)
    path = path.lower()
    for cin, cout in [
        ("ä", "ae"),
        ("ö", "oe"),
        ("ü", "ue"),
        ("ß", "ss"),
    ]:
        path = path.replace(cin, cout)
    path = unidecode.unidecode(path)
    path = path.replace("\\", "/")
    path = re.sub("/+", "/", path)
    # delete : drive
    path = re.sub(r"[:]+", "", path)
    path = re.sub(r"[^a-z0-9/_.\-]+", " ", path)
    path = path.strip()
    path = re.sub(r"\s+", "_", path)
    path = re.sub(r"_+", "_", path)
    path = path.strip("/")
    if not path:
        raise InvalidPath(_path)
    if path != _path:
        logging.debug(f"Translating {_path} => {path}")

    return path


def get_default_storage_location() -> str:
    return os.path.join(
        appdirs.user_data_dir(
            appname="datatools", appauthor=None, version=None, roaming=False
        ),
        "data",
    )


def get_query_arg(kwargs: dict, key: str, default=None) -> str:
    """kwargs only come as lists"""
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


def make_file_readonly(file_path: str) -> None:
    current_permissions = os.stat(file_path).st_mode
    readonly_permissions = current_permissions & ~FILEMOD_WRITE
    os.chmod(file_path, readonly_permissions)


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
        logging.debug(f"Translate {source} => {uri}")
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


def parse_cli_metadata(metadata_key_vals):
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
            logging.warning(f"cannot parse {key_value}")

    return result


def json_serialize(x):
    if isinstance(x, datetime.datetime):
        return x.strftime(DATETIMETZ_FMT)
    elif isinstance(x, datetime.date):
        return x.strftime(DATE_FMT)
    elif isinstance(x, datetime.time):
        return x.strftime(TIME_FMT)
    elif isinstance(x, type):
        # classname
        return x.__name__
    else:
        raise NotImplementedError(type(x))
