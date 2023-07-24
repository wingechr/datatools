import datetime
import json
import logging
import os
import re
import socket
import time
from pathlib import Path
from urllib.parse import unquote, unquote_plus, urlsplit

import appdirs
import requests
import tzlocal
import unidecode

from .exceptions import InvalidPath

DATETIMETZ_FMT = "%Y-%m-%dT%H:%M:%S%z"

FILEMOD_WRITE = 0o222
ANONYMOUS_USER = "Anonymous"
LOCALHOST = "localhost"


def get_free_port():
    """Get a free port by binding to port 0 and releasing it."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((LOCALHOST, 0))
    _, port = sock.getsockname()
    sock.close()
    return port


def wait_for_server(url, timeout_s=5):
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


def normalize_path(path: str) -> str:
    """should be all lowercase"""
    _path = path  # save original
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
    # explicitly delete some
    path = re.sub(r"[#:]+", "", path)
    path = re.sub(r"[^a-z0-9/_.\-]+", " ", path)
    path = path.strip()
    path = re.sub(r"\s+", "_", path)
    path = re.sub(r"_+", "_", path)
    path = path.strip("/")
    if not path:
        raise InvalidPath(_path)
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


def get_hostname():
    return socket.gethostname()


def get_fqhostname():
    return socket.getfqdn()


def get_username():
    # getpass.getuser() does not always work
    return os.environ.get("USERNAME") or os.environ.get("USER") or ANONYMOUS_USER


def get_user_w_host():
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


def get_now_str():
    now = get_now()
    now_str = now.strftime(DATETIMETZ_FMT)
    # add ":" in offset
    now_str = re.sub("([+-][0-9]{2})([0-9]{2})$", r"\1:\2", now_str)
    return now_str


def platform_is_windows():
    # os.name: 'posix', 'nt', 'java'
    return os.name == "nt"


def make_file_readonly(file_path):
    current_permissions = os.stat(file_path).st_mode
    readonly_permissions = current_permissions & ~FILEMOD_WRITE
    os.chmod(file_path, readonly_permissions)


def make_file_writable(file_path):
    current_permissions = os.stat(file_path).st_mode
    readonly_permissions = current_permissions | FILEMOD_WRITE
    os.chmod(file_path, readonly_permissions)


def path_to_file_uri(abspath: Path) -> str:
    """
    Args:
        abspath(Path): must be already absolute path!
    """
    uri = abspath.as_uri()
    uri = urlsplit(uri)

    if not uri.netloc:
        uri = uri._replace(netloc=get_hostname())

    uri = uri.geturl()  # unsplit

    # we dont want it quoted
    uri = unquote(uri)

    return uri


def file_uri_to_path(uri: str) -> str:
    url = urlsplit(uri)

    if url.scheme != "file":
        raise Exception(f"Not a file path: {uri}")

    is_local = url.netloc == get_hostname()
    is_win = re.match("/[a-zA-Z]:/", url.path) or (not is_local)

    path = url.path
    if is_win:
        if is_local:
            # remove starting /
            path = path.lstrip("/")

        else:  # unc share
            path = f"//{url.netloc}{path}"
        path = path.replace("/", "\\")
    else:  # posix
        if not is_local:
            raise NotImplementedError(f"unc share in posix: {uri}")
        pass

    return path


def remove_auth_from_uri(uri: str) -> str:
    return re.sub("[^/]*@", "", uri)


def uri_to_data_path(uri: str) -> str:
    url = urlsplit(uri)
    if url.scheme == "https":
        url = url._replace(scheme="http")
    url = url._replace(query=None)
    uri = url.geturl()
    uri = uri.rstrip("/")
    # also remove passwords
    uri = remove_auth_from_uri(uri)

    # remove ports etc
    uri = re.sub(":[^/]*", "", uri)

    uri = re.sub("/+", "/", uri)

    # remove fragment separator
    uri = uri.replace("#", "")

    return uri


def parse_cli_metadata(metadata_key_vals):
    """cli: list of key=value"""
    metadata = {}
    for key_value in metadata_key_vals:
        key, value = key_value.split("=")
        key = key.strip()
        value = value.strip()
        try:
            value = json.loads(value)
        except Exception:
            pass
        metadata[key] = value
    return metadata
