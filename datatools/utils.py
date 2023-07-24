import datetime
import getpass
import logging
import os
import re
import socket
from pathlib import Path
from urllib.parse import unquote, unquote_plus, urlsplit

import appdirs
import tzlocal
import unidecode

from .exceptions import InvalidPath

DATETIMETZ_FMT = "%Y-%m-%dT%H:%M:%S%z"

FILEMOD_WRITE = 0o222


def get_free_port():
    """Get a free port by binding to port 0 and releasing it."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("localhost", 0))
    _, port = sock.getsockname()
    sock.close()
    return port


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
    # explicitly delete "":"
    path = path.replace(":", "")
    path = re.sub(r"[^a-z0-9/_.\-#]+", " ", path)
    path = path.strip()
    path = re.sub(r"\s+", "_", path)
    path = path.strip("/")
    if not path:
        raise InvalidPath(_path)
    return path


def get_default_storage_location() -> str:
    return appdirs.user_data_dir(
        appname="datatools", appauthor=None, version=None, roaming=True
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


def get_username():
    return getpass.getuser()


def get_user_w_host():
    return f"{get_username()}@{get_hostname()}"


def get_now() -> datetime.datetime:
    # my local timezone, e.g.  DstTzInfo 'Europe/Berlin' LMT+0:53:00 STD
    tz_local = tzlocal.get_localzone()
    # timezone as current utc offset (does not know about dst),
    # e.g. datetime.timezone(datetime.timedelta(seconds=3600))
    now = datetime.datetime.now()
    # convert unaware datetime to proper timezone...
    now_tz = tz_local.localize(now)
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
