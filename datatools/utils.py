import logging
import os
import re
import socket
from pathlib import Path
from urllib.parse import unquote, unquote_plus, urlsplit

import appdirs
import unidecode

from .exceptions import InvalidPath


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


def platform_is_windows():
    # os.name: 'posix', 'nt', 'java'
    return os.name == "nt"


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


def uri_to_data_path(uri: str) -> str:
    url = urlsplit(uri)
    if url.scheme == "https":
        url = url._replace(scheme="http")
    url = url._replace(query=None)
    uri = url.geturl()
    uri = uri.rstrip("/")
    # remove ports etc
    uri = re.sub(":[^/]*", "", uri)
    uri = re.sub("/+", "/", uri)

    # remove fragment separator
    uri = uri.replace("#", "")

    return uri
