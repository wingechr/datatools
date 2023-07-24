import logging
import os
import re
import socket
from urllib.parse import unquote_plus

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
    path = re.sub("[^a-z0-9/_.-]+", " ", path)
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


def file_to_data_path(file_path: str) -> str:
    data_path = os.path.abspath(file_path)
    # TODO: add host
    return data_path


def get_query_arg(kwargs: dict, key: str, default=None) -> str:
    """kwargs only come as lists"""
    values = kwargs.get(key)
    if not values:
        return default
    if len(values) > 1:
        logging.warning("multiple values defined")
    value = values[0]
    return unquote_plus(value)
