import datetime
import getpass
import hashlib
import os
import socket
from stat import S_IREAD, S_IRGRP, S_IROTH
from urllib.parse import urlsplit
from functools import cache

import appdirs
import tzlocal

DATETIMETZ_FMT = "%Y-%m-%d %H:%M:%S%z"
DATE_FMT = "%Y-%m-%d"


def make_readonly(filepath):
    os.chmod(filepath, S_IREAD | S_IRGRP | S_IROTH)


def get_hash(filepath, method="sha256"):
    hasher = getattr(hashlib, method)()
    with open(filepath, "rb") as file:
        hasher.update(file.read())
    result = {}
    result[method] = hasher.hexdigest()
    return result


def get_now():
    tz_local = tzlocal.get_localzone()
    now = datetime.datetime.now()
    now_tz = now.replace(tzinfo=tz_local)
    return now_tz


def get_now_str():
    return get_now().strftime(DATETIMETZ_FMT)


def get_today_str():
    return get_now().strftime(DATE_FMT)


@cache
def get_host():
    """Return current domain name"""
    # return socket.gethostname()
    return socket.getfqdn()


@cache
def get_user():
    """Return current user name"""
    return getpass.getuser()


@cache
def get_user_long():
    return f"{get_user()}@{get_host()}"


import json
import os

DEFAULT_ENCODING = "utf-8"
DEFAULT_JSON_INDENT = 2


def create_filecache(from_bytes, to_bytes):
    def filecache(filepath, create, *args, **kwargs):
        if not os.path.isfile(filepath):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            try:
                with open(filepath, "wb") as file:
                    data = create(*args, **kwargs)
                    bytes = to_bytes(data)
                    file.write(bytes)
            except Exception:
                if os.path.isfile(filepath):
                    print(f"DEL {os.path.abspath(filepath)}")
                    os.remove(filepath)

                raise

        with open(filepath, "rb") as file:
            bytes = file.read()
        data = from_bytes(bytes)
        return data

    return filecache


def create_bytes_to_str(encoding=DEFAULT_ENCODING):
    def bytes_to_str(bytes):
        return bytes.decode(encoding)

    return bytes_to_str


def create_str_to_bytes(encoding=DEFAULT_ENCODING):
    def str_to_bytes(string):
        return string.encode(encoding)

    return str_to_bytes


def create_obj_to_str(indent=DEFAULT_JSON_INDENT, sort_keys=False, ensure_ascii=False):
    def obj_to_str(obj):
        return json.dumps(
            obj, indent=indent, sort_keys=sort_keys, ensure_ascii=ensure_ascii
        )

    return obj_to_str


def create_obj_to_bytes(
    indent=DEFAULT_JSON_INDENT,
    sort_keys=False,
    ensure_ascii=False,
    encoding=DEFAULT_ENCODING,
):
    obj_to_str = create_obj_to_str(
        indent=indent, sort_keys=sort_keys, ensure_ascii=ensure_ascii
    )
    str_to_bytes = create_str_to_bytes(encoding=encoding)

    def obj_to_bytes(obj):
        string = obj_to_str(obj)
        bytes = str_to_bytes(string)
        return bytes

    return obj_to_bytes


def create_bytes_to_obj(encoding=DEFAULT_ENCODING):
    bytes_to_str = create_bytes_to_str(encoding=encoding)
    str_to_obj = create_str_to_obj()

    def bytes_to_obj(bytes):
        string = bytes_to_str(bytes)
        obj = str_to_obj(string)
        return obj

    return bytes_to_obj


def create_str_to_obj():
    def str_to_obj(string):
        return json.loads(string)

    return str_to_obj


def create_bytes_to_bytes():
    def bytes_to_bytes(bytes):
        return bytes

    return bytes_to_bytes


filecache_bytes = create_filecache(create_bytes_to_bytes(), create_bytes_to_bytes())
filecache_str = create_filecache(create_bytes_to_str(), create_str_to_bytes())
filecache_json = create_filecache(create_bytes_to_obj(), create_obj_to_bytes())


def get_app_data_dir(appname):
    return appdirs.user_data_dir(appname, appauthor=None, version=None, roaming=False)


def normpath(path):
    return os.path.realpath(path).replace("\\", "/")


def get_local_path(uri, base_path):
    url = urlsplit(uri)

    host = url.hostname or get_host()
    path = url.path

    # TODO: maybe urldecode spaces? but not all special chars?

    if not path.startswith("/"):
        path = "/" + path

    # if path == "/":
    #    path = "/index.html"

    path = host + path

    if url.fragment:
        path += url.fragment

    path = base_path + "/" + path
    path = normpath(path)

    return path


def assert_file_folder(filepath: str):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
