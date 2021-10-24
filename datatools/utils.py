__version__ = "0.0.1"

from abc import abstractmethod
from io import BytesIO
import re
import logging
import json
import datetime
from urllib.parse import unquote_plus
import unidecode
import hashlib
from collections import UserDict

import socket
import getpass


from datatools.exceptions import DuplicateKeyException


class UniqueDict(UserDict):
    def __setitem__(self, key, value):
        if key in self:
            raise DuplicateKeyException(key)
        return super().__setitem__(key, value)


def get_user():
    """Return current user name"""
    return getpass.getuser()


def get_host():
    """Return current host name"""
    return socket.gethostname()


def get_user_host():
    return "%s@%s" % (get_user(), get_host())


DEFAULT_ENCODING = "utf-8"
DATETIME_UTC_FMT = "%Y-%m-%d %H:%M:%S.%f"


def get_timestamp_utc():
    return datetime.datetime.utcnow()


def strftime(value):
    return value.strftime(DATETIME_UTC_FMT)


def get_timestamp_utc_str():
    return strftime(get_timestamp_utc())


def strptime(value):
    return datetime.datetime.strptime(value, DATETIME_UTC_FMT)


def get_unix_utc():
    """
    Return current unix timestamp (utc, with milliseconds)
    """
    return get_timestamp_utc().timestamp()


def normalize_name(name):
    """
    >>> normalize_name('Hello  World!')
    'hello_world'
    >>> normalize_name('helloWorld')
    'hello_world'
    >>> normalize_name('_private_4')
    '_private_4'
    >>> normalize_name('François fährt Straßenbahn zum Café Málaga')
    'francois_faehrt_strassenbahn_zum_cafe_malaga'
    """
    name = unquote_plus(name)

    # manual replacements for german
    for cin, cout in [
        ("ä", "ae"),
        ("ö", "oe"),
        ("ü", "ue"),
        ("Ä", "Ae"),
        ("Ö", "Oe"),
        ("Ü", "Ue"),
        ("ß", "ss"),
    ]:
        name = name.replace(cin, cout)

    # camel case to python
    name = re.sub("([a-z])([A-Z])", r"\1_\2", name)

    # maske ascii
    name = unidecode.unidecode(name)

    # lower case and remove all blocks of invalid characters
    name = name.lower()
    name = re.sub("[^a-z0-9]+", "_", name).rstrip("_")

    return name


def get_byte_hash(byte_data):
    if not isinstance(byte_data, bytes):
        raise NotImplementedError("data must be bytes")
    md5 = hashlib.md5(byte_data).hexdigest()
    return md5


def get_data_hash(data):
    return get_byte_hash(json_dumpb(data))


class JsonSerializable:
    def to_file(self):
        return BytesIO(json_dumpb(self))

    def get_id(self):
        return get_byte_hash(json_dumpb(self))


def json_dumps(value):
    def serialize(obj):
        if isinstance(obj, JsonSerializable):
            return dict(
                (k, v) for k, v in obj.__dict__.items() if not k.startswith("_")
            )
        elif isinstance(obj, datetime.datetime):
            return strftime(obj)
        else:
            raise NotImplementedError(type(obj))

    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, indent=2, default=serialize
    )


def json_dumpb(value):
    return json_dumps(value).encode()


def json_loads(value):
    return json.loads(value)


def json_loadb(value):
    return json_loads(value.decode())
