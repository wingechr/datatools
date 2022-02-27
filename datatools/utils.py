__version__ = "0.0.1"

import datetime
import getpass
import hashlib
import json
import logging
import os
import platform
import re
import socket
import subprocess
from collections import UserDict
from io import BytesIO
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWRITE
from urllib.parse import unquote_plus
from urllib.request import pathname2url

import chardet
import unidecode

from .exceptions import DuplicateKeyException, IntegrityException, InvalidValueException

DEFAULT_ENCODING = "utf-8"
DATETIME_UTC_FMT = "%Y-%m-%d %H:%M:%S.%f"
BYTES_SAMPLE = 2048


def read_bytes_sample(filepath, size=BYTES_SAMPLE):
    with open(filepath, "rb") as file:
        if size > 0:
            bytes_data = file.read(size)
        else:
            bytes_data = file.read()
    return bytes_data


def detect_text_encoding_from_filepath(filepath, size=BYTES_SAMPLE):
    bytes = read_bytes_sample(filepath, size=size)
    return detect_text_encoding_from_bytes(bytes, size=size)


def detect_text_encoding_from_bytes(bytes, size=BYTES_SAMPLE):
    if size > 0:
        bytes = bytes[:size]
    return chardet.detect(bytes)["encoding"]


def os_open_filepath(filepath):
    if platform.system() == "Darwin":  # macOS
        subprocess.call(("open", filepath))
    elif platform.system() == "Windows":  # Windows
        os.startfile(filepath)
    else:  # linux variants
        subprocess.call(("xdg-open", filepath))


def path2file_uri(path):
    # https://en.wikipedia.org/wiki/File_URI_scheme
    path = os.path.abspath(path)
    url = pathname2url(path)
    # in UNIX, this starts with only one slash,
    # but on windows local path with three
    # and windows network path even four
    # which seems inconsistent

    if url.startswith("////"):
        # windows network path, including the host
        # we want file://host/share/path
        uri = "file:" + url[2:]
        return uri

    if url.startswith("///"):
        # windows localpath
        url = url[2:]

    # prefix with file://HOSTNAME
    uri = "file://%s%s" % (get_host(), url)
    return uri


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


def get_timestamp_utc():
    return datetime.datetime.utcnow()


def strftime(value):
    return value.strftime(DATETIME_UTC_FMT)


def get_timestamp_utc_str():
    return strftime(get_timestamp_utc())


def strptime(value):
    return datetime.datetime.strptime(value, DATETIME_UTC_FMT)


def make_file_readlonly(filepath):
    os.chmod(filepath, S_IREAD | S_IRGRP | S_IROTH)


def make_file_writable(filepath):
    os.chmod(filepath, S_IWRITE)


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


def validate_file_id(file_id):
    if not isinstance(file_id, str) or len(file_id) != 32:
        raise InvalidValueException(file_id)
    return file_id


class HashedByteIterator:
    DEFAULT_CHUNK_SIZE = 2 ** 24

    def __init__(
        self, data_stream, expected_hash=None, chunk_size=None, max_bytes=None
    ):
        self.data_stream = data_stream
        self.hash = hashlib.md5()
        self.size_bytes = 0
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE
        self.expected_hash = expected_hash

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def _stop_iteration(self):
        logging.info("stop")
        self.data_stream.__exit__(None, None, None)
        if self.expected_hash:
            file_id = self.get_current_hash()
            if file_id != self.expected_hash:
                raise IntegrityException(self.expected_hash)
            else:
                logging.debug("Integrity check ok")

    def __iter__(self):
        logging.error(type(self.data_stream))
        if isinstance(self.data_stream, str):
            logging.info("opening file: %s", self.data_stream)
            self.data_stream = open(self.data_stream, "rb")
        elif isinstance(self.data_stream, bytes):
            logging.info("buffer data: %s")
            self.data_stream = BytesIO(self.data_stream)
        self.data_stream.__enter__()
        return self

    def __next__(self):
        chunk_size = self.chunk_size
        if self.max_bytes and self.size_bytes + chunk_size > self.max_bytes:
            chunk_size = self.max_bytes - self.size_bytes
        chunk = self.read(chunk_size)
        if not chunk:
            self._stop_iteration()
            raise StopIteration()
        self.size_bytes += len(chunk)
        return chunk

    def read(self, size=-1):
        chunk = self.data_stream.read(size)
        logging.debug("read %d bytes (max_bytes=%d)", len(chunk), size)
        self.hash.update(chunk)
        return chunk

    def get_current_hash(self):
        return self.hash.hexdigest()

    def get_current_size_bytes(self):
        return self.size_bytes
