import datetime
import getpass
import hashlib
import json
import logging
import os
import re
import socket
from contextlib import ExitStack
from dataclasses import dataclass, field
from enum import Enum
from functools import cache, cached_property
from os import makedirs
from os.path import dirname, isfile, realpath
from pathlib import Path

# from shutil import move
from stat import S_IREAD, S_IRGRP, S_IROTH

# from tempfile import NamedTemporaryFile
from typing import Callable
from urllib.parse import urlsplit

import appdirs
import frictionless
import jsonpath_ng as jp
import jsonschema
import requests
import tzlocal
from filelock import FileLock

DEFAULT_ENCODING = "utf-8"
DEFAULT_JSON_INDENT = 2

DATETIMETZ_FMT = "%Y-%m-%d %H:%M:%S%z"
DATE_FMT = "%Y-%m-%d"

__app_name__ = "datatools"


def make_readonly(filepath):
    os.chmod(filepath, S_IREAD | S_IRGRP | S_IROTH)


def get_hash(filepath, method="sha256"):
    hash = getattr(hashlib, method)()
    with open(filepath, "rb") as file:
        while chunk := file.read(8192):
            hash.update(chunk)
    return hash.hexdigest()


def get_size_bytes(filepath):
    return os.path.getsize(filepath)


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
    return socket.getfqdn()


@cache
def get_user():
    """Return current user name"""
    return getpass.getuser()


@cache
def get_user_long():
    return f"{get_user()}@{get_host()}"


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


def validate_resource(resource_descriptor):
    res = frictionless.Resource(resource_descriptor)
    rep = res.validate()

    if rep.stats["errors"]:
        errors = []
        for task in rep.tasks:
            for err in task["errors"]:
                errors.append(err["message"])

        err_str = "\n".join(errors)
        # logging.error(err_str)
        raise ValueError(err_str)


@cache
def get_cache_dir():
    return appdirs.user_cache_dir(__app_name__, appauthor=None, version=None)


@cache
def get_local_cache_path(url, cache_dir=None):
    cache_dir = cache_dir or get_cache_dir() + "/schema"
    url_parts = urlsplit(url)
    path = cache_dir + "/" + url_parts.netloc + "/" + url_parts.path
    return realpath(path)


@cache
def get_jsonschema(schema_url, cache_dir=None, encoding="utf-8"):
    local_path = get_local_path(schema_url, cache_dir=cache_dir)
    if not isfile(local_path):
        makedirs(dirname(local_path), exist_ok=True)
        res = requests.get(schema_url)
        res.raise_for_status()
        res = json.dumps(res.json(), indent=4, ensure_ascii=False)
        with open(local_path, "w", encoding=encoding) as file:
            file.write(res)
    with open(local_path, "r", encoding=encoding) as file:
        return json.load(file)


def get_jsonschema_validator(schema):
    """Return validator instance for schema.

    Example:

    >>> schema = {"type": "object", "properties": {"id": {"type": "integer"}}, "required": [ "id" ]}  # noqa
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
        schema = get_jsonschema(schema)

    validator_cls = jsonschema.validators.validator_for(schema)
    # check if schema is valid
    validator_cls.check_schema(schema)
    validator = validator_cls(schema)

    def validator_function(instance):
        errors = []
        for err in validator.iter_errors(instance):
            # path in data structure where error occurs
            path = "$" + "/".join(str(x) for x in err.absolute_path)
            errors.append("%s in %s" % (err.message, path))
        if errors:
            err_str = "\n".join(errors)
            # logging.error(err_str)
            raise ValueError(err_str)

    return validator_function


class ByteContext:
    def __init__(self, filepath: Path, exit_stack: ExitStack = None, default=b""):
        self._exit_stack = exit_stack
        self._filepath = filepath
        self._lock = FileLock(self._filepath + ".lock")
        self._byte_data = default
        self._has_changed = False

    def __enter__(self):
        assert not self._lock.is_locked
        assert_file_folder(self._lock.lock_file)
        self._lock.acquire()

        assert_file_folder(self._filepath)
        if os.path.isfile(self._filepath):
            logging.debug(f"Reading from {self._filepath}")
            with open(self._filepath, "rb") as file:
                self._byte_data = file.read()

        return self

    def __exit__(self, *args):
        assert self._lock.is_locked
        if self._has_changed:
            logging.debug(f"Writing to {self._filepath}")
            with open(self._filepath, "wb") as file:
                file.write(self._byte_data)
        self._lock.release()

    def _assert_open(self):
        if not self._lock.is_locked:
            self._exit_stack.enter_context(self)

    @property
    def byte_data(self):
        self._assert_open()
        return self._byte_data

    @byte_data.setter
    def byte_data(self, byte_data):
        assert isinstance(byte_data, bytes)
        self._assert_open()
        self._has_changed = True
        self._byte_data = byte_data


class StorageType(Enum):
    SINGLE_RESOURCE_JSON = 1
    SINGLE_RESOURCE_YML = 2
    DATAPACKAGE = 3


@dataclass(frozen=True)
class Repository:
    """
    If used like a dictionary: resources
    """

    location: Path = field(default=None)
    _exit_stack: ExitStack = field(init=False, repr=False, default_factory=ExitStack)
    _metadata_storage_instances: dict = field(
        init=False, repr=False, default_factory=dict
    )

    def __post_init__(self):
        object.__setattr__(
            self,
            "location",
            normpath(self.location or get_app_data_dir(__app_name__) + "/data"),
        )
        logging.debug(f"LOCATION: {self.location}")

    def __enter__(self):
        logging.debug("ENTER")
        self._exit_stack.__enter__()

    def __exit__(self, *error):
        logging.debug("EXIT")
        self._exit_stack.__exit__(*error)

    def __getitem__(self, uri: str):
        return Resource(self, uri)

    def get_resource_location(self, uri: str):
        location = get_local_path(uri, base_path=self.location)
        location = normpath(location)
        return location

    def find_datapackage_index_location(self, resource):
        # walk paths
        path = resource.location
        while True:
            # split off the last part
            path = re.match("^(.+)/[^/]+$", path).groups()[0]
            if not path.startswith(self.location):
                break
            metadata_storage_location = path + "/datapackage.json"
            if os.path.isfile(metadata_storage_location):
                return metadata_storage_location

    def get_metadata_storage(self, resource: "Resource"):
        single_resource_json_path = resource.location + ".metadata.json"
        single_resource_yml_path = resource.location + ".metadata.yml"

        logging.debug(f"meta storage location: {self.location}")
        logging.debug(f"resource location: {resource.location}")

        assert resource.location.startswith(self.location)

        metadata_storage_location = self.find_datapackage_index_location(resource)
        if metadata_storage_location:
            metadata_storage_type = StorageType.DATAPACKAGE
        else:
            # TODO: user preference for yamlor json

            if os.path.isfile(single_resource_yml_path):
                metadata_storage_location = single_resource_yml_path
                metadata_storage_type = StorageType.SINGLE_RESOURCE_YML
            else:
                metadata_storage_location = single_resource_json_path
                metadata_storage_type = StorageType.SINGLE_RESOURCE_JSON

        if not metadata_storage_type:
            raise NotImplementedError(resource)

        if metadata_storage_location not in self._metadata_storage_instances:
            ms = MetadataStorage(self, metadata_storage_location, metadata_storage_type)
            self._metadata_storage_instances[metadata_storage_location] = ms

        return self._metadata_storage_instances[metadata_storage_location]


@dataclass(frozen=True)
class Resource:
    repository: "Repository" = field(repr=False)
    uri: str

    @cached_property
    def location(self):
        return self.repository.get_resource_location(self.uri)

    @cached_property
    def metadata(self):
        metadata_storage = self.repository.get_metadata_storage(self)
        return metadata_storage[self]

    def download(self):
        logging.warning("TODO: determine correct loader from scheme")

        # if os.path.isfile(self.location):
        #    raise FileExistsError(self.location)
        #
        # os.makedirs(os.path.dirname(self.location), exist_ok=True)
        #
        # with requests.get(self.uri, stream=True) as res:
        #    res.raise_for_status()
        #    content_type = res.headers.get("Content-Type")
        #    with NamedTemporaryFile("wb", delete=False) as file:
        #        for chunk in res.iter_content(chunk_size=8192):
        #            file.write(chunk)
        #    move(file.name, self.location)

        # content_type_parts = [x.strip() for x in content_type.split(";")]
        # media_type = content_type_parts[0]
        # content_type_options = {}
        # for p in content_type_parts[1:]:
        #    k, v = p.split("=")
        #    content_type_options[k.strip()] = v.strip()
        # encoding = content_type_options.get("encoding")
        user = get_user_long()
        now_str = get_today_str()

        meta = self.metadata
        meta["name"] = normalize_name(os.path.basename(self.location))

        meta["contributors[0].name"] = user
        meta["contributors[0].date"] = now_str
        meta["contributors[0].description"] = "download from source"
        meta["sources[0].path"] = self.uri
        meta["sources[0].accessDate"] = now_str

        # if media_type:
        #    meta["mediatype"] = media_type
        # if encoding:
        #    meta["encoding"] = encoding

        meta["hash"] = f'sha256:{get_hash(self.location, "sha256")}'
        meta["bytes"] = get_size_bytes(self.location)

        logging.info(meta["hash"])


@dataclass(frozen=True)
class Metadata:
    _metadata_storage: "MetadataStorage" = field(repr=False)
    _resource: "Resource" = field(repr=False)
    _data: dict
    _has_changed: Callable = field(repr=False)

    # TODO: item assignment/loading (with json path?)
    def __setitem__(self, key, val):
        # create json path expression from key
        jp_expr = jp.parse(key)
        jp_expr.update_or_create(self._data, val)
        self._has_changed()

    def __getitem__(self, key):
        jp_expr = jp.parse(key)
        match = jp_expr.find(self._data)
        if not match:
            return None
        if len(match) > 1:
            logging.warning("multiple matches")
        return match[0].value


@dataclass(frozen=True)
class MetadataStorage:
    repository: "Repository" = field(repr=False)
    location: Path
    storage_type: object
    _resources: dict = field(init=False, repr=False, default_factory=dict)
    _data: object = field(init=False, repr=False)
    _file: ByteContext = field(init=False, repr=False)

    @cached_property
    def container_location(self):
        return normpath(os.path.dirname(self.location))

    def __getitem__(self, resource: "Resource"):
        # get metadata from resource path

        data = self._data

        def changed():
            self._file._has_changed = True

        return Metadata(self, resource, data, changed)

    def __post_init__(self):
        object.__setattr__(
            self,
            "_file",
            ByteContext(
                filepath=self.location,
                exit_stack=self.repository._exit_stack,
                default=create_obj_to_bytes()({}),
            ),
        )
        object.__setattr__(self, "_data", create_bytes_to_obj()(self._file.byte_data))


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

    # lower case and remove all blocks of invalid characters
    name = name.lower()
    name = re.sub("[^a-z0-9]+", "_", name).strip("_")

    return name
