__version__ = "0.0.0"
__app_name__ = "datatools"
__all__ = ["Repository"]


import json
import logging
import os
import re
from contextlib import ExitStack
from dataclasses import dataclass, field
from enum import Enum
from functools import cache, cached_property
from os import makedirs
from os.path import dirname, isfile, realpath
from pathlib import Path
from urllib.parse import urlsplit

import appdirs
import frictionless
import jsonschema
import requests
from filelock import FileLock

from .utils import (
    assert_file_folder,
    create_bytes_to_obj,
    create_obj_to_bytes,
    get_app_data_dir,
    get_local_path,
    normpath,
)


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
            with open(self._filepath, "rb") as file:
                self._byte_data = file.read()

        return self

    def __exit__(self, *args):
        assert self._lock.is_locked
        if self._has_changed:
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
        raise NotImplementedError()


@dataclass(frozen=True)
class Metadata:
    _metadata_storage: "MetadataStorage" = field(repr=False)
    _resource: "Resource" = field(repr=False)
    _data: dict

    # TODO: item assignment/loading (with json path?)
    def __setitem__(self, key, val):
        self._metadata_storage._has_changed


@dataclass(frozen=True)
class MetadataStorage:
    repository: "Repository" = field(repr=False)
    location: Path
    storage_type: object
    _resources: dict = field(init=False, repr=False, default_factory=dict)
    _data: object = field(init=False, repr=False)
    _file: ByteContext = field(init=False, repr=False)
    _has_changed: bool = field(init=False, default=False)

    @cached_property
    def container_location(self):
        return normpath(os.path.dirname(self.location))

    def __getitem__(self, resource: "Resource"):
        # get metadata from resource path
        assert resource.location.startswith(
            self.container_location
        ), f"{resource.location} > {self.container_location}"

        data = {}  # TODO

        return Metadata(self, resource, data)

    def __post_init__(self):
        object.__setattr__(
            self,
            "_file",
            ByteContext(
                filepath=self.location,
                exit_stack=self.repository._exit_stack,
                default=create_obj_to_bytes()({"resources": []}),
            ),
        )
        object.__setattr__(self, "_data", create_bytes_to_obj()(self._file.byte_data))
