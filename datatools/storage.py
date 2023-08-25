import abc
import hashlib
import json
import logging
import os
import re
from io import BufferedReader
from tempfile import NamedTemporaryFile
from typing import Iterable, Union
from urllib.parse import urlsplit

import jsonpath_ng

from .constants import (
    DEFAULT_HASH_METHOD,
    GLOBAL_LOCATION,
    LOCAL_LOCATION,
    ROOT_METADATA_PATH,
    STORAGE_SCHEME,
)
from .exceptions import DataDoesNotExists, DataExists, InvalidPath
from .loader import load_file, open_uri
from .utils import (
    as_byte_iterator,
    as_uri,
    get_now_str,
    get_user_w_host,
    is_file_readonly,
    json_serialize,
    make_file_readonly,
    make_file_writable,
    normalize_path,
)


def delete_file(filepath: str) -> str:
    if not os.path.exists(filepath):
        return
    logging.debug(f"DELETING {filepath}")
    make_file_writable(file_path=filepath)
    os.remove(filepath)


def is_metadata_path(path: str):
    return ".metadata." in path


def is_temp_path(path: str):
    return "+" in path


class AbstractStorage(abc.ABC):
    @abc.abstractmethod
    def resource(self, source_uri: str) -> "AbstractStorageResource":
        ...


class AbstractStorageResource(abc.ABC):
    @abc.abstractproperty
    def metadata(self) -> "AbstractStorageResourceMetadata":
        ...

    @abc.abstractmethod
    def exists(self) -> bool:
        ...

    @abc.abstractmethod
    def delete(self, delete_metadata: bool = False) -> None:
        ...

    @abc.abstractmethod
    def write(
        self, data: Union[BufferedReader, bytes, Iterable], exist_ok=False
    ) -> None:
        ...

    @abc.abstractmethod
    def save(self, exist_ok=False) -> None:
        ...

    @abc.abstractmethod
    def open(self) -> BufferedReader:
        ...


class AbstractStorageResourceMetadata(abc.ABC):
    @abc.abstractmethod
    def get(self, key: str) -> object:
        ...

    @abc.abstractmethod
    def update(self, metadata: dict) -> None:
        ...


class Storage(AbstractStorage):
    def __init__(self, location=None):
        self.__location = os.path.abspath(location or LOCAL_LOCATION)

    @property
    def location(self):
        return self.__location

    def __str__(self):
        return f"Storage({self.location})"

    def resource(self, source_uri: str) -> "AbstractStorageResource":
        return StorageResource(storage=self, source_uri=source_uri)

    def check(self, fix=False):
        for name in self.search():
            res = self.resource(source_uri=name)

            # logging.debug(res.filepath)

            if not is_file_readonly(res.filepath):
                if fix:
                    make_file_readonly(res.filepath)
                    logging.info(f"FIXED: File not readonly: {res.filepath}")
                else:
                    logging.warning(f"File not readonly: {res.filepath}")

            norm_path = normalize_path(name)

            # ignore case not now:
            if norm_path != name:
                logging.warning(f"invalid path: {name}")

    def search(self, *path_patterns) -> Iterable[str]:
        path_patterns = [re.compile(".*" + p.lower()) for p in path_patterns]
        for rt, _ds, fs in os.walk(self.location):
            rt_rl = os.path.relpath(rt, self.location).replace("\\", "/")
            for filename in fs:
                path = f"{rt_rl}/{filename}"
                if is_metadata_path(path):
                    continue
                if is_temp_path(path):
                    continue
                if all(p.match(path) for p in path_patterns):
                    yield path


class StorageResource(AbstractStorageResource):
    def __init__(self, storage: "Storage", source_uri: str):
        self.__storage = storage
        self.__source_uri = as_uri(source_uri)

        url = urlsplit(self.__source_uri)
        if url.scheme == STORAGE_SCHEME:
            name = url.path
        else:
            name = self.__source_uri
        self.__name = normalize_path(name)

        if is_metadata_path(self.__name):
            raise InvalidPath(self.__name)
        if is_temp_path(self.__name):
            raise InvalidPath(self.__name)

        self.__uri = f"{STORAGE_SCHEME}:///{self.__name}"
        self.__filepath = os.path.abspath(self.__storage.location + "/" + self.__name)

    def __str__(self):
        return f"Resource('{self.source_uri}')"

    @property
    def name(self):
        return self.__name

    @property
    def source_uri(self):
        return self.__source_uri

    @property
    def filepath(self):
        return self.__filepath

    @property
    def metadata(self) -> "AbstractStorageResourceMetadata":
        return StorageResourceMetadata(resource=self)

    def exists(self) -> bool:
        return os.path.exists(self.filepath)

    def delete(self, delete_metadata: bool = False) -> None:
        delete_file(self.filepath)

        if delete_metadata:
            self.metadata.delete()

    def save(self, exist_ok=False) -> None:
        """save from source"""
        if self.exists():
            if not exist_ok:
                raise DataDoesNotExists(self)
            return
        byte_data, metadata = open_uri(self.source_uri)
        self.write(data=byte_data, exist_ok=exist_ok)
        self.metadata.update(metadata)

    def write(
        self, data: Union[BufferedReader, bytes, Iterable], exist_ok=False
    ) -> None:
        if self.exists():
            if not exist_ok:
                raise DataExists(self)
            logging.info(f"Overwriting existing file: {self.filepath}")

        # write data into temporary file
        tmp_dir = os.path.dirname(self.filepath)
        tmp_prefix = os.path.basename(self.filepath) + "+"

        hash_method = DEFAULT_HASH_METHOD
        hasher = getattr(hashlib, hash_method)()
        size = 0

        os.makedirs(tmp_dir, exist_ok=True)
        with NamedTemporaryFile(
            "wb", dir=tmp_dir, prefix=tmp_prefix, delete=False
        ) as file:
            logging.debug(f"WRITING {file.name}")
            for chunk in as_byte_iterator(data):
                file.write(chunk)
                size += len(chunk)
                hasher.update(chunk)

        # move to final location
        try:
            logging.debug(f"MOVE {file.name} => {self.filepath}")
            os.rename(file.name, self.filepath)
        except Exception:
            delete_file(file.name)
            raise
        make_file_readonly(self.filepath)

        # update metadata
        metadata = {
            f"hash.{hash_method}": hasher.hexdigest(),
            "size": size,
            "source.user": get_user_w_host(),
            "source.datetime": get_now_str(),
            "source.name": self.__name,
        }
        self.metadata.update(metadata)

    def open(self) -> BufferedReader:
        self.save(exist_ok=True)
        logging.debug(f"READING {self.filepath}")
        file = open(self.filepath, "rb")
        return file

    def load(self, **kwargs):
        self.save(exist_ok=True)
        return load_file(filepath=self.filepath, **kwargs)


class StorageResourceMetadata(AbstractStorageResourceMetadata):
    def __init__(self, resource: "AbstractStorageResource"):
        self.__resource = resource
        self.__filepath = self.__resource.filepath + ".metadata.json"

    def __read(self) -> dict:
        if not os.path.exists(self.__filepath):
            metadata = {}
        else:
            logging.debug(f"READING {self.__filepath}")
            with open(self.__filepath, "rb") as file:
                metadata = json.load(file)
        return metadata

    def update(self, metadata: dict) -> None:
        # get existing metadata
        _metadata = self.__read()

        # update
        for key, value in metadata.items():
            metadata_path_pattern = self.__create_metadata_path_pattern(
                metadata_path=key
            )
            logging.debug(f"update metadata: {key} => {value}")
            metadata_path_pattern.update_or_create(_metadata, value)

        # convert to bytes
        metadata_bytes = json.dumps(
            _metadata, indent=2, ensure_ascii=False, default=json_serialize
        ).encode()

        # save
        os.makedirs(os.path.dirname(self.__filepath), exist_ok=True)
        logging.debug(f"WRITING {self.__filepath}")
        with open(self.__filepath, "wb") as file:
            file.write(metadata_bytes)

    def get(self, key: str = None) -> object:
        metadata = self.__read()

        key = key or ROOT_METADATA_PATH
        metadata_path_pattern = self.__create_metadata_path_pattern(metadata_path=key)
        match = metadata_path_pattern.find(metadata)
        result = [x.value for x in match]

        # TODO: we always get a list (multiple matches),
        # but most of the time, we want only one
        if len(result) == 0:
            result = None
        elif len(result) == 1:
            result = result[0]
        else:
            logging.warning("multiple results in metadata found")

        return result

    def _delete(self):
        delete_file(self.__filepath)

    def __create_metadata_path_pattern(self, metadata_path: str) -> str:
        metadata_path_pattern = jsonpath_ng.parse(metadata_path)
        return metadata_path_pattern


class StorageGlobal(Storage):
    def __init__(self):
        super().__init__(location=GLOBAL_LOCATION)


class StorageEnv(Storage):
    def __init__(self, env_location):
        location = os.environ[env_location]
        super().__init__(location=location)
