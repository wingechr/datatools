import abc
import hashlib
import json
import logging
import os
import re
from io import BufferedReader
from tempfile import NamedTemporaryFile
from typing import Iterable, Union

import jsonpath_ng

from . import loader
from .constants import (
    DEFAULT_HASH_METHOD,
    GLOBAL_LOCATION,
    LOCAL_LOCATION,
    ROOT_METADATA_PATH,
)
from .exceptions import DataDoesNotExists, DataExists, InvalidPath
from .utils import (
    as_byte_iterator,
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
    def resource(self, name: str) -> "AbstractStorageResource":
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

    def resource(self, name: str) -> "AbstractStorageResource":
        return StorageResource(storage=self, name=name)

    def check(self, fix=False):
        """scan location and check for problems"""
        for rt, _ds, fs in os.walk(self.location):
            # also make folder readonly
            make_file_readonly(rt)
            if not is_file_readonly(rt):
                if fix:
                    make_file_readonly(rt)
                    logging.info(f"FIXED: File not readonly: {rt}")
                else:
                    logging.warning(f"File not readonly: {rt}")

            rt_rl = os.path.relpath(rt, self.location).replace("\\", "/")
            rt = rt.replace("\\", "/")
            for filename in fs:
                filepath = f"{rt}/{filename}"
                filepath_rel = f"{rt_rl}/{filename}"

                if is_metadata_path(filepath_rel):
                    continue

                if is_temp_path(filename):
                    logging.warning(f"Possibly incomplete tempfile: {filepath}")
                    continue

                logging.debug(filepath)

                if not is_file_readonly(filepath):
                    if fix:
                        make_file_readonly(filepath)
                        logging.info(f"FIXED: File not readonly: {filepath}")
                    else:
                        logging.warning(f"File not readonly: {filepath}")

                norm_path = self._get_norm_data_path(filepath_rel)

                # ignore case not now:
                if norm_path != filepath_rel:
                    data_path_old = self._get_data_filepath(filepath_rel)
                    data_path_new = self._get_data_filepath(norm_path)

                    meta_data_path_old = self._get_metadata_filepath(data_path_old)
                    meta_data_path_new = self._get_metadata_filepath(data_path_new)

                    if fix:
                        os.rename(data_path_old, data_path_new)
                        if os.path.exists(meta_data_path_old):
                            os.rename(meta_data_path_old, meta_data_path_new)
                        logging.info(f"FIXED: invalid path: {filepath_rel}")
                    else:
                        logging.warning(f"invalid path: {filepath_rel}")

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
    def __init__(self, storage: "Storage", name: str):
        self.__storage = storage
        self.__name = self.__get_norm_name(name)
        self.__filepath = os.path.abspath(self.__storage.location + "/" + self.__name)

    @property
    def name(self):
        return self.__name

    @property
    def filepath(self):
        return self.__filepath

    def __get_norm_name(self, name: str) -> str:
        norm_name = normalize_path(name)

        if is_metadata_path(norm_name):
            raise InvalidPath(norm_name)

        if is_temp_path(norm_name):
            raise InvalidPath(norm_name)

        return norm_name

    @property
    def metadata(self) -> "AbstractStorageResourceMetadata":
        return StorageResourceMetadata(resource=self)

    def exists(self) -> bool:
        return os.path.exists(self.__filepath)

    def delete(self, delete_metadata: bool = False) -> None:
        delete_file(self.__filepath)

        if delete_metadata:
            self.metadata.delete()

    def save(self, exist_ok=False) -> None:
        raise NotImplementedError()

    def write(
        self, data: Union[BufferedReader, bytes, Iterable], exist_ok=False
    ) -> None:
        if self.exists():
            if not exist_ok:
                raise DataExists(self.__filepath)
            logging.info(f"Overwriting existing file: {self.__filepath}")

        # write data into temporary file
        tmp_dir = os.path.dirname(self.__filepath)
        tmp_prefix = os.path.basename(self.__filepath) + "+"

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
            logging.debug(f"MOVE {file.name} => {self.__filepath}")
            os.rename(file.name, self.__filepath)
        except Exception:
            delete_file(file.name)
            raise
        make_file_readonly(self.__filepath)

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
        if not self.exists():
            raise DataDoesNotExists(self.name)

        logging.debug(f"READING {self.__filepath}")
        file = open(self.__filepath, "rb")
        return file

    def load(self, **kwargs):
        if not self.exists():
            self.save()
        return loader.load(filepath=self.filepath, **kwargs)


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
