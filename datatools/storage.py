import abc
import json
import logging
import os
from io import BytesIO, IOBase
from typing import Any, Dict, Type

import jsonpath_ng

# TODO: must be loaded for classes to be registeres
# there should be a better wayto do that
from . import converters, generators  # noqa
from .constants import GLOBAL_LOCATION, ROOT_METADATA_PATH
from .converters import AbstractConverter
from .exceptions import DataDoesNotExists, DataExists
from .generators import AbstractDataGenerator
from .utils import (
    ByteBufferWrapper,
    get_now_str,
    get_resource_path_name,
    get_user_w_host,
    make_file_readonly,
    make_file_writable,
)


class AbstractStorage(abc.ABC):
    def resource(self, data_source, name: str = None) -> "Resource":
        return Resource(storage=self, data_source=data_source, name=name)

    def _validate_name(self, name: str) -> str:
        return get_resource_path_name(name)

    @abc.abstractmethod
    def _resource_delete(self, resource_name: str) -> None:
        ...

    @abc.abstractmethod
    def _resource_exists(self, resource_name: str) -> None:
        ...

    @abc.abstractmethod
    def _metadata_update(self, resource_name: str, metadata: Dict[str, Any]) -> None:
        ...

    @abc.abstractmethod
    def _metadata_query(self, resource_name: str, key: str) -> Any:
        ...

    @abc.abstractmethod
    def _bytes_write(self, resource_name: str, byte_buffer: IOBase) -> None:
        ...

    @abc.abstractmethod
    def _bytes_open(self, resource_name: str) -> IOBase:
        ...


class Metadata:
    def update(self, metadata: Dict[str, Any]) -> None:
        return self.__storage._metadata_update(
            resource_name=self.__resource_name, metadata=metadata
        )

    def query(self, key: str = None) -> Any:
        return self.__storage._metadata_query(
            resource_name=self.__resource_name, key=key
        )

    def __init__(self, storage: "AbstractStorage", resource_name: str) -> None:
        self.__storage = storage
        self.__resource_name = resource_name


class Resource:
    def __init__(
        self, storage: "AbstractStorage", data_source: Any, name: str = None
    ) -> None:
        self.__data_generator = AbstractDataGenerator.get_instance(
            data_source=data_source
        )

        name = name or self.__data_generator.create_name()
        name = storage._validate_name(name)

        media_type, data_type = self.__data_generator.get_media_data_type(name=name)
        self.__media_type = media_type
        self.__data_type = data_type

        self.__storage = storage
        self.__name = name
        self.__metadata = Metadata(storage=storage, resource_name=name)

    @property
    def metadata(self) -> "Metadata":
        return self.__metadata

    @property
    def name(self) -> str:
        return self.__name

    def exists(self) -> bool:
        return self.__storage._resource_exists(resource_name=self.__name)

    def delete(self) -> None:
        if not self.exists():
            raise DataDoesNotExists(self)
        return self.__storage._resource_delete(resource_name=self.__name)

    def save(self) -> None:
        if self.exists():
            raise DataExists(self)

        # get extra kwargs from metadata
        create_kwargs = {
            k: self.__metadata.query(k) for k in self.__data_generator.create_kwargs
        }

        data, metadata_create = self.__data_generator.create_data_metadata(
            **create_kwargs
        )

        data_type = type(data)  # type from actual data, not from resource

        converter = AbstractConverter.get_instance(
            media_type=self.__media_type, data_type=data_type
        )
        # get extra kwargs from metadata
        encode_kwargs = {k: self.__metadata.query(k) for k in converter.encode_kwargs}
        bytes_buffer, metadata_encode = converter.encode(data, **encode_kwargs)

        bytes_buffer_wrapper = ByteBufferWrapper(bytes_buffer)

        with bytes_buffer_wrapper:
            self.__storage._bytes_write(
                resource_name=self.__name, byte_buffer=bytes_buffer_wrapper
            )

        metadata_save = {
            "created": {"datetime": get_now_str(), "user": get_user_w_host()},
            "size": bytes_buffer_wrapper.bytes,
            "hash": {
                method: hasher.hexdigest()
                for method, hasher in bytes_buffer_wrapper.hashers.items()
            },
        }

        self.metadata.update(metadata_create | metadata_encode | metadata_save)

    def load(self, data_type: Type = None) -> Any:
        if not self.exists():
            self.save()

        data_type = data_type or self.__data_type

        converter = AbstractConverter.get_instance(
            media_type=self.__media_type, data_type=data_type or self.__data_type
        )

        # get extra kwargs from metadata
        decode_kwargs = {k: self.__metadata.query(k) for k in converter.decode_kwargs}

        # load bytes and decode
        with self.__storage._bytes_open(resource_name=self.__name) as bytes_buffer:
            data = converter.decode(bytes_buffer, **decode_kwargs)

        return data


class TestMemoryStorage(AbstractStorage):
    def __init__(self):
        self.__data = {}
        self.__metadata = {}

    def _resource_delete(self, resource_name: str) -> None:
        logging.debug(f"Deleting {resource_name}")
        del self.__data[resource_name]
        del self.__metadata[resource_name]

    def _resource_exists(self, resource_name: str) -> None:
        return resource_name in self.__data

    def _metadata_update(self, resource_name: str, metadata: Dict[str, Any]) -> None:
        if resource_name not in self.__metadata:
            self.__metadata[resource_name] = {}
        metadata_res = self.__metadata[resource_name]
        for key, val in metadata.items():
            metadata_res[key] = val

    def _metadata_query(self, resource_name: str, key: str) -> Any:
        if resource_name not in self.__metadata:
            self.__metadata[resource_name] = {}
        metadata_res = self.__metadata[resource_name]
        return metadata_res.get(key)

    def _bytes_write(self, resource_name: str, byte_buffer: IOBase) -> None:
        logging.debug(f"Writing {resource_name}")
        bdata = byte_buffer.read()
        self.__data[resource_name] = bdata

    def _bytes_open(self, resource_name: str) -> IOBase:
        logging.debug(f"Reading {resource_name}")
        bdata = self.__data[resource_name]
        return BytesIO(bdata)


class FileStorage(AbstractStorage):
    def __init__(self, location: str = None):
        self.__location = location or "."

    def _get_filepath(self, resource_name: str):
        return os.path.join(self.__location, resource_name)

    def _get_filepath_metadata(self, resource_name: str):
        return self._get_filepath(resource_name=resource_name) + ".metadata.json"

    def _resource_delete(self, resource_name: str) -> None:
        filepath = self._get_filepath(resource_name=resource_name)
        filepath_meta = self._get_filepath_metadata(resource_name=resource_name)
        logging.debug(f"Deleting {filepath}")
        make_file_writable(filepath)
        os.remove(filepath)
        if os.path.isfile(filepath_meta):
            os.remove(filepath_meta)

    def _resource_exists(self, resource_name: str) -> None:
        filepath = self._get_filepath(resource_name=resource_name)
        return os.path.isfile(filepath)

    def _read_metadata(self, filepath_meta):
        if not os.path.isfile(filepath_meta):
            return {}
        with open(filepath_meta, "r", encoding="utf-8") as file:
            return json.load(file)

    def _metadata_update(self, resource_name: str, metadata: Dict[str, Any]) -> None:
        filepath_meta = self._get_filepath_metadata(resource_name=resource_name)
        metadata_all = self._read_metadata(filepath_meta)
        if resource_name not in metadata_all:
            metadata_all[resource_name] = {}

        metadata_res = metadata_all[resource_name]
        for key, val in metadata.items():
            key_pattern = jsonpath_ng.parse(key)
            key_pattern.update_or_create(metadata_res, val)

        sdata = json.dumps(metadata_all, ensure_ascii=False, indent=2)
        os.makedirs(os.path.dirname(filepath_meta), exist_ok=True)
        with open(filepath_meta, "w", encoding="utf-8") as file:
            file.write(sdata)

    def _metadata_query(self, resource_name: str, key: str = None) -> Any:
        filepath_meta = self._get_filepath_metadata(resource_name=resource_name)
        metadata_all = self._read_metadata(filepath_meta)
        if resource_name not in metadata_all:
            metadata_all[resource_name] = {}
        metadata_res = metadata_all[resource_name]

        key = key or ROOT_METADATA_PATH
        key_pattern = jsonpath_ng.parse(key)
        match = key_pattern.find(metadata_res)
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

    def _bytes_write(self, resource_name: str, byte_buffer: IOBase) -> None:
        filepath = self._get_filepath(resource_name=resource_name)
        filepath_temp = filepath + ".tmp"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        logging.debug(f"Writing {filepath}")

        assert not os.path.exists(filepath_temp)  # should not exist
        with open(filepath_temp, "wb") as file:
            # TODO: chunked
            file.write(byte_buffer.read())

        assert not os.path.exists(filepath)  # should not exist
        os.rename(filepath_temp, filepath)

        make_file_readonly(filepath)

    def _bytes_open(self, resource_name: str) -> IOBase:
        filepath = self._get_filepath(resource_name=resource_name)
        logging.debug(f"Reading {filepath}")
        return open(filepath, "rb")


class Storage(FileStorage):
    pass


class StorageGlobal(Storage):
    """Storage with user level global location"""

    def __init__(self):
        super().__init__(location=GLOBAL_LOCATION)


class StorageEnv(Storage):
    """Storage with location from environment variable"""

    def __init__(self, env_location):
        location = os.environ[env_location]
        super().__init__(location=location)
