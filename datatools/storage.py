import abc
import json
import logging
import os
import re
import tempfile
from io import IOBase
from typing import Any, Callable, Dict, Iterable, Type, Union

import jsonpath_ng

# TODO: must be loaded for classes to be registeres
# there should be a better wayto do that
from . import generators, loaders  # noqa
from .constants import (
    GLOBAL_LOCATION,
    MEDIA_TYPE_METADATA_PATH,
    ROOT_METADATA_PATH,
    STORAGE_SCHEME,
)
from .exceptions import DataDoesNotExists, DataExists
from .generators import AbstractDataGenerator
from .loaders import AbstractConverter
from .utils import (
    ByteBufferWrapper,
    get_now_str,
    get_resource_path_name,
    get_user_w_host,
    make_file_readonly,
    make_file_writable,
    rmtree_readonly,
)

METADATA_JSON_SUFFIX = ".metadata.json"
TEMPFILE_SUFFIX = ".tmp"


class AbstractStorage(abc.ABC):
    def resource(self, source: Any, name: Union[str, Callable] = None) -> "Resource":
        """Create resource descriptor.

        Parameters
        ----------
        source : Any
            _description_
        name : Union[str, Callable], optional
            _description_, by default None

        Returns
        -------
        Resource
            _description_
        """
        return Resource(storage=self, source=source, name=name)

    @abc.abstractmethod
    def _resource_delete(self, resource_name: str) -> None: ...

    @abc.abstractmethod
    def _resource_exists(self, resource_name: str) -> bool: ...

    @abc.abstractmethod
    def _metadata_update(
        self, resource_name: str, metadata: Dict[str, Any]
    ) -> None: ...

    @abc.abstractmethod
    def _metadata_query(self, resource_name: str, key: str) -> Any: ...

    @abc.abstractmethod
    def _bytes_write(self, resource_name: str, byte_buffer: IOBase) -> None: ...

    @abc.abstractmethod
    def _bytes_open(self, resource_name: str) -> IOBase: ...

    @abc.abstractmethod
    def find_resources(self, *args, **kwargs) -> Iterable["Resource"]: ...

    @abc.abstractmethod
    def _validate_name(self, name: str) -> str: ...

    @abc.abstractmethod
    def __enter__(self) -> "AbstractStorage": ...

    @abc.abstractmethod
    def __exit__(self, *args) -> None: ...


class Metadata:
    def update(self, metadata: Dict[str, Any]) -> None:
        """_summary_

        Args:
            metadata (Dict[str, Any]): _description_

        Returns:
            _type_: _description_
        """
        self._storage._metadata_update(
            resource_name=self._resource_name, metadata=metadata
        )

    def query(self, key: str = None) -> Any:
        """_summary_

        Args:
            key (str, optional): _description_. Defaults to None.

        Returns:
            Any: _description_
        """
        return self._storage._metadata_query(resource_name=self._resource_name, key=key)

    def __init__(self, storage: "AbstractStorage", resource_name: str) -> None:
        self.__storage = storage
        self.__resource_name = resource_name

    @property
    def _storage(self) -> AbstractStorage:
        return self.__storage

    @property
    def _resource_name(self) -> str:
        return self.__resource_name


class Resource:
    """_summary_"""

    def __init__(
        self,
        storage: "AbstractStorage",
        source: Any,
        name: Union[str | Callable] = None,
    ) -> None:
        self.__storage = storage
        self.__data_generator = AbstractDataGenerator.get_instance(source=source)

        # create first draft of name
        _name = name if isinstance(name, str) else self._data_generator.create_name()
        # if name is a function: use it to modify the draft
        if isinstance(name, Callable):
            _name = name(_name)
        # finally: storage must approve
        self.__name = storage._validate_name(_name)
        self.__metadata = Metadata(storage=self._storage, resource_name=self.name)

    def __str__(self) -> str:
        return self.uri

    @property
    def uri(self) -> str:
        return f"{STORAGE_SCHEME}:///{self.name}"

    @property
    def metadata(self) -> "Metadata":
        return self.__metadata

    @property
    def name(self) -> str:
        return self.__name

    @property
    def _storage(self) -> AbstractStorage:
        return self.__storage

    @property
    def _data_generator(self) -> AbstractDataGenerator:
        return self.__data_generator

    def exists(self) -> bool:
        return self._storage._resource_exists(resource_name=self.name)

    def delete(self) -> None:
        if not self.exists():
            raise DataDoesNotExists(self)
        self._storage._resource_delete(resource_name=self.name)

    def save(self, media_type: str = None) -> None:
        if self.exists():
            raise DataExists(self)

        # get extra kwargs from metadata
        create_kwargs = {
            k: self.metadata.query(k) for k in self._data_generator.create_kwargs
        }

        data, metadata_create = self._data_generator.create_data_metadata(
            **create_kwargs
        )

        data_type = type(data)  # type from actual data, not from resource
        # priority: function argument, metadata, data generator
        if not media_type:
            media_type = self.metadata.query(MEDIA_TYPE_METADATA_PATH)
        if not media_type:
            media_type = self._data_generator.get_media_data_type(name=self.name)[0]
        converter = AbstractConverter.get_instance(
            media_type=media_type, data_type=data_type
        )

        # get extra kwargs from metadata
        encode_kwargs = {k: self.metadata.query(k) for k in converter.encode_kwargs}
        bytes_buffer, metadata_encode = converter.encode(data, **encode_kwargs)

        bytes_buffer_wrapper = ByteBufferWrapper(bytes_buffer)

        with bytes_buffer_wrapper:
            self._storage._bytes_write(
                resource_name=self.name, byte_buffer=bytes_buffer_wrapper
            )

        metadata_save = {
            "created": {"datetime": get_now_str(), "user": get_user_w_host()},
            "size": bytes_buffer_wrapper.bytes,
            "hash": {
                method: hasher.hexdigest()
                for method, hasher in bytes_buffer_wrapper.hashers.items()
            },
            MEDIA_TYPE_METADATA_PATH: media_type,
        }

        new_metadata = {}  # older python cant do dict1 | dict2
        new_metadata.update(metadata_create)
        new_metadata.update(metadata_encode)
        new_metadata.update(metadata_save)

        self.metadata.update(new_metadata)

    def load(self, data_type: Type = None) -> Any:
        if not self.exists():
            self.save()

        # priority: function argument, data generator
        _media_type, _data_type = self._data_generator.get_media_data_type(
            name=self.name
        )

        if not data_type:
            data_type = _data_type
        media_type = self.metadata.query(MEDIA_TYPE_METADATA_PATH)
        if not media_type:
            media_type = _media_type

        converter = AbstractConverter.get_instance(
            media_type=media_type, data_type=data_type
        )

        # get extra kwargs from metadata
        decode_kwargs = {k: self.metadata.query(k) for k in converter.decode_kwargs}

        # load bytes and decode
        with self._storage._bytes_open(resource_name=self.name) as bytes_buffer:
            data = converter.decode(bytes_buffer, **decode_kwargs)

        return data

    def _open(self) -> IOBase:
        return self._storage._bytes_open(self.name)


class Storage(AbstractStorage):

    def __init__(self, location: str = None):
        self.__location = os.path.abspath(os.path.realpath(location or "."))

    @property
    def _location(self):
        return self.__location

    def __str__(self):
        return self._location

    def _get_filepath(self, resource_name: str):
        relpath = resource_name
        if not relpath.startswith("/"):
            relpath = "/" + relpath
        filepath = os.path.realpath(self._location + relpath)
        return filepath

    def _get_filepath_metadata(self, resource_name: str):
        return self._get_filepath(resource_name=resource_name) + METADATA_JSON_SUFFIX

    def _resource_delete(self, resource_name: str) -> None:
        filepath = self._get_filepath(resource_name=resource_name)
        filepath_meta = self._get_filepath_metadata(resource_name=resource_name)
        logging.debug(f"Deleting {filepath}")
        make_file_writable(filepath)
        os.remove(filepath)
        if os.path.isfile(filepath_meta):
            os.remove(filepath_meta)

    def _resource_exists(self, resource_name: str) -> bool:
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
        filepath_temp = filepath + TEMPFILE_SUFFIX
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

    def find_resources(self, *patterns, **kwargs) -> Iterable[Resource]:
        for rt, _ds, filenames in os.walk(self._location):
            for filename in filenames:
                # find only metadata
                if not filename.endswith(METADATA_JSON_SUFFIX):
                    continue
                suffix_len = len(METADATA_JSON_SUFFIX)
                filename = filename[:-suffix_len]
                filepath = os.path.join(rt, filename)
                filepath_rel = os.path.relpath(filepath, self._location)
                name = filepath_rel.replace("\\", "/")
                if not all(re.match(f".*{pat}", name) for pat in patterns):
                    continue

                # exists = os.path.isfile(filepath)
                if self._validate_name(name) != name:
                    logging.warning(f"invalid name: {name}")

                if not os.path.isfile(filepath):
                    logging.info(f"only metadata: {name}")

                yield self.resource(name=name)

    def _validate_name(self, name: str) -> str:
        name_new = get_resource_path_name(name)
        if (
            not name_new
            or name_new.endswith(METADATA_JSON_SUFFIX)
            or name_new.endswith(TEMPFILE_SUFFIX)
        ):
            raise ValueError(f"Invalid name: {name_new}")
        return name_new

    def __enter__(self) -> AbstractStorage:
        return self

    def __exit__(self, *args) -> None:
        return


class StorageGlobal(Storage):
    """Storage with user level global location"""

    def __init__(self):
        super().__init__(location=GLOBAL_LOCATION)


class StorageEnv(Storage):
    """Storage with location from environment variable"""

    def __init__(self, env_location):
        location = os.environ[env_location]
        super().__init__(location=location)


class StorageTemp(Storage):
    """"""

    def __init__(self):
        location = tempfile.mkdtemp()
        super().__init__(location=location)

    def __exit__(self, *args):
        # cleanup
        super().__exit__(*args)
        rmtree_readonly(self._location)
