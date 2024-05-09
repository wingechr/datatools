import abc
import datetime
import hashlib
import json
import logging
import os
from io import BytesIO
from typing import Any, Callable, Dict, Tuple, Type, TypeVar

import bs4

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


ByteBuffer = TypeVar("ByteBuffer")
MetaKey = TypeVar("MetaKey", bound=str)
MetaVal = TypeVar("MetaVal", bound=Any)
MetaDict = Dict[MetaKey, MetaVal]
Name = TypeVar("Name", bound=str)
Data = TypeVar("Data", bound=Any)
Source = TypeVar("Source", bound=Any)
Location = TypeVar("Location", bound=Any)
MediaType = TypeVar("MediaType", bound=str)


class StorageError(Exception):
    pass


class ResourceExsists(StorageError):
    pass


class ResourceDoesNotExsists(StorageError):
    pass


class BaseClassMeta(abc.ABCMeta):
    def __init__(cls, name, bases, dct):
        registry = cls._subclasses  # must exist in base class
        if name in registry:
            raise KeyError(f"class name already registered: {name}")
        registry[name] = cls
        super().__init__(name, bases, dct)


class BaseClass(abc.ABC, metaclass=BaseClassMeta):
    _subclasses = {}

    @classmethod
    def _is_class_for(cls, **kwargs) -> bool:
        return False

    @classmethod
    def _get_class(cls, **kwargs) -> Type:
        for subclass in reversed(cls._subclasses.values()):
            if subclass._is_class_for(**kwargs):
                return subclass
        raise NotImplementedError(kwargs)


class Converter(BaseClass):
    _subclasses = {}  # overwrite from BaseClass

    encode_kwargs = []
    decode_kwargs = []

    @classmethod
    def _is_class_for(cls, media_type: MediaType, data_type: Type) -> bool:
        return False

    @classmethod
    def get_instance(cls, media_type: MediaType, data_type: Type) -> "Converter":
        subclass = cls._get_class(media_type=media_type, data_type=data_type)
        return subclass()

    @abc.abstractmethod
    def encode(self, data: Data, **kwargs) -> Tuple[ByteBuffer, MetaDict]:
        ...

    @abc.abstractmethod
    def decode(self, bytes_buffer: ByteBuffer, **kwargs) -> Data:
        ...


class BytesIteratorBuffer:
    def __init__(self, bytes_iter) -> None:
        self.bytes_iter = bytes_iter

        self.buffer = b""
        self.bytes = 0

    def read(self, n=None):
        # read more data

        while True:
            if n is not None and n <= self.bytes:
                # if bytesare specified and we have enough data
                break
            try:
                chunk = next(self.bytes_iter)
            except StopIteration:
                break
            self.buffer += chunk
            logging.info(f"read chunk {len(chunk)}")
            self.bytes += len(chunk)

        # how many do we return
        n = self.bytes if n is None else min(n, self.bytes)
        # split buffer
        data = self.buffer[:n]
        self.buffer = self.buffer[n:]
        return data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class ByteConverter(Converter):
    @classmethod
    def _is_class_for(cls, media_type: MediaType, data_type: Type) -> bool:
        return issubclass(data_type, (bytes, BytesIO, BytesIteratorBuffer))

    def encode(self, data: bytes, **kwargs) -> Tuple[ByteBuffer, MetaDict]:
        if isinstance(data, bytes):
            data = BytesIO(data)
        return data, {}

    def decode(self, bytes_buffer: ByteBuffer, **kwargs) -> bytes:
        return bytes_buffer.read()


class ByteBufferWrapper:
    def __init__(self, buffer):
        self.buffer = buffer
        self.bytes = 0
        self.hash_md5 = hashlib.md5()
        self.hash_sha256 = hashlib.sha256()

    def __enter__(self):
        self.buffer.__enter__()
        return self

    def __exit__(self, *args):
        self.buffer.__exit__(*args)

    def read(self, n=None):
        chunk = self.buffer.read(n)

        self.bytes += len(chunk)
        self.hash_md5.update(chunk)
        self.hash_sha256.update(chunk)

        return chunk


class DataGenerator(BaseClass):
    _subclasses = {}  # overwrite from BaseClass
    create_kwargs = []

    @classmethod
    def _is_class_for(cls, data_source: Source) -> bool:
        return False

    @classmethod
    def get_instance(cls, data_source: Source) -> "DataGenerator":
        subclass = cls._get_class(data_source=data_source)
        return subclass(data_source=data_source)

    def __init__(self, data_source: Source) -> None:
        self._data_source = data_source

    @abc.abstractmethod
    def create_name(self) -> Name:
        ...

    @abc.abstractmethod
    def get_media_data_type(self, name: Name) -> Tuple[MediaType, Type]:
        ...

    @abc.abstractmethod
    def create_data_metadata(self, **kwargs) -> Tuple[Data, MetaDict]:
        ...


class Metadata:
    def update(self, metadata: MetaDict) -> None:
        return self.__storage._metadata_update(
            resource_name=self.__resource_name, metadata=metadata
        )

    def query(self, key: MetaKey) -> MetaVal:
        return self.__storage._metadata_query(
            resource_name=self.__resource_name, key=key
        )

    def __init__(self, storage: "Storage", resource_name: Name) -> None:
        self.__storage = storage
        self.__resource_name = resource_name


class Resource:
    metadata_class = Metadata

    def __init__(
        self, storage: "Storage", data_source: Source, name: Name = None
    ) -> None:
        self.__data_generator = DataGenerator.get_instance(data_source=data_source)

        name = name or self.__data_generator.create_name()
        name = storage._validate_name(name)

        media_type, data_type = self.__data_generator.get_media_data_type(name=name)
        self.__media_type = media_type
        self.__data_type = data_type

        self.__storage = storage
        self.__name = name
        self.__metadata = self.metadata_class(storage=storage, resource_name=name)

    @property
    def metadata(self) -> "Metadata":
        return self.__metadata

    def exists(self) -> bool:
        return self.__storage._resource_exists(resource_name=self.__name)

    def delete(self) -> None:
        if not self.exists():
            raise ResourceDoesNotExsists(self)
        return self.__storage._resource_delete(resource_name=self.__name)

    def save(self) -> None:
        if self.exists():
            raise ResourceExsists(self)

        # get extra kwargs from metadata
        create_kwargs = {
            k: self.__metadata.query(k) for k in self.__data_generator.create_kwargs
        }

        data, metadata_create = self.__data_generator.create_data_metadata(
            **create_kwargs
        )

        data_type = type(data)  # type from actual data, not from resource
        converter = Converter.get_instance(
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
            "created": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "bytes": bytes_buffer_wrapper.bytes,
            "md5": bytes_buffer_wrapper.hash_md5.hexdigest(),
            "sha256": bytes_buffer_wrapper.hash_sha256.hexdigest(),
        }

        self.metadata.update(metadata_create | metadata_encode | metadata_save)

    def load(self) -> Data:
        if not self.exists():
            self.save()

        converter = Converter.get_instance(
            media_type=self.__media_type, data_type=self.__data_type
        )

        # get extra kwargs from metadata
        decode_kwargs = {k: self.__metadata.query(k) for k in converter.decode_kwargs}

        # load bytes and decode
        with self.__storage._bytes_open(resource_name=self.__name) as bytes_buffer:
            data = converter.decode(bytes_buffer, **decode_kwargs)

        return data


class Storage(abc.ABC):
    resource_class = Resource

    def resource(self, data_source, name: Name = None):
        return self.resource_class(storage=self, data_source=data_source, name=name)

    def _validate_name(self, name: Name) -> Name:
        return name

    @abc.abstractmethod
    def _resource_delete(self, resource_name: Name) -> None:
        ...

    @abc.abstractmethod
    def _resource_exists(self, resource_name: Name) -> None:
        ...

    @abc.abstractmethod
    def _metadata_update(self, resource_name: Name, metadata: MetaDict) -> None:
        ...

    @abc.abstractmethod
    def _metadata_query(self, resource_name: Name, key: MetaKey) -> MetaVal:
        ...

    @abc.abstractmethod
    def _bytes_write(self, resource_name: Name, byte_buffer: ByteBuffer) -> None:
        ...

    @abc.abstractmethod
    def _bytes_open(self, resource_name: Name) -> ByteBuffer:
        ...


class TestMemoryStorage(Storage):
    def __init__(self):
        self.__data = {}
        self.__metadata = {}

    def _resource_delete(self, resource_name: Name) -> None:
        logging.debug(f"Deleting {resource_name}")
        del self.__data[resource_name]
        del self.__metadata[resource_name]

    def _resource_exists(self, resource_name: Name) -> None:
        return resource_name in self.__data

    def _metadata_update(self, resource_name: Name, metadata: MetaDict) -> None:
        if resource_name not in self.__metadata:
            self.__metadata[resource_name] = {}
        metadata_res = self.__metadata[resource_name]
        for key, val in metadata.items():
            metadata_res[key] = val

    def _metadata_query(self, resource_name: Name, key: MetaKey) -> MetaVal:
        if resource_name not in self.__metadata:
            self.__metadata[resource_name] = {}
        metadata_res = self.__metadata[resource_name]
        return metadata_res.get(key)

    def _bytes_write(self, resource_name: Name, byte_buffer: ByteBuffer) -> None:
        logging.debug(f"Writing {resource_name}")
        bdata = byte_buffer.read()
        self.__data[resource_name] = bdata

    def _bytes_open(self, resource_name: Name) -> ByteBuffer:
        logging.debug(f"Reading {resource_name}")
        bdata = self.__data[resource_name]
        return BytesIO(bdata)


class TestFileStorage(Storage):
    def __init__(self, location: str = None):
        self.__location = location or "."

    def _get_filepath(self, resource_name: Name):
        return os.path.join(self.__location, resource_name)

    def _get_filepath_metadata(self, resource_name: Name):
        return self._get_filepath(resource_name=resource_name) + ".metadata.json"

    def _resource_delete(self, resource_name: Name) -> None:
        filepath = self._get_filepath(resource_name=resource_name)
        filepath_meta = self._get_filepath_metadata(resource_name=resource_name)
        logging.debug(f"Deleting {filepath}")
        os.remove(filepath)
        if os.path.isfile(filepath_meta):
            os.remove(filepath_meta)

    def _resource_exists(self, resource_name: Name) -> None:
        filepath = self._get_filepath(resource_name=resource_name)
        return os.path.isfile(filepath)

    def _read_metadata(self, filepath_meta):
        if not os.path.isfile(filepath_meta):
            return {}
        with open(filepath_meta, "r", encoding="utf-8") as file:
            return json.load(file)

    def _metadata_update(self, resource_name: Name, metadata: MetaDict) -> None:
        filepath_meta = self._get_filepath_metadata(resource_name=resource_name)
        metadata_all = self._read_metadata(filepath_meta)
        if resource_name not in metadata_all:
            metadata_all[resource_name] = {}
        metadata_res = metadata_all[resource_name]
        for key, val in metadata.items():
            metadata_res[key] = val
        sdata = json.dumps(metadata_all, ensure_ascii=False, indent=2)
        with open(filepath_meta, "w", encoding="utf-8") as file:
            file.write(sdata)

    def _metadata_query(self, resource_name: Name, key: MetaKey) -> MetaVal:
        filepath_meta = self._get_filepath_metadata(resource_name=resource_name)
        metadata_all = self._read_metadata(filepath_meta)
        if resource_name not in metadata_all:
            metadata_all[resource_name] = {}
        metadata_res = metadata_all[resource_name]
        return metadata_res.get(key)

    def _bytes_write(self, resource_name: Name, byte_buffer: ByteBuffer) -> None:
        filepath = self._get_filepath(resource_name=resource_name)
        logging.debug(f"Writing {filepath}")
        with open(filepath, "wb") as file:
            file.write(byte_buffer.read())

    def _bytes_open(self, resource_name: Name) -> ByteBuffer:
        filepath = self._get_filepath(resource_name=resource_name)
        logging.debug(f"Reading {filepath}")
        return open(filepath, "rb")


class FunctionDataGenerator(DataGenerator):
    @classmethod
    def _is_class_for(cls, data_source: Source) -> bool:
        return isinstance(data_source, Callable)

    def create_name(self) -> Name:
        # return fnuction name
        function = self._data_source
        return function.__name__

    def get_media_data_type(self, name: Name) -> Tuple[MediaType, Type]:
        return "text/plain", str

    def create_data_metadata(self, **kwargs) -> Tuple[Data, MetaDict]:
        data = self._data_source()
        metadata = {}
        return data, metadata


class HttpDataGenerator(DataGenerator):
    @classmethod
    def _is_class_for(cls, data_source: Source) -> bool:
        return isinstance(data_source, str) and (
            data_source.startswith("http://") or data_source.startswith("https://")
        )

    def create_name(self) -> Name:
        return "example.html"

    def get_media_data_type(self, name: Name) -> Tuple[MediaType, Type]:
        return "text/plain", bs4.BeautifulSoup

    def create_data_metadata(self, **kwargs) -> Tuple[Data, MetaDict]:
        import requests

        res = requests.get(self._data_source, stream=True)
        res.raise_for_status()
        print(res.headers)
        # max_bytes = int(res.headers["Content-Length"])
        chunk_size = 1024
        bytes_iter = res.iter_content(chunk_size=chunk_size)
        data = BytesIteratorBuffer(bytes_iter=bytes_iter)

        metadata = dict({"media_type": res.headers["Content-Type"]})
        return data, metadata


class StringConverter(Converter):
    decode_kwargs = ["encoding"]
    encode_kwargs = ["encoding"]
    default_encoding = "utf-8"

    @classmethod
    def _is_class_for(cls, media_type: MediaType, data_type: Type) -> bool:
        return issubclass(data_type, str) and media_type.startswith("text/")

    def encode(
        self, data: Data, encoding=None, **kwargs
    ) -> Tuple[ByteBuffer, MetaDict]:
        encoding = encoding or self.default_encoding
        metadata = {"encoding": encoding}
        bdata = data.encode(encoding=encoding)
        buffer = BytesIO(bdata)
        return buffer, metadata

    def decode(self, bytes_buffer: ByteBuffer, encoding=None, **kwargs) -> Data:
        encoding = encoding or self.default_encoding
        bdata = bytes_buffer.read()
        sdata = bdata.decode(encoding=encoding)
        return sdata


class HtmlBs4Converter(Converter):
    @classmethod
    def _is_class_for(cls, media_type: MediaType, data_type: Type) -> bool:
        return issubclass(data_type, bs4.BeautifulSoup)

    def encode(self, data: bs4.BeautifulSoup, **kwargs) -> Tuple[ByteBuffer, MetaDict]:
        return data.prettify(), {}

    def decode(self, bytes_buffer: ByteBuffer, **kwargs) -> bs4.BeautifulSoup:
        return bs4.BeautifulSoup(bytes_buffer, features="lxml")


_data = "test"


def create_data():
    return _data


st = TestFileStorage()
res = st.resource(create_data)
res = st.resource("https://example.com")

# if res.exists():
#    res.delete()

print(res.exists())
# res.metadata.update({"encoding": "utf-8"})
print(_data, type(_data), res.exists())
data = res.load()
print(type(data), res.exists(), res.metadata.query("md5"))
# res.delete()
# print(res.exists(), res.metadata.query("encoding"))
