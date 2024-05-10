import abc
import re
from io import BytesIO, RawIOBase
from typing import Any, Dict, Tuple, Type

from datatools.classes import RegistryAbstractBase
from datatools.generators import AbstractDataGenerator
from datatools.utils import BytesIteratorBuffer


class AbstractConverter(RegistryAbstractBase):
    _subclasses = {}  # overwrite from BaseClass

    encode_kwargs = []
    decode_kwargs = []

    @classmethod
    def _is_class_for(cls, media_type: str, data_type: Type) -> bool:
        return False

    @classmethod
    def get_instance(cls, media_type: str, data_type: Type) -> "AbstractConverter":
        subclass = cls._get_class(media_type=media_type, data_type=data_type)
        return subclass()

    @abc.abstractmethod
    def encode(self, data: Any, **kwargs) -> Tuple[RawIOBase, Dict[str, Any]]:
        ...

    @abc.abstractmethod
    def decode(self, bytes_buffer: RawIOBase, **kwargs) -> Any:
        ...


class StringConverter(AbstractConverter):
    decode_kwargs = ["encoding"]
    encode_kwargs = ["encoding"]
    default_encoding = "utf-8"

    @classmethod
    def _is_class_for(cls, media_type: str, data_type: Type) -> bool:
        return issubclass(data_type, str) and media_type.startswith("text/")

    def encode(
        self, data: Any, encoding=None, **kwargs
    ) -> Tuple[RawIOBase, Dict[str, Any]]:
        encoding = encoding or self.default_encoding
        metadata = {"encoding": encoding}
        bdata = data.encode(encoding=encoding)
        buffer = BytesIO(bdata)
        return buffer, metadata

    def decode(self, bytes_buffer: RawIOBase, encoding=None, **kwargs) -> Any:
        encoding = encoding or self.default_encoding
        bdata = bytes_buffer.read()
        sdata = bdata.decode(encoding=encoding)
        return sdata


class HttpDataGenerator(AbstractDataGenerator):
    @classmethod
    def _is_class_for(cls, data_source: Any) -> bool:
        return isinstance(data_source, str) and re.match(r"^https?://", data_source)

    def create_name(self) -> str:
        raise NotImplementedError()

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        # this can be all kinds of things.
        # SHOULD be determined from suffix, but that will not always work
        return super().get_media_data_type(name=name)

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
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


class ByteConverter(AbstractConverter):
    @classmethod
    def _is_class_for(cls, media_type: str, data_type: Type) -> bool:
        return issubclass(data_type, (bytes, BytesIO, BytesIteratorBuffer))

    def encode(self, data: bytes, **kwargs) -> Tuple[RawIOBase, Dict[str, Any]]:
        if isinstance(data, bytes):
            data = BytesIO(data)
        return data, {}

    def decode(self, bytes_buffer: RawIOBase, **kwargs) -> bytes:
        return bytes_buffer.read()
