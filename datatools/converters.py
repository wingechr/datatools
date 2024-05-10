import abc
import json
import pickle
from io import BytesIO, IOBase
from typing import Any, Dict, Tuple, Type

import pandas as pd

from .classes import RegistryAbstractBase
from .utils import json_serialize


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
    def encode(self, data: Any, **kwargs) -> Tuple[IOBase, Dict[str, Any]]:
        ...

    @abc.abstractmethod
    def decode(self, bytes_buffer: IOBase, **kwargs) -> Any:
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
    ) -> Tuple[IOBase, Dict[str, Any]]:
        encoding = encoding or self.default_encoding
        metadata = {"encoding": encoding}
        bdata = data.encode(encoding=encoding)
        buffer = BytesIO(bdata)
        return buffer, metadata

    def decode(self, bytes_buffer: IOBase, encoding=None, **kwargs) -> Any:
        encoding = encoding if encoding is not None else self.default_encoding
        bdata = bytes_buffer.read()
        sdata = bdata.decode(encoding=encoding)
        return sdata


class JsonConverter(StringConverter):
    decode_kwargs = ["encoding"]
    encode_kwargs = ["encoding", "indent"]
    default_encoding = "utf-8"
    default_indent = 2

    @classmethod
    def _is_class_for(cls, media_type: str, data_type: Type) -> bool:
        return media_type in ("application/json", "text/json")

    def encode(
        self, data: Any, encoding=None, indent=None, **kwargs
    ) -> Tuple[IOBase, Dict[str, Any]]:
        encoding = encoding if encoding is not None else self.default_encoding
        indent = indent if indent is not None else self.default_indent

        metadata = {"encoding": encoding, "indent": indent}
        sdata = json.dumps(
            data,
            indent=indent,
            ensure_ascii=False,
            sort_keys=False,
            default=json_serialize,
        )
        bdata = sdata.encode()
        buffer = BytesIO(bdata)
        return buffer, metadata

    def decode(self, bytes_buffer: IOBase, encoding=None, **kwargs) -> Any:
        encoding = encoding or self.default_encoding
        bdata = bytes_buffer.read()
        sdata = bdata.decode(encoding=encoding)
        data = json.loads(sdata)
        return data


class PickleConverter(AbstractConverter):
    @classmethod
    def _is_class_for(cls, media_type: str, data_type: Type) -> bool:
        return media_type == "application/x-pickle"

    def encode(self, data: object, **kwargs) -> Tuple[IOBase, Dict[str, Any]]:
        return BytesIO(pickle.dumps(data)), {}

    def decode(self, bytes_buffer: IOBase, **kwargs) -> bytes:
        return pickle.load(bytes_buffer)


class BytesPassthroughConverter(AbstractConverter):
    @classmethod
    def _is_class_for(cls, media_type: str, data_type: Type) -> bool:
        return issubclass(data_type, (bytes, IOBase))

    def encode(self, data: Any, **kwargs) -> Tuple[IOBase, Dict[str, Any]]:
        if isinstance(data, bytes):
            data = BytesIO(data)
        return data, {}

    def decode(self, bytes_buffer: IOBase, **kwargs) -> Any:
        return bytes_buffer.read()


class TestPandasCsvConverter(AbstractConverter):
    @classmethod
    def _is_class_for(cls, media_type: str, data_type: Type) -> bool:
        return issubclass(data_type, pd.DataFrame) and media_type == "text/csv"

    def encode(
        self, data: pd.DataFrame, encoding=None, **kwargs
    ) -> Tuple[IOBase, Dict[str, Any]]:
        metadata = {}
        # TODO: settings
        buf = BytesIO()  # TODO: bufferes reader/writer?
        # TODO: index?
        data.to_csv(buf, index=False)
        buf.seek(0)
        return buf, metadata

    def decode(self, bytes_buffer: IOBase, encoding=None, **kwargs) -> Any:
        return pd.read_csv(bytes_buffer)
