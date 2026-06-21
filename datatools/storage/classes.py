"""TODO"""

from collections.abc import Iterable
import logging
import os
from pathlib import Path
from typing import Any

from ..utils import TextFile
from .types import (
    UID,
    DataStorage,
    MetadataAttribute,
    MetadataStorage,
    MetadataValue,
    StorageInvalidUidError,
)


class MemoryMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, data: dict | None = None):
        self._data = {} if data is None else data

    def _getitem(self, attribtue: MetadataAttribute) -> Iterable[MetadataValue]:
        value = self._data.get(attribtue)
        if value is None:
            return []
        else:
            return [value]

    def _setitem(self, attribtue: MetadataAttribute, value: MetadataValue) -> None:
        self._data[attribtue] = value


class JsonFileMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, path: Path):
        self._file = TextFile(path)
        self._storage: MemoryMetadataStorage  # created in __enter__
        self._changed: bool

    def __enter__(self) -> MetadataStorage:
        self._changed = False
        if not self._file.exists():
            data = {}
        else:
            data = self._file.load_json()
            if not isinstance(data, dict):
                raise ValueError("json file for metadata must be dict")

        self._storage = MemoryMetadataStorage(data)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._changed:
            self._file.dump_json(self._storage._data)
        self._changed = False

    def _getitem(self, attribtue: MetadataAttribute) -> Iterable[MetadataValue]:
        return self._storage._getitem(attribtue)

    def _setitem(self, attribtue: MetadataAttribute, value: MetadataValue) -> None:
        self._changed = True
        return self._storage._setitem(attribtue, value)


class MemoryDataStorage(DataStorage[Any]):
    """TODO"""

    def __init__(self, location: str | None = None):
        super().__init__(location=":memory")
        self.__data: dict[UID, Any] = {}
        self.__metadata: dict[UID, MemoryMetadataStorage] = {}

    def _contains(self, uid: UID) -> bool:
        return uid in self.__data

    def _iter(self) -> Iterable[UID]:
        return iter(self.__data)

    def _getitem(self, uid: UID) -> Any:
        return self.__data[uid]

    def _setitem(self, uid: UID, data: Any) -> None:
        self.__data[uid] = data
        if uid not in self.__metadata:
            self.__metadata[uid] = MemoryMetadataStorage()

    def _delitem(self, uid: UID) -> None:
        del self.__data[uid]
        # dont delete metadata

    def _metadata(self, uid: UID) -> MemoryMetadataStorage:
        return self.__metadata[uid]


class FileDataStorage(DataStorage[bytes]):
    """TODO"""

    metadata_sufix = ".metadata.json"

    def __init__(self, location: str | None = None):
        self._location: Path  # absolute, resolved location
        super().__init__(location=Path(location or ".").resolve())

    def _contains(self, uid: UID) -> bool:
        path = self._get_abs_path(uid)
        return path.exists()

    def _getitem(self, uid: UID) -> bytes:
        path = self._get_abs_path(uid)
        logging.debug("Reading %s", path)
        return path.read_bytes()

    def _setitem(self, uid: UID, data: bytes) -> None:
        path = self._get_abs_path(uid)
        logging.debug("Writing %s", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def _delitem(self, uid: UID) -> None:
        path = self._get_abs_path(uid)
        logging.debug("Deleting %s", path)
        os.remove(path)

    def _iter(self) -> Iterable[UID]:
        for root, _, fs in os.walk(self._location):
            for f in fs:
                if f.endswith(self.metadata_sufix):
                    continue
                path = Path(root) / str(f)
                uid = str(path.relative_to(self._location))
                yield uid

    def _metadata(self, uid: UID) -> JsonFileMetadataStorage:
        path = self._get_abs_path(uid)
        path_metadata = path.with_name(path.name + self.metadata_sufix).resolve()
        return JsonFileMetadataStorage(path_metadata)

    def _get_abs_path(self, uid: UID) -> Path:
        return (self._location / uid).resolve()

    def _get_valid_uid(self, uid: UID) -> UID:
        """should be a relative path"""
        abs_path = self._get_abs_path(uid)
        if not abs_path.is_relative_to(self._location):
            raise StorageInvalidUidError(
                f"Cannot use uid outside of storage location: {uid}", uid=UID()
            )
        if abs_path.exists() and not abs_path.is_file():
            raise StorageInvalidUidError(f"uid is cannot be a file: {uid}", uid=UID())
        return UID(abs_path.relative_to(self._location))
