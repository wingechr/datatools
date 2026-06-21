"""TODO"""

from collections.abc import Iterable
import os
from pathlib import Path
from typing import Any

from ..utils import TextFile
from .types import UID, DataStorage, MetadataAttribute, MetadataStorage, MetadataValue


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

    def __enter__(self) -> MetadataStorage:
        if self._file.path.exists():
            data = {}
        else:
            data = self._file.load_json()
            if not isinstance(data, dict):
                raise ValueError("json file for metadata must be dict")

        self._storage = MemoryMetadataStorage(data)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return self._file.dump_json(self._storage._data)

    def _getitem(self, attribtue: MetadataAttribute) -> Iterable[MetadataValue]:
        return self._storage._getitem(attribtue)

    def _setitem(self, attribtue: MetadataAttribute, value: MetadataValue) -> None:
        return self._storage._setitem(attribtue, value)


class MemoryDataStorage(DataStorage[Any]):
    """TODO"""

    def __init__(self):
        self.__data: dict[UID, Any] = {}
        self.__metadata: dict[UID, MemoryMetadataStorage] = {}

    def _contains(self, uid: UID) -> bool:
        return uid in self.__data

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

    def __init__(self):
        pass

    def _contains(self, uid: UID) -> bool:
        return Path(uid).exists()

    def _getitem(self, uid: UID) -> bytes:
        return Path(uid).read_bytes()

    def _setitem(self, uid: UID, data: bytes) -> None:
        Path(uid).parent.mkdir(parents=True, exist_ok=True)
        Path(uid).write_bytes(data)

    def _delitem(self, uid: UID) -> None:
        os.remove(Path(uid))

    def _metadata(self, uid: UID) -> JsonFileMetadataStorage:
        path = Path(uid)
        path_metadata = path.with_name(path.name + ".metadata.json")
        return JsonFileMetadataStorage(path_metadata)
