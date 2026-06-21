"""TODO"""

from collections.abc import Iterable
from typing import Any

from .types import UID, DataStorage, MetadataAttribute, MetadataStorage, MetadataValue


class MemoryMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self):
        self.__data = {}

    def _getitem(self, attribtue: MetadataAttribute) -> Iterable[MetadataValue]:
        value = self.__data.get(attribtue)
        if value is None:
            return []
        else:
            return [value]

    def _setitem(self, attribtue: MetadataAttribute, value: MetadataValue) -> None:
        self.__data[attribtue] = value


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
