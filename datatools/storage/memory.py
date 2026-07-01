"""TODO"""

from abc import abstractmethod
from collections.abc import Iterable
from typing import Any

from datatools.storage.base import DataStorage, MetadataStorage
from datatools.types import UID, MetadataAttribute, MetadataValue
from datatools.utils import jsonpath_get, jsonpath_update


class MemoryMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, data: dict | None = None):
        self._data = {} if data is None else data

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        return jsonpath_get(data=self._data, key=attribute)

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        jsonpath_update(data=self._data, key=attribute, val=value)


class PersistentMemoryMetadataStorage(MemoryMetadataStorage):
    """TODO"""

    def __init__(self):
        super().__init__(data=self._load_or_init())
        self._changed = False

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        super()._setitem(attribute=attribute, value=value)
        self._changed = True

    def __del__(self):
        if self._changed:
            self._dump(self._data)

    @abstractmethod
    def _load_or_init(self) -> dict | None: ...

    @abstractmethod
    def _dump(self, data: dict) -> None: ...


class MemoryDataStorage(DataStorage):
    """TODO"""

    def __init__(self):
        super().__init__(location=None)
        self.__data: dict[UID, Any] = {}
        self.__metadata: dict[UID, MemoryMetadataStorage] = {}

    def _contains(self, uid: UID) -> bool:
        return uid in self.__data

    def _getitem(self, uid: UID) -> Any:
        return self.__data[uid]

    def _setitem(self, uid: UID, data: Any) -> None:
        self.__data[uid] = data

    def _delitem(self, uid: UID) -> None:
        del self.__data[uid]
        # dont delete metadata

    def _list(self) -> Iterable[UID]:
        return self.__data.keys()

    def _metadata(self, uid: UID) -> MemoryMetadataStorage:
        if uid not in self.__metadata:
            self.__metadata[uid] = MemoryMetadataStorage()
        return self.__metadata[uid]
