"""TODO"""

from abc import abstractmethod
from collections.abc import Iterable
from typing import Any

from datatools.storage.base import DataStorage, MetadataStorage
from datatools.types import MetadataAttribute, MetadataValue, Name
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
        self._changed = False
        super().__init__(data=self._load_or_init())

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

    def __init__(self, location=None):
        # _location: unused - only for harmonized interface
        super().__init__(location=None)
        self.__data: dict[Name, Any] = {}
        self.__metadata: dict[Name, MemoryMetadataStorage] = {}

    def _contains(self, name: Name) -> bool:
        return name in self.__data

    def _getitem(self, name: Name) -> Any:
        return self.__data[name]

    def _setitem(self, name: Name, data: Any) -> None:
        self.__data[name] = data

    def _delitem(self, name: Name) -> None:
        del self.__data[name]
        # dont delete metadata

    def _list(self) -> Iterable[Name]:
        return self.__data.keys()

    def _metadata(self, name: Name) -> MemoryMetadataStorage:
        if name not in self.__metadata:
            self.__metadata[name] = MemoryMetadataStorage()
        return self.__metadata[name]
