"""Abstract classes / interfaces, types"""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Generic, TypeVar

Data = TypeVar("Data")
UID = str
MetadataAttribute = str
MetadataValue = str
MetadataPairs = (
    Mapping[MetadataAttribute, MetadataValue]
    | Iterable[tuple[MetadataAttribute, MetadataValue]]
)


class StorageException(Exception):
    """Base class for cutom Exceptions."""

    pass


class StorageFileNotFoundError(FileNotFoundError, StorageException):
    """Data does not exist in storage."""

    pass


class StorageFileExistsError(FileExistsError, StorageException):
    """Data already exists in storage."""

    pass


class StorageInvalidUidError(KeyError, StorageException):
    """UID not valid in this storage.

    has attribute uid for corrected UID.
    """

    def __init__(self, message, uid: UID):
        super().__init__(message)
        self.uid = UID  # corrected UID


class SubprocessStatus(StorageException):
    """TODO."""

    pass


class MetadataStorage(ABC):  # TODO: subclass AbstractContextManager ?
    """Abstract metadata storage."""

    @abstractmethod
    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]: ...

    @abstractmethod
    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None: ...

    def _match(self, **filters: MetadataValue) -> bool:
        return all(value in self[attribute] for attribute, value in filters.items())

    def __getitem__(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        return self._getitem(attribute=attribute)

    def __setitem__(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        return self._setitem(attribute=attribute, value=value)


class DataStorage(ABC, Generic[Data]):
    """Abstract data storage."""

    def __init__(self, location: Any = None):
        self._location = location

    @abstractmethod
    def _contains(self, uid: UID) -> bool: ...

    @abstractmethod
    def _getitem(self, uid: UID) -> Data: ...

    @abstractmethod
    def _setitem(self, uid: UID, data: Data) -> None: ...

    @abstractmethod
    def _delitem(self, uid: UID) -> None: ...

    @abstractmethod
    def _metadata(self, uid: UID) -> MetadataStorage: ...

    @abstractmethod
    def _list(self, **filters: MetadataValue) -> Iterable[UID]: ...

    @classmethod
    def _can_handle_location(cls, location: str) -> bool:
        return False

    def _get_valid_uid(self, uid: UID) -> UID:
        return UID(uid)

    def __iter__(self) -> Iterator[UID]:
        return iter(self._list())

    def __contains__(self, uid: UID) -> bool:
        self._assert_valid_uid(uid=uid)
        return self._contains(uid=uid)

    def __getitem__(self, uid: UID) -> Data:
        self._assert_valid_uid(uid=uid)
        if uid not in self:
            raise StorageFileNotFoundError(f"Not found: {uid}")
        return self._getitem(uid=uid)

    def __setitem__(self, uid: UID, data: Data) -> None:
        self._assert_valid_uid(uid=uid)
        if uid in self:
            raise StorageFileExistsError(f"Already exists: {uid}")
        return self._setitem(uid=uid, data=data)

    def __delitem__(self, uid: UID) -> None:
        self._assert_valid_uid(uid=uid)
        if uid not in self:
            raise StorageFileNotFoundError(f"Not found: {uid}")
        return self._delitem(uid=uid)

    def metadata(self, uid: UID) -> MetadataStorage:
        """Metadata container associated with data."""
        self._assert_valid_uid(uid=uid)
        return self._metadata(uid=uid)

    def list(self, **filters: MetadataValue) -> Iterable[UID]:
        """list UIDs for given metadata query."""
        return self._list(**filters)

    def _assert_valid_uid(self, uid: UID):
        valid_uid = self._get_valid_uid(uid)
        if uid != valid_uid:
            raise StorageInvalidUidError(
                f"Invalid uid: {uid} => {valid_uid}", uid=valid_uid
            )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self._location})"

    def info(self) -> dict:
        """TODO"""
        return {"Location": str(self._location), "Class": str(self.__class__.__name__)}
