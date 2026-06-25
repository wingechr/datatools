"""Abstract classes / interfaces, types"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping
import functools
import hashlib
import json
import pickle
from typing import Any, TypeVar

JsonPrimitive = str | float | int | bool | None
Json = JsonPrimitive | list[JsonPrimitive] | dict[str, JsonPrimitive]

UID = str
ByteData = bytes
MetadataAttribute = str
MetadataValue = Json
MetadataPairs = (
    Mapping[MetadataAttribute, MetadataValue]
    | Iterable[tuple[MetadataAttribute, MetadataValue]]
)

XSub = TypeVar("XSub")


def iter_subclasses(cls: type[XSub]) -> Iterable[type[XSub]]:
    """TODO"""
    yield cls
    for subcls in cls.__subclasses__():
        yield from iter_subclasses(subcls)


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


class DataStorage(ABC):
    """Abstract data storage."""

    def __init__(self, location: Any = None):
        self._location = location

    @abstractmethod
    def _contains(self, uid: UID) -> bool: ...

    @abstractmethod
    def _getitem(self, uid: UID) -> ByteData: ...

    @abstractmethod
    def _setitem(self, uid: UID, data: ByteData) -> None: ...

    @abstractmethod
    def _delitem(self, uid: UID) -> None: ...

    @abstractmethod
    def _metadata(self, uid: UID) -> MetadataStorage: ...

    @abstractmethod
    def _list(self, **filters: MetadataValue) -> Iterable[UID]: ...

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        return False

    def _get_valid_uid(self, uid: UID) -> UID:
        return UID(uid)

    def __iter__(self) -> Iterator[UID]:
        return iter(self._list())

    def __contains__(self, uid: UID) -> bool:
        self._assert_valid_uid(uid=uid)
        return self._contains(uid=uid)

    def __getitem__(self, uid: UID) -> ByteData:
        self._assert_valid_uid(uid=uid)
        if uid not in self:
            raise StorageFileNotFoundError(f"Not found: {uid}")
        return self._getitem(uid=uid)

    def __setitem__(self, uid: UID, data: ByteData) -> None:
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

    def import_from_uri(self, uri: str, **options) -> UID:
        """TODO"""
        importer_class = infer_importer_class(uri, **options)
        importer = importer_class(data_storage=self, uri=uri, **options)
        return importer()

    def cache(self, *args, **kwargs) -> Callable:
        """TODO"""

        Result = TypeVar("Result")

        def decorator(fun: Callable[..., Result]) -> Callable:
            """TODO"""

            function_id = fun.__name__

            def get_output_uid(args: tuple, kwargs: dict) -> UID:
                hash_data = {"function": function_id, "args": args, "kwargs": kwargs}
                hash_data_s = json.dumps(
                    hash_data, ensure_ascii=False, indent=0, sort_keys=True
                )
                hash_data_b = hash_data_s.encode("utf-8")
                hashsum = hashlib.md5(hash_data_b).hexdigest()  # noqa:S324
                return hashsum

            def dump_output(result: Result) -> ByteData:
                return pickle.dumps(result)  # type:ignore

            def load_output(data: ByteData) -> Result:
                return pickle.loads(data)  # type:ignore # noqa

            @functools.wraps(fun)
            def _fun(*args, **kwargs):
                output_uid = get_output_uid(args, kwargs)
                if output_uid not in self:
                    # call base function
                    # NOTE: we only support namedarguments
                    result = fun(*args, **kwargs)
                    self[output_uid] = dump_output(result)

                # retrieval
                data = self[output_uid]
                result = load_output(data)
                return result

            return _fun

        return decorator


class Importer(ABC):
    """TODO"""

    @classmethod
    def _can_handle(cls, uri: str, **options) -> bool:
        return False

    def __init__(self, data_storage: DataStorage, uri: str, **options):
        self._data_storage = data_storage
        self._options = options
        self._uri = uri

    @abstractmethod
    def _get_data_and_metadata(
        self, uri: str, **options
    ) -> tuple[bytes, MetadataPairs]: ...

    @abstractmethod
    def _get_output_uid(self, uri: str, **options) -> UID: ...

    def _get_valid_output_uid(self, uri: str, **options) -> UID:
        output_uid = self._get_output_uid(self._uri, **self._options)
        if output_uid in self._data_storage:
            raise StorageFileExistsError(output_uid)
        return output_uid

    def __call__(self) -> UID:
        """TODO"""
        output_uid = self._get_valid_output_uid(self._uri, **self._options)
        data, metadata = self._get_data_and_metadata(self._uri, **self._options)
        self._data_storage[output_uid] = data
        metadata_storage = self._data_storage.metadata(output_uid)
        for k, v in metadata:
            metadata_storage[k] = v
        return output_uid


def _find_import_classes() -> dict[str, type[Importer]]:
    return {c.__name__: c for c in list(iter_subclasses(Importer))[1:]}


def infer_importer_class(uri: str, **options) -> type[Importer]:
    """TODO"""
    for cls in _find_import_classes().values():
        if cls._can_handle(uri, **options):
            return cls
    raise NotImplementedError(f"Cannot infer Importer class for {uri}")


def _find_storage_classes() -> dict[str, type[DataStorage]]:
    return {c.__name__: c for c in list(iter_subclasses(DataStorage))[1:]}


def infer_storage_class(location: str, storage_class=str | None) -> type[DataStorage]:
    """TODO"""
    storage_classes = _find_storage_classes()
    if isinstance(storage_class, str) and storage_class:
        return storage_classes[storage_class]
    for cls in storage_classes.values():
        if cls._can_handle(location):
            return cls
    raise NotImplementedError(f"Cannot infer DataStorage class for location {location}")


def as_storage(storage: str | DataStorage) -> DataStorage:
    """TODO"""
    if not isinstance(storage, DataStorage):
        storage = infer_storage_class(storage)(storage)

    return storage
