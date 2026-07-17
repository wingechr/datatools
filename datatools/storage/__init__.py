"""init"""

__all__ = ["FileDataStorage", "HttpDataStorage", "SqlDataStorage", "storage"]


from datatools.storage.base import DataStorage
from datatools.storage.file import FileDataStorage
from datatools.storage.http import HttpDataStorage
from datatools.storage.sql import SqlDataStorage

storage_classes: dict[str, type[DataStorage]] = {
    c.__name__: c for c in [FileDataStorage, HttpDataStorage, SqlDataStorage]
}


def _infer_storage_class(location: str, storage_class=str | None) -> type[DataStorage]:
    """TODO

    this function should be in __main__ so that
    """
    if isinstance(storage_class, str) and storage_class:
        return storage_classes[storage_class]
    for cls in storage_classes.values():
        if cls._can_handle(location):
            return cls
    raise NotImplementedError(f"Cannot infer DataStorage class for location {location}")


def storage(location: str, storage_class=str | None) -> DataStorage:
    """TODO"""
    StorageClass = _infer_storage_class(location, storage_class=storage_class)
    return StorageClass(location)
