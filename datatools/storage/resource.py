"""TODO"""

import logging
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from datatools.storage import DataStorage
    from datatools.storage.base import MetadataStorage
    from datatools.types import ByteData, ReadableByteBuffer


class Resource:
    """TODO"""

    def __init__(self, name: str, storage: Union["DataStorage", str] = "."):
        # cannot import in module
        from datatools.storage import DataStorage, storage as get_storage

        if not isinstance(storage, DataStorage):
            storage = get_storage(storage)
        self._storage = storage
        self._name = name

    def __str__(self) -> str:
        return self._name

    def exists(self) -> bool:
        """TODO"""
        return self._storage.has(self._name)

    def open(self) -> "ReadableByteBuffer":
        """TODO"""
        return self._storage.open(self._name)

    def read(self) -> bytes:
        """TODO"""
        return self._storage.read(self._name)

    def write(self, data: "ByteData") -> None:
        """TODO"""
        return self._storage.write(self._name, data)

    def delete(self) -> None:
        """TODO"""
        return self._storage.delete(self._name)

    @property
    def metadata(self) -> "MetadataStorage":
        """Metadata container associated with data."""
        return self._storage.metadata(self._name)

    def import_from_uri(
        self, uri: str, exist_ok: bool = False, **options
    ) -> "Resource":
        """TODO"""
        new_name = self._storage.import_from_uri(
            uri, self._name, exist_ok=exist_ok, **options
        )
        if new_name != self._name:
            logging.warning("Changed name to %s", new_name)
            self._name = new_name
        return self
