"""Abstract classes / interfaces, types"""

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import ParamSpec, TypeAlias, TypeVar

FunParams = ParamSpec("FunParams")
FunResult = TypeVar("FunResult")
SubCls = TypeVar("SubCls")

Json: TypeAlias = str | int | float | bool | None | list["Json"] | dict[str, "Json"]
StrPath = Path | str
UID = str
ByteData = bytes
MetadataAttribute = str
MetadataValue: TypeAlias = Json
MetadataPairs: TypeAlias = (
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
