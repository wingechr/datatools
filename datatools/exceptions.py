"""Exceptions."""

from datatools.types import Name


class StorageException(Exception):
    """Base class for cutom Exceptions."""

    pass


class StorageFileNotFoundError(FileNotFoundError, StorageException):
    """Data does not exist in storage."""

    pass


class StorageFileExistsError(FileExistsError, StorageException):
    """Data already exists in storage."""

    pass


class StorageInvalidNameError(KeyError, StorageException):
    """Name not valid in this storage.

    has attribute name for corrected Name.
    """

    def __init__(self, message, name: Name):
        super().__init__(message)
        self.name = name  # corrected Name


class SubprocessStatus(StorageException):
    """TODO."""

    pass
