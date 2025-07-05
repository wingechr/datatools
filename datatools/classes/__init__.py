import inspect
from typing import Any, Union

__all__ = [
    "StorageException",
    "ProcessException",
    "ConverterException",
    "Type",
    "UnknownType",
    "ResourcePath",
    "MetadataKey",
    "MetadataValue",
    "Key",
]

Type = Any
UnknownType = inspect._empty
ResourcePath = str
MetadataKey = str
MetadataValue = Any
Key = Union[None, int, str]


class DatatoolsException(Exception):
    pass


class StorageException(DatatoolsException):
    pass


class InvalidPathException(StorageException):
    pass


class ConverterException(DatatoolsException):
    pass


class ProcessException(DatatoolsException):
    pass
