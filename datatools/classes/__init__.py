from typing import Any, Union

__all__ = [
    "StorageException",
    "ProcessException",
    "ConverterException",
    "Type",
    "ResourcePath",
    "MetadataKey",
    "MetadataValue",
    "ParameterKey",
]


Type = Union[type, str, None]
ResourcePath = str
MetadataKey = str
MetadataValue = Any
ParameterKey = Union[None, int, str]


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
