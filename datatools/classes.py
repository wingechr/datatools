from typing import Any, Callable, Union
from urllib.parse import ParseResult

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

Url = ParseResult
Type = Union[type, str, None, type(Callable)]
ResourcePath = str
MetadataKey = str
MetadataValue = Any
ParameterKey = Union[None, int, str]
OptionalStr = Union[str, None]


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
