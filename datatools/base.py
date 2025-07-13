import os
from io import DEFAULT_BUFFER_SIZE as _DEFAULT_BUFFER_SIZE
from pathlib import Path
from typing import Any, Callable, Dict, Union

import appdirs

Type = Union[type, str, None, type(Callable)]
ResourceName = str
MetadataKey = str
MetadataValue = Any
ParameterKey = Union[None, int, str]
ParamterTypes = dict[str, Type]
OptionalStr = Union[str, None]
StrPath = Union[Path, str]

FUNCTION_URI_PREFIX = "function://"
PROCESS_URI_PREFIX = "process://"
PARAM_SQL_QUERY = "q"
ROOT_METADATA_PATH = "$"  # root
MEDIA_TYPE_METADATA_PATH = "mediaType"
HASHED_DATA_PATH_PREFIX = "hash"
HASH_METHODS = ["md5", "sha256"]
DEFAULT_HASH_METHOD: str = HASH_METHODS[0]
GLOBAL_LOCATION = os.path.join(
    appdirs.user_data_dir(
        appname="datatools", appauthor=None, version=None, roaming=False
    ),
    "data",
)
DEFAULT_LOCAL_LOCATION = "__data__"
DEFAULT_BUFFER_SIZE = _DEFAULT_BUFFER_SIZE  # default is 1024 * 8, wsgi uses 1024 * 16
DATETIMETZ_FMT = "%Y-%m-%dT%H:%M:%S%z"
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H:%M:%S"
FILEMOD_WRITE = 0o222
ANONYMOUS_USER = "Anonymous"
LOCALHOST = "localhost"
STORAGE_SCHEME = "data"
RESOURCE_URI_PREFIX = f"{STORAGE_SCHEME}:///"


class MetadataDict(Dict[str, Any]):
    pass


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
