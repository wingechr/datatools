import os
from io import DEFAULT_BUFFER_SIZE as _DEFAULT_BUFFER_SIZE

import appdirs

PARAM_SQL_QUERY = "q"
ROOT_METADATA_PATH = "$"  # root
HASHED_DATA_PATH_PREFIX = "hash"
ALLOWED_HASH_METHODS = ["md5", "sha256"]

DEFAULT_HASH_METHOD = ALLOWED_HASH_METHODS[0]

GLOBAL_LOCATION = os.path.join(
    appdirs.user_data_dir(
        appname="datatools", appauthor=None, version=None, roaming=False
    ),
    "data",
)
LOCAL_LOCATION = "__data__"

# default is 1024 * 8
# wsgi often uses 1024 * 16
DEFAULT_BUFFER_SIZE = _DEFAULT_BUFFER_SIZE


DATETIMETZ_FMT = "%Y-%m-%dT%H:%M:%S%z"
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H:%M:%S"
FILEMOD_WRITE = 0o222
ANONYMOUS_USER = "Anonymous"
LOCALHOST = "localhost"


STORAGE_SCHEME = "data"
