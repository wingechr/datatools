import re
from urllib.parse import unquote_plus

import unidecode

from .exceptions import InvalidPath


def normalize_path(path: str) -> str:
    """should be all lowercase"""
    _path = path  # save original
    path = unquote_plus(path)
    path = path.lower()
    for cin, cout in [
        ("ä", "ae"),
        ("ö", "oe"),
        ("ü", "ue"),
        ("ß", "ss"),
    ]:
        path = path.replace(cin, cout)
    path = unidecode.unidecode(path)
    path = path.replace("\\", "/")
    path = re.sub("/+", "/", path)
    path = re.sub("[^a-z0-9/_.-]+", " ", path)
    path = path.strip()
    path = re.sub(r"\s+", "_", path)
    path = path.strip("/")
    if not path:
        raise InvalidPath(_path)
    return path
