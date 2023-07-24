import os
import re
from pathlib import Path
from typing import Tuple
from urllib.parse import urlsplit

import requests

from .utils import (
    file_uri_to_path,
    normalize_path,
    path_to_file_uri,
    remove_auth_from_uri,
    uri_to_data_path,
)


def read_uri(uri: str) -> Tuple[bytes, str, dict]:
    if not re.match(".+://", uri, re.IGNORECASE):
        # assume local path
        uri = path_to_file_uri(Path(uri).absolute())

    metadata = {}
    metadata["source.path"] = remove_auth_from_uri(uri)

    url = urlsplit(uri)
    # protocol routing
    if url.scheme == "file":
        file_path = file_uri_to_path(uri)
        with open(file_path, "rb") as file:
            data = file.read()
    elif url.scheme in ["http", "https"]:
        res = requests.get(uri)
        res.raise_for_status()
        data = res.content
    else:
        raise NotImplementedError(url.scheme)

    data_path = normalize_path(uri_to_data_path(uri))

    return data, data_path, metadata


def write_uri(uri, data: bytes):
    if not re.match(".+://", uri, re.IGNORECASE):
        # assume local path
        uri = path_to_file_uri(Path(uri).absolute())

    url = urlsplit(uri)
    # protocol routing
    if url.scheme == "file":
        file_path = file_uri_to_path(uri)
        if os.path.exist(file_path):
            raise FileExistsError(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file:
            file.write(data)
    else:
        raise NotImplementedError(url.scheme)
