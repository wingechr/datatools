import logging
import os
import re
from pathlib import Path
from typing import Tuple
from urllib.parse import urlsplit

import requests

from .utils import (
    filepath_abs_to_uri,
    parse_content_type,
    remove_auth_from_uri,
    uri_to_filepath_abs,
)


def read_uri(uri: str) -> Tuple[bytes, str, dict]:
    metadata = {}
    metadata["source.path"] = remove_auth_from_uri(uri)

    url_parts = urlsplit(uri)

    # protocol routing
    if url_parts.scheme == "file":
        file_path = uri_to_filepath_abs(uri)
        with open(file_path, "rb") as file:
            data = file.read()
    elif url_parts.scheme in ["http", "https"]:
        res = requests.get(uri)
        res.raise_for_status()
        content_type = res.headers.get("Content-Type")
        if content_type:
            _meta = parse_content_type(content_type)
            metadata.update(_meta)
            logging.info(_meta)
        data = res.content
    else:
        raise NotImplementedError(url_parts.scheme)

    return data, metadata


def write_uri(uri, data: bytes):
    if not re.match(".+://", uri, re.IGNORECASE):
        # assume local path
        uri = filepath_abs_to_uri(Path(uri).absolute())

    url_parts = urlsplit(uri)
    # protocol routing
    if url_parts.scheme == "file":
        file_path = uri_to_filepath_abs(uri)
        if os.path.exist(file_path):
            raise FileExistsError(file_path)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file:
            file.write(data)
    else:
        raise NotImplementedError(url_parts.scheme)
