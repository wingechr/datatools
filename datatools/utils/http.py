import logging  # noqa
from tempfile import mkstemp

import requests

from ..utils.byte import DEFAULT_CHUNK_SIZE, Iterator
from ..utils.filepath import assert_not_exist, move


def open(source_uri, hash_method=None):
    with requests.get(source_uri, stream=True) as response:
        response.raise_for_status()
        return Iterator(
            response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE),
            hash_method=hash_method,
        )


def download(source_uri, target_file_path, overwrite=False):
    assert_not_exist(target_file_path, overwrite=overwrite)

    file, file_path = mkstemp(text=False)
    for chunk in open(source_uri):
        file.write(chunk)
    file.close()

    move(file_path, target_file_path)
