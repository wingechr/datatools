import logging
import tempfile

import requests

from .files import assert_not_exist, move


def download_file(source_uri, target_filepath, overwrite=False):
    assert_not_exist(target_filepath, overwrite=overwrite)
    CHUNK_SIZE = 2**20
    with tempfile.NamedTemporaryFile(delete=False) as file:
        logging.debug(f"GET {source_uri} ==> {file.name}")
        with requests.get(source_uri, stream=True) as response:
            response.raise_for_status()
            bytes_sum = 0
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                logging.debug(f"got chunk: {len(chunk)}")
                bytes_sum += len(chunk)
                file.write(chunk)
            logging.debug(f"TOTAL BYTES: {bytes_sum:d}")
    move(file.name, target_filepath)
