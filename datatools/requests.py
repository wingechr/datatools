import logging
import tempfile

import requests

from .files import move


def download_file(source_uri, target_filepath):
    CHUNK_SIZE = 2**20
    with tempfile.NamedTemporaryFile(delete=False) as file:
        logging.debug(f"GET {source_uri} ==> {file.name}")
        with requests.get(source_uri, stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                logging.debug(f"got chunk: {len(chunk)}")
                file.write(chunk)
    move(file.name, target_filepath)
