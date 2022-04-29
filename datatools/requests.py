import shutil
import tempfile

import requests


def download_file(source_uri, target_filepath):
    CHUNK_SIZE = 2**20
    with tempfile.NamedTemporaryFile(delete=False) as file:
        with requests.get(source_uri, stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                file.write(chunk)
    shutil.move(file.name, target_filepath)
