import requests

CHUNK_SIZE = 2**20


def download(source_uri, target_file_path):
    with open(target_file_path, "wb") as file:
        with requests.get(source_uri, stream=True) as response:
            response.raise_for_status()
            bytes_sum = 0
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                bytes_sum += len(chunk)
                file.write(chunk)
