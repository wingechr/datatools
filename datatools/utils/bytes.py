import hashlib

from .json import dumps


def hash_bytes(byte_data, method="sha256") -> str:
    hasher = getattr(hashlib, method)()
    hasher.update(byte_data)
    return hasher.hexdigest()


def hash_json(json_data, method="sha256") -> str:
    bytes_data = dumps(json_data).encode()
    return hash_bytes(bytes_data, method=method)


def hash_file(file_path, method="sha256") -> str:
    with open(file_path, "rb") as file:
        bytes_data = file.read()
    return hash_bytes(bytes_data, method=method)
