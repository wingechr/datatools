import hashlib

from .json import json_dumps

CHUNK_SIZE = 2**20


def iter_bytes(file):
    while True:
        chunk = file.read(CHUNK_SIZE)
        if not chunk:
            break
        yield chunk


def hash_sha256(byte_iter):
    sha256 = hashlib.sha256()
    for chunk in byte_iter:
        sha256.update(chunk)
    return sha256.hexdigest()


def hash_sha256_obj(obj):
    bytes = json_dumps(obj).encode()
    return hash_sha256([bytes])


def hash_sha256_filepath(filepath):
    with open(filepath, "rb") as file:
        sha256 = hash_sha256(iter_bytes(file))
    return sha256
