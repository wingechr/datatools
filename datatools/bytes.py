import hashlib
import logging

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
    sha256 = sha256.hexdigest()
    logging.debug(f"SHA256: {sha256}")
    return sha256


def hash_sha256_obj(obj):
    bytes = json_dumps(obj).encode()
    hash = hash_sha256([bytes])
    return hash


def hash_sha256_filepath(filepath):
    logging.debug(f"HASHING {filepath}")
    with open(filepath, "rb") as file:
        sha256 = hash_sha256(iter_bytes(file))
    return sha256
