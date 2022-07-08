import hashlib
import logging  # noqa
from io import BytesIO

import chardet

from .. import utils  # noqa


def hash(byte_data, method="sha256") -> str:
    hasher = getattr(hashlib, method)()
    hasher.update(byte_data)
    return hasher.hexdigest()


class HashedIterator:
    DEFAULT_CHUNK_SIZE = 2**24

    def __init__(
        self, data_stream, expected_hash=None, chunk_size=None, max_bytes=None
    ):
        self.data_stream = data_stream
        self.hash = hashlib.sha256()
        self.size_bytes = 0
        self.max_bytes = max_bytes
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE
        self.expected_hash = expected_hash

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.data_stream:
            self.data_stream.__exit__(*args)

    def _stop_iteration(self):
        self.data_stream.__exit__(None, None, None)
        if self.expected_hash:
            file_id = self.get_current_hash()
            if file_id != self.expected_hash:
                raise Exception(self.expected_hash)
            else:
                logging.debug("Integrity check ok")

    def __iter__(self):
        if isinstance(self.data_stream, str):
            logging.debug("opening file: %s", self.data_stream)
            self.data_stream = open(self.data_stream, "rb")
        elif isinstance(self.data_stream, bytes):
            logging.debug("buffer data: %s")
            self.data_stream = BytesIO(self.data_stream)
        self.data_stream.__enter__()
        return self

    def __next__(self):
        chunk_size = self.chunk_size
        if self.max_bytes and self.size_bytes + chunk_size > self.max_bytes:
            chunk_size = self.max_bytes - self.size_bytes
        chunk = self.read(chunk_size)
        if not chunk:
            self._stop_iteration()
            raise StopIteration()
        self.size_bytes += len(chunk)
        return chunk

    def read(self, size=-1):
        chunk = self.data_stream.read(size)
        logging.debug("read %d bytes (max_bytes=%d)", len(chunk), size)
        self.hash.update(chunk)
        return chunk

    def get_current_hash(self):
        return self.hash.hexdigest()

    def get_current_size_bytes(self):
        return self.size_bytes


def detect_encoding(byte_data, size=None):
    if size > 0:
        byte_data = byte_data[:size]
    return chardet.detect(byte_data)["encoding"]
