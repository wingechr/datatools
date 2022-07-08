import hashlib
import logging  # noqa
from io import BytesIO

import chardet


def hash(byte_data, method="sha256") -> str:
    iterator = Iterator(byte_data, hash_method=method)
    iterator.read()
    return iterator.get_current_hash()


class Iterator:
    __slots__ = ["data_stream", "hasher", "size", "max_size", "chunk_size"]
    DEFAULT_CHUNK_SIZE = 2**24

    def __init__(self, data_stream, chunk_size=None, max_size=None, hash_method=None):
        if hash_method:
            self.hasher = getattr(hashlib, hash_method)()
        else:
            self.hasher = None
        self.size = 0
        self.max_size = max_size
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE

        self.data_stream = data_stream
        if isinstance(self.data_stream, str):
            logging.debug("opening file: %s", self.data_stream)
            self.data_stream = open(self.data_stream, "rb")
        elif isinstance(self.data_stream, bytes):
            logging.debug("buffer data: %s")
            self.data_stream = BytesIO(self.data_stream)

    def __enter__(self):
        logging.debug("OPEN")
        self.data_stream.__enter__()
        return self

    def __exit__(self, *args):
        logging.debug("CLOSE")
        self.data_stream.__exit__(*args)

    def __iter__(self):
        return self

    def __del__(self):
        self.__exit__(None, None, None)

    def __next__(self):
        chunk = self.read(self.chunk_size)
        if not chunk:
            self.data_stream.__exit__(None, None, None)
            raise StopIteration()
        return chunk

    def read(self, size=-1):
        if not self.size:
            self.__enter__()

        if self.max_size and self.size + size > self.max_size:
            size = self.max_size - self.size
        chunk = self.data_stream.read(size)
        self.size += len(chunk)
        logging.debug("read %d bytes (max_size=%d)", len(chunk), size)
        if self.hasher:
            self.hasher.update(chunk)
        return chunk

    def get_current_hash(self):
        if self.hasher:
            return self.hasher.hexdigest()
        else:
            raise Exception("no hasher")

    def get_current_size(self):
        return self.size


def detect_encoding(byte_data, size=None):
    if size > 0:
        byte_data = byte_data[:size]
    return chardet.detect(byte_data)["encoding"]
