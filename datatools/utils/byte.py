import hashlib
import logging  # noqa
from io import BufferedReader, BytesIO

import chardet

DEFAULT_CHUNK_SIZE = 2**24


def hash(byte_data, method="sha256") -> str:
    iterator = Iterator(byte_data, hash_method=method)
    iterator.read()
    return iterator.get_current_hash()


def validate(byte_data, method, hashsum):
    data_hashsum = hash(byte_data, method=method)
    if hashsum != data_hashsum:
        raise Exception(
            "Validation Error: expected %s does not match %s", hashsum, data_hashsum
        )


def validate_hash(byte_data, hash):
    method, hashsum = hash.split(":")
    return validate(byte_data, method, hashsum)


class Iterator:
    __slots__ = [
        "data_stream",
        "hasher",
        "size",
        "max_size",
        "chunk_size",
        "iter_buffer",
    ]

    def __init__(self, data_stream, chunk_size=None, max_size=None, hash_method=None):
        if hash_method:
            self.hasher = getattr(hashlib, hash_method)()
        else:
            self.hasher = None
        self.size = 0
        self.max_size = max_size
        self.chunk_size = chunk_size or DEFAULT_CHUNK_SIZE

        self.iter_buffer = None

        logging.debug("%s", type(data_stream))
        if isinstance(data_stream, str):
            logging.debug("opening file: %s", data_stream)
            self.data_stream = open(data_stream, "rb")
        elif isinstance(data_stream, bytes):
            logging.debug("buffer data: %s")
            self.data_stream = BytesIO(data_stream)
        elif isinstance(data_stream, (BytesIO, BufferedReader)):
            self.data_stream = data_stream
        else:
            self.data_stream = iter(data_stream)
            self.iter_buffer = b""

    def __iter__(self):
        return self

    def __del__(self):
        try:
            # self.data_stream.__exit__(None, None, None)
            self.data_stream.close()
        except Exception:
            pass

    def __next__(self):
        chunk = self.read(self.chunk_size)
        if not chunk:
            raise StopIteration()
        return chunk

    def read(self, size=-1):
        if self.max_size and self.size + size > self.max_size:
            size = self.max_size - self.size

        if self.iter_buffer is None:
            # not an iterator, so read
            chunk = self.data_stream.read(size)
        else:
            for chunk in self.data_stream:
                self.iter_buffer += chunk
                if size > -1 and len(self.iter_buffer) >= size:
                    break
            if size > -1:
                chunk = self.iter_buffer[:size]
                self.iter_buffer = self.iter_buffer[size:]
            else:
                chunk = self.iter_buffer
                self.iter_buffer = b""

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
