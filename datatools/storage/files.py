import logging
import hashlib
import tempfile
import os
import shutil


from .exceptions import ObjectNotFoundException


class AbstractFileStorage:
    def get(self, file_id):
        """
        Args:
            file_id(str): 32 character md5 hash

        Returns:
            data_stream (IOBase like)

        Raises:
            ObjectNotFoundException
        """
        raise NotImplementedError()

    def set(self, data_stream):
        """
        Args:
            data_stream: IOBase like readable binary stream

        Returns:
            file_id(str): 32 character md5 hash
        """
        raise NotImplementedError()


class HashedByteIterator:
    DEFAULT_CHUNK_SIZE = 2 ** 1  # 2 ** 24

    def __init__(self, data_stream):
        self.data_stream = data_stream
        self.hash = hashlib.md5()
        self.chunk_size = self.DEFAULT_CHUNK_SIZE

    def __iter__(self):
        return self

    def __next__(self):
        chunk = self.data_stream.read(self.chunk_size)
        logging.debug("read %d bytes (size=%d)", len(chunk), self.chunk_size)
        if not chunk:
            raise StopIteration()
        self.hash.update(chunk)
        return chunk

    def get_file_id(self):
        return self.hash.hexdigest()


class FileSystemStorage(AbstractFileStorage):

    DEFAULT_DATA_DIR = ".cache"

    def __init__(self, data_dir=None):
        self.data_dir = os.path.abspath(data_dir or self.DEFAULT_DATA_DIR)

        if not os.path.isdir(self.data_dir):
            logging.debug("creating data dir: %s", self.data_dir)
            os.makedirs(self.data_dir, exist_ok=True)
        else:
            logging.debug("using data dir: %s", self.data_dir)

    def get_filepath(self, file_id):
        return os.path.join(self.data_dir, file_id)

    def get(self, file_id):
        filepath = self.get_filepath(file_id)
        if not os.path.isfile(filepath):
            raise ObjectNotFoundException(file_id)
        return open(filepath, "rb")

    def set(self, data_stream):
        data_stream = HashedByteIterator(data_stream)
        with tempfile.TemporaryFile("wb", delete=False) as file:
            for chunk in data_stream:
                file.write(chunk)
        file_id = data_stream.get_file_id()
        filepath = self.get_filepath(file_id)
        tmp_filepath = file.name
        if os.path.isfile(filepath):
            # file exists already
            logging.debug("file already in storage: %s", file_id)
            os.remove(tmp_filepath)
        else:
            # copy file
            logging.debug("adding file: %s", filepath)
            shutil.move(tmp_filepath, filepath)
        return file_id
