import logging
import os
import shutil

# import stat
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from .exceptions import ObjectNotFoundException
from .utils import HashedByteIterator, make_file_readlonly, validate_file_id


def normpath(path):
    result = path.replace("\\", "/")  # windows -> normal
    result = result.lstrip("./")
    return result


def relpath(path, start):
    result = os.path.relpath(os.path.abspath(start), os.path.abspath(path))
    result = normpath(result)
    return result


def get_filepath_uri(filepath):
    filepath = Path(os.path.realpath(filepath))
    uri = filepath.as_uri()
    return uri


def assert_slash_end(path):
    if not path.endswith("/"):
        path += "/"
    return path


def walk_rel(start, filter=None):
    for rt, _ds, fs in os.walk(start):
        rt_rel = os.path.relpath(rt, start)
        for f in fs:
            filepath_rel = normpath(os.path.join(rt_rel, f))
            if not filter or filter(filepath_rel):
                yield filepath_rel
            else:
                logging.debug(f"SKIPPING: {filepath_rel}")


def copy_uri(source_uri, target_path, overwrite=False):
    source_path = urlparse(source_uri).path
    if source_path.startswith("/./"):  # relative path
        source_path = source_path.lstrip("/./")
    copy(source_path, target_path, overwrite=overwrite)


def makedirs(path, exist_ok=True):
    if os.path.isdir(path):
        return
    logging.debug(f"MAKEDIR {path}")
    os.makedirs(path, exist_ok=exist_ok)


class FileSystemStorage:

    DEFAULT_DATA_DIR = ".cache"

    def __init__(self, data_dir=None, sf_depth=0, sf_len=2):
        self.data_dir = os.path.abspath(data_dir or self.DEFAULT_DATA_DIR)
        self.sf_depth = sf_depth
        self.sf_len = sf_len

    def __contains__(self, file_id):
        filepath = self._get_filepath(file_id)
        return os.path.isfile(filepath)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def _get_filepath(self, file_id):
        data_dir = self.data_dir
        for i in range(self.sf_depth):
            subfolder = file_id[i * self.sf_len : (i + 1) * self.sf_len]
            data_dir = os.path.join(data_dir, subfolder)
        if not os.path.isdir(data_dir):
            logging.debug("creating data dir: %s", data_dir)
            makedirs(data_dir)
        return os.path.join(data_dir, file_id)

    def get_file(self, file_id, check_integrity=False):
        """
        Args:
            file_id(str): 32 character md5 hash
            check_integrity(bool): check hash on read

        Returns:
            data_stream (IOBase like)

        Raises:
            ObjectNotFoundException
        """
        file_id = validate_file_id(file_id)
        if file_id not in self:
            raise ObjectNotFoundException(file_id)
        filepath = self._get_filepath(file_id)
        file = open(filepath, "rb").__enter__()
        if check_integrity:
            file = HashedByteIterator(file, expected_hash=file_id)
        return file

    def set_file(self, data_stream):
        """
        Args:
            data_stream: IOBase like readable binary stream

        Returns:
            file_id(str): 32 character md5 hash
        """
        data_stream = HashedByteIterator(data_stream)
        with tempfile.NamedTemporaryFile("wb", delete=False) as file:
            for chunk in data_stream:
                file.write(chunk)
        file_id = data_stream.get_current_hash()
        file_size = data_stream.get_current_size_bytes()
        filepath = self._get_filepath(file_id)
        tmp_filepath = file.name
        if os.path.isfile(filepath):
            # file exists already
            logging.debug("file already in storage: %s (%d bytes)", file_id, file_size)
            os.remove(tmp_filepath)
        else:
            # copy file
            logging.debug("adding file %s: %s (%d bytes)", filepath, file_id, file_size)
            move(tmp_filepath, filepath)
            # make readonly
            make_file_readlonly(filepath)
        return file_id


def copy(source_filepath, target_filepath, overwrite=False):
    assert_not_exist(target_filepath, overwrite=overwrite)
    makedirs(os.path.dirname(target_filepath), exist_ok=True)
    logging.debug(f"COPY {source_filepath} ==> {target_filepath}")
    shutil.copy(source_filepath, target_filepath)


def move(source_filepath, target_filepath, overwrite=False):
    assert_not_exist(target_filepath, overwrite=overwrite)
    makedirs(os.path.dirname(target_filepath), exist_ok=True)
    logging.debug(f"MOVE {source_filepath} ==> {target_filepath}")
    shutil.move(source_filepath, target_filepath)


def assert_not_exist(target_filepath, overwrite=False):
    if not os.path.exists(target_filepath):
        return
    if not overwrite:
        logging.error(f"File exists: {target_filepath}")
        raise FileExistsError(f"File exists: {target_filepath}")
    logging.debug(f"RM {target_filepath}")
    os.remove(target_filepath)
