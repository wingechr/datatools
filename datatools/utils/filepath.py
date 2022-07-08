import logging  # noqa
import os
import shutil
from os.path import dirname, exists, getsize, isdir, join, realpath, relpath

# import stat
from pathlib import Path
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWRITE
from urllib.parse import urlparse


def make_file_readlonly(file_path):
    os.chmod(file_path, S_IREAD | S_IRGRP | S_IROTH)


def make_file_writable(file_path):
    os.chmod(file_path, S_IWRITE)


def get_size(file_path) -> int:
    return getsize(file_path)


def normpath(path):
    result = path.replace("\\", "/")  # windows -> normal
    result = result.lstrip("./")
    return result


def get_file_path_uri(file_path):
    file_path = Path(realpath(file_path))
    uri = file_path.as_uri()
    return uri


def assert_slash_end(path):
    if not path.endswith("/"):
        path += "/"
    return path


def walk_rel(start, filter=None):
    for rt, _ds, fs in os.walk(start):
        rt_rel = relpath(rt, start)
        for f in fs:
            file_path_rel = normpath(join(rt_rel, f))
            if not filter or filter(file_path_rel):
                yield file_path_rel
            else:
                logging.debug(f"SKIPPING: {file_path_rel}")


def copy_uri(source_uri, target_path, overwrite=False):
    source_path = urlparse(source_uri).path
    if source_path.startswith("/./"):  # relative path
        source_path = source_path.lstrip("/./")
    copy(source_path, target_path, overwrite=overwrite)


def makedirs(path, exist_ok=True):
    if isdir(path):
        return
    logging.debug(f"MAKEDIR {path}")
    os.makedirs(path, exist_ok=exist_ok)


def copy(source_file_path, target_file_path, overwrite=False):
    assert_not_exist(target_file_path, overwrite=overwrite)
    makedirs(dirname(target_file_path), exist_ok=True)
    logging.debug(f"COPY {source_file_path} ==> {target_file_path}")
    shutil.copy(source_file_path, target_file_path)


def move(source_file_path, target_file_path, overwrite=False):
    assert_not_exist(target_file_path, overwrite=overwrite)
    makedirs(dirname(target_file_path), exist_ok=True)
    logging.debug(f"MOVE {source_file_path} ==> {target_file_path}")
    shutil.move(source_file_path, target_file_path)


def assert_not_exist(target_file_path, overwrite=False):
    if not exists(target_file_path):
        return
    if not overwrite:
        logging.error(f"File exists: {target_file_path}")
        raise FileExistsError(f"File exists: {target_file_path}")
    logging.debug(f"RM {target_file_path}")
    os.remove(target_file_path)
