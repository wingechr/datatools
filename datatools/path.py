import os
import shutil
import stat
from pathlib import Path
from urllib.parse import urlparse


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


def copy_uri(source_uri, target_filepath, source_base_path="."):
    source_filepath = urlparse(source_uri).path
    # remove leading slash
    source_filepath = source_filepath[1:]
    # if relative path: append source_base_path
    if source_filepath.startswith("./"):
        source_filepath = source_base_path + source_filepath[1:]

    shutil.copy(source_filepath, target_filepath)
    # make files also writable, so scons can delete them
    os.chmod(target_filepath, stat.S_IWRITE | stat.S_IREAD)
