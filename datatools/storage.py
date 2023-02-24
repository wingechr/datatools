import json
import logging
import os
import re
import shutil
import tempfile
import urllib.parse
from contextlib import ExitStack
from pathlib import Path
from zipfile import ZipFile

import requests
from filelock import FileLock, SoftFileLock

from . import __app_name__
from .utils import get_hash, get_now_str, get_user_long, make_readonly


class DataIndex:
    def __init__(self, base_dir):
        self._base_dir = os.path.abspath(base_dir)
        self._data_dir = os.path.join(self._base_dir, "data")
        self._index_json = os.path.join(self._base_dir, "datapackage.json")
        self._index_json_lock = FileLock(self._index_json + ".lock")
        self._data = None
        self._encoding = "utf-8"
        self._changed = None

    def __enter__(self):
        os.makedirs(self._base_dir, exist_ok=True)
        os.makedirs(self._data_dir, exist_ok=True)
        if not os.path.exists(self._index_json):
            logging.info(f"initializing index in : {self._base_dir}")
            self._data = {"resources": []}
            self._write_index()
        self._index_json_lock.__enter__()
        self._read_index()

        return self

    def __exit__(self, exc_type, exc_value, tb):
        if self._changed and not exc_value:
            # write if there was no error
            self._write_index()
        self._index_json_lock.__exit__(exc_type, exc_value, tb)

    def _write_index(self):
        data_s = json.dumps(self._data, ensure_ascii=False, indent=2)
        data_b = data_s.encode(encoding=self._encoding)
        logging.debug(f"writing index: {self._index_json}")
        with open(self._index_json, "wb") as file:
            file.write(data_b)
        self._changed = False

    def _read_index(self):
        logging.debug(f"reading index: {self._index_json}")
        with open(self._index_json, encoding=self._encoding) as file:
            self._data = json.load(file)
        self._changed = False

    def get_resource_id(self, abspath):
        assert abspath
        abspath = os.path.abspath(abspath)
        relpath = os.path.relpath(abspath, self._data_dir)
        relpath = relpath.replace("\\", "/")
        return relpath

    def contains_resource(self, resource_id):
        abs_path = self.get_abs_path(resource_id)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        return os.path.exists(abs_path)

    def get_abs_path(self, resource_id):
        resource_id = resource_id.lstrip("/")
        assert resource_id
        return os.path.abspath(os.path.join(self._data_dir, resource_id))

    def find_resource_ids_in_repo(self):
        resource_ids = set()
        for rt, _, fs in os.walk(self._data_dir):
            for f in fs:
                abspath = os.path.join(rt, f)
                resource_id = self.get_resource_id(abspath)
                if resource_id in resource_ids:
                    logging.warning(f"duplicate path: {resource_id}")
                    continue
                resource_ids.add(resource_id)
        return resource_ids

    def find_resource_ids_in_index(self):
        resource_ids = set()
        for res in self._data["resources"]:
            resource_id = res.get("path")
            if not resource_id:
                logging.warning(f"resource without path: {res}")
                continue
            if resource_id in resource_ids:
                logging.warning(f"duplicate path: {resource_id}")
                continue
            resource_ids.add(resource_id)

        return resource_ids

    def remove(self, resource_id):
        # TODO: duplicates?
        idx = None
        for i, res in enumerate(self._data["resources"]):
            if res.get("path") == resource_id:
                idx = i
                break

        if idx is None:
            raise KeyError(resource_id)

        assert self._data["resources"][idx]["path"] == resource_id
        del self._data["resources"][idx]

        self._changed = True

    def update(self, abspath, source):
        resource_id = self.get_resource_id(abspath)
        resource = self._get_metadata(abspath, source)
        resource["path"] = resource_id

        # TODO
        res2idx = self.find_resource_ids_in_index()
        if resource_id in res2idx:
            logging.info(f"overwriting metadata for {resource_id}")
            assert self._data["resources"][res2idx[resource_id]]["path"] == resource_id
            del self._data["resources"][res2idx[resource_id]]
            self._changed = True

        self._data["resources"].append(resource)
        self._changed = True

    def _get_metadata(self, abspath, source):
        return {
            "hash": get_hash(abspath),
            "download": {
                "datetime": get_now_str(),
                "user": get_user_long(),
                "source": source,
            },
        }

    def download(self, uri, force=False):
        def get_temp_path(suffix=""):
            """do not create the file"""
            tf = tempfile.mktemp(prefix=f"{__app_name__}_", suffix=suffix)
            return tf

        handler = get_handler(uri)
        resource_id = handler.get_local_rel_path(uri)
        filepath = self.get_abs_path(resource_id)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if os.path.exists(filepath):
            if not force:
                logging.info(f"exists: {resource_id}")
                return
            else:
                logging.warning(f"overwriting exists: {resource_id}")

        tmp_path = get_temp_path()

        handler.handle(uri, tmp_path)
        shutil.move(tmp_path, filepath)
        make_readonly(filepath)

        self.update(filepath, source=uri)

    def check(self, fix, delete, hash):
        files = self.find_resource_ids_in_repo()
        resources = self.find_resource_ids_in_index()

        # find files that are not in index
        for f in files - resources:
            if fix:
                logging.info(f"Adding: {f}")
                path = self.get_abs_path(f)
                self.update(path, source=None)
            else:
                logging.warning(f"File not in index: {f}")

        # find resources that are not in directory
        for f in resources - files:
            if fix and delete:
                logging.info(f"removing {f}")
                self.remove(f)
            else:
                logging.warning(f"File does not exist: {f}")

        # TODO fix duplicates

        # TODO: check hash
        if hash:
            for res in self._data["resources"]:
                assert res["path"] in files
                path = self.get_abs_path(res["path"])
                index_hash = res.get("hash")
                if index_hash:
                    for method, hashsum in index_hash.items():
                        digest = get_hash(path, method)[method]
                        if digest != hashsum:
                            if fix:
                                logging.warning(
                                    f"Fixing Wrong {method} hashsum for {path}: "
                                    f"{digest}, expected {hashsum}"
                                )
                                res["hash"][method] = get_hash(path, method)[method]
                                self._changed = True
                                # TODO
                            else:
                                logging.warning(
                                    f"Wrong {method} hashsum for {path}: "
                                    f"{digest}, expected {hashsum}"
                                )
                else:  # no hash
                    if fix:
                        logging.warning(f"Fixing No hashsum for {path}")
                        res["hash"] = get_hash(path, method)
                        self._changed = True
                    else:
                        logging.warning(f"No hashsum for {path}")

        # readonly
        for f in files:
            path = self.get_abs_path(f)
            make_readonly(path)


class LoaderHttp:
    def get_local_rel_path(self, uri) -> str:
        url = urllib.parse.urlsplit(uri)
        url.path.split("/")

        host = url.hostname or "localhost"
        path = url.path

        # TODO: maybe urldecode spaces? but not all special chars?

        if not path.startswith("/"):
            path = "/" + path

        if path == "/":
            path = "/index.html"

        path = host + path

        if url.fragment:
            path += "#" + url.fragment

        return path

    @staticmethod
    def can_handle_scheme(scheme) -> bool:
        return scheme.lower() in ("http", "https")

    def handle(self, uri, filepath):
        res = requests.get(uri)
        res.raise_for_status()
        with open(filepath, "wb") as file:
            file.write(res.content)


def get_handler(uri):
    def get_scheme(uri):
        scheme = urllib.parse.urlsplit(uri).scheme
        if not scheme:
            scheme = "file"
        return scheme

    scheme = get_scheme(uri)

    for handler_cls in [LoaderHttp]:
        if handler_cls.can_handle_scheme(scheme):
            return handler_cls()
    raise NotImplementedError(scheme)


def get_index_instance(base_path: str):
    if str(base_path).endswith(".zip"):
        return ZipFileIndex(base_path)
    return FolderIndex(base_path)


def get_index_base_path(filepath: str) -> str:
    """Find the path of the appropriate index location for a file path

    NOTE: this does NOT include the name of the index (i.e. datapackage.json),
    but the base dir it is located in

    * if the file is in a zip file, the index will be /datapackage.json
        inside the zip
    * otherwise: find the first datapackage.json (going up)
    * if none is found: use the same folder the file is in
    """

    filepath = Path(filepath)

    if re.match(r".*\.zip/", filepath.as_posix()):
        for p in filepath.parents:
            if str(p).endswith(".zip"):
                return p.as_posix()
        raise ValueError(filepath)

    for p in filepath.parents:
        if p.joinpath("datapackage.json").exists():
            return p.as_posix()
    # return dir
    return filepath.parent.as_posix()


class MultiIndex:
    def __init__(self):
        self.exit_stack = None
        self.indices = None

    def _enter(self, res):
        return self.exit_stack.enter_context(res)

    def __enter__(self):
        self.exit_stack = ExitStack().__enter__()
        self.indices = {}

    def __exit__(self, *error):
        self.exit_stack.__exit__(*error)

    def get_index(self, base_path):
        base_path = str(Path(base_path).absolute().as_posix())
        if base_path not in self.indices:
            idx = get_index_instance(base_path)
            self.indices[base_path] = self._enter(idx)

        return self.indices[base_path]

    def get_index_for_file(self, filepath):
        base_path = get_index_base_path(filepath)
        return self.get_index(base_path)


class Index:
    def __init__(self, base_path):
        self.base_path = Path(base_path).absolute()
        self.changed = True
        self.data = None
        self.resources_by_path = None
        self.exit_stack = None

    def _enter(self, res):
        return self.exit_stack.enter_context(res)

    def _index_resource(self, res):
        path = res.get("path")
        if not path:
            logging.warning("no path")
        if path in self.resources_by_path:
            logging.warning("updating existing path")
        self.resources_by_path[path] = res

    def append_resource(self, res):
        path = res.get("path")
        if not path:
            logging.warning("no path")
        if path in self.resources_by_path:
            logging.warning("updating existing path")
            self.resources_by_path[path].update(res)
        else:
            self.data["resources"].append(res)
            self.resources_by_path[path] = res

    def __enter__(self):
        self.exit_stack = ExitStack().__enter__()

        Path(self._lockfile).parent.mkdir(parents=True, exist_ok=True)
        self._enter(SoftFileLock(self._lockfile))

        if self._exist():
            self.data = self._load()
        else:
            logging.info("INIT")
            self.data = {"resources": []}

        self.resources_by_path = {}
        for res in self.data["resources"]:
            self._index_resource(res)

        return self

    def __exit__(self, *error):
        if self.changed and not any(error):
            self._write(self.data)
        self.exit_stack.__exit__(*error)

    def _load(self):
        logging.info("READ")
        data_b = self._load_b()
        return json.loads(data_b.decode("utf-8"))

    def _write(self, data):
        logging.info("WRITE")
        data_b = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        self._write_b(data_b)

    def _load_b(self):
        raise NotImplementedError()

    def _write_b(self, data):
        raise NotImplementedError()

    @property
    def _lockfile(self):
        raise NotImplementedError()


class FolderIndex(Index):
    @property
    def json_path(self):
        return self.base_path.joinpath("datapackage.json")

    @property
    def _lockfile(self):
        return str(self.json_path) + ".lock"

    def _exist(self):
        return self.json_path.exists()

    def _load_b(self):
        with open(self.json_path, "rb") as file:
            return file.read()

    def _write_b(self, data):
        self.base_path.mkdir(parents=True, exist_ok=True)
        with open(self.json_path, "wb") as file:
            file.write(data)


class ZipFileIndex(Index):
    @property
    def _lockfile(self):
        return str(self.base_path) + ".lock"

    @property
    def zip_path(self):
        return self.base_path

    def _exist(self):
        if not self.zip_path.exists():
            return False

        with ZipFile(self.zip_path, "r") as zip:
            logging.info(zip.namelist())
            return "datapackage.json" in zip.namelist()

    def _load_b(self):
        with ZipFile(self.zip_path, "r") as zip:
            with zip.open("datapackage.json", "r") as file:
                return file.read()

    def _write_b(self, data):
        self.zip_path.parent.mkdir(parents=True, exist_ok=True)
        with ZipFile(self.zip_path, "w") as zip:
            with zip.open("datapackage.json", "w") as file:
                file.write(data)


with MultiIndex() as idx:
    idx.get
