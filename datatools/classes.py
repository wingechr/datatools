import json
import logging
import os
import re
import urllib.parse
from contextlib import ExitStack
from functools import cached_property
from pathlib import Path
from zipfile import ZipFile

import appdirs
import requests
from filelock import FileLock, SoftFileLock

from datatools import __app_name__, conf
from datatools.utils import get_hash, get_now_str, get_user_long, make_readonly

IS_OPEN = "_is_open"
IS_CHANGED = "_is_changed"


def is_open(obj):
    return getattr(obj, IS_OPEN, False)


def set_open(obj, value):
    return setattr(obj, IS_OPEN, value)


def is_changed(obj):
    return getattr(obj, IS_CHANGED, False)


def set_changed(obj, value):
    return setattr(obj, IS_CHANGED, value)


def assert_open(fun):
    def decorated_fun(obj, *args, **kwargs):
        if not is_open(obj):
            exit_stack = conf.exit_stack
            exit_stack.enter_context(obj)
        return fun(obj, *args, **kwargs)

    return decorated_fun


def assert_open_changed(fun):
    def decorated_fun(obj, *args, **kwargs):
        if not is_open(obj):
            exit_stack = conf.exit_stack
            exit_stack.enter_context(obj)
        setattr(obj, IS_CHANGED, True)
        return fun(obj, *args, **kwargs)

    return decorated_fun


def get_resource_handler(uri):
    def get_scheme(uri):
        # FIXME
        if os.path.isfile(uri):
            return "file"

        scheme = urllib.parse.urlsplit(uri).scheme
        if not scheme:
            scheme = "file"
        return scheme

    scheme = get_scheme(uri)

    handler = None
    for handler_cls in [HttpResource, FileResource]:
        if handler_cls.can_handle_scheme(scheme):
            handler = handler_cls(uri)
            break

    if not handler:
        raise NotImplementedError(scheme)

    return handler


class AbstractResourceContainer:
    _class = None

    def __init__(self):
        self._instances = {}

    def _get_instance_id(self, *args, **kwargs):
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        """get/create instance"""
        instance_id = self._get_instance_id(*args, **kwargs)
        logging.debug(f"Instance Id: {instance_id}")
        if instance_id not in self._instances:
            # create instance
            instance = self._class(*args, **kwargs)
            self._instances[instance_id] = instance

        return self._instances[instance_id]


class ResourceContainerMixin:
    _instances = None  # class should override this with a dict

    @classmethod
    def _get_instance_id(cls, *args, **kwargs):
        raise NotImplementedError()

    @classmethod
    def get_instance(cls, *args, **kwargs):
        """get/create instance"""
        instance_id = cls._get_instance_id(*args, **kwargs)
        logging.debug(f"Instance Id: {instance_id}")
        if instance_id not in cls._instances:
            # create instance
            instance = cls(*args, **kwargs)
            cls._instances[instance_id] = instance

        return cls._instances[instance_id]


class ByteResource:
    default_value = b""

    def __init__(self, path):
        self.path = self.get_realpath(path)
        self._lock = None
        self._data = None

    @property
    def _lock_path(self):
        return self.path + ".lock"

    @classmethod
    def get_realpath(cls, path):
        return str(Path(path).resolve())

    @classmethod
    def get_uri(cls, path):
        return Path(path).resolve().as_uri()

    def __enter__(self):
        # logging.debug(f"__enter__: {self}")
        if is_open(self):
            return self
        # logging.debug(f"OPENING: {self}")
        os.makedirs(os.path.dirname(self._lock_path), exist_ok=True)
        self._lock = SoftFileLock(self._lock_path).__enter__()
        self._data = self._read()
        set_open(self, True)
        set_changed(self, False)
        return self

    def __exit__(self, *exc):
        # logging.debug(f"__exit__: {self}")
        if not is_open(self):
            return
        # logging.debug(f"CLOSING: {self}, errors={any(exc)}, changed={is_changed(self)}")  # noqa
        if not any(exc) and is_changed(self):
            self._write(self._data)
        self._lock.__exit__(*exc)
        set_changed(self, False)

    def _read(self):
        if not os.path.isfile(self.path):
            return self.default_value
        logging.debug(f"READING from {self.path}")
        with open(self.path, "rb") as file:
            return file.read()

    # @assert_open
    def _write(self, data):
        assert is_open(self)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        logging.debug(f"WRITING to {self.path}")
        with open(self.path, "wb") as file:
            file.write(data)


class TextResource(ByteResource):
    encoding = "utf-8"

    def _read(self):
        return super()._read().decode(self.encoding)

    def _write(self, data):
        super()._write(data.encode(self.encoding))


class JsonResource(TextResource):
    indent = 2
    sort_keys = False
    ensure_ascii = False
    default_value = {}

    def _read(self):
        if not os.path.isfile(self.path):
            return self.default_value
        return json.loads(super()._read())

    def _write(self, data):
        super()._write(
            json.dumps(
                data,
                indent=self.indent,
                sort_keys=self.sort_keys,
                ensure_ascii=self.ensure_ascii,
            )
        )

    @assert_open_changed
    def f(self, k, v):
        self._data[k] = v


class PackageIndex(JsonResource, ResourceContainerMixin):
    default_value = {"resources": []}
    _instances = {}

    @classmethod
    def _get_instance_id(cls, path, *args, **kwargs):
        # FIXME: lower(), because in windows,its case insensitive.
        # but what about unix?
        return cls.get_uri(path).lower()

    @assert_open
    def get_resource(self, path):
        for res in self._data["resources"]:
            if res.get("path") == path:
                return res
        return {"path": path}

    @assert_open_changed
    def set_resource(self, res):
        path = res["path"]
        for i, r in enumerate(self._data["resources"]):
            # logging.debug(r)
            if r.get("path") == path:
                self._data["resources"][i] = res
                return
        self._data["resources"].append(res)


class TestJsonResourceContainer(JsonResource, ResourceContainerMixin):
    _instances = {}

    @classmethod
    def _get_instance_id(cls, path, *args, **kwargs):
        # FIXME: lower(), because in windows,its case insensitive.
        # but what about unix?
        return cls.get_uri(path).lower()


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


def _get_index_instance(base_path: str):
    if str(base_path).endswith(".zip"):
        return _ZipFileIndex(base_path)
    return _FolderIndex(base_path)


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


class _MultiIndex:
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
            idx = _get_index_instance(base_path)
            self.indices[base_path] = self._enter(idx)

        return self.indices[base_path]

    def get_index_for_file(self, filepath):
        base_path = get_index_base_path(filepath)
        return self.get_index(base_path)


class _Index:
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

    def get_rel_path(self, file):
        return Path(file).relative_to(self.base_path).as_posix()

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


class _FolderIndex(_Index):
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


class _ZipFileIndex(_Index):
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


def get_cache_location(cache_dir=None):
    if not cache_dir:
        path = appdirs.user_data_dir(
            __app_name__, appauthor=None, version=None, roaming=False
        )
        path += "/data"
    else:
        path = cache_dir
    path = os.path.abspath(path)
    return path


class _Cache:
    def __init__(self, base_dir):
        self.base_path = Path(base_dir).absolute()
        os.makedirs(self.base_path, exist_ok=True)
        logging.debug(f"base_dir: {self.base_path.as_posix()}")

    def get_location(self, location):
        path = Path(location)
        if path.is_absolute():
            return location
        path = self.base_path.joinpath(path)
        return path.as_posix()

    def resource(self, location):
        location = self.get_location(location)
        return Resource(self, location)


class ResourceMetadata:
    """Proxy object for resource inside a metadata storage"""

    @cached_property
    def index_location(self):
        for p in Path(self.resource.location).absolute().parents:
            i = os.path.join(p, "datapackage.json")
            if os.path.isfile(i):
                return i
        return self.resource.location + ".datapackage.json"

    @cached_property
    def relative_path(self):
        # TODO
        return (
            Path(self.resource.location)
            .absolute()
            .relative_to(Path(self.index_location).absolute().parent)
            .as_posix()
        )

    def __init__(self, resource):
        self.resource = resource
        self.index = PackageIndex.get_instance(self.index_location)

    def get(self, key=None, value_default=None):
        res = self.index.get_resource(self.relative_path)
        if not key:
            return res
        else:
            return res.get(key, value_default)

    def set(self, key, value=None):
        res = self.index.get_resource(self.relative_path)
        if isinstance(key, dict):
            res.update(key)
        else:
            res[key] = value

        self.index.set_resource(res)

    def check(self, key, value=None):
        if value is None:
            value = self.get(key)
        raise NotImplementedError()

    def update(self, key):
        raise NotImplementedError()


class Resource:
    def __init__(self, location):
        self.original_location = location

    @cached_property
    def metadata(self):
        return ResourceMetadata(self)

    @cached_property
    def is_remote(self):
        return False

    @cached_property
    def location(self):
        return self.original_location

    @property
    def exists(self):
        return os.path.isfile(self.location)


class RemoteResource(Resource):
    @cached_property
    def is_remote(self):
        return True

    @cached_property
    def relative_location(self):
        """relative to cache root"""
        url = urllib.parse.urlsplit(self.original_location)
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

    def cache(self):
        raise NotImplementedError()

    @property
    def location(self):
        return (
            Path(conf.cache_dir).joinpath(self.relative_location).absolute().as_posix()
        )

    @property
    def exists(self):
        if not super().exists:
            logging.info(f"Caching {self.original_location} -> {self.location}")
            self.cache()
            if super().exists:
                caching_meta = self._get_caching_metadata()
                self.metadata.set(caching_meta)

        return super().exists

    def _get_caching_metadata(self):
        return {
            "hash": get_hash(self.location),
            "download": {
                "datetime": get_now_str(),
                "user": get_user_long(),
                "source": self.original_location,
            },
        }


class FileResource(Resource):
    @staticmethod
    def can_handle_scheme(scheme) -> bool:
        return scheme.lower() in ("file",)


class HttpResource(RemoteResource):
    @staticmethod
    def can_handle_scheme(scheme) -> bool:
        return scheme.lower() in ("http", "https")

    def cache(self):
        src = self.original_location
        tgt = self.location

        os.makedirs(os.path.dirname(tgt), exist_ok=True)
        res = requests.get(src)

        try:
            res.raise_for_status()
        except Exception as exc:
            logging.error(exc)
            return

        with open(tgt, "wb") as file:
            file.write(res.content)


def main_test():
    jsons = TestJsonResourceContainer

    with conf.exit_stack:
        with jsons.get_instance("test.json") as j1:
            j1.f("a", 1)

            with j1:
                jsons.get_instance("test.json").f("a", 4)


if __name__ == "__main__":
    logging.basicConfig(
        format="[%(asctime)s %(levelname)7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )
    main_test()
