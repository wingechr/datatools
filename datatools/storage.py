import functools
import hashlib
import json
import logging
import os
import pickle
import re
from io import BufferedReader
from tempfile import NamedTemporaryFile
from typing import Callable, Iterable, Union

import jsonpath_ng

from .constants import (
    DEFAULT_HASH_METHOD,
    GLOBAL_LOCATION,
    LOCAL_LOCATION,
    ROOT_METADATA_PATH,
)
from .exceptions import DataDoesNotExists, DataExists, InvalidPath
from .loader import load_file, open_uri
from .schema import validate
from .utils import (
    as_byte_iterator,
    as_uri,
    delete_file,
    get_now_str,
    get_resource_path_name,
    get_user_w_host,
    is_file_readonly,
    json_serialize,
    make_file_readonly,
)


class Storage:
    """Storage class for data and metadata"""

    def __init__(self, location=None):
        self.__location = os.path.abspath(location or LOCAL_LOCATION)

    @property
    def location(self):
        return self.__location

    def __str__(self):
        return f"Storage({self.location})"

    def resource(self, source_uri: str = None, name: str = None) -> "Resource":
        """
        Args:
            source_uri (str, optional): source URI, if it is an external resource.
                If no name is provided, name will be generated from source_uri
            name (str, optional): resource name

        Returns:
            Resource instance
        """
        if source_uri:
            return UriResource(storage=self, source_uri=source_uri, name=name)
        else:
            return Resource(storage=self, name=name)

    def cache(
        self,
        get_name: Callable = None,
        from_bytes: Callable = None,
        to_bytes: Callable = None,
    ) -> "Cache":
        """
        Args:
            get_name (Callable, optional): TODO
            from_bytes (Callable, optional): TODO
            to_bytes (Callable, optional): TODO
        Returns:
            Callable function decorator
        """
        return Cache(
            storage=self,
            get_name=get_name,
            from_bytes=from_bytes,
            to_bytes=to_bytes,
        )

    def check(self, fix=False):
        """check for problems in storage"""
        for res in self.find_resources():
            if not is_file_readonly(res.filepath):
                if fix:
                    make_file_readonly(res.filepath)
                    logging.info(f"FIXED: File not readonly: {res.filepath}")
                else:
                    logging.warning(f"File not readonly: {res.filepath}")

    @classmethod
    def is_metadata_path(cls, path: str):
        return ".metadata." in path

    @classmethod
    def is_temp_path(cls, path: str):
        return "+" in path

    def find_resources(self, *path_patterns) -> Iterable["Resource"]:
        """Find resources by name
        Args:

            path_patterns (str): name patterns

        Yields:
            Resource
        """
        path_patterns = [re.compile(".*" + p.lower()) for p in path_patterns]
        for rt, _ds, fs in os.walk(self.location):
            rt_rl = os.path.relpath(rt, self.location).replace("\\", "/")
            for filename in fs:
                path = f"{rt_rl}/{filename}"
                if self.is_metadata_path(path):
                    continue
                if self.is_temp_path(path):
                    continue
                if all(p.match(path) for p in path_patterns):
                    yield self.resource("data://" + path)


class Resource:
    """Store data and metadata"""

    def __init__(self, storage: "Storage", name: str):
        self.__storage = storage
        self.__name = get_resource_path_name(name)
        if self.__storage.is_metadata_path(self.__name):
            raise InvalidPath(self.__name)
        if self.__storage.is_temp_path(self.__name):
            raise InvalidPath(self.__name)
        self.__filepath = os.path.abspath(self.__storage.location + "/" + self.__name)

    def __str__(self):
        return self.name

    @property
    def name(self) -> str:
        """unique, normalized dataset name"""
        return self.__name

    @property
    def filepath(self) -> str:
        """filepath of data in storage"""
        return self.__filepath

    @property
    def metadata(self) -> "Metadata":
        """meta data for this resource

        returns:
            Metadata
        """
        return Metadata(resource=self)

    def exists(self) -> bool:
        """Check if resource ecists in storage.

        returns:
            bool
        """
        return os.path.exists(self.filepath)

    def delete(self, delete_metadata: bool = False) -> None:
        """Delete resource from storage.

        Args:
            delete_metadata (bool, optional): Also delete metadata.
        """
        delete_file(self.filepath)

        if delete_metadata:
            self.metadata.delete()

    def write(
        self, data: Union[BufferedReader, bytes, Iterable], exist_ok=False
    ) -> None:
        """Write binary data into storage"""
        if self.exists():
            if not exist_ok:
                raise DataExists(self)
            logging.info(f"Overwriting existing file: {self.filepath}")

        # write data into temporary file
        tmp_dir = os.path.dirname(self.filepath)
        tmp_prefix = os.path.basename(self.filepath) + "+"

        hash_method = DEFAULT_HASH_METHOD
        hasher = getattr(hashlib, hash_method)()
        size = 0

        os.makedirs(tmp_dir, exist_ok=True)
        with NamedTemporaryFile(
            "wb", dir=tmp_dir, prefix=tmp_prefix, delete=False
        ) as file:
            logging.debug(f"WRITING {file.name}")
            for chunk in as_byte_iterator(data):
                file.write(chunk)
                size += len(chunk)
                hasher.update(chunk)

        # move to final location
        try:
            logging.debug(f"MOVE {file.name} => {self.filepath}")
            os.rename(file.name, self.filepath)
        except Exception:
            delete_file(file.name)
            raise
        make_file_readonly(self.filepath)

        # update metadata
        metadata = {
            f"hash.{hash_method}": hasher.hexdigest(),
            "size": size,
            "source.user": get_user_w_host(),
            "source.datetime": get_now_str(),
            "source.name": self.name,
        }
        self.metadata.update(metadata)

    def open(self) -> BufferedReader:
        """Open resource as binary BufferedReader"""
        if not self.exists():
            self.save()
        logging.debug(f"READING {self.filepath}")
        file = open(self.filepath, "rb")
        return file

    def load(self, validate_schema=False, **kwargs):
        """Load data using loading functions determined by the resource's mediatype"""
        if not self.exists():
            self.save()
        data = load_file(filepath=self.filepath, **kwargs)
        if validate_schema:
            schema = self.metadata.get("schema")
            validate(data, schema)
        return data


class UriResource(Resource):
    """Resource that can be automatically loaded from external uri"""

    def __init__(self, storage: "Storage", source_uri: str, name: str = None):
        """
        Args:
            source_uri (str): URI of location.
                If not an URI: assume it's a local file path.
            name (str, optional): if not specified: name will be generated from uri
        """
        self.__source_uri = as_uri(source_uri)
        super().__init__(storage=storage, name=name or self.__source_uri)

    @property
    def source_uri(self):
        """URI of data source"""
        return self.__source_uri

    def save(self, exist_ok=False) -> None:
        """save from source"""
        if self.exists():
            if not exist_ok:
                raise DataDoesNotExists(self)
            return
        byte_data, metadata = open_uri(self.source_uri)
        super().write(data=byte_data, exist_ok=exist_ok)
        self.metadata.update(metadata)


class Metadata:
    """Metadata object associated with resource"""

    def __init__(self, resource: "Resource"):
        self.__resource = resource
        self.__filepath = self.__resource.filepath + ".metadata.json"

    def __read(self) -> dict:
        if not os.path.exists(self.__filepath):
            metadata = {}
        else:
            logging.debug(f"READING {self.__filepath}")
            with open(self.__filepath, "rb") as file:
                metadata = json.load(file)
        return metadata

    def update(self, metadata: dict) -> None:
        """Update metadata.

        Args:
            metadata (dict): key, value mapping, where keys are jsonpaths

        """
        # get existing metadata
        _metadata = self.__read()

        # update
        for key, value in metadata.items():
            metadata_path_pattern = self.__create_metadata_path_pattern(
                metadata_path=key
            )
            logging.debug(f"update metadata: {key} => {value}")
            metadata_path_pattern.update_or_create(_metadata, value)

        # convert to bytes
        metadata_bytes = json.dumps(
            _metadata, indent=2, ensure_ascii=False, default=json_serialize
        ).encode()

        # save
        os.makedirs(os.path.dirname(self.__filepath), exist_ok=True)
        logging.debug(f"WRITING {self.__filepath}")
        with open(self.__filepath, "wb") as file:
            file.write(metadata_bytes)

    def get(self, key: str = None, default: Union[str, Callable] = None) -> object:
        """Get metadata value.

        Args:
            key (str, optional): metadata key (json path)
            default (Union[str, Callable], optional): default value
                or function to create default value fromresoutce instance


        Returns:
            object

        """
        metadata = self.__read()

        key = key or ROOT_METADATA_PATH
        metadata_path_pattern = self.__create_metadata_path_pattern(metadata_path=key)
        match = metadata_path_pattern.find(metadata)
        result = [x.value for x in match]

        # TODO: we always get a list (multiple matches),
        # but most of the time, we want only one
        if len(result) == 0:
            result = None
        elif len(result) == 1:
            result = result[0]
        else:
            logging.warning("multiple results in metadata found")

        if not result and default:
            if isinstance(default, Callable):
                default = default(self.__resource)
                # TODO: save?
            result = default

        return result

    def _delete(self):
        delete_file(self.__filepath)

    def __create_metadata_path_pattern(self, metadata_path: str) -> str:
        metadata_path_pattern = jsonpath_ng.parse(metadata_path)
        return metadata_path_pattern


class Cache:
    """Decorator"""

    def __init__(
        self,
        storage: Storage = None,
        get_name: Callable = None,
        from_bytes: Callable = None,
        to_bytes: Callable = None,
    ):
        self.__storage = storage or Storage()
        self.__get_name = get_name or self.default_get_name
        self.__from_bytes = from_bytes or pickle.loads
        self.__to_bytes = to_bytes or pickle.dumps

    def __call__(self, fun):
        @functools.wraps(fun)
        def _fun(*args, **kwargs):
            # get data_path from function + arguments
            name = self.__get_name(fun, args, kwargs)
            res = self.__storage.resource(name=name)

            # try to get data from store
            if not res.exists():
                # actually call function and write data
                data = fun(*args, **kwargs)
                byte_data = self.__to_bytes(data)
                res.write(data=byte_data)

                # add job description as metadata
                job_description = self.get_job_description(fun, args, kwargs)
                metadata = {"source.creation": job_description}
                res.metadata.update(metadata)

            # load data from storage
            with res.open() as file:
                byte_data = file.read()

            data = self.__from_bytes(byte_data)
            return data

        return _fun

    @classmethod
    def get_hash(cls, object):
        bytes = json.dumps(
            object, sort_keys=True, ensure_ascii=False, default=json_serialize
        ).encode()
        job_id = hashlib.md5(bytes).hexdigest()
        return job_id

    @classmethod
    def get_job_description(cls, fun, args, kwargs):
        return {
            "function": fun.__name__,
            "args": args,
            "kwargs": kwargs,
            "description": fun.__doc__,  # todo: maybe cleanup into plain text
        }

    @classmethod
    def default_get_name(cls, fun, args, kwargs):
        f_name = f"{fun.__name__}"
        job_desc = cls.get_job_description(fun, args, kwargs)
        # we only hash part of it:
        job_id = cls.get_hash({"args": job_desc["args"], "kwargs": job_desc["kwargs"]})
        # we can shorten the hash: 8 to 10 chars should be enough
        job_id = job_id[:8]
        return f"cache/{f_name}_{job_id}.pickle"


class StorageGlobal(Storage):
    """Storage with user level global location"""

    def __init__(self):
        super().__init__(location=GLOBAL_LOCATION)


class StorageEnv(Storage):
    """Storage with location from environment variable"""

    def __init__(self, env_location):
        location = os.environ[env_location]
        super().__init__(location=location)
