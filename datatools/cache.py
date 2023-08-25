import functools
import hashlib
import json
import pickle

from . import storage
from .utils import json_serialize


def get_hash(object):
    bytes = json.dumps(
        object, sort_keys=True, ensure_ascii=False, default=json_serialize
    ).encode()
    job_id = hashlib.md5(bytes).hexdigest()
    return job_id


def get_job_description(fun, args, kwargs):
    return {
        "function": fun.__name__,
        "args": args,
        "kwargs": kwargs,
        "description": fun.__doc__,  # todo: maybe cleanup into plain text
    }


def default_get_name(fun, args, kwargs):
    f_name = f"{fun.__name__}"
    job_desc = get_job_description(fun, args, kwargs)
    # we only hash part of it:
    job_id = get_hash({"args": job_desc["args"], "kwargs": job_desc["kwargs"]})
    # we can shorten the hash: 8 to 10 chars should be enough
    job_id = job_id[:8]
    return f"cache/{f_name}_{job_id}.pickle"


def default_storage():
    return storage.Storage()


class Cache:
    def __init__(
        self,
        storage_instance: storage.AbstractStorage = None,
        get_name=None,
        from_bytes=None,
        to_bytes=None,
        name_prefix=None,
    ):
        self.__storage = storage_instance or default_storage()
        self.__get_name = get_name or default_get_name
        self.__from_bytes = from_bytes or pickle.loads
        self.__to_bytes = to_bytes or pickle.dumps
        self.__name_prefix = name_prefix or "cache://"

    def __call__(self, fun):
        @functools.wraps(fun)
        def _fun(*args, **kwargs):
            # get data_path from function + arguments
            name = self.__name_prefix + self.__get_name(fun, args, kwargs)
            res = self.__storage.resource(source_uri=name)

            # try to get data from store
            if not res.exists():
                # actually call function and write data
                data = fun(*args, **kwargs)
                byte_data = self.__to_bytes(data)
                res.write(data=byte_data)

                # add job description as metadata
                job_description = get_job_description(fun, args, kwargs)
                metadata = {"source.creation": job_description}
                res.metadata.update(metadata)

            # load data from storage
            with res.open() as file:
                byte_data = file.read()
            data = self.__from_bytes(byte_data)
            return data

        return _fun
