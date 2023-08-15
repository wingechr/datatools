import functools
import hashlib
import json
import logging
import pickle
from typing import Callable

from datatools.utils import json_serialize

DEFAULT_FROM_BYTES = pickle.loads
DEFAULT_TO_BYTES = pickle.dumps
DEFAULT_MEDIA_TYPE = "application/x-pickle"


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


def default_get_path(fun, args, kwargs):
    f_name = f"{fun.__name__}"
    job_desc = get_job_description(fun, args, kwargs)
    # we only hash part of it:
    job_id = get_hash({"args": job_desc["args"], "kwargs": job_desc["kwargs"]})
    # we can shorten the hash: 8 to 10 chars should be enough
    job_id = job_id[:8]
    return f"cache/{f_name}_{job_id}.pickle"


def cache(
    storage,
    get_path=None,
    from_bytes=None,
    to_bytes=None,
    path_prefix=None,
) -> Callable:
    """ """
    get_path = get_path or default_get_path
    from_bytes = from_bytes or DEFAULT_FROM_BYTES
    to_bytes = to_bytes or DEFAULT_TO_BYTES
    path_prefix = path_prefix or ""

    def decorator(fun):
        @functools.wraps(fun)
        def _fun(*args, **kwargs):
            # get data_path from function + arguments
            data_path = path_prefix + get_path(fun, args, kwargs)
            # try to get data from store
            if not storage.data_exists(data_path=data_path):
                job_description = get_job_description(fun, args, kwargs)
                # actually call function
                data = fun(*args, **kwargs)
                metadata = {"source.creation": job_description}
                byte_data = to_bytes(data)
                norm_data_path = storage.data_put(byte_data, data_path)
                storage.metadata_set(data_path=norm_data_path, metadata=metadata)

            with storage.data_open(data_path=data_path) as file:
                byte_data = file.read()
            logging.debug("Loaded from cache")
            data = from_bytes(byte_data)

            return data

        return _fun

    return decorator
