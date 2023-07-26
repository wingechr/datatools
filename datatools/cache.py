import functools
import hashlib
import json
import logging
import pickle

DEFAULT_FROM_BYTES = pickle.loads
DEFAULT_TO_BYTES = pickle.dumps


def get_hash(object):
    bytes = json.dumps(object, sort_keys=True, ensure_ascii=False).encode()
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
    return f"{f_name}_{job_id}"


def cache(
    storage,
    get_path=None,
    from_bytes=None,
    to_bytes=None,
    path_prefix=None,
):
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
                storage.metadata_put(data_path=norm_data_path, metadata=metadata)

            byte_data = storage.data_get(data_path=data_path)
            logging.debug("Loaded from cache")
            data = from_bytes(byte_data)

            return data

        return _fun

    return decorator
