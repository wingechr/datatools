# NOTE: we dont actually use ABC/abstractmethod
# so that we can create decider instance
import abc
import hashlib
import json
import logging
import os
import re
import subprocess as sp
import sys
import traceback
from http import HTTPStatus
from typing import Union
from urllib.parse import parse_qs, unquote_plus
from wsgiref.simple_server import make_server

import jsonpath_ng
import requests

from . import exceptions
from .cache import cache
from .exceptions import DataDoesNotExists, DataExists, DatatoolsException, InvalidPath
from .utils import (
    LOCALHOST,
    get_default_storage_location,
    get_now_str,
    get_user_w_host,
    make_file_readonly,
    make_file_writable,
    normalize_path,
)

# remote
PARAM_METADATA_PATH = "p"
PARAM_DATA_PATH = "path"
PARAM_VALUE = "value"
HASHED_DATA_PATH_PREFIX = "hash/"
DEFAULT_HASH_METHOD = "md5"
ROOT_METADATA_PATH = "$"  # root
DEFAULT_PORT = 8000
ALLOWED_HASH_METHODS = ["md5", "sha256"]


class AbstractStorage(abc.ABC):
    def __init__(self, location):
        self.location = location
        logging.debug(f"Location: {self.location}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def _normalize_data_path(self, data_path: str) -> str:
        """should be all lowercase"""
        norm_path = normalize_path(data_path)
        # TODO: maybe later: remove double check:
        if norm_path != normalize_path(norm_path):
            raise InvalidPath(data_path)
        if re.match(r".*\.metadata\..*", norm_path):
            raise InvalidPath(data_path)
        logging.debug(f"Translating {data_path} => {norm_path}")
        return norm_path

    def cache(
        self,
        get_path=None,
        from_bytes=None,
        to_bytes=None,
        path_prefix: str = None,
    ):
        """decorator"""
        return cache(
            storage=self,
            get_path=get_path,
            from_bytes=from_bytes,
            to_bytes=to_bytes,
            path_prefix=path_prefix,
        )

    def metadata_get(self, data_path: str, metadata_path: str = None) -> object:
        raise NotImplementedError()

    def metadata_put(self, data_path: str, metadata: dict) -> None:
        raise NotImplementedError()

    def data_put(self, data: bytes, data_path: str = None) -> str:
        raise NotImplementedError()

    def data_get(self, data_path: str) -> bytes:
        raise NotImplementedError()

    def data_delete(self, data_path: str) -> None:
        raise NotImplementedError()

    def data_exists(self, data_path: str) -> bool:
        raise NotImplementedError()


class Storage(AbstractStorage):
    def __new__(self, location=None):
        """Switch"""
        if location and re.match("https?://", location or ""):
            logging.debug("REMOTE INSTANCE")
            return RemoteStorage(location=location)
        else:
            location = os.path.abspath(location or get_default_storage_location())
            logging.debug("LOCAL INSTANCE")
            return LocalStorage(location=location)


class LocalStorage(AbstractStorage):
    def metadata_get(self, data_path: str, metadata_path: str = None) -> object:
        metadata_path_pattern = self._create_metadata_path_pattern(metadata_path)
        metadata_filepath = self._get_metadata_filepath(data_path=data_path)
        if not os.path.exists(metadata_filepath):
            return None
        logging.debug(f"READING {metadata_filepath}")
        with open(metadata_filepath, "rb") as file:
            metadata = json.load(file)
        match = metadata_path_pattern.find(metadata)
        result = [x.value for x in match]
        # TODO: we always get a list (multiple matches),
        # but most of the time, we want only one
        if len(result) == 1:
            result = result[0]
        logging.debug(f"get metadata: {metadata_path} => {result}")
        return result

    def metadata_put(self, data_path: str, metadata: dict) -> None:
        metadata_filepath = self._get_metadata_filepath(data_path=data_path)

        if not os.path.exists(metadata_filepath):
            _metadata = {}
        else:
            logging.debug(f"READING {metadata_filepath}")
            mode = "rb"
            with open(metadata_filepath, mode) as file:
                _metadata = json.load(file)

        for metadata_path, value in metadata.items():
            metadata_path_pattern = self._create_metadata_path_pattern(metadata_path)
            logging.debug(f"update metadata: {metadata_path} => {value}")
            metadata_path_pattern.update_or_create(_metadata, value)

        metadata_bytes = json.dumps(_metadata, indent=2, ensure_ascii=False).encode()

        logging.debug(f"WRITING {metadata_filepath}")
        mode = "wb"
        with open(metadata_filepath, mode) as file:
            file.write(metadata_bytes)

        return None

    def data_put(self, data: bytes, data_path: str = None) -> str:
        if not data_path:
            data_path = f"{HASHED_DATA_PATH_PREFIX}{DEFAULT_HASH_METHOD}"
        if data_path.startswith(HASHED_DATA_PATH_PREFIX):
            offset = len(HASHED_DATA_PATH_PREFIX)
            hash_method = data_path[offset:]
            if hash_method not in ALLOWED_HASH_METHODS:
                raise InvalidPath(data_path)
            data_path = None
        else:
            hash_method = DEFAULT_HASH_METHOD

        hasher = getattr(hashlib, hash_method)()
        hasher.update(data)
        hashsum = hasher.hexdigest()

        if not data_path:
            data_path = f"{HASHED_DATA_PATH_PREFIX}{hash_method}/{hashsum}"
            norm_data_path = self._normalize_data_path(data_path=data_path)
            data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
            if os.path.exists(data_filepath):
                logging.info("data already existed")
        else:
            norm_data_path = self._normalize_data_path(data_path=data_path)
            if norm_data_path.startswith(HASHED_DATA_PATH_PREFIX):
                raise InvalidPath(data_path)

            data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
            if os.path.exists(data_filepath):
                raise DataExists(data_path)

        os.makedirs(os.path.dirname(data_filepath), exist_ok=True)
        logging.debug(f"WRITING {data_filepath}")
        with open(data_filepath, "wb") as file:
            file.write(data)
        make_file_readonly(data_filepath)

        # write metadata
        metadata = {
            f"hash.{hash_method}": hashsum,
            "size": len(data),
            "source.user": get_user_w_host(),
            "source.datetime": get_now_str(),
            "source.name": norm_data_path,
        }
        self.metadata_put(data_path=norm_data_path, metadata=metadata)

        return norm_data_path

    def data_get(self, data_path: str) -> bytes:
        norm_data_path = self._normalize_data_path(data_path=data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        if not os.path.exists(data_filepath):
            raise DataDoesNotExists(data_path)
        logging.debug(f"READING {data_filepath}")
        with open(data_filepath, "rb") as file:
            data = file.read()
        return data

    def data_delete(self, data_path: str) -> None:
        """
        delete file (or do nothing if it soed not exist)
        """
        norm_data_path = self._normalize_data_path(data_path=data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        if os.path.exists(data_filepath):
            make_file_writable(data_filepath)
            logging.debug(f"DELETING {data_filepath}")
            os.remove(data_filepath)
        return None

    def data_exists(self, data_path: str) -> bool:
        norm_data_path = self._normalize_data_path(data_path=data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        return os.path.exists(data_filepath)

    def _create_metadata_path_pattern(self, metadata_path: str) -> str:
        metadata_path = metadata_path or ROOT_METADATA_PATH
        metadata_path = jsonpath_ng.parse(metadata_path)
        return metadata_path

    def _get_data_filepath(self, norm_data_path: str):
        filepath = os.path.join(self.location, norm_data_path)
        filepath = os.path.abspath(filepath)
        return filepath

    def _get_metadata_filepath(self, data_path: str):
        norm_data_path = self._normalize_data_path(data_path=data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        metadata_filepath = data_filepath + ".metadata.json"
        os.makedirs(os.path.dirname(metadata_filepath), exist_ok=True)
        metadata_filepath = os.path.abspath(metadata_filepath)
        return metadata_filepath


class RemoteStorage(AbstractStorage):
    """ """

    def metadata_get(self, data_path: str, metadata_path: str = None) -> object:
        metadata_path = metadata_path or ROOT_METADATA_PATH
        result = self._request(
            method="GET",
            data_path=data_path,
            params={PARAM_METADATA_PATH: metadata_path},
        ).json()
        return result[PARAM_VALUE]

    def metadata_put(self, data_path: str, metadata: dict) -> None:
        self._request(method="PATCH", data_path=data_path, data=metadata)

    def data_put(self, data: bytes, data_path: str = None) -> str:
        if not data_path:
            data_path = f"{HASHED_DATA_PATH_PREFIX}{DEFAULT_HASH_METHOD}"

        if data_path.startswith(HASHED_DATA_PATH_PREFIX):
            method = "POST"
        else:
            method = "PUT"
        result = self._request(
            method=method,
            data_path=data_path,
            data=data,
        )
        return result.json()[PARAM_DATA_PATH]

    def data_get(self, data_path: str) -> bytes:
        return self._request(method="GET", data_path=data_path).content

    def data_delete(self, data_path: str) -> None:
        self._request("DELETE", data_path=data_path)
        return None

    def data_exists(self, data_path: str) -> bool:
        try:
            self._request("HEAD", data_path=data_path)
            return True
        except DataDoesNotExists:
            return False

    def _request(
        self,
        method: str,
        data_path: str,
        data: Union[bytes, object] = None,
        params: dict = None,
    ) -> bytes:
        if data_path:
            norm_data_path = self._normalize_data_path(data_path)
        else:
            norm_data_path = ""
        url_path = "/" + norm_data_path
        url = self.location + url_path
        # make sure data is bytes
        if not (data is None or isinstance(data, bytes)):
            data = json.dumps(data).encode()
        logging.debug(f"CLI REQ: {method} {norm_data_path}")
        res = requests.request(method=method, url=url, data=data, params=params)
        logging.debug(f"CLI RES: {res.status_code}")
        if not res.ok:
            try:
                if method == "HEAD":
                    # no body
                    message = {"error_msg": "", "error_cls": DataDoesNotExists.__name__}
                else:
                    message = res.json()
                error_msg = message["error_msg"]
                error_cls = getattr(exceptions, message["error_cls"])
            except Exception:
                # FXIME: only debug
                error_msg = res.content
                error_cls = Exception
            logging.error(f"{error_cls.__name__}: {error_msg}")
            raise error_cls(error_msg)
        return res


class StorageServerRoutes:
    def __init__(self, storage):
        self._storage = storage

    def data_put(self, data, args, _kwargs):
        data_path = args[0].lstrip("/")
        data_path = self._storage.data_put(data=data, data_path=data_path)
        return {PARAM_DATA_PATH: data_path}

    def metadata_put(self, data, args, _kwargs):
        data_path = args[0].lstrip("/")
        metadata = json.loads(data.decode())
        self._storage.metadata_put(data_path, metadata=metadata)

    def data_get_or_metadata_get(self, _data, args, kwargs):
        data_path = args[0].lstrip("/")
        metadata_path = kwargs.get(PARAM_METADATA_PATH)
        if metadata_path:
            # is list
            metadata_path = metadata_path[0]
            metadata_path = unquote_plus(metadata_path)
            metadata = self._storage.metadata_get(
                data_path=data_path, metadata_path=metadata_path
            )
            data = {PARAM_VALUE: metadata}
        else:
            data = self._storage.data_get(data_path=data_path)

        return data

    def data_delete(self, _data, args, _kwargs):
        data_path = args[0].lstrip("/")
        self._storage.data_delete(data_path=data_path)

    def data_exists(self, _data, args, _kwargs):
        data_path = args[0].lstrip("/")
        if not data_path:  # only for testing if server works
            return None
        if not self._storage.data_exists(data_path=data_path):
            raise DataDoesNotExists(data_path)


class StorageServer:
    def __init__(self, storage, port=None):
        self.port = port or DEFAULT_PORT
        self.server = make_server(LOCALHOST, self.port, self.application)

        routes = StorageServerRoutes(storage)
        self._routes = [
            (re.compile("HEAD (.+)"), routes.data_exists),
            (re.compile("PUT (.+)"), routes.data_put),
            (re.compile("POST (.+)"), routes.data_put),
            (re.compile("GET (.+)"), routes.data_get_or_metadata_get),
            (re.compile("DELETE (.+)"), routes.data_delete),
            (re.compile("PATCH (.+)"), routes.metadata_put),
        ]

    def serve_forever(self):
        logging.debug(f"Start serving on {self.port}")
        self.server.serve_forever()

    def application(self, environ, start_response):
        method = environ["REQUEST_METHOD"].upper()
        path = environ["PATH_INFO"]
        query = parse_qs(environ["QUERY_STRING"], strict_parsing=False)
        content_length = int(environ["CONTENT_LENGTH"] or "0")
        if content_length:
            # TODO: we would want to just pass
            # input to main.save, but we need to specify the number of bytes
            # in advance
            input = environ["wsgi.input"]
            bdata = input.read(content_length)
        else:
            bdata = b""

        status_code = 404
        result = b""
        output_content_type = "application/octet-stream"

        # routing
        routing_pattern = f"{method} {path}"
        selected_handler = None
        path_args = tuple()
        logging.debug(f"SRV REQ: {routing_pattern}")
        for pat, handler in self._routes:
            match = pat.match(routing_pattern)
            if match:
                path_args = match.groups()
                selected_handler = handler
                break

        if selected_handler:
            try:
                logging.debug(
                    f"SRV: {selected_handler.__name__}"
                    f"({len(bdata)}, {path_args}, {query})"
                )
                result = selected_handler(bdata, path_args, query)
                status_code = 200
            except DatatoolsException as exc:
                if isinstance(exc, DataDoesNotExists):
                    status_code = 404
                else:
                    status_code = 400
                result = {"error_msg": str(exc), "error_cls": exc.__class__.__name__}
            except Exception:
                logging.error(traceback.format_exc())
                status_code = 500

        if method == "HEAD":
            # HEAD: no body
            result = None

        if isinstance(result, str):
            output_content_type = "text/plain"
            result = result.encode()
        if not isinstance(result, bytes):
            output_content_type = "application/json"
            result = json.dumps(result).encode()

        content_length_result = len(result)
        # TODO get other success codes
        status = "%s %s" % (status_code, HTTPStatus(status_code).phrase)

        response_headers = []
        response_headers += [
            ("Content-Type", output_content_type),
            ("Content-Length", str(content_length_result)),
        ]

        logging.debug(f"SRV RES: {status} {response_headers}")

        start_response(status, response_headers)

        return [result]


class _TestCliStorage(AbstractStorage):
    def _call(self, data, args):
        cmd = [
            sys.executable,
            "-m",
            "datatools",
            "-d",
            self.location,
            "-l",
            "debug",
        ] + args
        proc = sp.Popen(cmd, stdout=sp.PIPE, stdin=sp.PIPE, stderr=sp.PIPE)
        logging.debug(" ".join(cmd) + f" ({proc.pid})")
        out, err = proc.communicate(data)
        if proc.returncode:
            # try to get error class / message
            err_text = err.decode().splitlines()
            # last line
            err_text = [x.strip() for x in err_text if x.strip()]
            err_text = err_text[-1]
            try:
                err_cls, err_msg = re.match(".*<([^:]+): ([^>]+)>", err_text).groups()
                err_cls = getattr(exceptions, err_cls)
            except Exception:
                logging.error(f"cannot parse error: {err_text}")
                err_cls = Exception
                err_msg = err_text
            raise err_cls(err_msg)

        return out

    def metadata_get(self, data_path: str, metadata_path: str = None) -> object:
        args = ["metadata-get", data_path]
        if metadata_path:
            args += [metadata_path]
        res = self._call(b"", args)
        res = json.loads(res.decode().strip())
        return res

    def metadata_put(self, data_path: str, metadata: dict) -> None:
        meta_key_vals = []
        for key, val in metadata.items():
            meta_key_vals.append(f"{key}={val}")
        args = ["metadata-put", data_path] + meta_key_vals
        self._call(b"", args)

    def data_put(self, data: bytes, data_path=None) -> str:
        if isinstance(data, str):
            file_path = data
            data = None
        else:
            file_path = "-"
        args = ["data-put", file_path]
        if data_path:
            args += [data_path]

        res = self._call(data, args)
        return res.decode().strip()

    def data_get(self, data_path: str) -> bytes:
        file_path = "-"
        args = ["data-get", data_path, file_path]
        res = self._call(b"", args)
        return res

    def data_delete(self, data_path: str) -> bytes:
        args = ["data-delete", data_path]
        self._call(b"", args)

    def data_exists(self, data_path: str) -> bool:
        args = ["data-exists", data_path]
        try:
            self._call(b"", args)
            return True
        except DataDoesNotExists:
            return False

    def serve(self, port=None):
        cmd = [
            sys.executable,
            "-m",
            "datatools",
            "-d",
            self.location,
            "serve",
            "--port",
            str(port),
        ]
        proc = sp.Popen(cmd)
        logging.debug(" ".join(cmd) + f" ({proc.pid})")
        return proc
