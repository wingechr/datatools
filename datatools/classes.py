# NOTE: we dont actually use ABC/abstractmethod
# so that we can create decider instance
import abc
import hashlib
import json
import logging
import os
import re
import subprocess as sp
import traceback
from http import HTTPStatus
from typing import Union
from urllib.parse import parse_qs, unquote_plus
from wsgiref.simple_server import make_server

import jsonpath_ng
import requests

from . import exceptions
from .exceptions import DataDoesNotExists, DataExists, DatatoolsException, InvalidPath
from .utils import get_default_storage_location, get_query_arg, normalize_path

# remote
PARAM_HASH_METHOD = "hash"
PARAM_METADATA_PATH = "p"
PARAM_DATA_PATH = "path"
PARAM_VALUE = "value"
HASHED_DATA_PATH_PREFIX = "hash/"
DEFAULT_HASH_METHOD = "md5"
ROOT_METADATA_PATH = "$"  # root
DEFAULT_PORT = 8000


class AbstractStorage(abc.ABC):
    def __init__(self, location):
        self._location = location
        logging.debug(f"Location: {self._location}")

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
        return norm_path

    def metadata_get(self, data_path: str, metadata_path: str = None) -> object:
        raise NotImplementedError()

    def metadata_put(self, data_path: str, metadata: dict) -> None:
        raise NotImplementedError()

    def data_put(
        self, data: bytes, data_path: str = None, hash_method: str = None
    ) -> str:
        raise NotImplementedError()

    def data_get(self, data_path: str) -> bytes:
        raise NotImplementedError()

    def data_delete(self, data_path: str) -> None:
        raise NotImplementedError()


class Storage(AbstractStorage):
    def __new__(self, location):
        """Switch"""
        if re.match("https?://", location or ""):
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

    def data_put(
        self, data: bytes, data_path: str = None, hash_method: str = None
    ) -> str:
        hash_method = hash_method or DEFAULT_HASH_METHOD
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

        metadata = {f"hash.{hash_method}": hashsum}
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
            logging.debug(f"DELETING {data_filepath}")
            os.remove(data_filepath)
        return None

    def _create_metadata_path_pattern(self, metadata_path: str) -> str:
        metadata_path = metadata_path or ROOT_METADATA_PATH
        metadata_path = jsonpath_ng.parse(metadata_path)
        return metadata_path

    def _get_data_filepath(self, norm_data_path: str):
        filepath = os.path.join(self._location, norm_data_path)
        return filepath

    def _get_metadata_filepath(self, data_path: str):
        norm_data_path = self._normalize_data_path(data_path=data_path)
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        metadata_filepath = data_filepath + ".metadata.json"
        os.makedirs(os.path.dirname(metadata_filepath), exist_ok=True)
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

    def data_put(
        self, data: bytes, data_path: str = None, hash_method: str = None
    ) -> str:
        if data_path:
            result = self._request(
                method="PUT",
                data_path=data_path,
                data=data,
                params={PARAM_HASH_METHOD: hash_method},
            )
        else:
            result = self._request(
                method="POST",
                data_path="",
                data=data,
                params={PARAM_HASH_METHOD: hash_method},
            )
        return result.json()[PARAM_DATA_PATH]

    def data_get(self, data_path: str) -> bytes:
        return self._request(method="GET", data_path=data_path).content

    def data_delete(self, data_path: str) -> None:
        self._request("DELETE", data_path=data_path)
        return None

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
        url = self._location + url_path
        # make sure data is bytes
        if not (data is None or isinstance(data, bytes)):
            data = json.dumps(data).encode()
        logging.debug(f"CLI REQ: {method} {norm_data_path}")
        res = requests.request(method=method, url=url, data=data, params=params)
        logging.debug(f"CLI RES: {res.status_code}")
        if not res.ok:
            try:
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

    def data_put(self, data, args, kwargs):
        if args:
            data_path = args[0]
        else:
            data_path = None
        hash_method = get_query_arg(kwargs, PARAM_HASH_METHOD)
        data_path = self._storage.data_put(
            data=data, data_path=data_path, hash_method=hash_method
        )
        return {PARAM_DATA_PATH: data_path}

    def metadata_put(self, data, args, _kwargs):
        data_path = args[0]
        metadata = json.loads(data.decode())
        self._storage.metadata_put(data_path, metadata=metadata)

    def data_get_or_metadata_get(self, _data, args, kwargs):
        data_path = args[0]
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
        data_path = args[0]
        self._storage.data_delete(data_path=data_path)


class StorageServer:
    def __init__(self, storage, port=None):
        self._port = port or DEFAULT_PORT
        self._host = "localhost"
        self._server = make_server(self._host, self._port, self.application)

        routes = StorageServerRoutes(storage)
        self._routes = [
            (re.compile("PUT (.+)"), routes.data_put),
            (re.compile("POST"), routes.data_put),
            (re.compile("GET (.+)"), routes.data_get_or_metadata_get),
            (re.compile("DELETE (.+)"), routes.data_delete),
            (re.compile("PATCH (.+)"), routes.metadata_put),
        ]

    def serve_forever(self):
        logging.debug(f"Start serving on {self._port}")
        self._server.serve_forever()

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
                logging.warning(exc)
                status_code = 400
                result = {"error_msg": str(exc), "error_cls": exc.__class__.__name__}
            except Exception:
                logging.error(traceback.format_exc())
                status_code = 500

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


class TestCliStorage(AbstractStorage):
    def __init__(self, location):
        self.location = location
        # self.path_main = os.path.join(
        #    os.path.dirname(datatools.__file__), "__main__.py"
        # )
        # assert os.path.isfile(self.path_main)

    def _call(self, data, args):
        cmd = ["python", "-m", "datatools", "-d", self.location, "-l", "debug"] + args
        proc = sp.Popen(cmd, stdout=sp.PIPE, stdin=sp.PIPE, stderr=sp.PIPE)
        logging.debug(" ".join(cmd) + f" ({proc.pid})")
        out, err = proc.communicate(data)
        if proc.returncode:
            try:
                # try to get erro class / message
                err_text = err.decode().splitlines()
                # last line
                err_text = [x.strip() for x in err_text if x.strip()]
                err_text = err_text[-1]
                err_cls, err_msg = re.match(".*<([^:]+): ([^>]+)>", err_text).groups()
                err_cls = getattr(exceptions, err_cls)
            except Exception:
                logging.error("cannot parse error")
                logging.error(err.decode().splitlines()[-1])
                err_cls = Exception
                err_msg = err
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

    def data_put(
        self, data: bytes, data_path: str = None, hash_method: str = None
    ) -> str:
        file_path = "-"
        args = ["data-put", file_path]
        if data_path:
            args += [data_path]
        if hash_method:
            args += ["-h", hash_method]

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

    def serve(self, location=None, port=None):
        cmd = [
            "python",
            "-m",
            "datatools",
            "-d",
            location,
            "serve",
            "--port",
            str(port),
        ]
        proc = sp.Popen(cmd)
        logging.debug(" ".join(cmd) + f" ({proc.pid})")
        return proc
