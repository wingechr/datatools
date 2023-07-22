# NOTE: we dont actually use ABC/abstractmethod
# so that we can create decider instance
import abc
import hashlib
import json
import logging
import os
import re
from http import HTTPStatus
from typing import Union
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

import jsonpath_ng
import requests

from .exceptions import DataDoesNotExists, DataExists, DatatoolsException, InvalidPath
from .utils import normalize_path

# remote
PARAM_HASH_METHOD = "hash"
PARAM_METADATA_PATH = "p"
PARAM_DATA_PATH = "path"
PARAM_VALUE = "value"
HASHED_DATA_PATH_PREFIX = "hash/"
DEFAULT_HASH_METHOD = "md5"
ROOT_METADATA_PATH = "$"  # root
DEFAULT_LOCATION = "./data"
DEFAULT_PORT = 80


class DatatoolsAbstract(abc.ABC):
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

    def metadata_get(self, data_path: str, metadata_path: str = None) -> list:
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


class Datatools(DatatoolsAbstract):
    def __new__(self, location):
        """Switch"""
        if re.match("https?://", location or ""):
            logging.debug("REMOTE INSTANCE")
            return DatatoolsRemote(location=location)
        else:
            location = os.path.abspath(location or DEFAULT_LOCATION)
            logging.debug("LOCAL INSTANCE")
            return DatatoolsLocal(location=location)


class DatatoolsLocal(DatatoolsAbstract):
    def metadata_get(self, data_path: str, metadata_path: str = None) -> list:
        metadata_path_pattern = self._create_metadata_path_pattern(metadata_path)
        metadata_filepath = self._get_metadata_filepath(data_path=data_path)
        if not os.path.exists(metadata_filepath):
            return None
        logging.debug(f"READING {metadata_filepath}")
        with open(metadata_filepath, "rb") as file:
            metadata = json.load(file)
        match = metadata_path_pattern.find(metadata)
        # TODO: alyways return list?
        result = [x.value for x in match]
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
        if not data_path:
            hash_method = hash_method or DEFAULT_HASH_METHOD
            hasher = getattr(hashlib, hash_method)()
            hasher.update(data)
            hashsum = hasher.hexdigest()
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


class DatatoolsRemote(DatatoolsAbstract):
    """ """

    def metadata_get(self, data_path: str, metadata_path: str = None) -> list:
        metadata_path = metadata_path or ROOT_METADATA_PATH
        result = self._request(
            method="GET",
            path=data_path,
            params={PARAM_METADATA_PATH: metadata_path},
        ).json()
        return result[PARAM_VALUE]

    def metadata_put(self, data_path: str, metadata: dict) -> None:
        self._request(method="PACH", path=data_path, data=metadata)

    def data_put(
        self, data: bytes, data_path: str = None, hash_method: str = None
    ) -> str:
        if data_path:
            result = self._request(
                method="PUT",
                path=data_path,
                data=data,
                params={PARAM_HASH_METHOD: hash_method},
            )
        else:
            result = self._request(
                method="POST",
                path="",
                data=data,
                params={PARAM_HASH_METHOD: hash_method},
            )
        return result.json()[PARAM_DATA_PATH]

    def data_get(self, data_path: str) -> bytes:
        return self._request(method="GET", path=data_path).content

    def data_delete(self, data_path: str) -> None:
        self._request("DELETE", data_path)
        return None

    def _request(
        self,
        method: str,
        data_path: str,
        data: Union[bytes, object] = None,
        params: dict = None,
    ) -> bytes:
        norm_data_path = self._normalize_data_path(data_path)
        url_path = "/" + norm_data_path
        url = self.location + url_path
        # make sure data is bytes
        if not (data is None or isinstance(data, bytes)):
            data = json.dumps(data).encode()
        res = requests.request(method=method, url=url, data=data, params=params)
        if not res.ok:
            try:
                message = res.json()
            except Exception as exc:
                # FXIME: only debug
                message = str(exc)
            raise Exception(message)
        return res


class DatatoolsServer:
    def __init__(self, location=None, port=None):
        self._port = port or DEFAULT_PORT
        self._host = "localhost"
        self._server = make_server(self._host, self._port, self.application)
        self._instance = Datatools(location=location)
        self._routes = {}

    def serve_forever(self):
        self._server.serve_forever()

    def __enter__(self):
        self._instance.__enter__()
        return self

    def __exit__(self, *args):
        self._instance.__exit__(*args)

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

        status_code = 200
        result = b""

        # routing
        routing_pattern = f"{method} {path}"
        selected_handler = None
        path_args = tuple()
        for pat, handler in self._routes.items():
            match = pat.matches(routing_pattern)
            if match:
                path_args = match.groups()
                selected_handler = handler
                break
        if not selected_handler:
            raise NotImplementedError(routing_pattern)

        try:
            result = selected_handler(bdata, *path_args, **query)
        except DatatoolsException:
            status = 400
        except Exception:
            status = 500

        if isinstance(result, str):
            result = result.encode()
        if not isinstance(result, bytes):
            result = json.dumps(result).encode()

        content_length_result = len(result)
        # TODO get other success codes
        status = "%s %s" % (status_code, HTTPStatus(status_code).phrase)
        output_content_type = ""  # TODO
        response_headers = []
        response_headers += [
            ("Content-type", output_content_type),
            ("Content-Length", str(content_length_result)),
        ]
        start_response(status, response_headers)

        return [result]
