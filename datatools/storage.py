# NOTE: we dont actually use ABC/abstractmethod
# so that we can create decider instance
import abc
import json
import logging
import os
import re
import subprocess as sp
import sys
import traceback
from http import HTTPStatus
from io import BufferedReader, BytesIO
from tempfile import NamedTemporaryFile
from typing import Union
from urllib.parse import parse_qs, unquote_plus
from wsgiref.simple_server import make_server

import jsonpath_ng
import requests

from .cache import DEFAULT_FROM_BYTES, DEFAULT_MEDIA_TYPE, cache
from .exceptions import (
    DataDoesNotExists,
    DataExists,
    DatatoolsException,
    IntegrityError,
    InvalidPath,
    raise_err,
)
from .load import read_uri
from .utils import (
    LOCALHOST,
    BufferedReaderHashWrapper,
    BufferedReaderIterator,
    BufferedReaderMaxSizeWrapper,
    get_default_storage_location,
    get_now_str,
    get_user_w_host,
    is_uri,
    json_serialize,
    make_file_readonly,
    make_file_writable,
    normalize_path,
)

# remote
PARAM_METADATA_PATH = "p"
PARAM_DATA_PATH = "path"
PARAM_VALUE = "value"
ROOT_METADATA_PATH = "$"  # root
DEFAULT_PORT = 8000
HASHED_DATA_PATH_PREFIX = "hash/"
ALLOWED_HASH_METHODS = ["md5", "sha256"]

DEFAULT_HASH_METHOD = ALLOWED_HASH_METHODS[0]

HEADER_DATA_PATH = "X-DATATOOLS-PATH"
HEADER_ERROR = "X-DATATOOLS-ERROR"


class AbstractStorage(abc.ABC):
    def __init__(self, location):
        self.location = location
        logging.debug(f"Location: {self.location}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def _get_norm_data_path(self, data_path: str) -> str:
        """should be all lowercase
        Returns:
            str: norm_data_path
        Raises:
            InvalidPath
        """
        norm_data_path = normalize_path(data_path)
        # TODO: maybe later: remove double check:
        if norm_data_path != normalize_path(norm_data_path):
            raise InvalidPath(data_path)
        if re.match(r".*\.metadata\..*", norm_data_path):
            raise InvalidPath(data_path)
        return norm_data_path

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

    def _metadata_get(self, norm_data_path: str, metadata_path: str) -> object:
        raise NotImplementedError()

    def _metadata_put(self, norm_data_path: str, metadata: dict) -> None:
        raise NotImplementedError()

    def _data_put(
        self, norm_data_path: str, data: BufferedReader, exist_ok: bool
    ) -> str:
        """Returns norm_data_path"""
        raise NotImplementedError()

    def _data_open(self, norm_data_path: str) -> BufferedReader:
        raise NotImplementedError()

    def _data_delete(self, norm_data_path: str) -> None:
        raise NotImplementedError()

    def _data_exists(self, norm_data_path: str) -> str:
        """Returns norm_data_path"""
        raise NotImplementedError()

    def metadata_get(self, data_path: str, metadata_path: str = None) -> object:
        metadata_path = metadata_path or ROOT_METADATA_PATH
        norm_data_path = self._get_norm_data_path(data_path)
        return self._metadata_get(
            norm_data_path=norm_data_path, metadata_path=metadata_path
        )

    def metadata_put(self, data_path: str, metadata: dict) -> None:
        norm_data_path = self._get_norm_data_path(data_path)
        return self._metadata_put(norm_data_path=norm_data_path, metadata=metadata)

    def data_put(
        self, data: Union[BufferedReader, bytes], data_path: str = None, exist_ok=None
    ) -> str:
        # if not datapath: map to default hash endpoint
        if not data_path:
            norm_data_path = self._get_norm_data_path(
                f"{HASHED_DATA_PATH_PREFIX}{DEFAULT_HASH_METHOD}"
            )
            if exist_ok is False:
                logging.warning("without a data path, exist_ok = False will be ignored")
            exist_ok = True
        else:
            exist_ok = bool(exist_ok)
            norm_data_path = self._get_norm_data_path(data_path)

        if isinstance(data, bytes):
            data = BufferedReaderMaxSizeWrapper(BytesIO(data), max_size=len(data))

        return self._data_put(
            data=data, norm_data_path=norm_data_path, exist_ok=exist_ok
        )

    def data_open(self, data_path: str, auto_load_uri: bool = False) -> BufferedReader:
        if auto_load_uri and not self.data_exists(data_path=data_path):
            if not is_uri(data_path):
                raise NotImplementedError(
                    "auto_load_uri only works if data_path is uri"
                )
            uri = data_path
            data, metadata = read_uri(uri)
            norm_data_path = self.data_put(
                data=data, data_path=data_path, exist_ok=False
            )
            if metadata:
                self._metadata_put(norm_data_path=norm_data_path, metadata=metadata)

        norm_data_path = self._get_norm_data_path(data_path)

        return self._data_open(norm_data_path=norm_data_path)

    def data_get(
        self, data_path: str, auto_load_uri: bool = False, auto_decode: bool = False
    ) -> Union[bytes, object]:
        with self.data_open(data_path=data_path, auto_load_uri=auto_load_uri) as file:
            data = file.read()

        if auto_decode:
            # MUST provide mediatype
            # TODO: select decode based on media type: right now: only default (pickle)
            mediatype = self.metadata_get(
                data_path=data_path, metadata_path="mediatype"
            )
            if mediatype == DEFAULT_MEDIA_TYPE:
                data = DEFAULT_FROM_BYTES(data)
            else:
                raise NotImplementedError(
                    "Can only auto_decode if mediatype is registered"
                )

        return data

    def data_delete(self, data_path: str) -> None:
        norm_data_path = self._get_norm_data_path(data_path)
        return self._data_delete(norm_data_path=norm_data_path)

    def data_exists(self, data_path: str) -> str:
        norm_data_path = self._get_norm_data_path(data_path)
        return self._data_exists(norm_data_path=norm_data_path)


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
    # HELPER METHODS

    def _create_metadata_path_pattern(self, metadata_path: str) -> str:
        metadata_path_pattern = jsonpath_ng.parse(metadata_path)
        return metadata_path_pattern

    def _get_data_filepath(self, norm_data_path: str, create_parent_dir=False) -> str:
        data_filepath = os.path.join(self.location, norm_data_path)
        data_filepath = self._get_abs_path_with_parent(
            path=data_filepath, create_parent_dir=create_parent_dir
        )
        return data_filepath

    def _get_metadata_filepath(
        self, norm_data_path: str, create_parent_dir=False
    ) -> str:
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        metadata_filepath = data_filepath + ".metadata.json"
        metadata_filepath = self._get_abs_path_with_parent(
            path=metadata_filepath, create_parent_dir=create_parent_dir
        )
        return metadata_filepath

    def _get_abs_path_with_parent(self, path, create_parent_dir) -> str:
        path = os.path.abspath(path)
        if create_parent_dir:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    # METHODS FROM BASE CLASS

    def _metadata_get(self, norm_data_path: str, metadata_path: str) -> object:
        metadata_path_pattern = self._create_metadata_path_pattern(metadata_path)
        metadata_filepath = self._get_metadata_filepath(norm_data_path=norm_data_path)
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

    def _metadata_put(self, norm_data_path: str, metadata: dict) -> None:
        metadata_filepath = self._get_metadata_filepath(
            norm_data_path=norm_data_path, create_parent_dir=True
        )

        if not os.path.exists(metadata_filepath):
            _metadata = {}
        else:
            logging.debug(f"READING {metadata_filepath}")
            with open(metadata_filepath, "rb") as file:
                _metadata = json.load(file)

        for metadata_path, value in metadata.items():
            metadata_path_pattern = self._create_metadata_path_pattern(metadata_path)
            logging.debug(f"update metadata: {metadata_path} => {value}")
            metadata_path_pattern.update_or_create(_metadata, value)

        metadata_bytes = json.dumps(
            _metadata, indent=2, ensure_ascii=False, default=json_serialize
        ).encode()

        logging.debug(f"WRITING {metadata_filepath}")
        with open(metadata_filepath, "wb") as file:
            file.write(metadata_bytes)

        return None

    def _data_put(
        self, norm_data_path: str, data: BufferedReader, exist_ok: bool
    ) -> str:
        filepath_is_hash = norm_data_path.startswith(HASHED_DATA_PATH_PREFIX)

        if filepath_is_hash:
            offset = len(HASHED_DATA_PATH_PREFIX)
            hash_method = norm_data_path[offset:]
            if hash_method not in ALLOWED_HASH_METHODS:
                raise InvalidPath(norm_data_path)
            # add a placeholder for filename
            norm_data_path = norm_data_path + "/__HASHFILE__"
        else:
            hash_method = DEFAULT_HASH_METHOD

        if not filepath_is_hash and self._data_exists(norm_data_path=norm_data_path):
            if not exist_ok:
                raise DataExists(norm_data_path)
            logging.info(f"Skipping existing file: {norm_data_path}")
            return norm_data_path

        # write data
        data_filepath = self._get_data_filepath(
            norm_data_path=norm_data_path, create_parent_dir=True
        )
        data_dir = os.path.dirname(data_filepath)
        filename = os.path.basename(data_filepath)
        with NamedTemporaryFile(
            mode="wb", dir=data_dir, delete=False, prefix=filename + "+"
        ) as file:
            logging.debug(f"WRITING {file.name}")
            buf = BufferedReaderHashWrapper(data, hash_method=hash_method)
            buf = BufferedReaderIterator(buf)
            for chunk in buf:
                file.write(chunk)

        hashsum = buf.hexdigest()

        # get data hashsum
        # hasher = getattr(hashlib, hash_method)()
        # hasher.update(data)
        # assert hasher.hexdigest() == hashsum

        if filepath_is_hash:
            data_filepath = os.path.join(data_dir, hashsum)
            # replace placeholder
            n_placeholder = len("__HASHFILE__")
            norm_data_path = norm_data_path[:-n_placeholder] + hashsum

        if os.path.exists(data_filepath):
            os.remove(file.name)
            if filepath_is_hash:
                logging.debug(f"Skipping existing hashed file: {data_filepath}")
            else:
                raise IntegrityError(f"File should not exist: {data_filepath}")
        else:
            logging.debug(f"MOVE_TEMP {file.name} => {data_filepath}")
            os.rename(file.name, data_filepath)

        make_file_readonly(data_filepath)

        # write metadata
        metadata = {
            f"hash.{hash_method}": hashsum,
            "size": len(data),
            "source.user": get_user_w_host(),
            "source.datetime": get_now_str(),
            "source.name": norm_data_path,
        }
        self._metadata_put(norm_data_path=norm_data_path, metadata=metadata)

        return norm_data_path

    def _data_open(self, norm_data_path: str) -> BufferedReader:
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        if not os.path.exists(data_filepath):
            raise DataDoesNotExists(norm_data_path)
        logging.debug(f"READING {data_filepath}")
        # with open(data_filepath, "rb") as file:
        #    data = file.read()
        data = open(data_filepath, "rb")
        return data

    def _data_delete(self, norm_data_path: str) -> None:
        """
        delete file (or do nothing if it soed not exist)
        """
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        if os.path.exists(data_filepath):
            make_file_writable(data_filepath)
            logging.debug(f"DELETING {data_filepath}")
            os.remove(data_filepath)
        return None

    def _data_exists(self, norm_data_path: str) -> str:
        data_filepath = self._get_data_filepath(norm_data_path=norm_data_path)
        if os.path.exists(data_filepath):
            return norm_data_path
        return None


class RemoteStorage(AbstractStorage):
    """ """

    def _metadata_get(self, norm_data_path: str, metadata_path: str) -> object:
        result = self._request(
            method="GET",
            data_path=norm_data_path,
            params={PARAM_METADATA_PATH: metadata_path},
        ).json()
        return result[PARAM_VALUE]

    def _metadata_put(self, norm_data_path: str, metadata: dict) -> None:
        self._request(method="PATCH", data_path=norm_data_path, data=metadata)

    def _data_put(
        self, norm_data_path: str, data: BufferedReader, exist_ok: bool
    ) -> str:
        # TODO: unused exist_ok

        if norm_data_path.startswith(HASHED_DATA_PATH_PREFIX):
            method = "POST"
        else:
            method = "PUT"
        result = self._request(
            method=method,
            data_path=norm_data_path,
            data=data,
        )
        return result.json()[PARAM_DATA_PATH]

    def _data_open(self, norm_data_path: str) -> BufferedReader:
        return self._request(method="GET", data_path=norm_data_path).raw

    def _data_delete(self, norm_data_path: str) -> None:
        self._request("DELETE", data_path=norm_data_path)

    def _data_exists(self, norm_data_path: str) -> str:
        try:
            res = self._request("HEAD", data_path=norm_data_path)
            return res.headers.get(HEADER_DATA_PATH, "")
        except DataDoesNotExists:
            return None

    def _request(
        self,
        method: str,
        data_path: str,
        data: Union[bytes, object, BufferedReader] = None,
        params: dict = None,
    ) -> requests.Response:
        if data_path:
            norm_data_path = self._get_norm_data_path(data_path)
        else:
            norm_data_path = ""
        url_path = "/" + norm_data_path
        url = self.location + url_path
        # make sure data is bytes
        if data is not None:
            if isinstance(data, dict):
                data = json.dumps(data, default=json_serialize).encode()
            if isinstance(data, bytes):
                data = BytesIO(data)
            buf = data
        else:
            buf = None

        logging.debug(f"CLI REQ: {method} {norm_data_path}")

        res = requests.request(
            method=method, url=url, data=buf, params=params, stream=True
        )

        logging.debug(f"CLI RES: {res.status_code} {res.headers}")
        if not res.ok:
            # get error from header
            raise_err(res.headers.get(HEADER_ERROR, ""))
        return res


class StorageServerRoutes:
    def __init__(self, storage):
        self._storage = storage

    def data_put(self, data, args, _kwargs, _headers):
        data_path = args[0].lstrip("/")
        data_path = self._storage.data_put(data=data, data_path=data_path)
        return {PARAM_DATA_PATH: data_path}

    def metadata_put(self, data, args, _kwargs, _headers):
        data_path = args[0].lstrip("/")
        metadata = json.loads(data.decode())
        self._storage.metadata_put(data_path, metadata=metadata)

    def data_get_or_metadata_get(self, _data, args, kwargs, _headers):
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

    def data_delete(self, _data, args, _kwargs, _headers):
        data_path = args[0].lstrip("/")
        self._storage.data_delete(data_path=data_path)

    def data_exists(self, _data, args, _kwargs, _headers) -> None:
        data_path = args[0].lstrip("/")
        if not data_path:  # only for testing if server works
            return None
        norm_data_path = self._storage.data_exists(data_path=data_path)
        if not norm_data_path:
            raise DataDoesNotExists(data_path)
        else:
            # need to add path as header, because HEAD request has no body
            _headers.append((HEADER_DATA_PATH, norm_data_path))
            # TODO: maybe always add this header


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

        if method in ["PUT", "POST", "PATCH"]:
            content_length = int(environ["CONTENT_LENGTH"])

            input = BufferedReaderMaxSizeWrapper(
                environ["wsgi.input"], max_size=content_length
            )
            bdata = input.read()
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

        response_headers = []
        if selected_handler:
            try:
                logging.debug(
                    f"SRV: {selected_handler.__name__}"
                    f"({len(bdata)}, {path_args}, {query})"
                )
                result = selected_handler(bdata, path_args, query, response_headers)
                status_code = 200
            except DatatoolsException as exc:
                if isinstance(exc, DataDoesNotExists):
                    status_code = 404
                    response_headers.append((HEADER_DATA_PATH, f"{exc}"))
                else:
                    status_code = 400
                result = {"error_msg": str(exc), "error_cls": exc.__class__.__name__}
                response_headers.append(
                    (HEADER_ERROR, f"{exc.__class__.__name__}: {exc}")
                )
            except Exception:
                logging.error(traceback.format_exc())
                status_code = 500

        # if method == "HEAD":
        # HEAD: no body
        #    result = None
        if result is None:
            result = b""
        elif isinstance(result, str):
            output_content_type = "text/plain"
            result = result.encode()
        elif not isinstance(result, bytes):
            output_content_type = "application/json"
            result = json.dumps(result, default=json_serialize).encode()

        content_length_result = len(result)
        status = "%s %s" % (status_code, HTTPStatus(status_code).phrase)

        response_headers += [
            ("Content-Type", output_content_type),
            ("Content-Length", str(content_length_result)),
        ]

        logging.debug(f"SRV RES: {status} {response_headers}")

        start_response(status, response_headers)

        return [result]


class _TestCliStorage(AbstractStorage):
    def _proc(self, args, pipe=None):
        cmd = [
            sys.executable,
            "-m",
            "datatools",
            "-d",
            self.location,
            "-l",
            "debug",
        ] + args
        proc = sp.Popen(cmd, stdout=pipe, stdin=pipe, stderr=pipe)
        logging.debug(" ".join(cmd) + f" (pid={proc.pid})")
        return proc

    def _call(self, data, args):
        proc = self._proc(args, sp.PIPE)
        out, err = proc.communicate(data)
        if proc.returncode:
            # try to get error class / message
            err_text = err.decode().splitlines()
            # last non empty line
            err_text = [x.strip() for x in err_text if x.strip()]
            err_text = err_text[-1]
            # strip
            err_text_match = re.match(".*<([^:>]+: [^>]*)>", err_text)
            if err_text_match:
                err_text = err_text_match.groups()[0]
                raise_err(err_text)
            else:
                raise Exception(err_text)

        return out

    def serve(self, port=None):
        return self._proc(["serve", "--port", str(port)])

    def _metadata_get(self, norm_data_path: str, metadata_path: str) -> object:
        args = ["metadata-get", norm_data_path]
        if metadata_path:
            args += [metadata_path]
        res = self._call(b"", args)
        res = json.loads(res.decode().strip())
        return res

    def _metadata_put(self, norm_data_path: str, metadata: dict) -> None:
        meta_key_vals = []
        for key, val in metadata.items():
            meta_key_vals.append(f"{key}={val}")
        args = ["metadata-put", norm_data_path] + meta_key_vals
        self._call(b"", args)

    def _data_put(
        self, norm_data_path: str, data: BufferedReader, exist_ok: bool
    ) -> str:
        if isinstance(data, str):
            file_path = data
            data = None
        else:
            file_path = "-"
        args = ["data-put", file_path]
        if norm_data_path:
            args += [norm_data_path]
        if exist_ok:
            args += ["--exist-ok"]

        if data:
            data = data.read()

        res = self._call(data, args)
        return res.decode().strip()

    def _data_open(self, norm_data_path: str) -> BufferedReader:
        file_path = "-"
        args = ["data-get", norm_data_path, file_path]
        res = self._call(b"", args)
        res = BytesIO(res)
        return res

    def _data_delete(self, norm_data_path: str) -> bytes:
        args = ["data-delete", norm_data_path]
        self._call(b"", args)

    def _data_exists(self, norm_data_path: str) -> str:
        args = ["data-exists", norm_data_path]
        try:
            out = self._call(b"", args)
            return out.decode().strip()
        except DataDoesNotExists:
            return None
