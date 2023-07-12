"""

"""
import hashlib
import json
import logging
import os
import socket
import subprocess as sp
import sys
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from contextlib import ExitStack
from http import HTTPStatus
from tempfile import TemporaryDirectory
from time import sleep
from typing import Tuple
from urllib.parse import parse_qs, quote
from wsgiref.simple_server import make_server

import jsonpath_ng as jp
import requests as req

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


class NonzeroReturncode(Exception):
    pass


class Script:
    def __init__(self, script=__file__, default_args=None):
        self.script = os.path.abspath(script)
        self.default_args = default_args or []

    def __call__(self, args: list, binput: bytes = None) -> bytes:
        cmd_args = ["python", self.script] + self.default_args + list(args)
        logging.info(" ".join(cmd_args))
        proc = sp.Popen(
            cmd_args,
            stdout=sp.PIPE,
            stderr=sp.PIPE,
            stdin=sp.PIPE if binput else None,
        )
        out, _err = proc.communicate(input=binput)
        if proc.returncode:
            # TODO: error class
            raise NonzeroReturncode(_err.decode())
        return out


class AbstractMain(ABC):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    @abstractmethod
    def exists(self, source: str) -> str:
        ...

    @abstractmethod
    def load(self, source: str) -> bytes:
        ...

    @abstractmethod
    def save(
        self,
        source: str | bytes,
        dest: str = None,
        hash_method: str = "md5",
        metadata: dict = None,
    ) -> str:
        ...

    @abstractmethod
    def meta_set(self, source: str, key: str, value: object):
        ...

    @abstractmethod
    def meta_get(self, source: str, key: str = None):
        ...


class MainBase(ExitStack, AbstractMain):
    def __init__(self, location) -> None:
        self.location = location

        ExitStack.__init__(self)

    def get_path(self, dest):
        # TODO: get_path of already
        # normalized id should return the path itself
        path = dest
        path = path.replace("\\", "/")
        path = path.split("/")
        path = [quote(x) for x in path]
        path = "/".join(path)
        path = path.strip("/")
        if not path:
            raise ValueError(path)
        return path

    def load_resource(self, source) -> Tuple[bytes, str, object]:
        metadata = {}
        dest = None
        if not source or source == "-":
            data = sys.stdin.buffer.read()
        else:
            logging.info(f"read from {source}")
            with open(source, "rb") as file:
                data = file.read()
            dest = source
        # TODO: http, ...

        return data, dest, metadata

    def write_resource(self, bdata, destination=None):
        if not destination or destination == "-":
            logging.info("write to stdout")
            destination = sys.stdout.buffer
        else:
            logging.info(f"write to {destination}")
            # assert not os.path.exists(destination)
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            destination = self.enter_context(open(destination, "wb"))
        destination.write(bdata)
        destination.close()

    def save(
        self,
        source: str | bytes,
        dest: str = None,
        hash_method: str = "md5",
        metadata: dict = None,
    ) -> str:
        metadata = metadata or {}

        if isinstance(source, str):
            _bdata, _path, _metadata = self.load_resource(source)
            source = _bdata
            metadata = _metadata | metadata
            dest = dest or _path

        if not isinstance(source, bytes):
            source = source.read()

        return self._save(
            source=source, dest=dest, hash_method=hash_method, metadata=metadata
        )


class Main(MainBase):
    def exists(self, source: str) -> str:
        path = self.get_path(source)
        destination = self.location + "/" + path
        return os.path.isfile(destination)

    def load(self, source: str) -> bytes:
        source = self.location + "/" + source
        bdata, _dest, _metadata = self.load_resource(source)
        return bdata

    def _save(
        self,
        source: bytes,
        dest: str = None,
        hash_method: str = "md5",
        metadata: dict = None,
    ) -> str:
        metadata = metadata or {}

        hashsum = getattr(hashlib, hash_method)(source).hexdigest()

        if dest:
            path = self.get_path(dest)
        else:
            path = f"hash/{hash_method}/{hashsum}"

        metadata["hash"] = f"{hash_method}:{hashsum}"

        logging.info(f"saving {len(source)} bytes to {path}")

        destination = self.location + "/" + path
        self.write_resource(source, destination=destination)

        for k, v in metadata.items():
            self.meta_set(path, k, v)

        return path

    def meta_set(self, source, key, value):
        jp_expr = jp.parse(key)

        metadata_file = self.location + "/" + source + ".metadata.json"
        if os.path.isfile(metadata_file):
            with open(metadata_file, "rb") as file:
                metadata = json.load(file)
        else:
            metadata = {}

        jp_expr.update_or_create(metadata, value)

        metadatas = json.dumps(metadata, indent=4, ensure_ascii=False)
        metadatab = metadatas.encode()
        os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
        with open(metadata_file, "wb") as file:
            file.write(metadatab)

    def meta_get(self, source, key=None):
        key = key or "$"
        jp_expr = jp.parse(key)

        metadata_file = self.location + "/" + source + ".metadata.json"
        if os.path.isfile(metadata_file):
            with open(metadata_file, "rb") as file:
                metadata = json.load(file)
        else:
            metadata = {}

        match = jp_expr.find(metadata)

        if not match:
            value = None
        elif len(match) > 1:
            logging.warning("multiple matches")  # TODO
        value = match[0].value

        return value


class MainRemote(MainBase):
    def exists(self, source: str) -> str:
        url = self.location + "/data/" + source
        # FIXME: dont try to load the file, maybe a new api endpoint?
        # or a query?
        res = req.request(method="GET", url=url)
        return res.ok

    def _req(self, method, path, data=None, params=None):
        url = self.location + path
        if not (data is None or isinstance(data, bytes)):
            data = json.dumps(data).encode()
        res = req.request(method=method, url=url, data=data, params=params)
        if not res.ok:
            try:
                message = res.json()
            except Exception:
                message = ""
            raise Exception(message)
        return res.content

    def load(self, source: str) -> bytes:
        return self._req(method="GET", path="/data/" + source)

    def _save(
        self,
        source: bytes,
        dest: str = None,
        hash_method: str = "md5",
        metadata: dict = None,
    ) -> str:
        if metadata:
            raise NotImplementedError()

        if dest:
            path = "/data/" + dest
            method = "PUT"
        else:
            path = "/data"
            method = "POST"

        resp = self._req(
            method=method, path=path, data=source, params={"hashMethod": hash_method}
        )

        return json.loads(resp)["path"]

    def meta_set(self, source: str, key: str, value: object):
        self._req(
            method="PATCH", path="/metadata/" + source, data=value, params={"key": key}
        )

    def meta_get(self, source: str, key: str = None):
        res = self._req(method="GET", path="/metadata/" + source, params={"key": key})
        return json.loads(res)


def run_test(main):
    data_in = b"test"
    d_id = main.save(source=data_in)
    assert main.exists(d_id)
    data_out = main.load(d_id)
    assert data_in == data_out, (data_in, data_out)

    d_id = "test/file.txt"
    assert not main.exists(d_id)
    d_id = main.save(source=data_in, dest=d_id)
    assert main.exists(d_id)
    data_out = main.load(d_id)
    assert data_in == data_out, (data_in, data_out)

    data_in = [1, 2]
    key = "a.x.y"
    main.meta_set(source=d_id, key=key, value=data_in)
    data_out = main.meta_get(source=d_id, key=key)
    assert data_in == data_out, (data_in, data_out)

    data_out = main.meta_get(source=d_id)


class MainTestCli(AbstractMain):
    def __init__(self, location):
        self.script = Script(__file__, ["--location", location])

    @staticmethod
    def _from_string(value: str) -> object:
        """try to use json, otherwise keep string
        TODO create testcase
        """
        try:
            return json.loads(value)
        except Exception:
            return value

    @staticmethod
    def _to_string(value: object) -> str:
        """try to use json, otherwise keep string
        TODO create testcase

        """
        if isinstance(value, str):
            return value
        return json.dumps(value)

    def save(
        self,
        source: str | bytes,
        dest: str = None,
        hash_method: str = "md5",
        metadata: dict = None,
    ) -> str:
        if metadata:
            raise NotImplementedError()
        if isinstance(source, bytes):
            stdin = source
            source = "-"
        else:
            stdin = None
        args = ["--hash-method", hash_method, "save", source]
        if dest:
            args.append(dest)
        bout = self.script(args, stdin)
        return bout.decode().strip()

    def load(self, source: str) -> bytes:
        return self.script(["load", source])

    def meta_get(self, source, key=None):
        args = ["meta-get", source]
        if key:
            args.append(key)
        value = self.script(args).decode().strip()
        value = self._from_string(value)
        return value

    def meta_set(self, source: str, key: str, value: object):
        value = self._to_string(value)
        self.script(["meta-set", source, key, value])

    def exists(self, source: str) -> str:
        try:
            self.script(["exists", source])
        except NonzeroReturncode:
            return False
        return True


class MainRemoteTestServer:
    """run testserver in subprocess"""

    host = "localhost"  # "0.0.0.0" sometimes causes problems in Windows

    def __init__(self, location, script=__file__) -> None:
        self.proc = None
        self.port = self.get_free_port()
        self.url = f"http://{self.host}:{self.port}"
        self.cmd_args = [
            "python",
            script,
            "serve",
            "--port",
            str(self.port),
            "--location",
            location,
        ]

    def __enter__(self):
        self.proc = sp.Popen(self.cmd_args)
        self.wait_for_port(self.port)
        return self

    def __exit__(self, *args):
        self.proc.kill()

    @classmethod
    def get_free_port(cls):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((cls.host, 0))
        return sock.getsockname()[1]

    def wait_for_port(self, timeout_s=30):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        time_wait_total_s = 0
        time_wait_step_s = 0.1
        while True:
            try:
                sock.connect((self.host, self.port))
                break
            except Exception:
                sleep(time_wait_step_s)
                time_wait_total_s += time_wait_step_s
                if timeout_s is not None and time_wait_total_s >= timeout_s:
                    raise

        sock.close()


class MainServer:
    def __init__(self, main, port=80):
        self.main = main
        self.port = port

    def serve(self) -> None:
        with make_server("localhost", self.port, self.application) as server:
            logging.info(server.server_address)
            server.serve_forever()

    def application(environ, start_response):
        method = environ["REQUEST_METHOD"].upper()
        path = environ["PATH_INFO"]
        query = parse_qs(environ["QUERY_STRING"], strict_parsing=False)
        content_length = int(environ["CONTENT_LENGTH"] or "0")

        if method == "POST" and path == "/data":
            input = environ["wsgi.input"]
            # TODO: we would want to just pass
            # input to main.save, but we need to specify the number of bytes
            # in advance
            bdata = input.read(content_length)
            path = main.save(source=bdata)

            result = {"path": path}
            result = json.dumps(result).encode()
            status_code = 200
        elif method == "PUT" and path.startswith("/data/"):
            lp = len("/data/")
            path = path[lp:]  # remove left "/data/"
            input = environ["wsgi.input"]
            # TODO: we would want to just pass
            # input to main.save, but we need to specify the number of bytes
            # in advance
            bdata = input.read(content_length)
            path = main.save(source=bdata, dest=path)

            result = {"path": path}
            result = json.dumps(result).encode()
            status_code = 200

        elif method == "GET" and path.startswith("/data/"):
            lp = len("/data/")
            path = path[lp:]  # remove left "/data/":
            result = main.load(source=path)

            status_code = 200
        elif method == "PATCH" and path.startswith("/metadata/"):
            lp = len("/metadata/")
            path = path[lp:]  # remove left "/data/"

            key = query["key"][0]  # TODO validate for multiple

            input = environ["wsgi.input"]
            # TODO: we would want to just pass
            # input to main.save, but we need to specify the number of bytes
            # in advance
            bdata = input.read(content_length)
            value = json.loads(bdata.decode())  # todo use encoding encoding

            main.meta_set(source=path, key=key, value=value)

            result = b""
            status_code = 200
        elif method == "GET" and path.startswith("/metadata/"):
            lp = len("/metadata/")
            path = path[lp:]  # remove left "/data/"
            key = query.get("key", [None])[0]

            res = main.meta_get(source=path, key=key)

            result = json.dumps(res, indent=4, ensure_ascii=False).encode()
            status_code = 200
        else:
            status_code = 400
            result = {"error": f"{method} {path}"}
            result = json.dumps(result).encode()

        content_length_result = len(result)

        # TODO get other success codes
        status = "%s %s" % (status_code, HTTPStatus(status_code).phrase)

        output_content_type = ""

        response_headers = []
        response_headers += [
            ("Content-type", output_content_type),
            ("Content-Length", str(content_length_result)),
        ]

        start_response(status, response_headers)

        return [result]


if __name__ == "__main__":
    ap = ArgumentParser()
    ap.add_argument(
        "cmd",
        choices=["test", "serve", "exists", "save", "load", "meta-set", "meta-get"],
    )
    ap.add_argument("--location", default=".data")
    ap.add_argument("--port", type=int, default=80)
    ap.add_argument("--hash-method", choices=["md5", "sha256"], default="md5")
    ap.add_argument("source", nargs="?")
    ap.add_argument("destination", nargs="?")
    ap.add_argument("value", nargs="?")

    kwargs = vars(ap.parse_args())

    if kwargs["cmd"] == "test":
        # local test
        with TemporaryDirectory() as location:
            with Main(location=location) as main:
                run_test(main)
        # CLI
        with TemporaryDirectory() as location:
            with MainTestCli(location=location) as main:
                run_test(main)

        # remote test
        with TemporaryDirectory() as location, MainRemoteTestServer(location) as srv:
            # remote test
            with MainRemote(location=srv.url) as main:
                run_test(main)

    elif kwargs["cmd"] == "serve":
        with Main(location=kwargs["location"]) as main:
            MainServer(main=main, port=kwargs["port"]).serve()

    else:
        with Main(location=kwargs["location"]) as main:

            def cli(cmd, kwargs):
                if cmd == "save":
                    path = main.save(
                        source=kwargs["source"],
                        dest=kwargs["destination"],
                        hash_method=kwargs["hash_method"],
                    )
                    print(path)
                elif cmd == "load":
                    bdata = main.load(source=kwargs["source"])
                    main.write_resource(bdata, destination=kwargs["destination"])
                elif cmd == "meta-get":
                    res = main.meta_get(
                        source=kwargs["source"], key=kwargs["destination"]
                    )
                    res = json.dumps(res, indent=4, ensure_ascii=False)
                    print(res)
                elif cmd == "meta-set":
                    try:
                        value = json.loads(kwargs["value"])
                    except Exception:
                        value = kwargs["value"]
                    main.meta_set(
                        source=kwargs["source"],
                        key=kwargs["destination"],
                        value=value,
                    )
                elif cmd == "exists":
                    res = main.exists(source=kwargs["source"])
                    print(res)
                    if not res:
                        sys.exit(1)
                else:
                    raise NotImplementedError(cmd)

            cli(kwargs["cmd"], kwargs)
