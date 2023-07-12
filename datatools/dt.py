"""

"""
import argparse
import hashlib
import json
import logging
import os
import re
import socket

# import os
import subprocess as sp
import sys
from contextlib import ExitStack
from http import HTTPStatus
from io import BytesIO
from tempfile import TemporaryDirectory
from typing import Tuple
from urllib.parse import parse_qs, quote
from wsgiref.simple_server import make_server

import jsonpath_ng as jp
import requests

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


def get_free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


class Main(ExitStack):
    def __init__(self, location=".data") -> None:
        # decide if remote or local
        self.is_remote = bool(re.match("http[s]?://", location, re.IGNORECASE))
        self.location = location

        ExitStack.__init__(self)

    def get_path(self, dest):
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

    def serve(self, port: int, application) -> None:
        server = make_server("", port, application)
        logging.info(server.server_address)
        server = self.enter_context(server)
        server.serve_forever()

    def check(self, source: str) -> str:
        pass

    def load(self, source: str) -> bytes:
        if self.is_remote:
            url = self.location + "/data/" + source
            res = requests.request(method="GET", url=url)
            res.raise_for_status()
            bdata = res.content

        else:
            source = self.location + "/" + source
            bdata, _dest, _metadata = self.load_resource(source)

        return bdata

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
            _metadata.update(metadata)
            _path = dest or _path
            return self.save(
                source=_bdata,
                dest=_path,
                hash_method=hash_method,
                metadata=_metadata,
            )
        if isinstance(source, bytes):
            source = BytesIO(source)

        # assert now that data is bytes buffer / file
        bdata = source.read()

        if self.is_remote:
            if dest:
                url = self.location + "/data/" + dest
                res = requests.request(method="PUT", url=url, data=bdata)
            else:
                url = self.location + "/data"
                res = requests.request(method="POST", url=url, data=bdata)

            if not res.ok:
                raise Exception(res.json())

            res = res.json()
            path = res["path"]

        else:
            hashsum = getattr(hashlib, hash_method)(bdata).hexdigest()

            if dest:
                path = self.get_path(dest)
            else:
                path = f"hash/{hash_method}/{hashsum}"

            metadata["hash"] = f"{hash_method}:{hashsum}"

            logging.info(f"saving {len(bdata)} bytes to {path}")

            destination = self.location + "/" + path
            self.write_resource(bdata, destination=destination)

            for k, v in metadata.items():
                self.meta_save(path, k, v)

        return path

    def meta_save(self, source, key, value):
        logging.info(f"saving metadata {source}: {key} = {value}")
        if self.is_remote:
            url = self.location + "/metadata/" + source
            res = requests.request(
                method="PATCH", url=url, json=value, params={"key": key}
            )
            res.raise_for_status()
        else:
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

    def meta_load(self, source, key=None):
        if not key:
            key = "$"  # root
        logging.info(f"loading metadata {source}: {key}")

        if self.is_remote:
            url = self.location + "/metadata/" + source
            res = requests.request(method="GET", url=url, params={"key": key})
            res.raise_for_status()

            value = res.json()
        else:
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


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "cmd",
        choices=["test", "serve", "check", "save", "load", "meta-set", "meta-get"],
    )
    ap.add_argument("--location", default=".data")
    ap.add_argument("--port", type=int, default=8888)
    ap.add_argument("--hash-method", choices=["md5", "sha256"], default="md5")
    ap.add_argument("source", nargs="?")
    ap.add_argument("destination", nargs="?")
    # ap.add_argument("key", nargs="?")
    ap.add_argument("value", nargs="?")

    kwargs = vars(ap.parse_args())
    if kwargs["cmd"] == "test":
        port = get_free_port()

        # local test
        with TemporaryDirectory() as location:
            with Main(location=location) as main:
                data_in = b"test"
                d_id = main.save(source=data_in)
                logging.info(d_id)
                data_out = main.load(d_id)
                logging.info(data_out)
                assert data_in == data_out

                d_id = "test/file.txt"
                d_id = main.save(source=data_in, dest=d_id)
                data_out = main.load(d_id)
                logging.info(data_out)
                assert data_in == data_out

                data_in = [1, 2]
                key = "a.x.y"
                main.meta_save(source=d_id, key=key, value=data_in)
                data_out = main.meta_load(source=d_id, key=key)
                logging.info(data_out)
                assert data_in == data_out

                data_out = main.meta_load(source=d_id)
                logging.info(data_out)
        # CLI
        with TemporaryDirectory() as location:

            def run_cmd(args, binput=None):
                cmd_args = ["python", __file__, "--location", location] + list(args)
                proc = sp.Popen(
                    cmd_args,
                    stdout=sp.PIPE,
                    stderr=sp.PIPE,
                    stdin=sp.PIPE if binput else None,
                )
                out, _err = proc.communicate(input=binput)
                if proc.returncode:
                    raise Exception(_err.decode())
                return out

            data_in = b"test"
            # remove newline from print
            d_id = run_cmd(["save", "-"], data_in).decode().rstrip()
            logging.info(d_id)

            data_out = run_cmd(["load", d_id, "-"])
            logging.info(data_out)
            assert data_in == data_out

            d_id = "test/file.txt"
            # remove newline from print
            d_id = run_cmd(["save", "-", d_id], data_in).decode().rstrip()
            data_out = run_cmd(["load", d_id, "-"])
            logging.info(data_out)
            assert data_in == data_out

            data_in = [1, 2]
            key = "a.x.y"
            run_cmd(
                [
                    "meta-set",
                    d_id,
                    key,
                    json.dumps(data_in),
                ]
            )

            data_out = run_cmd(["meta-get", d_id, key])  # .decode().rstrip()
            logging.info(data_out)
            data_out = json.loads(data_out)
            logging.info(data_out)
            assert data_in == data_out

            data_out = run_cmd(["meta-get", d_id])  # .decode().rstrip()
            logging.info(data_out)

        # remote test
        with TemporaryDirectory() as location:
            # start server
            cmd_args = [
                "python",
                __file__,
                "serve",
                "--port",
                str(port),
                "--location",
                location,
            ]
            logging.info(cmd_args)
            proc = sp.Popen(cmd_args)

            # import time
            # time.sleep(2)

            # remote test
            with Main(location=f"http://localhost:{port}") as main:
                data_in = b"test"
                d_id = main.save(source=data_in)
                logging.info(d_id)
                data_out = main.load(d_id)
                logging.info(data_out)
                assert data_in == data_out

                d_id = "test/file.txt"
                d_id = main.save(source=data_in, dest=d_id)

                data_out = main.load(d_id)
                logging.info(data_out)
                assert data_in == data_out

                data_in = [1, 2]
                key = "a.x.y"
                main.meta_save(source=d_id, key=key, value=data_in)
                data_out = main.meta_load(source=d_id, key=key)
                logging.info(data_out)
                assert data_in == data_out

                data_out = main.meta_load(source=d_id)
                logging.info(data_out)

            proc.kill()

    elif kwargs["cmd"] == "serve":
        with Main(location=kwargs["location"]) as main:

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

                    main.meta_save(source=path, key=key, value=value)

                    result = b""
                    status_code = 200
                elif method == "GET" and path.startswith("/metadata/"):
                    lp = len("/metadata/")
                    path = path[lp:]  # remove left "/data/"
                    key = query.get("key", [None])[0]

                    res = main.meta_load(source=path, key=key)

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

                # response_messages = [{"level": "DEBUG", "data": "test"}]

                response_headers = []
                response_headers += [
                    ("Content-type", output_content_type),
                    ("Content-Length", str(content_length_result)),
                ]

                # response_headers.append(
                #    ("Messages", json.dumps(response_messages, ensure_ascii=True))
                # )

                start_response(status, response_headers)

                # logging.warning(status)
                # logging.warning(response_headers)
                # logging.warning(result)
                return [result]

            main.serve(port=kwargs["port"], application=application)
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
                    res = main.meta_load(
                        source=kwargs["source"], key=kwargs["destination"]
                    )
                    res = json.dumps(res, indent=4, ensure_ascii=False)
                    print(res)
                elif cmd == "meta-set":
                    try:
                        value = json.loads(kwargs["value"])
                    except Exception:
                        value = kwargs["value"]
                    main.meta_save(
                        source=kwargs["source"],
                        key=kwargs["destination"],
                        value=value,
                    )
                elif cmd == "check":
                    res = main.check(source=kwargs["source"])
                    logging.info(res)
                else:
                    raise NotImplementedError(cmd)

            cli(kwargs["cmd"], kwargs)
