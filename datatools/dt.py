"""

# save binary data
## REST

POST /?hashmethod=path=,,, < BYTES > /uri

## CLI
save --hash-method=md5 local_uri > /uri

## PY
save(bdata:bytes|local_uri, path:str=None, hash_method:str="md5") -> str


# load  binary data
## REST
GET /uri > BYTES

## CLI
load /uri > BYTES

## PY
load(uri:str) -> bytes

# load meta data
## REST
GET meta/uri?key=key > JSON
or
GET /uri?metadata=key > JSON

## CLI
meta load /uri [/key] -> string[json]
or
load /uri --metadata=key > string[json]

## PY
meta_load(uri:str, key:str=None) -> object[json]


# update meta data
## REST
PATCH /uri?key=key < JSON
chator
PATCH /uri?metadata=key < JSON

## CLI
meta save /uri /key string[json]

## PY
meta_save(uri:str, key:str, value:object[json])


"""
import argparse
import hashlib

# import os
import subprocess as sp
import sys
from contextlib import ExitStack
from io import BytesIO

# from threading import Thread
from typing import Tuple
from urllib.parse import quote
from wsgiref.simple_server import make_server

# import requests


class Main(ExitStack):
    def __init__(self, location=".data") -> None:
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
            data = sys.stdin.buffer
        else:
            data = self.enter_context(open(source, "rb"))
            dest = source
        # TODO: http, ...
        return data, dest, metadata

    def write_resource(self, bdata, destination=None):
        if not destination or destination == "-":
            destination = sys.stdout.buffer
        else:
            destination = self.enter_context(open(destination, "wb"))
        destination.write(bdata)
        destination.close()

    def serve(self, port: int, application) -> None:
        server = self.enter_context(make_server("", port, application))
        server.serve_forever()

    def check(self, source: str) -> str:
        pass

    def load(self, source: str) -> bytes:
        bdata = b"todo"
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
        hashsum = getattr(hashlib, hash_method)(bdata).hexdigest()

        if not dest:
            path = f"hash/{hash_method}/{hashsum}"
        else:
            path = self.get_path(dest)
        metadata["hash"] = f"{hash_method}:{hashsum}"

        print(f"saving {len(bdata)} bytes to {path}")
        for k, v in metadata.items():
            self.meta_save(path, k, v)

        return path

    def meta_save(self, path, key, value):
        print(f"saving metadata {path}: {key} = {value}")

    def meta_load(self, path, key, value):
        value = None
        print(f"loading metadata {path}: {key} = {value}")
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
    ap.add_argument("key", nargs="?")
    ap.add_argument("value", nargs="?")

    kwargs = vars(ap.parse_args())

    with Main(location=kwargs["location"]) as main:
        cmd = kwargs.pop("cmd")
        if cmd == "test":
            cmd_args = ["python", __file__, "serve", "--port", str(kwargs["port"])]
            print(cmd_args)
            proc = sp.Popen(cmd_args)

        elif cmd == "serve":

            def application(environ, start_response):
                cmd = environ["PATH"]
                print(cmd)

            main.serve(port=kwargs["port"], application=application)

        else:

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
                elif cmd == "meta-load":
                    pass
                elif cmd == "meta-save":
                    pass
                elif cmd == "check":
                    pass
                else:
                    raise NotImplementedError(cmd)

            cli(cmd, kwargs)
