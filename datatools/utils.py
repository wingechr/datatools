"""TODO"""

from collections.abc import Callable
import json
import logging
import os
from pathlib import Path
import re
import socket
import sys
from typing import Any, Literal
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname


class TextFile:
    """TODO"""

    def __init__(
        self,
        path: str | Path,
        encoding="utf-8",
        errors: Literal["strict", "replace", "ignore"] = "strict",
        ensure_ascii=False,
        sort_keys=False,
        indent=2,
    ):
        self.path = Path(path)
        self.encoding = encoding
        self.errors = errors
        self.ensure_ascii = ensure_ascii
        self.sort_keys = sort_keys
        self.indent = indent

    def exists(self) -> bool:
        """TODO"""
        return self.path.exists()

    def load_bytes(self) -> bytes:
        """TODO"""
        logging.debug("Reading %s", self.path)
        with self.path.open("rb") as file:
            return file.read()

    def load_str(self) -> str:
        """TODO"""
        data_b = self.load_bytes()
        data_s = data_b.decode(encoding=self.encoding, errors=self.errors)
        return data_s

    def load_json(self) -> Any:
        """TODO"""
        data_s = self.load_str()
        return json.loads(data_s)

    def dump_bytes(self, data: bytes) -> None:
        """TODO"""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        logging.debug("Writing %s", self.path)
        with self.path.open("wb") as file:
            file.write(data)

    def dump_str(self, data: str) -> None:
        """TODO"""
        data_b = data.encode(encoding=self.encoding, errors=self.errors)
        self.dump_bytes(data_b)

    def dump_json(self, data: Any) -> None:
        """TODO"""
        data_s = json.dumps(
            data,
            ensure_ascii=self.ensure_ascii,
            sort_keys=self.sort_keys,
            indent=self.indent,
        )
        self.dump_str(data_s)


def find_subclass(base_cls, name: str):
    """TODO"""
    for cls in base_cls.__subclasses__():
        if cls.__name__ == name:
            return cls
        found = find_subclass(cls, name)
        if found:
            return found
    return None


def wrap_exception(function: Callable[[], None], debug: bool = True):
    """TODO"""
    try:
        # your logic here
        function()
    except Exception as e:
        if debug:
            logging.exception(e)  # includes stack trace
        else:
            logging.error(e)
        sys.exit(1)


def parse_cmd_vals(arguments: list[str]) -> dict[str, str]:
    """TODO"""
    items = [kv.split("=", 1) for kv in arguments]
    return {k: try_parse_json_str(v) for k, v in items}


def get_free_port() -> int:
    """TODO"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))  # 0 = let the OS choose
        port = s.getsockname()[1]
    return port


def file_uri_to_path(uri: str) -> Path:
    """TODO"""
    parts = urlparse(uri)
    if parts.netloc:
        raise NotImplementedError(uri)
    elif parts.path.startswith("/./") or parts.path.startswith("/../"):
        # relative path (not standard file:// schema)
        path_s = Path(os.getcwd()).as_posix() + parts.path
    else:
        path_s = url2pathname(unquote(parts.path))
    path = Path(path_s)

    return path


def reverse_prints(stdout_data: bytes) -> list[str]:
    """TODO"""
    text = stdout_data.decode(sys.stdout.encoding, errors="replace")
    lines = text.splitlines(keepends=False)[::-1]
    return lines


def try_parse_json_str(s: str) -> Any:
    """TODO"""
    try:
        return json.loads(s)
    except Exception:
        return s


def is_file_uri_or_path(x: str | Path) -> bool:
    """TODO"""
    if isinstance(x, Path):
        return True
    return bool(re.match(r"file://", x)) or "://" not in x


def uri_or_path_to_path(x: str | Path) -> Path:
    """TODO"""
    if isinstance(x, Path):
        return x
    elif re.match(r"file://", x):
        return file_uri_to_path(x)
    else:
        return Path(x)
