import json
import logging
import os
from tempfile import TemporaryDirectory

from datatools.utils import make_file_writable

b_hello_world = b"hello world"
md5_hello_world = "5eb63bbbe01eeed093cb22bb8f5acdc3"
b_hello = b"hello"
md5_hello = "5d41402abc4b2a76b9719d911017c592"


def objects_euqal(left, right):
    left = json.dumps(left, sort_keys=True)
    right = json.dumps(right, sort_keys=True)
    return left == right


logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


class MyTemporaryDirectory(TemporaryDirectory):
    """before cleanup: make all files writable to delete them"""

    def __enter__(self):
        self.name = super().__enter__()
        return self.name

    def __exit__(self, *args):
        # make files writable so cleanup can delete them
        logging.warning(f"__exit__ {self.name}")
        for rt, _ds, fs in os.walk(self.name):
            for f in fs:
                filepath = f"{rt}/{f}"
                make_file_writable(filepath)

        # super exit: will delete
        super().__exit__(*args)
