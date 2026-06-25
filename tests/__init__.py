"""package init"""

from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest import TestCase

from datatools.utils import get_free_port

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
    force=True,
)


def start_http_server(
    directory: str = ".", port: int | None = None, host: str = "127.0.0.1"
) -> str:
    """TODO"""
    port = port or get_free_port()
    server = ThreadingHTTPServer(
        (host, port),
        partial(SimpleHTTPRequestHandler, directory=directory),
    )
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://{host}:{port}"
    return url


class TempdirTestCase(TestCase):
    """TODO"""

    @classmethod
    def setUpClass(cls) -> None:
        """TODO"""
        super().setUpClass()
        # create temp dir
        cls._temp_dir = TemporaryDirectory()
        cls.temp_dir = Path(cls._temp_dir.name)
        # assert cls.temp_dir.exists() # noqa

    @classmethod
    def tearDownClass(cls) -> None:
        """TODO"""
        cls._temp_dir.cleanup()
        # assert not cls.temp_dir.exists() # noqa
        super().tearDownClass()
