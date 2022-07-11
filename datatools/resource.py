import os
import re
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse, urlunparse
from urllib.request import url2pathname


def is_uri(value):
    return re.match("^[a-z7-9+-]+://", value)


class Resource:

    __slots__ = [
        "__scheme",
        "__netloc",
        "__path",
        # "__params",
        "__query",
        # "__fragment"
    ]

    def __init__(self, uri):
        if not is_uri(uri):
            # Local or windows UNC
            uri = Path(os.path.abspath(uri)).as_uri()
        uri = urlparse(uri)
        self.__scheme = uri.scheme
        self.__netloc = uri.netloc
        self.__path = unquote(uri.path).replace("\\", "/")
        self.__query = parse_qs(uri.query)
        # self.__params = uri.params
        # self.__fragment = uri.fragment

    @property
    def uri(self):
        return urlunparse(
            (
                self.__scheme,
                self.__netloc,
                quote(self.__path),
                None,  # params
                urlencode(self.__query, doseq=True),
                None,  # fragment
            )
        )

    @property
    def path(self):
        if not self.__scheme == "file":
            raise ValueError(self.__scheme)
        path = os.path.abspath(
            os.path.join("//%s/" % self.__netloc, url2pathname(self.__path))
        )
        path = path.replace("\\", "/")
        return path
