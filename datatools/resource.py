import re
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse, urlunparse


def is_uri(value):
    return re.match(r"^[a-z7-9+-]+://", value)


class Resource:

    __slots__ = ["__scheme", "__netloc", "__path", "__query", "__fragment"]

    def __init__(self, uri):
        uri = uri.replace("\\", "/")
        if re.match(r"^[a-zA-Z]:/", uri):
            # absolute Windows path with drive
            uri = "file:///" + uri
        elif re.match(".*:", uri):
            # uri
            pass
        elif re.match("^/[^/]", uri):
            # absolute unix path
            uri = "file://" + uri
        else:
            # relative path or windows UNC
            uri = "file:" + uri

        uri = urlparse(uri, allow_fragments=True)
        print(uri)
        self.__scheme = uri.scheme
        self.__netloc = uri.netloc
        self.__path = unquote(uri.path)
        self.__query = parse_qs(uri.query)
        self.__fragment = uri.fragment

    @property
    def uri(self):
        uri = urlunparse(
            (
                self.__scheme,
                self.__netloc,
                quote(self.__path),
                None,  # params is no longer used
                urlencode(self.__query, doseq=True),
                self.__fragment,
            )
        )

        if self.__scheme == "file" and re.match("^[^/][^:]*$", self.path):
            uri = uri.replace("file:///", "file:")

        return uri

    @property
    def path(self):
        if not self.__scheme == "file":
            raise ValueError(self.__scheme)
        path = self.__path
        if re.match("^/[A-Za-z]:", path):
            path = path[1:]  # remove slash
        if self.__netloc:
            # windows unc
            path = "//" + self.__netloc + path

        return path
