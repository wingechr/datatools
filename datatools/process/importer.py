"""Abstract classes / interfaces, types"""

from abc import ABC
from collections.abc import Callable, Iterable
import re

from typing_extensions import override

from datatools.process.task import AnnotatedFunction
from datatools.types import FunToWritableBuffer, Name, WritableBuffer
from datatools.utils import (
    get_name_from_uri,
    http_get_stream,
    is_file_uri_or_path,
    query_sql,
    read_file_uri_stream,
    remove_credentials_from_netloc,
    sql_query_result_to_csv,
    subclasses_by_name,
    uri_or_path_to_path,
)


class Importer(ABC):
    """TODO"""

    output_write_byte_data: FunToWritableBuffer | None = None
    get_data: Callable

    @classmethod
    def can_handle(cls, uri: str, **options) -> bool:
        """Can class handle uri"""
        return False

    @classmethod
    def get_output_name(cls, uri: str, **options) -> str:
        """TODO"""
        return get_name_from_uri(uri)


def infer_importer_class(uri: str, **options) -> type[Importer]:
    """TODO"""
    for cls in subclasses_by_name(Importer).values():
        if cls.can_handle(uri, **options):
            return cls
    raise NotImplementedError(f"Cannot infer Importer class for {uri}")


def write_chunks(data: Iterable[bytes], buf: WritableBuffer):
    """TODO"""
    for chunk in data:
        buf.write(chunk)


class HttpImporter(Importer):
    """TODO"""

    # use generic id (tool does not matter)
    get_data = AnnotatedFunction.wrap(function_id="GET")(http_get_stream)
    output_write_byte_data: FunToWritableBuffer = write_chunks

    @classmethod
    @override
    def can_handle(cls, uri: str, **options) -> bool:
        return bool(re.match(r"^https?://", uri))

    @classmethod
    def get_output_name(cls, uri: str, **options) -> str:
        """TODO"""
        return get_name_from_uri(remove_credentials_from_netloc(uri))


class FileImporter(Importer):
    """TODO"""

    # use generic id (tool does not matter)
    get_data = AnnotatedFunction.wrap(function_id="COPY")(read_file_uri_stream)
    output_write_byte_data: FunToWritableBuffer = write_chunks

    @classmethod
    def can_handle(cls, uri: str, **options) -> bool:
        """Either file:// protocol or no protocol"""
        return is_file_uri_or_path(uri)

    @classmethod
    def get_output_name(cls, uri: str, **options) -> Name:
        """TODO"""

        path = uri_or_path_to_path(uri).resolve()
        # FIXME:
        # we cant/dont want to use full path as Name
        # but we may want to use more than just the file name
        # but there is no good way to automatically determine what part
        # of the path to keep.
        # Maybe later, we can use a prefix option or something like that

        name = path.name
        return name


class SqlImporter(Importer):
    """TODO"""

    # use generic id (tool does not matter)
    get_data = AnnotatedFunction.wrap(function_id="QUERY")(query_sql)
    output_write_byte_data: FunToWritableBuffer = sql_query_result_to_csv

    @classmethod
    @override
    def can_handle(cls, uri: str, **options) -> bool:
        return bool(re.match(r"^[^/]*sql[^/]*://", uri))
