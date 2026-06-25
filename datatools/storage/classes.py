"""TODO"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
import datetime
import functools
import logging
import os
from pathlib import Path
import pickle
import re
import subprocess as sp
import sys
from typing import Any, Literal

import httpx
import rdflib
from sqlalchemy import (
    VARBINARY,
    VARCHAR,
    Column,
    Engine,
    MetaData,
    Table,
    create_engine,
)

from datatools.importer import infer_importer_class
from datatools.job.classes import (
    FunctionWrapper,
    make_job,
)
from datatools.types import (
    UID,
    ByteData,
    FunParams,
    FunResult,
    Json,
    MetadataAttribute,
    MetadataValue,
    StorageFileExistsError,
    StorageFileNotFoundError,
    StorageInvalidUidError,
    SubprocessStatus,
)
from datatools.utils import (
    TextFile,
    get_md5_hash,
    is_file_uri_or_path,
    reverse_prints,
    try_parse_json_str,
    uri_or_path_to_path,
)

metadata = MetaData()

table_data = Table(
    "data",
    metadata,
    Column("uid", VARCHAR, primary_key=True),
    Column("data", VARBINARY),
)
table_metadata = Table(
    "metadata",
    metadata,
    Column("uid", VARCHAR),
    Column("attribute", VARCHAR),
    Column("value", VARCHAR),
)


class MetadataStorage(ABC):  # TODO: subclass AbstractContextManager ?
    """Abstract metadata storage."""

    @abstractmethod
    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]: ...

    @abstractmethod
    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None: ...

    def _match(self, **filters: MetadataValue) -> bool:
        return all(value in self[attribute] for attribute, value in filters.items())

    def __getitem__(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        return self._getitem(attribute=attribute)

    def __setitem__(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        return self._setitem(attribute=attribute, value=value)


class DataStorage(ABC):
    """Abstract data storage."""

    def __init__(self, location: Any = None):
        self._location = location

    @abstractmethod
    def _contains(self, uid: UID) -> bool: ...

    @abstractmethod
    def _getitem(self, uid: UID) -> ByteData: ...

    @abstractmethod
    def _setitem(self, uid: UID, data: ByteData) -> None: ...

    @abstractmethod
    def _delitem(self, uid: UID) -> None: ...

    @abstractmethod
    def _metadata(self, uid: UID) -> MetadataStorage: ...

    @abstractmethod
    def _list(self, **filters: MetadataValue) -> Iterable[UID]: ...

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        return False

    def _get_valid_uid(self, uid: UID) -> UID:
        return UID(uid)

    def __iter__(self) -> Iterator[UID]:
        return iter(self._list())

    def __contains__(self, uid: UID) -> bool:
        self._assert_valid_uid(uid=uid)
        return self._contains(uid=uid)

    def __getitem__(self, uid: UID) -> ByteData:
        self._assert_valid_uid(uid=uid)
        if uid not in self:
            raise StorageFileNotFoundError(f"Not found: {uid}")
        return self._getitem(uid=uid)

    def __setitem__(self, uid: UID, data: ByteData) -> None:
        self._assert_valid_uid(uid=uid)
        if uid in self:
            raise StorageFileExistsError(f"Already exists: {uid}")
        return self._setitem(uid=uid, data=data)

    def __delitem__(self, uid: UID) -> None:
        self._assert_valid_uid(uid=uid)
        if uid not in self:
            raise StorageFileNotFoundError(f"Not found: {uid}")
        return self._delitem(uid=uid)

    def metadata(self, uid: UID) -> MetadataStorage:
        """Metadata container associated with data."""
        self._assert_valid_uid(uid=uid)
        return self._metadata(uid=uid)

    def find(self, **filters: MetadataValue) -> Iterable[UID]:
        """list UIDs for given metadata query."""
        return self._list(**filters)

    def _assert_valid_uid(self, uid: UID):
        valid_uid = self._get_valid_uid(uid)
        if uid != valid_uid:
            raise StorageInvalidUidError(
                f"Invalid uid: {uid} => {valid_uid}", uid=valid_uid
            )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self._location})"

    def info(self) -> dict:
        """TODO"""
        return {"Location": str(self._location), "Class": str(self.__class__.__name__)}

    def import_from_uri(self, uri: str, **options) -> UID:
        """TODO"""
        importer_class = infer_importer_class(uri, **options)
        importer = importer_class(data_storage=self, uri=uri, **options)
        return importer()

    def cache(
        self,
        output_to_bytes: Callable[[FunResult], bytes] | None = None,
        output_from_bytes: Callable[[bytes], FunResult] | None = None,
        get_hash_data: Callable[[str, dict], Json] | None = None,
        get_hash: Callable[[Json], str] | None = None,
    ) -> Callable:
        """TODO"""

        def default_get_hash_data(function_id: str, parameter: dict) -> Json:
            return {
                "function": function_id,
                "parameter": parameter,
            }  # type:ignore

        _output_to_bytes: Callable[[FunResult], bytes] = output_to_bytes or pickle.dumps
        _output_from_bytes: Callable[[bytes], FunResult] = (
            output_from_bytes or pickle.loads
        )
        _get_hash_data: Callable[[str, dict], Json] = (
            get_hash_data or default_get_hash_data
        )
        _get_hash: Callable[[Json], str] = get_hash or get_md5_hash

        def decorator(fun: Callable[FunParams, FunResult]) -> Callable:
            """TODO"""

            wrapped_fun = FunctionWrapper.assert_wrapped(fun)
            function_id = wrapped_fun.get_function_id()

            @functools.wraps(fun)
            def _fun(*args, **kwargs):
                param_values = wrapped_fun.get_argument_dict(*args, **kwargs)
                hash_data = _get_hash_data(function_id, param_values)
                output_uid = _get_hash(hash_data)
                if output_uid not in self:
                    # call base function
                    # NOTE: we only support namedarguments
                    result = fun(*args, **kwargs)
                    self[output_uid] = _output_to_bytes(result)

                # retrieval
                data = self[output_uid]
                result = _output_from_bytes(data)
                return result

            return _fun

        return decorator

    def job(
        self,
        function: Callable,
        input_converters: dict[str, Callable[[FunResult], bytes]],
        output_converters: dict[str, Callable[[bytes], FunResult]],
    ):
        """TODO"""

        # wrap handler
        metadata = {}

        def wrap_input_handle(handle):
            def handle_(uid: UID):
                bdata = self[uid]
                return handle(bdata)

            return handle_

        def wrap_output_handle(handle):
            def handle_(data: Any, uid: UID):
                bdata = handle(data)
                self[uid] = bdata
                for k, v in metadata.items():
                    self.metadata(uid)[k] = v

            return handle_

        job = make_job(
            function,
            input_readers={
                name: wrap_input_handle(conv) for name, conv in input_converters.items()
            },
            output_writers={
                name: wrap_output_handle(conv)
                for name, conv in output_converters.items()
            },
        )

        @functools.wraps(job)
        def _job(*args, **kwargs):
            # update metadata before running job
            # so output handlers can use it
            metadata["job_timestamp"] = datetime.datetime.now().isoformat()

            job(*args, **kwargs)

        return _job


class MemoryMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, data: dict | None = None):
        self._data = {} if data is None else data

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        value = self._data.get(attribute)
        if value is None:
            return []
        else:
            return [value]

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._data[attribute] = value


class JsonLdFileMetadataStorage(MetadataStorage):
    """FIXME

    - we load data on init and save on __del__, which is uper unsafe.
    - but we also dont want to load file every time?
    """

    def __init__(self, path: Path, uid: UID):
        self._file = TextFile(path)
        self._changed = False
        self._graph = self._load_graph()
        self._uid = uid

    def _load_graph(self) -> rdflib.Dataset:
        graph = rdflib.Dataset()
        if self._file.exists():
            graph.parse(self._file.path, format="json-ld")
        return graph

    def __del__(self):
        if self._changed:
            data_b = self._graph.serialize(
                format="json-ld",
                indent=2,
                # auto_compact=True,
                expand=True,
                sort_keys=True,
                encoding="utf-8",
            )

            self._file.dump_bytes(data_b)

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        subj = self._as_uri(self._uid)
        pred = self._as_uri(attribute)
        for obj in self._graph.objects(subj, pred):
            # TODO: smarter way convert result?
            yield try_parse_json_str(str(obj))

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        subj = self._as_uri(self._uid)
        pred = self._as_uri(attribute)
        obj = self._as_uri_or_literal(value)
        triple = (subj, pred, obj)
        self._graph.add(triple)
        self._changed = True

    def _as_uri(self, x: str) -> rdflib.URIRef:
        """FIXME"""
        return rdflib.URIRef("urn:" + x)

    def _as_uri_or_literal(self, x: MetadataValue) -> rdflib.URIRef | rdflib.Literal:
        """FIXME"""
        return rdflib.Literal(x)


class JsonFileMetadataStorage(MetadataStorage):
    """FIXME

    - we load data on init and save on __del__, which is uper unsafe.
    - but we also dont want to load file every time?
    """

    def __init__(self, path: Path):
        self._file = TextFile(path)
        self._storage = MemoryMetadataStorage(self._load_data())
        self._changed = False

    def _load_data(self) -> dict:
        if not self._file.exists():
            data = {}
        else:
            data = self._file.load_json()
            if not isinstance(data, dict):
                raise ValueError("json file for metadata must be dict")
        return data

    def __del__(self):
        if self._changed:
            self._file.dump_json(self._storage._data)

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        return self._storage._getitem(attribute)

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._changed = True
        self._storage._setitem(attribute, value)


class MemoryDataStorage(DataStorage):
    """TODO"""

    def __init__(self):
        super().__init__(location=":memory")
        self.__data: dict[UID, Any] = {}
        self.__metadata: dict[UID, MemoryMetadataStorage] = {}

    def _contains(self, uid: UID) -> bool:
        return uid in self.__data

    def _getitem(self, uid: UID) -> Any:
        return self.__data[uid]

    def _setitem(self, uid: UID, data: Any) -> None:
        self.__data[uid] = data

    def _delitem(self, uid: UID) -> None:
        del self.__data[uid]
        # dont delete metadata

    def _list(self, **filters: MetadataValue) -> Iterable[UID]:
        for uid in self.__data:
            if filters:
                metadata = self._metadata(uid)
                if not metadata._match(**filters):
                    continue

            yield uid

    def _metadata(self, uid: UID) -> MemoryMetadataStorage:
        if uid not in self.__metadata:
            self.__metadata[uid] = MemoryMetadataStorage()
        return self.__metadata[uid]


class FileDataStorage(DataStorage):
    """TODO"""

    metadata_sufix = ".metadata.json"

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        """Either file:// protocol or no protocol"""
        return is_file_uri_or_path(location)

    def __init__(self, location: str = "."):
        path = uri_or_path_to_path(location).resolve()
        self._location: Path  # absolute, resolved location
        super().__init__(location=path)

    def _contains(self, uid: UID) -> bool:
        path = self._get_abs_path(uid)
        return path.exists()

    def _getitem(self, uid: UID) -> bytes:
        path = self._get_abs_path(uid)
        logging.debug("Reading %s", path)
        return path.read_bytes()

    def _setitem(self, uid: UID, data: bytes) -> None:
        path = self._get_abs_path(uid)
        logging.debug("Writing %s", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def _delitem(self, uid: UID) -> None:
        path = self._get_abs_path(uid)
        logging.debug("Deleting %s", path)
        os.remove(path)

    def _list(self, **filters: MetadataValue) -> Iterable[UID]:
        for root, _, fs in os.walk(self._location):
            for f in fs:
                if f.endswith(self.metadata_sufix):
                    continue
                path = Path(root) / str(f)
                uid = str(path.relative_to(self._location))

                if filters:
                    metadata = self._metadata(uid)
                    if not metadata._match(**filters):
                        continue

                yield uid

    def _metadata(self, uid: UID) -> JsonFileMetadataStorage:
        path = self._get_abs_path(uid)
        path_metadata = path.with_name(path.name + self.metadata_sufix).resolve()
        return JsonFileMetadataStorage(path_metadata)

    def _get_abs_path(self, uid: UID) -> Path:
        return (self._location / uid).resolve()

    def _get_valid_uid(self, uid: UID) -> UID:
        """should be a relative path"""
        abs_path = self._get_abs_path(uid)
        if not abs_path.is_relative_to(self._location):
            raise StorageInvalidUidError(
                f"Cannot use uid outside of storage location: {uid}", uid=UID()
            )
        if abs_path.exists() and not abs_path.is_file():
            raise StorageInvalidUidError(f"uid is cannot be a file: {uid}", uid=UID())
        return UID(abs_path.relative_to(self._location))


class FileDataStorageWithRdfMetadata(FileDataStorage):
    """TODO"""

    def _metadata(self, uid: UID) -> JsonLdFileMetadataStorage:
        path = self._get_abs_path(uid)
        path_metadata = path.with_name(path.name + self.metadata_sufix).resolve()
        return JsonLdFileMetadataStorage(path_metadata, uid=uid)


class HttpMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, url: str):
        self._location = url

    def _request(
        self,
        path: str = "/",
        method: Literal["GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"] = "GET",
        params: dict | None = None,
        data: dict | None = None,
    ):
        url = self._location + path
        resp = httpx.request(method=method, url=url, params=params, json=data)
        resp.raise_for_status()
        return resp

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        return self._request(params={"a": attribute}).json()

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._request(method="POST", data={attribute: value})


class HttpDataStorage(DataStorage):
    """TODO

    TODO: maybe add option to delegate import to handle on remote side?
    """

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        return bool(re.match(r"^https?://", location))

    def _request(
        self,
        path: str = "/",
        method: Literal["GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"] = "GET",
        params: dict | None = None,
        data: bytes | None = None,
    ):
        url = self._location + path
        resp = httpx.request(method=method, url=url, params=params, content=data)
        resp.raise_for_status()
        return resp

    def _contains(self, uid: UID) -> bool:
        try:
            resp = self._request(path=f"/{uid}", method="HEAD")
            return resp.is_success
        except httpx.HTTPStatusError as exc:
            if not exc.response.status_code == 404:
                raise
        return False

    def _getitem(self, uid: UID) -> bytes:
        resp = self._request(path=f"/{uid}", method="GET")
        return resp.content

    def _setitem(self, uid: UID, data: bytes) -> None:
        self._request(path=f"/{uid}", method="PUT", data=data)

    def _delitem(self, uid: UID) -> None:
        self._request(path=f"/{uid}", method="DELETE")

    def _list(self, **filters: MetadataValue) -> Iterable[UID]:
        filters_list = [f"{k}={v}" for k, v in filters.items()]
        return self._request(path="/", params={"q": filters_list}).json()

    def _metadata(self, uid: UID) -> HttpMetadataStorage:
        url = self._location + f"/{uid}/metadata"
        return HttpMetadataStorage(url)


class SqlMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, engine: Engine, uid: UID):
        self._engine = engine
        self._uid = uid

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        with self._engine.begin() as con:
            resp = con.execute(
                table_metadata.select()
                .with_only_columns(table_metadata.c.value)
                .where(
                    table_metadata.c.uid == self._uid,
                    table_metadata.c.attribute == attribute,
                )
            )
            # todo: better parser
            return [try_parse_json_str(x[0]) for x in resp.fetchall()]

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        with self._engine.begin() as con:
            con.execute(
                table_metadata.insert().values(
                    uid=self._uid, attribute=attribute, value=value
                )
            )


class SqlDataStorage(DataStorage):
    """TODO"""

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        return bool(re.match(r"^.*sql.*://", location))

    def __init__(self, location: str = "sqlite:///:memory:"):
        super().__init__(location=location)
        self._engine = create_engine(location)

        metadata.create_all(self._engine)

    def _contains(self, uid: UID) -> bool:
        with self._engine.begin() as con:
            resp = con.execute(
                table_data.select()
                .with_only_columns(table_data.c.uid)
                .where(table_data.c.uid == uid)
            )
            n_res = len(resp.fetchall())
        return bool(n_res)

    def _getitem(self, uid: UID) -> bytes:
        with self._engine.begin() as con:
            resp = con.execute(
                table_data.select()
                .with_only_columns(table_data.c.data)
                .where(table_data.c.uid == uid)
            )
            row = resp.fetchone()
            if not row:
                raise Exception()
            return row[0]

    def _setitem(self, uid: UID, data: bytes) -> None:
        with self._engine.begin() as con:
            con.execute(table_data.insert().values(uid=uid, data=data))

    def _delitem(self, uid: UID) -> None:
        with self._engine.begin() as con:
            con.execute(table_data.delete().where(table_data.c.uid == uid))

    def _list(self, **filters: MetadataValue) -> Iterable[UID]:
        with self._engine.begin() as con:
            resp = con.execute(table_data.select().with_only_columns(table_data.c.uid))
            uids = {x[0] for x in resp.fetchall()}
            # TODO query join directly in database
            for a, v in filters.items():
                print(con.execute(table_metadata.select()).fetchall())
                resp = con.execute(
                    table_metadata.select()
                    .with_only_columns(table_metadata.c.uid)
                    .where(table_metadata.c.attribute == a, table_metadata.c.value == v)
                )
                uids_filtered = {x[0] for x in resp.fetchall()}
                uids = uids & uids_filtered

        return uids

    def _metadata(self, uid: UID) -> MetadataStorage:
        return SqlMetadataStorage(engine=self._engine, uid=uid)


class TestCliMetadataDataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, uid: UID, request: Callable):
        self._uid = uid
        self._request = request

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        data = self._request("metadata", "get", self._uid, str(attribute))
        for text in reverse_prints(data):
            yield try_parse_json_str(text)

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._request("metadata", "set", self._uid, f"{attribute}={value}")


class CliWrapperDataStorage(DataStorage):
    """TODO"""

    def __init__(self, location: Any = None):
        self._location = location
        self._python = sys.executable
        self._module = str(Path(__file__).parent / "__main__.py")

    def _request(self, *args: str, data: bytes | None = None) -> bytes:
        cmd = [self._python, self._module, "-l", str(self._location)] + list(args)
        logging.debug(cmd)
        pop = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE)
        stdout, _stderr = pop.communicate(data)
        if pop.returncode:
            raise SubprocessStatus(pop.returncode)
        return stdout

    def _contains(self, uid: UID) -> bool:
        try:
            self._request("has", uid)
        except SubprocessStatus:
            return False
        return True

    def _getitem(self, uid: UID) -> bytes:
        return self._request("get", uid)

    def _setitem(self, uid: UID, data: bytes) -> None:
        self._request("put", uid, data=data)

    def _delitem(self, uid: UID) -> None:
        self._request("delete", uid)

    def _metadata(self, uid: UID) -> MetadataStorage:
        return TestCliMetadataDataStorage(uid, self._request)

    def _list(self, **filters: MetadataValue) -> Iterable[UID]:
        filters_str = [f"{k}={v}" for k, v in filters.items()]
        data = self._request("find", *filters_str)
        return reverse_prints(data)
