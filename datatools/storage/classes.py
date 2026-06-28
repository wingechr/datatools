"""TODO"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
import datetime
import functools
import json
import logging
import os
from pathlib import Path
import pickle
import re
from typing import Any, Literal

from click.testing import CliRunner
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
from datatools.job.classes import FunctionWrapper, Job, default_get_job_hashsum
from datatools.types import (
    UID,
    ByteData,
    FunParams,
    FunResult,
    MetadataAttribute,
    MetadataValue,
    StorageFileExistsError,
    StorageFileNotFoundError,
    StorageInvalidUidError,
    SubprocessStatus,
)
from datatools.utils import (
    TextFile,
    identity,
    is_file_uri_or_path,
    json_dumps_for_print,
    jsonpath_get,
    jsonpath_update,
    reverse_prints,
    try_parse_json_str,
    uri_or_path_to_path,
)

_OUTPUT_PARAM_NAME = "__output"  # any name, must not collide with parameters

sql_base = MetaData()

table_data = Table(
    "data",
    sql_base,
    Column("uid", VARCHAR, primary_key=True),
    Column("data", VARBINARY),
)

table_metadata = Table(
    "metadata",
    sql_base,
    Column("uid", VARCHAR, primary_key=True),
    Column("metadata", VARCHAR),  # or use JSON
)


class MetadataStorage(ABC):  # TODO: subclass AbstractContextManager ?
    """Abstract metadata storage."""

    @abstractmethod
    def _getitem(self, attribute: MetadataAttribute) -> list[MetadataValue]: ...

    @abstractmethod
    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None: ...

    def _match(self, **filters: MetadataValue) -> bool:
        # FIXME: implement betetr operators
        does_match = all(
            value in self[attribute] for attribute, value in filters.items()
        )
        for attribute, value in filters.items():
            values = self[attribute]
            logging.error((attribute, value, values))

        return does_match

    def __getitem__(self, attribute: MetadataAttribute) -> list[MetadataValue]:
        return self._getitem(attribute=attribute)

    def __setitem__(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        return self._setitem(attribute=attribute, value=value)


class DataStorage(ABC):
    """Abstract data storage."""

    is_delegating: bool = False  # delegate to another Storage

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
    def _list(self) -> Iterable[UID]: ...

    def _find(self, **filters: MetadataValue) -> Iterable[UID]:
        """primitive implementation"""
        for uid in self._list():
            if filters:
                metadata = self._metadata(uid)
                if not metadata._match(**filters):
                    continue

            yield uid

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
        return self._find(**filters)

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

    def import_from_uri(self, uri: str, uid: UID | None = None, **options) -> UID:
        """TODO"""

        importer_class = infer_importer_class(uri, **options)
        uid = uid or importer_class.get_output_uid(uri, **options)

        # logging.error((uri, uid, options))

        job = self.job(
            function=importer_class.get_data,
            output_converters={_OUTPUT_PARAM_NAME: importer_class.output_to_bytes},
        )
        job(uid, uri, **options)
        return uid

    def cache(
        self,
        output_to_bytes: Callable[[Any], bytes] = pickle.dumps,
        output_from_bytes: Callable[[bytes], Any] = pickle.loads,
        get_uid_from_hash: Callable[[str], str] = identity,
        get_job_hashsum: Callable[..., str] = default_get_job_hashsum,
    ) -> Callable:
        """TODO"""

        def decorator(function: Callable[FunParams, FunResult]) -> Callable:
            """TODO"""

            # any name, must not collide with parameters
            _single_output_param_name = "__output"

            job = self.job(
                function=function,
                output_converters={_single_output_param_name: output_to_bytes},
                get_job_hashsum=get_job_hashsum,
            )

            @functools.wraps(function)
            def _fun(*args, **kwargs):
                hashsum = job.get_job_hashsum(*args, **kwargs)
                output_uid = get_uid_from_hash(hashsum)
                # logging.error((hash_data, hashsum))

                if output_uid not in self:
                    # run job (first arg (_output_name) is uid)
                    job(output_uid, *args, **kwargs)

                # retrieval
                data = self[output_uid]
                result = output_from_bytes(data)
                return result

            return _fun

        return decorator

    def job(
        self,
        function: Callable,
        output_converters: dict[str, Callable[[Any], bytes] | None]
        | Callable[[bytes], Any],
        input_converters: dict[str, Callable[[bytes], Any] | None] | None = None,
        get_job_hashsum: Callable[..., str] = default_get_job_hashsum,
        skip_finished: bool = False,
    ) -> Job:
        """TODO"""

        wrapped_function = FunctionWrapper.assert_wrapped(function)
        if not isinstance(output_converters, dict):
            output_converters = {_OUTPUT_PARAM_NAME: output_converters}

        # update metadata before running job
        # so output handlers can use it
        timestamp = datetime.datetime.now().isoformat()

        metadata_origin = {
            "timestamp": timestamp,
            "parameter": {},  # will be filled by input handlers
            "function": {"@id": wrapped_function.get_function_id()},
        }

        def wrap_input_handler(name: str, handler: Callable):
            def handle_(uid: UID):
                metadata_origin["parameter"][name] = {
                    "@value": uid,
                    "converter": FunctionWrapper.assert_wrapped(
                        handler
                    ).get_function_id(),
                }
                bdata = self[uid]
                return handler(bdata)

            return handle_

        def create_input_handler(name):
            def handle_(value: Any):
                metadata_origin["parameter"][name] = value
                return value

            return handle_

        def wrap_output_handler(handler: Callable):
            def handle_(data: Any, uid: UID):
                bdata = handler(data)
                self[uid] = bdata
                metadata = self.metadata(uid)
                metadata["origin"] = metadata_origin | {
                    "converter": FunctionWrapper.assert_wrapped(
                        handler
                    ).get_function_id(),
                }

            return handle_

        def check_uids_exist(**uids):
            return all(uid in self for uid in uids.values())

        wrapped_output_handlers = {
            name: wrap_output_handler(conv or identity)
            for name, conv in output_converters.items()
        }

        input_converters = input_converters or {}
        # !! we need to wrap all input parameters
        wrapped_input_handlers = {
            name: (
                wrap_input_handler(name, input_converters[name] or identity)
                if name in input_converters
                else create_input_handler(name)
            )
            for name in wrapped_function.fun_parameter_names
        }

        return Job(
            function,
            output_writers=wrapped_output_handlers,
            input_readers=wrapped_input_handlers,
            get_job_hashsum=get_job_hashsum,
            check_done=check_uids_exist if skip_finished else None,
        )


class MemoryMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, data: dict | None = None):
        self._data = {} if data is None else data

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        return jsonpath_get(data=self._data, key=attribute)

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        jsonpath_update(data=self._data, key=attribute, val=value)


class PersistentMemoryMetadataStorage(MemoryMetadataStorage):
    """TODO"""

    def __init__(self):
        super().__init__(data=self._load_or_init())
        self._changed = False

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        super()._setitem(attribute=attribute, value=value)
        self._changed = True

    def __del__(self):
        if self._changed:
            self._dump(self._data)

    @abstractmethod
    def _load_or_init(self) -> dict | None: ...

    @abstractmethod
    def _dump(self, data: dict) -> None: ...


class JsonFileMetadataStorage(PersistentMemoryMetadataStorage):
    """FIXME

    - we load data on init and save on __del__, which is uper unsafe.
    - but we also dont want to load file every time?
    """

    def __init__(self, path: Path):
        self._file = TextFile(path)
        super().__init__()

    def _load_or_init(self) -> dict | None:
        if self._file.exists():
            data = self._file.load_json()
            if not isinstance(data, dict):
                raise ValueError("json file for metadata must be dict")
            return data

    def _dump(self, data: dict) -> None:
        self._file.dump_json(data)


class JsonLdFileMetadataStorage(JsonFileMetadataStorage):
    """FIXME

    this is all still very experimental: clients expect to use
    jsonpath queries, so we convert from / to json

    for now, we just parse json as jsonld and back to see if its possible

    """

    def __init__(self, path: Path, uid: UID):
        self.uid = uid
        self.context = {"@vocab": "urn:dummy/"}
        super().__init__(path)

    def _load_or_init(self) -> dict | None:
        data = super()._load_or_init()
        if not data:
            data = {"@id": self.uid, "@context": self.context}

        return data

    def _dump(self, data: dict) -> None:
        # rdf roundtrip test

        data_s = json.dumps(data)
        g = rdflib.Graph()
        g.parse(data=data_s, format="json-ld")
        data_s_new = g.serialize(format="json-ld", context=self.context)
        data_new = json.loads(data_s_new)

        super()._dump(data_new)

    def _as_uri(self, x: str) -> rdflib.URIRef:
        """FIXME"""
        return rdflib.URIRef("urn:" + x)

    def _as_uri_or_literal(self, x: MetadataValue) -> rdflib.URIRef | rdflib.Literal:
        """FIXME"""
        return rdflib.Literal(x)


class MemoryDataStorage(DataStorage):
    """TODO"""

    def __init__(self):
        super().__init__(location=None)
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

    def _list(self) -> Iterable[UID]:
        logging.error(self.__data.keys())
        return self.__data.keys()

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

    def _list(self) -> Iterable[UID]:
        for root, _, fs in os.walk(self._location):
            for f in fs:
                if f.endswith(self.metadata_sufix):
                    continue
                path = Path(root) / str(f)
                uid = str(path.relative_to(self._location))

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
        return abs_path.relative_to(self._location).as_posix()


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
    """TODO"""

    is_delegating = True  # delegates to http server

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

    def _list(self) -> Iterable[UID]:
        return self._request(path="/").json()

    def _find(self, **filters: MetadataValue) -> Iterable[UID]:
        filters_list = [f"{k}={v}" for k, v in filters.items()]
        return self._request(path="/", params={"q": filters_list}).json()

    def _metadata(self, uid: UID) -> HttpMetadataStorage:
        url = self._location + f"/{uid}/metadata"
        return HttpMetadataStorage(url)

    def info(self) -> dict:
        """TODO"""
        info_remote = self._request(path="/info").json()
        info_client = super().info()
        info_client.update({"remote": info_remote})

        return info_client


class SqlMetadataStorage(PersistentMemoryMetadataStorage):
    """TODO"""

    def __init__(self, engine: Engine, uid: UID):
        self._engine = engine
        self._uid = uid
        super().__init__()

    def _load_or_init(self) -> dict | None:
        with self._engine.begin() as con:
            rows = con.execute(
                table_metadata.select()
                .with_only_columns(table_metadata.c.metadata)
                .where(
                    table_metadata.c.uid == self._uid,
                )
            ).fetchall()
        if rows:
            data_s = rows[0][0]
            return json.loads(data_s)

    def _dump(self, data: dict) -> None:
        data_s = json.dumps(data, ensure_ascii=False)
        # check if row exists
        with self._engine.begin() as con:
            resp = con.execute(
                table_metadata.select()
                .with_only_columns(table_metadata.c.uid)
                .where(table_metadata.c.uid == self._uid)
            )
            n_res = len(resp.fetchall())
            if n_res:
                # update
                con.execute(
                    table_metadata.update()
                    .values(metadata=data_s)
                    .where(table_metadata.c.uid == self._uid)
                )
            else:
                # insert
                con.execute(
                    table_metadata.insert().values(uid=self._uid, metadata=data_s)
                )


class SqlDataStorage(DataStorage):
    """TODO

    TODO: for faster query, we should split structured data
    into triples? for now: we only use Json

    """

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        return bool(re.match(r"^.*sql.*://", location))

    def __init__(self, location: str = "sqlite:///:memory:"):
        super().__init__(location=location)
        self._engine = create_engine(location)

        sql_base.create_all(self._engine)

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

    def _list(self) -> Iterable[UID]:
        with self._engine.begin() as con:
            resp = con.execute(table_data.select().with_only_columns(table_data.c.uid))
            uids = {x[0] for x in resp.fetchall()}
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
        logging.warning("cli meta get: %s", data)
        return try_parse_json_str(data)

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._request(
            "metadata",
            "set",
            self._uid,
            f"{attribute}={json_dumps_for_print(value)}",
        )


class CliWrapperDataStorage(DataStorage):
    """TODO"""

    is_delegating = True  # delegates to script

    def __init__(self, location: Any = None):
        self._location = location
        self._script = str(Path(__file__).parent / "__main__.py")
        self._clirunner = CliRunner()

        from datatools.storage.__main__ import main as storage_main_cli

        self._storage_main_cli = storage_main_cli

    def _request(self, *args: str, data: bytes | None = None) -> bytes:
        cmd = ["-l", str(self._location)] + list(args)
        # stdout, _stderr = call_script(
        #    self._script, cmd, data
        # )
        logging.debug("CLI " + " ".join(cmd))
        result = self._clirunner.invoke(self._storage_main_cli, cmd, input=data)
        if result.exit_code:
            raise SubprocessStatus(result.exit_code)

        stdout = result.stdout_bytes

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

    def _find(self, **filters: MetadataValue) -> Iterable[UID]:
        filters_str = [f"{k}={v}" for k, v in filters.items()]
        data = self._request("find", *filters_str)
        return reverse_prints(data)

    def _list(self) -> Iterable[UID]:
        return self._find()

    def info(self) -> dict:
        """TODO"""
        info_remote = self._request("info")
        logging.error(info_remote)
        info_remote = json.loads(info_remote)
        info_client = super().info()
        info_client.update({"remote": info_remote})

        return info_client
