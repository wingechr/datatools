"""TODO"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
import datetime
import functools
import pickle
from typing import Any

from datatools.exceptions import (
    StorageFileExistsError,
    StorageFileNotFoundError,
    StorageInvalidUidError,
)
from datatools.job.importer import infer_importer_class
from datatools.job.job import FunctionWrapper, Job, default_get_job_hashsum
from datatools.types import (
    SINGLE_OUTPUT_PARAM_NAME,
    UID,
    ByteData,
    FunFromBytes,
    FunHashsum,
    FunParams,
    FunResult,
    FunToBytes,
    MetadataAttribute,
    MetadataValue,
)
from datatools.utils import get_user_w_host, identity


class MetadataStorage(ABC):  # TODO: subclass AbstractContextManager ?
    """Abstract metadata storage."""

    @abstractmethod
    def _getitem(self, attribute: MetadataAttribute) -> list[MetadataValue]: ...

    @abstractmethod
    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None: ...

    def _match(self, **filters: MetadataValue) -> bool:
        # FIXME: implement better operators
        does_match = all(
            value in self[attribute] for attribute, value in filters.items()
        )
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
        return UID(uid).strip()

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
            output_converters={
                SINGLE_OUTPUT_PARAM_NAME: importer_class.output_to_bytes
            },
        )
        job(uid, uri, **options)
        return uid

    def cache(
        self,
        output_to_bytes: FunToBytes = pickle.dumps,
        output_from_bytes: FunFromBytes = pickle.loads,
        get_uid_from_hash: Callable[[str], str] = identity,
        get_job_hashsum: FunHashsum = default_get_job_hashsum,
    ) -> Callable:
        """TODO"""

        def decorator(function: Callable[FunParams, FunResult]) -> Callable:
            """TODO"""

            job = self.job(
                function=function,
                output_converters={SINGLE_OUTPUT_PARAM_NAME: output_to_bytes},
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
        output_converters: dict[str, FunToBytes | None] | FunToBytes,
        input_converters: dict[str, FunFromBytes | None] | None = None,
        get_job_hashsum: FunHashsum = default_get_job_hashsum,
        skip_finished: bool = False,
    ) -> Job:
        """TODO"""

        wrapped_function = FunctionWrapper.assert_wrapped(function)
        if not isinstance(output_converters, dict):
            output_converters = {SINGLE_OUTPUT_PARAM_NAME: output_converters}

        # update metadata before running job
        # so output handlers can use it
        timestamp = datetime.datetime.now().isoformat()

        metadata_origin = {
            "timestamp": timestamp,
            "parameter": {},  # will be filled by input handlers
            "function": {
                "@id": wrapped_function.get_function_id(),
                "description": wrapped_function.description,
            },
            "user": get_user_w_host(),
        }

        def wrap_input_handler(name: str, handler: Callable):
            def handle_(uid: UID):
                handler_w = FunctionWrapper.assert_wrapped(handler)
                metadata_origin["parameter"][name] = {
                    "@value": uid,
                    "@id": handler_w.get_function_id(),
                    "description": handler_w.description,
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
            handler_w = FunctionWrapper.assert_wrapped(handler)

            def handle_(data: Any, uid: UID):
                bdata = handler(data)
                self[uid] = bdata
                metadata = self.metadata(uid)
                metadata["origin"] = metadata_origin | {
                    "conversion": {
                        "@id": handler_w.get_function_id(),
                        "description": handler_w.description,
                    },
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
