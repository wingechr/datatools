"""TODO"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
import contextlib
import functools
import pickle
from typing import Any

from datatools.exceptions import (
    StorageFileExistsError,
    StorageFileNotFoundError,
    StorageInvalidNameError,
)
from datatools.job.importer import infer_importer_class
from datatools.job.job import FunctionWrapper, Job, default_get_job_hashsum
from datatools.types import (
    PROP_CONVERTED_WITH,
    PROP_CREATOR,
    PROP_DATETIME,
    PROP_FUNCTION,
    PROP_GENERATED_BY,
    PROP_PARAMETER,
    PROP_PARAMETER_NAME,
    PROP_PARAMETER_VALUE,
    SINGLE_OUTPUT_PARAM_NAME,
    ByteData,
    FunFromBytes,
    FunHashsum,
    FunParams,
    FunResult,
    FunToBytes,
    MetadataAttribute,
    MetadataValue,
    Name,
)
from datatools.utils import (
    get_now_str,
    get_user_w_host,
    identity,
    remove_credentials_from_netloc,
)


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
    def _contains(self, name: Name) -> bool: ...

    @abstractmethod
    def _getitem(self, name: Name) -> ByteData: ...

    @abstractmethod
    def _setitem(self, name: Name, data: ByteData) -> None: ...

    @abstractmethod
    def _delitem(self, name: Name) -> None: ...

    @abstractmethod
    def _metadata(self, name: Name) -> MetadataStorage: ...

    @abstractmethod
    def _list(self) -> Iterable[Name]: ...

    def _find(self, **filters: MetadataValue) -> Iterable[Name]:
        """primitive implementation"""
        for name in self._list():
            if filters:
                metadata = self._metadata(name)
                if not metadata._match(**filters):
                    continue

            yield name

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        return False

    def _get_valid_name(self, name: Name) -> Name:
        return Name(name).strip()

    def __iter__(self) -> Iterator[Name]:
        return iter(self._list())

    def __contains__(self, name: Name) -> bool:
        self._assert_valid_name(name=name)
        return self._contains(name=name)

    def __getitem__(self, name: Name) -> ByteData:
        self._assert_valid_name(name=name)
        if name not in self:
            raise StorageFileNotFoundError(f"Not found: {name}")
        return self._getitem(name=name)

    def __setitem__(self, name: Name, data: ByteData) -> None:
        self._assert_valid_name(name=name)
        if name in self:
            raise StorageFileExistsError(f"Already exists: {name}")
        return self._setitem(name=name, data=data)

    def __delitem__(self, name: Name) -> None:
        self._assert_valid_name(name=name)
        if name not in self:
            raise StorageFileNotFoundError(f"Not found: {name}")
        return self._delitem(name=name)

    def metadata(self, name: Name) -> MetadataStorage:
        """Metadata container associated with data."""
        self._assert_valid_name(name=name)
        return self._metadata(name=name)

    def find(self, **filters: MetadataValue) -> Iterable[Name]:
        """list names for given metadata query."""
        return self._find(**filters)

    def _assert_valid_name(self, name: Name):
        valid_name = self._get_valid_name(name)
        if name != valid_name:
            raise StorageInvalidNameError(
                f"Invalid name: {name} => {valid_name}", name=valid_name
            )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self._location})"

    def info(self) -> dict:
        """TODO"""
        return {"Location": str(self._location), "Class": str(self.__class__.__name__)}

    def import_from_uri(self, uri: str, name: Name | None = None, **options) -> Name:
        """TODO"""

        importer_class = infer_importer_class(uri, **options)
        name = name or importer_class.get_output_name(uri, **options)

        # logging.error((uri, name, options))

        job = self.job(
            function=importer_class.get_data,
            output_converters={
                SINGLE_OUTPUT_PARAM_NAME: importer_class.output_to_bytes
            },
        )
        job(name, uri, **options)
        return name

    def cache(
        self,
        output_to_bytes: FunToBytes = pickle.dumps,
        output_from_bytes: FunFromBytes = pickle.loads,
        get_name_from_hash: Callable[[str], str] = identity,
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
                output_name = get_name_from_hash(hashsum)
                # logging.error((hash_data, hashsum))

                if output_name not in self:
                    # run job (first arg (_output_name) is name)
                    job(output_name, *args, **kwargs)

                # retrieval
                data = self[output_name]
                result = output_from_bytes(data)
                return result

            return _fun

        return decorator

    def job(
        self,
        function: Callable,
        output_converters: dict[str, FunToBytes | None] | FunToBytes | None = None,
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

        callback_data = {
            "metadata_origin": {
                PROP_FUNCTION: wrapped_function.get_metadata(),
                PROP_PARAMETER: [],  # will be filled by input handlers
                PROP_DATETIME: get_now_str(),
                PROP_CREATOR: get_user_w_host(),
            },
            "input_parameter_values": {},
            "job": None,  # will be filled later
        }

        def wrap_input_handler(name: str, handler: Callable):
            def handle_(name_value: Name):
                callback_data["input_parameter_values"][name] = name_value
                handler_w = FunctionWrapper.assert_wrapped(handler)

                callback_data["metadata_origin"][PROP_PARAMETER].append(
                    {
                        PROP_PARAMETER_VALUE: name_value,
                        PROP_PARAMETER_NAME: name,
                        PROP_CONVERTED_WITH: handler_w.get_metadata(),
                    }
                )

                bdata = self[name_value]
                return handler(bdata)

            return handle_

        def create_input_handler(name):
            def handle_(value: Any):
                with contextlib.suppress(Exception):
                    # TODO: maybe get fromhandler
                    value = remove_credentials_from_netloc(value)

                callback_data["input_parameter_values"][name] = value
                callback_data["metadata_origin"][PROP_PARAMETER].append(
                    {PROP_PARAMETER_VALUE: value, PROP_PARAMETER_NAME: name}
                )

                return value

            return handle_

        def wrap_output_handler(handler: Callable | None = None):
            def create_job_id():
                # generate job id (once)
                if "@id" not in callback_data["metadata_origin"]:
                    job: Job = callback_data["job"]
                    job_hashsum = job.get_job_hashsum(
                        **callback_data["input_parameter_values"]
                    )
                    job_id = f"job:{job_hashsum}"
                    callback_data["metadata_origin"]["@id"] = job_id

            if handler:
                handler_w = FunctionWrapper.assert_wrapped(handler)

                def handle_(data: Any, name: Name):
                    bdata = handler(data)
                    self[name] = bdata
                    create_job_id()
                    metadata = self.metadata(name)
                    metadata[PROP_GENERATED_BY] = callback_data["metadata_origin"] | {
                        PROP_CONVERTED_WITH: handler_w.get_metadata(),
                    }
            else:

                def handle_(data: Any, name: Name):
                    self[name] = data
                    create_job_id()
                    metadata = self.metadata(name)
                    metadata[PROP_GENERATED_BY] = callback_data["metadata_origin"]

            return handle_

        def check_names_exist(**names):
            return all(name in self for name in names.values())

        wrapped_output_handlers = {
            name: wrap_output_handler(conv) for name, conv in output_converters.items()
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

        job = Job(
            function,
            output_writers=wrapped_output_handlers,
            input_readers=wrapped_input_handlers,
            get_job_hashsum=get_job_hashsum,
            check_done=check_names_exist if skip_finished else None,
        )
        callback_data["job"] = job
        return job
