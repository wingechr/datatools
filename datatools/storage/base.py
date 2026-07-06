"""TODO"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
import contextlib
import functools
import hashlib
import pickle
from typing import Any

from datatools.exceptions import (
    StorageFileExistsError,
    StorageFileNotFoundError,
    StorageInvalidNameError,
)
from datatools.process.importer import infer_importer_class
from datatools.process.task import AnnotatedFunction, Task, default_get_job_hashsum
from datatools.types import (
    PROP_CREATOR,
    PROP_DATETIME,
    PROP_FILE,
    PROP_FUNCTION,
    PROP_GENERATED_BY,
    PROP_JOB,
    PROP_LOADED_WITH,
    PROP_PARAMETER,
    PROP_PARAMETER_NAME,
    PROP_PARAMETER_VALUE,
    PROP_SAVED_WITH,
    PROP_SIZE,
    SINGLE_OUTPUT_PARAM_NAME,
    ByteData,
    FunFromBytes,
    FunHashsum,
    FunParams,
    FunResult,
    FunToBytes,
    Json,
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
    def get(self, attribute: MetadataAttribute) -> list[MetadataValue]: ...

    @abstractmethod
    def set(self, attribute: MetadataAttribute, value: MetadataValue) -> None: ...

    def _match(self, **filters: MetadataValue) -> bool:
        # FIXME: implement better operators
        does_match = all(
            value in self.get(attribute) for attribute, value in filters.items()
        )
        return does_match


class DataStorage(ABC):
    """Abstract data storage."""

    is_delegating: bool = False  # delegate to another Storage

    def __init__(self, location: Any = None):
        self._location = location

    @abstractmethod
    def _has(self, name: Name) -> bool: ...

    @abstractmethod
    def _read(self, name: Name) -> ByteData: ...

    @abstractmethod
    def _write(self, name: Name, data: ByteData) -> None: ...

    @abstractmethod
    def _delete(self, name: Name) -> None: ...

    @abstractmethod
    def _metadata(self, name: Name) -> MetadataStorage: ...

    @abstractmethod
    def _list(self) -> Iterable[Name]: ...

    def find(self, **filters: MetadataValue) -> Iterable[Name]:
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

    def has(self, name: Name) -> bool:
        """TODO"""
        self._assert_valid_name(name=name)
        return self._has(name=name)

    def read(self, name: Name) -> ByteData:
        """TODO"""
        self._assert_valid_name(name=name)
        if not self.has(name):
            raise StorageFileNotFoundError(f"Not found: {name}")
        return self._read(name=name)

    def write(self, name: Name, data: ByteData) -> None:
        """TODO"""
        self._assert_valid_name(name=name)
        if self.has(name):
            raise StorageFileExistsError(f"Already exists: {name}")
        return self._write(name=name, data=data)

    def delete(self, name: Name) -> None:
        """TODO"""
        self._assert_valid_name(name=name)
        if not self.has(name):
            raise StorageFileNotFoundError(f"Not found: {name}")
        return self._delete(name=name)

    def metadata(self, name: Name) -> MetadataStorage:
        """Metadata container associated with data."""
        self._assert_valid_name(name=name)
        return self._metadata(name=name)

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

        task = self.task(
            function=importer_class.get_data,
            output_converters={
                SINGLE_OUTPUT_PARAM_NAME: importer_class.output_to_bytes
            },
        )
        task(name, uri, **options)
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

            task = self.task(
                function=function,
                output_converters={SINGLE_OUTPUT_PARAM_NAME: output_to_bytes},
                get_job_hashsum=get_job_hashsum,
            )

            @functools.wraps(function)
            def _fun(*args, **kwargs):
                hashsum = task.get_job_hashsum(*args, **kwargs)
                output_name = get_name_from_hash(hashsum)
                # logging.error((hash_data, hashsum))

                if not self.has(output_name):
                    # run task (first arg (_output_name) is name)
                    task(output_name, *args, **kwargs)

                # retrieval
                data = self.read(output_name)
                result = output_from_bytes(data)
                return result

            return _fun

        return decorator

    def task(
        self,
        function: Callable,
        output_converters: dict[str, FunToBytes | None] | FunToBytes | None = None,
        input_converters: dict[str, FunFromBytes | None] | None = None,
        metadata_generator: Callable[[Any], dict[str, Json]] | None = None,
        get_job_hashsum: FunHashsum = default_get_job_hashsum,
        skip_finished: bool = False,
    ) -> Task:
        """TODO"""

        wrapped_function = AnnotatedFunction.assert_wrapped(function)
        if not isinstance(output_converters, dict):
            output_converters = {SINGLE_OUTPUT_PARAM_NAME: output_converters}

        # update metadata before running task
        # so output handlers can use it

        callback_data = {
            "metadata_activity": {
                # "@id": None,
                "@type": "Activity",
                PROP_DATETIME: get_now_str(),
                PROP_CREATOR: get_user_w_host(),
                PROP_FUNCTION: wrapped_function.get_metadata(),
                PROP_PARAMETER: [],  # will be filled by input handlers
            },
            "metadata_generated": {},
            "input_parameter_values": {},
            "task": None,  # will be filled later
        }

        def wrap_input_handler(name: str, handler: Callable):
            def handle_(name_value: Name):
                callback_data["input_parameter_values"][name] = name_value
                handler_w = AnnotatedFunction.assert_wrapped(handler)

                callback_data["metadata_activity"][PROP_PARAMETER].append(
                    {
                        "@type": "Input",
                        # "@id": set later when we have it
                        PROP_PARAMETER_VALUE: name_value,
                        PROP_PARAMETER_NAME: name,
                        PROP_LOADED_WITH: handler_w.get_metadata(),
                    }
                )

                bdata = self.read(name_value)
                return handler(bdata)

            return handle_

        def create_input_handler(name):
            def handle_(value: Any):
                with contextlib.suppress(Exception):
                    # TODO: maybe get from handler
                    value = remove_credentials_from_netloc(value)

                callback_data["input_parameter_values"][name] = value
                callback_data["metadata_activity"][PROP_PARAMETER].append(
                    {
                        "@type": "Input",
                        PROP_PARAMETER_VALUE: value,
                        PROP_PARAMETER_NAME: name,
                    }
                )

                return value

            return handle_

        def update_metadata_task_id(data: Any):
            # generate task id (once)
            metadata_activity = "@id" in callback_data["metadata_activity"]

            if not metadata_activity:
                task: Task = callback_data["task"]
                job_hashsum = task.get_job_hashsum(
                    **callback_data["input_parameter_values"]
                )
                job_id = f"job:{job_hashsum}"
                datatime = callback_data["metadata_activity"][PROP_DATETIME]
                activity_id = f"activity:{job_hashsum}-{datatime}"
                callback_data["metadata_activity"]["@id"] = activity_id
                callback_data["metadata_activity"][PROP_JOB] = job_id
                # update ids for input parameters
                for p in callback_data["metadata_activity"][PROP_PARAMETER]:
                    p["@id"] = activity_id + "/input/" + p[PROP_PARAMETER_NAME]

                # generate metadata
                if metadata_generator:
                    callback_data["metadata_generated"] = metadata_generator(data)

        def wrap_output_handler(param_name: str, handler: Callable | None = None):
            if handler:
                handler_w = AnnotatedFunction.assert_wrapped(handler)
                meta_saved_with = {
                    # "@id": set later when we have it
                    PROP_FUNCTION: handler_w.get_metadata(),
                    PROP_PARAMETER_NAME: param_name,
                }
            else:
                meta_saved_with = {}
                handler = identity

            def handle_(data: Any, name: Name):
                bdata = handler(data)
                self.write(name, bdata)
                update_metadata_task_id(data)

                output_metadata = {
                    "@type": "Output",
                    "name": name,
                    "@id": callback_data["metadata_activity"]["@id"]
                    + "/output/"
                    + param_name,
                    PROP_FILE: {
                        "@id": "md5:" + hashlib.md5(bdata).hexdigest(),  # noqa:S324
                        "@type": "File",
                        PROP_SIZE: len(bdata),
                    },
                    PROP_GENERATED_BY: callback_data["metadata_activity"],
                }

                if meta_saved_with:
                    # update id
                    output_metadata[PROP_SAVED_WITH] = meta_saved_with

                # cannot set root itself:
                metadata = self.metadata(name)
                for key, val in output_metadata.items():
                    metadata.set(key, val)

                # generated metadata
                for key, val in callback_data["metadata_generated"].items():
                    metadata.set(key, val)

                metadata.set('$."$schema"', "TODO")  # need to quote

            return handle_

        def check_names_exist(**names):
            return all(self.has(name) for name in names.values())

        wrapped_output_handlers = {
            name: wrap_output_handler(name, conv)
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

        task = Task(
            function,
            output_writers=wrapped_output_handlers,
            input_readers=wrapped_input_handlers,
            get_job_hashsum=get_job_hashsum,
            check_done=check_names_exist if skip_finished else None,
        )
        callback_data["task"] = task
        return task
