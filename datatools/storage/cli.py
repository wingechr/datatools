"""TODO"""

from collections.abc import Callable, Iterable
import logging
from pathlib import Path
from typing import Any

from click.testing import CliRunner
from typing_extensions import override

from datatools.exceptions import (
    SubprocessStatus,
)
from datatools.storage.base import DataStorage, MetadataStorage
from datatools.types import MetadataAttribute, MetadataValue, Name
from datatools.utils import (
    as_bytes,
    json_dumps,
    json_loadb,
    reverse_prints,
    try_parse_json_str,
)


class TestCliMetadataDataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, name: Name, request: Callable[..., Iterable[bytes]]):
        self._name = name
        self._request = request

    @override
    def get(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        bytes_iterable = self._request("metadata", "get", self._name, str(attribute))
        sdata = "\n".join(reverse_prints(bytes_iterable))
        return try_parse_json_str(sdata)

    @override
    def set(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._request(
            "metadata",
            "set",
            self._name,
            f"{attribute}={json_dumps(value)}",
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

    def _request(
        self, *args: str, data: Iterable[bytes] | None = None
    ) -> Iterable[bytes]:
        cmd = ["-l", str(self._location)] + list(args)
        logging.debug("CLI " + " ".join(cmd))

        # NOTE: this is really only for testing
        # making it also streaming/chunked requires to set up separate threads
        # and pathing into sys.stdout buffers - so it's not worth it
        bdata = as_bytes(data) if data else None
        result = self._clirunner.invoke(self._storage_main_cli, cmd, input=bdata)
        exit_code = result.exit_code
        stdout_bytes = result.stdout_bytes

        if exit_code:
            raise SubprocessStatus(exit_code)

        # IMPORTANT: do not use yield,
        # otherwise calls that dont have/consume output will not
        # execute the function
        return [stdout_bytes]

    def _has(self, name: Name) -> bool:
        try:
            self._request("has", name)
        except SubprocessStatus:
            return False
        return True

    def _read(self, name: Name) -> Iterable[bytes]:
        yield from self._request("read", name)

    def _write(self, name: Name, data: Iterable[bytes]) -> None:
        self._request("write", name, data=data)

    def _delete(self, name: Name) -> None:
        self._request("delete", name)

    def _metadata(self, name: Name) -> MetadataStorage:
        return TestCliMetadataDataStorage(name, self._request)

    @override
    def find(self, **filters: MetadataValue) -> Iterable[Name]:
        filters_str = [f"{k}={v}" for k, v in filters.items()]
        data = self._request("find", *filters_str)
        return reverse_prints(data)

    def _list(self) -> Iterable[Name]:
        raise NotImplementedError()  # we implement find # pragma: no coverage

    def info(self) -> dict:
        """TODO"""
        info_remote = self._request("info")

        bdata = as_bytes(info_remote)  # i dont think there is a point in streaming this
        info_remote = json_loadb(bdata)
        info_client = super().info()
        info_client.update({"remote": info_remote})

        return info_client
