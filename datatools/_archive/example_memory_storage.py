import logging
from io import BytesIO, IOBase
from typing import Any, Dict


class TestMemoryStorage:
    def __init__(self):
        self.__data = {}
        self.__metadata = {}

    def _resource_delete(self, resource_name: str) -> None:
        logging.debug("Deleting %s", resource_name)
        del self.__data[resource_name]
        del self.__metadata[resource_name]

    def _resource_exists(self, resource_name: str) -> bool:
        return resource_name in self.__data

    def _metadata_update(self, resource_name: str, metadata: Dict[str, Any]) -> None:
        if resource_name not in self.__metadata:
            self.__metadata[resource_name] = {}
        metadata_res = self.__metadata[resource_name]
        for key, val in metadata.items():
            metadata_res[key] = val

    def _metadata_query(self, resource_name: str, key: str) -> Any:
        if resource_name not in self.__metadata:
            self.__metadata[resource_name] = {}
        metadata_res = self.__metadata[resource_name]
        return metadata_res.get(key)

    def _bytes_write(self, resource_name: str, byte_buffer: IOBase) -> None:
        logging.debug("Writing %s", resource_name)
        bdata = byte_buffer.read()
        self.__data[resource_name] = bdata

    def _bytes_open(self, resource_name: str) -> IOBase:
        logging.debug("Reading %s", resource_name)
        bdata = self.__data[resource_name]
        return BytesIO(bdata)
