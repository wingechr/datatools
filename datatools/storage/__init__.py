import json
from dataclasses import dataclass
from functools import cache, cached_property
from io import IOBase
from pathlib import Path
from typing import Any


class StorageException(Exception):
    pass


ResourcePath = str
MetadataKey = str
MetadataValue = Any


@dataclass(frozen=True)
class AbstractStorage:

    location: str

    def resource(self, path: ResourcePath) -> "Resource": ...

    def contains(self, path: ResourcePath) -> bool: ...

    def open(self, path: ResourcePath) -> IOBase: ...

    def delete(self, path: ResourcePath, delete_metadata: bool = False) -> None: ...

    def write(self, path: ResourcePath, data: IOBase) -> None: ...

    def metadata_set(
        self, path: ResourcePath, key: MetadataKey, value: MetadataValue
    ) -> None: ...

    def metadata_get(self, path: ResourcePath, key: MetadataKey) -> MetadataValue: ...


class Storage(AbstractStorage):
    """
    dict like path -> Resource
    """

    location: str

    @cache
    def resource(self, path: ResourcePath) -> "Resource":
        # TODO: validate path?
        return Resource(self, path)

    def contains(self, path: ResourcePath) -> bool:
        return self.__get_filepath(path).exists()

    def open(self, path: ResourcePath) -> IOBase:
        return open(self.__get_filepath(path), "rb")

    def write(self, path: ResourcePath, data: IOBase) -> None:
        filepath = self.__get_filepath(path)
        with self.__open_write(filepath) as file:
            file.write(data.read())

    def delete(self, path: ResourcePath, delete_metadata: bool = False) -> None:
        filepath = self.__get_filepath(path)
        filepath.unlink()
        if delete_metadata:
            filepath_meta = self.__get_filepath_metadata(path)
            filepath_meta.unlink()

    def metadata_set(
        self, path: ResourcePath, key: MetadataKey, value: MetadataValue
    ) -> None:
        filepath_meta = self.__get_filepath_metadata(path)
        metadata = self.__metadata_read(filepath_meta)
        metadata[key] = value
        data = json.dumps(metadata, indent=2, ensure_ascii=False).encode()
        with self.__open_write(filepath_meta) as file:
            file.write(data)

    def metadata_get(self, path: ResourcePath, key: MetadataKey) -> MetadataValue:
        filepath_meta = self.__get_filepath_metadata(path)
        metadata = self.__metadata_read(filepath_meta)
        return metadata.get(key)

    @cache
    def __get_filepath(self, path: ResourcePath) -> Path:
        # TODO: ensure its a proper path inside of storage locacion
        # folders in path must not contain "."
        # file must contain "." and suffix
        return Path(self.location) / path

    @cache
    def __get_filepath_metadata(self, path: ResourcePath) -> Path:
        filepath = self.__get_filepath(path)
        return filepath.parent / f"{filepath.name}.metadata.json"

    def __open_write(self, filepath: Path) -> IOBase:
        filepath.parent.mkdir(exist_ok=True, parents=True)
        return open(filepath, "wb")

    def __metadata_read(self, filepath_meta: Path) -> dict:
        if not filepath_meta.exists():
            return {}
        with filepath_meta.open(encoding="utf-8") as file:
            return json.load(file)


@dataclass(frozen=True)
class Resource:
    storage: "Storage"
    path: ResourcePath

    def exist(self) -> bool:
        return self.storage.contains(self.path)

    def open(self) -> IOBase:
        return self.storage.open(self.path)

    def write(self, data: IOBase) -> None:
        return self.storage.write(self.path, data)

    def delete(self, delete_metadata: bool = False) -> None:
        return self.storage.delete(self.path, delete_metadata=delete_metadata)

    @cached_property
    def metadata(self) -> "Metadata":
        return Metadata(self)


@dataclass(frozen=True)
class Metadata:
    resource: Resource

    def get(self, key: MetadataKey) -> MetadataValue:
        return self.resource.storage.metadata_get(self.resource.path, key)

    def set(self, key: MetadataKey, value: MetadataValue) -> None:
        return self.resource.storage.metadata_set(self.resource.path, key, value)


__all__ = ["Storage", "Resource"]
