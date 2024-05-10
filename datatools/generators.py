import abc
import re
from typing import Any, Callable, Dict, Tuple, Type

from datatools.classes import RegistryAbstractBase


class AbstractDataGenerator(RegistryAbstractBase):
    _subclasses = {}  # overwrite from BaseClass
    create_kwargs = []

    @classmethod
    def _is_class_for(cls, data_source: Any) -> bool:
        return False

    @classmethod
    def get_instance(cls, data_source: Any) -> "AbstractDataGenerator":
        subclass = cls._get_class(data_source=data_source)
        return subclass(data_source=data_source)

    def __init__(self, data_source: Any) -> None:
        self._data_source = data_source

    @abc.abstractmethod
    def create_name(self) -> str:
        ...

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        # defaults, only dependent on name (suffix)
        if re.match(r"^.*\.json$", name):
            return ("application/json", object)
        # default: binary
        return ("application/octet-stream", bytes)

    @abc.abstractmethod
    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        ...


class FunctionDataGenerator(AbstractDataGenerator):
    @classmethod
    def _is_class_for(cls, data_source: Any) -> bool:
        return isinstance(data_source, Callable)

    def create_name(self) -> str:
        # return fnuction name
        function = self._data_source
        return function.__name__

    def get_media_data_type(self, name: str) -> Tuple[str, Type]:
        return "text/plain", str

    def create_data_metadata(self, **kwargs) -> Tuple[Any, Dict[str, Any]]:
        data = self._data_source()
        metadata = {}
        return data, metadata
