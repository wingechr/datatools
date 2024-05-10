import abc
from typing import Type


class RegistryAbstractMetaBase(abc.ABCMeta):
    def __init__(cls, name, bases, dct):
        registry = cls._subclasses  # must exist in base class
        if name in registry:
            raise KeyError(f"class name already registered: {name}")
        registry[name] = cls
        super().__init__(name, bases, dct)


class RegistryAbstractBase(abc.ABC, metaclass=RegistryAbstractMetaBase):
    _subclasses = {}

    @classmethod
    def _is_class_for(cls, **kwargs) -> bool:
        return False

    @classmethod
    def _get_class(cls, **kwargs) -> Type:
        for subclass in reversed(list(cls._subclasses.values())):
            if subclass._is_class_for(**kwargs):
                return subclass
        raise NotImplementedError(f"{kwargs} {cls}")
