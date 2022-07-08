import logging  # noqa

from .. import utils  # noqa


class FrozenMap:
    """immutable, ordered map of unique hashables mapping to variables"""

    __slots__ = ["__values", "__indices", "__keys"]

    def __init__(self, items):

        self.__indices = {}  # key -> idx
        self.__keys = []
        self.__values = []

        for key, val in items:
            self.__setitem(key, val)

    def __setitem(self, key, val):
        if key in self.__indices:  # ensure uniqueness
            raise KeyError(f"{key} not unique")
        idx = len(self.__indices)
        self.__indices[key] = idx
        self.__keys.append(key)
        self.__values.append(val)

    def __len__(self):
        return len(self.__keys)

    def __getitem__(self, key):
        idx = self.__indices[key]
        return self.__values[idx]

    def __contains__(self, key):
        return key in self.__indices

    def keys(self):
        return iter(self.__keys)

    def values(self):
        return iter(self.__values)

    def items(self):
        return zip(self.__keys, self.__values)

    def index(self, key):
        return self.__indices[key]


class UniqueMap(FrozenMap):
    """ordered map of unique hashables mapping to variables"""

    def __setitem__(self, key, val):
        return self.__setitem(key, val)
