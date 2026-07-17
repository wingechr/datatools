"""TODO"""

from abc import abstractmethod
from collections.abc import Callable
import datetime
from io import BytesIO, TextIOWrapper
import json
import math
from typing import TYPE_CHECKING, Any, BinaryIO

import numpy as np
import pandas as pd
from pydantic import BaseModel

from datatools.types import (
    DATE_FMT,
    DATETIMETZ_FMT,
    DEFAULT_ENCODING,
    ENCODING_ERROOR,
    TIME_FMT,
    Json,
    SubCls,
)

if TYPE_CHECKING:
    from pandas._typing import JsonFrameOrient


class PersistentBytesIO(BytesIO):
    """A BytesIO that keeps its final content accessible after close()."""

    def __init__(self, data: bytes = b"", *args, **kwargs):
        self.data = data
        super().__init__(data, *args, **kwargs)

    def write(self, data: bytes) -> int:
        """TODO"""
        self.data += data
        return super().write(data)


def isna(x: Any) -> bool:
    """TODO

    Example:

    >>> import pandas as pd
    >>> import numpy as np
    >>> isna(0)
    False
    >>> isna("")
    False
    >>> isna(float("nan"))
    True
    >>> isna(float("inf"))
    True
    >>> isna(None)
    True
    >>> isna(pd.NA)
    True
    >>> isna(np.nan)
    True

    """
    return bool(
        x is None
        or isinstance(x, float)
        and (math.isnan(x) or math.isinf(x))
        or pd.isna(x)
    )


def json_serialize(x: Any) -> Json:
    """TODO

    Example:

    >>> import json
    >>> import numpy as np
    >>> import datetime
    >>> from pydantic import BaseModel
    >>> json.dumps(datetime.date(2001,2,3), default=json_serialize)
    '"2001-02-03"'
    >>> json.dumps(datetime.time(4,5,6), default=json_serialize)
    '"04:05:06"'
    >>> json.dumps(datetime.datetime(2001,2,3,4,5,6), default=json_serialize)
    '"2001-02-03T04:05:06"'
    >>> json.dumps(np.nan, allow_nan=True, default=json_serialize)
    'NaN'
    >>> repr(json_serialize(np.nan))
    'None'
    >>> json.dumps(float('nan'), allow_nan=True, default=json_serialize)
    'NaN'
    >>> repr(json_serialize(float('nan')))
    'None'
    >>> json.dumps(np.int64(0), default=json_serialize)
    '0'
    >>> json_serialize(np.float64(0.5))
    0.5
    >>> json.dumps(np.bool(0), default=json_serialize)
    'false'
    >>> json.dumps(object(), default=json_serialize)
    Traceback (most recent call last):
    ...
    NotImplementedError:
    >>> class Test(BaseModel):
    ...    value: int
    >>> json.dumps(Test(value='10'), default=json_serialize)
    '{"value": 10}'


    """
    if isinstance(x, datetime.datetime):
        return x.strftime(DATETIMETZ_FMT)
    elif isinstance(x, datetime.date):
        return x.strftime(DATE_FMT)
    elif isinstance(x, datetime.time):
        return x.strftime(TIME_FMT)
    elif isna(x):
        # FIXME: when using json.dumps(default=json_serialize), nan will NOT
        # be forwarded to this function
        return None
    elif isinstance(x, np.bool_):
        return bool(x)
    elif np.issubdtype(type(x), np.integer):
        return int(x)
    elif np.issubdtype(type(x), np.floating):
        return float(x)
    elif isinstance(x, BaseModel):
        return x.model_dump(mode="json")
    else:
        raise NotImplementedError(f"{x.__class__}: {x}")


class IO:
    """TODO"""

    @classmethod
    def with_conf(cls: type[SubCls], **class_attrs) -> type[SubCls]:
        """TODO"""
        return type(cls.__name__, (cls,), class_attrs)  # type:ignore

    @classmethod
    @abstractmethod
    def load(cls, buf: BytesIO) -> Any: ...

    @classmethod
    @abstractmethod
    def dump(cls, data: Any, buf: BytesIO) -> None: ...

    @classmethod
    def dumpb(cls, data: Any) -> bytes:
        """TODO"""
        buf = PersistentBytesIO()
        cls.dump(data, buf)
        return buf.data

    @classmethod
    def loadb(cls, data: bytes) -> Any:
        """TODO"""
        return cls.load(BytesIO(data))


class StrIO(IO):
    r"""TODO

    Example:

    >>> StrIO.load(PersistentBytesIO('äöü'.encode(encoding='utf-8')))
    'äöü'
    >>> buf = PersistentBytesIO('äöü'.encode(encoding='windows-1252'))
    >>> buf.data
    b'\xe4\xf6\xfc'
    >>> StrIO.with_conf(encoding='windows-1252').load(PersistentBytesIO(buf.data))
    'äöü'

    >>> buf = PersistentBytesIO()
    >>> s = StrIO.with_conf(encoding='windows-1252')
    >>> s.dump('äöü', buf)
    >>> buf.data
    b'\xe4\xf6\xfc'
    >>> s.load(PersistentBytesIO(buf.data))
    'äöü'

    """

    encoding: str = DEFAULT_ENCODING
    encoding_errors: ENCODING_ERROOR = "strict"
    newline: str | None = ""

    @classmethod
    def load(cls, buf: BinaryIO) -> str:
        """TODO"""
        sdata = cls._read_text_buffer(buf).read()
        return sdata

    @classmethod
    def dump(cls, data: str, buf: BinaryIO) -> None:
        """TODO"""
        cls._write_text_buffer(buf).write(data)

    @classmethod
    def _read_text_buffer(
        cls,
        byte_buf: BinaryIO,
    ) -> TextIOWrapper:
        """Wrap a byte buffer for reading, decoding bytes -> str."""
        return TextIOWrapper(
            byte_buf,
            encoding=cls.encoding,
            errors=cls.encoding_errors,
            newline=cls.newline,
        )

    @classmethod
    def _write_text_buffer(
        cls,
        byte_buf: BinaryIO,
    ) -> TextIOWrapper:
        """Wrap a byte buffer for writing, encoding str -> bytes."""
        return TextIOWrapper(
            byte_buf,
            encoding=cls.encoding,
            errors=cls.encoding_errors,
            newline=cls.newline,
            write_through=True,
        )


class JsonIO(StrIO):
    r"""TODO

    Example:

    >>> buf = PersistentBytesIO()
    >>> j = JsonIO.with_conf(indent=1, encoding='windows-1252')
    >>> j.dump(['täst', 1], buf)
    >>> buf.data
    b'[\n "t\xe4st",\n 1\n]'
    >>> j.load(PersistentBytesIO(buf.data))
    ['täst', 1]

    """

    ensure_ascii: bool = False
    sort_keys: bool = False
    indent: int = 2
    default: Callable = json_serialize

    @classmethod
    def load(cls, buf: BinaryIO) -> Json:
        """TODO"""
        return json.load(cls._read_text_buffer(buf))

    @classmethod
    def dump(cls, data: Json, buf: BinaryIO) -> None:
        """TODO"""
        json.dump(
            data,
            cls._write_text_buffer(buf),
            ensure_ascii=cls.ensure_ascii,
            sort_keys=cls.sort_keys,
            indent=cls.indent,
            default=json_serialize,
        )

    @classmethod
    def dumps(cls, data: Any) -> str:
        """TODO"""
        return json.dumps(
            data,
            ensure_ascii=cls.ensure_ascii,
            sort_keys=cls.sort_keys,
            indent=cls.indent,
            default=json_serialize,
        )

    @classmethod
    def loads(cls, data: str) -> Any:
        """TODO"""
        return json.loads(data)


class DataFrameJsonIO(JsonIO):
    r"""TODO

    Example:

    >>> import pandas as pd
    >>> df = pd.DataFrame([{"a": 1, "b": 2}, {"a": 2}])
    >>> buf = PersistentBytesIO()
    >>> DataFrameJsonIO.with_conf(indent=0,orient="records").dump(df, buf)
    >>> buf.data
    b'[{"a":1,"b":2.0},{"a":2,"b":null}]'

    """

    orient: "JsonFrameOrient" = "table"

    @classmethod
    def load(cls, buf: BinaryIO) -> pd.DataFrame:
        """TODO"""
        return pd.read_json(
            cls._read_text_buffer(buf),
            orient=cls.orient,
            encoding=cls.encoding,
            encoding_errors=cls.encoding_errors,
        )

    @classmethod
    def dump(cls, data: pd.DataFrame, buf: BinaryIO) -> None:
        """TODO"""
        data.to_json(
            cls._write_text_buffer(buf),
            orient=cls.orient,
            indent=cls.indent,
            # TODO: what about encoding etc
        )


class DataFrameCsvIO(StrIO):
    r"""TODO

    >>> import pandas as pd
    >>> df = pd.DataFrame([{"a": 1, "b": 2}, {"a": 2}]).rename_axis(index="idx")
    >>> buf = PersistentBytesIO()
    >>> DataFrameCsvIO.with_conf(indent=0,orient="records").dump(df, buf)
    >>> buf.data
    b'$idx,a,b\n0,1,2.0\n1,2,\n'

    """  # noqa:E501

    lineterminator: str | None = "\n"
    sep: str = ","

    @classmethod
    def load(cls, buf: BinaryIO) -> pd.DataFrame:
        """TODO"""
        df = pd.read_csv(cls._read_text_buffer(buf), sep=cls.sep)
        df = df.set_index([c for c in df.columns if c.startswith("$")])
        df = df.rename_axis(index=[str(c)[1:] for c in df.index.names])
        return df

    @classmethod
    def dump(cls, data: pd.DataFrame, buf: BinaryIO) -> None:
        """TODO"""
        df = data
        df = df.rename_axis(index=[f"${c}" for c in df.index.names])
        df.to_csv(
            cls._write_text_buffer(buf), lineterminator=cls.lineterminator, sep=cls.sep
        )
