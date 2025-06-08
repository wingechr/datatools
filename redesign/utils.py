from typing import Any, Union, get_args
from io import BytesIO, TextIOWrapper
import pandas as pd

RoleType = Union[str, int, None]


def csv_to_df(
    data: bytes, encoding="utf-8", index=None, **read_csv_kwargs
) -> pd.DataFrame:
    buf = TextIOWrapper(BytesIO(data), encoding=encoding)
    df = pd.read_csv(buf, **read_csv_kwargs)
    if index:
        df = df.set_index(index)
    return df


def df_to_csv(
    data: pd.DataFrame, encoding="utf-8", index=False, **to_csv_kwargs
) -> bytes:
    buf = BytesIO()
    data.to_csv(buf, index=index, encoding=encoding, **to_csv_kwargs)
    buf.seek(0)
    data_bytes = buf.read()
    return data_bytes


def get_value_type(dtype: type) -> type:
    # dict[Any, int] -> int
    # list[int] -> int
    return get_args(dtype)[-1]


def is_uri(s: str) -> bool:
    return ":" in s


def is_data_uri(s: str) -> bool:
    return s.startswith("data://")


def is_path(s: str) -> bool:
    return "\\" in s or "/" in s


def dict_add(dct: dict, key: Any, value: Any) -> None:
    assert key not in dct
    dct[key] = value


def split_args_kwargs(
    data: dict[RoleType, Any], assert_args_range: bool = True
) -> tuple[tuple[Any], dict[str, Any]]:
    args_d = {}
    kwargs = {}
    if None in data:  # primitive
        assert set(data) == {None}
        args = [data[None]]
    else:
        for k, v in data.items():
            if isinstance(k, int):
                args_d[k] = v
            elif isinstance(k, str):
                kwargs[k] = v
            else:
                raise TypeError(k)
        if assert_args_range:
            assert set(args_d) == set(range(len(args_d)))
        # fill missing positionals with None
        max_idx = max(args_d) + 1 if args_d else 0
        args = [args_d[i] for i in range(max_idx)]
    return args, kwargs


def get_type_name(cls: type) -> str:
    if cls is None:
        return "Any"
    return f"{cls.__module__}.{cls.__qualname__}"


def as_io_dict(x) -> dict:
    if x is None:
        return {}
    if not isinstance(x, dict):
        return {None: x}
    return x


def get_default_filetype(dtype):
    return "csv"
