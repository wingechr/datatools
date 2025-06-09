from io import BytesIO, TextIOWrapper
from typing import Any, Callable, Union, get_args

import pandas as pd

RoleType = Union[str, int, None]


def csv_to_df(bdata: bytes, encoding="utf-8", index=None, **kwargs) -> pd.DataFrame:
    buf = TextIOWrapper(BytesIO(bdata), encoding=encoding)
    df = pd.read_csv(buf, **kwargs)
    # TODO index from metadata
    index = "key"
    if index:
        df = df.set_index(index)
    return df


def df_to_csv(df: pd.DataFrame, encoding="utf-8", index=False, **kwargs) -> bytes:
    buf = BytesIO()
    # TODO index from metadata
    index = True
    df.to_csv(buf, index=index, encoding=encoding, **kwargs)
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


def infer_converter(from_type: Union[str, type], to_type: Union[str, type]) -> Callable:
    if from_type == bytes and isinstance(to_type, str):
        return lambda x: x
    elif (from_type, to_type) == ("csv", pd.DataFrame):
        return csv_to_df
    elif (from_type, to_type) == (pd.DataFrame, "csv"):
        return df_to_csv
    else:
        raise NotImplementedError((from_type, to_type))


def get_default_filetype(dtype):
    if dtype == pd.DataFrame:
        return "csv"
    return "pickle"


def infer_loader(uri: str) -> Callable:
    def dummy_loader(uri: str) -> bytes:
        path = uri.replace("file://", "")
        with open(path, "rb") as file:
            return file.read()

    return dummy_loader
