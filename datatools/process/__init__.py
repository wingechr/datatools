from dataclasses import dataclass
from typing import Any, Callable, Union, cast

__all__ = ["Process", "ProcessException"]

RoleType = Union[None, int, str]

Inputs = dict[RoleType, Callable]
Outputs = dict[RoleType, Callable]


@dataclass(frozen=True)
class Function:
    def __call__(self, *args, **kwargs) -> Any:
        pass


@dataclass(frozen=True)
class Input:
    def __call__(self) -> Any:
        pass


@dataclass(frozen=True)
class Output:
    def __call__(self, data: Any) -> None:
        pass


class ProcessException(Exception):
    def __call__(self) -> Any:
        pass


Parameter = Union[Input, Output]

RoleTypeDict = dict[RoleType, Any]
RoleTypeAny = Union[
    None, Any, list[Any], RoleTypeDict, tuple[list[Any], dict[str, Any]]
]


def any_to_dict(
    items: RoleTypeAny,
) -> dict[RoleType, Any]:
    if items is None:
        return {}
    elif isinstance(items, tuple):
        args, kwargs = items
        return dict(list(enumerate(args)) + list(kwargs.items()))
    elif isinstance(items, list):
        return dict(enumerate(items))
    elif isinstance(items, dict):
        return items
    else:
        return {None: items}


def get_args_kwargs_from_dict(
    data: dict[RoleType, Any],
) -> tuple[list[Any], dict[str, Any]]:
    args_d = {}
    kwargs = {}
    if None in data:  # primitive: must be the only one
        args = [data[None]]
    else:
        for k, v in data.items():
            if isinstance(k, int):
                args_d[k] = v
            elif isinstance(k, str):
                kwargs[k] = v
            else:
                raise TypeError(k)
        if args_d:
            # fill missing positionals with None
            args = [args_d.get(i, None) for i in range(max(args_d) + 1)]
        else:
            args = []

    return args, kwargs


@dataclass(frozen=True)
class Process:
    function: Callable
    inputs: RoleTypeAny = None
    outputs: RoleTypeAny = None

    def __post_init__(self) -> None:
        # must use __setattr__ because we use frozen=True
        object.__setattr__(self, "inputs", any_to_dict(self.inputs))
        object.__setattr__(self, "outputs", any_to_dict(self.outputs))

    def __call__(self) -> None:
        """Run the process."""

        # read input values
        data = {
            idx: read_input()
            for idx, read_input in cast(dict[RoleType, Callable], self.inputs).items()
        }

        # map to function arguments
        args, kwargs = get_args_kwargs_from_dict(data)

        # call original functions
        result = self.function(*args, **kwargs)

        # map to outputs and save
        for idx, write_output in cast(dict[RoleType, Callable], self.outputs).items():
            value = result if idx is None else result[idx]
            write_output(value)
