from functools import cache as _cache
from functools import update_wrapper
from typing import Callable


def cache(func: Callable) -> Callable:
    """Update cache decorator that preserves metadata.

    Parameters
    ----------
    func : Callable
        original function

    Returns
    -------
    Callable
        cache decorated function
    """

    cached_func = _cache(func)
    return update_wrapper(cached_func, func)
