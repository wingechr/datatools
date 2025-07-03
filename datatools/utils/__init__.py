from functools import cache as _cache
from functools import update_wrapper


def cache(func):
    """Update cache decorator that preserves metadata"""
    cached_func = _cache(func)
    return update_wrapper(cached_func, func)
