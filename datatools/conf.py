from contextlib import ExitStack

import appdirs

from datatools import __app_name__

exit_stack = ExitStack()

global_cache_dir = (
    appdirs.user_data_dir(__app_name__, appauthor=None, version=None, roaming=False)
    + "/data"
)

cache_dir = global_cache_dir
