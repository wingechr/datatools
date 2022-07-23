import logging
import traceback


def show_trace(func):
    def decorator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logging.error(traceback.format_exc())
            raise

    return decorator
