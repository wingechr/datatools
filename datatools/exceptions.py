import logging
import re


class DatatoolsException(Exception):
    pass


class NonzeroReturncode(DatatoolsException):
    pass


class DataExists(DatatoolsException):
    pass


class InvalidPath(DatatoolsException):
    pass


class DataDoesNotExists(DatatoolsException):
    pass


def raise_err(cls_name_and_msg: str):
    logging.error(cls_name_and_msg)
    try:
        cls_name, msg = re.match("^([^:]+): (.*)$", cls_name_and_msg).groups()
        err_cls = globals()[cls_name]
    except Exception:
        err_cls = Exception
        msg = cls_name_and_msg
    raise err_cls(msg)
