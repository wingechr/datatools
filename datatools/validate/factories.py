"""
* convert/fix and/or validate values in fields
* log changes to data
* optionally continue on validation failure to collect all problems in a dataset
* convert unique constraints in dataset
"""


import datetime
import re

from ..convert import convert as _convert
from .exceptions import (
    ConversionException,
    NullableException,
    ValidationException,
    ValidationNotImplementedError,
)
from .utils import parse_sql_type


def convert(val, to_type):
    if val is None:
        raise NullableException()
    try:
        return _convert(val, to_type)
    except NotImplementedError as exc:
        raise ValidationNotImplementedError(exc)
    except Exception as exc:
        raise ConversionException(exc)


def ValColumn(field, value_validator, val_null=None):
    """
    Args:
        field(str): name of field
        value_validator(callable): val -> val | Exception
        val_null(iterable, optional): values that are allowed as None
    """

    vals_null = set(val_null) if val_null else set()

    def validator():
        def call(row):
            # update context, row

            val = row.get(field)

            if val in vals_null:
                row[field] = None
            else:
                row[field] = value_validator(val)

        return call

    return validator


def ValExcessFields(fields):
    fields_set = set(fields)

    def validator():
        def call(row):
            row_set = set(row)
            excess = row_set - fields_set
            if excess:
                raise ValidationException(excess)

        return call

    return validator


def ValUnique(fields):
    def get_key(row):
        return tuple(row[f] for f in fields)

    def validator():

        index = set()

        def call(row):
            key = get_key(row)
            if key in index:
                raise ValidationException("Duplicate Key: %s" % (key,))
            else:
                index.add(key)

        return call

    return validator


def ValDict(field, val_map, val_null=None):
    def value_validator(val):
        return val_map[val]

    return ValColumn(field, value_validator, val_null=val_null)


def ValRegexp(field, pattern, val_null=None):
    pat = re.compile(pattern)

    def value_validator(val):
        assert pat.match(val)
        return val

    return ValColumn(field, value_validator, val_null=val_null)


def ValEnum(field, vals, val_null=None):
    val_map = dict((v, v) for v in vals)
    return ValDict(field, val_map, val_null=val_null)


def ValSql(field, sql_type, val_null=None):
    type_name, type_args = parse_sql_type(sql_type)

    if type_name == "CHAR":
        length = type_args[0]

        def value_validator(val):
            val = convert(val, str)
            if len(val) != length:
                raise ValidationException("len(%s): %d != %d" % (val, len(val), length))
            return val

    elif type_name == "VARCHAR":
        max_length = type_args[0]

        def value_validator(val):
            val = convert(val, str)
            if len(val) > max_length:
                raise ValidationException(
                    "len(%s): %d > %d" % (val, len(val), max_length)
                )
            return val

    elif type_name == "BIT":

        def value_validator(val):
            val = convert(val, bool)
            return val

    elif type_name == "FLOAT":

        def value_validator(val):
            val = convert(val, float)
            return val

    elif type_name == "TINYINT":
        min_val, max_val = 0, 255

        def value_validator(val):
            val = convert(val, int)
            if not min_val <= val <= max_val:
                raise ValidationException("%d <= %d <= %d" % min_val, val, max_val)
            return val

    elif type_name == "SMALLINT":
        min_val, max_val = -32768, 32767

        def value_validator(val):
            val = convert(val, int)
            if not min_val <= val <= max_val:
                raise ValidationException("%d <= %d <= %d" % min_val, val, max_val)
            return val

    elif type_name == "INT":
        min_val, max_val = -2147483648, 2147483647

        def value_validator(val):
            val = convert(val, int)
            if not min_val <= val <= max_val:
                raise ValidationException("%d <= %d <= %d" % min_val, val, max_val)
            return val

    elif type_name == "BIGINT":

        def value_validator(val):
            val = convert(val, int)
            return val

    elif type_name == "DATE":

        def value_validator(val):
            val = convert(val, datetime.date)
            return val

    elif type_name == "TIME":

        def value_validator(val):
            val = convert(val, datetime.time)
            return val

    elif type_name == "DATETIME":

        def value_validator(val):
            val = convert(val, datetime.datetime)
            return val

    else:
        raise NotImplementedError(type_name)

    return ValColumn(field, value_validator, val_null=val_null)
