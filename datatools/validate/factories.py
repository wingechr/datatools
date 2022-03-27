"""
* convert/fix and/or validate values in fields
* log changes to data
* optionally continue on validation failure to collect all problems in a dataset
* convert unique constraints in dataset
"""


import datetime
import re

from ..convert import convert
from .exceptions import ValidationException
from .utils import parse_sql_type


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
                try:
                    row[field] = value_validator(val)
                except Exception as exc:
                    raise ValidationException(str(exc))

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


def ValBool(
    field,
    val_true=(True, 1, "1", "true", "True", "TRUE"),
    val_false=(False, 0, "0", "false", "False", "FALSE"),
    val_null=None,
):
    val_map = {}
    for v in val_true:
        val_map[v] = True
    for v in val_false:
        val_map[v] = False
    return ValDict(field, val_map, val_null=val_null)


def ValSql(field, sql_type, val_null=None):
    type_name, type_args = parse_sql_type(sql_type)

    if type_name == "CHAR":
        length = type_args[0]

        def value_validator(val):
            val = convert(val, str)
            assert len(val) == length
            return val

    elif type_name == "VARCHAR":
        max_length = type_args[0]

        def value_validator(val):
            val = convert(val, str)
            assert len(val) <= max_length
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
            assert min_val <= val <= max_val
            return val

    elif type_name == "SMALLINT":
        min_val, max_val = -32768, 32767

        def value_validator(val):
            val = convert(val, int)
            assert min_val <= val <= max_val
            return val

    elif type_name == "INT":
        min_val, max_val = -2147483648, 2147483647

        def value_validator(val):
            val = convert(val, int)
            assert min_val <= val <= max_val
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
