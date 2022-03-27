"""
* convert/fix and/or validate values in fields
* log changes to data
* optionally continue on validation failure to collect all problems in a dataset
* convert unique constraints in dataset
"""

from collections import OrderedDict

from .exceptions import ValidationException
from .factories import ValColumn, ValExcessFields, ValSql, ValStr, ValUnique


def validate(data, validator_classes, fail_fast=False):

    validators = OrderedDict((name, c()) for name, c in validator_classes.items())
    errors = {}

    # apply validators to each row
    for index, row in enumerate(data):
        for name, validate_row in validators.items():
            try:
                validate_row(row)
            except ValidationException as exc:
                key = (name, type(exc))
                if key not in errors:
                    errors[key] = []
                errors[key].append((index, str(exc)))
                if fail_fast:
                    raise Exception(errors)

    # return errors
    return errors
