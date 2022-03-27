"""
* convert/fix and/or validate values in fields
* log changes to data
* optionally continue on validation failure to collect all problems in a dataset
* convert unique constraints in dataset
"""
from .exceptions import ValidationException
from .factories import ValExcessFields, ValRegexp, ValSql, ValUnique


def validate(data, validator_classes):

    validators = [c() for c in validator_classes]
    errors = {}

    # apply validators to each row
    for index, row in enumerate(data):
        for validate_row in validators:
            try:
                validate_row(row)
            except ValidationException as exc:
                if index not in errors:
                    errors[index] = []
                errors[index].append(str(exc))

    # return errors
    return errors
