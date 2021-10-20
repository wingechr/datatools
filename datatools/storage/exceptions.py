class ObjectNotFoundException(Exception):
    pass


class InvalidValueException(Exception):
    pass


def validate_file_id(file_id):
    if not isinstance(file_id, str) or len(file_id) != 32:
        raise InvalidValueException(file_id)
    return file_id


class IntegrityException(Exception):
    pass