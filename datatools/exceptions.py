class ValidationException(Exception):
    pass


class DuplicateKeyException(ValidationException):
    pass
