class DatatoolsException(Exception):
    pass


class ObjectNotFoundException(DatatoolsException):
    pass


class InvalidValueException(DatatoolsException):
    pass


class IntegrityException(DatatoolsException):
    pass


class DuplicateKeyException(DatatoolsException):
    pass


class ValidationException(DatatoolsException):
    pass
