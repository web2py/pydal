# -*- coding: utf-8 -*-


class NotFoundException(Exception):
    pass


class NotAuthorizedException(Exception):
    pass


class NotOnNOSQLError(NotImplementedError):
    def __init__(self, message=None):
        if message is None:
            message = "Not Supported on NoSQL databases"
        super(NotOnNOSQLError, self).__init__(message)
