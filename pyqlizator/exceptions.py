

class Error(Exception):

    def __init__(self, code, message, details=None, original_exception=None):
        self.code = code
        self.message = message
        self.details = details
        self.original_exception = original_exception
        msg = '[{}] {}: {}'.format(code, message, details)
        super(Error, self).__init__(msg)
