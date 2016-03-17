

class Error(Exception):

    def __init__(self, code, message, original_exception=None):
        self.code = code
        self.message = message
        self.original_exception = original_exception
        msg = '[{}] {}'.format(code, message)
        super(Error, self).__init__(msg)

