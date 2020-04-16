# author: Dan Crankshaw, ucbrise lab, https://github.com/ucbrise/clipper

class HamsException(Exception):
    def __init__(self, msg, *args):
        self.msg = msg
        super(Exception, self).__init__(msg, *args)


class UnconnectedException(HamsException):
    def __init__(self, *args):
        message = (
        self.message = message
        super(UnconnectedException, self).__init__(message, *args)
