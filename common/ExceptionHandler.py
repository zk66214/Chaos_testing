from common.Log import *

class ExceptionHandler():
    @staticmethod
    def raise_up(message):
        _log = MyLog()
        _log.error(message)
        raise Exception(message)