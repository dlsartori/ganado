# from krnl_cfg_items import *


class UserBaseException(Exception):          # Abstract Base Class
    def __init__(self, msg: str):
        super().__init__(msg)


class DBErrorException(UserBaseException):       # Abstract Base Class
    def __init__(self, msg: str):
        super().__init__(msg)


class DBReadError(DBErrorException):         # Concrete Class
    def __init__(self, msg=''):
        self.__msg = msg
        super().__init__(msg)


class DBWriteError(DBErrorException):        # Concrete Class
    def __init__(self, msg=''):
        self.__msg = msg
        super().__init__(msg)


class DBAccessError(DBErrorException):         # Concrete Class
    def __init__(self, msg=''):
        self.__msg = msg
        super().__init__(msg)
