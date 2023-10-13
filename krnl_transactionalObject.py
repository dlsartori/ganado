from krnl_config import tables_and_binding_objects, tables_and_methods
from os import path
import functools
from collections import deque
from custom_types import DataTable, getRecords
# from krnl_abstract_class_activity import Activity

def moduleName():
    return str(path.basename(__file__))

# def singleton(cls):         # Wrapper class for singleton classes -> All activities are singletons.
#     """Make a class a Singleton class (only one instance)"""
#     @functools.wraps(cls)
#     def wrapper_singleton(*args, **kwargs):
#         if not wrapper_singleton.instance:
#             wrapper_singleton.instance = cls(*args, **kwargs)
#         return wrapper_singleton.instance
#     wrapper_singleton.instance = None
#     return wrapper_singleton


class TransactionalObject(object):
    __objClass = 100
    __objType = 2

    # __replication_deque = deque()  # deque to be used as a circular queue of classes. Used in IntervalTimer funcs.

    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, '_initialize'):
            cls._initialize()               # Inicializa clases que definan metodo _initialize()
        super().__init_subclass__()

        if cls.__name__ in tables_and_binding_objects.values():
            # 1. Replaces Object_Name with object (ex: 'Animal' with <class Animal>) in tables_and_binding_objects dict.
            k = next((k for k in tables_and_binding_objects if tables_and_binding_objects[k] == cls.__name__), None)
            tables_and_binding_objects[k] = cls
            # 2. Gets method/function from Method_Name via getattr()
            if k in tables_and_methods:
                tables_and_methods[k] = getattr(cls, tables_and_methods[k]) or None

        # appends cls of ONLY the Child classes by checking if cls is in __bases__. This way, appends Bovine, Caprine,
        # etc., and REMOVES Animal. This is in order to avoid the execution of processReplicated() by Parent Classes.
        # if hasattr(cls, 'processReplicated'):
        #     TransactionalObject.__replication_deque.append(cls)
        #     # krnl_logger.info(f'^^^^^^^^^^^^^^ deque: now adding: {cls}. mro: {cls.__mro__}   ^^^^^^^^^^^^^^^^')
        #     for c in cls.__bases__:  # cls.__bases__ is tuple.
        #         if c in TransactionalObject.__replication_deque:
        #             TransactionalObject.__replication_deque.remove(c)  # Removes Parent Class(es), keeps Child classes

    # @staticmethod
    # def get_classes_deque():
    #     return TransactionalObject.__replication_deque


    def __init__(self):
        pass

    temp = getRecords('tblObjetosTiposDeTransferenciaDePropiedad', '', '', None, 'fldID', 'fldName')
    __ownershipXferDict = {}
    if isinstance(temp, DataTable) and temp.dataLen > 0 and len(temp.dataList[0]) > 0:
        for j in range(temp.dataLen):
            __ownershipXferDict[temp.dataList[j][1]] = str(temp.dataList[j][0])  # {NombreTransfProp: ID_TransfProp, }

    @property
    def ownershipXferDict(self):
        return self.__ownershipXferDict

        # Diccionario Geo.Entidades. Diccionario de Diccionarios con definiciones de las Localizaciones en Sistema
    temp = getRecords('tblGeoEntidades', '', '', None, '*')
    if isinstance(temp, DataTable) and temp.dataLen and temp.dataList[0]:
        __localizationsDict = {}  # {statusName: [statusID, activeYN]}
        for j in range(temp.dataLen):
            rowDict = temp.unpackItem(j)
            key = rowDict['fldID']
            rowDict.pop('fldID')
            __localizationsDict[key] = rowDict
    else:
        raise ImportError(f' ERR_Sys_DBCannotRead table tblGeoEntidades. Exiting...')


    @property
    def localizationsDict(self):
        return TransactionalObject.__localizationsDict

# transactObj = TransactionalObject()

