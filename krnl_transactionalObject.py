from os import path
import functools
from collections import deque
from krnl_custom_types import DataTable, getRecords, DBTrigger
from krnl_db_access import _create_db_trigger
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

    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, '_initialize'):
            cls._initialize()               # Inicializa clases que definan metodo _initialize()
        super().__init_subclass__()

        # if cls.__name__ in tables_and_binding_objects.values():     # todo: this loop to be deprecated.
        #     # 1. Replaces Object_Name with object (ex: 'Animal' with <class Animal>) in tables_and_binding_objects dict.
        #     k = next((k for k in tables_and_binding_objects if tables_and_binding_objects[k] == cls.__name__), None)
        #     tables_and_binding_objects[k] = cls
        #     # 2. Gets method/function from Method_Name via getattr()
        #     if k in tables_and_methods:
        #         tables_and_methods[k] = getattr(cls, tables_and_methods[k]) or None

        # TODO: THESE ARE THE NEW TRIGGERS. Initializes all db triggers defined for class cls, if any.
        # Name mangling required here to prevent inheritance from resolving to wrong data.
        triggers_list = getattr(cls, '_' + cls.__name__ + '__db_triggers_list', None)
        if triggers_list:
            for j in triggers_list:
                if isinstance(j, (list, tuple)) and len(j) == 2:  # j=(trigger_name, trigger_processing_func (callable))
                    _ = DBTrigger(trig_name=j[0], process_func=j[1], calling_obj=cls)

        # # TODO: THESE ARE THE NEW TRIGGERS. Initializes all db triggers defined for class cls, if any.
        # triggers_dict = getattr(cls, '_' + cls.__name__ + '__db_triggers_list', None)
        # if triggers_dict:  # Name mangling required here to prevent inheritance from resolving to wrong data.
        #     for trig in triggers_dict:
        #         if 'repl' in str(trig.type):
        #             classes_with_replication[trig] = cls  # {trigger_obj: calling object for method, }
        #         elif 'dupl' in str(trig.type):
        #             classes_with_duplication[trig] = cls
        #         else:
        #             pass

        # # Initializes all db triggers defined for class cls, if any. TODO: THESE ARE THE NEW TRIGGERS.
        # triggers_dict = getattr(cls, '_' + cls.__name__ + '__db_triggers_list', None)
        # if triggers_dict:  # Name mangling required here to prevent inheritance from resolving to wrong data.
        #     for item in triggers_dict:
        #         if callable(item):
        #             trigger_str = item(cls)
        #             item_name = item.__name__       # pulls name string to identify "dupl", "repl" particles.
        #         elif hasattr(item, '__func__'):
        #             trigger_str = item.__func__(cls)
        #             item_name = item.__func__.__name__
        #         elif isinstance(item, str):
        #             trigger_str = item
        #             item_name = item
        #         else:
        #             trigger_str = None
        #             item_name = ''
        #         if trigger_str:
        #             _create_db_trigger(trigger_str)  # Runs ALL triggers defined for cls.
        #             # classes_with_replication = {id(trigger_generator): [cls, processing_function(callable)], }
        #             if 'repl' in item_name.lower():  # "dupl", "repl" particles needed to assign items to dicts below.
        #                 classes_with_replication[id(item)] = (cls, triggers_dict[item])
        #             elif 'dupl' in item_name.lower():
        #                 classes_with_duplication[id(item)] = (cls, triggers_dict[item])

        # Initializes uid_dicts in the respective subclasses. Used to manage duplication in Animales,Caravanas,Geo, etc.
        try:
            # This way of calling is works ONLY because _init_uid_dicts is defined at the lowest level of inheritance.
            # (Bovine, Caprine, etc) and NOT in their parent classes.
            cls._init_uid_dicts()  # executes only when _init_uid_dicts is defined
            # If passes, initializes dictionary {class: Duplication_method, } for all classes that implement duplication
            # classes_with_duplication[cls] = cls._processDuplicates
        except AttributeError:
            pass  # Otherwise, igonres.

        # TODO(cmt): IMPORTANT! this one MUST GO AFTER _init_uid_dicts(), as it uses __active_uids_dict data.
        if hasattr(cls, '_' + cls.__name__ + '_memory_data'):
            cls._init_memory_data()  # Dict {uid: {last_inventory: val, }, }  for now...



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

