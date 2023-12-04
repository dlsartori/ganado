import threading
from abc import ABC
from custom_types import DataTable, setRecord, delRecord, setupArgs, getRecords, dbRead
from krnl_config import *
from krnl_sqlite import getFldCompare
from threading import Lock
from krnl_exceptions import DBAccessError
from collections import defaultdict
from datetime import tzinfo, datetime, timedelta
from krnl_geo_new import Geo
import functools        # For decorator definitions
from krnl_abstract_class_prog_activity import ProgActivity
from krnl_async_buffer import BufferAsyncCursor, AsyncBuffer, BufferWriter

"""
Implements Abstract Class Activity from where all Activity singleton classes are derived.
"""

def moduleName():
    return str(os.path.basename(__file__))

ACTIVITY_LOWER_LIMIT = 15
ACTIVITY_UPPER_LIMIT = 30
ACTIVITY_DAYS_TO_ALERT = 15
ACTIVITY_DAYS_TO_EXPIRE = 365
ACTIVITY_DEFAULT_OPERATOR = None
UNDEFINED = 0


class ProgActivityCursor(BufferAsyncCursor):
    """ Implements the execution of methods Activity._paUpdate(), Activity._paMatchAndClose() in a separate thread,
    to run the checks and update database tables when an executed activity is eligible as the closing activity for 1 or
    more ProgActivities or for updating the object's myProgActivities dictionary after object's conditions have changed.
    Decouples all these db-intensive tasks from the front-end thread is the right way to go to free-up the front-end.
    """
    _writes_to_db = MAIN_DB_NAME     # Flag to signal that the class uses db-write functions setRecord(), setRecords()

    def __init__(self, *args, event=None, the_object=None, the_callable=None, **kwargs):

        self._args = args       # Data for the object to be operated on (stored, bufferized, etc).
        self._kwargs = kwargs
        super().__init__(event=event, the_object=the_object, the_callable=the_callable)

    @classmethod
    def format_item(cls, *args, event=None, the_object=None, the_callable=None, **kwargs):
        """ Item-specific method to put item on the AsyncBuffer queue. Called from AsyncBuffer.enqueue()
        Standard interface: type(cursor)=cls -> valid cursor. None: Invalid object. Do not put in queue.
        @param event: Event object created for the cursor/item to signal its results are ready for fetching.
        @param args: All data to be appended (put) to the queue as a BufferAsyncCursor object.
        @param kwargs: More data to be appended to the cursor.
        @param the_object: the object for which the callable is called. Optional.
        @param the_callable: callable to execute operations on the object. Optional.
        """
        if not the_object or not the_callable:
            return None
        # calls ProgActivityCursor __init__() and sets event and data values
        return cls(*args, event=event, the_object=the_object, the_callable=the_callable, **kwargs)   # returns cursor.


    def execute(self):
        # self._object -> Activity object (an instance of one of the subclasses of class Activity).
        # self._callable -> Activity._paMatchAndClose(), Activity._paCreateExecInstance() (for now...)
        if callable(self._callable):
            # print(f'lalalalalalala execute {self.__class__.__qualname__}({lineNum()}): \n{self._callable}, '
            #       f'object: {self._object}, args: {self._args}')
            return self._callable(self._object, *self._args, **self._kwargs)

    def reset(self):
        self._args = []
        self._kwargs = {}



class Activity(ABC):
    """                            *** Abstract class. Not to be instantiated ***
    This class inherits to the Activity classes defined in classes Tag, Animal, PersonActivityAnimal and Device.
    It implements the specific details for common methods operations of those 4 classes, in particular, access to DB
    tables and logic to update obj_data on those tables.
    Common Activities supported: Inventory(set/get), Status(set/get), LocalizationActivityAnimal(set/get)
    """
    __slots__ = ('__isValidFlag', '__activityName', '__activityID', '__doInventory', '__enableActivity', '_trigger',
                 '__tblRAName', '__tblLinkName', '__tblDataName', '__tblRAPName', '_progActivitiesPermittedStatus',
                 '__tblDataProgramacionName', '__tblLinkPAName', '__tblPASequenceActivitiesName', '__tblPATriggersName',
                 '__tblPADataStatusName', '__tblPASequencesName', '_decoratorName', '_dataProgDict', '__outerAttr',
                 '_activityFields', '__progDataFields', '__supportsPA', '_dataProgramacion', '_memDataDict',
                 '__tblObjectsName', '_memFuncBusy'
                 )



    def __init_subclass__(cls, **kwargs):
        try:        # Uses __ to ensure __method_name is not inherited, then must check for name-mangling in getattr()
            if getattr(cls, '_' + cls.__name__ + '__method_name') is not None:  # _method is inherited-> creates a mess.
                cls.register_class()       # Only registers classes with a valid method name.

        except (AttributeError, NameError):
            print(f'UUUUUUUUUUUUUHH Activity.__init_subclass__(): No __method_name for {cls} - {moduleName()}({lineNum()})')
        super().__init_subclass__(**kwargs)
        # Load ALL elements of the dictionary that have value == cls.
        for k, v in tables_and_binding_objects.items():
            if cls.__name__ == v:
                # 1. Replaces Object_Name with object (ex: 'Animal' with <class Animal>) in tables_and_binding_objects.
                tables_and_binding_objects[k] = cls
                # 2. Gets method/function from Method_Name via getattr()
                if k in tables_and_methods:
                    tables_and_methods[k] = getattr(cls, tables_and_methods[k]) or None  # A bound method is returned.

    @classmethod
    def register_class(cls):
        cls._activity_class_register.add(cls)           # Accesses the right _activity_class_register

    @classmethod
    def get_class_register(cls):
        return cls._activity_class_register

    # Buffer manager to execute Activity._paMatchAndClose() method in an independent, asynchronous thread.
    # TODO: thread_priority: 0 - 20. Fine-tune in final version, as this cursor should have a mid-priority thread.
    __progActivityBuffer = AsyncBuffer(ProgActivityCursor, autostart=True, thread_priority=8)  # mid-priority thread

    # Setea records en [Data XXX Actividades Programadas Status]. Cada record 1:1 con records en tblLinkPA.
    _paStatusDict = {'undefined': 0,    # TODO: chequear como "is not undefined" para incluir 0 y None.
                     'openActive': 1,
                     'openExpired': 2,
                     'closedInTime': 4,
                     'closedExpired': 5,    # Cierre despues de progDate+UpperWindow y ANTES de progData+daysExpiration
                     'closedNotExecuted': 6,    # ProgActivities sin cierre despues de daysExpiration.
                                                # Este status es chequeado y seteado SOLO por funciones de background.
                     'closedBaja': 7,
                     'closedLocalizChange': 8,
                     'closedCancelled': 9,
                     'closedReplaced': 10,
                     'closedBySystem': 11   # same as 'closedNotExecuted', but this closure is done by cleanup tasks.
                     }

    __lock = Lock()
    __tblActivityStatusNames = 'tblActividadesStatus'
    __deepDiff_ignore_types = (Geo,)        # Types to ignore in DeepDiff comparisons.

    @classmethod
    def __raiseMethodError(cls):  # Common error exception for incorrect object/class type
        raise AttributeError(f'ERR_SYS_Invalid call: {callerFunction(depth=1, getCallers=False)} called from '
                             f'{cls.__name__}. Must be called from a subclass.')

    temp0 = getRecords('tblUnidadesNombres', '', '', None, '*')
    if not isinstance(temp0, DataTable):
        raise (DBAccessError, 'ERR_DBAccess: cannot read table [Unidades Nombres]. Exiting.')

    _unitsDict = {}          # {fldName: (unitID, Sigla, TipoDeUnidad, SistemaDeUnidades), }
    for j in range(temp0.dataLen):
        d = temp0.unpackItem(j)
        _unitsDict[d['fldName']] = (d['fldID'], str(d['fldAcronym']).lower(), d['fldFK_TipoUnidad'],
                                    d['fldFK_SistemaDeUnidades'])

    _unitsDict_lower = {k.lower(): v for k, v in _unitsDict.items()}  # {fldName: (unitID, Sigla, TipoDeUnidad, SistemaDeUnidades), }



    @classmethod
    def _getTblActivityStatusName(cls):
        return Activity.__tblActivityStatusNames

    __tblRANames = {'tblAnimalesRegistroDeActividades': 'tblAnimalesRegistroDeActividadesProgramadas',
                    'tblPersonasRegistroDeActividades': 'tblPersonasRegistroDeActividadesProgramadas',
                    'tblCaravanasRegistroDeActividades': 'tblCaravanasRegistroDeActividadesProgramadas',
                    'tblDispositivosRegistroDeActividades': 'tblDispositivosRegistroDeActividadesProgramadas',
                    'tblListasRegistroDeActividades': '',
                    'tblTMRegistroDeActividades': '',
                    'tblProyectosRegistroDeActividades': ''}

    __linkTables = {'tblAnimalesRegistroDeActividades': 'tblLinkAnimalesActividades',
                    'tblPersonasRegistroDeActividades': 'tblLinkPersonasActividades',
                    'tblDispositivosRegistroDeActividades': 'tblLinkDispositivosActividades',
                    'tblCaravanasRegistroDeActividades': 'tblLinkCaravanasActividades',
                    'tblTMRegistroDeActividades': None, 'tblListasRegistroDeActividades': None,
                    'tblProyectosRegistroDeActividades': None}

    __progTables = {'tblAnimalesRegistroDeActividadesProgramadas': ('tblLinkAnimalesActividadesProgramadas',
                                                                    'tblAnimalesActividadesProgramadasTriggers',
                                                                    'tblDataAnimalesActividadesProgramadasStatus',
                                                                    'tblAnimalesAPSecuencias',
                                                                    'tblAnimalesAPSecuenciasActividades'),

                    'tblPersonasRegistroDeActividadesProgramadas': ('tblLinkPersonasActividadesProgramadas',
                                                                    'tblPersonasActividadesProgramadasTriggers'
                                                                    'tblDataPersonasActividadesProgramadasStatus',
                                                                    'tblPersonasAPSecuencias',
                                                                    'tblPersonasAPSecuenciasActividades'),

                    'tblDispositivosRegistroDeActividadesProgramadas': ('tblLinkDispositivosActividadesProgramadas',
                                                                        'tblDispositivosActividadesProgramadasTriggers',
                                                                    'tblDataDispositivosActividadesProgramadasStatus',
                                                                        'tblDispositivosAPSecuencias',
                                                                        'tblDispositivosAPSecuenciasActividades'),

                    'tblCaravanasRegistroDeActividadesProgramadas': (None, None, None, None, None),
                    'tblTMRegistroDeActividadesProgramadas': (None, None, None, None, None),
                    'tblListasRegistroDeActividadesProgramadas': (None, None, None, None, None),
                    'tblProyectosRegistroDeActividadesProgramadas': (None, None, None, None, None)
                    }

    @staticmethod
    def _tblRANames():
        return Activity.__tblRANames

    @classmethod
    def _geTblLinkName(cls, tblRAName):
        return cls.__linkTables.get(tblRAName)

    @classmethod
    def _getLinkTables(cls):
        return list(cls.__linkTables.values())

    temp = getRecords(__tblActivityStatusNames, '', '', None, 'fldID', 'fldStatus')
    if type(temp) is str:
        krnl_logger.error(f'ERR_DB_ReadError-Iniziatilization faiulre: {temp}  {callerFunction()}. Exiting... ')
        raise DBAccessError(f'ERR_DB_ReadError-Iniziatilization faiulre: {temp}  {callerFunction()}. Exiting... ')
    __activityStatusDict = {}
    for j in range(temp.dataLen):
        __activityStatusDict[temp.getVal(j, 'fldStatus')] = temp.getVal(j, 'fldID')

    @staticmethod
    def _getActivityStatusDict():
        return Activity.__activityStatusDict

    __excludedFieldsDefaultBase = {'fldComment', 'fldFK_UserID', 'fldTimeStamp', 'fldTimeStampSync', 'fldBitmask',
                                   'fldPushUpload', 'fldFK_DataProgramacion'}

    @classmethod
    def _getBaseExcludedFields(cls):
        try:
            return cls.__excludedFieldsDefaultBase
        except (AttributeError, NameError):
            pass

    @classmethod
    def _addBaseExcludedField(cls, fld):
        try:
            if fld not in cls._excluded_fields:
                cls.__excludedFieldsDefaultBase.add(fld)  #
        except (AttributeError, NameError, ValueError):
            pass

    @classmethod
    def _removeBaseExcludedField(cls, fld):
        try:
            if fld in cls._excluded_fields:
                cls.__excludedFieldsDefaultBase.discard(fld)
        except (AttributeError, NameError, ValueError):
            pass

    # @classmethod
    # def getClassExcludedFields(cls):
    #     try:
    #         return cls._excluded_fields  # __classExcludedFieldsClose (set) defined in each subclass.
    #     except (AttributeError, NameError):
    #         pass

    @staticmethod
    def getPARegisterDict():            # TODO: Must be implemented in all subclasses.
        pass


    def __new__(cls, *args, **kwargs):
        if cls is Activity:
            krnl_logger.error(f"ERR_TypeError: {cls.__name__} cannot be instantiated, please use a subclass")
            raise TypeError(f'ERR_TypeError: class {cls} cannot be instantiated. Please, use one of its subclasses.')
        return super().__new__(cls)

    def __init__(self, isValid, activityName=None, activityID=None, invActivity=False,enableActivity=activityEnableFull,
                 tblRA=None, *args, tblLinkName='', tblDataName='', tblObjectsName='', excluded_fields=(), **kwargs):
        self.__tblRAName = tblRA
        self.__tblRAPName = self.__tblRANames.get(self.__tblRAName, '')
        self.__doInventory = invActivity            # 0: Activity is non-Inventory. 1: Activity counts as Inventory
        self.__supportsPA = kwargs.get('supportsPA', False)  # Si no se pasa, asume False
        self.__isValidFlag = isValid
        self.__activityName = activityName
        self.__activityID = activityID               # Debe ser igual a fldFK_Actividad
        self.__enableActivity = enableActivity
        self.__tblLinkName = self._geTblLinkName(self.__tblRAName) if self._geTblLinkName(self.__tblRAName) \
                                else tblLinkName
        self.__tblDataName = tblDataName
        self.__tblObjectsName = tblObjectsName
        self.__tblDataProgramacionName = 'tblDataProgramacionDeActividades'  # misma para todas las clases de objetos.
        self.__tblPADataStatusName = self.__progTables[self.__tblRAPName][2] if self.__tblRAPName else ''
        self.__tblLinkPAName = self.__progTables[self.__tblRAPName][0] if self.__tblRAPName else ''

        # Almacena OBJETO de la clase llamadora ( Outer class = Bovine, Caprine, Tag, Person, Device, etc)
        # TODO(cmt): __outerAttr => ONLY 1 object per Activity and per Thread (recursion with a different object for
        #  SAME activity and SAME Thread is not supported). Used by all re-entrant methods (that could be called by
        #  different threads: Activity.__call__, _setInventory, _setStatus, _setLocaliz, _getRecordLinkTables, etc.).
        self.__outerAttr = {}  # Atributo DINAMICO. {threadID: outerObject, }
        self._decoratorName = next((kwargs[j] for j in kwargs if 'decorator' in str(j).lower()), None)
        self.__excluded_fields = (self.getActivityExcludedFieldsClose(self._activityName) or set()).union(excluded_fields)
        super().__init__()

    @property
    def outerObject(self):
        if self.__outerAttr:
            k = threading.current_thread().ident
            if k in self.__outerAttr:
                return self.__outerAttr[k]
            else:
                val = f'ERR_SYS_Threading: __outerAttr called by {self} with key {threading.current_thread().ident} ' \
                          f'but key does not exist in __outerAttr dictionary.'
                krnl_logger.error(val)
                raise KeyError(val)
        krnl_logger.warning(f'ERR_SYS_Runtime: outerObject called on an empty __outerAttr dict. Callers: '
                            f'{callerFunction(getCallers=True, namesOnly=True)}')

    @outerObject.setter
    def outerObject(self, obj=None):
        """ Appends an object (Animal, Tag, Person) to __outerAttr, which is a dict that stores 1 obj value for each
        thread calling the function: {thread_id: object}. In this way threads access the correct object regardless of
        how and when the OS switches thread execution. This wasn't the case with the previous stack structure.
        @param obj: not None -> added to __outerAttr dict. """
        if obj is not None:
            self.__outerAttr[threading.current_thread().ident] = obj      # TODO: Is this line thread-safe?? Check..


    def _pop_outerAttr_key(self, thread_id):            # Private method. Not meant for general use.
        """ This one is important: when a thread is shutdown, this function must be called for all the active Activity
        objects so that the __outerAttr entry for that thread is removed from the Activity object dictionary.
        This prevents __outerAttr dictionary from growing too large as threads are created and killed throughout the
        running life of the program.
        @return: thread.ident() value (int) if found in __outerAttr dictionary. None of thread_id not found.
        """
        return self.__outerAttr.pop(thread_id, None)


    @property
    def shortName(self):
        return self._short_name.lower()   # defined in the relevant subclasses. Contains a short name for the Activity


    def __del__(self):
        pass
        # try:
        #     if self.__outerAttr:             # Destructor checks consistency of __outerAttr y reporta inconsistencias
        #         krnl_logger.warning(f'ERR_SYS_Logic Error: __outerAttr len: {len(self.__outerAttr)} / '
        #                             f'Activity: {self._activityName}. Class: {self.__class__.__name__}')
        # except AttributeError:
        #     pass


    @property
    def _isValid(self):
        return self.__isValidFlag

    @_isValid.setter
    def _isValid(self, val):
        self.__isValidFlag = 0 if not val else 1

    @property
    def _activityName(self):
        return self.__activityName

    @property
    def _excluded_fields(self):
        return self.__excluded_fields

    @property
    def _supportsPA(self):
        """ The activity supports the definition of Programmed Activities that are to be performed in the future """
        return self.__supportsPA

    @property
    def _activityID(self):
        return self.__activityID

    @property
    def _tblRAName(self):
        return self.__tblRAName

    @property
    def _tblLinkName(self):
        return self.__tblLinkName

    @property
    def _tblLinkPAName(self):
        return self.__tblLinkPAName

    @property
    def _tblDataName(self):
        return self.__tblDataName

    @property
    def _tblRAPName(self):
        return self.__tblRAPName

    @property
    def _tblPADataStatusName(self):
        return self.__tblPADataStatusName

    @classmethod
    def tblDataProgramacionName(cls):
        cls.__raiseMethodError()      # Esta funcion se debe definir en TODAS las subclases.


    def _outerAttr(self):
        return self.__outerAttr  # In order to look at __outerAttr during debugging.

    # @staticmethod                   # TODO: DEPRECATED. NO SE USA MAS as of 4.1.7!
    # def memFuncReentryWrapper(func):  # Wrapper to manange re-entry of memory-writing functions for SAME outerObject
    #     """ Codigo standard para las funciones con re-entry que escriben en memoria (inventory, status, localization,
    #     category). El manejo de re-entry se activa solo en el caso en que distintos threads intenten modificar al mismo
    #     tiempo EL MISMO outerObject. Hace esto chequeando el estado del dict __outerAttr de la Actividdad.
    #     """
    #     @functools.wraps(func)
    #     def wrapper(self, *args, **kwargs):     # _memDataDict: {outerObject: [value, timestamp], }
    #         if self.outerObject in self._memDataDict:  # Hay reentry: llaman con el mismo outerObject desde otro thread.
    #             with self.__lock:  # Solo entra si hay 2 o mas del mismo outerObject en __outerAttr dict.
    #                 self._memFuncReentry += 1  # NO DECREMENTAR. Logica de re-entry usa valor acumulado
    #                 reentryLevel = self._memFuncReentry     # Local variable, to be able to check recursion level.
    #
    #             retValue = func(self, *args, **kwargs)        # Llama a funcion original
    #
    #             if reentryLevel == 1:  # Solo la 1ra instancia de re-entry resetea _memFuncReentry
    #                 with self.__lock:  # Lock porque estas 2 lineas en conjunto no son atomicas.
    #                     self._memFuncReentry = 0  # Resetea _memFuncReentry. Esta linea SI es atomica.
    #                     self._memDataDict.pop(self.outerObject, None)  # Limpia dict al salir para que la logica funque.
    #
    #         else:        # Aqui NO hay re-entry para el mismo outerObject.
    #             with self.__lock:
    #                 self._memDataDict[self.outerObject] = []   # Pone outerObject en el dict, para la logica de reentry.
    #
    #             retValue = func(self, *args, **kwargs)  # Llama a funcion original
    #             with self.__lock:
    #                 self._memDataDict.pop(self.outerObject, None)  # Limpia dict al salir para que la logica funque.
    #
    #         return retValue
    #     return wrapper
    #
    # @staticmethod
    # def memFuncReentryWrapper00(func):  # Wrapper to manange re-entry of memory-writing functions for SAME outerObject
    #     """ Codigo standard para las funciones con re-entry que escriben en memoria (inventory, status, localization,
    #     category). El manejo de re-entry se activa solo en el caso en que distintos threads intenten modificar al mismo
    #     tiempo EL MISMO outerObject. Hace esto chequeando el estado del dict __outerAttr de la Actividdad.
    #     """
    #     @functools.wraps(func)
    #     def wrapper(self, *args, **kwargs):
    #         # TODO(cmt): _memFuncReentry to be set > 0 only if outerObject IS THE SAME for 2+ nested calls of same func().
    #         outerObjectCount = list(self.__outerAttr.values()).count(self.outerObject)
    #         with self.__lock:  # Solo entra si hay 2 o mas del mismo outerObject en __outerAttr dict.
    #             if outerObjectCount > 1:
    #                 self._memFuncReentry = outerObjectCount - 1  # NO DECREMENTAR. Logica de re-entry usa valor acumulado
    #             reentryLevel = self._memFuncReentry  # Local variable. 1st entry to this code will ALWAYS make it 0.
    #         kwargs['reentry_level'] = reentryLevel
    #
    #         retValue = func(self, *args, **kwargs)  # Llama a funcion original
    #
    #         with self.__lock:  # Lock porque estas 2 lineas en conjunto no son atomicas.
    #             if reentryLevel == 0:  # Solo la 1ra instancia de re-entry resetea _memFuncReentry
    #                 self._memFuncReentry = 0  # Resetea _memFuncReentry. Esta linea SI es atomica.
    #             self.outerObject = None  # Codigo re-entrante: SIEMPRE debe setear outerObject de calling thread = None
    #         return retValue
    #
    #     return wrapper

    @property
    def _isInventoryActivity(self):
        return self.__doInventory

    def _getActivityID(self, tblActivityID=None):
        """ returns __activityID by querying tblActivityID. If tblActivityID is None, returns self.__activityID"""
        return tblActivityID if tblActivityID else self.__activityID

    def _packActivityParams(self, *args, **kwargs):
        """ Packs DataTable parameters for an Activity in a dict. of form {fldUID: fldValue, } with all fldValue!=None
            Creates and returns a dictionary with all the non-NULL fields used/modified by an operation/activity.
            Returns values only from dataList[0]. Not defined for tables with more than 1 record in dataList.
        """
        paramsDict = {}
        for j in args:
            if isinstance(j, DataTable):
                valuesDict = {j.getFldUID(fld): j.getVal(0, fld) for fld in j.fldNames if j.getVal(0, fld)
                               not in (None, {}, [], ())}
                if valuesDict:
                    paramsDict.update(valuesDict)  # No debieran haber fldUID repetidos que se sobreescriban en update()
        return paramsDict

    def _doInventory(self, **kwargs):
        """
        Checks whether an Activity (other than InventoryActivityTag) must record or not an Inventory entry.
        Gives priority to a directive passed in kwargs on whether to execute or not an Inventory Activity.
        @param kwargs: 'recordInventory'=True/False
        Logic: kwargs 'recordInventory' overrides the Activity setting of internal variable _doInventory
        @return: True if Activity must perform Inventory, otherwise False
        """
        # Si no se pasa kwargs['recordInventory'] -> usa _doInventory
        recordInventory = next((kwargs.get(j) for j in kwargs if j.lower().startswith('recordinvent')), VOID)
        return self._isInventoryActivity if recordInventory == VOID else bool(recordInventory)

    # @memFuncReentryWrapper          # Re-entry handler para funciones que sincronizan datos de DB en memoria.
    # TODO(cmt): full blown function with re-entry management, NO decorator and NO auxiliary re-entry flags.
    def _setInventory(self, *args: DataTable, **kwargs):
        """
        Inserts Inventory obj_data for the object in the DB. This function is called by classes Animal,Tag,Person,Device
        This function hides all the complexities of accessing obj_data and writing tables for an Inventory operation.
        @param * args: List of data tables (tblRA, tblLink, tblInventory) to write to DB.
        @param kwargs: isProg: True->Programmed Activity. Used by Signature methods to run the Programmed Activities Logic.
        @return: Success: ID_Actividad RA (int), Error: errorCode (str), None if nothing's written.
        """
        tblRA = next((j for j in args if j.tblName == self._tblRAName), DataTable(self._tblRAName))
        tblLink = next((j for j in args if j.tblName == self._tblLinkName), DataTable(self._tblLinkName))
        tblData = next((j for j in args if j.tblName == self._tblDataName), DataTable(self._tblDataName))
        outer_obj = self.outerObject
        if self._isValid:
            # Prioridad eventDate:    1: fldDate(tblData); 2: kwargs(fecha valida); 3: timeStamp.
            eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), defaultVal=time_mt('datetime'), **kwargs)
            activityID = self._getActivityID(tblRA.getVal(0, 'fldFK_NombreActividad'))
            tblRA.setVal(0, fldFK_NombreActividad=activityID)
            tblData.setVal(0, fldDate=eventDate)         # Mismo valor para DB y para memoria.
            if outer_obj.supportsMemData:        # parametros para actualizar variables en memoria se pasan en
                tblData.setVal(0, fldMemData=1)  # Activa flag Memory Data en el registro que se escribe en DB
            idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)
            retValue = idActividadRA

            if isinstance(retValue, int):
                """ En caso e reentry solo escribira en memoria el valor de eventDate mas alto disponible """
                with self.__lock:  # lock: the whole block must be atomic to ensure integrity of what's written to mem.
                    if outer_obj.lastInventory:
                        if outer_obj._lastInventory[1] < eventDate:
                            # Writes items to memory list only if data in memory is from a date EARLIER than eventDate.
                            outer_obj.lastInventory = [eventDate, eventDate]
                    else:
                        outer_obj.lastInventory = [eventDate, eventDate]
            else:
                retValue = f'ERR_Sys_Object not valid or activity not defined: {retValue}- {callerFunction()}'
                krnl_logger.info(retValue)
                print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)
        else:
            retValue = None

        return retValue


    def _getInventory(self, sDate='', eDate='', *args, event_date=False, **kwargs):
        """
        Returns ALL records in table Data Inventario between sValue and eValue. sValue=eValue ='' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param event_date: True -> Returns the timestamp the value requested was recorded instead of the value itself.
        Used in ProgActivities.
        @param kwargs: mode='value' -> Returns last value from DB. If no mode or mode='memory' returns value from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        fldName = 'fldDate'
        modeArg = 'value'           # kwargs.get('mode', 'mem')
        retValue = None

        # if 'mem' in modeArg.lower():
        #     # Todo(cmt): esto para aprovechar los datos en memoria y evitar accesos a DB.
        #     if self.outerObject.supportsMemData:
        #         # Returns data from memory
        #         return self.outerObject.lastInventory if not event_date else self.outerObject._lastInventory[1]
        #     else:
        #         modeArg = 'value'

        tblRA = setupArgs(self._tblRAName, *args)  # __tblRAName viene de class Activity
        tblLink = setupArgs(self._tblLinkName, *args)  # __tblLinkName viene de class Activity
        tblData = setupArgs(self._tblDataName, *args, **kwargs)  # __tblLinkName viene de class Activity
        qryTable = self._getRecordsLinkTables(tblRA, tblLink, tblData)
        if isinstance(qryTable, DataTable):
            if qryTable.dataLen <= 1:
                result = qryTable
            else:
                result = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
            if 'val' in modeArg.lower():
                # Retorna PRIMER o ULTIMO Registro en funcion de valores de sDate, eDate.
                retValue = result.getVal(0, fldName) if not event_date else result.getVal(0, 'fldDate')
            else:
                # Retorna DataTable con registros.
                retValue = result
        return retValue


    def _setStatus(self,  *args, status=None, **kwargs):    #
        """
        MANDATORIO: ID_Actividad, ID_Status, ActivoYN, FechaEvento -> fldFK_Actividad, fldFK_Status, fldFlag, fldDate
        Inserts Status obj_data for the object in the DB. This function is called by classes Animal, Tag, Person, etc.
        This function hides all the complexities of accessing obj_data and writing tables for an Inventory operation.
        @param status: status value (string)
        @param args: DataTable objects, with all the tables and fields to be written to DB
        @param kwargs: 'status'=statusValue -> Status to set when status is not passed via DataTable
                       'recordInventory' = True/False -> Forces the insertion (or not) of an Inventory Record. Higher
                        priority than _doInventory setting.
                       'isProg' = True/False -> Activity is Programmed Activity, or not. If not passed -> assumes False.
        @return: Success: ID_Actividad RA (int), Error: errorCode (str)
        """
        # reentryLevel = kwargs.get('reentry_level', 0)    # toma valor pasado por el wrapper (si se llamo via wrapper)
        args = [j for j in args if isinstance(j, DataTable)]
        tblData = next((j for j in args if j.tblName == self._tblDataName), None)
        tblData = tblData if tblData else DataTable(self._tblDataName)
        statusDict = self.outerObject.statusDict
        if status in statusDict:
            statusID = statusDict[status][0]
        else:
            statusID = tblData.getVal(0, 'fldFK_Status')            # Logica para aceptar statusName y statusID
        status = str(statusID).strip()                      # Convierte a str y ve si el str esta en statusDict
        if status not in statusDict:      # Si no esta, asume que es un statusID y va a busar el status correspondiente
            status = next((j for j in statusDict if statusDict[j][0] == statusID), None)
            if status is None:
                status = kwargs.get('status')       # Si no se encontro status en DataTables, lo toma de kwargs
                if status not in statusDict:
                    retValue = f'ERR_INP_InvalidArgument: {status}. {moduleName()}({lineNum()} - ' \
                               f'{callerFunction()})'
                    return retValue      # Sale si es valor de status no valido

        tblRA = next((j for j in args if j.tblName == self._tblRAName), DataTable(self._tblRAName))
        tblLink = next((j for j in args if j.tblName == self._tblLinkName), DataTable(self._tblLinkName))

        if self._isValid and self.outerObject.validateActivity(self._activityName):
            # Prioridad eventDate:    1: fldDate(tblData); 2: kwargs(fecha valida); 3: timeStamp.
            timeStamp = time_mt('datetime')
            eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), defaultVal=timeStamp, **kwargs)
            activityID = tblRA.getVal(0, 'fldFK_NombreActividad')
            activityID = activityID if activityID else self._activityID
            tblRA.setVal(0, fldFK_NombreActividad=activityID)
            tblData.setVal(0, fldDate=eventDate)
            currentStatusID = self.outerObject.status.get()  # Toma valor de memoria si objeto tiene supportMemData=True

            # permittedFrom()  definida en las sub-Clases (Tag, Animal, PersonActivityAnimal, Bovine, Caprine, etc)
            if statusDict[status][0] in self.permittedFrom()[currentStatusID] or kwargs.get('enforce'):  # Ignora no permitidos.
                flagExecInstance = currentStatusID != statusDict[status][0]
                commentData = tblData.getVal(0, 'fldComment', '') or '' + f'ObjectID: ' \
                                                                    f'{self.outerObject.getID} / Activity: {activityID}'
                tblData.setVal(0, fldFK_Status=statusDict[status][0], fldFlag=statusDict[status][1],
                               fldComment=commentData)

                if self.outerObject.supportsMemData:  # parametros para actualizar variables en memoria se pasan en
                    tblData.setVal(0, fldMemData=1)  # Activa flag Memory Data en el registro que se escribe en DB
                idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)
                retValue = idActividadRA

                if isinstance(retValue, int):
                    """ En caso e reentry solo escribira en memoria el valor de eventDate mas alto disponible """
                    with self.__lock:  # lock: the whole block must be atomic to ensure integrity of what's written to mem.
                        if self.outerObject.lastStatus:
                            if self.outerObject._lastStatus[1] < eventDate:
                                # Writes the list to memory only if memory data is from a date EARLIER than eventDate.
                                self.outerObject.lastStatus = [statusDict[status][0], eventDate]
                        else:
                            self.outerObject.lastStatus = [statusDict[status][0], eventDate]

                    if flagExecInstance:
                        self._paCreateExecInstance(outer_obj=self.outerObject)
                else:
                    retValue = f'ERR_Sys_Object not valid or activity not defined: {retValue}- {callerFunction()}'
                    krnl_logger.info(retValue)
                    print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)
            else:                                                           # Sale si valor de status no permitido
                retValue = f'INFO_INP_InvalidArguments: Status {statusDict[status][0]} not set.'
                krnl_logger.info(retValue)
        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - {callerFunction(getCallers=True)}'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}  - {retValue}', dismiss_print=DISMISS_PRINT)

        return retValue

    def _getStatus(self, sDate='', eDate='', *args, event_date=False, **kwargs):
        """
        Returns records in table Data Status between sValue and eValue. If sValue = eValue = '' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode='value' -> Returns last value from DB. If no mode or mode='memory' returns value
                        from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        fldName = 'fldFK_Status'
        modeArg = 'value'           # kwargs.get('mode', 'mem')
        retValue = None

        # if 'mem' in modeArg.lower():
        #     # Todo(cmt): esto para aprovechar los datos en memoria y evitar accesos a DB.
        #     if self.outerObject.supportsMemData:
        #         retValue = self.outerObject.lastStatus if not event_date else self.outerObject._lastStatus[1]
        #         return retValue
        #     else:
        #         modeArg = 'value'

        tblRA = setupArgs(self._tblRAName, *args)  # __tblRAName viene de class Activity
        tblLink = setupArgs(self._tblLinkName, *args)  # __tblLinkName viene de class Activity
        tblData = setupArgs(self._tblDataName, *args, **kwargs)  # __tblDataName viene de class Activity
        qryTable = self._getRecordsLinkTables(tblRA, tblLink, tblData)
        if isinstance(qryTable, DataTable):
            if qryTable.dataLen <= 1:
                result = qryTable
            else:
                result = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
            if 'val' in modeArg.lower():
                # Retorna PRIMER o ULTIMO Registro en funcion de valores de sDate, eDate.
                retValue = result.getVal(0, fldName) if not event_date else result.getVal(0, 'fldDate')
            else:
                # Retorna DataTable con registros.
                retValue = result

        return retValue


    def _setLocalization(self, *args: DataTable, **kwargs):
        """
        MANDATORIO: ID_Actividad, ID_Localizacion
        creates a LocalizationActivityAnimal record in the DB. This function is called by classes Animal, Tag,
        PersonActivityAnimal and Device. This function hides all the complexities of accessing obj_data and writing
        tables for a LocalizationActivityAnimal operation.
        @param tblRA, tblLink, tblData: list of DataTable objects, with all the tables and fields to be written to DB
        @param args: Additional obj_data tablas to write (Inventory, etc): WRITTEN AS PASSED. They must come here complete!
        @return: Success: ID_Actividad RA (int), Error: errorCode (str)
        """
        tblData = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblDataName), None)
        tblData = tblData if tblData else DataTable(self._tblDataName)
        loc = tblData.getVal(0, 'fldFK_Localizacion')
        if not loc:
            loc = kwargs.get('localization', None)
        elif isinstance(loc, str):
            loc = Geo.getObject(loc)
        if not isinstance(loc, Geo):  # No se paso localizacion valida. Sale nomas...
            return None

        if self._isValid and self.outerObject.validateActivity(self._activityName):
            # Prioridad eventDate:    1: fldDate(tblData); 2: kwargs(fecha valida); 3: timeStamp.
            timeStamp = time_mt('datetime')
            eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), defaultVal=time_mt('datetime'), **kwargs)
            flagExecInstance = self.outerObject.lastLocalization != loc
            tblRA = next((j for j in args if j.tblName == self._tblRAName), DataTable(self._tblRAName))
            tblLink = next((j for j in args if j.tblName == self._tblLinkName), DataTable(self._tblLinkName))
            activityID = tblRA.getVal(0, 'fldFK_NombreActividad')
            activityID = activityID if activityID else self._activityID
            tblRA.setVal(0, fldFK_NombreActividad=activityID)
            tblData.setVal(0, fldFK_Localizacion=loc.ID, fldFK_NivelDeLocalizacion=loc.localizLevel, fldDate=eventDate)

            if self.outerObject.supportsMemData:  # parametros para actualizar variables en memoria se pasan en
                tblData.setVal(0, fldMemData=1)         # Activa flag Memory Data en el registro que se escribe en DB
            idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)
            retValue = idActividadRA
            if isinstance(retValue, int):
                """ Escribe en Memoria SIEMPRE (en todos los reentry), con el valor de eventDate mas alto disponible """
                with self.__lock:  # lock: the whole block must be atomic to ensure integrity of what's written to mem.
                    if self.outerObject.lastLocalization:
                        if self.outerObject._lastLocalization[1] < eventDate:
                            # Writes the list to memory only if data in memory is from a date EARLIER than eventDate.
                            self.outerObject.lastLocalization = [loc, eventDate]
                    else:
                        self.outerObject.lastLocalization = [loc, eventDate]

                # TODO: Here go the matchAndClose() checks and inventory checks.
                if flagExecInstance:
                    self._paCreateExecInstance(outer_obj=self.outerObject)
            else:
                retValue = f'ERR_Sys_Object not valid or activity not defined: {retValue}- {callerFunction()}'
                krnl_logger.info(retValue)
                print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)
        else:
            retValue = None

        return retValue

    def _getLocalization(self, sDate='', eDate='', *args, event_date=False, **kwargs):
        """
        Returns records in table Data Status between sValue and eValue. If sValue = eValue = '' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode='value' -> Returns last value from DB. If no mode or mode='memory' returns value
                        from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        fldName = 'fldFK_Localizacion'
        modeArg = 'value'       # kwargs.get('mode', 'mem')
        retValue = None
        # if 'mem' in modeArg.lower():
        #     # Todo(cmt): esto para aprovechar los datos en memoria y evitar accesos a DB.
        #     if self.outerObject.supportsMemData:  # Returns data from memory. Geo object!
        #         retValue = self.outerObject.lastLocalization if not event_date else self.outerObject._lastLocalization[1]
        #         return retValue
        #     else:
        #         modeArg = 'value'

        tblRA = setupArgs(self._tblRAName, *args)  # __tblRAName viene de class Activity
        tblLink = setupArgs(self._tblLinkName, *args)  # __tblLinkName viene de class Activity
        tblData = setupArgs(self._tblDataName, *args, **kwargs)  # __tblDataName viene de class Activity
        qryTable = self._getRecordsLinkTables(tblRA, tblLink, tblData)   # Busca TODAS las actividades de RA (*)
        if isinstance(qryTable, DataTable):
            if qryTable.dataLen <= 1:
                result = qryTable
            else:
                result = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
            if 'val' in modeArg.lower():
                # Retorna PRIMER o ULTIMO Registro en funcion de valores de sDate, eDate.
                retValue = result.getVal(0, fldName) if not event_date else result.getVal(0, 'fldDate')
            else:
                # Retorna DataTable con registros.
                retValue = result
        return retValue

    def _setPerson(self, *args, **kwargs):
        """
        Creates records in [Data Animales Actividad Personas] table in the DB.
        This function is called by classes Animal, Tag and Device
        @param args: tblData: is parsed as 1 person per _dataList record.
        @return: Success: ID_Actividad RA (int), Error: errorCode (str)
        """
        tblObjects = getRecords('tblPersonas', '', '', None, '*', fldPersonLevel=1)
        # Lista de personas activas en el sistema
        validPersonList = [tblObjects.getVal(j, 'fldID') for j in range(tblObjects.dataLen)
                           if not tblObjects.getVal(j, 'fldDateExit') and tblObjects.getVal(j, 'fldPersonLevel') == 1]
        args = set([j for j in args if isinstance(j, DataTable)])
        tblRA = next((j for j in args if j.tblName == self._tblRAName), DataTable(self._tblRAName))
        tblLink = next((j for j in args if j.tblName == self._tblLinkName), DataTable(self._tblLinkName))
        tblData = next((j for j in args if j.tblName == self._tblDataName), DataTable(self._tblDataName))

        if self._isValid and self.outerObject.validateActivity(self._activityName):
            activityID = tblRA.getVal(0, 'fldFK_NombreActividad')
            activityID = activityID or self._activityID
            tblRA.setVal(0, fldFK_NombreActividad=activityID)
            eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), defaultVal=time_mt('datetime'), **kwargs)
            initialPersons = tblData.getCol('fldFK_Persona')

            # Valida datos pasados en tabla [Data Animales Actividad Personas]
            wrtTblData = DataTable(tblData.tblName)
            for j in range(tblData.dataLen):
                # Arma wrtTablaData con todas los idPersonas VALIDAS y demas datos de tabla [Data Actividad Personas]
                current_idx = wrtTblData.dataLen   # Empieza en 0 y se va incrementando con cada registro que se agrega
                idPerson = tblData.getVal(j, 'fldFK_Persona')
                if idPerson not in validPersonList:
                    continue    # Sale si la persona no es valida
                percent = tblData.getVal(j, 'fldPercentageOwnership') if tblData.getVal(j, 'fldPercentageOwnership') \
                                                                    is not None else 1  # Si no se pasa %, asume 100%
                wrtTblData.setVal(current_idx, fldPercentageOwnership=percent, fldFK_Persona=idPerson,
                                  fldComment=tblData.getVal(j, 'fldComment'), fldDate=eventDate,
                                  fldFlag=(tblData.getVal(j, 'fldFlag') if tblData.getVal(j,'fldFlag') in (0,1) else 1))
                if not wrtTblData.getVal(current_idx, 'fldFlag'):
                    wrtTblData.setVal(current_idx, fldPercentageOwnership=0)  # Si no es Persona activa, quita ownership
            retValue = self._createActivityRA(tblRA, tblLink, wrtTblData, **kwargs)

            if self.__supportsPA:
                self._paMatchAndClose(retValue, execute_date=tblData.getVal(0, 'fldDate'),
                                      excluded_fields=self._excluded_fields)
            if set(initialPersons).symmetric_difference(wrtTblData.getCol('fldFK_Persona')):
                # IF list of owners changes, goes to check if a PA needs to be created.
                self._paCreateExecInstance(outer_obj=self.outerObject)
            # TODO: Crear lista de Objetos para los que se define la Actividad (Ejecutada o Programada).


        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - {callerFunction(getCallers=True)}'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}  - {retValue}', dismiss_print=DISMISS_PRINT)

        return retValue

    def _setTM(self, *args: DataTable, id_activity=None, id_transaction=None, **kwargs):
        """
        Inserts record "idActividadTM" in [Data Animales Actividad MoneyActivity]. Creates records in
        [Animales Registro De Actividades] and [Link Animales Actividades] if required.
        @param id_activity: idActividadRA from [TM Registro De Actividades]. Goes to fldFK_ActividadTM
        @param id_transaction: idTransaccion from [Data TM Transacciones]. Goes to fldFK_Transaccion
        @param args: DataTables. Only [Animales Registro De Actividades] and [Data Animales Actividad MoneyActivity] are parsed
        @param kwargs: Arguments passed to [Data Animales Actividad MoneyActivity] table.
        @return: idActividadRA: Success / errorCode (str) on error
        """
        if self._isValid:
            tblRA = setupArgs(self.__tblRAName, *args)
            tblLink = setupArgs(self.__tblLinkName, *args)
            tblData = setupArgs(self.__tblDataName, *args, **kwargs)
            if id_activity:
                tblData.setVal(0, fldFK_ActividadTM=id_activity)
            if id_transaction:
                tblData.setVal(0, fldFK_Transaccion=id_transaction)
            if not tblData.getVal(0, 'fldFK_ActividadTM') and not tblData.getVal(0, 'fldFK_Transaccion'):
                retValue = f'ERR_INP_Invalid argument: idActividadTM. Missing Actividad TM and/or Transaccion - ' \
                           f'{callerFunction()}'
                krnl_logger.info(retValue)
                print(f'{retValue}', dismiss_print=DISMISS_PRINT)
                return retValue             # Sale con error si no se paso idActividadTM, o si no es valida.
            timeStamp = time_mt('datetime')
            userID = sessionActiveUser
            wrtTables = []
            activityID = self._activityID if tblRA.getVal(0, 'fldFK_NombreActividad') is None \
                else tblRA.getVal(0, 'fldFK_NombreActividad')
            idActividadRA = tblRA.getVal(0, 'fldID')
            if idActividadRA is None:
                # NO se paso idActividadRA->Insertar records en tblRA,tblLink
                tblRA.setVal(0, fldTimeStamp=timeStamp, fldFK_UserID=userID, fldFK_NombreActividad=activityID)
                idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))
                if type(idActividadRA) is str:
                    retValue = f'ERR_DB_WriteError - Function/Method {moduleName()}({lineNum()}) - ' \
                               f'{callerFunction()}'
                    krnl_logger.info(retValue)
                    print(f'{retValue}', dismiss_print=DISMISS_PRINT)
                    return retValue  # Sale si hay error de escritura en RA.
                # Setea valores en tabla Link SOLO si se inserto registro en RA
                commentLink = tblLink.getVal(0, 'fldComment') if tblLink.getVal(0, 'fldComment') is not None else \
                    f'Activity: {self.__activityName} / ActividadTM: {tblData.getVal(0, "fldFK_ActividadTM")}'
                tblLink.setVal(0, fldFK_Actividad=idActividadRA, fldFK=self.outerObject.getID,fldComment=commentLink)
                wrtTables.append(tblLink)  # Si no se paso idActividad RA, escribe tabla Link

            # Setea valores en tabla Data y escribe
            eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), defaultVal=time_mt('datetime'), **kwargs)
            tblData.setVal(0, fldFK_Actividad=idActividadRA, fldDate=eventDate)
            tblLink.undoOnError = True  # Se deben deshacer escrituras previas si falla escritura de __tblLinkName
            wrtTables.append(tblData)

            retValue = [setRecord(j.tblName, **j.unpackItem(0)) for j in wrtTables]
            # Si hay error de escritura en tblLink o tblData, hace undo de tblRA y sale con error.
            if any(isinstance(val, str) for val in retValue):
                if tblRA.getVal(0, 'fldID') is None:
                    _ = delRecord(tblRA.tblName, idActividadRA)
                for j, tbl in enumerate(wrtTables):
                    if isinstance(retValue[j], int):
                        _ = delRecord(tbl.tblName, retValue[j])
                retValue = 'ERR_DB_WriteError' + f' {moduleName()}({lineNum()})'
                krnl_logger.error(retValue)
                print(f'Deleting record {idActividadRA} / Table: {tblRA.tblName} / retValue: {retValue}',
                      dismiss_print=DISMISS_PRINT)
            else:
                retValue = idActividadRA
        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - tm.set()'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}  - {retValue}', dismiss_print=DISMISS_PRINT)

        return retValue

    __tblMontosName = 'tblDataTMMontos'
    def _getTM(self, sDate='', eDate='', *args, **kwargs):
        """
        Returns records in table [Data MoneyActivity Montos] between sValue and eValue. sValue=eValue ='' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode=datatable->Returns DataTable with full record.  mode='fullRecord' -> Returns last Record
        in full.
        @return: Object DataTable with information from MoneyActivity Montos. None if nothing's found
        """
        retValue = None
        if self._isValid and self.outerObject.validateActivity(self.__activityName):
            tblRA = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblRAName),
                           DataTable(self._tblRAName))
            tblLink = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblLinkName),
                           DataTable(self._tblLinkName))
            tblData = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblDataName), None)
            tmActivityRecords = self._getRecordsLinkTables(tblRA, tblLink, tblData)
            colAnimalActivity = tmActivityRecords.getCol('fldFK_ActividadTM')  # Recs c/ ID_ActividadTM de ID_Animal
            if colAnimalActivity:
                qryTable = getRecords(self.__tblMontosName, '', '', None, '*', fldFK_Actividad=colAnimalActivity)
                if qryTable.dataLen:
                    if qryTable.dataLen <= 1:
                        retValue = qryTable  # qryTable tiene 1 solo registro (Ultimo o Primero)
                    else:
                        retValue = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
                    print(f'{moduleName()}({lineNum()}) retTable: {retValue.fldNames} // Data: {retValue.dataList}',
                          dismiss_print=DISMISS_PRINT)
            else:
                retValue = None  # Retorna None si no se encuentra nada.
        return retValue


    def _getRecordsLinkTables(self, tblRA, tblLink, tblData: DataTable = None, *, activity_list=('*',),
                              outer_obj_id=None):
        """
        Reads DB records out of a join of RA and Link Tables. If Data Table is provided, returns records from Data Table
        otherwise returns records from tblRA.
        @param tblRA: Registro De Actividades. DataTable object.
        @param tblLink: Tabla Link.DataTable object.
        @param tblData: Tabla Data opcional (Data Inventario, Data Status, etc). DataTable object.
        @param activity_list: Activity Names (table RA) for which records are pulled. Activities are checked against
        Activities dictionary defined for the object. * -> All Activities
        @return: 1 Object class DataTable with: All field Names from table Registro De Actividdades OR
                                                All field Names from Data Table (if passed)
        """
        activity_list = activity_list if isinstance(activity_list, (list, tuple, set)) else ('*', )
        activity_list = set([j.strip() for j in activity_list if isinstance(j, str)])  # Filtra activity_list
        tblRA_fldID = tblRA.getDBFldName("fldID")
        strWhere1 = ';'
        if '*' not in activity_list:
            # activityDict es {activity_name: activityID}
            activityDict = {k: self.activities.get(k) for k in activity_list if k in self.activities}
            if activityDict:
                activityString = '('
                for i in activityDict:  # ------- Arma string de con todos los Nombres Actividad seleccionados.
                    activityString += f'"{activityDict[i]}"'
                    activityString += ', ' if i != list(activityDict.keys())[-1] else ')'
                strWhere1 = f'AND "{tblRA.dbTblName}"."{tblRA.getDBFldName("fldFK_NombreActividad")}" IN ' \
                            f'{activityString}; '

        strSelect = 'SELECT '+(f'"{tblData.dbTblName}".*' if tblData.tblName is not None else f'"{tblRA.dbTblName}".* ')
        strFrom = f' FROM "{tblRA.dbTblName}" INNER JOIN "{tblLink.dbTblName}" USING ("{tblRA_fldID}") '
        joinDataTable = f' INNER JOIN "{tblData.dbTblName}" USING ("{tblRA_fldID}") ' if tblData is not None else ''
        val = outer_obj_id or \
              (('"'+self.outerObject.ID+'"') if isinstance(self.outerObject.ID, str) else self.outerObject.ID)
        strWhere0 = f'"{tblLink.dbTblName}"."{tblLink.getDBFldName("fldFK")}"={val} '

        strSQL = strSelect + strFrom + joinDataTable + 'WHERE ' + strWhere0 + strWhere1
        # print(f'ReadLinkTable-SUPPORT CLASSES({lineNum()}) - strSQL: {strSQL} ', dismiss_print=DISMISS_PRINT)
        dbReadTbl = tblData.tblName if isinstance(tblData, DataTable) else tblRA.tblName
        return dbRead(dbReadTbl, strSQL)


    def _createActivityRA(self, *args: DataTable, tbl_data_name='', **kwargs):
        """
        General Function to set tables [Registro De Actividades], [Link Actividades], [Data Table] for 1 ACTIVITY ONLY,
        to values specified by arguments. Used to reduce duplication of code with multiple methods.
        If fldID is not provided in argTblRA, new records are created in RA and Table Link.
        argTblRA, argTblLink, argTblData MUST BE VALID AND CONSISTENT, OTHERWISE DATA CORRUPTION IN DB WILL OCCUR.
        @param tbl_data_name: tblData Name when tblData cannot be univocally assigned.
        @param args: tblRA, tblLink, tblData: list of DataTable objects, with all the tables and fields to write to DB.
        @param kwargs: Additional obj_data tablas to write (Inventory, etc): WRITTEN AS PASSED. Must come here complete!
               recordInventory: overrides the default _doInventory setting for the Activity
        @return: Success: ID_Actividad RA (int), Error: errorCode (str)
        """
        # Referencia a DataTables pasadas en args para poder pasar resultados al caller (en particular fldID de tblLink)
        args = [j for j in args if isinstance(j, DataTable)]
        tblRA = next((j for j in args if j.tblName == self._tblRAName), DataTable(self._tblRAName))
        tblLink = next((j for j in args if j.tblName == self._tblLinkName), DataTable(self._tblLinkName))
        if tbl_data_name:
            self.__tblDataName = tbl_data_name
        tblData = next((j for j in args if j.tblName == self._tblDataName), DataTable(self._tblDataName))
        timeStamp = time_mt('datetime')   # TODO(cmt): timeStamp (para RA) debe ser SIEMPRE tiempo monotonico
        eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), timeStamp=timeStamp, **kwargs)
        activityID = tblRA.getVal(0, 'fldFK_NombreActividad') or self._activityID
        userID = tblRA.getVal(0, 'fldFK_UserID') or sessionActiveUser
        fldFK_val = tblLink.getVal(0, 'fldFK') or self.outerObject.ID
        idActividadRA = tblRA.getVal(0, 'fldID')
        commentRA = tblRA.getVal(0, 'fldComment', '') or ''
        commentRA += (' ' if commentRA else '') + f'ObjectID:{fldFK_val} / {self._activityName}'
        tblRA.setVal(0, fldTimeStamp=timeStamp, fldFK_UserID=userID, fldFK_NombreActividad=activityID,
                     fldComment=commentRA)
        if not idActividadRA:
            idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))  # TODO(cmt): AQUI GENERA DB LOCKED ERROR DESDE FRONTEND
            if isinstance(idActividadRA, str):
                retValue = f'ERR_DB_WriteError: {idActividadRA} - {moduleName()}({lineNum()}) - {callerFunction()})'
                krnl_logger.warning(retValue)
                return retValue  # Sale c/error
            tblRA.undoOnError = True
        else:
            # Se paso idActividad RA en tblRA: TODO(cmt) Busca fldID de tblLink (que debe existir por la correspondencia
            #  1->1 entre tblRA y tblLink) y setea al valor correspondiente p/ generar UPDATE de tblLink
            temp = getRecords(tblLink.tblName, '', '', None, 'fldID', fldFK=fldFK_val, fldFK_Actividad=idActividadRA)
            if not isinstance(temp, str):
                recordID = temp.getVal(0, 'fldID')
                if recordID:
                    tblLink.setVal(0, fldID=recordID)

        commentLink = tblLink.getVal(0, 'fldComment') or ''
        commentLink += (' ' if commentLink else '') + f'Activity: {activityID} / {eventDate}'
        tblLink.setVal(0, fldFK_Actividad=idActividadRA, fldFK=fldFK_val, fldDB_ID=MAIN_DB_ID, fldComment=commentLink)
        tblLink.undoOnError = True
        idLinkRecord = setRecord(tblLink.tblName, **tblLink.unpackItem(0))
        if isinstance(idLinkRecord, str):
            # TODO(cmt): condicion para chequear si hay que borrar record en tlbRA en caso de errores de escritura.
            if tblRA.getVal(0, 'fldID') is None:
                retValue = f'ERR_DB_WriteError: {idLinkRecord}. Table Name: {tblLink.tblName} - {callerFunction()})'
                _ = delRecord(tblRA.tblName, idActividadRA)
                krnl_logger.error(retValue)
                print(f'Deleting record {idActividadRA}. Table: {tblRA.tblName} - Function retValue: {retValue}',
                      dismiss_print=DISMISS_PRINT)
                return retValue  # Sale c/error
        tblLink.setVal(0, fldID=idLinkRecord)  # TODO(cmt): seteo CRITICO para evitar errores "UNIQUE Constraint Failed"

        # 3. Setea tblData. Aqui se indica a setRecord() actualizar data (lastInventory, lastCategory, etc) en memoria
        #  ->NO SETEAR fldID=idActividadRA aqui: Se usa abajo para ejecutar undo del record de tblRA si hay write error
        commentData = tblData.getVal(0, 'fldComment') or ''
        commentData += (' / ' if commentData else '') + f'ObjectID: {fldFK_val}'
        tblData.setVal(0, fldFK_Actividad=idActividadRA, fldDate=eventDate, fldComment=commentData, fldFK_UserID=userID)
        idDataRecord = setRecord(tblData.tblName, **tblData.unpackItem(0))  # setRecord() escribe en DB y actualiza mem.
        if isinstance(idDataRecord, str):
            retValue = f'ERR_DB_WriteError: {idDataRecord} - {moduleName()}({lineNum()}) - {callerFunction()})'
            krnl_logger.error(retValue)
            if tblRA.getVal(0, 'fldID') is None:
                _ = delRecord(tblRA.tblName, idActividadRA)
                _ = delRecord(tblLink.tblName, idLinkRecord)
                krnl_logger.info(f'Deleting records {tblRA.tblName}:{idActividadRA}; {tblLink}{idLinkRecord} - '
                                 f'Function retValue: {retValue}')
        else:
            # print(f'((((((((((( createActivityRA() -------------- Just wrote to {tblData.tblName}: {dicto}',
            #       dismiss_print=DISMISS_PRINT)
            tblData.setVal(0, fldID=idDataRecord)
            tblRA.setVal(0, fldID=idActividadRA)
            retValue = idActividadRA

        return retValue

    def __isClosingActivity(self, paObj=None, *, execute_fields=None, outer_obj=None, excluded_fields=(),
                            excl_mode='append', trunc_val='day', **kwargs):
        """Compares self with paObj to determine whether self qualifies as a closure activity for paObj.
        _executeFields must be fully populated with valid data.
        self determines which fields will be compared. Hence not all the fields in obj dictionaries may be compared.
        If a field in _executeFields is not present in the prog. activity data (__progDataFields), the data is not compared.
        @param execute_fields: Execution fields for Activity. Must be local argument to allow for code nesting/reentry.
        @param trunc_val: 'day', 'hour', 'minute', 'second' (str). datetime fields to be truncated for comparison.
                         None -> Nothing truncated.
        @param paObj: progActivity expected to be closed.
        @param excluded_fields: fields to exclude from comparison
        @param excl_mode: 'append': appends the list passed to __paExcludedFieldsDefault.
                          'replace': replaces the __paExcludedFieldsDefault list with the list passed.
        @param kwargs: Alternative for passing a dictionary when obj is not passed. See how this goes...
        @return True/False based on the comparison result."""
        if isinstance(paObj, ProgActivity) or kwargs:
            executeFields = execute_fields if isinstance(execute_fields, dict) else {}
            executeDate = executeFields.get('execution_date', '') or executeFields.get('execute_date', '')
            if not isinstance(executeDate, datetime):
                try:
                    executeDate = datetime.strptime(executeDate, fDateTime)
                except (TypeError, ValueError):
                    executeDate = None
            if not isinstance(executeDate, datetime) or outer_obj is None:
                return False            # Bad execution_date or missing outer_obj, exits with False.

            d2 = paObj.getPAFields() if paObj else kwargs  # paFields son campos de Cierre (ProgActivity Close Data)
            matchDates = pa_match_execution_date(executeDate, d2)  # Compares execution_date separately for ease of use.
            if not matchDates:
                print(f' ***** Execution dates {executeDate} not in the range of dates in {d2}. Exiting')
                return False
            executeFields.pop('execution_date', None)  # Removes execution_date, as it will be checked separately.

            # Activity execution data to compare with programmed activities and find a progActivity to close (if one
            # exists). executeFields MUST BE LOCAL VARIABLE in order to support concurrent execution of Activity Class
            # code (simultaneous calls to Activity Class methods from same or different threads)
            if excl_mode.strip().lower().startswith('repl') and excluded_fields:
                exclFields = set(excluded_fields)
            else:
                exclFields = self._excluded_fields
                if isinstance(excluded_fields, (list, tuple, set)):
                    exclFields.update(excluded_fields)

            d1Comp = executeFields.copy()
            d2Comp = d2.copy()          # Aux dictionary. Original d2 must be retained with all keys.
            for k in exclFields:
                d1Comp.pop(k, None)   # Removes all excluded_fields from comparions.   , d2Comp.pop(k, None)

            print(f'__isClosingActivity(1111) Data:\nexecuteFields:{executeFields}\nd1Comp:{d1Comp} - d2Comp:{d2Comp}',
                  dismiss_print=DISMISS_PRINT)

            # TODO(cmt) COMPARISON RULES. keys in execute_fields compare as follows:
            #  All fields in d1Comp are compared with the data in d2Comp: If one match with d2 gives False, the
            #  comparison is False. If any of keys in d1Comp is not present in d2 -> also False.
            #  1) If 'fld' particle in key it's DB field name, uses compare() function. Else, flattens d2 to a list of
            #  dicts and:
            #  2) For every dict in d2Flat. attempts comp method: outer_obj.getattr(outer_obj,d1Comp[k]).comp(d[k]).
            #  If that fails, res is = False (comp() is not implemented for k and k is NOT a fldName, hence False).
            #  2.a. First, uses compare() with fields of the form "tblName.fldName", using getFldCompare().
            #  2.b Else if no dict was found in d2Flat then k may be a db field name: searches for keyname using the
            #  "shortname" property to fetch a field name particle, pulls the compare value (comp_val) and runs
            #  outer_obj.getattr(outer_obj, d1Comp[k]).comp(comp_val).
            d2Flat = list(nested_dict_iterator_gen(d2))
            matchResults = {}
            for k in d1Comp:
                fmt_k = k.lower().strip()       # formatted k.
                # Here k can be in the form of "fldName", "tblName.fldName" or special name ('category, 'age', etc).
                # if form  "tblName.fldName" is detected, must prioritize comparison using getFldCompare().
                # Priority of execution:
                # If dkey from dic == k OR dkey is contained in k, comp_val is pulled from that dkey.
                #    k may or may not contain "fld" particle, so that "age", "localization", etc. compare OK.
                # 1) Gets comparison value (comp_val) first via direct string comparison. If that fails uses shortName.
                comp_val = next((dic[dkey] for dic in d2Flat for dkey in dic if
                                 (dkey.lower().strip() in k.lower() or fmt_k in dkey.lower())), VOID)
                if comp_val == VOID:
                    if "fld" not in fmt_k:
                        # k is a special name: gets shortName from k to compare with each dkey (fldName,special,proper).
                        comp_val = next((dic[dkey] for dic in d2Flat for dkey in dic if
                                         fmt_k[:min(len(fmt_k), 6)] in dkey.lower()), VOID)
                    else:
                        # k is fldName: gets dkey shortName for each dkey to compare with k (fldName,special,proper).
                        comp_val = next((dic[dkey] for dic in d2Flat for dkey in dic if
                                         dkey.lower().strip()[:min(len(dkey.strip()), 6)] in k.lower()), VOID)
                # 2) With comp_val in hand:
                # 2.1) if k has the "fld" particle in it:
                if 'fld' in fmt_k:  # si k contiene fld -> NO es atributo y NO tiene comp() definido: usa compare()
                    # 2.1.1) If "." in k, assumes proper names for k and dkey and checks using getFldCompare()
                    if "." in k:    # if "." in fldName => proper field name: uses Compare_Index
                        # Here k and dkey must be both VALID    "tblName.fldName" strings, and they CAN BE DIFFERENT.
                        comp_val = next((dic[dkey] for dic in d2Flat for dkey in dic if getFldCompare(k, dkey)), VOID)
                    # 2.1.2) If "." NOT in k, uses comp_val obtained in the beginning and uses compare().
                    res = compare(d1Comp[k], comp_val)   # if not d but 'fld' in k.lower() => then res = False.
                else:
                    # 2.2.1) If "fld" not in k tries to pull an attribute named "k" from outer_obj and execute comp().
                    try:            # try executes successfully if k implements comp() method.
                        res = getattr(outer_obj, k.strip()).comp(comp_val)
                    except (AttributeError, NameError, TypeError, ValueError):
                        # 2.2.2 If getting attribute named "k" fails or comp() is not implemented, returns False.
                        res = False  # if comp() is not implemented, then key is not found, match is False.
                matchResults[k] = res

            print(f' ***** Compare Results = {matchResults}; execution date: {matchDates}', dismiss_print=DISMISS_PRINT)
            return all(j for j in matchResults.values()) and matchDates
        return False


    def __isClosingActivity01(self, paObj=None, *, execute_fields=None, outer_obj=None, excluded_fields=(),
                              excl_mode='append', trunc_val='day', **kwargs):
        """Compares self with paObj to determine whether self qualifies as a closure activity for paObj.
        _executeFields must be fully populated with valid data.
        self determines which fields will be compared. Hence not all the fields in obj dictionaries may be compared.
        If a field in _executeFields is not present in the prog. activity data (__progDataFields), the data is not compared.
        @param execute_fields: Execution fields for Activity. Must be local argument to allow for code nesting/reentry.
        @param trunc_val: 'day', 'hour', 'minute', 'second' (str). datetime fields to be truncated for comparison.
                         None -> Nothing truncated.
        @param paObj: progActivity expected to be closed.
        @param excluded_fields: fields to exclude from comparison
        @param excl_mode: 'append': appends the list passed to __paExcludedFieldsDefault.
                          'replace': replaces the __paExcludedFieldsDefault list with the list passed.
        @param kwargs: Alternative for passing a dictionary when obj is not passed. See how this goes...
        @return True/False based on the comparison result."""
        if isinstance(paObj, ProgActivity) or kwargs:
            executeFields = execute_fields if isinstance(execute_fields, dict) else {}
            executeDate = executeFields.get('execution_date', '')
            if not isinstance(executeDate, datetime):
                try:
                    executeDate = datetime.strptime(executeDate, fDateTime)
                except (TypeError, ValueError):
                    executeDate = None
            if not isinstance(executeDate, datetime) or outer_obj is None:
                return False            # Bad execution_date or missing outer_obj, exits with False.

            d2 = paObj.getPAFields() if paObj else kwargs  # paFields son campos de Cierre (Execute Data)
            matchDates = pa_match_execution_date(executeDate, d2)  # Compares execution_date separately for ease of use.
            if not matchDates:
                print(f' ***** Execution dates {executeDate} not in the range of dates in {d2}. Exiting')
                return False
            executeFields.pop('execution_date', None)  # Removes execution_date, as it will be checked separately.

            # Activity execution data to compare with programmed activities and find a progActivity to close (if one
            # exists). executeFields MUST BE LOCAL VARIABLE in order to support concurrent execution of Activity Class
            # code (simultaneous calls to Activity Class methods from same or different threads)
            if excl_mode.strip().lower().startswith('repl') and excluded_fields:
                exclFields = set(excluded_fields)
            else:
                exclFields = self._excluded_fields
                if isinstance(excluded_fields, (list, tuple, set)):
                    exclFields.update(excluded_fields)

            d1Comp = executeFields.copy()
            d2Comp = d2.copy()   # Aux dictionary. Original d2 must be retained with all keys.
            for k in exclFields:
                d1Comp.pop(k, None)   # Removes all excluded_fields from comparions.   , d2Comp.pop(k, None)

            print(f'__isClosingActivity(1111) Data:\nexecuteFields:{executeFields}\nd1Comp:{d1Comp} - d2Comp:{d2Comp}',
                  dismiss_print=DISMISS_PRINT)
            # added, removed, changed = dictCompare(d1Comp, d2Comp, compare_args=deepDiff_args.update(
            #            {'ignore_type_in_groups': self.__deepDiff_ignore_types, 'truncate_datetime': trunc_val}))

            # TODO(cmt) COMPARISON RULES. keys in execute_fields compare as follows:
            #  All fields in d1Comp are compared with the data in d2Comp: If one match with d2 gives False, the
            #  comparison is False. If any of keys in d1Comp is not present in d2 -> also False.
            #  1) If 'fld' particle in key it's DB field name, uses compare() function. Else, flattens d2 to a list of
            #  dicts and:
            #  2) For every dict in d2Flat. attempts comp method: outer_obj.getattr(outer_obj,d1Comp[k]).comp(d[k]).
            #  If that fails, res is = False (comp() is not implemented for k and k is NOT a fldName, hence False).
            #  2.a. First, uses compare() with fields of the form "tblName.fldName", using getFldCompare().
            #  2.b Else if no dict was found in d2Flat then k may be a db field name: searches for keyname using the
            #  "shortname" property to fetch a field name particle, pulls the compare value (comp_val) and runs
            #  outer_obj.getattr(outer_obj, d1Comp[k]).comp(comp_val).
            d2Flat = list(nested_dict_iterator_gen(d2))
            matchResults = {}
            for k in d1Comp:
                # Here k can be in the form of "fldName" or "tblName.fldName".
                # if form  "tblName.fldName" is detected, must prioritize comparison using getFldCompare().
                # Priority of execution:
                # 1) if dkey from dic == k OR dkey is contained in k, dic2 is populated with that dic from d2Flat.
                # 2) if 1 fails, executes getFldCompare(k, dkey). This results in dic2 populated or dic2={} if fails.
                dic2, dic2val = next(((dic, dic[dkey])for dic in d2Flat for dkey in dic if (dkey.lower().strip()
                                      in k.lower() or k.lower().strip() in dkey.lower())), ({}, VOID))
                if 'fld' in k.lower():  # si k contiene fld -> NO es atributo y NO tiene comp() definido: usa compare()
                    if "." in k: # if "." in fldName => proper field name: uses Compare_Index #if not dic2 and "." in k:
                        # Here, k and dkey must be both VALID (proper) "tblName.fldName" strings.
                        dic2val = next((dic[dkey] for dic in d2Flat for dkey in dic if getFldCompare(k, dkey)), VOID)
                    res = compare(d1Comp[k], dic2val)   # if not d but 'fld' in k.lower() => then res = False.
                elif dic2:
                    try:
                        res = getattr(outer_obj, k).comp(dic2val)  # executes successfully if k implements comp() method
                    except (AttributeError, NameError, TypeError, KeyError, ValueError):
                        res = False          # if not: it's not a field, it's not an attribute => result = False.
                else:
                    # NO key in d2Flat could be matched to k: pulls keyname using shortName property in Activity class.
                    try:
                        comp_val = next((dic[dkey] for dic in d2Flat for dkey in dic if
                                         getattr(outer_obj, k).shortName.lower() in dkey.lower().strip()), VOID)
                    except (AttributeError, NameError):
                        res = False         # if shortName is not implemented, then key is not found, match is False.
                    else:
                        try:                # if shortName is implemented, tries with comp() method.
                            res = getattr(outer_obj, k).comp(comp_val)
                        except (AttributeError, NameError, TypeError, KeyError, ValueError):
                            res = False     # if comp() is not implemented, then key is not found, match is False.
                matchResults[k] = res

            print(f' ***** Compare Results = {matchResults}; execution date: {matchDates}', dismiss_print=DISMISS_PRINT)
            return all(j is True for j in matchResults.values()) and matchDates
        return False


    def __isClosingActivity_Original(self, paObj=None, *, execute_fields=None, outer_obj=None, excluded_fields=(),
                                     excl_mode='append', trunc_val='day', **kwargs):
        """Compares self with paObj to determine whether self qualifies as a closure activity for paObj.
        _executeFields must be fully populated with valid data.
        self determines which fields will be compared. Hence not all the fields in obj dictionaries may be compared.
        If a field in _executeFields is not present in the prog. activity data (__progDataFields), the data is not compared.
        @param execute_fields: Execution fields for Activity. Must be local argument to allow for code nesting/reentry.
        @param trunc_val: 'day', 'hour', 'minute', 'second' (str). datetime fields to be truncated for comparison.
                         None -> Nothing truncated.
        @param paObj: progActivity expected to be closed.
        @param excluded_fields: fields to exclude from comparison
        @param excl_mode: 'append': appends the list passed to __paExcludedFieldsDefault.
                          'replace': replaces the __paExcludedFieldsDefault list with the list passed.
        @param kwargs: Alternative for passing a dictionary when obj is not passed. See how this goes...
        @return True/False based on the comparison result."""
        if isinstance(paObj, ProgActivity) or kwargs:
            executeFields = execute_fields if isinstance(execute_fields, dict) else {}
            executeDate = executeFields.get('execution_date', '')
            if not isinstance(executeDate, datetime):
                try:
                    executeDate = datetime.strptime(executeDate, fDateTime)
                except (TypeError, ValueError):
                    executeDate = None
            if not isinstance(executeDate, datetime) or outer_obj is None:
                return False  # Bad execution_date or missing outer_obj, exits with False.

            d2 = paObj.getPAFields() if paObj else kwargs  # paFields son campos de Cierre.
            matchDates = pa_match_execution_date(executeDate, d2)  # Compares execution_date separately for ease of use.
            if not matchDates:
                print(f' ***** Execution dates {executeDate} not in the range of dates in {d2}. Exiting')
                return False
            executeFields.pop('execution_date', None)  # Removes execution_date, as it will be checked separately.

            # Activity execution data to compare with programmed activities and find a progActivity to close (if one
            # exists). executeFields MUST BE LOCAL VARIABLE in order to support concurrent execution of Activity Class
            # code (simultaneous calls to Activity Class methods from same or different threads)
            if excl_mode.strip().lower().startswith('repl') and excluded_fields:
                exclFields = set(excluded_fields)
            else:
                exclFields = self._excluded_fields
                if isinstance(excluded_fields, (list, tuple, set)):
                    exclFields.update(excluded_fields)

            d1Comp = executeFields.copy()
            d2Comp = d2.copy()  # Aux dictionary for DeepDiff comparison. Original d2 must be retained with all keys.
            for k in exclFields:
                d1Comp.pop(k, None)  # , d2Comp.pop(k, None)

            print(f'__isClosingActivity(1133) Data:\nexecuteFields:{executeFields}\nd1Comp:{d1Comp} - d2Comp:{d2Comp}',
                  dismiss_print=DISMISS_PRINT)
            # added, removed, changed = dictCompare(d1Comp, d2Comp, compare_args=deepDiff_args.update(
            #            {'ignore_type_in_groups': self.__deepDiff_ignore_types, 'truncate_datetime': trunc_val}))

            # TODO(cmt) COMPARISON RULES. keys in execute_fields compare as follows: All fields in d1Comp are compared
            #  If any match with d2 gives False, the comparison is False. Any of them is not present in d2-> also False.
            #  1) If 'fld' particle in key it's DB field name, uses compare() function. Else, flattens d2 to a list and:
            #  2) if a dict is found in d2Flat attempts outer_obj.getattr(outer_obj,d1Comp[k]).comp(d[k]) with k as key.
            #  If that fails, then res = False (comp() is not implemented for k and k is NOT a db fldName, hence False).
            #  3) Else if no dict was found in d2Flat then a db field name may have been passed in d2Flat: searches for
            #  keyname using the fldName property to fetch a field name particle, pulls the compare value (comp_val)
            #  and runs outer_obj.getattr(outer_obj, d1Comp[k]).comp(comp_val).
            d2Flat = list(nested_dict_iterator_gen(d2))
            matchResults = {}
            for k in d1Comp:
                dic2 = next((dic for dic in d2Flat for dkey in dic if dkey.lower().strip() in k.lower()),
                            {})  # check dkey==k.
                if 'fld' in k.lower():  # si key contiene fld -> NO es atributo, NO tiene comp() definido: usa compare()
                    res = compare(d1Comp[k], dic2.get(k, None))  # if not d but 'fld' in k.lower() => then res = False.
                elif dic2:
                    try:
                        res = getattr(outer_obj, k).comp(dic2[k])  # executes successfully if k implements comp()
                    except (AttributeError, NameError, TypeError, KeyError, ValueError):
                        res = False  # if not, result if False.
                else:
                    # db field name passed in d2Flat, needs to pull keyname using fldName property in Activity class.
                    comp_val = next((dic[dkey] for dic in d2Flat for dkey in dic if
                                     getattr(outer_obj, k).shortName.lower() in dkey.lower().strip()), None)
                    try:  # executes if conditions[k] implements comp().
                        res = getattr(outer_obj, k).comp(comp_val)
                    except (AttributeError, NameError, TypeError, KeyError, ValueError):
                        res = False
                matchResults[k] = res

            print(f' ***** Compare Results = {matchResults}; execution date: {matchDates}', dismiss_print=DISMISS_PRINT)
            return all(j is True for j in matchResults.values()) and matchDates
        return False

    @staticmethod
    def stopBuffers(buffer=None):
        """ Stops async_buffer by join() of the calling thread and waiting to flush all data from the async_buffer"""
        if buffer is None:
            pass        # Close all open AsyncBuffer buffers registered with Activity Class. TODO: Implement.
        Activity.__progActivityBuffer.stop()


    @staticmethod   # TODO(cmt): Passing buffer as an arg. Cannot define the buffer inside because of buffer management.
    def __paDifferedProcessing(async_buffer=None):       # This wrapper works wonders in a separate, prioritized thread!
        buffer = async_buffer

        def __aux_wrapper(func):                    # This wrapper is needed in order to pass argument async_buffer.
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):     # self is an Activity object.
                # enqueue() is the only way to access the cursor object for the call. Results are NOT pulled by default.
                # Only if the buffer queue is running.
                # print(f'WWWWWWWWWWWWWWWWrapper -> Queuer: {func.__name__} / buffer stopped: {buffer.is_stopped()}')
                if not buffer._is_stopped:      # Accesses _is_stopped directly to minimize use of locks.
                    # print(f'\nlalala _paMatchAndClose({lineNum()})!! : self:{self}  /args: {args} / kwargs: {kwargs}'
                    #       f' / outer_obj_id: {self.outerObject.ID} ')
                    """ MUST pass outerObject for the thread because __outerAttr is dynamic & thread-dependent. """
                    if not kwargs.get('outer_obj'):    # if outer_obj is passed in _paMatchAndClose kwargs, keeps it.
                        kwargs.update({'outer_obj': self.outerObject})      # If not, pulls it from outerObject.
                    cur = buffer.enqueue(*args, the_object=self, the_callable=func, **kwargs)  # TODO:TO ANOTHER THREAD!
                    # TODO(cmt): Si espera por cur.result, se pierde toda la ganancia (50 usec va a 3 - 12 msec!)
                    if cur and kwargs.get('get_result') is True:  # Reserved feature:wait and return results if required
                        return cur.result       # result property goes into a wait loop until results are ready.
                    return None  # Normal use:return None and not wait for the processing of the cursor by writer thread
                return func(self, *args, **kwargs)      # Si no hay AsyncBuffer, ejecuta normalmente.
            return wrapper
        return __aux_wrapper


    # @staticmethod               # TODO(cmt). Original method. Working version.
    # def __paDifferedProcessing_Old(func):  # This wrapper works wonders!
    #     @functools.wraps(func)
    #     def wrapper(self, *args, **kwargs):  # self is an Activity object.
    #         # enqueue() is the only way to access the cursor object for the call. Results are NOT pulled by default.
    #         try:
    #             # print(f'\nlalala _paMatchAndClose({lineNum()})!! : self:{self}  /args: {args} / kwargs: {kwargs}'
    #             #       f' / outer_obj_id: {self.outerObject.ID} ')
    #             """ MUST pass outerObject for this thread because __outerAttr is dynamic and thread-dependent. """
    #             kwargs.update({'outer_obj': self.outerObject})
    #             cur = self.__progActivityBuffer.enqueue(args=args, kwargs=kwargs, the_object=self, the_callable=func)
    #             # TODO(cmt): Si espera por cur.result, se pierde toda la ganancia (70 usec va a 3-12 msec si espera!)
    #             if cur and kwargs.get('get_result') is True:  # Reserved feature to wait/return results if required
    #                 return cur.result
    #             return None  # Normal use:return None and not wait for the processing of cursor by the writer thread
    #         except(AttributeError, NameError):
    #             return func(self, *args, **kwargs)  # Si no hay AsyncBuffer, ejecuta normalmente.
    #
    #     return wrapper

    @timerWrapper(iterations=10)
    @__paDifferedProcessing(__progActivityBuffer)
    def _paMatchAndClose(self, idClosingActivity=None, *args, outer_obj=None, execute_fields=None, excluded_fields=None,
                         closing_status=None, force_close=False):
        """ self is an executed Activity object.
        Closes a programmed activity for given idObject. Sets closure status in accordance with closing status of the
        activity. The calling activity (self) is Closing Activity. The Activity to be closed must be found in RAP.
        @param args: DataTable arguments (tblLinkPA, tblPADataStatus).
        @param execute_fields: {fldName: fldValue, } dict with execution fields to compare against progActivities fields
        @param idClosingActivity: idActivityRA (attached to self) that works as closing activity.
        @param closing_status: string. Optional. A key from _paStatusDict. If 'closedBaja' is passed, closes ALL
        activities in myProgActivities for outer object.
        @param force_close: Forces closing of ProgActivity without checking any conditions in execute_fields.
        @param kwargs: Dict with ProgActivity Data
        @return: idLinkPARecord with data for Activity and object (Animal, etc) for which activity was closed.
        """
        # TODO(cmt): Below the only 2 lines required to decorate any Activity method with __funcWrapper. This is
        #  because outerObject is a dynamic attribute associated to an executing thread. When a thread switches,
        #  so does the outerObject value (which is kept in a dictionary, attached to each threadID).
        if not outer_obj:
            outer_obj = self.outerObject  # Si NO se llama desde __funcWrapper, se usa self.outerObject.

        tblDataStatus = setupArgs(self.__tblPADataStatusName, *args)
        tblLinkPA = setupArgs(self._tblLinkPAName, *args)

        timeStamp = time_mt('datetime')
        eventDate = datetime_from_kwargs(timeStamp, **execute_fields)
        # 1. Lista de paObjects con ActivityID == executed Activity. Y Lista de paID tomada de esos objetos.
        if 'baja' in self._activityName.lower():
            myPAList = list(outer_obj.myProgActivities.keys())  # If it's Baja: close all ProgActivities for outerObject
            closing_status = 'closedBaja'
        else:
            myPAList = [o for o in outer_obj.myProgActivities if o.activityID == self._activityID]

        myPAIDList = [o.ID for o in myPAList]     # TODO(cmt): ID list of all activities for outerObject and activityID

        # print(f'\nlalala _paMatchAndClose({lineNum()})!! : self:{self} /idClosing: {idClosingActivity} /args: {args}'
        #       f' / outer_obj_id: {outer_obj.ID} / outer_obj: {outer_obj}')

        # tblLink = getRecords(self._tblLinkName, '', '', None, fldFK_Actividad=idClosingActivity, fldFK=outer_obj)
        # if tblLink.dataLen:
        #     linkRec = tblLink.unpackItem(0)

        # 2. Para cada record, comparar parametros de la actividad self con los parametros de cada registro leido de DB.
        # Cuando hay match, retornar el valor de ID_Actividad Programada
        # Lista de progActivities de tblRAP presentes en myProgActivities con _activityID igual a ID actividad ejecutada
        tblRAP = getRecords(self._tblRAPName, '', '', None, '*', fldID=myPAIDList)
        dataProgRecordsCol = tblRAP.getCol('fldFK_DataProgramacion')
        if not dataProgRecordsCol:
        #     # print(f'******** Prog. Activity {self._activityName} for {outer_obj.__class__.__name__} #'
        #     #       f'{outer_obj.ID} not found. Nothing closed. Exiting.', dismiss_print=DISMISS_PRINT)
            return None

        tblDataProg = getRecords(self.tblDataProgramacionName(), '', '', '*', fldID=dataProgRecordsCol)
        tblLinkedRecords = getRecords(tblLinkPA.tblName, '', '', None, '*', fldFK_Actividad=myPAIDList,
                                      fldFK=outer_obj.ID)  # Some these records to be closed w/ idClosingActivity
        paFields = {}
        tblData_index = 0       # Index para tblDataStatus
        closedPARecord = []
        retValue = None
        for j in range(tblRAP.dataLen):
            paObj = None            # Resetea paObj, porque es condicion de ejecucion mas abajo...
            # 3. Loop iterando en Programacion de Actividades para definir PA y comparar con parametros executeFields.
            j_link, linkRecordDict = next(((i, tblLinkedRecords.unpackItem(i)) for i in range(tblLinkedRecords.dataLen)
                            if tblLinkedRecords.getVal(i, 'fldFK_Actividad') == tblRAP.getVal(j, 'fldID')), (None, {}))

            # Busca record en tblDataProgramacion asociado al registro de j de tblRAP
            j_prog, progRecordDict = next(((i, tblDataProg.unpackItem(i)) for i in range(tblDataProg.dataLen) if
                              tblDataProg.getVal(i, 'fldID') == tblRAP.getVal(j, 'fldFK_DataProgramacion')), (None, {}))

            if 'baja' in self._activityName.lower() or force_close:  # force_close used by paCleanup() in bkgd thread.
                # Busca en getPARegisterDict usando fldID de ProgActivity. Setea paObj para generar cierre si es 'Baja'
                paObj = next((o for o in self.getPARegisterDict() if o.ID == tblRAP.getVal(j, 'fldID')), None)

            elif progRecordDict:  # Dict. completo con parametros de tblDataProgramacion
                # print(f'EEEEEEEEEEEY: AQUI LLEGUE!!!!!!!!!! {lineNum()}')
                # Obtiene record de tblLinkPA con fldFK_Actividad=paObj.activityID y fldFK=outerObject.ID
                paFields['fldProgrammedDate'] = linkRecordDict.get('fldProgrammedDate')  # Fecha Actividad Programada.

                # Prog. Activity data to compare with executionData and find a progActivity to close.
                # paFields['fldInstanciaDeSecuencia'] = progRecordDict.get('fldInstanciaDeSecuencia', None)
                paFields['fldWindowLowerLimit'] = progRecordDict.get('fldWindowLowerLimit', 0)
                paFields['fldWindowUpperLimit'] = progRecordDict.get('fldWindowUpperLimit', 0)
                paFields['fldDaysToAlert'] = progRecordDict.get('fldDaysToAlert', 15)
                paFields['fldDaysToExpire'] = progRecordDict.get('fldDaysToExpire', 30)
                paFields['fldPAData'] = progRecordDict.get('fldPAData', {})
                paFields['fldFK_ClaseDeAnimal'] = tblRAP.getVal(0, 'fldFK_ClaseDeAnimal', None)     # from tblRAP
                paFields['fldFK_Secuencia'] = tblRAP.getVal(0, 'fldFK_Secuencia', None)             # from tblRAP
                # paFields['fldComment'] = progRecordDict.get('fldComment', '')
                exclFields = self._excluded_fields.union(excluded_fields)

                """ pass copies here because dicts are battered by __isClosingActivity(). """
                execute_fields_copy = execute_fields.copy()
                paFields_copy = paFields.copy()

                # Busca TODAS las progActivities con matching conditions y actualiza tablas con datos de cierre.
                if self.__isClosingActivity(outer_obj=outer_obj, execute_fields=execute_fields_copy,
                                            excluded_fields=exclFields, **paFields_copy) is True:
                    # Busca en getPARegisterDict porque tiene como indice de entrada el ID de la progActivity.
                    paObj = next((o for o in self.getPARegisterDict() if o.ID == tblRAP.getVal(j, 'fldID')), None)
                else:
                    print(f'EEEEEEEEEEEY: self.__isClosingActivity() dio False!!!!!!!!!!!!!!')
            else:
                pass

            if paObj:
                tblLinkedRecords.setVal(j_link, fldFK_ActividadDeCierre=idClosingActivity)
                closingStatus = self._paStatusDict.get(closing_status, None) or \
                                self.getPAClosingStatus(execute_date=execute_fields.get('execution_date', None),
                                                            pa_obj=paObj, prog_date=paFields['fldProgrammedDate'])
                if closingStatus is not None:
                    retValue = tblLinkedRecords.getVal(j_link, 'fldID')
                    # Crea nuevos registros en tblDataStatus
                    tblDataStatus.setVal(tblData_index, fldDate=eventDate, fldFK_Status=closingStatus,
                                         fldFK_Actividad=tblLinkedRecords.getVal(j_link, 'fldFK_Actividad'))
                    if tblLinkedRecords.dataLen:
                        tblLinkedRecords.setRecords()
                    if tblDataStatus.dataLen:
                        tblDataStatus.setRecords()
                    # Remueve progActivity cerrada de myProgActivities
                    outer_obj.myProgActivities.pop(paObj, None)
                    print(f'EEEEEEEEEEY {moduleName()}({lineNum()}), JUST popped {paObj.ID} from myProgActivities')
                    # Chequea Final Close del registro de progActivity en tblRAP
                    if paObj.checkFinalClose():
                        closedPARecord.append(tblRAP.getVal(j, 'fldID'))

                tblData_index += 1

        print(f'******* {moduleName()}({lineNum()}) - Prog. Activity {self._activityName}, RAP record #{closedPARecord}'
              f' closed for good.\n    *** Also closed {self._activityName} for {outer_obj}:{bool(retValue)}',
              dismiss_print=DISMISS_PRINT)
        return retValue

    def getPAClosingStatus(self, execute_date=None, *, prog_date=None, pa_obj=None):
        """ Returns either closedInTime or closedExpired based on the dates provided.
        @return: closing status (int). None: nothing should be closed."""
        if execute_date and pa_obj:
            closed_in_time = in_between_nums(execute_date, lower_limit=prog_date-timedelta(days=pa_obj._lowerWindow),
                                             upper_limit=prog_date+timedelta(days=pa_obj._upperWindow))
            if closed_in_time:
                return self._paStatusDict['closedInTime']
            elif in_between_nums(execute_date, lower_limit=prog_date-timedelta(days=pa_obj._lowerWindow),
                                                     upper_limit=prog_date+timedelta(days=pa_obj._daysToExpire)):
                return self._paStatusDict['closedExpired']
        return None

    # _paStatusDict = { 'undefined': 0,
    #                       'openActive': 1,  # TODO: REad this from db.
    #                  'openExpired': 2,
    #                  'closedInTime': 4,
    #                  'closedExpired': 5,
    #                  'closedNotExecuted': 6,
    #                  'closedBaja': 7,
    #                  'closedLocalizChange': 8,
    #                  'closedCancelled': 9,
    #                  'closedReplaced':': 10
    #                   'closedBySystem':': 10
    #                  }


    # @timerWrapper(iterations=4)
    @__paDifferedProcessing(__progActivityBuffer)
    def _paCreateExecInstance(self, *, outer_obj=None):         # self is the Activity to be created as ProgActivity.
        """ Creates EXECUTION INSTANCES from an already existing ProgActivity, based on passed conditions.
        Travels RAP when conditions for an object change and updates ProgActivities for the object.
        Only ADDS required ProgActivities. No removals here (removals are performed in the background by cleanup funcs.
        Meant to be used only from the foreground for now).
        @param conditions: Dict: keys, values for conditions that have changed for the object.
        @param outer_obj: object for which ProgActivities are to be added. Must be passed as an arg by the wrapper to
        preserve the correct value across threads.
        @return: None or strError (str).
        """
        """ Below the only 2 lines required to decorate any Activity method with __funcWrapper. """
        if not outer_obj:
            outer_obj = self.outerObject  # Si NO se llama desde __funcWrapper, se usa self.outerObject.

        if hasattr(outer_obj, 'animalClassID'):
            temp: DataTable = getRecords(self.__tblRAPName, '', '', None, '*', fldFlag=(1, 2),
                                         fldFK_ClaseDeAnimal=outer_obj.animalClassID)
        else:
            temp: DataTable = getRecords(self.__tblRAPName, '', '', None, '*', fldFlag=(1, 2))
        # print(f'{moduleName()}({lineNum()}) - JJJJJJJJJJJJJJJJJJJJJJust entering {callerFunction(getCallers=True)}')
        if isinstance(temp, str):
            krnl_logger.error(f'ERR_DBAccess: cannot read from {self.__tblRAPName}. Error: {temp}')
            return temp
        temp1 = getRecords(self.__tblDataProgramacionName, '', '',None,'*', fldID=temp.getCol('fldFK_DataProgramacion'))
        if isinstance(temp1, str):
            krnl_logger.error(f'ERR_DBAccess: cannot read from {self.__tblDataProgramacionName}. Error: {temp1}')
            return temp1

        paSet = set()
        eventDate = time_mt('datetime')
        for j in range(temp.dataLen):
            matchResults = {}
            # 1. Checks if date is within PA validity date.
            if temp.getVal(j, 'fldFlag') == 0 or (isinstance(temp.getVal(j, 'fldFechaFinDeEjecucion'), datetime)
                                                  and temp.getVal(j, 'fldFechaFinDeEjecucion') <= eventDate):
                continue    # ProgActivity no longer Active: execution instances are not to be created from it.

            # 2. Travels tblRAP pulling the associated record from tabla Data Programacion and checks conditions.
            # 05Jul23: ALL conditions defined in fldPADataCreacion must be met for 1 execution instance to be created.
            createDict = temp1.unpackItem(fldID=temp.getVal(j, 'fldFK_DataProgramacion')).get('fldPADataCreacion', {})
            for k in createDict:
                if k in self._excluded_fields:
                    createDict.pop(k)                   # para remover 'fldComment', etc.
            # Check conditions defined in createDict against current states and attributes in the target object.
            # TODO(cmt): All keys in createDict MUST match outer_obj's attributes (methods, properties or variables).
            #  First tries comp(), 2nd tries get() with compare(), 3rd assumes it's a property and also uses compare().
            #  If k is not a valid outer_obj attribute, skips and continues with next k.
            for k in createDict:
                if hasattr(outer_obj, k.strip().lower()):
                    attr = getattr(outer_obj, k.lower())
                else:
                    continue            # attribute k not found, goes to next k.
                try:  # executes if conditions[k] implements comp().
                    res = attr.comp(createDict.get(k))
                except (AttributeError, TypeError, KeyError, ValueError):
                    try:        # If comp() is not implemented, tries get()
                        res = compare(attr.get(), createDict.get(k))
                    except (AttributeError, TypeError, KeyError, ValueError):
                        # If get() is not implemented assumes it's a property (ex. dob): uses that value for compare()
                        res = compare(attr, createDict.get(k))
                matchResults[k] = res

            # 3. Checks results and adds paObj to set() if all matches are True and paObj is valid.
            if any(j is False for j in matchResults.values()):  #  or not matchResults: TODO -> REMOVE THE # !!!
                continue
            paObj = next((o for o in outer_obj.getPAClass().getPARegisterDict() if o.ID==temp.getVal(j, 'fldID')), None)
            if paObj:                                        # getPARegisterDict()={paObj: ActivityID}
                paSet.add(paObj)  # End main for loop.

        addedPA = list(paSet.difference(outer_obj.myProgActivities)) if paSet else None  # paSet not empty -> Hay PA.
        if addedPA:  # if there's new progActivities create records in tblLinkPA for outer_obj, write to DB and register
            tblLinkPA = DataTable(self.__tblLinkPAName)
            for j, o in enumerate(addedPA):
                if isinstance(o.referenceEvent, (int, float)):  # TODO: leave this option for now. See if it's of any use.
                    # referenceEvent es el dia del año a asignar a fldProgrammedDate. daysToProgDate debiera ser 0.
                    progDate = datetime(eventDate.year, 1, 1, eventDate.hour, eventDate.minute, eventDate.second) + \
                               timedelta(days=o.referenceEvent)
                    if progDate + timedelta(days=o.daysToProgDate) < eventDate - timedelta(days=o.lowerWindow):
                        progDate = datetime(progDate.year+1, progDate.month, progDate.day, progDate.hour,
                                            progDate.minute, progDate.second)      # Adds 1 yr.
                elif isinstance(o.referenceEvent, str):
                    try:
                        # TODO(cmt): o.referenceEvent is type str and outer_obj.getattr(outer_obj, o.referenceEvent) is
                        #  a property. 3 possibilities to get the fldProgrammedDate reference date (aka progDate):
                        #  If str o.referenceEvent converts to datetime, that will be the ref_date. Else:
                        #  If outer_obj.getattr(outer_obj, o.referenceEvent) returns a datetime object, that's ref_date.
                        #  Else, if outer_obj.getattr(outer_obj, o.referenceEvent) implements get(), calls get().
                        #  Else progDate = None.
                        #  Finally, if ref date is datetime, progDate = ref date + timedelta(days=o.daysToProgDate)
                        progDate = getattr(outer_obj, o.referenceEvent)
                        if not isinstance(progDate, datetime):     # dob, for instance, will return datetime directly.
                            # if value returned directly is not datetime, attempts to execute get()
                            progDate = getattr(outer_obj, o.referenceEvent).get(event_date=True)
                    except (AttributeError, TypeError, ValueError):
                        try:
                            progDate = datetime.strptime(o.progDateRef, fDateTime)  # Si es datetime asigna directamente
                        except (TypeError, ValueError):
                            progDate = None
                else:
                    progDate = None

                if isinstance(progDate, datetime):
                    if isinstance(o.daysToProgDate, (int, float)):
                        progDate += timedelta(days=o.daysToProgDate)
                    if progDate >= eventDate - timedelta(days=o.lowerWindow):
                        tblAux = getRecords(tblLinkPA.tblName,'','',None, '*', fldFK=outer_obj.ID, fldFK_Actividad=o.ID)
                        if tblAux.dataLen:
                            continue  # skips if a record for that ProgActivity ID and that target object already exists
                        # 1. Creates execution instance. This record will be repository to get myProgActivities from DB.
                        tblLinkPA.setVal(j, fldFK=outer_obj.ID, fldFK_Actividad=o.ID, fldProgrammedDate=progDate,
                                         fldComment=f'Activity {o.activityName} created by system on {eventDate}')
                        # 2. Register in PA memory dict
                        outer_obj.registerProgActivity(o)
            # Actualiza tblLinkPA con todas las ProgActivities agregadas.
            if tblLinkPA.dataLen:
                tblLinkPA.setRecords()
        return None


# =================================== FIN CLASES ACTIVITY =========================================================== #
