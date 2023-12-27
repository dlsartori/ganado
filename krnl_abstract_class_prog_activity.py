from __future__ import annotations
from abc import ABC
# from krnl_parsing_functions import fldNameFromUID, tblNameFromUID
from krnl_custom_types import DataTable, setRecord, delRecord
from krnl_config import *
from threading import Lock
from krnl_exceptions import DBAccessError
from datetime import tzinfo, datetime, timedelta
# from krnl_geo_new import Geo
from krnl_custom_types import setupArgs, getRecords
"""
Implements Classes ProgActivity, ActivityTrigger
"""

def moduleName():
    return str(os.path.basename(__file__))

ACTIVITY_LOWER_LIMIT = 15
ACTIVITY_UPPER_LIMIT = 30
ACTIVITY_DAYS_TO_ALERT = 15
ACTIVITY_DAYS_TO_EXPIRE = 365
# ACTIVITY_DEFAULT_OPERATOR = None
# ALL_OBJECTS = 0         # Used in ProgActivities

# Fields to be excluded from comparisons. Used in Activity and Trigger classes



class ProgActivity(ABC):
    """
   Base class for all Programmed Activity classes in the system. Inherits to Programmed Activities for Animal, Tag, etc.
   ** This class is not to be instantiated (abstract). All instances to come from the subclasses spawn from this one. **
   The instances of the subclasses will be a ProgActivity object used to program future tasks on system objects.
   The interactions with Activity objects are mainly to associate a performed Activity with a given programmed activity,
   to perform updates and close progActivities based on the data provided by the performed activities.
    """
    __slots__ = ('__isValidFlag', '__activityName', '__activityID', '__enableActivity', '__tblObjectsName',
                 '__fldID_DataProg', '__animalClass', '__jsonDataClose', '_sequence', '_seqInstance', '_lowerWindow',
                 '_upperWindow', '__daysToAlert', '_daysToExpire', '__targetAgeDays', '__AgeDaysDeviation',
                 '__tblLinkName', '_decoratorName', '_dataProgDict', '_activityFields', '_dataProgramacion',
                 '__progDataFields', '__referenceEvent', '__daysToProgDate', '__conditionsCreatePA', '__jsonDataCreate',
                 '__conditionsClosePA')

    __lock = Lock()

    __paExcludedFieldsDefault = {'fldComment', 'fldFK_UserID', 'fldTimeStamp', 'fldTimeStampSync', 'fldBitmask',
                                 'fldPushUpload', 'fldFK_DataProgramacion'}

    __tblRANames = {'tblAnimalesRegistroDeActividades': 'tblAnimalesRegistroDeActividadesProgramadas',
                    'tblPersonasRegistroDeActividades': 'tblPersonasRegistroDeActividadesProgramadas',
                    'tblCaravanasRegistroDeActividades': 'tblCaravanasRegistroDeActividadesProgramadas',
                    'tblDispositivosRegistroDeActividades': 'tblDispositivosRegistroDeActividadesProgramadas',
                    'tblListasRegistroDeActividades': None,
                    'tblTMRegistroDeActividades': None,
                    'tblProyectosRegistroDeActividades': None}

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
                    'tblProyectosRegistroDeActividadesProgramadas': (None, None, None, None, None)}


    # __classRegister = {}            # {cls: classID(int), }
    #
    # @classmethod
    # def registerClass(cls):
    #     ProgActivity.__classRegister[cls] = cls.getPAClassID()       # {cls: classID(int), }
    #
    # @classmethod
    # def unregisterClass(cls):
    #     ProgActivity.__classRegister.pop(cls, None)
    #
    # @staticmethod
    # def getClassRegister():
    #     return ProgActivity.__classRegister
    #
    # @classmethod
    # def getPAClassID(cls):
    #     """ Returns progActivity Class ID (int). """
    #     return cls._paClassID                   # Accesses the right _paClassID through cls.
    #
    # @classmethod
    # def getPAClass(cls, classID):
    #     return next((k for k in cls.getClassRegister() if cls.getClassRegister()[k] == classID), None)

    @classmethod
    def getTblActivityStatusName(cls):
        return cls.__tblActivityStatusNames

    __tblActivityStatusNames = 'tblActividadesStatus'
    temp = getRecords(__tblActivityStatusNames, '', '', None, 'fldID', 'fldStatus')
    if isinstance(temp, str):
        e = f'ERR_DB_ReadError-Iniziatilization faiulre: {temp}  {callerFunction()}. Exiting... '
        krnl_logger.error(e)
        raise DBAccessError(e)
    __activityStatusDict = {}
    for j in range(temp.dataLen):
        __activityStatusDict[temp.getVal(j, 'fldStatus')] = temp.getVal(j, 'fldID')

    @classmethod
    def getActivityStatusDict(cls):
        return cls.__activityStatusDict

    @classmethod
    def progActivitiesPermittedStatus(cls):             # Implemented in subclasses.
        pass

    @classmethod
    def tblRANames(cls):
        return cls.__tblRANames

    @classmethod
    def progTables(cls):
        return cls.__progTables

    @classmethod
    def geTblLinkName(cls, tblRAName):
        return cls.__linkTables.get(tblRAName)

    @classmethod
    def getLinkTables(cls):
        return list(cls.__linkTables.values())

    @classmethod                    # Must be implemented in subclasses.
    def tblRAPName(cls):
        raise TypeError(f'ERR_SYS_Invalid call: tblRAPName called from {cls.__name__}. Must be called from a subclass.')

    PersonProgActivitiesPermittedStatus = {}        # Future development. Implement in the corresponding module.
    DeviceProgActivitiesPermittedStatus = {}        # Future development.  Implement in the corresponding module.

    def __new__(cls, *args, **kwargs):              # kwargs are required to be passed to __init__()
        if cls is ProgActivity:                     # Prevents instantiation of ProgActivity class.args,
            krnl_logger.error(f"ERR_SYS_Invalid type: {cls.__name__} cannot be instantiated, must use a subclass")
            raise TypeError(f"{cls.__name__} cannot be instantiated. Please use a subclass.")
        return super().__new__(cls)

    def __init__(self, ID=None, isValid=True, activityID=None, enableActivity=activityEnableFull, *args, RAP_dict=None,
                 prog_data_dict=None, json_dict_create=None, json_dict_close=None, **kwargs):
        """ Creates a ProgActivity object
        @param kwargs: data from tblRAP
        @param prog_data_dict: Full record from [Data Programacion De Actividades]
        @param json_dict_create: Data Actividad Programada - Creacion  (JSON/Dict), [Data Programacion De Actividades]
        @param json_dict_close: Data Actividad Programada - Cierre  (JSON/Dict) from [Data Programacion De Actividades]
        """
        if not ID:
            self.__ID = self.__isValidFlag = self.__isActive = self.__activityID = self.__activityName = None
            return

        if not isinstance(RAP_dict, dict):
            RAP_dict = {}
        self.__ID = ID      # asigna fldID (int) a __ID.
        self.__isValidFlag = isValid
        self.__Terminal_ID = RAP_dict.get('fldTerminal_ID', None)
        self.__isActive = RAP_dict.get('fldFlag', 1)  # 0: Not Active; 1: Active; 2: Permanent Activity.
        self.__animalClass = RAP_dict.get('fldFK_ClaseDeAnimal')
        self._sequence = RAP_dict.get('fldFK_Secuencia', None)
        self.__executionWindow = RAP_dict.get('fldVentanaDeEjecucion', None)
        self.__recurrence = RAP_dict.get('fldRecurrenciaDias', 0)
        self.__dateExecutionEnds = RAP_dict.get('fldFechaFinDeEjecucion', None)
        self.__enableActivity = enableActivity
        self.__activityID = activityID              # es dato de tblRAP
        self.__activityName = next((k for k in self.progActivitiesDict() if self.progActivitiesDict()[k] ==
                                    self.__activityID), None)

        # All  data below from prog_data_dict
        # fldID de registro en Programacion De Actividades. 1:1 con progActivity
        self.__fldID_DataProg = prog_data_dict.get('fldID', None)
        self.__jsonDataClose = json_dict_close     # fldPAData dict for Closure
        self._lowerWindow = prog_data_dict.get('fldWindowLowerLimit', None)
        self._upperWindow = prog_data_dict.get('fldWindowUpperLimit', None)
        self.__daysToAlert = prog_data_dict.get('fldDaysToAlert', None)
        self._daysToExpire = prog_data_dict.get('fldDaysToExpire', None)


        # Data for creation of execution instances of ProgActivity in tblLinkPA
        self.__jsonDataCreate = json_dict_create  # fldPAData dict for Creation of execution instances.
        self.__referenceEvent = prog_data_dict.get('fldProgDateReference')  # Pointer to fldProgrammedDate (int, str)
        self.__daysToProgDate = prog_data_dict.get('fldDaysToProgDate')

        super().__init__()
        # print(f'-------- __progDataFields: {prog_data_dict}\n-------- __jsonDataClose: {self.__jsonDataClose}',
        #       dismiss_print=DISMISS_PRINT)

    @property
    def ID(self):
        return self.__ID

    @property
    def dataProgramacion(self):
        """ Returns fldID of Programmed Data field for the progActivity"""
        return self._dataProgramacion

    @property
    def dbID(self):
        return self.__Terminal_ID

    def getPAFields(self):
        return self.__jsonDataClose

    @property
    def _isValid(self):
        return self.__isValidFlag

    @_isValid.setter
    def _isValid(self, val):
        self.__isValidFlag = bool(val)

    @property
    def isActive(self):
        return self.__isActive

    @property
    def activityName(self):
        return self.__activityName


    @property
    def activityID(self):
        return self.__activityID

    # @property
    # def localization(self):
    #     return self.__localization

    @property
    def referenceEvent(self):
        return self.__referenceEvent

    @property
    def daysToProgDate(self):
        return self.__daysToProgDate

    @property
    def lowerWindow(self):
        return self._lowerWindow

    @property
    def upperWindow(self):
        return self._upperWindow

    @property
    def progDateRef(self):
        return self.__referenceEvent

    @classmethod
    def raiseMethodError(cls):             # Common error exception for incorrect object/class type
        raise AttributeError(f'ERR_SYS_Invalid call: {callerFunction(depth=1, getCallers=False)} called from '
                             f'{cls.__name__}. Must be called from a subclass.')

    @classmethod
    def getPARegisterDict(cls):
        cls.raiseMethodError()               # {paObj: ActivityID}. To be called by subclasses only

    @classmethod
    def tblTriggersName(cls):                # Must be implemented in subclasses.
        cls.raiseMethodError()

    @classmethod
    def tblDataProgramacionName(cls):            # Must be implemented in subclasses.
        cls.raiseMethodError()

    @classmethod
    def tblLinkPAName(cls):
        cls.raiseMethodError()

    @classmethod
    def tblPADataStatusName(cls):
        cls.raiseMethodError()

    def getActivityID(self, tblActivityID=None):
        """ returns __activityID by querying tblActivityID. If tblActivityID is None, returns self.__activityID"""
        return tblActivityID if tblActivityID else self.activityID

    @classmethod
    def _excludedFieldsDefault(cls):
        return cls.__paExcludedFieldsDefault

    @classmethod
    def _excludedFieldsDefaultCreate(cls):
        return cls.__excludedFieldsDefaultCreate


    def packActivityParams(self, *args, **kwargs):
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

    @classmethod
    def _paCreateActivity(cls, activity_id=None, *args: DataTable, enable_activity=activityEnableFull, **kwargs):
        """ Creates a Programmed Activity (PA) of type cls and registers the created object in __registerDict.
        Records data in the RAP, Data Programacion, tblLinkPA in database.
        TO BE CALLED from Animal.paCreateActivity().
        @return: ProgActivity Object. ERR_ (str) if class is not valid or if recordActivity() finished with errors.
        """
        if cls is ProgActivity:
            return f'ERR_INP_InvaLid type. Class {cls} cannot be instantiated. Please use a subclass.'

        if activity_id not in cls.progActivitiesDict().values():  # cls pulls the right dict.
            # Actividad no definida o no valida para progActivity.
            return f'ERR_INP_InvaLid Argument(s)-{callerFunction()}: {activity_id}'

        # 1.Busca progActivity en DB (tabla RAP) que tenga mismo trigger y misma activityID). Si existe, lo retorna
        tblRAP = setupArgs(cls.tblRAPName(), *args)
        animalClassID = tblRAP.getVal(0, 'fldFK_ClaseDeAnimal') or kwargs['fldFK_ClaseDeAnimal']
        activityID = tblRAP.getVal(0, 'fldFK_Actividad', None) or activity_id
        if not animalClassID:
            retValue = f'ERR_INP_Invalid Argument(s): missing mandatory argument Clase De Animal.'
            return retValue

        # 2. Si progActivity no existe -> crea objeto y registros nuevos en DB
        retValue = None
        tblRAP.setVal(0, fldFK_Actividad=activityID, fldTerminal_ID=TERMINAL_ID)   # Sets fldTerminal_ID for later use.
        # TODO(cmt): tblDataProgramacion MUST COME FULLY populated for data integrity. tblLinkPA can be empty.
        tblLinkPA = next((j for j in args if j.tblName == cls.tblLinkPAName()), DataTable(cls.tblLinkPAName()))
        tblDataProg = next((j for j in args if j.tblName == cls.tblDataProgramacionName()),
                           DataTable(cls.tblDataProgramacionName()))
        # if any(not isinstance(j, DataTable) for j in (tblLinkPA, tblDataProg)):
        #     retValue = f'ERR_INP_Invalid Argument(s): {tblDataProg, tblLinkPA}'
        # if isinstance(tblDataProg, DataTable):
        if tblDataProg.getVal(0, 'fldPAData') is None:
            retValue = f'ERR_INP_Invalid Argument(s): {tblDataProg} missing mandatory arguments fldPAData.'
        if isinstance(retValue, str):
            krnl_logger.error(retValue)
            return retValue

        dataProgFullData = tblDataProg.unpackItem(0)
        progDataFields = set(tblDataProg.fldNames) - cls._excludedFieldsDefault()
        progDataDict = {k: dataProgFullData.get(k) for k in progDataFields}
        jsonDictClose = dataProgFullData.get('fldPAData', {})
        jsonDictCreate = dataProgFullData.get('fldPADataCreacion', {})
        if any(not j for j in (progDataDict, jsonDictClose, activityID)):
            retValue = f'ERR_INP_Invalid arguments: {tblDataProg} missing mandatory arguments for ProgActivity creation.'
            krnl_logger.error(retValue)
            return retValue

        idActivityRAP = cls.recordActivity(tblRAP, tblLinkPA, tblDataProg, *args, **kwargs)     # sets fldID in tblRAP.
        if isinstance(idActivityRAP, str):
            retValue = f'ERR_DB_Access. Cannot write to DB. Error: {tblRAP.tblName}. {activity_id} not created.'
            krnl_logger.error(retValue)
            return retValue
        RAPDict = tblRAP.unpackItem(0)
        try:
            obj = cls(RAPDict['fldID'], True, activityID, enable_activity, *args, prog_data_dict=progDataDict,
                      RAP_dict=RAPDict, json_dict_close=jsonDictClose, json_dict_create=jsonDictCreate, **kwargs)
        except (TypeError, NameError, AttributeError, ValueError, KeyError, IndexError) as e:
            krnl_logger.error(f'ERR_SYS_ProgActivity: Cannot create ProgActivity. Error: {e}')
            obj = f'ERR_SYS_ProgActivity._paCreateActivity({lineNum()}): {e}'
        return obj

    # self.__animalClass = kwargs.get('fldFK_ClaseDeAnimal')
    # self._sequence = kwargs.get('fldFK_Secuencia', None)
    # self.__executionWindow = kwargs.get('fldVentanaDeEjecucion', None)
    # self.__recurrence = kwargs.get('fldRecurrenciaDias', 0)
    # self.__dateExecutionEnds = kwargs.get('fldFechaFinDeEjecucion', None)


    @classmethod
    def loadFromDB(cls):            # TODO: 18-dec-23 TO BE DEPRECATED. ProgActivity Objects to be created on demand.
        """ Loads ProgActivities from DB Tables, creates PA Objects and returns list of objects.
        DOES NOT write to registerDict. This is to be done by the subclasses on their particular dictionaries.
        @return: PA list [pa1, pa2, ] with pa: ProgActivity Object. ERR_(str) if fails. [] if no progActivities loaded.
        """
        if cls is ProgActivity:
            return []       # Wrong class, exits with nothing.

        tblRAP = getRecords(cls.tblRAPName(), '', '', None, '*', fldFlag=(1, 2))  # Only active and permanent Activities
        tblDataProg = getRecords(cls.tblDataProgramacionName(), '', '', None, '*')
        if isinstance(tblRAP, DataTable) and isinstance(tblDataProg, DataTable):
            if tblRAP.dataLen:
                objList = []
                for j in range(tblRAP.dataLen):
                    RAPDict = tblRAP.unpackItem(j)
                    activityID = RAPDict.get('fldFK_Actividad', None)
                    idDataProg = tblRAP.getVal(j, 'fldFK_DataProgramacion')
                    dataProgFullData = next((tblDataProg.unpackItem(j) for j in range(tblDataProg.dataLen)
                                             if tblDataProg.getVal(j, 'fldID') == idDataProg), {})
                    if dataProgFullData and activityID:
                        progDataFields = set(tblDataProg.fldNames) - cls._excludedFieldsDefault()
                        progDataDict = {k: dataProgFullData.get(k) for k in progDataFields}
                        jsonDictClose = dataProgFullData.get('fldPAData', {})
                        jsonDictCreate = dataProgFullData.get('fldPADataCreacion', {})
                        try:
                            obj = cls(RAPDict['fldID'], True, activityID, activityEnableFull, RAP_dict=RAPDict,
                                      prog_data_dict=progDataDict, json_dict_create=jsonDictCreate,
                                      json_dict_close=jsonDictClose, excluded_fields=())
                            objList.append(obj)
                        except (TypeError, AttributeError, NameError, ValueError):
                            pass  # Ignores any non valid data.
                print(f'abstract_class_prog_activity PA Objects: {[j.activityID for j in objList]}')
                return objList
        else:
            err = f'ERR_DB_Cannot read table(s) from DB: {tblRAP + ", " if isinstance(tblRAP, str) else ""} ' \
                  f'{tblDataProg if isinstance(tblDataProg, str) else ""}'
            krnl_logger.error(err)
            return f'{moduleName()}({lineNum()}) - {err}'

        return []  # Returns [] if no progActivities loaded

    @classmethod
    def loadFromDB00(cls):
        """ Loads ProgActivities from DB Tables, creates PA Objects and returns list of objects.
        DOES NOT write to registerDict. This is to be done by the subclasses on their particular dictionaries.
        @return: PA list [pa1, pa2, ] with pa: ProgActivity Object. ERR_(str) if fails. [] if no progActivities loaded.
        """
        if cls is ProgActivity:
            return []  # Wrong class, exits with nothing.

        tblRAP = getRecords(cls.tblRAPName(), '', '', None, '*', fldFlag=(1, 2))  # Only active and permanent Activities
        tblDataProg = getRecords(cls.tblDataProgramacionName(), '', '', None, '*')
        if isinstance(tblRAP, DataTable) and isinstance(tblDataProg, DataTable):
            if tblRAP.dataLen:
                objList = []
                for j in range(tblRAP.dataLen):
                    RAPDict = tblRAP.unpackItem(j)
                    activityID = RAPDict.get('fldFK_Actividad', None)
                    idDataProg = tblRAP.getVal(j, 'fldFK_DataProgramacion')
                    dataProgFullData = next((tblDataProg.unpackItem(j) for j in range(tblDataProg.dataLen)
                                             if tblDataProg.getVal(j, 'fldID') == idDataProg), {})
                    if dataProgFullData and activityID:
                        progDataFields = set(tblDataProg.fldNames) - cls._excludedFieldsDefault()
                        progDataDict = {k: dataProgFullData.get(k) for k in progDataFields}
                        jsonDictClose = dataProgFullData.get('fldPAData', {})
                        jsonDictCreate = dataProgFullData.get('fldPADataCreacion', {})
                        try:
                            obj = cls(RAPDict['fldID'], True, activityID, activityEnableFull, RAP_dict=RAPDict,
                                      prog_data_dict=progDataDict, json_dict_create=jsonDictCreate,
                                      json_dict_close=jsonDictClose, excluded_fields=())
                            objList.append(obj)
                        except (TypeError, AttributeError, NameError, ValueError):
                            pass  # Ignores any non valid data.
                print(f'abstract_class_prog_activity PA Objects: {[j.activityID for j in objList]}')
                return objList
        else:
            err = f'ERR_DB_Cannot read table(s) from DB: {tblRAP + ", " if isinstance(tblRAP, str) else ""} ' \
                  f'{tblDataProg if isinstance(tblDataProg, str) else ""}'
            krnl_logger.error(err)
            return f'{moduleName()}({lineNum()}) - {err}'

        return []  # Returns [] if no progActivities loaded



    @classmethod
    def recordActivity(cls, *args: DataTable, get_tables=False, **kwargs) -> int | tuple | str:
        """                *** IMPORTANT: Accessed by background threads. ***
        Creates a programmed activity entry in tables [XX Registro De Actividades Programadas].
         All obj_data in these tables MUST BE fully filled-in by UI functions (except for object data)
         The Trigger ID (if defined for the activity), MUST be passed in the tblRAP table.
        Inserts 1 activity at a time. __activityID MUST BE EQUAL to fldID in tblRAP to preserve consistency of DB. idLink
        is the tblLink val associated to tblRAP. fldID must also be passed.
        Trigger ID is checked if passed. If an activity already exists with that Trigger ID, the function exits.
        tblLinkPA must have fields fldFK and fldProgrammedDate fully populated. 1 table record for each fldFK.
        @param get_tables: True -> returns tblRAP, tblLink, tblDataProgramacion, in that order.
        @param items: List of items to create tblLinkPA records associated to the created progActivity.
        @param args: DataTables with parameters for RAP, linkPA, tblDataProgramacion
        @param kwargs: {fldName: fldValue, }
        @return: idActividadRAP (int) or errorCode (str)
        """
        if cls is ProgActivity:   # Debe ser llamada desde subclases. Sale si se la llama desde ProgActivity.
            retValue = f'ERR_Type_Invalid class type: {cls.__class__.__name__}'
            return retValue if get_tables is False else retValue, None, None

        argsTables = set([j for j in args if isinstance(j, DataTable)]) if args else set()
        tblRAP = setupArgs(cls.tblRAPName(), *argsTables)
        if not tblRAP.getVal(0, 'fldFlag'):
            tblRAP.setVal(0, fldFlag=1)     # Si no se paso flag Activa, setea a 1.
        tblDataProgramacion = next((j for j in args if j.tblName == cls.tblDataProgramacionName()), None)
        activityID = tblRAP.getVal(0, 'fldFK_Actividad')
        tblLinkPA = next((j for j in args if j.tblName == cls.tblLinkPAName()), None)
        if activityID not in cls.progActivitiesDict().values() or not tblLinkPA:     # cls/self call the right method
            retValue = f'ERR_INP_Argument(s) missing or invalid arguments, Activity {activityID}. ' \
                       f'Prog. Activity not created'
        # TODO: fldPAData is a highly variable dict with no validation at the moment. ALL DATA in it MUST BE CORRECT!
        else:
            colItems = tblLinkPA.getCol('fldFK')
            colDates = tblLinkPA.getCol('fldProgrammedDate')
            for j in range(tblLinkPA.dataLen):
                if not isinstance(colItems[j], int) or not isinstance(colDates[j], datetime):
                    tblLinkPA.popRecord(j)  # remueve records con campos no validos de tblLinkPA, si hubiera.
            retValue = None
        if isinstance(retValue, str):
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}): {retValue} - {callerFunction()}', dismiss_print=DISMISS_PRINT)
            return retValue if get_tables is False else retValue, None, None

        timeStamp = time_mt('datetime')
        userID = sessionActiveUser
        date = tblDataProgramacion.getVal(0, 'fldDate', timeStamp) or timeStamp
        tblDataProgramacion.setVal(0,
                        fldWindowUpperLimit=tblDataProgramacion.getVal(0, 'fldWindowUpperLimit', ACTIVITY_UPPER_LIMIT),
                        fldWindowLowerLimit=tblDataProgramacion.getVal(0, 'fldWindowLowerLimit', ACTIVITY_LOWER_LIMIT),
                        fldDaysToAlert=tblDataProgramacion.getVal(0, 'fldDaysToAlert', ACTIVITY_DAYS_TO_ALERT),
                        fldDaysToExpire=tblDataProgramacion.getVal(0, 'fldDaysToExpire', ACTIVITY_UPPER_LIMIT+10),
                        fldFlagExpiration=tblDataProgramacion.getVal(0, 'fldFlagExpiration', 1),
                        fldPAData=tblDataProgramacion.getVal(0, 'fldPAData', {}),
                        fldPADataCreacion=tblDataProgramacion.getVal(0, 'fldPADataCreacion', {}),
                        fldComment=tblDataProgramacion.getVal(0, 'fldComment', ''),
                        fldDate=date
                                   )
        idDataProgramacion = setRecord(tblDataProgramacion.tblName, **tblDataProgramacion.unpackItem(0))
        if isinstance(idDataProgramacion, str):  # str: Hubo error de escritura
            retValue = f'ERR_DB_WriteError {moduleName()}({lineNum()}) - {callerFunction()}'
            krnl_logger.error(retValue)
            print(retValue, dismiss_print=DISMISS_PRINT)
            return retValue if get_tables is False else retValue, None, None

        tblDataProgramacion.setVal(0, fldID=idDataProgramacion)
        # TODO(cmt): fldPAData es JSON/Dict (convertido via PARSE_DECLTYPES). Deben venir en parametros de entrada
        # Setea campos en tblRAP
        tblRAP.setVal(0, fldID=None, fldTimeStamp=timeStamp, fldFK_DataProgramacion=idDataProgramacion,
                      fldfK_UserID=userID)
        idActividadRAP = setRecord(tblRAP.tblName, **tblRAP.unpackItem(0))
        if isinstance(idActividadRAP, str):  # str: Hubo error de escritura
            delRecord(tblDataProgramacion.tblName, idDataProgramacion)
            retValue = f'ERR_DB_WriteError. Table {tblDataProgramacion.tblName} - {callerFunction()}'
            krnl_logger.error(retValue)
            print(f'{moduleName()}({lineNum()}): {retValue}', dismiss_print=DISMISS_PRINT)
        else:
            tblRAP.setVal(0, fldID=idActividadRAP)
            retValue = idActividadRAP

        # Crea registros en tabla Link Actividades Programadas TODO: (fldProgrammedDate, fldFK DEBEN venir en tblLinkPA)
        if tblLinkPA and tblLinkPA.dataLen:
            for j in range(tblLinkPA.dataLen):
                tblLinkPA.setVal(j, fldFK_Actividad=idActividadRAP)
            _ = tblLinkPA.setRecords()  # _ count of records processed by setRecords()
            if not _:
                krnl_logger.warning(f'ERR_DBAccessError. {callerFunction(namesOnly=True)}: One or more records in table'
                                    f' {tblLinkPA.tblName} not written:  ({[j for j in _ if isinstance(j, str)]})')

        return retValue if not get_tables else (tblRAP, tblLinkPA, tblDataProgramacion)


    def updateStatus(self, status=None, *args, target_objects=(), pa_active_flag=None, **kwargs):
        """                     IMPORTANT: Accessed by background threads.
        Sets / updates progActivity Status in table [Data Actividades Programadas Status] FOR SPECIFIED OBJECTS.
        Also, sets isActive flag for progActivity, updating flag in RAP table. Values for flag: 0, 1, 2.
        Re-entry is not an issue in this class because all progActivities are individual objects with own data structure
        However, target_objects must be passed as an argument to the method as call applies only to specific objects.
        @param pa_active_flag: Sets Active Flag in RAP record, if passed.
        @param target_objects: (int) List of objects IDs (Animal, Person) for which the progActivity is to be updated.
        @param status: status val if not provided in tblDataStatus DataTable
        @return: recordID (int) if successful or errorCode (str)
        """
        # First, checks and updates Active Flag in RAP, if passed.
        if pa_active_flag in (0, 1, 2):
            self.__isActive = pa_active_flag
            if pa_active_flag == 0:           # Si se setea progActivity a Inactiva, remueve de registerDict.
                self.getPARegisterDict().pop(self)
            recordRAP = getRecords(self.tblRAPName(), '', '', None, '*', fldID=self.ID)
            if isinstance(recordRAP, str):
                retValue = f'ERR_DB_Cannot read table {self.tblRAPName()}. {moduleName()}({lineNum()})'
                krnl_logger.warning(retValue)
                return None
            setRecord(self.tblRAPName(), fldID=self.ID, fldFlag=pa_active_flag)
            return True

        # If it's not an Active Flag update for tblRAP, goes to update records in tblDataStatus.
        retValue = None
        if status not in ProgActivity.getActivityStatusDict():
            retValue = f'ERR_INP_Argument not valid: {status}. {moduleName()}({lineNum()})'
            krnl_logger.info(retValue)
        if not target_objects:
            retValue = f'ERR_INP_Argument not valid: Target Objects missing. {moduleName()}({lineNum()})'
            krnl_logger.info(retValue)
        tblLinkPA = getRecords(self.tblLinkPAName(), '', '', None, '*', fldFK=target_objects, fldFK_Actividad=self.ID)
        if isinstance(tblLinkPA, str):
            retValue = f'ERR_DB_Cannot read table {self.tblLinkPAName()}. {moduleName()}({lineNum()})'
            krnl_logger.warning(retValue)
        if type(retValue) is str:
            print(f'{retValue} - krnl_dataTable({lineNum()}) - {callerFunction()}', dismiss_print=DISMISS_PRINT)
            return retValue

        # Crea registro en [Data XXX Actividades Programadas Status]
        tblDataStatus = setupArgs(self.tblPADataStatusName(), *args, **kwargs)
        eventDate = tblDataStatus.getVal(0, 'fldDate', time_mt('datetime'))
        eventDate = eventDate if isinstance(eventDate, datetime) else time_mt('datetime')
        for j in range(tblLinkPA.dataLen):
            tblDataStatus.setVal(j, fldDate=eventDate, fldFK_Status=status, fldFK_Actividad=tblLinkPA.getVal(j,'fldID'))
        retValue = tblDataStatus.setRecords()
        return retValue

    def getLinkRecords(self, target_obj=None):
        """ Gets all records in tblLinkPA for idActivityRAP associated to this PA
        @return: returns all selected records from tblLinkPA in DataTable form or None if Activity is not valid"""
        return getRecords(self.tblLinkPAName(), '', '', None, '*', fldFK_Actividad=self.activityID)

    def checkFinalClose(self):
        """ Closes programmed activity self. Updates data structures and DB tables so that the progActivity is no
        longer used or loaded from DB on startup.
        The closure in RAP is done only when no open records are found in tblLinkPA related to the progActivity.
        @return: True if activity was closed. None if activity not closed or nothing is done.
        """
        if self.isActive == 1:  # Chequea si se debe cerrar definitivamente (setear Active=0) a la progActivity.
            openLinkRecords = getRecords(self.tblLinkPAName(), '', '', None, '*', fldFK_Actividad=self.ID,
                                         fldFK_ActividadDeCierre=None)
            if not openLinkRecords.dataLen:
                # Si no hay pendientes de la actividad, setea paOjb a Inactive,actualiza memory data struct y tablas DB.
                _ = self.updateStatus(pa_active_flag=0)
                self.paUnregisterActivity()  # Removes itself from the general PA register. Calls the right Unregister.
                return True
        return None



    def getPAClosingStatus(self):
        paStatusDict = {'openActive': 1,
                        'openExpired': 2,
                        'closedInTime': 4,
                        'closedOutOfTime': 5,
                        'closedTimeout': 6,
                        'closedBaja': 7,
                        'closedLocalizChange': 8,
                        'closedCancelled': 9,
                        'closedSuspended': 10,
                        'closedReplaced': 11,
                        'closedExpired': 12

                        }
        return paStatusDict.get('closedInTime')


# =================================== FIN CLASS PROGACTIVITY ========================================================= #









# ------------------------------------------------------------------------------------------------------------------- #

class ActivityTrigger(object):
    """ A Trigger is an object with a set of conditions that are checked regularly by the system and once met, create
    a Programmed Activity for the class where the Trigger object is registered.
    Trigger objects are registered in each ProgActivity Class and are managed directly by each class.
    """
    __slots__ = ('__ID', '__triggerOnOff', '_triggerConditions', '__sequenceActivities', '__treatment',
                 '_triggerActivityID', '_progActivityID', '_dataProgramacion', '__infoAplicaciones', '_timeStamp',
                 '_triggerStart', '_triggerEnd', '_recurrence', '__triggerDescription', '_dataProgDict', '__excludedFields')

    # __excludedFieldsDefalut = _excludedFieldsDefault
    # Dict Operators es un template (class attribute) que define que campos del trigger se usan, y con que operadores.
    # _triggerOperators = {'_triggerConditions': (operator.eq, (), (), {}), '_triggerActivityID': (operator.eq,(),(),{}),
    #                      '_progActivityID': (operator.eq, (), (), {}), '_triggerStart': (operator.eq, (), (), {}),
    #                      '_triggerEnd': (operator.eq, (), (), {}), '_recurrence': (operator.eq, (), (), {}),
    #                      '_dataProgDict': (operator.eq, (), (), {}),}
    # @classmethod
    # def getTriggerFields(cls):
    #     return tuple(cls._triggerOperators.keys())  # Usado por obj. Animal,Person,etc p/ estandarizar trigger fields

    def __init__(self, *args, excluded_fields=(), **kwargs):
        self.__ID = kwargs.get('fldID', None)
        self.__triggerOnOff = kwargs.get('fldFlag', 1)
        self.__triggerDescription = kwargs.get('fldDescription', 1)
        self._triggerConditions = kwargs.get('fldCondicionesTrigger', {})
        self._timeStamp = kwargs.get('fldTimeStamp', 0)
        self._triggerActivityID = kwargs.get('fldFK_ActividadDisparadora', None)
        self._progActivityID = kwargs.get('fldFK_ActividadProgramada', None)
        self._dataProgramacion = kwargs.get('fldFK_DataProgramacion', None)  # ID_Data Programacion
        self._dataProgDict = kwargs.get('dataProgDict', {})   # Dictionary with data from record fldFK_DataProgramacion
        self._triggerStart = kwargs.get('fldTriggerStart', None)
        self._triggerEnd = kwargs.get('fldTriggerEnd', None)
        self._recurrence = kwargs.get('fldRecurrenciaDias', 0)
        # self.__triggerConditionMet = kwargs.get('fldCondicionTriggerCumplida', None)
        self._excludedFields = self._excludedFieldsDefault().union(excluded_fields)
        super().__init__()

        # _triggerFields: Usado para comparar triggers, entre otras cosas a definir...
        # Debe tener exactamente los mismos keys que _triggerOperators y contiene los valores a usar en comparaciones
        # Compare Triggers: if _dataProgramacion is equal, _dataProgDict is assumed equal and is not compared.
        # self._triggerFields = {'fldFK_ActividadDisparadora': self._triggerActivityID, 'fldTimeStamp': self._timeStamp,
        #                        'fldFK_DataProgramacion': self._dataProgramacion, 'fldTriggerStart': self._triggerStart,
        #                        'fldTriggerEnd': self._triggerEnd, 'fldRecurrenciaDias': self._recurrence,
        #                        'fldFlag': self.__triggerOnOff, 'dataProgDict': self._dataProgDict,
        #                        'fldCondicionTriggerCumplida': self.__triggerConditionMet}

    @property
    def ID(self):
        return self.__ID

    @property
    def description(self):
        return self.__triggerDescription

    # @property
    # def triggerFields(self):
    #     return self._triggerFields

    @property
    def dataProgDict(self):
        return self._dataProgDict

    @property
    def isActive(self):
        return self.__triggerOnOff

    def checkTriggerPAConditions(self):
        """ Checks if Trigger conditions are met. If so, a Programmed Activity (PA) is generated """
        pass

    @classmethod
    def _excludedFieldsDefault(cls):
        return cls.__excludedFieldsDefault

    def compareTriggers(self, obj=None, *, excluded_fields=('fldID', 'dataProgDict'), excl_mode='append',
                        trunc_val='day', **kwargs):
        """Compares _triggerOperators values of 2 Trigger objects or between 1 Trigger (self) and **kwargs.
         @param obj: Trigger object
        @param excluded_fields:
        @param excl_mode: 'append': appends the list passed to __paExcludedFieldsDefault.
                          'replace': replaces the __paExcludedFieldsDefault list with the list passed.
        @param kwargs: trigger data when obj is not passed.
        @return True/False based on the comparison result."""
        if isinstance(obj, ActivityTrigger) or isinstance(kwargs, dict):  #  and not isinstance(kwargs, DeepDiff)):
            exclFields = self._excludedFields.union(excluded_fields)  # TODO:arreglar pasaje de excluded_fields como arg
            if excl_mode.strip().lower().startswith('append'):
                exclFields.update(excluded_fields)
            elif excluded_fields:
                exclFields = excluded_fields

            losDicts1 = self.triggerFields
            losDicts2 = obj.triggerFields if isinstance(obj, ActivityTrigger) else kwargs
            for k in exclFields:        # Removes excluded fields from dicts.
                try:
                    losDicts1.pop(k)
                    losDicts2.pop(k)
                except (KeyError, TypeError):
                    continue

            # Unpacks nested dicts with dict_iterator_gen(). Removes duplicate keys by retaining the last value for key
            # flatDict1 = {k: d[k] for d in nested_dict_iterator_gen(losDicts1) for k in d}  # Full dict de-nested
            # flatDict2 = {k: d[k] for d in nested_dict_iterator_gen(losDicts2) for k in d}  # Full dict de-nested
            # # Formats datetime and strings to allow for comparison
            # flatDict1 = {k: (trunc_datetime(flatDict1[k], trunc_val) if isinstance(flatDict1[k], datetime) else
            #                  ((removeAccents(flatDict1[k], str_to_lower=True) if isinstance(flatDict1[k], str) else
            #                    flatDict1[k]))) for k in flatDict1}
            # flatDict2 = {k: (trunc_datetime(flatDict2[k], trunc_val) if isinstance(flatDict2[k], datetime) else
            #                  ((removeAccents(flatDict2[k], str_to_lower=True) if isinstance(flatDict2[k], str) else
            #                    flatDict2[k]))) for k in flatDict2}
            #
            # flatList2Eqv = [getEquivalentFld(j) for j in flatDict2]  # tuple(map(getEquivalentFld, flatDict2))
            print(f' ******** losDicts1: {losDicts1}\n ******** losDicts2: {losDicts2}', dismiss_print=DISMISS_PRINT)
            compareDict = DeepDiff(losDicts1, losDicts2, truncate_datetime=trunc_val,  ignore_string_type_changes=True,
                                   ignore_string_case=True, ignore_numeric_type_changes=True, ignore_order=True,
                                   verbose_level=1).to_dict()


            # for k1 in flatDict1:
            #     # Recorre operandDicts y busca todos los keys definidos en (key_names_operand1,), (key_names_operand2,)
            #     k1Eqv = getEquivalentFld(fldNameFromUID(k1))
            #     if any(j.__contains__(k1) or k1.__contains__(j) for j in exclFields) or k1Eqv in exclFields:
            #     # or k1Eqv not in flatList2Eqv:
            #         continue  # continua si ya se proceso o si esta en excluded fields
            #     # else:
            #     # TODO(cmt): _specialFields={'fldName': (opr|function, (key_names_operand1,)|(),
            #     #  (key_names_operand2,)|(), {})
            #     # Toma el operador de k1 si k1 esta en _specialFields. Si no esta, opr asigna == (default)
            #     opr = _specialFields[k1Eqv][0] if k1Eqv in _specialFields else operator.eq
            #     k2 = next((k for k in flatDict2 if getEquivalentFld(k) in getFieldEquivalentNames(k1Eqv)), None)
            #     if k1Eqv in _specialFields:
            #         # Arma diccionarios de parametros para pasar opr. Pasa solo Equivalent fieldNames a operands.
            #         operand1 = {getEquivalentFld(fldNameFromUID(key)): flatDict1[key] for key in flatDict1 if
            #                     getEquivalentFld(fldNameFromUID(key)) in _specialFields[k1Eqv][1]} \
            #             if _specialFields[k1Eqv][1] else flatDict1.get(k1, VOID)
            #         operand2 = {getEquivalentFld(fldNameFromUID(key)): flatDict2[key] for key in flatDict2 if
            #                     getEquivalentFld(fldNameFromUID(key)) in _specialFields[k1Eqv][2]} \
            #             if _specialFields[k1Eqv][2] else flatDict2.get(k2, VOID)
            #     else:
            #         operand1 = flatDict1.get(k1, VOID)
            #         operand2 = flatDict2.get(k2, VOID)
            #
            #     # No compara 2 operandos nulos o sin valores. Comparacion trivial que da falsos True
            #     if all(j == VOID for j in (operand1, operand2)):
            #         pass
            #     else:
            #         try:
            #             compareDict[k1Eqv] = opr(operand1, operand2)
            #         except (TypeError, AttributeError, ValueError, KeyError, NameError):
            #             compareDict[k1Eqv] = False

            print(f'*** compare Triggers compareDict={compareDict}\nkwargs={kwargs}', dismiss_print=DISMISS_PRINT)
            print(f'NOW the new stuff: {dictCompare(losDicts1, losDicts2)}')
            return not(bool(compareDict))
        return False

    # def compareTriggers04(self, obj=None, *, excluded_fields=('fldID',), excl_mode='append', trunc_val='day', **kwargs):
    #     """Compares _triggerOperators values of 2 Trigger objects or between 1 Trigger (self) and **kwargs.
    #      @param obj: Trigger object
    #     @param excluded_fields:
    #     @param excl_mode: 'append': appends the list passed to __paExcludedFieldsDefault.
    #                       'replace': replaces the __paExcludedFieldsDefault list with the list passed.
    #     @param kwargs: trigger data when obj is not passed.
    #     @return True/False based on the comparison result."""
    #     if isinstance(obj, ActivityTrigger) or isinstance(kwargs, dict):  # and not isinstance(kwargs, DeepDiff)):
    #         compareDict = {}  # {fldName: result (True/False),} Dict. con tuplas de valores de op1 y op2 p/ cada fldName
    #         exclFields = __paExcludedFieldsDefault
    #         if excl_mode.strip().lower().startswith('append'):
    #             exclFields.extend(excluded_fields)
    #         elif excluded_fields:
    #             exclFields = excluded_fields
    #         losDicts1 = self.triggerFields
    #         losDicts2 = obj.triggerFields if isinstance(obj, ActivityTrigger) else kwargs
    #
    #         # Unpacks nested dicts with dict_iterator_gen(). Removes duplicate keys by retaining the last value for key
    #         flatDict1 = {k: d[k] for d in nested_dict_iterator_gen(losDicts1) for k in d}  # Full dict de-nested
    #         flatDict2 = {k: d[k] for d in nested_dict_iterator_gen(losDicts2) for k in d}  # Full dict de-nested
    #         # Formats datetime and strings to allow for comparison
    #         flatDict1 = {k: (trunc_datetime(flatDict1[k], trunc_val) if isinstance(flatDict1[k], datetime) else
    #                          ((removeAccents(flatDict1[k], str_to_lower=True) if isinstance(flatDict1[k], str) else
    #                            flatDict1[k]))) for k in flatDict1}
    #         flatDict2 = {k: (trunc_datetime(flatDict2[k], trunc_val) if isinstance(flatDict2[k], datetime) else
    #                          ((removeAccents(flatDict2[k], str_to_lower=True) if isinstance(flatDict2[k], str) else
    #                            flatDict2[k]))) for k in flatDict2}
    #
    #         flatList2Eqv = [getEquivalentFld(j) for j in flatDict2]  # tuple(map(getEquivalentFld, flatDict2))
    #         print(f' ******** flatDict1: {flatDict1}\n ******** flatDict2: {flatDict2}', dismiss_print=DISMISS_PRINT)
    #
    #         # exact_match: True -> Compares all Trigger fields - False -> Ignores all fields with values == None.
    #         exact_match = False if obj is None else True
    #
    #         for k1 in flatDict1:
    #             # Recorre operandDicts y busca todos los keys definidos en (key_names_operand1,), (key_names_operand2,)
    #             k1Eqv = getEquivalentFld(fldNameFromUID(k1))
    #             if any(j.__contains__(k1) or k1.__contains__(j) for j in exclFields) or k1Eqv in exclFields:
    #                 # or k1Eqv not in flatList2Eqv:
    #                 continue  # continua si ya se proceso o si esta en excluded fields
    #             # else:
    #             # TODO(cmt): _specialFields={'fldName': (opr|function, (key_names_operand1,)|(),
    #             #  (key_names_operand2,)|(), {})
    #             # Toma el operador de k1 si k1 esta en _specialFields. Si no esta, opr asigna == (default)
    #             opr = _specialFields[k1Eqv][0] if k1Eqv in _specialFields else operator.eq
    #             k2 = next((k for k in flatDict2 if getEquivalentFld(k) in getFieldEquivalentNames(k1Eqv)), None)
    #             if k1Eqv in _specialFields:
    #                 # Arma diccionarios de parametros para pasar opr. Pasa solo Equivalent fieldNames a operands.
    #                 operand1 = {getEquivalentFld(fldNameFromUID(key)): flatDict1[key] for key in flatDict1 if
    #                             getEquivalentFld(fldNameFromUID(key)) in _specialFields[k1Eqv][1]} \
    #                     if _specialFields[k1Eqv][1] else flatDict1.get(k1, VOID)
    #                 operand2 = {getEquivalentFld(fldNameFromUID(key)): flatDict2[key] for key in flatDict2 if
    #                             getEquivalentFld(fldNameFromUID(key)) in _specialFields[k1Eqv][2]} \
    #                     if _specialFields[k1Eqv][2] else flatDict2.get(k2, VOID)
    #             else:
    #                 operand1 = flatDict1.get(k1, VOID)
    #                 operand2 = flatDict2.get(k2, VOID)
    #
    #             # No compara 2 operandos nulos o sin valores. Comparacion trivial que da falsos True
    #             if all(j == VOID for j in (operand1, operand2)):
    #                 pass
    #             else:
    #                 try:
    #                     compareDict[k1Eqv] = opr(operand1, operand2)
    #                 except (TypeError, AttributeError, ValueError, KeyError, NameError):
    #                     compareDict[k1Eqv] = False
    #
    #         print(f'*** compare Triggers compareDict={compareDict}\nkwargs={kwargs}', dismiss_print=DISMISS_PRINT)
    #         return compareDict
    #     return {}

    # def compareTriggers03(self, obj=None, *, excluded_fields=('fldID',), excl_mode='append', trunc_val='day', **kwargs):
    #     """Compares _triggerOperators values of 2 Trigger objects or between 1 Trigger (self) and **kwargs.
    #      @param obj: Trigger object
    #     @param excluded_fields:
    #     @param excl_mode: 'append': appends the list passed to __paExcludedFieldsDefault.
    #                       'replace': replaces the __paExcludedFieldsDefault list with the list passed.
    #     @param kwargs: trigger data when obj is not passed.
    #     @return True/False based on the comparison result."""
    #     if isinstance(obj, ActivityTrigger) or isinstance(kwargs, dict):  #  and not isinstance(kwargs, DeepDiff)):
    #         compareDict = {}  # {fldName: result (True/False),} Dict. con tuplas de valores de op1 y op2 p/ cada fldName
    #         exclFields = __paExcludedFieldsDefault
    #         if excl_mode.strip().lower().startswith('append'):
    #             exclFields.extend(excluded_fields)
    #         elif excluded_fields:
    #             exclFields = excluded_fields
    #         losDicts1 = self.triggerFields
    #         losDicts2 = obj.triggerFields if isinstance(obj, ActivityTrigger) else kwargs
    #
    #         # Unpacks nested dicts with dict_iterator_gen(). Removes duplicate keys by retaining the last value for key
    #         flatDict1 = {k: d[k] for d in nested_dict_iterator_gen(losDicts1) for k in d}  # Full dict de-nested
    #         flatDict2 = {k: d[k] for d in nested_dict_iterator_gen(losDicts2) for k in d}  # Full dict de-nested
    #         # Formats datetime and strings to allow for comparison
    #         flatDict1 = {k: (trunc_datetime(flatDict1[k], trunc_val) if isinstance(flatDict1[k], datetime) else
    #                          ((removeAccents(flatDict1[k], str_to_lower=True) if isinstance(flatDict1[k], str) else
    #                            flatDict1[k]))) for k in flatDict1}
    #         flatDict2 = {k: (trunc_datetime(flatDict2[k], trunc_val) if isinstance(flatDict2[k], datetime) else
    #                          ((removeAccents(flatDict2[k], str_to_lower=True) if isinstance(flatDict2[k], str) else
    #                            flatDict2[k]))) for k in flatDict2}
    #
    #         flatList2Eqv = [getEquivalentFld(j) for j in flatDict2]  # tuple(map(getEquivalentFld, flatDict2))
    #         print(f' ******** flatDict1: {flatDict1}\n ******** flatDict2: {flatDict2}', dismiss_print=DISMISS_PRINT)
    #
    #         # exact_match: True -> Compares all Trigger fields - False -> Ignores all fields with values == None.
    #         exact_match = False if obj is None else True
    #
    #         for k1 in flatDict1:
    #             # Recorre operandDicts y busca todos los keys definidos en (key_names_operand1,), (key_names_operand2,)
    #             k1Eqv = getEquivalentFld(fldNameFromUID(k1))
    #             if any(j.__contains__(k1) or k1.__contains__(j) for j in exclFields) or k1Eqv in exclFields:
    #             # or k1Eqv not in flatList2Eqv:
    #                 continue  # continua si ya se proceso o si esta en excluded fields
    #             # else:
    #             # TODO(cmt): _specialFields={'fldName': (opr|function, (key_names_operand1,)|(),
    #             #  (key_names_operand2,)|(), {})
    #             # Toma el operador de k1 si k1 esta en _specialFields. Si no esta, opr asigna == (default)
    #             opr = _specialFields[k1Eqv][0] if k1Eqv in _specialFields else operator.eq
    #             k2 = next((k for k in flatDict2 if getEquivalentFld(k) in getFieldEquivalentNames(k1Eqv)), None)
    #             if k1Eqv in _specialFields:
    #                 # Arma diccionarios de parametros para pasar opr. Pasa solo Equivalent fieldNames a operands.
    #                 operand1 = {getEquivalentFld(fldNameFromUID(key)): flatDict1[key] for key in flatDict1 if
    #                             getEquivalentFld(fldNameFromUID(key)) in _specialFields[k1Eqv][1]} \
    #                     if _specialFields[k1Eqv][1] else flatDict1.get(k1, VOID)
    #                 operand2 = {getEquivalentFld(fldNameFromUID(key)): flatDict2[key] for key in flatDict2 if
    #                             getEquivalentFld(fldNameFromUID(key)) in _specialFields[k1Eqv][2]} \
    #                     if _specialFields[k1Eqv][2] else flatDict2.get(k2, VOID)
    #             else:
    #                 operand1 = flatDict1.get(k1, VOID)
    #                 operand2 = flatDict2.get(k2, VOID)
    #
    #             # No compara 2 operandos nulos o sin valores. Comparacion trivial que da falsos True
    #             if all(j == VOID for j in (operand1, operand2)):
    #                 pass
    #             else:
    #                 try:
    #                     compareDict[k1Eqv] = opr(operand1, operand2)
    #                 except (TypeError, AttributeError, ValueError, KeyError, NameError):
    #                     compareDict[k1Eqv] = False
    #
    #         print(f'*** compare Triggers compareDict={compareDict}\nkwargs={kwargs}', dismiss_print=DISMISS_PRINT)
    #         return compareDict
    #     return {}

    # def compareTriggers02(self, obj=None, *, excluded_fields=(), excl_mode='append', **kwargs):
    #     """Compares _triggerOperators values of 2 Trigger objects or between 1 Trigger (self) and **kwargs.
    #      @param obj: Trigger object
    #     @param excluded_fields:
    #     @param excl_mode: 'append': appends the list passed to __paExcludedFieldsDefault.
    #                       'replace': replaces the __paExcludedFieldsDefault list with the list passed.
    #     @param kwargs: trigger data when obj is not passed.
    #     @return True/False based on the comparison result."""
    #     if isinstance(obj, ActivityTrigger) or (isinstance(kwargs, dict) and not isinstance(kwargs, DeepDiff)):
    #         compareDict = {}  # {fldName: result (True/False),} Dict. con tuplas de valores de op1 y op2 p/ cada fldName
    #         exclFields = __paExcludedFieldsDefault
    #         if excl_mode.strip().lower().startswith('append'):
    #             exclFields.extend(excluded_fields)
    #         elif excluded_fields:
    #             exclFields = excluded_fields
    #         losDicts1 = self.triggerFields
    #         losDicts2 = obj.triggerFields if isinstance(obj, ActivityTrigger) else kwargs
    #         # Desempaca dicts anidados con dict_iterator_gen
    #         flatList1 = [d for d in nested_dict_iterator_gen(losDicts1)]      # lista de diccionarios {k: v} de-nested
    #         flatList2 = [d for d in nested_dict_iterator_gen(losDicts2)]      # lista de diccionarios {k: v} de-nested
    #         specialDict = defaultdict(list)    # {fldName: (opr, val), }    # Diccionario para ciertas ops. especiales
    #         # exact_match: True -> Compares all Trigger fields - False -> Ignores all fields with values == None.
    #         exact_match = False if obj is None else True
    #
    #         for d1 in flatList1:
    #             for k1 in d1:
    #                 # va al proximo si ya se proceso, si esta en excluded fields, no esta en _triggerOperators o
    #                 # d1[k1] == None con exact_match = False ('loose match' que ignora campos en None).
    #                 k1Eqv = getEquivalentFld(fldNameFromUID(k1) if fldNameFromUID(k1) else k1)
    #                 if any(exclFields[j].__contains__(k1Eqv) or k1Eqv.__contains__(exclFields[j]) for j in
    #                        range(len(exclFields))) or k1Eqv in compareDict or (exact_match is False and d1[k1] is None):
    #                     continue
    #                 else:
    #                     opr = operator.eq  # Igual, operador por default.
    #                     if k1Eqv in _specialFields:  # TODO: _specialFields={'fldName': (opr | function, #_of_args, {})
    #                         opr = _specialFields[k1Eqv][0]     # Toma el operador de k1 si k1 esta en _specialFields.
    #                         # if _specialFields[k1Eqv][1]:        # Si el Dict esta poblado...
    #                         if k1Eqv not in specialDict:  # Entra solo si key aun no existe.
    #                             specialDict[k1Eqv].append(opr)
    #                             specialDict[k1Eqv].append(d1[k1])
    #                     elif k1 in self._triggerOperators:
    #                         opr = self._triggerOperators[k1]    # Toma el operador de k1 de _triggerOperators
    #
    #                     break_param = None
    #                     for d2 in flatList2:
    #                         for k2 in d2:
    #                             k2Eqv = getEquivalentFld(fldNameFromUID(k2) if fldNameFromUID(k2) else k2)
    #
    #                             if k2Eqv not in compareDict and \
    #                                     any(exclFields[j].__contains__(k2Eqv) for j in range(len(exclFields))) is False:
    #                                 if k2Eqv in _specialFields:
    #                                     opr = _specialFields[k2Eqv][0]  # Toma el operador de k1 si esta en _specialFields.
    #                                     # if _specialFields[k2Eqv][1]:  # Si el Dict esta poblado...
    #                                     if k2Eqv not in specialDict:        # Entra solo si key aun no existe.
    #                                         specialDict[k2Eqv].append(opr)        # Almacena datos.
    #                                         specialDict[k2Eqv].append(d2[k2])
    #
    #                             if k2Eqv.__contains__(k1Eqv) or k1.__contains__(k2Eqv):
    #                                 if k1Eqv in _specialFields:
    #                                     result = opr(d1[k1], d2[k2])
    #                                 else:
    #                                     result = operate(opr, d1[k1], d2[k2])
    #                                 compareDict[k1Eqv] = result
    #                                 break_param = 1
    #                                 break
    #                             continue
    #                         if break_param:
    #                             break_param = None
    #                             break
    #                         continue
    #
    #         # Ejecuta comparacion de multiKeyNames, si hay.
    #         for key in specialDict:
    #             operands = {}  # {operandName: operandValue, }
    #             # Entra aqui el dict o la lista de _specialFields[key] esta poblada.
    #             if _specialFields[key][1] and isinstance(_specialFields[key][1], (list, tuple, set)):
    #                 for operandName in _specialFields[key][1]:  # [0]: operador / [1]: value
    #                     operands[operandName] = specialDict[operandName][1] if specialDict[operandName] else None
    #                 if any(j is None for j in operands.values()):
    #                     pass
    #                 else:
    #                     result = _specialFields[key][0](**operands)
    #                     compareDict[key] = result
    #                 break
    #
    #
    #         print(f'*** compareTriggers(). compareDict={compareDict}\n*** Object kwargs={kwargs} - result: {result}',
    #               dismiss_print=DISMISS_PRINT)
    #         return compareDict

    _init_funcs = ()

# ======================================= FIN CLASS ActivityTrigger ================================================== #


