from __future__ import annotations
import threading
import concurrent
from concurrent.futures import ProcessPoolExecutor
from abc import ABC
import pandas as pd
import numpy as np
from krnl_custom_types import DataTable, setRecord, delRecord, setupArgs, getRecords, dbRead, DBTrigger, getFldName, \
    getTblName, AbstractMemoryData, getrecords
from krnl_config import *
from krnl_db_query import getFldCompare, SQLiteQuery, _DataBase, AccessSerializer
from krnl_exceptions import DBAccessError
from collections import defaultdict
from datetime import tzinfo, datetime, timedelta
from krnl_geo import Geo
from krnl_db_access import setrecords
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
    """ Implements the execution of methods Activity._paUpdate(), Activity._paMatchAndClose() in a separate thread, in
    deferred execution, to run the checks and update database tables when an executed activity is eligible as the
    closing activity for 1 or more ProgActivities or for updating the object's myProgActivities dictionary after
    object's conditions have changed.
    Decouples all these db-intensive tasks from the front-end thread is the right way to go to free-up the front-end.
    """
    _writes_to_db = MAIN_DB_NAME     # Flag to signal that the class uses db-write functions setRecord(), setrecords()

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
        if callable(self._callable):
            # self._object -> Activity object (an instance of one of the subclasses of class Activity).
            # self._callable -> Activity._paMatchAndClose(), Activity._paCreateExecInstance() (for now...)
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

    # Dict of entity object classes, to single out uid_dicts used in memory_data management.
    _entity_obj_classes = {}         # {main_class: (object sub_classes, ), }   {Animal: (Bovine, Caprine)}
    
    def __init_subclass__(cls, **kwargs):
        """
        1) Registers classes that define __method_name, to create its Activity objects in EO.__init_subclass__()
        2) Creates trigger objects associated to classes that define attribute __db_triggers_list.
        3) register Activity classes that implement supports_mem_data() for Activities that use data caching in memory.
        @param kwargs:
        @return: None
        """
        try:        # Uses __ to ensure __method_name is not inherited, then must check for name-mangling in getattr()
            if getattr(cls, '_' + cls.__name__ + '__method_name') is not None:
                cls.register_class()       # Only registers classes with a valid method name.
        except (AttributeError, NameError):
            print(f'UUUUUUUUHH Activity.__init_subclass__(): No __method_name for {cls} - {moduleName()}({lineNum()})')
        super().__init_subclass__(**kwargs)

        # Initializes DB triggers. Name mangling required here to prevent inheritance from resolving to wrong data.
        triggers_list = getattr(cls, '_' + cls.__name__ + '__db_triggers_list', None)
        if triggers_list:
            for j in triggers_list:
                if isinstance(j, (list, tuple)) and len(j) == 2:  # j=(trigger_name, trigger_processing_func (callable))
                    _ = DBTrigger(trig_name=j[0], process_func=j[1], calling_obj=cls)

        # Creates register for classes that __supports_mem_data, to access the uid dicts and methods of those classes
        # TODO(cmt): Required to make _memory_data_init_last_rec() calls from EntityObject.__init_subclass__().
        if cls.supports_mem_data():
            cls._memory_data_classes.add(cls)  # Register (set) with all Activity classes that support_mem_data
            print(f'MEMORY DATA CLASSES: {cls._memory_data_classes}')


    @classmethod
    def register_class(cls):
        cls._activity_class_register[cls] = None            # {ActivityClass: ObjectClass | None, }

    @classmethod
    def get_class_register(cls):
        return cls._activity_class_register                 # {ActivityClass: ObjectClass | None, }

    @classmethod
    def getActivityClass(cls, activityName):
        return next((j for j in cls._activity_class_register if j.getActivityName() == activityName), None)

    @classmethod
    def getActivityObject(cls, activityName):
        return next((v for k, v in cls.definedActivities().items() if k == activityName), None)


    @classmethod
    def get_local_uids_dict(cls):
        """ pd.DataFrame used by Activity Classes that implement MemoryData
            The _dict particle is legacy. Left there for convenience. The actual object is a pandas DataFrame.
        """
        return getattr(cls, '_' + cls.__name__ + '__local_active_uids_dict', None)

    @classmethod
    def supports_mem_data(cls):
        """ True/False for Activities that support data mirroring in memory (Implement MemoryData class and db-memory
        data synchronization).
        __local_active_uids_dict is the 'flag' used to detect supports_mem_data condition. """
        return hasattr(cls, '_' + cls.__name__ + '__local_active_uids_dict')

    @classmethod
    def get_mem_dataframes(cls):        # returns dict {class_obj: Activity mem_dataframe, }
        """ Returns list of dataframes with mem_data for Activity cls. None if cls does not support mem_data.
        User must know the structure of dataframes returned.
        Returns dict 'cause all mem Activities create multiple dfs for different object types (Bovine, Caprine, etc.).
        @return: Returns dict of dataframes with mem_data for Activity cls. Empty dict if cls does not support mem_data.
        """
        return getattr(cls, '_' + cls.__name__ + '__local_active_uids_dict', {})


    # Buffer manager to execute Activity._paMatchAndClose() method in an independent, asynchronous thread.
    # TODO: thread_priority: 0 - 20. Fine-tune in final version, as this cursor should have a mid-priority thread.
    __progActivityBuffer = AsyncBuffer(ProgActivityCursor, autostart=True, thread_priority=8)  # mid-priority thread

    # Setea records en [Data XXX Actividades Programadas Status]. Cada record 1:1 con records en tblLinkPA.
    _paStatusDict = {'undefined': 0,    # TODO: chequear como "is not undefined" para incluir 0 y None.
                     'openActive': 1,
                     'openExpired': 2,
                     'closedInTime': 4,
                     'closedExpired': 5,    # Cierre despues de progDate+UpperWindow y ANTES de progData+daysExpiration
                     'closedNotExecuted': 6,   # ProgActivities sin cierre despues de daysExpiration.
                                               # Este status es chequeado y seteado SOLO por funciones de background.
                     'closedBaja': 7,
                     'closedLocalizChange': 8,
                     'closedCancelled': 9,
                     'closedReplaced': 10,
                     'closedBySystem': 11   # same as 'closedNotExecuted', but this closure is done by cleanup tasks.
                     }

    __tblActivityStatusNames = 'tblActividadesStatus'
    # Dict with Futures to monitor threaded execution of init functions for all mem_data classes (Used in is_ready(),
    # wait_till_ready() methods)
    _futures = defaultdict(list)  # {ActivityClass: (future_obj,), }. Initialized in EntityObject.__init_subclass__()

    # TODO(cmt): Now I see the light: There's no need to wait on dictionary initialization for mem_data Activities!!.
    #  This is because the logic that handles the mem_data dictionaries already covers dicts changing dynamically.
    #  So, if a certain uid is not found in the local dict by a _get_mem_data() or set_mem_data() call, the function
    #  will pull the data from db and will update the corresponding dict.
    #  With this, Activity._futures dict will still be populated during the intialization of EntityObjects,
    #  but is NOT used at the moment. Because of all this, the code commented below is no longer needed.
    #  THERE IS HOWEVER a race-condition scenario: set_mem_data() can add data to a dict BEFORE
    #  initialization completes. For these cases, uses wait_till_ready() to address concurrent access on a
    #  per-dictionary basis. The locking condition is checked both in set_mem_data() and in
    #  _memory_data_init_last_rec().

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

    tempdf = getrecords(__tblActivityStatusNames, 'fldID', 'fldStatus')
    if tempdf.empty:
        krnl_logger.error(f'ERR_DB_ReadError-Initialization faiulre: {tempdf.db.tbl_name}. Exiting... ')
        raise DBAccessError(f'ERR_DB_ReadError-Initialization faiulre:{tempdf.db.tbl_name}.Exiting... ')
    __activityStatusDict = dict(zip(tempdf['fldStatus'], tempdf['fldID']))

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

    @staticmethod
    def getPARegisterDict():            # TODO: Must be implemented in all subclasses.
        pass

    # 1 or more Activity classes derived from this one support ProgActivities.
    @classmethod
    def classSupportsPA(cls):
        return bool(cls._supportsPADict)            # Required to drive the flagExecInstance logic.



    # class ActivitiesMemoryData(AbstractMemoryData):     # DEPRECATED. ALL MEMORY DATA MIGRATED TO pd.DataFrames.
    #     """ MemoryData class for most Animal Activities. "Standard" class for Activities that:
    #         1. Use fldDate as the filter field and always fetch the last record inserted in db, based on fldDate value.
    #         2. Retrieves 1 or more values with a standardized structure as memory data value(s).
    #         3.
    #     """
    #     days_ago = time_mt('dt') - timedelta(seconds=1, microseconds=0)  # 1 sec=60 days ago from time_mt().
    #
    #     def __init__(self, *args, field_names=None, values=(), **kwargs):
    #         """
    #         @param fields_values: {fldName: fldValue, } dict with all fields and values.
    #         @param inventory: Temporary param for access to USE_DAYS_MULT simulations during code tesing.
    #         @param values: A list or tuple of values that match 1->1 with field_names
    #         """
    #         super().__init__(*args, **kwargs)
    #         self.__record = dict(zip(field_names, values))
    #         if 'invent' in [k.lower() for k in kwargs] and USE_DAYS_MULT:
    #             self.__record['fldDate'] = self.days_ago
    #
    #     @property
    #     def value(self):
    #         return self.__record
    #
    #     @property
    #     def record(self):
    #         return self.__record

    # ------------------------------------------ End ActivitiesMemoryData ------------------------------------------#


    @classmethod
    def _load_memory_data_last_rec(cls, *, tbl=None, flds=(), keys=None, max_col=None):
        """ Loads database fields to memory for oft-accessed data. Executed from __init__() in singleton initialization
        @param tbl: Activity table name (str).
        @param flds: fld names to store in memory (tuple).
        @param keys: set of uids to assign database records to. They become keys of the return dictionary.
        @param max_col: str. Column name belonging to tbl out of which the maximum value is to be extracted.
        @return: pandas DataFrame with memory data read from db, or None on failure or invalid arguments.
        """
        if strError in getTblName(tbl) or not flds:
            return None

        active_uids = tuple(set(keys))  # This is a set of uids. Must tuple it to properly convert to str(active_uids).
        active_uids_str = str(active_uids)
        active_uids_str = active_uids_str if len(active_uids) > 1 else active_uids_str.replace(',', '')
        link_fields = ('fldFK', )
        if max_col not in getFldName(tbl, '*', mode=1):
            max_col = 'fldDate'  # max_col always comes from Data Table. Defaults to fldDate.
        if max_col not in flds:
            raise AttributeError(f"ERR_AttributeError: Max. column missing in table {tbl}. Cannot initialize Memory Data.")
        tblLink_key = cls.getTblLinkName()
        tblLink_db = getTblName(tblLink_key)
        tblData = getTblName(tbl)
        link_fldNames = ", ".join([f'"{tblLink_db}"."{getFldName(tblLink_key, j)}"' for j in link_fields])
        flds_str = []
        for f in flds:
            # This loop is to maintain the order of fields in flds to create the dataframe.
            if f == max_col:
                flds_str.append(f'MAX("{tblData}"."{getFldName(tbl, max_col)}") AS "{max_col}"')
            else:
                flds_str.append(f'"{tblData}"."{getFldName(tbl, f)}"')
        data_fldNames = ", ".join(flds_str)
        # link_fldNames is fldFK only -> 1st column in dataframe
        sql_group = f'SELECT {link_fldNames}, {data_fldNames} FROM "{tblLink_db}" ' \
                    f'INNER JOIN "{tblData}" ON ' \
                    f'"{tblLink_db}"."{getFldName(tblLink_key, "fldFK_Actividad")}" == ' \
                    f'"{tblData}"."{getFldName(tbl, "fldFK_Actividad")}" ' \
                    f'GROUP BY "{tblLink_db}"."{getFldName(tblLink_key, "fldFK")}" ' \
                    f'HAVING "{getFldName(tblLink_key, "fldFK")}" IN {active_uids_str}; '
        df = pd.read_sql_query(sql_group, SQLiteQuery().conn)

        # Attemps to convert all strings in df_group[max_col] to python datetime objects.
        try:        # df[max_col] may be anything, not only dates.
            timecol = pd.Series(pd.to_datetime(df[max_col]).dt.to_pydatetime(), dtype=object)  # Tries datetime convers.
        except (AttributeError, Exception):
            pass
        else:
            if any(timecol <= datetime(1970, 1, 1, 0, 0)): # pd.to_datetime erroneously converts some types to this date
                pass
            else:
                df[max_col] = timecol   # max_col successfully converted to datetime.datetime
        df.fillna(np.nan).replace([np.nan], [None], inplace=True)      # Converts all null to None.
        return df


    @classmethod                                      # Activity Class method.
    def _memory_data_init_last_rec(cls, obj_class, active_uids: set, max_col=None, **kwargs):
        #                **** TODO(cmt): This function is designed to be run in its own thread ****
        """ Initializer for memory data that keeps LAST available database RECORD (last inventory, last status, etc.).
        Called from EntityObject __init_subclass__(). cls._memory_data_classes are initialized at this call.
        cls is InventoryActivity, StatusActivity, LocalizationActivity, BovineActivity.CategoryActivity, etc.
        THIS IS A FALLBACK FUNCTION that gets called when Activity classes don't implement it.
        It reads all required data from database and creates a dataframe with the memory data.
        Also copies the created dictionary to all parent classes that implement dictofdicts. This is required to
        operate memory_data logic consistently and maintain data integrity.
        Returns LAST values for fldNames passed (LAST meaning: db values associated with the last (highest) value of
        fldDate field in the data table, or the LAST value corresponding to fldName if fldDate is not defined for
        the table).
        @param obj_class: class to be used as key for __local_active_uids_dict.
        @param max_col: str. Table fldName of the field to pull max dates from. Defaults to fldDate.
        """
        local_uids_dict = getattr(cls, '_' + cls.__name__ + '__local_active_uids_dict', None)
        tblData_fields = getFldName(cls.tblDataName(), '*', mode=1)  # {fldName: dbFldName, }
        flds = [f for f in cls._mem_data_params['field_names'] if f in tblData_fields]   # Required!
        if local_uids_dict is not None:             # is None for classes that don't support_mem_data().
            if obj_class not in local_uids_dict:
                local_uids_dict[obj_class] = pd.DataFrame([], columns=flds)
            if not isinstance(max_col, str):
                max_col = 'fldDate'     # Default column to pull max date.
            # Check if any cls has _animalClassID attribute defined (Bovine, Caprine, etc. will have it).
            # Loads 1 dictionary at a time for: classes with _animalClassID == cls._animalClassID and for classes with
            # _animalClassID not defined (AnimalActivity, TagActivity, DeviceActivity classes).
            # In each call, keeps adding new keys to the class local dictionary for classes without animalClassID.
            # not hasattr('_animalClassID') are: InventoryActivity, StatusActivity, etc. (not defined for specific
            # animal classes.
            if not hasattr(cls, '_animalClassID') or cls.animalClassID() == obj_class.animalClassID():
                tblData_fields = getFldName(cls.tblDataName(), '*', mode=1)  # {fldName: dbFldName, }

                # dicto is of the form {uid: tuple_of_values, }
                df_loaded = cls._load_memory_data_last_rec(tbl=cls.tblDataName(), flds=flds, keys=active_uids,
                                                           max_col=max_col)
                # TODO: Testing dfs. Remove after testing.
                # df_test1 = df_loaded.copy()
                # df_test1.drop(range(100, 200), inplace=True)  # Removes 100 lines to generate non-duplicate rows
                # df_test2 = df_test1.copy()
                # df_test1['fldDate'] = pd.to_datetime(df_test1['fldDate'], format=fDateTime) + \
                #                       pd.Timedelta(days=10, hours=10, minutes=10)

                index = 'fldFK'

                # Compare 2 dataframes and update rows with new values. Outer join, not duplicating rows with same uid.
                # df_obj_class = df_test1.append(df_test2).append(df_test1)
                df_obj_class = local_uids_dict[obj_class]     # TODO: REINSTATE this line after testing

                if isinstance(df_obj_class, pd.DataFrame) and not df_obj_class.empty:
                    # Must work with default index in order to keep track of the rows in original merged df.
                    merged = pd.merge(df_loaded.reset_index(), df_obj_class, how='outer')
                    # Logic below works only on repeat values (value_counts > 1) reducing the number of rows to process.
                    # Sets index='fldFK' for next 2 lines to detect duplicate uid rows.
                    no_duplicates = merged[~merged.set_index(index).index.duplicated(keep=False)]  # Removes all repeat uid rows.
                    duplicates = merged[merged.set_index(index).index.duplicated(keep=False)]  # Pulls only duplicate uid rows in df.
                    # Adds a copy of index 'cause it's lost in the groupby() operation.
                    duplicates['saved_index'] = duplicates.index.to_list()
                    # Groups by ['fldFK', 'fldFK_Actividad'], passes 'saved_index' to the new structure and leaves only
                    # rows with MAX(fldDate) for each ['fldFK', 'fldFK_Actividad'] group. max_rows is a pd.Series.
                    max_rows = duplicates.groupby(['fldFK', 'fldFK_Actividad'])['saved_index'].max(max_col)

                    # This line below eliminates all duplicate uids with fldDate less than MAX(fldDate).
                    resolved_dupls = duplicates[duplicates.index.isin(max_rows)]
                    resolved_dupls.drop('saved_index', axis=1, inplace=True)
                    local_uids_dict[obj_class] = no_duplicates.append(resolved_dupls, sort=True)
                    local_uids_dict[obj_class].drop('index', axis=1, inplace=True)
                else:
                    local_uids_dict[obj_class] = df_loaded
                # dicto = {"uid": local_uids_dict[obj_class].iloc[0].to_dict()}  # Printing Category mem_data.
                print(f'\nMEMORY DATA for {cls.__name__}: {len(local_uids_dict[obj_class])} uid items.',
                      dismiss_print=DISMISS_PRINT)
        return


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

        # Stores the caller object (Object of class Bovine, Caprine, Tag, Person, Device, etc.)
        # TODO(cmt): __outerAttr => ONLY 1 object per Activity and per Thread (recursion with a different object for
        #  SAME activity and SAME Thread is not supported). Used by all methods that could be called in parallel by
        #  different threads: Activity.__call__, _setInventory, _setStatus, _setLocaliz, _getRecordLinkTables, etc.
        self.__outerAttr = {}  # Dynamic attribute for each Activity object: {threadID: outerObject, }
        # self.__outerAttr = defaultdict(list)      # Re-entrant structure: {threadID: [outerObject1, outerObject2, ] }
        self._decoratorName = next((kwargs[j] for j in kwargs if 'decorator' in str(j).lower()), None)
        self.__excluded_fields = (self.getActivityExcludedFieldsClose(self._activityName) or set()).union(excluded_fields)
        super().__init__()


    @property
    def outerObject(self):          # invoked from AnimalActivity, PersonActivity, TagActivity __call__() functions.
        if self.__outerAttr:
            k = threading.current_thread().ident
            if k in self.__outerAttr:
                return self.__outerAttr[k]      # Simpler, non re-entrant code as a thread doesn't interrupt itself.

                # if self.__outerAttr[k]:             # TODO: 2 Lines for for Activity and thread fully re-entrant code.
                #     return self.__outerAttr[k][-1]  # Returns last (most recent) element of the stack for this thread.
            else:
                val = f'ERR_SYS_Threading: __outerAttr called by {self} with key {k} but key does not exist in ' \
                      f'__outerAttr dictionary.'
                krnl_logger.error(val)
                raise KeyError(val)
        krnl_logger.warning(f'ERR_SYS_Runtime: outerObject called on an empty __outerAttr dict. Callers: '
                            f'{callerFunction(getCallers=True, namesOnly=True)}')

    @outerObject.setter
    def outerObject(self, obj=None):
        """ Appends an object (Animal, Tag, Person) to __outerAttr, which is a dict that stores 1 obj value for each
        thread calling the function: {thread_id: object}. In this way threads access the correct object regardless of
        how and when the OS switches thread execution. This wasn't the case with the previous stack structure.
        @param obj: Animal, Tag, Person object to be added to __outerAttr dict. """
        # if obj is not None:  # TODO(cmt): This if leads to data corruption when outerObj is called with None argument.
        # Line below works fine but doesn't clean itself up upon completion. Must use cleanup function at thread end.
        self.__outerAttr[threading.current_thread().ident] = obj

        # TODO: Lines below for Activity re-entrant code (implements a stack of outerObjects for each thread_id).
        #  Must still use a dict of stack/lists 'cause the last value appended must be made available to the getter.
        #  If a method func has a parameter "a" bound to it, and gets called with *args, it will add "a" to the
        #  beginning of *args and then pass it to the function. The beginning is important here: func is bound to
        #  the outerObject setter call.
        #  THIS IS THE MOST EFFICIENT WAY OF IMPLEMENTING FULLY THREAD-SAFE, RE-ENTRANT CODE.
        # k = threading.current_thread().ident
        # self.__outerAttr[k].append(obj)              # Adds animal obj, tag obj, person obj, etc. to stack.
        #
        # def inner(func, *args, **kwargs):
        # # func will be passed to inner as 1st argument by the invoking function 'cause it's bound in the func call
        #     ret = func(*args, **kwargs)
        #     self.__outerAttr[k].pop()           # Removes outerObj from stack.
        #     # if not self.__outerAttr.get(k):
        #     #     self.__outerAttr.pop(k)  # Cleans up dict key if stack is empty. Can also use cleanup function.
        #     return ret
        #
        # return inner

    def _outerAttr(self):
        return self.__outerAttr  # Needed to look at __outerAttr during debugging.


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
        return self._short_name   # defined in the relevant subclasses. Contains a short name for the Activity

    def __del__(self):
        pass
        # try:
        #     if self.__outerAttr:             # Destructor checks consistency of __outerAttr y reporta inconsistencias
        #         krnl_logger.warning(f'ERR_SYS_Logic Error: __outerAttr len: {len(self.__outerAttr)} / '
        #                             f'Activity: {self._activityName}. Class: {self.__class__.__name__}')
        # except AttributeError:
        #     pass

    @classmethod
    def is_ready(cls):
        """
        Returns state of readiness of memory data structures. Used to verify that initialization is complete.
        In Activity Classes, used for classes that support_mem_data(), that require special initializations.
        _futures dictionary is initialized in EntityObject.__init_subclass__(),
        @return: True if class is ready. False: not ready (init still running).
        """
        if cls.supports_mem_data():
            futures_list = Activity._futures.get(cls)
            if futures_list is not None:
                return all(j.done() for j in futures_list)
        return True     # Always ready for classes that don't use mem_data.


    @classmethod
    def wait_till_ready(cls, timeout=8):
        """ Waits for class cls to complete its initialization process. Can be called by class or by instance object.
        @return: True: all ready. False: timed out. There are tasks left unfinished.
        """
        if cls.supports_mem_data():
            timeout = timeout if isinstance(timeout, (int, float)) and timeout > 0 else 8
            futures_list = Activity._futures.get(cls, ())
            # print(f'Entering wait_till_ready() function. ', end=' ')
            if futures_list and any(j.running() for j in futures_list):
                print(f'{cls.__name__} initialization running. Now waiting...')
                result = concurrent.futures.wait(futures_list, timeout=timeout,
                                                 return_when=concurrent.futures.ALL_COMPLETED)
                return not result.not_done               # result = (done=set(), not_done=set()) - type namedtuple.
        return True

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


    @property
    def _isInventoryActivity(self):
        return self.__doInventory

    def _getActivityID(self, tblActivityID=None):
        """ returns __activityID by querying tblActivityID. If tblActivityID is None, returns self.__activityID"""
        return tblActivityID if tblActivityID else self.__activityID

    @staticmethod
    def set3frames(*args, dfRA_name=None, dfLink_name=None, dfData_name=None, default: str | None = 'dataframe'):
        """ Sets dfRA, dfLink, dfData with passed arguments.
        @param dfRA_name:
        @param dfLink_name:
        @param dfData_name:
        @param default: 'dataframe', 'df' | None. default to set dataframes to.
                        'dataframe': creates an empty DataFrame with name = dfName.
                         None: Returns None for the dataframe if dataframe name is not found in args.
            If any of the settings fail, raises ValueError
        """
        if any(n is None for n in (dfRA_name, dfLink_name, dfData_name)):
            raise ValueError('ERR_ValueError - set3frames: Error creating dataframes. Missing DataFrame names.')

        default = default.lower() if isinstance(default, str) else default
        if default is not None:
            default = 'dataframe'    # Any value different from None converts to 'dataframe' and returns full data.
        if default is not None and (default.startswith('dataframe') or default.startswith('df')):
            try:
                dfRA = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == dfRA_name),
                            pd.DataFrame.db.create(dfRA_name))
                dfLink = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == dfLink_name),
                              pd.DataFrame.db.create(dfLink_name))
                dfData = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == dfData_name),
                              pd.DataFrame.db.create(dfData_name))
            except (NameError, ValueError, TypeError, SyntaxError, AttributeError):
                raise ValueError('ERR_ValueError - set3frames Invalid argument(s): Error creating dataframes.')
        else:
            dfRA = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == dfRA_name), None)
            dfLink = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == dfLink_name), None)
            dfData = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == dfData_name), None)

        return dfRA, dfLink, dfData


    @staticmethod
    def activity_tasks(*, before=(), after=()):
        """ Wrapper to run a list of tasks before and after the @decorated Activity method executes.
        @param before: list of tasks to execute before Activity method.
        @param after: list of tasks to execute after Activity method.
        tasks passed in before and after lists DO NOT support return values.
        tasks in task lists CANNOT be bound, due to the structure of this call.
        """
        def tasks_decorator(func):
            """ Timer @decorator to time execution of function calls. """
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                # Turns args into a mutable iterable so DataTable that arguments can be passed back. This is required
                # for when the args list is incomplete and arguments are added or removed during the execution of func.

                # if 'returned_params' not in kwargs:
                #     kwargs['returned_params'] = {}      # Placeholder dict to pass params back from executed func.

                for fn in before:
                    fn(self, *args, **kwargs)

                for fn in after:
                    if 'doInventory' in fn.__name__:
                        args = list(args)
                        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName,
                                                               dfLink_name=self._tblLinkName,
                                                               dfData_name=self.tblDataName(), default=None)
                        if dfRA is None:
                            dfRA = pd.DataFrame.db.create(self._tblRAName)
                            args.append(dfRA)
                        if dfLink is None:
                            dfLink = pd.DataFrame.db.create(self._tblLinkName)
                            args.append(dfLink)
                        if dfData is None:
                            try:
                                dfData = pd.DataFrame.db.create(self.tblDataName())
                            except (AttributeError, TypeError):
                                pass
                            else:
                                args.append(dfData)

                retValue = func(self, *args, **kwargs)  # Executes function call. Updates execution_date, idActividadRA.

                for fn in after:
                    fn(self, *args, **kwargs)
                return retValue
            return wrapper
        return tasks_decorator


    def doInventory(self, *args: pd.DataFrame, **kwargs):
        """
        Checks whether an Activity (other than InventoryActivity) must record or not an Inventory entry.
        Gives priority to a directive passed in kwargs to execute or not an Inventory Activity.
        @param kwargs: 'recordInventory'=True/False
        Logic: kwargs 'recordInventory' overrides the Activity setting of internal variable doInventory
        @return: True if Activity must perform Inventory, otherwise False
        """
        # If kwargs['recordInventory'] is not passed (normal case) -> uses self._isInventoryActivity setting
        recordInventory = next((v for k, v in kwargs.items() if 'recordinvent' in k.lower()), VOID)
        inv_activity = self.outerObject.getActivitiesDict().get(kwargs.get('activity_name', None), None)
        inv_activity = inv_activity if inv_activity is not None else self._isInventoryActivity
        if (inv_activity if recordInventory == VOID else bool(recordInventory)):
            dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                                   dfData_name=self.tblDataName())
            fldDate = dfData.iloc[0, dfData.columns.get_loc('fldDate')] if not dfData.empty and 'fldDate' \
                                                                           in dfData.columns else None
            if pd.notnull(fldDate):
                # Sets inventory ONLY if a valid fldDate value is passed in frame dfData.
                self.outerObject.inventory.set(dfRA, dfLink, date=fldDate, **kwargs)


    def paScheduler(self, *args, **kwargs):
        """
        Runs from decorator @task_activity that is executed for selected functions.
        Uses data from dfRA and dfData dataframes that are set from within the function executed by the decorator.
        @param args: dfRA, dfLink, dfData dataframes.
        @param kwargs: execute_fields (dict), excluded_fields (dict). Passed by Activiy object making the call.
        @return:
        """
        if self._supportsPA:
            # idActividadRA comes set in dfRA. fldDate comes in dfData. Values generated in executed function.
            dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                                   dfData_name=self._tblDataName, default=None)
            date = dfData.iloc[0, dfData.columns.get_loc('fldDate')] if dfData is not None and 'fldDate' in \
                                                                        dfData.columns else None
            if pd.notnull(date):
                executeFields = self.activityExecuteFields(execution_date=date)
                execute_fields = kwargs.get('execute_fields', None)
                if execute_fields and isinstance(execute_fields, dict):
                    executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
                excluded_fields = kwargs.get('excluded_fields', None)
                excluded_fields = set(excluded_fields) if isinstance(excluded_fields, (list, set, tuple)) else set()
                excluded_fields.update(self.getActivityExcludedFieldsClose(self._activityName))
                self._paMatchAndClose(dfRA.iloc[0, dfRA.columns.get_loc('fldID')], execute_fields=executeFields,
                                      excluded_fields=excluded_fields)
                # Updates fldExecuteData, fldExcludedFields in tblLink so that this data is replicated to other nodes.
                # Uses fact that dfLink fldID arg. is updated by _createActivityRA().
                fldID_Link = dfLink.iloc[0, dfLink.columns.get_loc('fldID')] if dfLink is not None and 'fldID' \
                                                                                in dfLink.columns else None
                if pd.notnull(fldID_Link):
                    setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
                              fldExcludedFields=excluded_fields)
            # TODO: DO NOT manage paCreateExecutionInstance() here for now. Leave it only to the activities that must
            #  check the condition of flagExecutionInstance.
        return


    def set_mem_data(self, val: dict, *, uid=None):     # Works with iterators (01Aug24)
        """              # val must be of the form defined by cls._mem_data_params['field_names].
        @param uid: Object uid that data is associated with. Use when calling via Animal class (Bovine, Caprine, etc.).
        @param val: values to assign to __local_active_uids_dict[obj_uid].
        @return: True if val was set; None if val not set.
        """
        outer_object = self.outerObject  # outerObject can be Animal Class, or an instance of Animal class (object).
        try:
            uids_orig = outer_object.get_active_uids_iter()  # ITERABLE from Bovine, etc. NOT to be messed with!!
        except AttributeError:
            return None

        obj_uid = outer_object.ID if type(outer_object) is not type else uid
        obj_class = type(outer_object) if type(outer_object) is not type else outer_object
        obj_class = getattr(obj_class, '_parent', obj_class)  # TagStandardBovine, TagRFIDBovine -> resolve to TagBovine
        local_uids = getattr(self, '_' + self.__class__.__name__ + '__local_active_uids_dict', None)
        if local_uids is not None:
            if obj_class not in local_uids:     # Creates dataframe if 1st time.
                local_uids[obj_class] = pd.DataFrame([], columns=list(self._mem_data_params['field_names']))

            # Writes value to __local_active_uids_dict DataFrame only if obj_uid is valid (present in uids_orig dict).
            found = False
            for df in uids_orig:
                if any(df.fldObjectUID.isin((obj_uid,))):
                    found = True
                    if isinstance(val, dict):
                        if set(val) != set(local_uids[obj_class].columns.to_list()):
                            krnl_logger.error(f'ERR_Invalid Argument {val}. Cannot set Memory Data for Activity'
                                              f'{self.__class__.__name__}.')
                            return None
                        val = {k: v for k, v in val.items() if k in local_uids[obj_class].columns}
                        items = val.items()
                        # Uses is_ready() to sync with _memory_data_init_last_rec() and not modify dicts while it runs.
                        if not self.is_ready():
                            self.wait_till_ready()  # Waits with timeout. If call times out still writes data to local_uids.
                        # Skips all values not listed in df columns.
                        with self._slock_activity_mem_data:
                            local_uids[obj_class].loc[obj_uid, [j[0] for j in items]] = [j[1] for j in items]
                        return True
                continue            # If nothing found, moves to next item in iterator.

            if not found:        # object uid not in _active_uids_df original.
                if obj_uid in local_uids[obj_class].index:
                    if local_uids:
                        try:
                            with self._slock_activity_mem_data:  # Serializes concurrent access when writing to memory.
                                local_uids[obj_class].drop(index=obj_uid)       # Removes row corresponding to uid.
                        except (ValueError, AttributeError, TypeError, KeyError, IndexError):
                            pass
        return None


    # Full blown function with re-entry management. Decorator to execute pa scheduling code (see above)
    # @activity_tasks(after=(paScheduler, ))
    def _setInventory(self, *args: pd.DataFrame,  execute_fields=None, excluded_fields=None, **kwargs):
        """                 TODO: This method must support re-entry.
        Inserts Inventory obj_data for the object in the DB. This function is called by classes Animal,Tag,Person,Device
        @param set_tag_inventory: True -> executes Inventory Activity for object._tags_read set.
        This function hides all the complexities of accessing obj_data and db tables for an Inventory operation.
        @param * args: List of data tables (tblRA, tblLink, tblInventory) to write to DB.
        @param kwargs: isProg: True->Programmed Activity. Used by Signature methods to run the Programmed Activities Logic.
                       'date': datetime obj to use as Inventory date.
        @return: Success: ID_Actividad RA (int), Error: errorCode (str), None if nothing's written.
        """
        if type(self.outerObject) is type:          # set() funcs. can only be run by actual objects, not classes.
            raise TypeError(f'ERR_ValueError: Trying to run Inventory.set() via a class. This activity can only be run'
                            f' by object instances.')
        dfRA, dfLink, dfData = self.set3frames(dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self._tblDataName)
        if self._isValid:
            # Prioridad eventDate:    1: fldDate(tblData); 2: kwargs(fecha valida); 3: timeStamp.
            # Discards dates in the future for timeStamp, eventDate.
            time_now = time_mt('dt')
            timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if not dfRA.empty and pd.notnull(dfRA.loc[0, 'fldTimeStamp'])
                            else time_now, time_now)
            eventDate = min(getEventDate(tblDate=dfData.loc[0, 'fldDate'].to_pydatetime() if not dfData.empty and
                            pd.notnull(dfData.loc[0, 'fldDate']) else timeStamp, defaultVal=timeStamp, **kwargs),
                            timeStamp)
            activityID = self._getActivityID(dfRA.loc[0, 'fldFK_NombreActividad'] if not dfRA.empty and
                                              'fldFK_NombreActividad' in dfRA.columns else None) or self._activityID
            dfRA.loc[0, ('fldFK_NombreActividad', 'fldTimeStamp')] = (activityID, timeStamp)
            dfData.loc[0, 'fldDate'] = eventDate                # TODO: This value is passed back to paScheduler()

            idActividadRA = self._createActivityRA(dfRA, dfLink, dfData, **kwargs)
            retValue = idActividadRA
            if isinstance(retValue, int):
                dfRA.loc[0, 'fldID'] = idActividadRA            # TODO: This value is passed back to paScheduler()
                if self.supports_mem_data():
                    memVal = self.get_mem_data(query_func=self._getInventory)       # memVal is a dictionary
                    days2to = memVal.get('fldDaysToTimeout') or self.outerObject.daysToTimeout
                    if not memVal or memVal.get('fldDate', 0) < eventDate:
                        # writes mem_data only if data is newer than data currently in memory.
                        values = (eventDate, idActividadRA, days2to)
                        vals = {k: v for k, v in dfData.iloc[0].to_dict().items() if k in
                                self._mem_data_params['field_names']}
                        vals.update(dict(zip(('fldDate', 'fldFK_Actividad', 'fldDaysToTimeout'), values)))
                        # set_mem_data uses a lock to prevent concurrent write attempts by different threads
                        self.set_mem_data(vals)

                # TODO: the tags themselves may have other inventory events (if they are inventoried while not
                #  attached to an object, for instance). In short tag inventories are to be handled TOTALLY INDEPENDENT
                #  LY from any other type of inventory: they are handled by Tag Activities only. In particular, the
                #  TagInput Activity that must handle all tag inputs for the system. A tag Input is defined as an action
                #  whereby a tag object is scan or read, hence a inventory on it must be performed.
                #  With this, all the code below is redundant and improperly placed. Deprecated. (25Aug24)
                # Sets Inventory Activity for tags read.  Sets tag inventory w/ same timestamp as object inventory.
                # set_tag_inventory = kwargs.get('set_tag_inventory', True)
                # if set_tag_inventory and isinstance(set_tag_inventory, (tuple, list)):
                #     set_tag_inventory = set_tag_inventory.pop()   # pops last value from list

                # if set_tag_inventory and 'Tag' not in self.__class__.__name__ and \
                #         kwargs.get('comm_type', '').lower().strip() not in ('comision', 'commission'):
                # comm_type filters out calls from assignTags, where tag inventories are recorded by commission.set()

                # if any(j in self.__class__.__name__ for j in ('Animal', 'Device')):     # TODO: USE __mro__ here!!
                #     # Will set inventories for tags only when Animal or Device activites call, and tag_list not empty.
                #     tag_list = kwargs.get('tag_list', None) or getattr(self.outerObject, '_tags_read', None)
                #     if tag_list:
                #         if not hasattr(tag_list, "__iter__"):
                #             tag_list = (tag_list,)
                #         for t in tag_list:
                #             try:
                #                 # Recursive call to _setInventory. "tag_list" MUST NOT be in kwargs to avoid
                #                 # infinite looping.
                #                 t.inventory.set(date=max(timeStamp, eventDate))
                #             except (AttributeError, TypeError, NameError):
                #                 continue


                # Minimum required at Activity level to drive the ProgActivity logic. The rest is done by paScheduler().
                # This code is specific for the Activity (executeFields). idActividadRA just created and excluded_fields
                # are also required to be passed on.
                # if self._supportsPA:
                #     executeFields = self.activityExecuteFields(execution_date=eventDate)
                #     if execute_fields and isinstance(execute_fields, dict):
                #         executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
                #     if 'returned_params' in kwargs:
                #         kwargs['returned_params'].update({'idActividadRA': idActividadRA, 'execute_fields':executeFields,
                #                                           'excluded_fields': excluded_fields})  # for paScheduler()
                    # _setInventory() doesn't pass (for now) _paExecutionInstance.
            return retValue
        else:
            return None


    def get_mem_data(self, fld: str = None, *, query_func=None, uid=None):
        """
        To increase performance, can be called by and Activity object WITH OR WITHOUT an outerObject. When called
        without outerObject, a uid must be provided and an object instance is not created, reducing execution times.
        @param fld: str. key name to return value for (single val). If not provided, returns full row dictionary.
        @param query_func: callable to pull value from database in case object uid is not present in local_uids_dict.
        @param uid: Animal uid. Passed when calling with a Animal class instead of outerObject.
        @return: dictionary of form {fldName: fldValue, } with dict structure dictated by the Activity class.
                If fld is provided, returns value associated to fld.
        """
        outer_object = self.outerObject
        outer_obj_type = type(outer_object)   # outerObject can be an object or a class (Bovine, Caprine, etc.)
        obj_class = outer_obj_type if outer_obj_type is not type else outer_object
        obj_class = getattr(obj_class, '_parent', obj_class)  # TagStandardBovine, TagRFIDBovine -> resolve to TagBovine
        try:
            # Gets uids from Animal object or from Animal class, whichever is available.
            uids_orig = outer_object.get_active_uids_iter()
        except (AttributeError, KeyError):
            return None
        obj_uid = outer_object.ID if outer_obj_type is not type else uid
        local_uids = getattr(self, '_' + self.__class__.__name__ + '__local_active_uids_dict', {})
        local_df = local_uids.get(obj_class, None)  # Bovine, Caprine, etc are keys to local_uids dataframe.
        found = False
        for df in uids_orig:
            if isinstance(df, str):  # TODO: transition 'if'. Remove when migration to dataframe is complete.
                return df

            if obj_uid in df['fldObjectUID'].tolist():
                found = True
                # Returns value only if uid exists in object dataframe for outer_obj class
                if local_df is not None and not local_df.empty:
                    if obj_uid not in local_df.index:  # TODO: IS THIS OK??? VERIFY: local_df should NOT have fldObjectUID as index.
                        # uid not found:There's a new entry in _active_uids to be mirrored in memory (local_uid_dict)
                        if not callable(query_func):
                            return None     # no function to fetch data. Returns None.
                        db_record = query_func(mode='value', full_record=True, uid=obj_uid)  # New uid data row from db.
                        if not isinstance(db_record, dict):
                            return None
                        # Sets up new entry in __local_active_uids_dict
                        keys = self._mem_data_params['field_names']
                        values = [db_record[k] for k in keys]  # Sets up data list.
                        with self._slock_activity_mem_data:      # Serializes concurrent access when writing to memory.
                            local_uids[obj_class].loc[obj_uid, keys] = values
                    row_dict = local_uids[obj_class].loc[obj_uid].to_dict()
                    if fld:
                        return row_dict.get(fld, None)
                    return row_dict                     # Returns dictionary {fldName: fldValue, }
            continue        # If fldObjectUID not found, moves to the next item in iterator.

        if not found:
            # object uid not in _active_uids_df original. pops() it from the Activity dict if it's there.
            if local_uids:
                if obj_uid in local_uids[obj_class].index:          # index contains all uid values.
                    try:
                        with self._slock_activity_mem_data:  # Serializes concurrent access when writing to memory.
                            local_uids[obj_class].drop(index=obj_uid)
                    except (ValueError, TypeError, AttributeError, KeyError, IndexError):
                        pass
            return None


    def _getInventory(self, *args: pd.DataFrame, mode='mem', full_record=False, uid=None, all_records=None, **kwargs):
        """
        Returns ALL records in table Data Inventario between sValue and eValue. sValue=eValue ='' -> Last record
        """
        fldName = 'fldDate'
        modeArg = (mode or 'value') if self.supports_mem_data() else 'value'
        if 'mem' in modeArg:
            # Todo(cmt): get_mem_data reduces drastically db access. _getInventory() MUST support re-entry!!!
            tmp = self.get_mem_data(query_func=self._getInventory, uid=uid)
            if isinstance(tmp, (int, dict)):
                return tmp.get(fldName) if not full_record else tmp

        # THIS IS INVENTORY ACTIVITY..
        query_df = self._get_link_records(uid=uid, **kwargs)
        if not query_df.empty:
            if all_records:
                return query_df
        else:
            return None

        latest_rec = query_df.loc[query_df['fldDate'] == query_df['fldDate'].max()].iloc[0]
        return latest_rec if full_record else latest_rec[fldName]


    def _check_timeout(self, *, uid=None):       # TODO(cmt): Uses only memory data. Called by InventoryActivity obj.
        """ Checks for object timeout (since object's last recorded inventory).
        @return: (int) if days since last recorded inventory if time elapsed since last inventory exceeds object's
                 days_to_timeout value (timeout condition).
                 0 if no timeout.
                 None if timeout not defined for the object.
        """
        inv_rec = self._getInventory(uid=uid, full_record=True)
        last_inv = inv_rec.get('fldDate')
        days_to_timeout = inv_rec.get('fldDaysToTimeout') or getattr(self.outerObject, '_defaultDaysToTimeout', None)
        if isinstance(last_inv, datetime) and days_to_timeout is not None:
            if USE_DAYS_MULT:
                days = (time_mt()-last_inv.timestamp())*DAYS_MULT
            else:
                days = (time_mt('datetime')-last_inv).days
            return int(days) if days >= days_to_timeout else 0
        return None     # Returns None when timeout is not defined for the object or data is invalid.


    # @activity_tasks(after=(doInventory, ))
    def _setStatus(self,  *args: pd.DataFrame, status: str = None, **kwargs):    #
        """
        MANDATORIO: ID_Actividad, ID_Status, ActivoYN, FechaEvento -> fldFK_Actividad, fldFK_Status, fldFlag, fldDate
        Inserts Status obj_data for the object in the DB. This function is called by classes Animal, Tag, Person, etc.
        This function hides all the complexities of accessing obj_data and writing tables for an Inventory operation.
        @param status: status value (string)
        @param args: DataFrame objects, with all the tables and fields to be written to DB
        @param kwargs: 'status'=statusValue -> Status to set when status is not passed via DataTable
                       'recordInventory' = True/False -> Forces the insertion (or not) of an Inventory Record. Higher
                        priority than doInventory setting.
                       'isProg' = True/False -> Activity is Programmed Activity, or not. If not passed -> assumes False.
        @return: Success: ID_Actividad RA (int), Error: errorCode (str)
        """
        dfData = next((j for j in args if j.db.tbl_name == self._tblDataName),pd.DataFrame.db.create(self._tblDataName))
        statusDict = {k.lower(): v for k, v in self.outerObject.statusDict.items()}  # lowers statusdict for ease of use
        if isinstance(status, str):
            status = status.strip().lower()
            statusID = statusDict[status][0] if status in statusDict else None
        else:
            if isinstance(status, int):
                statusID = status if status in [j[0] for j in statusDict.values()] else None
            else:
                statusID = None
        if status is None:
            statusID = dfData.loc[0, 'fldFK_Status'] if not dfData.empty and 'fldFK_Status' in dfData.columns else None
        if pd.isnull(statusID):
            retValue = f'ERR_INP_InvalidArgument: {status}. {moduleName()}({lineNum()} - ' \
                       f'{callerFunction()})'
            return retValue      # Sale si es valor de status no valido
        else:
            status = next(k for k, v in self.outerObject.statusDict.items() if v[0] == statusID)
        dfRA = next((j for j in args if j.db.tbl_name == self._tblRAName), pd.DataFrame.db.create(self._tblRAName))
        dfLink = next((j for j in args if j.db.tbl_name == self._tblLinkName),pd.DataFrame.db.create(self._tblLinkName))

        if self._isValid and self.outerObject.validateActivity(self._activityName):
            # Prioridad eventDate:    1: fldDate(tblData); 2: kwargs(fecha valida); 3: timeStamp.
            # Discards dates in the future for timeStamp, eventDate.
            time_now = time_mt('dt')
            timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if not dfRA.empty else time_now, time_now)
            eventDate = min(getEventDate(tblDate=dfData.loc[0, 'fldDate'] if not dfData.empty else 0,
                                         defaultVal=timeStamp, **kwargs),
                            timeStamp)
            activityID = dfRA.loc[0, 'fldFK_NombreActividad'] if not dfRA.empty else self._activityID
            # activityID = activityID if activityID else self._activityID
            dfRA.loc[0, 'fldFK_NombreActividad'] = activityID
            dfData.loc[0, 'fldDate'] = eventDate
            # TODO(cmt): this (writing __outerAttr from the outerObject call) is safe, as the outerObject call sets up
            #  a dictionary, LOCAL TO THE ACTIVITY, of the form {thread_id: object}. Its safe AS LONG AS different
            #  methods of an activity always refer to the SAME outerObject. That is, Activity methods are NOT invoked
            #  by any objects other than the initial outerObject caller.
            currentStatusID = self.outerObject.status.get()
            # permittedFrom()  definida en las sub-Clases (Tag, Animal, PersonActivityAnimal, Bovine, Caprine, etc)
            if statusID in self.permittedFrom()[currentStatusID] or kwargs.get('enforce'):  # Ignora no permitidos.
                flagExecInstance = (currentStatusID != statusID) if self.classSupportsPA() else False
                commentData = dfData.loc[0, 'fldComment'] or '' + f'ObjectID: ' \
                                                                    f'{self.outerObject.getID} / Activity: {activityID}'
                dfData.loc[0, ('fldFK_Status', 'fldFlag', 'fldComment')] =   \
                    (statusID, self.outerObject.statusDict[status][1], commentData)
                idActividadRA = self._createActivityRA(dfRA, dfLink, dfData, **kwargs)
                retValue = idActividadRA
                if isinstance(retValue, int):
                    dfRA.loc[0, 'fldID'] = idActividadRA         # TODO: This value is passed back to paScheduler()
                    if self.supports_mem_data():
                        memVal = self.get_mem_data(query_func=self._getStatus)  # memVal is a dict
                        # TODO(cmt): Comparison to define update of a memory data record> This comparison must be done
                        #  here, at _set() function level (Not elsewhere, like in MemoryData objects for instance)
                        if not memVal or memVal['fldDate'] < eventDate:
                            values = (statusID, eventDate, idActividadRA)  # ('fldFK_Status', 'fldDate', 'fldFK_Actividad')
                            vals = {k: v for k, v in dfData.iloc[0].to_dict().items() if k
                                    in self._mem_data_params['field_names']}
                            vals.update(dict(zip(('fldFK_Status', 'fldDate', 'fldFK_Actividad'), values)))
                            # set_mem_data() uses a lock to prevent concurrent write attempts by different threads
                            self.set_mem_data(vals)

                    if flagExecInstance:
                        # Check if conditions are met for an instances of ProgActivities (1 or more) to be assigned to
                        # self.outerObject when flagExecInstance is set. This activity can be any activity whose
                        # execution create conditions for OTHER ProgActivity/ProgActivities to be defined on outerObject
                        self._paCreateExecInstance(outer_obj=self.outerObject)  # Defines creation of a ProgActivity.

                   # if self.doInventory(**kwargs):
                    #     _ = self.outerObject.inventory.set(tblRA, tblLink, date=max(timeStamp, eventDate))
                else:
                    retValue = f'ERR_Sys_Object not valid or activity not defined: {retValue}- {callerFunction()}'
                    krnl_logger.info(retValue)
                    print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)
            else:                                                           # Sale si valor de status no permitido
                retValue = f'INFO_INP_InvalidArguments: Status {statusID} not set.'
                krnl_logger.info(retValue)
        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - {callerFunction(getCallers=True)}'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}  - {retValue}', dismiss_print=DISMISS_PRINT)

        return retValue


    def _getStatus(self,  *args: pd.DataFrame, mode='mem', full_record=False, uid=None, all_records=False, **kwargs):
        """
        Returns records in table Data Status between sValue and eValue. If sValue = eValue = '' -> Last record
        """
        fldName = 'fldFK_Status'
        modeArg = (mode or 'value') if self.supports_mem_data() else 'value'
        uid = getattr(self.outerObject, 'ID', None) or uid
        if 'mem' in modeArg.lower():
            tmp = self.get_mem_data(query_func=self._getStatus, uid=uid)  # TODO(cmt):_getStatus() MUST support re-entry
            if isinstance(tmp, dict):
                return tmp.get(fldName) if not full_record else tmp

        query_df = self._get_link_records(uid=uid, **kwargs)
        if not query_df.empty:
            if all_records:
                return query_df
            latest_rec = query_df.loc[query_df['fldDate'] == query_df['fldDate'].max()].iloc[0]
            return latest_rec if full_record else latest_rec[fldName]
        return None

    # @activity_tasks(after=(doInventory, paScheduler))
    def _setLocalization(self, *args: pd.DataFrame, execute_fields=None, excluded_fields=None, **kwargs):
        """
        MANDATORIO: ID_Actividad, ID_Localizacion
        creates a LocalizationActivityAnimal record in the DB. This function is called by classes Animal, Tag,
        PersonActivityAnimal and Device. This function hides all the complexities of accessing obj_data and writing
        tables for a LocalizationActivityAnimal operation.
        @param tblRA, tblLink, tblData: list of DataTable objects, with all the tables and fields to be written to DB
        @param args: Additional obj_data tablas to write (Inventory, etc): WRITTEN AS PASSED. They must come here complete!
        @return: Success: ID_Actividad RA (int), Error: errorCode (str) or None.
        """
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self._tblDataName)
        loc = dfData.loc[0, 'fldFK_Localizacion'] if not dfData.empty else None
        if not loc:
            loc = kwargs.get('localization', None)
        if isinstance(loc, str):
            loc = Geo.getObject(loc)
            if isinstance(loc, (list, tuple)):      # Geo.getObject() returns a tuple with 1 or more objects.
                loc = loc[0]
        if not isinstance(loc, Geo):  # No se paso localizacion valida. Sale nomas...
            return None

        if self._isValid and self.outerObject.validateActivity(self._activityName):
            # flagExecInstance flags a condition change in the object that may (or may not) trigger PA creation.
            flagExecInstance = (self.outerObject.localization.get() != loc) if self.classSupportsPA() else False
            dfRA = next((j for j in args if j.db.tbl_name == self._tblRAName), pd.DataFrame.db.create(self._tblRAName))
            activityID = dfRA.loc[0, 'fldFK_NombreActividad'] if not dfRA.empty and 'fldFK_NombreActividad' in dfRA \
                                                                        else None
            activityID = activityID if activityID else self._activityID
            # Discards dates in the future for timeStamp, eventDate.
            time_now = time_mt('dt')
            timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if not dfRA.empty else time_now, time_now)
            date = dfData.loc[0, 'fldDate'] if not dfData.empty and 'fldDate' in dfData.columns else 0
            eventDate = min(getEventDate(tblDate=date, defaultVal=timeStamp, **kwargs), timeStamp)
            dfRA.loc[0, 'fldFK_NombreActividad'] = activityID
            dfData.loc[0, ('fldFK_Localizacion', 'fldDate')] = (loc.ID, eventDate)
            idActividadRA = self._createActivityRA(dfRA, dfLink, dfData, **kwargs)  # initializes dfRA.loc[0, 'fldID']
            retValue = idActividadRA
            if isinstance(retValue, int):
                # dfRA.loc[0, 'fldID'] = idActividadRA  # TODO: This value is passed back to paScheduler()
                if self.supports_mem_data():
                    memVal = self.get_mem_data(query_func=self._getLocalization)
                    if not memVal or memVal['fldDate'] < eventDate:
                        # localiz mem_data: ('fldFK_Localizacion', 'fldDate', 'fldFK_Actividad', 'localiz_level')
                        values = (loc.ID, eventDate, idActividadRA, loc.localizLevel)
                        vals = {k: v for k, v in dfData.iloc[0].to_dict().items()
                                if k in self._mem_data_params['field_names']}
                        vals.update(dict(zip(('fldFK_Status', 'fldDate', 'fldFK_Actividad',
                                              'fldFK_Nivel_De_Localizacion'), values)))
                        # set_mem_data() uses a lock to prevent concurrent write attempts by different threads
                        self.set_mem_data(vals)

                if flagExecInstance:
                    # Check if conditions are met for an instances of ProgActivities (1 or more) to be assigned to
                    # self.outerObject when flagExecInstance is set. This activity can be any activity whose
                    # execution create conditions for OTHER ProgActivity/ProgActivities to be defined on outerObject
                    self._paCreateExecInstance(outer_obj=self.outerObject)  # Defines creation of a ProgActivity.
            else:
                retValue = f'ERR_Sys_Object not valid or activity not defined: {retValue}- {callerFunction()}'
                krnl_logger.info(retValue)
                print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)
                return retValue
        else:
            retValue = None
        return retValue

    def _getLocalization(self, *args: pd.DataFrame, mode='mem', full_record=False, uid=None, all_records=False,
                         **kwargs):
        """                         *******     MUST support re-entry  ********
        Returns records in table Data Status between sValue and eValue. If sValue = eValue = '' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode='value' -> Returns last value from DB. If no mode or mode='memory' returns value
                        from Memory.
        @return: Geo object or dictionary of the form {fldName: fldValue} if full_record=True.
        """
        fldName = 'fldFK_Localizacion'
        modeArg = (mode or 'value') if self.supports_mem_data() else 'value'
        if 'mem' in modeArg.lower():
            tmp = self.get_mem_data(query_func=self._getLocalization, uid=uid)
            if tmp is not None:
                return tmp.get(fldName) if not full_record else tmp

        query_df = self._get_link_records(uid=uid, **kwargs)
        if not query_df.empty:
            if all_records:
                return query_df
            latest_rec = query_df.loc[query_df['fldDate'] == query_df['fldDate'].max()].iloc[0]
            return latest_rec if full_record else latest_rec[fldName]
        return None


    # @activity_tasks(after=(paScheduler, ))
    def _setPerson(self, *args: pd.DataFrame, execute_fields, excluded_fields, **kwargs):
        """
        Creates records in [Data Animales Actividad Personas] table in database.
        This function is called by classes Animal, Tag and Device
        @param args (type DataFrame): dfData is parsed as 1 person per dataframe record..
        @return: Success: ID_Actividad RA (int), Error: errorCode (str)
        """
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self._tblDataName)
        if dfData.empty:
            err = f'ERR_ValueError: Missing arguments in call to person.set(): Invalid or missing person data'
            krnl_logger.warning(err)
            return err

        tblPerson = 'tblPersonas'
        where_str = f'WHERE "{getFldName(tblPerson, "fldPersonLevel")}" == 1 AND ' \
                    f'("{getFldName(tblPerson,"fldDateExit")}" == 0 OR "{getFldName(tblPerson,"fldDateExit")}" IS NULL)'
        dfObjects = getrecords('tblPersonas', '*', where_str=where_str)  # where_str does NOT require ';' at the end.
        valid_person_uids = dfObjects['fldObjectUID'].tolist()
        if self._isValid and self.outerObject.validateActivity(self._activityName):
            flagExecInstance = False if self.classSupportsPA() else False  # TODO: define conditions for flag to be True
            activityID = dfRA.loc[0, 'fldFK_NombreActividad'] if not dfRA.empty and 'fldFK_NombreActividad' in \
                                                                 dfRA.columns else None
            activityID = activityID or self._activityID
            dfRA.loc[0, 'fldFK_NombreActividad'] = activityID
            time_now = time_mt('dt')
            timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if not dfRA.empty and 'fldTimeStamp' in dfRA.columns else
                                                                          time_now, time_now)
            date = dfData.loc[0, 'fldDate'] if 'fldDate' in dfData.columns and pd.notnull(dfData.loc[0, 'fldDate']) \
                                            else None
            eventDate = min(getEventDate(tblDate=date, defaultVal=timeStamp, **kwargs), timeStamp)
            dfData['fldDate'] = eventDate         # Sets date for all records.
            initial_persons = dfData['fldFK_Persona'].tolist() if 'fldFK_Persona' in dfData.columns else ()
            dfPersons = pd.DataFrame.db.create(dfData.db.tbl_name,
                                               data=dfData[dfData['fldFK_Persona'].isin(valid_person_uids)].to_dict())
            if dfPersons.empty:
                err = f'ERR_ValueError: Invalid arguments in call to person.set(): Invalid or missing person data'
                krnl_logger.warning(err)
                return err

            # Here dfPersons holds the records (not empty) with valid persons in the system. The non-valid were dropped.
            """IMPORTANT: There's no way, at this level, to validate fldPercent_Ownership, fldOwner_Active, etc. 
            It will only verify that the person is Active before assigning % Ownership. Will set % Ownership to 0 for
            Inactive Persons. The rest of the data is written to db as is. IT ALL MUST BE VALID when passed. """
            # Sets all inactive persons ownership to 0
            dfPersons.loc[dfPersons['fldFlag'].isnull(), 'fldPercentageOwnership'] = 0
            dfPersons.loc[dfPersons['fldPercentageOwnership'] > 1, 'fldPercentageOwnership'] = 1
            dfPersons.loc[dfPersons['fldPercentageOwnership'] < 0, 'fldPercentageOwnership'] = 0
            idActividadRA = self._createActivityRA(dfRA, dfLink, dfPersons, **kwargs)
            if isinstance(idActividadRA, int):
                retValue = idActividadRA
                dfRA.loc[0, 'fldID'] = idActividadRA  # TODO: This value is passed back to paScheduler()

                # TODO: See if this criteria below will work...
                if set(initial_persons).symmetric_difference(set(dfPersons['fldFK_Persona'].tolist())):
                    # IF list of owners changes, goes to check if a PA needs to be created.
                    self._paCreateExecInstance(outer_obj=self.outerObject)

                if flagExecInstance:
                    # Check if conditions are met for an instances of ProgActivities (1 or more) to be assigned to
                    # self.outerObject when flagExecInstance is set. This activity can be any activity whose
                    # execution create conditions for OTHER ProgActivity/ProgActivities to be defined on outerObject
                    self._paCreateExecInstance(outer_obj=self.outerObject)  # Defines creation of a ProgActivity.
            else:
                retValue = f'ERR_DBAccessError: {idActividadRA} - {callerFunction(getCallers=True)}'
        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - {callerFunction(getCallers=True)}'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}  - {retValue}', dismiss_print=DISMISS_PRINT)

        return retValue


    def _setTM(self, *args: DataTable, id_activity=None, id_transaction=None, **kwargs):
        """                 TODO: COMPLETE MIGRATION OF THIS METHOD TO DataFrame (30-Jul-24).
        Inserts record "idActividadTM" in [Data Animales Actividad MoneyActivity]. Creates records in
        [Animales Registro De Actividades] and [Link Animales Actividades] if required.
        @param id_activity: idActividadRA from [TM Registro De Actividades]. Goes to fldFK_ActividadTM
        @param id_transaction: idTransaccion from [Data TM Transacciones]. Goes to fldFK_Transaccion
        @param args: DataTables. Only [Animales Registro De Actividades] and [Data Animales Actividad MoneyActivity] are parsed
        @param kwargs: Arguments passed to [Data Animales Actividad MoneyActivity] table.
        @return: idActividadRA: Success / errorCode (str) on error
        """
        if self._isValid:
            dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                                   dfData_name=self._tblDataName)
            if id_activity:
                dfData.loc[0, 'fldFK_ActividadTM'] = id_activity
            if id_transaction:
                dfData.loc[0, 'fldFK_Transaccion'] = id_transaction
            if any(pd.isnull(j) for j in (dfData.loc[0, 'fldFK_ActividadTM'], dfData.loc[0, 'fldFK_Transaccion'])):
                retValue = f'ERR_INP_Invalid argument: idActividadTM. Missing Actividad TM and/or Transaccion - ' \
                           f'{callerFunction()}'
                krnl_logger.info(retValue)
                print(f'{retValue}', dismiss_print=DISMISS_PRINT)
                return retValue             # Sale con error si no se paso idActividadTM, o si no es valida.
            timeStamp = time_mt('datetime')
            userID = sessionActiveUser
            activityID = self._activityID if (dfRA.empty or pd.insull(dfRA.loc[0, 'fldFK_NombreActividad'])) \
                                            else dfRA.loc[0, 'fldFK_NombreActividad']
            idActividadRA = dfRA.loc[0, 'fldID'] if not dfRA.empty else None
            if idActividadRA is None:
                # NO se paso idActividadRA->Insertar records en tblRA,tblLink
                dfRA.loc[0, ('fldTimeStamp', 'fldFK_UserID', 'fldFK_NombreActividad')] = (timeStamp, userID, activityID)
                idActividadRA = setRecord(dfRA.db.tbl_name, **dfRA.loc[0].to_dict())
                if isinstance(idActividadRA, str):
                    retValue = f'ERR_DB_WriteError - Function/Method {moduleName()}({lineNum()}) - ' \
                               f'{callerFunction()}'
                    krnl_logger.info(retValue)
                    print(f'{retValue}', dismiss_print=DISMISS_PRINT)
                    return retValue  # Sale si hay error de escritura en RA.

                # Setea valores en tabla Link SOLO si se inserto registro en RA
                dfRA.loc[0, 'fldID'] = idActividadRA            # TODO: This value is passed back to paScheduler()
                commentLink = dfLink.loc[0, 'fldComment'] if (not dfLink.empty and
                                                              pd.notnull(dfLink.loc[0, 'fldComment'])) else \
                                f'Activity: {self.__activityName} / ActividadTM: {dfData.loc[0, "fldFK_ActividadTM"]}'
                dfLink.loc[0, ('fldFK_Actividad', 'fldFK','fldComment')] = \
                              (idActividadRA, self.outerObject.getID, commentLink)

            # Setea valores en tabla Data y escribe
            eventDate = getEventDate(tblDate=dfData.loc[0, 'fldDate'] if not dfData.empty else timeStamp,
                                     defaultVal=timeStamp, **kwargs)
            dfData.loc[0, ('fldFK_Actividad', 'fldDate')] = (idActividadRA, eventDate)
            wrtTables = (dfLink, dfData)
            retValue = [setRecord(j.db.tbl_name, **j.loc[0].to_dict()) for j in wrtTables]
            # Si hay error de escritura en tblLink o tblData, hace undo de tblRA y sale con error.
            if any(isinstance(val, str) for val in retValue):
                if pd.isnull(dfRA.loc[0, 'fldID']):
                    _ = delRecord(dfRA.db.tbl_name, idActividadRA)
                for j, tbl in enumerate(wrtTables):
                    if isinstance(retValue[j], int):
                        _ = delRecord(tbl.db.tbl_name, retValue[j])
                retValue = 'ERR_DB_WriteError' + f' {moduleName()}({lineNum()})'
                krnl_logger.error(retValue)
                print(f'Deleting record {idActividadRA} / Table: {dfRA.db.tbl_name} / retValue: {retValue}',
                      dismiss_print=DISMISS_PRINT)
            else:
                retValue = idActividadRA
        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - tm.set()'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}  - {retValue}', dismiss_print=DISMISS_PRINT)

        return retValue


    __tblMontosName = 'tblDataTMMontos'
    def _getTM(self,  *args, uid=None, full_record=False, all_records=False, **kwargs):
        """
        Returns records in table [Data MoneyActivity Montos] between sValue and eValue. sValue=eValue ='' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode=datatable->Returns DataTable with full record.  mode='full_record' -> Returns last Record
        in full.
        @return: Object DataTable with information from MoneyActivity Montos. None if nothing's found
        """
        retValue = None
        if self._isValid and self.outerObject.validateActivity(self.__activityName):
            uid = getattr(self.outerObject, 'ID', None) or uid
            tmActivityRecords = self._get_link_records(uid=uid, **kwargs)
            colAnimalActivity = tmActivityRecords['fldFK_ActividadTM'].tolist()  # Recs c/ ID_ActividadTM de ID_Animal
            if colAnimalActivity:
                colstr = str(tuple(colAnimalActivity)) if len(colAnimalActivity) > 1 \
                    else str(tuple(colAnimalActivity)).replace(',', '')
                whr_str = f'WHERE "{getFldName(tmActivityRecords.db.tbl_name, "fldFK_Actividad")}" IN {colstr}'
                df_query = getrecords(self.__tblMontosName, '*', where_str=whr_str)
                if len(df_query.index) > 0:
                    result = df_query  # qryTable tiene 1 solo registro (Ultimo o Primero)
                    if len(df_query.index) > 1:
                        return df_query    # Returns multiple records.
                    else:
                        retValue = result

        return retValue if not full_record else retValue.iloc[0].to_dict()

    # ------------------------------------------- End _set/_get functions ------------------------------------------- #


    # def _getRecordsLinkTables01(self, tblRA=None, tblLink=None, tblData=None, *, activity_list=('*',),
    #                           outer_obj_id=None, use_tbl_data=None, chunk_size=None):
    #     """         *** 30Aug24: DEPRECATED. Replaced by _get_link_records(), using DataFrames. ****
    #     Reads DB records out of a join of RA and Link Tables. If Data Table is provided returns records from DataTable
    #     otherwise returns records from tblRA.
    #     @param tblRA: Registro De Actividades. DataTable object.
    #     @param tblLink: Tabla Link.DataTable object.
    #     @param tblData: Tabla Data opcional (Data Inventario, Data Status, etc). DataTable object.
    #     @param activity_list: Activity Names (table RA) for which records are pulled. Activities are checked against
    #     Activities dictionary defined for the object. * -> All Activities
    #     @return: 1 DataTable Object with: All field Names from table Registro De Actividades OR All field Names from
    #              Data Table (if passed)
    #     """
    #     if use_tbl_data or any(isinstance(j, pd.DataFrame) for j in (tblRA, tblLink, tblData)):
    #         return self._get_link_records(use_tbl_data=True, activity_list=activity_list, uid=outer_obj_id,
    #                                       chunk_size=chunk_size)
    #
    #     activity_list = activity_list if isinstance(activity_list, (list, tuple, set)) else ('*',)
    #     activity_list = set([j.strip() for j in activity_list if isinstance(j, str)])  # Filtra activity_list
    #     tblRA_fldID = tblRA.getDBFldName("fldID")
    #     strWhere1 = ';'
    #     if '*' not in activity_list:        # activityDict = {activity_name: activityID}
    #
    #         activityDict = {k: self.activities.get(k) for k in activity_list if k in self.activities}
    #         if activityDict:
    #             activityString = '('
    #             for i in activityDict:  # ------- Arma string de con todos los Nombres Actividad seleccionados.
    #                 activityString += f'"{activityDict[i]}"'
    #                 activityString += ', ' if i != list(activityDict.keys())[-1] else ')'
    #             strWhere1 = f'"{tblRA.dbTblName}"."{tblRA.getDBFldName("fldFK_NombreActividad")}" IN ' \
    #                         f'{activityString}; '
    #
    #     outer_obj_id = outer_obj_id or self.outerObject.ID
    #     strSelect = 'SELECT '+(f'"{tblData.dbTblName}".*' if tblData.tblName is not None else f'"{tblRA.dbTblName}".*')
    #     strFrom = f' FROM "{tblRA.dbTblName}" INNER JOIN "{tblLink.dbTblName}" USING ("{tblRA_fldID}") '
    #     joinDataTable = f' INNER JOIN "{tblData.dbTblName}" USING ("{tblRA_fldID}") ' if tblData is not None else ''
    #     val = ('"' + outer_obj_id + '"' if isinstance(outer_obj_id, str) else outer_obj_id)  # or \
    #           # (('"' + self.outerObject.ID + '"') if isinstance(self.outerObject.ID, str) else self.outerObject.ID)
    #     if isinstance(outer_obj_id, tuple):
    #         strWhere0 = f'"{tblLink.dbTblName}"."{tblLink.getDBFldName("fldFK")}" IN {str(outer_obj_id)} '
    #     elif outer_obj_id == '*':
    #         strWhere0 = ''
    #     else:
    #         strWhere0 = f'"{tblLink.dbTblName}"."{tblLink.getDBFldName("fldFK")}" == {val} '
    #
    #     strSQL = strSelect + strFrom + joinDataTable + 'WHERE ' + strWhere0 + \
    #              ('AND ' if strWhere0 and strWhere1 != ';' else '') + strWhere1
    #     # print(f'ReadLinkTable-SUPPORT CLASSES({lineNum()}) - strSQL: {strSQL} ', dismiss_print=DISMISS_PRINT)
    #     dbReadTbl = tblData.tblName if isinstance(tblData, DataTable) else tblRA.tblName
    #     return dbRead(dbReadTbl, strSQL)


    def _get_link_records(self, *, use_tbl_data=True, activity_list=('*',), uid=None, db_name=None,
                          chunk_size=None) -> pd.DataFrame:     # Some changes in logic.
        """
        @param use_tbl_data: True -> Return values from Activity Data Table. False -> Returns values from RA table.
        @param activity_list: list of int. List of ActivityID to filter records. Default: All activities.
                CAUTION: Many activities are performed as part of other activities and ActivityID values may be
                superimposed or lost. LEAVE as '*' if not absolutely certain of what you're doing!.
        @param chunk_size: chunksize parameter to be passed to read_sql_query function. User must determine when reading
        in chunks is necessary.
        @param uid: uid list/tuple when function is called by a class instead of an object (outerObject not defined).
                    '*' or 'all': Pulls ALL uids ('fldObjectUID' field) from db table.
        @return: DataFrame with link records. Iterator when chunk_size != None.
        """
        db_name = db_name if db_name in _DataBase.active_databases() else MAIN_DB_NAME
        activity_list = activity_list if isinstance(activity_list, (list, tuple, set)) else ('*',)
        activity_list = set([j.strip() for j in activity_list if isinstance(j, str)])  # Filtra activity_list
        tblRA_fldID = getFldName(self._tblRAName, "fldID", db_name=db_name)
        tblRADBName = getTblName(self._tblRAName, db_name=db_name)
        tblLinkDBName = getTblName(self._tblLinkName, db_name=db_name)
        tblDataDBName = getTblName(self._tblDataName, db_name=db_name)
        strWhere1 = ';'
        if '*' not in activity_list:  # activityDict = {activity_name: activityID}
            activityDict = {k: self.activities.get(k) for k in activity_list if k in self.activities}
            if activityDict:
                activityString = str(tuple(activityDict.values()))
                if len(activityDict) == 1:
                    activityString.replace(',', '')     # Removes trailing ',' for 1 item list.
                strWhere1 = f'"{tblRADBName}"."{getFldName(self._tblRAName, "fldFK_NombreActividad",db_name=db_name)}"'\
                            f' IN {activityString}; '

        # New logic to prioritize where uid comes from. First checks for outerObject, then resorts to uid.
        outer_obj = self.outerObject
        outer_obj_id = outer_obj.ID if not type(outer_obj) is type else uid     # class calls use uid argument.
        strSelect = 'SELECT ' + f'"{tblDataDBName}".*' if use_tbl_data else f'"{tblRADBName}".* '
        strFrom = f' FROM "{tblRADBName}" INNER JOIN "{tblLinkDBName}" USING ("{tblRA_fldID}") '
        joinDataTable = f' INNER JOIN "{tblDataDBName}" USING ("{tblRA_fldID}") ' if use_tbl_data else ''

        if isinstance(outer_obj_id, tuple):
            tuple_str = str(outer_obj_id) if len(outer_obj_id) > 1 else str(outer_obj_id).replace(',', '')
            strWhere0 = f'"{tblLinkDBName}"."{getFldName(self._tblLinkName, "fldFK", db_name=db_name)}" IN {tuple_str} '
        elif isinstance(outer_obj_id, str) and (outer_obj_id.startswith('*') or outer_obj_id.lower().startswith('all')):
            strWhere0 = ''
        else:
            val = f'"{outer_obj_id}"' if isinstance(outer_obj_id, str) else outer_obj_id
            strWhere0 = f'"{tblLinkDBName}"."{getFldName(self._tblLinkName, "fldFK", db_name=db_name)}" == {val} '
        strSQL = strSelect + strFrom + joinDataTable + 'WHERE ' + \
                 strWhere0 + ('AND ' if strWhere0 and strWhere1 != ';' else '') + strWhere1

        return pd.read_sql_query(strSQL, SQLiteQuery().conn, chunksize=chunk_size)

    # _getRecordsLinkTables = _get_link_records


    def _createActivityRA00(self, *args: DataTable, tbl_data_name='', uid=None, **kwargs):
        """     30Aug24: DEPRECATED. Replaced by _createActivityRA below, using dataframes.
                21Jul24: Adds support for pandas DataFrames.
                22Jun24: Adds writing multiple tblLink, tblData records associated with a sigle RA Activity.
                TODO IMPORTANT: UPDATED tblData.fldID (as a return value in tblData) works only for 1 record.
        General Function to set tables [Registro De Actividades], [Link Actividades], [Data Table] for 1 ACTIVITY ONLY,
        to values specified by arguments. Used to reduce duplication of code with multiple methods.
        If fldID is not provided in argTblRA, new records are created in RA and Table Link.
        argTblRA, argTblLink, argTblData MUST BE VALID AND CONSISTENT, OTHERWISE DATA CORRUPTION IN DB WILL OCCUR.
        @param tbl_data_name: tblData Name when tblData cannot be univocally assigned.
        @param args: tblRA, tblLink, tblData: list of DataTable objects, with all the tables and fields to write to DB.
        @param kwargs: Additional obj_data tablas to write (Inventory, etc): WRITTEN AS PASSED. Must come here complete!
               recordInventory: overrides the default doInventory setting for the Activity
        @return: Success: ID_Actividad RA (int), Error: errorCode (str)
        """
        # Referencia a DataTables pasadas en args para poder pasar resultados al caller (en particular fldID de tblLink)
        if any(isinstance(j, pd.DataFrame) for j in args):
            return self._createActivityRA(*args, **kwargs)  # executes DataFrame function if any argument is DataFrame.

        args = [j for j in args if isinstance(j, DataTable)]
        tblRA = next((j for j in args if j.tblName == self._tblRAName), DataTable(self._tblRAName))
        tblLink = next((j for j in args if j.tblName == self._tblLinkName), DataTable(self._tblLinkName))
        if tbl_data_name:
            self.__tblDataName = tbl_data_name
        tblData = next((j for j in args if j.tblName == self._tblDataName), DataTable(self._tblDataName))
        timeStamp = tblRA.getVal(0, 'fldDate') or time_mt('datetime')   # TODO(cmt): timeStamp (para RA) debe ser SIEMPRE tiempo monotonico
        eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), timeStamp=timeStamp, **kwargs)
        activityID = tblRA.getVal(0, 'fldFK_NombreActividad') or self._activityID
        activityName = next((k for k, v in self.activities.items() if v == activityID), None) or self._activityName
        userID = tblRA.getVal(0, 'fldFK_UserID') or sessionActiveUser
        outer_object = self.outerObject
        fldFK_vals = tblLink.getCol('fldFK') or [getattr(outer_object, "ID", uid), ]
        idActividadRA = tblRA.getVal(0, 'fldID')
        commentRA = tblRA.getVal(0, 'fldComment', '') or ''
        commentRA += (' ' if commentRA else '') + f'ObjectID:{str(tuple(fldFK_vals))} / {activityName}'
        tblRA.setVal(0, fldTimeStamp=timeStamp, fldFK_UserID=userID, fldFK_NombreActividad=activityID,
                     fldComment=commentRA, fldTerminal_ID=f"{TERMINAL_ID}")     # Sets Terminal_ID to optimize Triggers.
        if tblRA.tblName.lower() == 'tblanimalesregistrodeactividades'.lower():
            # Inicializa Clase de Animal en tblRA Animales.
            tblRA.setVal(0, fldFK_ClaseDeAnimal=outer_object.animalClassID())


        if not idActividadRA:
            idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))  # TODO(cmt): AQUI GENERA DB LOCKED ERROR DESDE FRONTEND
            if isinstance(idActividadRA, str):
                retValue = f'ERR_DB_WriteError: {idActividadRA} - {moduleName()}({lineNum()}) - {callerFunction()})'
                krnl_logger.warning(retValue)
                return retValue  # Sale c/error
            # tblRA.undoOnError = True
            tblLink.setVal(0, fldFK_Actividad=idActividadRA)   # Creates 1 record in tblLink to support the logic below.
        else:
            # Se paso idActividad RA en tblRA: Busca fldID de tblLink (que debe existir por la correspondencia
            #  1->N entre tblRA y tblLink) y setea al valor correspondiente p/ generar UPDATE de tblLink
            # TODO(cmt). Importante: 1->N implica que habran en general N records de tblLink para 1 record de tblRA.
            # temp = getRecords(tblLink.tblName, '', '', None, 'fldID', fldFK=fldFK_val, fldFK_Actividad=idActividadRA)
            sql = f'SELECT "{tblLink.getDBFldName("fldID")}", "{tblLink.getDBFldName("fldFK")}"' \
                  f' FROM "{tblLink.dbTblName}" WHERE ' \
                  f'"{tblLink.getDBFldName("fldFK_Actividad")}" == "{idActividadRA}"; '   #  f'"{tblLink.getDBFldName("fldFK")}" IN {str(fldFK_vals)} AND ' \
            temp = dbRead(tblLink.tblName, sql)
            if not isinstance(temp, str):
                # Updates fldID values in tblLink, to establish fldFK correspondence with fldID
                linkRecordIDs = temp.getCol('fldID')
                fldFK_vals = temp.getCol('fldFK')
                if linkRecordIDs:
                    for idx, j in enumerate(linkRecordIDs):
                        tblLink.setVal(idx, fldID=j)

        if tblLink.dataLen == 0:            # empty tblLink passed. Must initialize 1st record.
            tblLink.setVal(0, fldFK_Actividad=idActividadRA)
        for j in range(tblLink.dataLen):
            commentLink = tblLink.getVal(j, 'fldComment') or ''
            commentLink += (' ' if commentLink else '') + f'Activity: {activityID} / {eventDate}'
            # if tblLink doesn't have a fldTerminal_ID field, ignores and writes the rest of fields.
            tblLink.setVal(j, fldFK_Actividad=idActividadRA, fldFK=fldFK_vals[j], fldTerminal_ID=TERMINAL_ID,
                           fldComment=commentLink)
        if tblLink.dataLen == 1:
            idLinkRecord = setRecord(tblLink.tblName, **tblLink.unpackItem(0))      # Throws exception on failure.
        else:
            idLinkRecord = tblLink.setRecords()
        if isinstance(idLinkRecord, str):
            if tblRA.getVal(0, 'fldID') is None:
                retValue = f'ERR_DB_WriteError: {idLinkRecord}. Table Name: {tblLink.tblName} - {callerFunction()})'
                _ = delRecord(tblRA.tblName, idActividadRA)
                krnl_logger.error(retValue)
                print(f'Deleting record {idActividadRA}. Table: {tblRA.tblName} - Function retValue: {retValue}',
                      dismiss_print=DISMISS_PRINT)
                return retValue  # Sale c/error

        # Must pull tblLInk fldIDs just created with setRecords
        # TODO(cmt): CRITICAL setting of tblLink.fldID to avoid "UNIQUE Constraint Failed" (when tblLink is used after
        #  for further db writes after this function returns).
        if tblLink.dataLen > 1:
            sql1 = f'SELECT * FROM "{tblLink.dbTblName} WHERE ' \
                   f'"{tblLink.getDBFldName("fldFK_Actividad")}" == {idActividadRA} ' \
                   f'"AND {tblLink.getDBFldName("fldFK")}" IN {str(fldFK_vals)}; '
            tbl1 = dbRead(tblLink.tblName, sql1)
            if isinstance(tbl1, DataTable):
                tblLink = tbl1
                # for j in range(tbl1.dataLen):
                #     tblLink.setVal(j, fldID=tbl1.getVal(j, 'fldID'))
        else:
            tblLink.setVal(0, fldID=idLinkRecord)

        # 3. Setea tblData. Aqui se indica a setRecord() actualizar data (lastInventory, lastCategory, etc) en memoria
        # TODO(cmt): tblData tambien puede tener multiples records. Se debe codificar para este caso.
        # -> NO SETEAR fldID=idActividadRA aqui: Se usa abajo para ejecutar undo del record de tblRA si hay write error
        if tblData.dataLen == 0:            # empty tblData passed. Must initialize 1st record.
            tblData.setVal(0, fldDate=eventDate)
        for j in range(tblData.dataLen):
            commentData = tblData.getVal(j, 'fldComment') or ''
            commentData += (' / ' if commentData else '') + f'{callerFunction(namesOnly=True, getCallers=True)}, ' \
                                                            f'ObjectID: {fldFK_vals[j]}'
            tblData.setVal(j, fldFK_Actividad=idActividadRA, fldDate=eventDate, fldComment=commentData,
                           fldFK_UserID=userID)
        if tblData.dataLen == 1:
            idDataRecord = setRecord(tblData.tblName, **tblData.unpackItem(0))  # escribe en DB.
        else:
            idDataRecord = tblData.setRecords()  # escribe en DB. Multiple Records.

        if isinstance(idDataRecord, str):
            retValue = f'ERR_DB_WriteError: {idDataRecord} - {moduleName()}({lineNum()}) - {callerFunction()})'
            krnl_logger.error(retValue)
            if tblRA.getVal(0, 'fldID') is None:
                _ = delRecord(tblRA.tblName, idActividadRA)
                for j in tblLink.dataLen:
                    _ = delRecord(tblLink.tblName, tblLink.getVal(j, 'fldID'))
                krnl_logger.info(f'Deleting records {tblRA.tblName}:{idActividadRA}; {tblLink}{idLinkRecord} - '
                                 f'Function retValue: {retValue}')
        else:
            # print(f'((((((((((( createActivityRA() -------------- Just wrote to {tblData.tblName}: {dicto}',
            #       dismiss_print=DISMISS_PRINT)
            if tblData.dataLen == 1:
                tblData.setVal(0, fldID=idDataRecord)  # TODO IMPORTANT: tblData fldID setting works only for 1 record.
            tblRA.setVal(0, fldID=idActividadRA)
            retValue = idActividadRA
        return retValue


    def _createActivityRA(self, *args: pd.DataFrame, tbl_data_name='', uid=None, **kwargs):
        """
        21Jul24: Adds support for pandas DataFrames.
        22Jun24: Adds writing multiple tblLink, tblData records associated with a sigle RA Activity.
                TODO IMPORTANT: UPDATED tblData.fldID (as a return value in tblData) works only for 1 record.
        General Function to set tables [Registro De Actividades], [Link Actividades], [Data Table] for 1 ACTIVITY ONLY,
        to values specified by arguments. Used to reduce duplication of code with multiple methods.
        If fldID is not provided in argTblRA, new records are created in RA and Table Link.
        argTblRA, argTblLink, argTblData MUST BE VALID AND CONSISTENT, OTHERWISE DATA CORRUPTION IN DB WILL OCCUR.
        @param tbl_data_name: tblData Name when tblData cannot be univocally assigned.
        @param args: tblRA, tblLink, tblData: list of DataTable objects, with all the tables and fields to write to DB.
        @param kwargs: Additional obj_data tablas to write (Inventory, etc): WRITTEN AS PASSED. Must come here complete!
               recordInventory: overrides the default doInventory setting for the Activity
        @return: Success: ID_Actividad RA (int), Error: errorCode (str)
        """
        args = [j for j in args if isinstance(j, pd.DataFrame)]
        dfRA = next((j for j in args if j.db.tbl_name == self._tblRAName), pd.DataFrame.db.create(self._tblRAName))
        dfLink = next((j for j in args if j.db.tbl_name == self._tblLinkName),pd.DataFrame.db.create(self._tblLinkName))
        if tbl_data_name:
            self.__tblDataName = tbl_data_name
        dfData = next((j for j in args if j.db.tbl_name == self._tblDataName),pd.DataFrame.db.create(self._tblDataName))
        timeStamp = dfRA.loc[0, 'fldTimeStamp'] if not dfRA.empty and 'fldTimeStamp' in dfRA.columns \
                                                   and pd.notnull(dfRA.loc[0, 'fldTimeStamp']) else time_mt('datetime')
        eventDate = getEventDate(tblDate=timeStamp, timeStamp=timeStamp, **kwargs)
        activityID = dfRA.loc[0, 'fldFK_NombreActividad'] if not dfRA.empty \
                                            and pd.notnull(dfRA.loc[0, 'fldFK_NombreActividad']) else self._activityID
        activityName = next((k for k, v in self.activities.items() if v == activityID), None) or self._activityName
        # userID = dfRA.loc[0, 'fldFK_UserID'] if not dfRA.empty and pd.notnull(dfRA.loc[0, 'fldFK_UserID']) \
        #                                      else sessionActiveUser
        userID = sessionActiveUser
        outer_obj = self.outerObject
        object_id = outer_obj.ID if type(outer_obj) is not type else uid
        fldFK_vals = dfLink['fldFK'].to_list() if not dfLink.empty else (object_id, )
        fldFK_vals_quoted = [f'"{j}"' for j in fldFK_vals if pd.notnull(j)]      # required for sql strings below.
        idActividadRA = dfRA.loc[0, 'fldID'] if not dfRA.empty and 'fldID' in dfRA.columns and \
                                                pd.notnull(dfRA.loc[0, 'fldID']) else None
        commentRA = dfRA.loc[0, 'fldComment'] if not dfRA.empty and 'fldComment' in dfRA.columns and \
                                                 isinstance(dfRA.loc[0, 'fldComment'], str) else ''
        commentRA += (' ' if commentRA else '') + f'ObjectID:{fldFK_vals_quoted} / {activityName}'
        dfRA.loc[0, ('fldTimeStamp', 'fldFK_UserID', 'fldFK_NombreActividad', 'fldComment', 'fldTerminal_ID')] = \
            [timeStamp, userID, activityID, commentRA, TERMINAL_ID]   # Sets Terminal_ID to optimize Triggers.
        if dfRA.db.tbl_name.lower() == 'tblanimalesregistrodeactividades':
            # Inicializa Clase de Animal en tblRA Animales.
            dfRA.loc[0, 'fldFK_ClaseDeAnimal'] = outer_obj.animalClassID()  # This call works for class and instances.

        if idActividadRA is None:  # TODO(cmt):Linea de abajo genera 'db locked error' cuando db writer no esta andando.
            idActividadRA = setRecord(dfRA.db.tbl_name, **dfRA.loc[0].to_dict())
            if isinstance(idActividadRA, str):
                retValue = f'ERR_DBAccess: {idActividadRA} - {moduleName()}({lineNum()}) - {callerFunction()})'
                krnl_logger.warning(retValue)
                return retValue  # Sale c/error
            dfRA.loc[0, 'fldID'] = idActividadRA     # TODO(cmt): this value used by @Activity.activity_tasks decorator.
            dfLink.loc[0, 'fldFK_Actividad'] = idActividadRA  # Creates 1 record in dfLink to support assignments below.
        else:
            # Se paso idActividad RA en fdRA: Busca el/LOS fldID de fdLink (que deben existir por la correspondencia
            #  1->N entre tblRA y tblLink) y setea a los valores correspondientes p/ generar UPDATE de fdLink
            # TODO(cmt). Importante: 1->N implica que habran en general N records de tblLink para 1 record de tblRA.
            sql = f'SELECT * FROM "{dfLink.db.db_tbl_name}" WHERE ' \
                  f'"{dfLink.db.dbcolname("fldFK_Actividad")}" == "{idActividadRA}"; '
            dftemp = pd.read_sql_query(sql, SQLiteQuery().conn)
            if not dftemp.empty:
                # Updates fldID values in tblLink to establish fldFK correspondence with fldID.
                dfLink = pd.DataFrame.db.create(dftemp.db.tbl_name, data=dftemp.to_dict())
                fldFK_vals = dfLink['fldFK'].to_list()
                fldFK_vals_quoted = [f'"{j}"' for j in fldFK_vals if not pd.isnull(j)]

        for j in dfLink.index:
            commentLink = dfLink.loc[j, 'fldComment'] if not dfLink.empty and 'fldComment' in dfLink.columns and \
                                                         isinstance(dfLink.loc[j, 'fldComment'], str) else ''
            commentLink += (' ' if commentLink else '') + f'Activity: {activityID} / {eventDate} / ' \
                                                          f'ObjectID: {fldFK_vals[j]}'
            dfLink.loc[j, ('fldFK_Actividad', 'fldFK', 'fldComment')] = (idActividadRA, fldFK_vals[j], commentLink)
            if 'fldTerminal_ID' in dfLink.db.field_names:
                dfLink.loc[j, 'fldTerminal_ID'] = TERMINAL_ID       # Field present only in Link Animales Actividades.
        # Here writes to db and gets actual fldID values for each record.
        idLinkRecord = setrecords(dfLink)       # idLinkRecord is a tuple of sqlite3.Cursor objects.
        if any(j.rowcount <= 0 for j in idLinkRecord):
            if pd.isnull(dfRA.loc[0, 'fldID']):
                retValue = f'ERR_DB_Access: tblname: {dfLink.db.tbl_name} - {callerFunction()})'
                _ = delRecord(dfRA.db.tbl_name, idActividadRA)
                krnl_logger.error(retValue + f'\nDeleting record {idActividadRA}. tbl: {dfRA.db.tbl_name}')
                return retValue

        # Must pull tblLInk fldIDs just created with setrecords()
        # TODO(cmt): CRITICAL setting of tblLink.fldID to avoid "UNIQUE Constraint Failed" (when tblLink is used
        #  after for further db writes after this function returns).
        sql1 = f'SELECT * FROM "{dfLink.db.db_tbl_name}" WHERE ' \
               f'"{dfLink.db.dbcolname("fldFK_Actividad")}" == {idActividadRA} ' \
               f'AND "{dfLink.db.dbcolname("fldFK")}" IN ({", ".join(fldFK_vals_quoted)}); '
        df1 = pd.read_sql_query(sql1, SQLiteQuery().conn)
        if not df1.empty:                           # Updates dfLink with fldID values set by setrecords() above.
            dfLink['fldID'] = df1['fldID']  # This is how to assign values so that args gets updated.

        # 3. Setea tblData. Aqui se indica a setRecord() actualizar data (lastInventory, lastCategory, etc) en memoria
        # TODO(cmt): tblData tambien puede tener multiples records. Se debe codificar para este caso.
        # -> NO SETEAR fldID=idActividadRA aqui: Se usa abajo para ejecutar undo del record de tblRA si hay write error
        if dfData.empty or not isinstance(dfData.loc[0, 'fldDate'], (datetime, pd.Timestamp)):
            dfData.loc[0, 'fldDate'] = eventDate

        for j in dfData.index:
            commentData = dfData.loc[j, 'fldComment'] if not dfLink.empty and 'fldComment' in dfData.columns and\
                                                         isinstance(dfData.loc[j, 'fldComment'], str)else None or ''
            commentData += (' / ' if commentData else '') + f'{callerFunction(namesOnly=True, getCallers=True)}'
            dfData.loc[j, ('fldFK_Actividad', 'fldDate', 'fldComment')] = \
                (idActividadRA, eventDate, commentData)
        idDataRecord = setrecords(dfData)  # escribe en DB. Multiple Records.
        if any(j.rowcount <= 0 for j in idDataRecord):
            retValue = f'ERR_DBAccess: {moduleName()}({lineNum()}) - {callerFunction()})'
            krnl_logger.error(retValue)
            if pd.isnull(dfRA.loc[0, 'fldID']):
                _ = delRecord(dfRA.db.tbl_name, idActividadRA)
                for j in dfLink.index.to_list():
                    _ = delRecord(dfLink.db.tbl_name, dfLink.loc[j, 'fldID'])
                krnl_logger.info(f'Deleting records {dfData.db.tbl_name}:{idActividadRA}; - '
                                 f'Error: {retValue}')
            return retValue

        sql2 = f'SELECT * FROM "{dfData.db.db_tbl_name}" WHERE "{dfData.db.dbcolname("fldFK_Actividad")}" == ' \
               f'{idActividadRA}; '
        df2 = pd.read_sql_query(sql2, SQLiteQuery().conn)
        if not df2.empty:
            dfData['fldID'] = df2['fldID']   # This is the way to assign values so that args gets updated (Buenos Aires, Jul-24)

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
            #  "shortname" property to fetch a field name particle, pulls the compare value (_comp_val) and runs
            #  outer_obj.getattr(outer_obj, d1Comp[k]).comp(_comp_val).
            d2Flat = list(nested_dict_iterator_gen(d2))
            matchResults = {}
            for k in d1Comp:
                fmt_k = k.lower().strip()       # formatted k.
                # Here k can be in the form of "fldName", "tblName.fldName" or special name ('category, 'age', etc).
                # if form  "tblName.fldName" is detected, must prioritize comparison using getFldCompare().
                # Priority of execution:
                # If dkey from dic == k OR dkey is contained in k, _comp_val is pulled from that dkey.
                #    k may or may not contain "fld" particle, so that "age", "localization", etc. compare OK.
                # 1) Gets comparison value (_comp_val) first via direct string comparison. If that fails uses shortName.
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
                # 2) With _comp_val in hand:
                # 2.1) if k has the "fld" particle in it:
                if 'fld' in fmt_k:  # si k contiene fld -> NO es atributo y NO tiene comp() definido: usa compare()
                    # 2.1.1) If "." in k, assumes proper names for k and dkey and checks using getFldCompare()
                    if "." in k:    # if "." in fldName => proper field name: uses Compare_Index
                        # Here k and dkey must be both VALID    "tblName.fldName" strings, and they CAN BE DIFFERENT.
                        comp_val = next((dic[dkey] for dic in d2Flat for dkey in dic if getFldCompare(k, dkey)), VOID)
                    # 2.1.2) If "." NOT in k, uses _comp_val obtained in the beginning and uses compare().
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
                # print(f'WWWWWWWWWWWWWrapper -> Queuer: {func.__name__} / buffer stopped: {buffer.is_stopped()}')
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


    # @timerWrapper(iterations=10)
    @__paDifferedProcessing(__progActivityBuffer)
    def _paMatchAndClose(self, idClosingActivity=None, *args, outer_obj=None, execute_fields=None, excluded_fields=None,
                         closing_status=None, force_close=False):
        """ self is an executed Activity object.
        Closes a programmed activity for given idObject. Sets closure status in accordance with closing status of the
        activity. The calling activity (self) is Closing Activity. The Activity to be closed must be found in RAP.
        @param args: DataTable arguments (tblLinkPA, tblPADataStatus).
        @param execute_fields: {fldName: fldValue, } dict with execution fields to compare against progActivities fields
            execute_fields must contain a valid closing date for the Activity.
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

        # 2. Para cada record, comparar parametros de la actividad self con los parametros de cada registro leido de DB.
        # Cuando hay match, retornar el valor de ID_Actividad Programada
        # Lista de progActivities de tblRAP presentes en myProgActivities con _activityID igual a ID actividad ejecutada
        tblRAP = getRecords(self._tblRAPName, '', '', None, '*', fldID=myPAIDList)
        dataProgRecordsCol = tblRAP.getCol('fldFK_DataProgramacion')
        if not dataProgRecordsCol:
            # print(f'******** Prog. Activity {self._activityName} for {outer_obj.__class__.__name__} #'
            #       f'{outer_obj.ID} not found. Nothing closed. Exiting.', dismiss_print=DISMISS_PRINT)
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
    @__paDifferedProcessing(__progActivityBuffer)  # The execution of this code goes to an async, no-wait queue.
    def _paCreateExecInstance(self, *, outer_obj=None):
        """ Creates EXECUTION INSTANCES out of an already existing ProgActivity, based on passed conditions: when
        monitored conditions change for outerObject, this function checks if the new conditions call for the assignment
        of one or more ProgActivities (existing in RAP) on the object.
        Travels RAP when conditions for an object change and updates ProgActivities for the object.
        Due to the loop execution, many ProgActivities (triggered by the conditions passed) may be set on outer_obj.
        Only ADDS required ProgActivities. No removals here (removals are performed in the background by cleanup funcs).
        Meant to be used only from the foreground for now!!.
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
                                         fldFK_ClaseDeAnimal=outer_obj.animalClassID())
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
