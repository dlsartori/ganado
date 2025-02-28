import sqlite3
import re
from threading import Lock
import pandas as pd
import numpy as np
from krnl_config import sessionActiveUser, callerFunction, time_mt, lineNum, removeAccents, os, krnl_logger
from krnl_custom_types import getRecords, setRecord, dbRead, DataTable, getFldName
from krnl_db_query import getTblName, SQLiteQuery
from datetime import datetime
from uuid import uuid4, UUID
from krnl_exceptions import DBAccessError
from krnl_transactionalObject import TransactionalObject

def moduleName():
    return str(os.path.basename(__file__))

def adapt_geo_to_UID(obj):
    """ 'Serializes' a Geo object to its ID value (integer) for storage as int in the DB (in a DB field of type GEO)."""
    if isinstance(obj, Geo):
        try:
            return obj.ID       # obj.ID es UUID (type str).
        except (AttributeError, NameError, ValueError):
            raise sqlite3.Error(f'Geo - Conversion error: {obj} is not a valid Geo object.')
    return obj

def convert_to_geo(val):
    """ Converts UUID from a DB column of name GEOTEXT to a Geo Object, returning the obj found in __registerDict[val]
     GEOTEXT column has TEXT affinity, but it doesn't return a string. Must decode() value received. """
    try:
        ret = Geo.getGeoEntities().get(val.decode('utf-8'), None)   # OJO:sqlite3 pasa val como byte. Must decode()!
    except (TypeError, ValueError, AttributeError):
        # raise sqlite3.Error(f'ERR_Conversion error: {val} Geo Entity not registered or {val} is of invalid type.')
        krnl_logger.warning(f'ERR_Conversion error: {val} Geo Entity not registered or {val} is of invalid type.')
        return val
    else:
        return ret if ret is not None else val    # if key is not found, returns val for processing by the application.


class Geo(TransactionalObject):
    # __objClass = 102
    # __objType = 2
    __lock = Lock()
    __tblEntitiesName = 'tblGeoEntidades'
    __tblContainingEntitiesName = 'tblGeoEntidadContainer'
    __tblEntityTypesName = 'tblGeoTiposDeEntidad'
    __tblLocalizationLevelsName = 'tblGeoNivelesDeLocalizacion'
    __tblObjectsName = __tblEntitiesName
    __tblObjectsDBName = getTblName(__tblObjectsName)
    __registerDict = {}         # {fldObjectUID: geoObj, }

    def __call__(self, caller_object=None, *args, **kwargs):        # Not sure this is required here.
        """
        @param caller_object: instance of Bovine, Caprine, etc., that invokes the Activity
        @param args:
        @param kwargs:
        @return: Activity object invoking __call__()
        """
        # item_obj=None above is important to allow to call fget() like that, without having to pass parameters.
        # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {(item_object, *args)}; kwargs: {kwargs}')
        self.outerObject = caller_object  # item_object es instance de Animal, Tag, etc. NO PUEDE SER class por ahora.
        return self


    # Variables para logica de manejo de objetos repetidos/duplicados.
    _fldID_list = []  # List of all active records pulled by getRecords() from tblAnimales.
    # _new_local_fldIDs = []  # UID list for records added tblAnimales by local application.
    _fldUPDATE_counter = 0  # Monotonic counter to detect and manage record UPDATEs.

    _active_uids_dict = {}  # {fldObjectUID: fld_Duplication_Index}
    _duplic_index_checksum = 0     # Sum of _Duplication_Index values to detect changes in _active_uids_df
    _active_duplication_index_dict = {}  # {fld_Duplication_Index: set(fldObjectUID, dupl_uid1, dupl_uid2, ), }

    @classmethod
    def getGeoEntities(cls):
        return cls.__registerDict       # {fldObjectUID: geoObj, }

    @classmethod
    def getObjectTblName(cls):
        return cls.__tblObjectsName

    temp1 = getRecords(__tblLocalizationLevelsName, '', '', None, 'fldID', 'fldName', 'fldNivelDeLocalizacion',
                         'fldGeoLocalizationActive', 'fldFlag_EntidadEstado', 'fldNivelContainerMandatorio')
    __localizationLevelsDict = {}
    for j in range(temp1.dataLen):
        # {localizLevelName: [localizLevel, localizLevelActive, Entidad Estado YN, Container Mandatorio, tipoEntidad], }
        __localizationLevelsDict[removeAccents(temp1.dataList[j][1])] = [temp1.dataList[j][2], temp1.dataList[j][3],
                                                                         temp1.dataList[j][4], temp1.dataList[j][5],
                                                                         temp1.dataList[j][0]]
    @classmethod
    def tblObjName(cls):
        return cls.__tblObjectsName

    @classmethod
    def tblObjDBName(cls):
        return cls.__tblObjectsDBName


    @classmethod
    def getLocalizLevelsDict(cls):
        return cls.__localizationLevelsDict


    # reserved name, so that it's not inherited by lower classes, resulting in multiple executions of the trigger.
    # @staticmethod
    # def _generate_trigger_duplication(tblName):
    #     temp = DataTable(tblName)
    #     tblObjDBName = temp.dbTblName
    #     Dupl_Index_val = f'(SELECT DISTINCT _Duplication_Index FROM "{tblObjDBName}" WHERE Identificadores_str ' \
    #                      f'== NEW. Identificadores_str AND "FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM "{tblObjDBName}" ' \
    #                      f'WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))) AND ' \
    #                      f'("Salida YN" == 0 OR "Salida YN" IS NULL)), '
    #
    #     flds_keys = f'_Duplication_Index'
    #     flds_values = f'{Dupl_Index_val}'
    #     if isinstance(temp, DataTable) and temp.fldNames:
    #         flds = temp.fldNames
    #         excluded_fields = ['fldID', 'fldObjectUID', 'fldTimeStamp', 'fldTerminal_ID', 'fld_Duplication_Index']
    #
    #         # TODO: Must excluded all GENERATED COLUMNS to avoid attempts to update them, which will fail. fldID must
    #         #  always be removed as its value is previously defined by the INSERT operation that fired the Trigger.
    #         tbl_info = exec_sql(sql=f'PRAGMA TABLE_XINFO("{tblObjDBName}");')
    #         if len(tbl_info) > 1:
    #             idx_colname = tbl_info[0].index('name')
    #             idx_hidden = tbl_info[0].index('hidden')
    #             tbl_data = tbl_info[1]
    #             restricted_cols = [tbl_data[j][idx_colname] for j in range(len(tbl_data)) if
    #                                tbl_data[j][idx_hidden] > 0]
    #             if restricted_cols:  # restricted_cols: Hidden and Generated cols do not support UPDATE operations.
    #                 restricted_fldnames = [k for k, v in temp.fldMap().items() if v in restricted_cols]
    #                 excluded_fields.extend(restricted_fldnames)
    #             excluded_fields = tuple(excluded_fields)
    #
    #         for f in excluded_fields:
    #             if f in flds:
    #                 flds.remove(f)
    #         fldDBNames = [v for k, v in temp.fldMap().items() if
    #                       k in flds]  # [getFldName(cls.tblObjName(), f) for f in flds]
    #         flds_keys += f', {str(fldDBNames)[1:-1]}'  # [1:-1] removes starting and final "[]" from string.
    #
    #         for f in fldDBNames:
    #             flds_values += f'NEW."{f}"' + (', ' if f != fldDBNames[-1] else '')
    #     db_trigger_duplication_str = f'CREATE TRIGGER IF NOT EXISTS "Trigger_{tblObjDBName}_INSERT" AFTER INSERT ON "{tblObjDBName}" ' \
    #                                  f'FOR EACH ROW BEGIN ' \
    #                                  f'UPDATE "{tblObjDBName}" SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1), _Duplication_Index = NEW.UID_Objeto ' \
    #                                  f'WHERE "{tblObjDBName}".ROWID == NEW.ROWID AND _Duplication_Index IS NULL; ' \
    #                                  f'UPDATE "{tblObjDBName}" SET ({flds_keys}) = ({flds_values}) ' \
    #                                  f'WHERE "{tblObjDBName}".ROWID IN (SELECT "{temp.getDBFldName("fldID")}" FROM (SELECT DISTINCT "{temp.getDBFldName("fldID")}", "FechaHora Registro" FROM "{tblObjDBName}" ' \
    #                                  f'WHERE Identificadores_str == NEW.Identificadores_str ' \
    #                                  f'AND ("Salida YN" == 0 OR "Salida YN" IS NULL) ' \
    #                                  f'AND "FechaHora Registro" >= (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM "{tblObjDBName}" ' \
    #                                  f'WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))))); ' \
    #                                  f'UPDATE _sys_Trigger_Tables SET TimeStamp = NEW."FechaHora Registro" ' \
    #                                  f'WHERE DB_Table_Name == "{tblObjDBName}" AND _sys_Trigger_Tables.ROWID == _sys_Trigger_Tables.Flag_ROWID AND NEW.Terminal_ID IS NOT NULL; ' \
    #                                  f'END; '
    #
    #     print(f'Mi triggercito "{temp.dbTblName}" es:\n {db_trigger_duplication_str}')
    #     return db_trigger_duplication_str


    # @classmethod
    # def _generate_trigger_duplication00(cls):
    #     Dupl_Index_val = f'(SELECT DISTINCT _Duplication_Index FROM "{cls.tblObjDBName()}" WHERE Identificadores_str ' \
    #                      f'== NEW. Identificadores_str AND "FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM "{cls.tblObjDBName()}" ' \
    #                      f'WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))) AND ' \
    #                      f'("Salida YN" == 0 OR "Salida YN" IS NULL)), '
    #
    #     flds_keys = f'_Duplication_Index'
    #     flds_values = f'{Dupl_Index_val}'
    #     temp = DataTable(cls.tblObjName())
    #     if temp.fldNames:
    #         flds = temp.fldNames
    #         excluded_fields = ['fldID', 'fldObjectUID', 'fldTimeStamp', 'fldTerminal_ID', 'fld_Duplication_Index']
    #
    #         # TODO: Must excluded all GENERATED COLUMNS to avoid attempts to update them, which will fail. fldID must
    #         #  always be removed as its value is previously defined by the INSERT operation that fired the Trigger.
    #         tbl_info = exec_sql(sql=f'PRAGMA TABLE_XINFO("{cls.tblObjDBName()}");')
    #         if len(tbl_info) > 1:
    #             idx_colname = tbl_info[0].index('name')
    #             idx_hidden = tbl_info[0].index('hidden')
    #             tbl_data = tbl_info[1]
    #             restricted_cols = [tbl_data[j][idx_colname] for j in range(len(tbl_data)) if
    #                                tbl_data[j][idx_hidden] > 0]
    #             if restricted_cols:  # restricted_cols: Hidden and Generated cols do not support UPDATE operations.
    #                 restricted_fldnames = [k for k, v in temp.fldMap().items() if v in restricted_cols]
    #                 excluded_fields.extend(restricted_fldnames)
    #             excluded_fields = tuple(excluded_fields)
    #
    #         for f in excluded_fields:
    #             if f in flds:
    #                 flds.remove(f)
    #         fldDBNames = [v for k, v in temp.fldMap().items() if
    #                       k in flds]  # [getFldName(cls.tblObjName(), f) for f in flds]
    #         flds_keys += f', {str(fldDBNames)[1:-1]}'  # [1:-1] removes starting and final "[]" from string.
    #
    #         for f in fldDBNames:
    #             flds_values += f'NEW."{f}"' + (', ' if f != fldDBNames[-1] else '')
    #     db_trigger_duplication_str = f'CREATE TRIGGER IF NOT EXISTS "Trigger_{cls.tblObjDBName()}_INSERT" AFTER INSERT ON "{cls.tblObjDBName()}" ' \
    #                                  f'FOR EACH ROW BEGIN ' \
    #                                  f'UPDATE "{cls.tblObjDBName()}" SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1), _Duplication_Index = NEW.UID_Objeto ' \
    #                                  f'WHERE "{cls.tblObjDBName()}".ROWID == NEW.ROWID AND _Duplication_Index IS NULL; ' \
    #                                  f'UPDATE "{cls.tblObjDBName()}" SET ({flds_keys}) = ({flds_values}) ' \
    #                                  f'WHERE "{cls.tblObjDBName()}".ROWID IN (SELECT "{temp.getDBFldName("fldID")}" FROM (SELECT DISTINCT "{temp.getDBFldName("fldID")}", "FechaHora Registro" FROM "{cls.tblObjDBName()}" ' \
    #                                  f'WHERE Identificadores_str == NEW.Identificadores_str ' \
    #                                  f'AND ("Salida YN" == 0 OR "Salida YN" IS NULL) ' \
    #                                  f'AND "FechaHora Registro" >= (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM "{cls.tblObjDBName()}" ' \
    #                                  f'WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))))); ' \
    #                                  f'UPDATE _sys_Trigger_Tables SET TimeStamp = NEW."FechaHora Registro" ' \
    #                                  f'WHERE DB_Table_Name == "{cls.tblObjDBName()}" AND _sys_Trigger_Tables.ROWID == _sys_Trigger_Tables.Flag_ROWID AND NEW.Terminal_ID IS NOT NULL; ' \
    #                                  f'END; '
    #
    #     print(f'Mi triggercito "{cls.tblObjDBName()}" es:\n {db_trigger_duplication_str}')
    #     return db_trigger_duplication_str

    @classmethod
    def _init_uid_dicts(cls):
        """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
        Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
        This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
        _processDuplicates() run by background functions.
        Uses a checksum of _active_uids_df.values() to determine if there are changes to the dict.
        @return: None
        """
        sql = f'SELECT * from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" IS NULL OR ' \
              f'"{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ; '
        df = pd.read_sql_query(sql, SQLiteQuery().conn)
        if df.empty:
            val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts no initialized. " \
                  f"System cannot operate."
            krnl_logger.warning(val)
            raise sqlite3.DatabaseError(val)

        col_uid = df['fldObjectUID'].tolist()
        col_duplication_index = df['fld_Duplication_Index'].replace(
            {np.nan: None, float('nan'): None, pd.NA: None}).tolist()
        dicto1 = dict(zip(col_uid, col_duplication_index))  # Reference dic

        # Check for differences in dict values. Must update _active_uids_df,  _active_duplication_index_dict.
        current_checksum = getattr(cls, '_duplic_index_checksum', 0)
        try:
            """ There is a long-shot chance that the checksum below fails when 2 or more items change and the 
                resulting sum remains unaltered. Then, there's an efficiency-driven bet that such a scenario will 
                NEVER materialize. IF it ever does, it will correct itself next time the checksum changes, and 
                all db records will be properly updated and re-assigned. """
            checksum = sum([UUID(j).int for j in col_duplication_index if isinstance(j, str)])
        except(TypeError, ValueError):
            checksum = current_checksum  # On checksum failure, must exit with checksums unchanged.

        temp_dict = {}
        if checksum != cls._duplic_index_checksum:
            # Initializes __active_Duplication_Index_dict ONLY FOR DUPLICATE uids.
            # An EMPTY _active_duplication_index_dict means there's NO duplicates for that uid in the db table.
            setattr(cls, '_duplic_index_checksum', checksum)  # Updates checksum.
            dicto2 = {k: v for k, v in dicto1.items() if v is not None}  # dict with None values stripped off.
            duplic_values = list(dicto2.values())
            for item in set(duplic_values):  # item is a _Duplication_Index value.
                if duplic_values.count(item) > 1:
                    # If item appears more than once in col_duplication_index, it's a duplicate item.
                    # Gets all the uids associated to _Duplication_Index for item.
                    uid_list = [col_uid[j] for j, val in enumerate(col_duplication_index) if val == item]
                    # ONLY DUPLICATE ITEMS HERE (_Duplication_Index count > 1), to make search more efficient.
                    temp_dict[item] = tuple(uid_list)

            if temp_dict:  # There are changes in _Duplication_Index values. Must update both dicts.
                with cls.__lock:  # Both must be updated in the lock block.
                    cls._active_uids_dict = dicto1
                    cls._active_duplication_index_dict = temp_dict
            else:
                # If there's differences in dict keys only -> Initializes / updates _active_uids_df only.
                if set(dicto1) != set(cls._active_uids_dict.keys()):  # Changes in keys only.Updates _active_uids_df
                    cls._active_uids_dict = dicto1  # THIS IS A SHARED DICT. The assignment operation should be atomic.

        return None

    # @classmethod
    # def _init_uid_dicts(cls):
    #     """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
    #     Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
    #     This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
    #     _processDuplicates() run by background functions.
    #     Uses a checksum of _active_uids_df.values() to determine if there are changes to the dict.
    #     @return: None
    #     """
    #     sql = f'SELECT * from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" IS NULL OR ' \
    #           f'"{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ; '
    #     temp = dbRead(cls.tblObjName(), sql)
    #     if not isinstance(temp, DataTable):
    #         val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts no initialized. " \
    #               f"System cannot operate."
    #         krnl_logger.warning(val)
    #         raise DBAccessError(val)
    #     if temp:
    #         idx_uid = temp.getFldIndex("fldObjectUID")
    #         col_uid = [j[idx_uid] for j in temp.dataList]
    #         idx_dupl = temp.getFldIndex("fld_Duplication_Index")
    #         col_duplication_index = [j[idx_dupl] for j in temp.dataList]  # temp.getCol("fld_Duplication_Index")
    #         dicto1 = dict(zip(col_uid, col_duplication_index))  # Reference dict with all values
    #
    #         # Check for differences in dict values. Must update _active_uids_df,  _active_duplication_index_dict.
    #         try:
    #             checksum = sum([UUID(j).int for j in col_duplication_index if isinstance(j, str)])
    #         except(TypeError, ValueError):
    #             checksum = cls._duplic_index_checksum  # On checksum failure, must exit with checksums unchanged.
    #         temp_dict = {}
    #         if checksum != cls._duplic_index_checksum:
    #             # Initializes __active_Duplication_Index_dict ONLY FOR DUPLICATE uids.
    #             # An EMPTY _active_duplication_index_dict means there's NO duplicates for that uid in the db table.
    #             cls._duplic_index_checksum = checksum
    #             dicto2 = {k: v for k, v in dicto1.items() if v is not None}  # dict with None values stripped off.
    #             # for item in col_duplication_index:              # item is a _Duplication_Index value.
    #             duplic_values = list(dicto2.values())
    #             for item in duplic_values:
    #                 if item is not None and duplic_values.count(item) > 1:
    #                     # If item appears more than once in col_duplication_index, it's a duplicate item.
    #                     # Gets all the uids associated to _Duplication_Index for item.
    #                     uid_list = [col_uid[j] for j, val in enumerate(col_duplication_index) if val == item]
    #                     # ONLY DUPLICATE ITEMS HERE (_Duplication_Index count > 1), to make search more efficient.
    #                     temp_dict[item] = tuple(uid_list)
    #
    #         if temp_dict:  # There are changes in _Duplication_Index values. Must update both dicts.
    #             with cls.__lock:  # Both must be updated in the lock block.
    #                 cls._active_uids_df = dicto1
    #                 cls._active_duplication_index_dict = temp_dict
    #         else:
    #             # If there's differences in dict keys only -> Initializes / updates _active_uids_df only.
    #             if set(dicto1) != set(cls._active_uids_df.keys()):  # Changes in keys only.Updates _active_uids_df
    #                 cls._active_uids_df = dicto1  # THIS IS A SHARED DICT. The assignment operation should be atomic.
    #
    #     return None

    @classmethod
    def _processDuplicates(cls):  # Run by  Caravanas, Geo, Personas in other classes.
        """             ******  Run from an AsyncCursor queue. NO PARAMETERS ACCEPTED FOR NOW. ******
                        ******  Run periodically as an IntervalTimer func. ******
                        ****** This code should (hopefully) execute in LESS than 5 msec (switchinterval).   ******
        Re-loads duplication management dicts for class cls.
        @return: True if dicts are updated, False if reading tblAnimales from db fails or dicts not updated.
        """
        sql_trigger_tables = f'SELECT * FROM _sys_Trigger_Tables WHERE DB_Table_Name == "{cls.tblObjDBName()}" AND ' \
                          f'ROWID == Flag_ROWID; '
        tempdf = pd.read_sql_query( sql_trigger_tables, SQLiteQuery().conn)  # Only 1 record (for Terminal_ID) is pulled.
        # if isinstance(temp, DataTable) and temp:
        time_stamp = tempdf.loc[0, 'fldTimeStamp']  # time of the latest update to the table.
        # print(f'hhhhhhooooooooooolaa!! "{cls.tblObjDBName()}".processDuplicates - TimeStamp  = {time_stamp}!!')
        # val = values_list[0] if values_list else None
        # if val:
        if isinstance(time_stamp, datetime) and time_stamp > tempdf.loc[0, 'fldLast_Processing']:
            cls._init_uid_dicts()  # Reloads uid_dicts for class Geo.
            print(f'hhhhhhooooooooooolaa!! Estamos en Geo.processDuplicates. Just updated the dicts!!')
            # If dicts are processed, remove animalClassID items from db _sys_Trigger_Table.

            # Updates record in _sys_Trigger_Tables if with all entries for table just processed removed.
            # TODO(cmt): VERY IMPORTANT. _sys_Trigger_Tables.Last_Processing MUST BE UPDATED here before exiting.
            tempdf.loc[0, 'fldLast_Processing'] = time_stamp
            _ = setRecord('tbl_sys_Trigger_Tables', **tempdf.loc[0].to_dict())
            return True
        return None

    __trig_name_duplication = 'Trigger_Geo Entidades_INSERT'

    @classmethod
    def _processReplicated(cls):
        pass

    # List here ALL triggers defined for Animales table. Triggers can be functions (callables) or str.
    # TODO: Careful here. _processDuplicates is stored as an UNBOUND method. Must be called as _processDuplicates(cls)
    __db_triggers_list = [(__trig_name_duplication, _processDuplicates), ]

    def __init__(self, *args, **kwargs):                    # Falta terminar este constructor
        self.__ID = str(kwargs.get('fldObjectUID'))
        try:
            _ = UUID(self.__ID)                         # Validacion de valor en columna fldObjectUID leida de db.
        except(ValueError, TypeError, AttributeError):
            raise TypeError(f'ERR_INP_TypeError: Invalid/malformed UID {self.__ID}. Geo object cannot be created.')
        self.__isValid = True
        self.__recordID = kwargs.get('fldID')       # Useful to access the record directly. NOT expected to change!!
        self.__isActive = kwargs.get('fldFlag', 1)
        self.__name = kwargs.get('fldName')
        self.__containers = kwargs.get('fldContainers', [])  # getting fldContainers from JSON field: Geo objects.

        if not self.__containers or not isinstance(self.__containers, (tuple, list, str)):
            self.__containers = []
        elif isinstance(self.__containers, str):
            self.__containers = [self.__containers, ]  # Si str no es convertible a str -> Exception y a corregir.
        else:
            self.__containers = list(self.__containers)
        self.__container_tree = []    # Lista de objetos Geo asociados a los IDs de __containers.
        self.__localizationLevel = kwargs.get('fldFK_NivelDeLocalizacion')
        self.__abbreviation = kwargs.get('fldAbbreviation', '')
        self.__entityType = kwargs.get('fldFK_TipoDeEntidad')   # int Code for Pais, Provincia, Establecimiento, etc.
        # self.__country = kwargs.get('fldFK_Pais')
        # self.__farmstead = kwargs.get('fldFK_Establecimiento', '')          # farmstead = Establecimiento
        self.__isStateEntity = kwargs.get('fldFlag_EntidadEstado', 0)
        self.__area = kwargs.get('fldArea')
        self.__areaUnits = kwargs.get('fldFK_Unidad', 11)           # 11: Hectarea.
        super().__init__()

    @property
    def ID(self):
        return self.__ID

    @property
    def recordID(self):
        return self.__recordID

    @property
    def name(self):
        return self.__name

    @classmethod
    def getName(cls, name: str):
        n = removeAccents(name)
        namesDict = {k: cls.getGeoEntities()[k].name for k in cls.getGeoEntities() if n in
                     removeAccents(cls.getGeoEntities()[k].name)}
        return namesDict

    @classmethod
    def getUID(cls, name: str):
        name = re.sub(r'[-\\|/@#$%^*()=+¿?{}"\'<>,:;_]', ' ', name)
        name_words = [j for j in removeAccents(name).split(" ") if j]
        return next((k for k in cls.getGeoEntities()
                     if all(word in removeAccents(cls.getGeoEntities()[k].name) for word in name_words)), None)

        # n = removeAccents(name)
        # return next((k for k in cls.getGeoEntities() if n == removeAccents(cls.getGeoEntities()[k].name)), None)

    @classmethod
    def getObject(cls, name: str, *, localiz_level=None):
        """ Returns the Geo objects associated to name. **** Caution: ALWAYS returns a tuple.  ****
        Name can be a UUID or a regular "human" string name ('El Palmar - Lote 2', '9 de Julio', 'santa fe', etc.).
        @return: Geo objects tuple (1 or more objects) if any found; () if name not found in getGeoEntities dict.
        """
        try:
            o = cls.getGeoEntities().get(name, None)            # Primero busca un uid y lo retorna si existe.
        except (SyntaxError, TypeError):
            return ()
        if o is None:
            if isinstance(name, str):
                try:
                    name = re.sub(r'[-\\|/@#$%^*()=+¿?{}"\'<>,:;_]', ' ', name)        # Remove all special characters.
                    name_words = [j for j in removeAccents(name).split(" ") if j]      # split to list of words.
                    name = "".join(j for j in name_words).strip()  # all spaces removed, for string comparison.
                except (TypeError, AttributeError, SyntaxError, ValueError):
                    return ()
                else:
                    name_matches = {k: v for k, v in cls.getGeoEntities().items() if
                                    all(word in removeAccents(v.name) for word in name_words)}
                    if localiz_level:
                        localized = {k: v for k, v in name_matches.items() if v.localizLevel == localiz_level}
                        if localized:
                            name_matches = localized   # If no matches with passed localiz_level -> returns all matches.
                    # List of ALL geo objects with names matching name
                    obj_list = [v for k, v in name_matches.items() if name in
                                removeAccents(re.sub(r'[-\\|/@#$%^*()=+¿?{}"\'<>,:;_ ]', '', v.name))]
                    return tuple(obj_list)
        else:
            return o,
        return ()


    @classmethod
    def _initialize(cls):
        return cls.loadGeoEntities()

    @classmethod
    def loadGeoEntities(cls):
        """ This method is run in TransactionalObject.__init_subclass__(). So class is not fully initialized yet!! """
        temp1 = getRecords(cls.tblObjName(), '', '', None, '*', fldFlag=1)  # Carga solo GeoEntidades activas.
        if isinstance(temp1, str):
            retValue = f'ERR_DBAccess: Cannot load Geography table. Error: {temp1}'
            krnl_logger.error(retValue)
            raise DBAccessError(retValue)

        for j in range(temp1.dataLen):
            tempDict = temp1.unpackItem(j)
            geoID = tempDict.get('fldObjectUID', '')  # Since fldObjectUID has TEXT affinity, converts fld value to str.
            if geoID:
                cls.getGeoEntities()[geoID] = cls(**tempDict)       # Creates Geo object and adds to __registerDict.

        # Hay que usar loops for separados para inicializar TUTTO el __registerDict
        for entity in cls.getGeoEntities().values():
            container_list = entity.__containers    # __containers aqui es una lista de UID(str).
            if container_list:       # TODO(cmt): Convierte elementos de __containers de UID(str) a Geo.
                entity.__containers = [cls.getGeoEntities()[j] for j in container_list]  # if j in cls.getGeoEntities()
                entity.__container_tree = entity.__containers.copy()  # copy: cannot _generate_container_tree() here!!

            # Initializes the full_uid_list. Used to drive the duplicate detection logic. --> DEPRECATED!!
        # cls._fldID_list = set(g.recordID for g in cls.getGeoEntities().values())
        # # Initializes _object_fldUPDATE_dict={fldID: fldUPDATE(dictionary), }. Used to process records UPDATEd by other nodes.
        # cls._fldUPDATE_dict = {g.recordID: temp1.getVal(0, 'fldUPDATE', fldID=g.recordID) for g in
        #                        cls.getGeoEntities().values()}

        return True

    def _generate_container_tree(self):
        """Creates a list of GEO objects which are all containers for self. """
        aux_container = self.__containers
        for j in aux_container:
            self.__container_tree.extend(self._iter_containers(j))
            self.__container_tree.append(j)
        self.__container_tree.append(self)   # TODO(cmt): self por def. esta contenido en self -> Agrega al tree.
        self.__container_tree = list(set(self.__container_tree))
        self.__container_tree.sort(key=lambda x: x.localizLevel, reverse=True)  # Ordena por localizLevel decr.
        # print(f'--- Tree for geoObject {self.name}({self.ID}): {[t.name for t in self.__container_tree]}',
        #       dismiss_print=DISMISS_PRINT)
        return self.__container_tree

    @staticmethod
    def _iter_containers(geo_obj):  # geo_obj DEBE ser objeto Geo.
        """ Iterator to build container tree for each Geo object. """
        for item in geo_obj.__containers:
            if isinstance(item, (tuple, list)):         # 1. Procesa lista de objetos Geo.
                for obj in item:
                    if isinstance(obj, Geo):
                        yield obj
                    for loc in geo_obj._iter_containers(obj):       # Procesa elementos (loc) de una lista (obj)
                        if isinstance(loc, Geo):
                            yield loc
            else:
                yield item                              # 2. Procesa objetos Geo individuales
                for obj in geo_obj._iter_containers(item):
                    if isinstance(obj, Geo):
                        # print(f'obj is: {o.name}')
                        yield obj

    @property
    def localizLevel(self):
        return self.__localizationLevel

    @property
    def containers(self):
        """ Returns tuple with Geo objects IDs. All containers for self. """
        return self.__containers

    @containers.setter
    def containers(self, item):
        self.__containers = item            # Careful!: Item can by anything...

    @property
    def container_tree(self):
        return self.__container_tree

    @property
    def entityType(self):
        return self.__entityType

    def contains(self, entity=None):
        """ True if self is a container for entity. That is, entity is a part of and is included in self."""
        try:
            return self in entity.container_tree
        except (AttributeError, TypeError, NameError):
            return False

    def contained_in(self, entity=None):
        """ True if entity is a container for self. That is, self is included in entity. """
        try:
            return entity in self.container_tree
        except TypeError:
            return False

    def comp(self, val):            # Method to be used to run comparisons in ProgActivities methods.
        """ Acepta como input Geo object o str (UUID). """
        if isinstance(val, str):
            val = self.getObject(val)
        return self.contained_in(val) if isinstance(val, Geo) else False


    @property
    def isStateEntity(self):
        return self.__isStateEntity

    @property
    def area(self):
        return self.__area, self.__areaUnits

    @property
    def isActive(self):
        return self.__isActive

    def register(self):  # NO HAY CHEQUEOS. obj debe ser valido
        self.__registerDict[self.ID] = self  # TODO: Design criteria: Si se repite key, sobreescribe con nuevo valor
        return self

    def unRegister(self):
        """
        Removes object from __registerDict
        @return: removed object if successful. None if fails.
        """
        return self.__registerDict.pop(self.ID, None)  # Retorna False si no encuentra el objeto


    @classmethod
    def createGeoEntity(cls, *args, name=None, entity_type: str = '',  containers=(), abbrev=None, state_entity=False,
                        **kwargs):
        """   For now, creates anything. TODO: Implement integrity checks for State Entities (country, province, etc.)
        Creates a geo Element (Country, Province, Establecimiento, Potrero, Location, etc). Adds it to the DB.
         @param state_entity: Flag True/False
         @param name: Entity Name. Mandatory.
        @param abbrev: Name abbreviation. Not mandatory.
         @param entity_type: Country, Province/State, Department, Establecimiento, Potrero, etc.
        @param containers: list. Geo Entitities objects that contain the new entity (1 or more).
        At least the MANDATORY container entity(es) must be provided.
        @return: geoObj (Geo object) or errorCode (str)
        """
        tblEntity = next((j for j in args if j.tblName == cls.__tblObjectsName), None)
        tblEntity = tblEntity if tblEntity is not None else DataTable(cls.__tblObjectsName, *args, **kwargs)
        entity_type_id = tblEntity.getVal(0, 'fldFK_TipoDeEntidad', None)
        if entity_type_id is None and removeAccents(entity_type) not in cls.getLocalizLevelsDict():
            retValue = f'ERR_INP_Invalid Argument Entity Type: {entity_type}'
            krnl_logger.info(retValue)
            return retValue
        else:
            entity_type_from_tbl = next((k for k in cls.getLocalizLevelsDict()
                                         if cls.getLocalizLevelsDict()[k] == entity_type_id), None)
            entity_type = entity_type_from_tbl if entity_type_from_tbl is not None else removeAccents(entity_type)
            entity_type_id= entity_type_id if entity_type_id is not None else cls.getLocalizLevelsDict()[entity_type][4]
            entity_type_level = cls.getLocalizLevelsDict()[entity_type][0]
            entity_mandatory_level = cls.getLocalizLevelsDict()[entity_type][3]
            state_entity= bool(state_entity) if state_entity is not None else cls.getLocalizLevelsDict()[entity_type][2]
        containers_from_tbl = tblEntity.getVal(0, 'fldContainers', None)
        if containers_from_tbl is not None:
            containers = containers_from_tbl
        if isinstance(containers, Geo):
            containers = [containers, ]
        elif not isinstance(containers, (list, tuple)):
            retValue = f'ERR_INP_Invalid Arguments. Mandatory container entities not valid or missing.'
            krnl_logger.info(retValue)
            return retValue

        # filtra solo containers que sean Geo y con localizLevel apropiado
        containers = [j for j in containers if isinstance(j, Geo) and j.localizLevel < entity_type_level]
        full_list = []
        for e in containers:
            full_list.extend(e.container_tree)
        full_list_localiz_level = [j.localizLevel for j in list(set(full_list))]  # Lista de localizLevel del Tree.
        if not full_list_localiz_level or all(j > entity_mandatory_level for j in full_list_localiz_level):
            retValue = f'ERR_INP_Invalid Arguments. Mandatory container entities not valid or missing.'
            krnl_logger.info(retValue)
            return retValue          # Sale si no se paso Mandatory Container requerido para ese localizLevel.

        name_from_tbl = tblEntity.getVal(0, 'fldName', None)
        name = name_from_tbl if name_from_tbl is not None else name
        name = removeAccents(name, str_to_lower=False)      # Remueve acentos, mantiene mayusculas.
        abbrev = abbrev if abbrev is not None else tblEntity.getVal(0, 'fldAbbreviation', None)
        abbrev = abbrev.strip().upper() if isinstance(abbrev, str) else None
        timeStamp = tblEntity.getVal(0, 'fldTimeStamp', None) or time_mt('datetime')
        user = tblEntity.getVal(0, 'fldFK_UserID', None) or sessionActiveUser
        containers_ids = [j.ID for j in containers]
        tblEntity.setVal(0, fldTimeStamp=timeStamp, fldFK_NivelDeLocalizacion=entity_type_level, fldName=name,fldFlag=1,
                         fldObjectUID=str(uuid4().hex), fldAbbreviation=abbrev, fldFlag_EntidadEstado=state_entity,
                         fldContainers=containers_ids, fldFK_TipoDeEntidad=entity_type_id, fldFK_UserID=user)
        idRecordGeo = setRecord(tblEntity.tblName, **tblEntity.unpackItem(0))
        if isinstance(idRecordGeo, str):
            retValue = f'ERR_DBAccess: Could not write to table {tblEntity.tblName}. Geo Entity not created.'
            krnl_logger.info(retValue)
            return retValue

        tblEntity.setVal(0, fldID=idRecordGeo, fldContainers=containers)        # Must use Geo Objects for containers.
        entityObj = cls(**tblEntity.unpackItem(0))      # Crea objeto Geo y lo registra.
        entityObj._generate_container_tree()
        entityObj.register()
        return entityObj


    def removeGeoEntity(self):
        """ Removes from __registerDict and sets as Inactive in datbase (for now, to avoid deleting records)
        Sets datetime of 'removal' in db record's fldComment
        """
        self.unRegister()
        return setRecord(self.__tblObjectsName, fldID=self.__recordID, fldFlag=0, fldComment=f'Record set to Inactive '
                                                                                  f'on {time_mt("datetime")}')



# ---------------------------------------------- End Class Geo --------------------------------------------------- #

# Complete initialization of Geo objects and data interfacess. Create Container Trees for Geo objects.
# TODO: Got to do this here. Cannot do in loadFromDB_old()!!.
for o in Geo.getGeoEntities().values():
    # print(f'Geo Object: {o.name}')
    o._generate_container_tree()

# These adapter / converter are used for 'ID_Localizacion' fields across the DB. NOT USED WITH THE Geo Tables, though..
sqlite3.register_adapter(Geo, adapt_geo_to_UID)    # Serializes Geo to int, to store in DB.
sqlite3.register_converter('GEOTEXT', convert_to_geo)  # Converts int to a Geo object querying getGeoEntities() by UUID.
krnl_logger.info('Geo structure initialized successfully.\n')
# ---------------------------------------------------------------------------------------------------------------- #
