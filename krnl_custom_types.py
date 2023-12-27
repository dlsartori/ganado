""" Defines types DataTable, Amount, Transaction.
    Defines functions setRecord(), getRecords(), delRecord()
    Defines JSON serializer class to enable storage of lists, tuples, dicts in JSON format in DB (in cols named 'JSON').
"""
from krnl_config import lineNum, strError, uidCh, callerFunction, krnl_logger, fDateTime, timerWrapper, print, time_mt,\
    removeAccents, DISMISS_PRINT, BIT_UPLOAD, BIT_SYNC, db_logger, MAIN_DB_NAME, exec_sql
from krnl_sqlite import getTblName, getFldName, SQLiteQuery, DBDuplicationMgr
from krnl_parsing_functions import strSQLConditions1Table, strSQLSelect1Table
from datetime import datetime
from collections import defaultdict
import os
import json
import sqlite3
from uuid import UUID, uuid4
from money import Money, XMoney
from decimal import Decimal
import functools
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from krnl_db_access import _upload_exempted, SqliteQueueDatabase, writeObj, DBAccessError, init_database
from krnl_async_buffer import BufferAsyncCursor, AsyncBuffer

ACTIVITY_LOWER_LIMIT = 15
ACTIVITY_UPPER_LIMIT = 30
ACTIVITY_DAYS_TO_ALERT = 15
ACTIVITY_DAYS_TO_EXPIRE = 365
ACTIVITY_DEFAULT_OPERATOR = None
MAX_WRT_ORDER = 100

def moduleName():
    return str(os.path.basename(__file__))

defaultCurrencyName = 'ARS'
defaultCurrencyCode = 32


class DataUploadCursor(BufferAsyncCursor):      # TODO(cmt): This class and Buffer are deprecated. NOT USED ANYMORE.
    """ Async cursor to write sync-records to database. sync-records are database records that must be uploaded to
    server or sync'd with other devices via a connection link.
    DataUploadCursor defines the specific implementation of cursor objects to perform the writes using setRecords().
    Rules:
        1) All data for the cursor must be passes as a single argument (data).
        2) an Event object can be passed optionally.
        3) Implement methods execute() and format_item() with the details for the object.
    """
    __lock = Lock()
    __tblDefaultName = 'tbl_Upload_To_Server'  # DB table to flush the async_buffer data
    __tblUploadName = __tblDefaultName
    __fldOrder = ('fldID', 'fldDBName', 'fldDBTableName', 'fldDBFields', 'fldDBRecord', 'fldDBOperation', 'fldBitmask',
                  'fldPushUpload', 'fldTimeStampSync')
    _writes_to_db = MAIN_DB_NAME  # Flag to signal that the class uses db-write functions setRecord(), setRecords()
    # __min_block_size = 0  # Number of records to write with setRecords.
    # __timeout_secs = 0.2  # set this to 1 - 5 secs in final version. Check what's best.
    # __temp_buffer = []  # Temporary buffer to park at least __min_block_size items before doing a write.
    # _temp_buffer_event = Event()
    # __timeout_flag = time_mt()  # Signal to flush stale buffer data to db.

    def __init__(self, *args, event=None, the_object=None, the_callable=None):
        """
        @param event: Event object that may be passed to signal completion. Not used here.
        @param data: All data to be appended (put) to the queue as a BufferAsyncCursor object.
        @param the_object: the object for which the callable is called. Optional.
        @param the_callable: callable to execute operations on the object. Optional.
        """
        self._args = args  # Data for the object to be operated on (stored, bufferized, etc).
        super().__init__(event=event, the_object=the_object, the_callable=the_callable)

    @classmethod
    def format_item(cls, *args, event=None, the_object=None, the_callable=None, **kwargs):
        """ Item-specific method to put item on the AsyncBuffer queue
        @param event: Event object that may be passed to signal completion. Not used here.
        @param args: All data to be appended (put) to the queue as a BufferAsyncCursor object.
        @param kwargs: Not used here.
        @param the_object: the object for which the callable is called
        @param the_callable: callable to execute operations on the object.
        @return: type(cursor)=cls -> valid cursor. None -> Invalid object: do NOT enqueue().
        """
        if not isinstance(args, (list, tuple)):
            return None
        if any(not isinstance(j, (list, tuple)) for j in args):  # checks for single record (setRecord() call)
            args = [args, ]
        # Creates a cursor with DataUploadCursor.__init__() and sets event and data values.
        return cls(*args, event=event, the_object=the_object, the_callable=the_callable)  # returns cursor.

    def execute(self):
        """ Object-specifc method to process the object (store in async_buffer, record to DB, etc).
        Called from an independent thread managed by the Writer, that writes self._data to table [_Upload_To_Server]
        using setRecords().
        """
        tbl = DataTable(self.__tblUploadName, *self._args, self.__fldOrder, db_sync=False)
        # TODO: 10Jun23. Look into creating a DataTable from the data in self._data with n records in it.
        #  (n = tbl_block_size attribute). Then pack the data with tblPack() and write that data to Upoad_To_Server.
        #  A variable number of DataTable instances will be generated in this way, each of them to
        #  be written to DB in a for loop. In this way multiple records can be sent over the connection link, along
        #  with all the table data, to optimize the data transfer.
        return tbl.setRecords()              # cur.rowcount de sqlite3 or None if fails

    def reset(self):
        self._args = []
        # self.__timeout_flag = time_mt()

# AsyncBuffer object with queue of records to _Upload_To_Server. Used by setRecord() and setRecords().
# TODO(cmt): this is the only one to use precedence=0, as it's directly tied (at low level) to the DB write cursor.
#  DEPRECATED AS OF 4.1.8
_uploadBuffer = AsyncBuffer(DataUploadCursor, autostart=False, precedence=0, thread_priority=3)  # thread_priority: 0-20. 0 is Highest.
# ------------------------------------------------------------------------------------------------------------------ #


class DataTable(object):
    """
    Class to store obj_data from DB queries. structures from DB are created initially by function dbRead().
    The objects can be queried to pull records and can be "unpacked" to format the obj_data inside the object
    Also used as the standard parameter-passing structure between functions and methods to access/process DB data.
    """
    __slots__ = ('_tblName', '__dbTableName', '_dataList', '_fldNames', '__dbFldNames', '__fldUIDs', '__fldMap',
                 '__fldNamesLen', '__fldIndices', '__uidFldMap', '__isValidFlag', '__undoOnError', '__wrtOrder',
                 '__pkAutoIncrement', '__operatorsDict', '__conditionsDict', '__breakOnError', '__associatedTables',
                 '__successfulAssociatedWrite', '_operation', '__table_default_bitmask', '__db_sync',
                 '__upload_to_server', '__process_duplicates')

    __operationsList = ('insert', 'update', 'replace', 'delete', 'cmd1', 'cmd2')  # Deprecated.
    __xferableItems = ('_tblName', '_dataList', '_fldNames', '_operation')  # Deprecated. items to pack to JSON for DB synchronizat.
    __tblUploadName = 'tbl_Upload_To_Server'         # Deprecated.
    __uploadTblFields = getFldName(__tblUploadName, '*', 1)       # Full field list for tbl_Upload_To_Server.   # Deprecated.
    __fldOrder = ('fldID', 'fldDBName', 'fldDBTableName', 'fldDBFields', 'fldDBRecord', 'fldDBOperation',
                  'fldBitmask', 'fldPushUpload', 'fldTimeStampSync')       # Deprecated.   # field order para _Upload_To_Server table.

    _writeObj = SqliteQueueDatabase(MAIN_DB_NAME)  # Defined as class attr. 'cause there's only 1 instance per database.

    # setRecords() constants:
    __SINGLE_BLOCK_SIZE = 500        # 200: Size of first block of data to write in multi-threading, in data records
    __THREAD_WRT_BLOCK_SIZE = 1000   # 1000: Number of records each thread writes to DB. Speed: 5000 records in 3 msec.
    __MAX_WRT_CONCURRENT_THREADS = 500  # Max number of write threads to be created. TODO: Test this limit!
    __func_names_strings = ('setRecords', )

    def __init__(self, tblName=None, dList=None, keyFieldNames=None, *args, db_sync=False, upload=1, **kwargs):
        """

        @param tblName: Table key name. Must be a valid database table.
        @param dList: List of lists. Data records to populate the table
        @param keyFieldNames: list of valid field names for tables. Key field Names, in the list order passed.
        @param args: Data Table table to set arguments to. TODO: to be removed. Incorrect implementation.
        @param db_sync: Obsolete. Deprecated. TODO: to be removed.
        @param upload:  Obsolete. Deprecated. TODO: to be removed.
        @param kwargs: remove_duplicates -> Enable/Disable removal of duplicate records in select tables. Default=True.
        """
        tblName = str(tblName).strip()

        if kwargs.get('non_db_table', None) is True:        # TODO: Placeholder para crear "non-db" tables.
            tblInfo = None
        else:
            tblInfo = getTblName(tblName, 1)  # tupla (dbTblName, tblIndex, pkAutoIncrementYN, isWITHOUTROWID, db_sync)

        if isinstance(tblInfo, str):
            self.__dbTableName = tblInfo    # tblInfo.__contains__(strError):   # _tblName no existe en DB
            self.__isValidFlag = False
            self._dataList = ()
            self._fldNames = ()
            self.__dbFldNames = ()
            self.__fldUIDs = {}                    # UIDs se crean durante la inicializacion. Se usan en Signatures.
            self._tblName = self.__dbTableName     # Ambos nombres de tabla contienen la particula ERR_
        else:
            self.__process_duplicates = bool(kwargs.get('remove_duplicate_records', True)) if kwargs else True
            self._tblName = tblName
            self.__isValidFlag = True
            self.__dbTableName = tblInfo[0] if tblInfo else None
            self.__pkAutoIncrement = tblInfo[2] if tblInfo else None  # 1: PK se autoincrementa; 0: PK no autoincrementa
            self._dataList = list(dList) if isinstance(dList, (list, tuple)) else []   # 1 sola [] para que compare con IS NOT
            if self._dataList:
                if any(not isinstance(j, (list, tuple)) for j in self._dataList):
                    self._dataList = [self.dataList, ]
                self._dataList = [list(j) for j in self._dataList]   # Convierte a lista todos los elementos internos

            fldNamesDict = getFldName(self._tblName, '*', 1)
            if keyFieldNames and isinstance(keyFieldNames, (list, tuple, set)):
                # TODO(cmt): keyFieldNames via function args. Se permiten campos ad-hoc no definidos en la Tabla en DB.
                self._fldNames = list(keyFieldNames)  # [j for j in keyFieldNames if isinstance(j, str)] # Acepta solo strings
                if 'fldID' not in self._fldNames:
                    self._fldNames.append('fldID')
                    for j in self._dataList:
                        # if isinstance(j, list):
                        j.append(None)
                self.__dbFldNames = [fldNamesDict[k] for k in self._fldNames if k in fldNamesDict]
                self.__fldMap = dict(zip(self._fldNames, self.__dbFldNames))
            else:
                self.__fldMap = fldNamesDict  # Si no se pasan keyFieldNames, toma fields de tabla en DB.
                self._fldNames = list(self.__fldMap.keys())  # List: se pueden agregar Campos una vez creada  tabla
                self.__dbFldNames = list(self.__fldMap.values())

            self.__fldNamesLen = len(self._fldNames)    # if hasattr(self._fldNames, '__iter__') else 0
            self.__fldIndices = dict.fromkeys(self._fldNames)
            for k in self.__fldIndices:
                self.__fldIndices[k] = self._fldNames.index(k)

            # Verifica integridad de dataList: len de cada records de dataList == __fldNamesLen
            if self._dataList:
                # if any(len(j) != self.__fldNamesLen for j in self._dataList):  # Comparacion correcta (y mas pesada)
                if len(self._dataList[0]) != self.__fldNamesLen:  # Comparacion solo del 1er record.Asume todos iguales.
                    print(f'%%%%%%%%% DataTable {lineNum()} Aqui hay pedo!: Table name: {self.tblName}; fldNamesLen: '
                          f'{self.__fldNamesLen}; len(dataList[0]): {len(self._dataList[0])} %%%%%%%%%%%%%')
                    self._dataList = []     # Si no coincide con fldNamesLen, no hay como asignar dataList a fldNames.

            # fldUIDs: {fldName: fldUID, }, con fldUID=self._tblName+uidCh+self.__fldName, y uidCh = '__' al momento..
            self.__fldUIDs = {j: self._tblName+uidCh+j for j in self._fldNames}  # Crea fldUID para non-Table fields too
            self.__uidFldMap = dict(zip(self._fldNames, self.__fldUIDs))
            self.__undoOnError = False  # True: Undo writes en esta tabla al fallar escritura posterior de otras tablas

            # operation to be performed on dataList when the table is transferred/sync'd (INSERT, UPDATE, DELETE, cmd)
            # self._operation = kwargs.get('_operation') if kwargs.get('_operation') in self.__operationsList else None
            self.__table_default_bitmask = tblInfo[4] if tblInfo else 0
            if self.tblName == self.__tblUploadName:
                self.__db_sync = self.__upload_to_server = self.__table_default_bitmask = 0  # tblUpload no va a server
            else:
                self.__upload_to_server = bool(upload) * 1 if isinstance(upload, int) else \
                    self.__table_default_bitmask & BIT_UPLOAD
                self.__db_sync = bool(db_sync) * 2 if isinstance(db_sync, int) else \
                    self.__table_default_bitmask & BIT_SYNC
                bitmask = self.__upload_to_server + self.__db_sync
                bitmask_idx = self.getFldIndex('fldBitmask')
                if bitmask_idx:
                    for j in self._dataList:
                        j = list(j)  # Setea bits de bitmask para registros individuales. list(j) por si j es tuple.
                        j[bitmask_idx] = bitmask
            if args:
                try:
                    tables = [t for t in args if isinstance(t, DataTable)]
                    for t in tables:  # Asigna valores de *args (si se pasaron)
                        if t.tblName == self._tblName and t.dataLen:
                            for j in range(t.dataLen):     # Inicializa registros en dataList (si se pasa mas de 1)
                                self.setVal(j, **t.unpackItem(j))  # Escribe obj_data de args en dataList[j] (1 o mas reg)
                            break
                except (TypeError, ValueError, IndexError, KeyError, NameError):
                    print(f'ERR_INP_InvalidArgument - DataTable __init__({lineNum()})')
                    krnl_logger.error(f'ERR_INP_InvalidArgument - DataTable __init__({lineNum()})')

            # Asigna data de kwargs. IMPORTANTE: se escriben en _dataList SOLO si esos campos en *args son None
            # Si kwargs[i] es lista o tuple se asigna cada elemento de kwargs[i] a un Index de _dataList
            if kwargs:
                _ = getFldName(tblName, '*', 1)
                commonKeys = set(kwargs).intersection(_)  # set(kwargs) arma un set con todos los keys de kwargs.
                kwargsParsed = {k: kwargs[k] for k in commonKeys}  # retorna solo campos presentes en tabla tblName
                for fName in kwargsParsed:
                    if isinstance(kwargsParsed[fName], dict):
                        pass                 # Ignora: Diccionarios no son un tipo valido para pasar en kwargs
                    else:
                        if not isinstance(kwargsParsed[fName], (list, tuple, set)):
                            kwargsParsed[fName] = [kwargsParsed[fName], ]
                        for j in range(len(kwargsParsed[fName])):  # j es Index de _dataList, fName es fldName
                            if self.getVal(j, fName) is None:
                                self.setVal(j, fName, kwargsParsed[fName][j])  # Escribe solo si kwargs['fldName'] = None,'',NULL

        super().__init__()
        # if kwargs.get('remove_duplicates', True):
        #     # If table supports Duplication removes all duplicates and returns Original Record only.
        #     self.__remove_duplicate_records()


    def __bool__(self):             # Allows to check on object as if tblData, if not tblData.
        return self.isValid and bool(self.dataLen)  # Returns False if dataLen is 0. Otherwise returns True.


    @property
    def isValid(self):
        return self.__isValidFlag

    @property
    def dataList(self):
        return self._dataList

    @property
    def dataLen(self):
        return len(self._dataList) if self._dataList else 0

    @property
    def fldNames(self):
        return list(self._fldNames)             # Se debe retornar listas, para que sean modificables.

    @property
    def dbFldNames(self):
        return list(self.__dbFldNames)          # Se debe retornar listas, para que sean modificables.


    def fldMap(self, *, reverse=False):
        """
        Returns dictionary {fldName: dbFldName, }
        @param reverse: reverses dictionary order
        @return: dictionary {fldName: dbFldName, } if reverse=False or {dbFldName: fldName, } if reverse=True.
        """
        if not reverse:
            return self.__fldMap
        else:
            return dict(zip(self.__fldMap.values(), self.__fldMap.keys()))

    @property
    def fldNamesLen(self):
        return self.__fldNamesLen if self.__isValidFlag else 0

    def getDBFldName(self, fName: str):  # Retorna False si no encuentra el campo
        # Retorna None si no encuentra el campo o si objeto DataTable no es valido.
        return self.__fldMap.get(str(fName).strip(), None)      # if self.isValid else None

    def getUIDDict(self):
        """
        @return: Dict. {fldName: fldUID}  fldUID = "tblName__fldName" -> Unique UID for field.
        """
        return self.__fldUIDs if self.__isValidFlag is True else {}

    @property
    def uids(self, *, mode=0):
        """Returns fldUIDs for DataTable object.
        @param mode: 0-> returns list of UIDs. 1-> returns dictionary {fldName: fldUID, }   """
        if not mode:
            return tuple(self.__fldUIDs.values())
        return self.__fldUIDs

    def getFldUID(self, fName):
        # Retorna None si no encuentra el campo o si objeto no es valido.
        if self.isValid:
            return self.__uidFldMap.get(str(fName).strip())      # Esta llamada retorna None si fName no se encuentra
        return None  # None si objeto no es valido.

    def getFldIndex(self, fName):
        return self.__fldIndices.get(fName)        # Retorna None si fName no esta en __fldIndices.

    @property
    def tblName(self):
        return self._tblName if self.isValid is True else None

    @property
    def dbTblName(self):
        return self.__dbTableName if self.isValid is True else None

    def index_min(self, fld: str = None):
        """
        Returns the record index (dataList index) where the minimum value of field fld is found.
        If multiple min values, returns a tuple with all the indices where fld has a min.
        @param fld: column for which the minimum is required.
        @return: dataTable index list (tuple).
        """
        if isinstance(fld, str):
            fld = fld.strip()
            if fld in self._fldNames:
                col = [j[self._fldNames.index(fld)] for j in self.dataList]     # self.getCol(fld)
                try:
                    limit = min(col)
                except (TypeError, ValueError, AttributeError):
                    return ()
                else:
                    return tuple([i for i, v in enumerate(col) if v == limit])
        return()


    def index_max(self, fld: str = None):
        """
        Returns the record index (dataList index) where the maximum value of field fld is found.
        If multiple max values, returns a tuple with all the indices where fld has a max.
        @param fld: column for which the maximum is required.
        @return: dataTable index list (tuple).
        """
        if isinstance(fld, str):
            fld = fld.strip()
            if fld in self._fldNames:
                col = [j[self._fldNames.index(fld)] for j in self.dataList]
                try:
                    limit = max(col)
                except (TypeError, ValueError, AttributeError):
                    return ()
                else:
                    return tuple([i for i, v in enumerate(col) if v == limit])
        return ()


    @property
    def pkAutoIncrement(self):
        """ Primary Key autoincrements ONLY if field is defined as PRIMARY KEY INTEGER in SQLite"""
        return bool(self.__pkAutoIncrement)

    @property
    def undoOnError(self):  # val: True or False
        return self.__undoOnError if self.isValid is True else None

    @undoOnError.setter
    def undoOnError(self, val):
        self.__undoOnError = bool(val)

    # @property
    # def wrtOrder(self):  # val: True or False
    #     return self.__wrtOrder if self.__isValidFlag is True else None
    #
    # @wrtOrder.setter
    # def wrtOrder(self, val):
    #     self.__wrtOrder = min(int(val), MAX_WRT_ORDER)
    #
    # @property
    # def breakOnError(self):  # val: True or False
    #     return self.__breakOnError if self.__isValidFlag is True else None
    #
    # @breakOnError.setter
    # def breakOnError(self, val):
    #     self.__breakOnError = bool(val)

    def clear(self):
        self._dataList *= 0

    def __remove_duplicate_records(self):       # TODO: 09-Dec-23: DROP THIS FUNCTION. WILL NOT BE NEEDED.
        """ Removes duplicate records for tables that implement Duplicate support. For multiple duplicates, returns
        only the Original Record (duplicate with the earliest value for fldTimeStamp).
        @return: None
        """
        if self._tblName in DBDuplicationMgr.get_tables_with_duplicates():   # updated set with tables with Duplication.
            if any(f not in self._fldNames for f in ('fldObjectUID', 'fld_Duplication_Index', 'fldTimeStamp')):
                # Creates auxiliary DataTable to find MIN(fldTimeStamp) if the required fields are not present in self.
                col_rowid = str(tuple(self.getCol("fldID")))   # Stringed list of fldID values in self DataTable.
                # sql = f'SELECT {getFldName(self._tblName, "fldID")}, {getFldName(self._tblName, "fldObjectUID")},' \
                #       f'{getFldName(self._tblName, "fld_Duplication_Index")}, ' \
                #       f'{getFldName(self._tblName, "fldTimeStamp")}' \
                #       f'FROM {self.dbTblName} WHERE {getFldName(self._tblName, "fldDateExit")} == 0 OR ' \
                #       f'{getFldName(self._tblName, "fldDateExit")} IS NULL; '
                sql = f'SELECT {getFldName(self._tblName, "fldID")}, {getFldName(self._tblName, "fldObjectUID")}, ' \
                      f'{getFldName(self._tblName, "fld_Duplication_Index")},' \
                      f'{getFldName(self._tblName, "fldTimeStamp")} FROM {self.dbTblName} WHERE ' \
                      f'{getFldName(self._tblName, "fldID")} IN {col_rowid}; '

                auxTable = dbRead(self._tblName, sql)
                if isinstance(auxTable, str) or not auxTable:
                    return None
                cols_repeat = auxTable.getCols('fldObjectUID', 'fld_Duplication_Index')
            else:
                cols_repeat = self.getCols('fldObjectUID', 'fld_Duplication_Index')   # fields already present in self.
                auxTable = self

            # Now pulls 1 or multiple Original Records, based on the MIN value of fldTimeStamp for each set of records.
            # First must identify sets of records with equal fld_Duplication_Index. Stores all records with count > 1.
            # Creates list of repeat _Duplication_Index and removes repeat items from list.
            repeat_items_set = set([cols_repeat[1][i] for i in range(len(cols_repeat))
                                   if cols_repeat[0][i] != cols_repeat[1][i]]) if self.dataLen > 1 else \
                {auxTable.getVal(auxTable.getRecordIndex(self.getVal(0, 'fldID')), 'fld_Duplication_Index')}

            for v in repeat_items_set:       # i: Index of dataList; v: Field value.
                # auxDict = {}                # {dataList index: TimeStamp value}
                # for i in range(auxTable.dataLen):
                #     if cols_repeat[1][i] == v:   # iterates through col_duplIndex pulling indices when list item = v
                #         auxDict[i] = auxTable.getVal(i, 'fldTimeStamp')

                # iterates through col_duplIndex pulling indices when list item = v
                auxDict = {i: auxTable.getVal(i, 'fldTimeStamp') for i in range(auxTable.dataLen)
                           if cols_repeat[1][i] == v}

                # Identifies the key with the min fldTimeStamp value and removes it from dict.
                auxDict.pop(min(auxDict, key=auxDict.get))  # removes the FIRST dict key to present a min value.

                # Finally, removes records from DataTable dataList for all sets of repeat _Duplication_Index
                for key in auxDict:
                    self.popRecord(key)


    def __remove_duplicate_records00(self):
        """ Removes duplicate records for tables that implemenet Duplicate support.
        @return: None
        """
        if self._tblName in DBDuplicationMgr.get_tables_with_duplicates():   # updated set with tables with Duplication.
            if any(f not in self._fldNames for f in ('fld_Duplication_Index', 'fldTimeStamp')):
                # Creates auxiliary DataTable to find MIN(fldTimeStamp) if the required fields are not present in self.
                col_rowid = str(tuple(self.getCol("fldID")))   # Stringed list of fldID values in self DataTable.
                sql = f'SELECT {getFldName(self._tblName, "fld_Duplication_Index")},' \
                      f'{getFldName(self._tblName, "fldTimeStamp")} FROM {self.dbTblName} WHERE ' \
                      f'{getFldName(self._tblName, "fldID")} IN {col_rowid}; '
                auxTable = dbRead(self._tblName, sql)
                if isinstance(auxTable, str) or not auxTable:
                    return None
                col_duplIndex = auxTable.getCol('fld_Duplication_Index')
            else:
                col_duplIndex = self.getCol('fld_Duplication_Index')        # field already present in self.
                auxTable = self

            # Now pulls 1 or multiple Original Records, based on the MIN value of fldTimeStamp for each set of records.
            # First must identify sets of records with equal fld_Duplication_Index. Stores all records with count > 1.
            repeat_items_list = [val for val in col_duplIndex if col_duplIndex.count(val) > 1]
            for v in repeat_items_list:       # i: Index of dataList; v: Field value.
                # auxDict = {}                # {dataList index: TimeStamp value}
                # for i in range(auxTable.dataLen):
                #     if col_duplIndex[i] == v:   # iterates through col_duplIndex pulling indices when list item = v
                #         auxDict[i] = auxTable.getVal(i, 'fldTimeStamp')

                # iterates through col_duplIndex pulling indices when list item = v
                auxDict = {i: auxTable.getVal(i,'fldTimeStamp') for i in range(auxTable.dataLen) if col_duplIndex[i]==v}

                # Identifies the key with the min fldTimeStamp value and removes it from dict.
                auxDict.pop(min(auxDict, key=auxDict.get))  # removes the FIRST dict key to present a min value.

                # Finally, removes records from DataTable dataList for all sets of repeat _Duplication_Index
                for key in auxDict:
                    self.popRecord(key)





    def unpackItem(self, j=-1, mode=0, *, fldID=None):
        """
        Unpacks the j-element of _dataList in dictionary form and returns the dict.
        j < 0 -> Operates list as per Python rules: accesses items starting from last one. -1 represents the last item
        @param mode: 0 -> returns dict with _fldNames as keys. Default mode.
                     1 -> returns dict with dbFieldNames as keys
                     2 -> returns dict with fldUIDs as keys
        @param fldID: returns the record corresponding to the fldID passed.

        *** If fldID is passed, it takes precedence over j. If fldID is None or not found returns {} ***

        @param j: index to _dataList table. Must be in the range of dataLen
        @return: {fldName1: value1, fldName2: value2, }. _fldNames from obj._fldNames list.
        """
        if fldID:
            try:
                fldID_idx = self.getFldIndex('fldID')
                j = next((i for i in range(self.dataLen) if self._dataList[i][fldID_idx] == fldID), None)
            except(TypeError, IndexError, ValueError, AttributeError):
                return {}
            else:
                if j is None:
                    return {}

        if self.__isValidFlag and self._fldNames:
            if not isinstance(j, int) or not self.dataLen:
                return {}
            if not mode:       # 0 (Default): retorna {fldName: fldValue, }
                return dict(zip(self._fldNames, self._dataList[j])) if abs(j) in range(self.dataLen + (j < 0)) else {}

            elif mode == 1:  # Mode is != 0
                return dict(zip(self.__dbFldNames, self._dataList[j])) if abs(j) in range(self.dataLen + (j<0)) else {}

            else:   # 2 o mas. retorna {fldUID: fldValue, }lp
                return dict(zip(self.__fldUIDs.values(), self._dataList[j])) \
                    if j in (range(self.dataLen) if j >= 0 else range(-self.dataLen, 0)) else {}  # tanto gre,gre aqui..
        return {}  # objeto no es valido.

    def setVal(self, recIndex=0, fName=None, val=None, **kwargs):
        """
        Sets the values for fieldNames passed in kwargs, of the record at recIndex position in _dataList
        Can INSERT (if recIndex > that last record in _dataList) or UPDATE a record (recindex within _dataList range)
        Any keyname not corresponding to a field in DataTable is ignored.
        @param fName: field Name. For backward compatibility
        @param val: fName val. For backward compatibility
        @param recIndex: record number to set in _dataList. recIndex >= dataLen, adds a record at the end of the list.
        @param kwargs: names and values of fields to update in DataTable.
        @return: Success: True/False None: nothing written
        """
        # if fldID:
        #     try:
        #         fldID_idx = self.getFldIndex('fldID')
        #         recIndex = next((i for i in self._dataList if self._dataList[i][fldID_idx] == fldID), None)
        #     except(TypeError, IndexError, ValueError, AttributeError):
        #         return None
        #     else:
        #         if recIndex is None:
        #             return None

        if self.__isValidFlag:
            if kwargs:
                if abs(recIndex) >= self.dataLen:   # Si recIndex >= dataLen, hace append de 1 registro
                    recIndex = self.dataLen
                    newRec = [None] * self.fldNamesLen
                    self._dataList.append(newRec)
                if self.dataLen:
                    for i in kwargs:
                        fName = i.strip()
                        if fName in self._fldNames:  # Si field name no esta en _fldNames, sale con False.
                            self._dataList[recIndex][self.getFldIndex(fName)] = kwargs[i]
                    return True
                else:
                    retValue = None

            else:  # Este else es por compatibilidad con versiones anteriores de setVal()
                if abs(recIndex) >= self.dataLen:
                    recIndex = self.dataLen
                    newRec = [None] * self.fldNamesLen
                    self._dataList.append(newRec)  # Si recIndex llego al final, hace append de 1 registro
                fName = str(fName).strip()
                if self.dataLen:
                    retValue = {}
                    if fName in self._fldNames:  # Si field name no esta en _fldNames, sale con False.
                        for j, name in enumerate(self.fldNames):  # ACTUALIZA valor val en registro existente
                            if name == fName:
                                self._dataList[recIndex][j] = val  # Actualiza valor del campo  fName en _dataList.
                                retValue[name] = val
                                break
                else:
                    retValue = None
        else:
            retValue = None
            krnl_logger.info(f'ERR_INP_Invalid DataTable {self.tblName}. {callerFunction()}({lineNum()}).')
        return retValue


    def getVal(self, recIndex=0, fName=None, defaultVal=None, *, fldID=None):
        """
        Gets the val for field fName, in the recIndex record of table _dataList. if fname == '*' returns the whole
        record, as a dictionary. if fname is not found or recIndex out of range returns None
        @param fName: Field Name whose value is to be retrieved. '*': Returns full record at recIndex, as a dictionary
        @param defaultVal: Value to return if Return Value is None (Default=None)
        @param recIndex: Record index to _dataList. recIndex == -1: Pulls the LAST record of _dataList
        @param fldID: gets record that contains fldID value.
        @return: fName val on success; if fName = '*': complete record at recIndex, as a dictionary.
        Empty (None) value field: defaultVal. None: invalid Parameters
        """
        if fldID:
            try:
                fldID_idx = self.getFldIndex('fldID')
                recIndex = next((i for i in range(self.dataLen) if self._dataList[i][fldID_idx] == fldID), None)
            except(TypeError, IndexError, ValueError, AttributeError):
                return None
            else:
                if recIndex is None:
                    return None

        fldName = str(fName).strip()
        if self.dataLen and abs(recIndex) < self.dataLen and self._dataList[recIndex]:
            if fldName in self._fldNames:
                return self._dataList[recIndex][self.getFldIndex(fldName)]
            elif fldName == '*':
                return self.unpackItem(recIndex)  # Retorna Diccionario
        return defaultVal


    def appendRecord(self, **kwargs):        # TODO(cmt): _dataList DEBE mantener siempre  el record length.
        """
        adds a record after the last record in _dataList with all valid values found in kwargs dict. Values not passed
        are set to None.
        @param kwargs: Dictionary {fName: fldValue, }  values in None are ignored.
        @param args: for future development
        @return: NADA
        """
        newRec = [None] * self.fldNamesLen  # se crea un registro vacio (todos los values en None)
        if kwargs:
            for i in kwargs:            # OJO: fldNames en kwargs pueden ser ad-hoc (no definidos DB)
                a = self.getFldIndex(i)
                if a is not None:           # Solo obvia None. 0, False se deben incluir.
                    newRec[a] = kwargs[i]
        self._dataList.append(newRec)
        return None

    def popRecord(self, recIndex=None, *, fldID=None):
        """ Removes record with index recIndex from dataList
        *** fldID, if passed, takes precedence over recIndex. ***
        @return: dataList[recIndex] if valid. None if dataList[recIndex] not found / not valid.
        """
        if fldID:
            try:
                fldID_idx = self.getFldIndex('fldID')
                recIndex = next((i for i in range(self.dataLen) if self._dataList[i][fldID_idx] == fldID), None)
            except(TypeError, IndexError, ValueError, AttributeError):
                pass

        if recIndex is not None:
            try:
                return self._dataList.pop(recIndex)
            except (IndexError, ValueError, TypeError):
                pass
        return None

    def getRecordIndex(self, fldID=None):
        """ Returns index j of dataList structure when passed the fldID value of the record.
        @return: index (int) or None if fldID is not found in dataList.
        """
        fldID_idx = self.getFldIndex('fldID')
        try:
            return next((j for j in range(self.dataLen) if self._dataList[j][fldID_idx] == fldID), None)
        except(IndexError, TypeError, ValueError):
            return None


    def insertCol(self, colName=None, val=None, *, ignore_db_names=False):
        """ Inserts column colName in DataTable if colName is a valid column name and is not yet part of the table.
            Initializes all records to val or None if val not provided.
            ignore_db_names=True->inserts column with name provided (regardless of colName being a valid db field name)
            @return: True: column inserted in DataTable. False: column not inserted.
        """
        try:
            colName = colName.strip()
        except (AttributeError, TypeError):
            return False
        fullFldNames = getFldName(self.tblName, '*', 1)  # self.tblName es un nombre de tabla ya validado.
        if colName not in fullFldNames and ignore_db_names is False:
            return False

        self._fldNames.append(colName)
        self.__dbFldNames.append(fullFldNames.get(colName, 'nonDB_' + colName))  # colName no esta en DB, agrega nonDB_
        self.__fldNamesLen = len(self._fldNames)  # if hasattr(self._fldNames, '__iter__') else 0
        self.__fldIndices = dict.fromkeys(self.__fldMap.keys())
        for k in self.__fldIndices:
            self.__fldIndices[k] = self._fldNames.index(k)
        self.__fldUIDs.update(**{colName: self._tblName + uidCh + colName})  # Agrega UIDs de colName
        self.__uidFldMap = dict(zip(self._fldNames, self.__fldUIDs))
        for j in self.dataList:
            if isinstance(j, list):
                j.append(val)       # Setea val en todos los registros correspondientes a la nueva columna.
        return True


    def getIntervalRecords(self, filterFldName, sValue='', eValue='', mode=0, **kwargs):
        """
        Gets the records between sValue and eValue. if sValue=eValue='' -> Last Record
        Returns a record based on the logic of Python's max(). So careful with its use: the fields picked must make
        sense for a max() operation.
        @param filterFldName: Filter Field Name to filter obj_data from. Mandatory. Must be datetime obj, int, float
        or othe type that can define a range.
        @param sValue: start Value. 0 or '0': get FIRST record. sValue=eValue='': get LAST record
        @param eValue: end Value. 0 or '0': get FIRST record. sValue=eValue='': get LAST record
        @param mode: 0: int, float, str and anything that is not a date in DB string format
                     1: Date(str) in the DB format 'YYYY-MM-DD HH:MM:SS XXXXXX'
        @param kwargs: fldName=fldValue -> Filters all records previously selected that meet the conditions
                       fldName=fldValue. USES OR LOGIC by default. AND logic to be implemented if ever required.
        @return: DataTable Object with Records meeting search criteria. EMPTY tuple if not found or MAX() not supported.
         DataTable construct.: __init__(obj, tblName, dList=None, keyFieldNames=None, dbFieldNames=None):
        """
        if self.isValid:
            fldName = str(filterFldName).strip()
            if fldName in self._fldNames:
                fldIndex = self.getFldIndex(fldName)
                indexVector = []  # Array con values de fldName. TODOS los records de la tabla. Para filtrar luego.
                for i in range(self.dataLen):
                    if self._dataList[i]:
                        value = self._dataList[i][fldIndex]        # TODO: value aqui DEBE ser datetime.
                        if mode == 1 and not isinstance(value, datetime):
                            continue                   # Ignora si fecha no es valida
                        indexVector.append(value)  # array temporal para ejecutar el max()/min(),index().

                if indexVector:
                    if sValue in (None, '') and eValue in (None, ''):     # LAST RECORD
                        try:
                            searchIndex = indexVector.index(max(indexVector))
                        except (ValueError, TypeError):
                            return {}
                        retList = [self.dataList[searchIndex], ]  # Debe crear lista de listas para que tutto funcione
                        retValue = DataTable(self.tblName, retList, self.fldNames, self.dbFldNames)

                    elif sValue in (0, '0') and eValue in (0, '0'):  # FIRST RECORD
                        try:
                            searchIndex = indexVector.index(min(indexVector))
                        except (ValueError, TypeError):
                            return {}
                        retList = [self.dataList[searchIndex], ]  # Debe crear lista de listas para que tutto funcione
                        retValue = DataTable(self.tblName, retList, self.fldNames, self.dbFldNames)

                    else:
                        startValue = datetime.strptime(sValue, fDateTime) if mode == 1 else sValue
                        endValue = datetime.strptime(eValue, fDateTime) if mode == 1 else eValue
                        selectVector = []  # Vector con indices de los registros de dataList a agregar
                        retList = []  # Filtered List of records to return.
                        if startValue > endValue:
                            for j in range(len(indexVector)):  # Ignora endValue si es menor que sValue
                                if indexVector[j] >= startValue:
                                    selectVector.append(j)
                        else:
                            for j in range(len(indexVector)):  # Va entre start Date y End Date
                                if startValue <= indexVector[j] <= endValue:
                                    selectVector.append(j)

                        for i in range(len(selectVector)):
                            retList.append(self.dataList[selectVector[i]])
                        if len(selectVector) <= 1:
                            retList = [retList, ]  # Debe crear lista de listas para que tutto funcione
                        retValue = DataTable(self.tblName, retList, self.fldNames, self.dbFldNames)
                else:
                    retValue = DataTable(self.tblName, [], self.fldNames, self.dbFldNames)
            else:
                retValue = DataTable(self.tblName, [], self.fldNames, self.dbFldNames)
        else:
            retValue = DataTable(self.tblName, [], self.fldNames, self.dbFldNames)

        # Filtra registros resultantes por los valores key=fldValue pasados en kwargs
        if kwargs and retValue.dataLen:     # and any(len(retValue.dataList[j]) > 0 for j in range(retValue.dataLen)):
            result = DataTable(retValue.tblName, [], retValue.fldNames, retValue.dbFldNames)
            for key in kwargs:
                k = key.strip()
                if k in retValue.fldNames:
                    for j in range(retValue.dataLen):
                        if retValue.dataList[j][k] == kwargs[key]:
                            result.appendRecord(**retValue.unpackItem(j))
            if result.dataLen:
                retValue = result
        return retValue

    def getCol(self, fldName=None):
        """
        Returns the obj_data "column" corresponding to fName. If fName is not valid retuns None. If no field name is
        provided, returns None.
        @param fldName:
        @return: list with column obj_data corresponding to field name.EMPTY list if nothing is found.
        """
        retValue = []
        if self.isValid:
            fldName = str(fldName).strip()
            fldIndex = self.getFldIndex(fldName)
            if fldIndex is not None:  # TODO(cmt): fldIndex CAN be 0 ==> When fldName not valid getFldIndex returns None
                retValue = [j[fldIndex] for j in self.dataList]
                # for j in range(self.dataLen):
                #     val = self._dataList[j][fldIndex] if self._dataList[j] else None
                #     retValue.append(val)  # Logica p/ que los indices de getCol sean identicos a los de _dataList.
                    # Si hay records de _dataList vacios ([]), ese item se completa con None
        return retValue

    def getCols(self, *args):
        """
        Returns the obj_data "columns" corresponding to fNames. If fName is not valid returns None. If no field name is
        provided, returns ().
        @param args: Field Names, comma-separated
        @return: Tuple ([fldValues, ], fldName2: [fldValues, ]) - () if nothing is found.
        """
        retValue = []
        if self.isValid and args:
            for fld in args:
                fldName = str(fld).strip()
                if fldName in self._fldNames:
                    fldIndex = self.getFldIndex(fldName)
                    if fldIndex is not None:
                        lst = []
                        for j in range(self.dataLen):
                            val = self._dataList[j][fldIndex] if len(self._dataList[j]) > 0 else None
                            lst.append(val)
                        retValue.append(lst)
        return tuple(retValue)

    __linkTables = ('tblLinkAnimalesActividades', 'tblLinkCaravanasActividades', 'tblLinkPersonasActividades',
                    'tblLinkDispositivosActividades')

    def packTable(self):            # Deprecated. No longer needed.
        return dict(zip(self.__xferableItems, [self.__getattribute__(j) for j in self.__xferableItems]))

    # @classmethod
    # def unpackTable(cls, tbl_dict):
    #     """Returns a DataTable object created from parameters passed in tbl_dict,or error if parameters are not valid"""
    #     # TODO: Implementar sin parametros de la tabla. Solo parametros externos como operation (si hace falta).
    #     # print(f"{tbl_dict.get('_tblName')}, {tbl_dict.get('_dataList')}, {tbl_dict.get('_fldNames')}, "
    #     #       f"{tbl_dict.get('_operation')}")
    #     temp = DataTable(tbl_dict.get('_tblName'), tbl_dict.get('_dataList'), tbl_dict.get('_fldNames'),
    #                      _operation=tbl_dict.get('_operation'))
    #     return temp if temp.isValid else f'ERR_SYS_Invalid Arguments. DataTable {tbl_dict.get("_tblName")} not created.'
    #
    # @classmethod
    # def unpackTableOriginal(cls, tbl_dict):
    #     """Returns a DataTable object created from parameters passed in tbl_dict,or error if parameters are not valid"""
    #
    #     # print(f"{tbl_dict.get('_tblName')}, {tbl_dict.get('_dataList')}, {tbl_dict.get('_fldNames')}, "
    #     #       f"{tbl_dict.get('_operation')}")
    #     temp = DataTable(tbl_dict.get('_tblName'), tbl_dict.get('_dataList'), tbl_dict.get('_fldNames'),
    #                      _operation=tbl_dict.get('_operation'))
    #     return temp if temp.isValid else f'ERR_SYS_Invalid Arguments. DataTable {tbl_dict.get("_tblName")} not created.'
    #

    @staticmethod
    def __writeScheduler(func):  # wrapper para lanzar setRecords() en multiple threads.
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if not isinstance(self, DataTable) or func.__name__ not in self.__func_names_strings:
                krnl_logger.error(f'ERR_SYS_Invalid function call {func.__name__}.')
                return f'ERR_SYS_Invalid function call {func.__name__}.'
            if self.dataLen <= self.__SINGLE_BLOCK_SIZE or self._writeObj.is_stopped():
                return func(self, *args, **kwargs)

            # Crea threads y las lanza si dataLen > __SINGLE_BLOCK_SIZE
            # Numero de threads a generar = numero de blocks.
            wrtBlockSize = self.__THREAD_WRT_BLOCK_SIZE if \
                int(self.dataLen / self.__THREAD_WRT_BLOCK_SIZE) <= self.__MAX_WRT_CONCURRENT_THREADS else \
                int(self.dataLen / self.__MAX_WRT_CONCURRENT_THREADS)
            number_of_blocks = int(self.dataLen / wrtBlockSize) + (1 if self.dataLen % wrtBlockSize else 0)
            arg_kwargs = []
            for j in range(number_of_blocks):
                min_idx = j * wrtBlockSize
                max_idx = (min_idx + wrtBlockSize) if j < number_of_blocks - 1 else self.dataLen
                arg_kwargs.append({'__min_idx': min_idx, '__max_idx': max_idx})

            retValue = []
            with ThreadPoolExecutor(max_workers=number_of_blocks + 1) as executor:
                try:
                    for result in executor.map(lambda d: func(self, **d), arg_kwargs, timeout=10):  # Timeout: 10 secs
                        print(f'***** result ThreadPoolExecutor = {result}')
                        retValue.append(result)   # Ahora setRecords() retorna int. Antes era: retValue.extend(result)
                except (Exception, TimeoutError) as e:
                    val = f'ERR_SYS_Thread execute failure: {e}'
                    retValue.append(val)
                    krnl_logger.error(val)
                # finally:
                #     retValue.sort()   # retValue es lista de ints con total de records escritos por cada thread. No hace falta sort().
            return retValue

        # Codigo debajo de esta linea se ejecuta una unica vez al inicializar. Son atributos de __writeScheduler.
        return wrapper


    # @staticmethod
    # def __writeScheduler00(func):  # wrapper para lanzar setRecords() en multiple threads.
    #     @functools.wraps(func)
    #     def wrapper(self, *args, **kwargs):
    #         if not isinstance(self, DataTable) or func.__name__ not in self.__func_names_strings:
    #             krnl_logger.error(f'ERR_SYS_Invalid function call {func.__name__}.')
    #             return f'ERR_SYS_Invalid function call {func.__name__}.'
    #         if self.dataLen <= self.__SINGLE_BLOCK_SIZE or self._writeObj.is_stopped():
    #             return func(self, *args, **kwargs)
    #
    #         # Crea threads y las lanza si dataLen > __SINGLE_BLOCK_SIZE
    #         # Numero de threads a generar = numero de blocks.
    #         wrtBlockSize = self.__THREAD_WRT_BLOCK_SIZE if \
    #             int(self.dataLen / self.__THREAD_WRT_BLOCK_SIZE) <= self.__MAX_WRT_CONCURRENT_THREADS else \
    #             int(self.dataLen / self.__MAX_WRT_CONCURRENT_THREADS)
    #         number_of_blocks = int(self.dataLen / wrtBlockSize) + (1 if self.dataLen % wrtBlockSize else 0)
    #         arg_kwargs = []
    #         for j in range(number_of_blocks):
    #             min_idx = j * wrtBlockSize
    #             max_idx = (min_idx + wrtBlockSize) if j < number_of_blocks - 1 else self.dataLen
    #             arg_kwargs.append({'__min_idx': min_idx, '__max_idx': max_idx})
    #
    #         retValue = []
    #         try:
    #             with ThreadPoolExecutor(max_workers=number_of_blocks + 1) as executor:
    #                 for result in executor.map(lambda i: func(self, **i), arg_kwargs, timeout=10):  # Timeout: 10 secs
    #                     print(f'***** result ThreadPoolExecutor = {result}')
    #                     retValue.append(result)  # Ahora setRecords() retorna int. Antes era: retValue.extend(result)
    #                 retValue.sort()
    #         except (Exception, TimeoutError) as e:
    #             retValue = f'ERR_SYS_Thread execute failure: {e}'
    #             krnl_logger.error(retValue)
    #         return retValue
    #
    #     # Codigo debajo de esta linea se ejecuta una unica vez al inicializar. Son atributos de __writeScheduler.
    #     return wrapper

    # @timerWrapper(iterations=50)
    @__writeScheduler
    def setRecords(self, **kwargs):
        # TODO(cmt): Tiempo de ejecucion: 2 mseg p/ escritura (contra tiempo de setRecord de 1 registro = 3 mseg.)
        """
        Funcion para escribir multiples registros pertenecientes a una UNICA TABLA. USA sqlite3.executemany().
        Returns list of cursors for INSERTed or UPDATEd records. The application must pull the recordID from the
        cursors if needed
            fldID=None, operation is INSERT.
            fldID<>None, operation is UPDATE. Must provide a VALID fldID.
        Important: calls _execute() function which in turn call sqlite3.execute(). Does NOT use executemany().
        Important: All fields in all records MUST be valid. (except for fldID in INSERT. There, fldID=None)
        @param async_data: False-> Blocks caller,wait for return data from write spooler. NOT IMPLEMENTED FOR NOW...
                           True (Default)-> Caller NOT blocked, no waiting for return data
        @param kwargs: min_idx, max_idx: Internos. Usados por unicamente por __writeScheduler
        @return: cur.rowcount: # of records processed. None: Error/Invalid arguments
        """
        if not self.dataLen:
            retValue = f'ERR_Empty Table {self.tblName} - Nothing to write or invalid table {callerFunction()}'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return None

        dbTblName = self.dbTblName
        db_fldID = self.getDBFldName('fldID')
        # Indices p/ manejo de escritura de datos con multi-threading (para el caso de dataList muy grandes)
        if kwargs.get('__max_idx') is None:
            min_idx = 0
            max_idx = self.dataLen
        else:
            min_idx = kwargs['__min_idx']  # Here, data has been split in chunks and will be written in multiple threads
            max_idx = kwargs['__max_idx']   # __min_idx, __max_idx DataTable indices, NOT fldID. Caution with this!!
            # print(f'setRecords - __min_idx={kwargs["__min_idx"]}, __max_idx={kwargs["__max_idx"]}')
        # ---------------------------------- 2 lineas sin testear --------------------------------------- #
        if 'fldID' not in self.fldNames:  # para cuando no se pasa fldID en DataTable. TODO: VERIFICAR funcionamiento..
            self.insertCol('fldID', None)   # insertCol() actualiza dbFieldNames list.
        # ----------------------------------------------------------------------------------------------- #

        record0 = self.dataList[min_idx] if self.dataLen else []   # dataList = [[record1], [record2],...[record_n]]
        fldID_idx = self.getFldIndex('fldID')  # indice de campo fldID en dbFldNames, para pasar a _execute()
        if isinstance(record0[fldID_idx], int) and record0[fldID_idx] > 0:  # TODO(cmt):**** Es UPDATE ****
            db_operation = 'UPDATE'
            for j in range(min_idx, max_idx):   # Append fldID value al final de cada record, para asignar al WHERE.
                # Agrega 1 columna con fldID al final del record 'j' para el WHERE "{db_fldID}"
                self.dataList[j].append(self.dataList[j][fldID_idx])
            strSQL = f' UPDATE "{dbTblName}" SET '
            for i in self.dbFldNames:
                strSQL += f'"{i}"=?, '
            strSQL = strSQL[:-2] + f' WHERE "{db_fldID}"=? ; '
            print(f'*** UPDATE *** DataTable.setRecords() strSQL: {strSQL}', dismiss_print=DISMISS_PRINT)
            print(f'*** UPDATE *** fldID_idx = {fldID_idx} / {self.tblName} dataList={self.dataList}',
                  dismiss_print=DISMISS_PRINT)
        else:                                 # TODO(cmt): ********* Es INSERT *********
            db_operation = 'INSERT'
            # Setea campos fldID en cada record, por si no fueron seteados (Operacion INSERT).
            qMarks = ' (' + (len(self.dbFldNames) * ' ?,')[:-1] + ') '
            strSQL = f' INSERT INTO "{dbTblName}" ' + str(tuple(self.dbFldNames)) + ' VALUES ' + qMarks + '; '

            # print(f'*** INSERT *** DataTable.setRecords() strSQL: {strSQL} / sqlParams: {sqlParams}', dismiss_print=DISMISS_PRINT)
        sqlParams = [self.dataList[i] for i in range(min_idx, max_idx)]
        # # Seteo especifico de NULL: cuando se necesita setear un campo a NULL, que que arriba se saltan los None.
        # for j in sqlParams:
        #     for i in range(len(j)):     # TODO(cmt): this test of NULL is too expensive. Will not be implemented.
        #         if j[i] == NULL:
        #             j[i] = None
        cur = self._writeObj.executemany_sql(strSQL, sqlParams, tbl=self.tblName, fldID_idx=fldID_idx)
        # TODO(cmt): Al retornar de executemany(), sqlParams tiene TODOS sus campos fldID actualizados c/ datos escritos
        #  en db. Estos se usan para poblar fldID de los registros de uploadRecords (abajo). 21Jul23:
        #  Todo este yara-yara de arriba (y lo de abajo) ya no es necesario si se usa una replicator app.
        #  Se hace enqueue desde aqui porque se conocen  los objetos y la operacion. Lo mismo hara el Server con el
        #  cursor que haga push a todos los Clients.
        if self.__upload_to_server and self.tblName not in _upload_exempted and not _uploadBuffer.is_stopped():
            # TODO: Send records to async queue. Later the load to DB will be done in the background using setRecords()
            if 'fldBitmask' not in self.fldNames:
                # Inserts Bitmask col in case the col has not been uploaded in the DataTable definition.
                self.insertCol('fldBitmask', self.__table_default_bitmask)      # Necesario para que tuple(self.dbFldNames) sea correcto.
                bitmask_idx = self.getFldIndex('fldBitmask')
                for j in sqlParams:
                    j.append(self.__table_default_bitmask)   # fldBitmask DEBIERA ser el ultimo campo. Asi, esto anda.
            else:
                bitmask_idx = self.getFldIndex('fldBitmask')
            #  -> timeStampSync lo setea el bkgd thread cuando envia el record al server o a otro device.
            if self.__db_sync or self.__upload_to_server:
                for j in sqlParams:
                    if j[bitmask_idx] is None:
                        j[bitmask_idx] = BIT_UPLOAD | BIT_SYNC
            # TODO(cmt): ALL records arrive here with fldID set in sqlParams so writes directly to _Upload_To_Server.
            uploadRecords = [[None, MAIN_DB_NAME, self.dbTblName, tuple(self.dbFldNames), j, db_operation,
                              self.__table_default_bitmask, None, None]
                             for j in sqlParams if j[bitmask_idx] & BIT_UPLOAD]
            _uploadBuffer.enqueue(uploadRecords)         # Lista de records que setRecords() procesara en bloque.
        return cur.rowcount     # cur.rowcount = 0 si funcion falla. Se usa esto para validar escritura.

# ----------------------------------------------- END DataTable ------------------------------------------------ #



class Amount(Money):
    """
    By design Amount objects cannot be modified after created via __init__(). Then all amounts created become read-only.
    In this sense, Amount objects become immutable.
    Inherits from Money, to use all the operands and structures already defined there.
    1:N --> 1 TM Activity : MULTIPLE TM Transactions.
    1:1 --> 1 TM Transaction : 1 target_obj_dict
    1:N --> 1 TM Transaction : MULTIPLE Amount objects.
    """
    __slots__ = ('__ID', '__transaction', '__monetaryInstrument', '__recordID', '_fldComment')
    __tblRAName = 'tblTMRegistroDeActividades'
    __tblMontosName = 'tblDataTMMontos'
    __tblCurrencyName = 'tblMonedasNombres'
    __tblLinkName = 'tblLinkMontosActividades'
    __tblObjectsName = __tblRAName
    __tblRA = DataTable(__tblRAName)
    __tblMontos = DataTable(__tblMontosName)
    __currenciesDict = {}               # {curName: (curCode, curEntity)}

    @classmethod
    def loadCurrencies(cls):
        temp = getRecords('tblTMMonedasNombres', '', '', None, 'fldID', 'fldCurrency', 'fldCurrencyName', 'fldCode',
                          'fldEntity', 'fldMinorUnit')
        if isinstance(temp, str):
            raise DBAccessError(temp)
        for j in range(temp.dataLen):   # {curName('XXX'): (Currency Name, Code, Entity/Country)}
            cls.__currenciesDict[temp.dataList[j][1]] = (temp.dataList[j][2], temp.dataList[j][3], temp.dataList[j][4])

    @classmethod
    def getCurrenciesDict(cls):
        return cls.__currenciesDict

    def __init__(self, amt, cur=None, *, transact_id=None, amount_id=None, monetary_instr=None, record_id=None):
        if isinstance(amt, (int, float)):
            amt = str(amt)
        super().__init__(amt, cur.upper().strip() if isinstance(cur, str) else defaultCurrencyName)  # Crea obj. Amount.

        # crea UUID para el objeto Amount (amount_id si es valido, o genera uuid nuevo)
        if isinstance(amount_id, UUID):
            self.__ID = str(amount_id.hex)
        elif isinstance(amount_id, str):
            try:
                _ = UUID(amount_id)
            except(ValueError, TypeError, SyntaxError):
                self.__ID = str(uuid4().hex)
            else:
                self.__ID = amount_id
        else:
            self.__ID = str(uuid4().hex)

        self.__transaction = transact_id     # TODO: Transaction can be passed here, or set later via setTransaction().
        self.__monetaryInstrument = monetary_instr      # Type of monetary instrument, if applicable (cash, check, etc).
        self.__recordID = record_id  # database record fldID value, for ease of access.
        self._fldComment = ''                       # For compatibility with fields in Montos table.


    @property
    def ID(self):
        return self.__ID

    @property
    def recordID(self):
        return self.__recordID

    @recordID.setter
    def recordID(self, val):
        if isinstance(val, int):
            self.__recordID = val

    @property
    def transaction(self):
        return self.__transaction

    def setTransaction(self, val):
        if isinstance(val, Transaction):
            self.__transaction = val

    @classmethod
    def tblMontosName(cls):
        return cls.__tblMontosName

    @classmethod
    def tblCurrencyName(cls):
        return cls.__tblCurrencyName


    # def __hash__(self):
    #     return super().__hash__()
    #
    def __repr__(self):
        return "{} {}".format(self._currency, self._amount)

    def __str__(self):
        return "{} {:,.10f}".format(self._currency, self._amount)

    # def __lt__(self, other):
    #     if not isinstance(other, Money):
    #         raise InvalidOperandType(other, '<')
    #     elif other.currency != self._currency:
    #         raise CurrencyMismatch(self._currency, other.currency, '<')
    #     else:
    #         return self._amount < other.amount
    #
    # def __le__(self, other):
    #     if not isinstance(other, Money):
    #         raise InvalidOperandType(other, '<=')
    #     elif other.currency != self._currency:
    #         raise CurrencyMismatch(self._currency, other.currency, '<=')
    #     else:
    #         return self._amount <= other.amount
    #
    # def __eq__(self, other):
    #     if isinstance(other, Money):
    #         return ((self._amount == other.amount) and
    #                 (self._currency == other.currency))
    #     return False

    # def __ne__(self, other):
    #     return not self == other
    #
    # def __gt__(self, other):
    #     if not isinstance(other, Money):
    #         raise InvalidOperandType(other, '>')
    #     elif other.currency != self._currency:
    #         raise CurrencyMismatch(self._currency, other.currency, '>')
    #     else:
    #         return self._amount > other.amount
    #
    # def __ge__(self, other):
    #     if not isinstance(other, Money):
    #         raise InvalidOperandType(other, '>=')
    #     elif other.currency != self._currency:
    #         raise CurrencyMismatch(self._currency, other.currency, '>=')
    #     else:
    #         return self._amount >= other.amount
    #
    # def __bool__(self):
    #     """
    #     Considering Money a numeric type (on ``amount``):
    #
    #     bool(Money(2, 'XXX')) --> True
    #     bool(Money(0, 'XXX')) --> False
    #     """
    #     return bool(self._amount)

    def __add__(self, other):
        obj = super().__add__(other)
        return self.__class__(obj.amount, obj.currency)

    def __radd__(self, other):
        obj = super().__radd__(other)
        return self.__class__(obj.amount, obj.currency)

    def __sub__(self, other):
        obj = super().__sub__(other)
        return self.__class__(obj.amount, obj.currency)

    def __rsub__(self, other):
        obj = super().__rsub__(other)
        return self.__class__(obj.amount, obj.currency)

    def __mul__(self, other):
        obj = super().__mul__(other)
        return self.__class__(obj.amount, obj.currency)

    def __rmul__(self, other):
        obj = super().__rmul__(other)
        return self.__class__(obj.amount, obj.currency)

    def __truediv__(self, other):
        obj = super().__truediv__(other)
        return self.__class__(obj.amount, obj.currency)

    def __floordiv__(self, other):
        obj = super().__floordiv__(other)
        return self.__class__(obj.amount, obj.currency)

    def __mod__(self, other):
        obj = super().__mod__(other)
        return self.__class__(obj.amount, obj.currency)

    def __divmod__(self, other):
        obj1, obj2 = super().__divmod__(other)
        return self.__class__(obj1.amount, obj1.currency), self.__class__(obj2.amount, obj2.currency)

    def __pow__(self, other):
        obj = super().__pow__(other)
        return self.__class__(obj.amount, obj.currency)

    def __neg__(self):
        obj = super().__neg__()
        return self.__class__(obj.amount, obj.currency)

    def __pos__(self):
        obj = super().__pos__()
        return self.__class__(obj.amount, obj.currency)

    def __abs__(self):
        obj = super().__abs__()
        return self.__class__(obj.amount, obj.currency)

    def __int__(self):
        return int(self._amount)

    def __float__(self):
        return float(self._amount)

    def __round__(self, ndigits=0):
        obj = super().__round__(ndigits)
        return self.__class__(obj.amount, obj.currency)

    def __composite_values__(self):
        return self._amount, self._currency

    @classmethod
    def fetch_obj(cls, amt_uid=None):
        """ Pulls the record identified by amt_uid from table [Data TM Montos] and creates an Amount object.
        @return: Amount object.
                 None if amt_uid not found or error reading table.
        """
        if not amt_uid or not isinstance(amt_uid, str):
            return None
        temp = getRecords(cls.__tblMontosName, '', '', None, '*', fldObjectUID=amt_uid)
        if isinstance(temp, str) or not temp.dataLen:
            return None
        d = temp.unpackItem(0)
        return cls(d.get('fldAmount'), d.get('fldCurrency'), record_id=d.get('fldID'), amount_id=d.get('fldObjectUID'),
                   transact_id=d.get('fldFK_Transaction'), monetary_instr=d.get('fldFK_InstrumentoMonetario'))

    @classmethod
    def getObject(cls, val: str):
        """ Pulls the record identified by amt_uid from table [Data TM Montos] and creates an Amount object.
        Defined for compatibility reasons.
       @return: Amount object.
                None if amt_uid not found or error reading table.
       """
        cls.fetch_obj(val)


    def record(self):
        """
        Records the amount object self in database.
        @return: True if Amount created, or raise Exception if error in db access.
        """
        idAmount = setRecord(self.__tblMontosName, fldAmount=self.amount, fldCurrency=self.currency,
                             fldID=self.recordID or None, fldObjectUID=self.ID,
                             fldFK_Transaccion=self.__transaction.ID if isinstance(self.__transaction, Transaction)
                             else None,
                             fldFK_InstrumentoMonetario=self.__monetaryInstrument or None,
                             fldComment=self._fldComment or "")
        if isinstance(idAmount, str):
            val = f'ERR_DB_Access: cannot write to table {self.__tblMontosName}. error: {idAmount}'
            krnl_logger.error(val)
            raise DBAccessError(val)
        if not self.__recordID:
            self.__recordID = idAmount        # Updates ID field in object if it was none.
        return True


    @classmethod
    def create_obj(cls, amt=None, cur=None):
        """ Creates an Amount object and stores it in database.
        Objects created here will not have a Transaction assigned.
        @return: Amount object.
        """
        obj = cls(amt, cur)
        obj.record()
        return obj          # Returns object with amount, currency, ID and recordID set. Transaction NOT set.

    def amend(self, amt, cur):
        """ Amends (updates) amount and currency values in calling Amount object (self). Stores in database.
        Meant to be used in case an error is made in amounts/currencies when creating the Amount object.
        Both amt AND cur must be provided. No default values in this func.
        @return: new Amount object or None if amendment failed.
        """
        try:
            new_amt = self.__class__(amt, cur)  # Creates new Amount with passed amt, cur.
        except(ValueError, TypeError):
            return None
        else:
            new_amt.__ID = self.__ID            # Assigns all same values as self and records in db.
            new_amt.__recordID = self.recordID
            new_amt.__monetaryInstrument = self.__monetaryInstrument
            new_amt.__transaction = self.__transaction
            new_amt._fldComment = self._fldComment
            new_amt.record()            # Stores the new Amount values in database updating record pointed to by self.ID
            return new_amt              # Must return new object


# ----------------------------------------------- END Class Amount --------------------------------------------------- #

class Transaction(object):
    __tblRAName = 'tblTMRegistroDeActividades'
    __tblMontosName = 'tblDataTMMontos'
    __tblCurrencyName = 'tblMonedasNombres'
    __tblDataName = 'tblDataTMTransacciones'
    __tblObjectsName = __tblRAName
    __tblRA = DataTable(__tblRAName)
    __tblMontos = DataTable(__tblMontosName)
    # TODO: Define "Transaction Activity" as a col. in TM Actividades Nombres. Load as dict to validate fldfk_Actividad
    #  (fldFlagTransactionActivity is the name of the implemented flag field)

    def __init__(self, **kwargs):
        self.__ID = kwargs.get('fldID')
        self.__activityID = kwargs.get('fldFK_Actividad')
        if not self.__ID or not isinstance(self.__ID, int) or not self.__activityID:
            raise ValueError('ERR_INP_ValueError: Cannot create object Transaction. Mandatory parameters missing.')

        self.__isValid = True
        self._description = kwargs.get('fldDescription', '')
        self._transactionID = kwargs.get('fldTransactionID')
        self._eventDate = kwargs.get('fldDate', time_mt('datetime'))
        self._personName = kwargs.get('fldName', '')
        self._personID = kwargs.get('fldThirdPtyID')
        self._contractID = kwargs.get('fldFK_Contrato')
        self._bankAccountUID = kwargs.get('fldBankAccountUID')
        self._transactionUID = kwargs.get('fldTransactionUID')
        self._supportDocs = kwargs.get('fldTransactionSupportDocs')
        self._quantity = kwargs.get('fldQuantity')
        self._units = kwargs.get('fldFK_Unidad')
        self._linkedTransaction = kwargs.get('fldFK_TransaccionDeReferencia')  # Previous transaction linked to this one
        self._comment = kwargs.get('fldComment', '')

    @property
    def ID(self):
        return self.__ID

    @property
    def isValid(self):
        return self.__isValid

    @property
    def _activityID(self):
        return self.__activityID

    def getAmountsFromTransaction(self):
        """ Gets all the Amounts associated to transaction ID. Returns list of Amount objects. """
        temp = getRecords(self.__tblMontosName, '', '', None, '*', fldFK_Transaccion=self.__ID)
        if not isinstance(temp, DataTable) or not temp.dataLen:
            return []          # Nothing found, or error reading table.
        return [Amount.fetch_obj(temp.getVal(j, 'fldObjectUID')) for j in temp.dataList]

    def getActivity(self):
        return self.__activityID

# ----------------------------------------------- END Class Transaction --------------------------------------------- #

#          &&&&&&&&&&&&&&&&  sqlite3 Adapters and Converters (that use the Amount type)  &&&&&&&&&&&&&&&&&&&
def adapt_to_uid(obj):
    """ 'Serializes' an Amount object to its ID value (UUID str) for storage in database (field AMOUNT in tblLinkMontos)
    @return: Object ID if set, or None if object doesn't have an ID.
    """
    if isinstance(obj, Amount):
        if not obj.ID:
            raise ValueError(f"ERR_ValueError: Amount object doesn't have a valid ID")  # Adapter MUST yield valid data.
        try:
            return obj.ID
        except (AttributeError, NameError, ValueError):
            krnl_logger.error(f'ERR_ValueError. Conversion error: {obj} is not a valid Amount object.')
            raise sqlite3.Error(f'ERR_ValueError. Conversion error: {obj} is not a valid Amount object.')
    return obj   # if anything other than Amount is passed, returns that thing for storage as is.

def convert_to_amount(uid_val):
    """ Converts val (UUID) to a Amount Object and returns the object. Data is retrieved from [tblMontos].
    Conversion is performed only for DB fields named 'AMOUNT' (see converter registration at the end of this file).
    @return: Amount object.
    """
    try:
        obj = Amount.fetch_obj(uid_val)
        if obj is None:
            raise ValueError('ERR_ValueError: Amount ID not found. Amount object cannot be created.')
    except (KeyError, TypeError, ValueError):
        raise ValueError(f'ERR_ValueError. Conversion error: %d is not a valid ID of Amount object.' % uid_val)
    else:
        return obj  # OJO:sqlite3 pasa val a esta func como type byte. Hay que castear a int

# Adapter/converters for Amount Objects
sqlite3.register_adapter(Amount, adapt_to_uid)    # Serializes Amount to int, to store in DB.
sqlite3.register_converter('AMOUNT', convert_to_amount)  # Converts int to Amount object fetching a record from Montos.


class JSONEncoderSerializable(json.JSONEncoder):       # sqlite3 JSON Adapter
    """ Implements serialization for non-serializable objets that are to be encoded to JSON.
    Whenever 'JSON' is found in the ColName of a column in the db this code is executed for each of the items of the
    list, tuple, set or dict to be written to db.
        Implemented conversions (all performed prior to JSON encoding):
            - datetime -> str
            - set -> tuple
            - Decimal -> str
            - Amount -> str     # int value corresponds to Amount record number (fldID) in tblMontos.
    """
    def default(self, o):           # Override de default() en clase JSONEncoder
        if isinstance(o, datetime):
            return datetime.strftime(o, fDateTime)  # datetime conversion to str
        elif isinstance(o, set):        # Tambien parece que JSON se atraganta con los sets(). No le gustan.
            return tuple(o)             # Hay que convertir los sets a algo hashable para que lo maneje el encoder.
        elif isinstance(o, Decimal):
            return str(o)   # Los Decimal de Montos se tienen que pasar a string para serializar a JSON.
        elif isinstance(o, Amount):         # TODO: conversion so that JSON conversion in DataUploadBuffer works.
            return str(o.ID)
        else:
            super().default(o)   # Code for super().default() only raises TypeError. Not sure it's required. Let's see

def adapt_to_json(data):
    try:        # TODO(cmt): OJO! Los dicts con int keys se pasan a str al convertir a JSON.
        return (json.dumps(data, cls=JSONEncoderSerializable, sort_keys=True)).encode()
    except(json.JSONDecodeError, Exception):
        raise DBAccessError('JSON encoding error: %s' % data)

sqlite3.register_adapter(set, adapt_to_json)   # Linea necesaria porque llama a JSONEncoderSerializable para los sets.
sqlite3.register_adapter(dict, adapt_to_json)  # Cuando sqlite3 recibe list, dict, tuple, set los convierte a JSON
sqlite3.register_adapter(list, adapt_to_json)
sqlite3.register_adapter(tuple, adapt_to_json)

# ================================================================================================================= #

class DBTrigger(object):
    """
    If a record with trig_name exists in DB, initializes the trigger with parameters read from DB, otherwise
    initializes with data from __init__ arguments. """
    __trigger_register = []  # Register to access all active triggers.
    __trigger_tbl_name = 'tbl_sys_Trigger_Tables'
    __trigger_db_table_name = '_sys_Trigger_Tables'
    __sql_is_running = 'select * from sqlite_master where type = "trigger" ; '

    def __init__(self, *, trig_name=None, trig_string=None, trig_type=None, process_func=None, calling_obj=None,
                 create=True):

        self.__trigger_name = trig_name
        trigger_data = self.__initialize_from_db() if trig_name else {}
        if trigger_data:
            self.__trigger_name = trigger_data.get("fldTrigger_Name")
            self._trigger_string = trigger_data.get("fldTrigger_String")
            if not isinstance(self._trigger_string, str):
                self._trigger_string = None
            self.__trigger_type = trigger_data.get("fldType", '') # duplication,  replication, etc.
            if isinstance(self.__trigger_type, str):
                self.__trigger_type = self.__trigger_type.lower()
            self.__processing_func = process_func or None       # Not read from DB
            self.__rowid = trigger_data.get("fldID")            # ROWID of the trigger assigned to this DBTrigger obj.
            self.__calling_object = calling_obj                 # Not read from DB
            self._isActive = True                               # Active or Inactive Trigger.
        else:
            self.__trigger_name = trig_name
            self._trigger_string = trig_string
            if not isinstance(self._trigger_string, str):
                self._trigger_string = None
            self.__trigger_type = trig_type.lower().strip() if isinstance(trig_type, str) else trig_type  # duplic replic, etc.
            self.__processing_func = process_func
            self._isActive = True  # Active or Inactive Trigger.
            self.__rowid = None         # ROWID of the trigger assigned to this DBTrigger obj.
            self.__calling_object = calling_obj

        self.__created_in_db = self.__create_db_trigger() if create else False
        self.__trigger_register.append(self)


    @classmethod
    def get_trigger_register(cls):
        return cls.__trigger_register

    @property
    def rowid(self):
        return self.__rowid

    @property
    def type(self):
        return self.__trigger_type

    @property
    def name(self):
        return self.__trigger_name

    @property
    def isActive(self):
        return self._isActive

    @isActive.setter
    def isActive(self, val):
        self._isActive = bool(val)

    @property
    def processing_method(self):
        return self.__processing_func

    @property
    def calling_object(self):
        return self.__calling_object

    @property
    def created_in_db(self):
        return self.__created_in_db

    @classmethod
    def get_triggers_running(cls):
        """ Returns a dictionary of the trigger_name: db_table_name for all triggers running in the db.
            @return: {trigger_name(str): db_tbl_name(str), }. Empty dict if no triggers are running.
        """
        fNames, rows = exec_sql(sql=cls.__sql_is_running)
        if rows:
            return {j[fNames.index("name")]: j[fNames.index("tbl_name")] for j in rows}
        return {}

    def is_running(self):
        """ Queries DB to check if trigger a with trigger_name is running
            @return: True / False
        """
        trigger_dict = self.get_triggers_running()
        if trigger_dict:
            trigger_list = [j.lower() for j in trigger_dict]        # all names to lower to compare trigger names.
            return self.__trigger_name.lower() in trigger_list
        return False


    def __create_db_trigger(self):
        """ Creates trigger defined by trigger_str in database.
           @return: True if created in db, False if not created.
           """
        # Basic consistency check for trigger_string.
        if isinstance(self._trigger_string, str) and self._trigger_string.lower().startswith('create '):
            qryObj = SQLiteQuery()
            cur = qryObj.execute(self._trigger_string)
            if not isinstance(cur, sqlite3.Cursor):
                db_logger.error(f'ERR_DB: Database trigger {self._trigger_string} could not be created.')
            return True
        return False

    def _drop_db_trigger(self):
        """ Creates trigger defined by trigger_str in database.
           @return: None
           """
        if self.__trigger_name:
            qryObj = SQLiteQuery()
            cur = qryObj.execute(f'DROP IF EXISTS {self.__trigger_name}; ')
            if not isinstance(cur, sqlite3.Cursor):
                db_logger.error(f'ERR_DB: Database trigger {self.__trigger_name} could not be dropped.')
        return None


    def __initialize_from_db(self):
        """ Gets record associated to this trigger object. Needed to query TimeStamp and Last_Processing.
        If trigger_name is found in _sys_Trigger_Tables, initializes the object to the parametes read from DB.
        @return: dict with parameters or {} if none found. """
        sql = f'SELECT * FROM "{self.__trigger_db_table_name}" WHERE ROWID == Flag_ROWID AND "Activa YN" > 0; '
        temp = dbRead(self.__trigger_tbl_name, sql)
        if not isinstance(temp, str) and temp:
            col_names = temp.getCol('fldTrigger_Name')
            for i, name in enumerate(col_names):
                if isinstance(name, str):
                    if name.lower() in self.__trigger_name.lower():
                        return temp.unpackItem(i)
        return {}

# --------------------------------------- End class DBTrigger -------------------------------------------------


@timerWrapper(iterations=50)
def setRecord(tblName: str, *, mode=None, **kwargs):
    """
    Funcion general para escribir campos de un registro de una tabla, pasados como parametros.
    Si se pasa 'fldID' en kwargs y kwargs['fldID'] > 0,  se actualiza el registro (UPDATE).
    Si no se pasa 'fldID' o kwargs['fldID'] not > 0, se inserta un registro (se deben incluir todos los campos NOT NULL,
    o devuelve error).
    Campos especiales: fldTimeStamp y fldFK_UserID -> Siempre se deberan setear con valores del sistema antes de pasar
        a setRecord() o cualquier otra funcion de escritura en DB. IGNORA (no escribe) valores None o NULL
    Uso:
        setRecord('tblCaravanas', fldID=395, fldFK_Color=4, fldFK_TagFormat=4) → actualiza reg. con recordID=395
        setRecord('tblCaravanas', fldTagNumber='TEST', fldFK_Color=3, fldFK_TagFormat=1) → inserta un nuevo reg.
    @param tblName: (string) KeyName de tabla
    @param kwargs: (string) KeyName de campo = (any) valor a escribir
    @param mode: Future development: INSERT OR -> REPLACE, ABORT, FAIL, IGNORE, ROLLBACK
    @return: recordID del registro (int) si se escribio en DB o errorCode (str) si hay error, None si no hay datos.
    """
    tblName = tblName.strip()
    tblInfo = getTblName(tblName, 1)  # Retorna tupla (dbTblName, tblIndex, pkAutoIncrementYN, without_rowid, db_sync)
    dbTblName = tblInfo[0]
    if strError in dbTblName:
        retValue = f'ERR_INP {moduleName()}({lineNum()}): Invalid argument {dbTblName}.'
        db_logger.warning(retValue)
        return retValue                                         # Sale si tblName no es valido.

    dbFldNames = getFldName(tblName, '*', 1)                # Retorna Diccionario {fldName: dbFldName, }
    db_fldID = dbFldNames.get('fldID', None)                # Si no hay campo fldID es tabla corrupta/no valida.
    commonKeys = set(dbFldNames).intersection(kwargs)       # Valida fldNames. Deja solo los validos para tblName
    # Arma Diccionario {dbFieldName: fldValue} con fldValues pasados en kwargs.
    wrtDict = {dbFldNames[j]: kwargs[j] for j in commonKeys if kwargs[j] is not None}   # Solo incluye los != None
    if not wrtDict or not db_fldID:
        return None

    fldID_orig = wrtDict.get(db_fldID, 0)   # TODO(cmt): valor pasado de fldID. INSERT=None; UPDATE: > 0
    if fldID_orig > 0:            # TODO(cmt): *************** UPDATE *****************
        db_operation = 'UPDATE'
        fldID_idx = None
        fldIDValue = wrtDict[db_fldID]      # valor de fldID en lista sqlParams, para pasar a execute()
        # wrtDict.pop(db_fldID)               # Quita el campo fldID para no actualizarlo en el write a DB.
        strSQL = f' UPDATE "{dbTblName}" SET '  # "" para encerrar variables porque los nombres contienen whitespace
        for i in wrtDict:
            strSQL += f'"{i}"=?, '
        strSQL = strSQL[:-2] + f' WHERE "{db_fldID}"={fldIDValue}; '
        sqlParams = list(wrtDict.values())
        # print(f'strSQL setRecord() *** UPDATE ***: {strSQL}', dismiss_print=DISMISS_PRINT)
    else:                   # TODO(cmt): ****** INSERT ******: SQL string parametrizado para insertar el nuevo record.
        db_operation = 'INSERT'
        wrtDict[db_fldID] = None       # INSERT: Crea campo fldID (Si no existe) y setea valor a None (NULL en SQLite)
        fieldsList = tuple(wrtDict.keys())
        fldID_idx = fieldsList.index(db_fldID)
        str_fieldsList = str(fieldsList)
        sqlParams = list(wrtDict.values())  # lista porque params se debe modificar en AsyncCursor.execute_sql()
        qMarks = ' (' + (len(sqlParams) * ' ?,')[:-1] + ') '
        strSQL = f' INSERT INTO "{dbTblName}" ' + str_fieldsList + ' VALUES ' + qMarks + '; '  # RETURNING ROWID;'
        # print(f'strSQL setRecord() *** INSERT ***: {strSQL} / sqlParams: {sqlParams} / fldID_idx={fldID_idx}',
        # dismiss_print=DISMISS_PRINT)

    cursor = writeObj.execute_sql(strSQL, sqlParams, tbl=tblName, fldID_idx=fldID_idx)
    if not isinstance(cursor, str) and cursor.rowcount > 0:   # rowcount = -1 -> registro no se inserto. Sale con error.
        retValue = cursor.lastrowid if fldID_idx is not None else fldID_orig   # On UPDATE NO se actualiza lastrowid
    else:
        retValue = f'ERR_DB_WriteError: Table Name: {tblName} - Error: {cursor} '
        db_logger.error(retValue)
        print(f'{retValue} - {callerFunction(getCallers=True)}', dismiss_print=DISMISS_PRINT)  # Print por ahora. Loggeo del error lo hace execute()
        raise DBAccessError(retValue)

    if tblName not in _upload_exempted and not _uploadBuffer.is_stopped():
        dbName_bitmask = getFldName(tblName, 'fldBitmask')
        bitmask_default = getTblName(tblName, 1)[4]         # Default bitmaks from Table Bitmask setting.
        bitmask_default = bitmask_default or 3          # bitmask_default no definido -> asume 3
        if dbName_bitmask not in wrtDict:
            # Si no se cargo fldBitmask en wrtDict, se crea el campo y asigna table_bitmask (default de la tabla).
            # Se agregan tambien todos los demas campos de tbl_Upload_To_Server
            wrtDict[dbName_bitmask] = bitmask_default
        bitmask = wrtDict.get(dbName_bitmask) or bitmask_default
        if bitmask & BIT_UPLOAD:
            wrtDict[db_fldID] = retValue    # TODO(cmt): Setea wrtDict para escribir fldID en tabla _Upload_To_Server.
            wrtDict[dbName_bitmask] = bitmask   # Actualiza valor de bitmask.
            sqlParams = list(wrtDict.values())
            record = [None, MAIN_DB_NAME, dbTblName, tuple(wrtDict.keys()), sqlParams, db_operation, 0, None, None]
            _uploadBuffer.enqueue(record)
    return retValue


def getRecords(tblName, sDate=None, eDate=None, condFldName=None, *args, **kwargs):        # TODO: NUEVA->Running.
    """     Implementation 21Sep22: removes Get FIRST, get LAST records. Parses date periods only.
    @param sDate: start Date.
    @param eDate: end Date. sDate = eDate = None -> Pulls all records matching the other conditions passed.
           Other values of sDate,eDate: treated as dates to pull records selecting by condFldName (must be DATE field)
    @param tblName: DB table keyname (tblCaravanas, tblDataPersonasDatos, tblDataTMTransasccionesMonetarias, etc)
    @param condFldName: fieldName de condicion. Ej. MAX('fldDate')
    @param args: "SELECT" fields for SQL String. *: All fields in _tblName
    @param kwargs: Conditions {fName:fldValue1, fldName2: fldValue2, }. If '' or None are passed as values,
    returns all records
    @return: DataTable object.
    """
    # ----------------------- 1) Parsing *argsSelect para StrSelect -----------------------------#
    tblName = str(tblName).strip()
    dbTblName = getTblName(tblName)
    if strError in dbTblName:
        print(f'{dbTblName} - Function/Method:{callerFunction()}')
        return dbTblName  # Sale si es tabla no valida
    # Si sValue == eValue =='' -> Toma ULTIMO REGISTRO para CondFldName, mas las demas condiciones pasadas en **kwargs.
    if sDate == eDate == '':
        groupOp = 'MAX'
    elif sDate == eDate == 0:
        groupOp = 'MIN'
    else:
        groupOp = None
    # strSQLSelect1Table chequea validez de tblWrite y de campos *argsSelect
    strSelect = strSQLSelect1Table(tblName, groupOp, condFldName, 0, *args)
    if strError in strSelect:
        retValue = f'ERR_INP: {strSelect} - Function/Method:{callerFunction()}'
        krnl_logger.info(retValue)
        print(retValue)
        return retValue
    strFrom = f' FROM "{dbTblName}"'

    # ----------------------- 2) Parsing de **kwargs -> Diccionario de Condiciones -----------------------------#
    strAnd = ''
    where = ' WHERE ' if kwargs else ''
    strWhere = where
    conditionsDict = strSQLConditions1Table(tblName, **kwargs)
    if conditionsDict:
        for i in conditionsDict:  # Genera string de condiciones AND con el diccionario de condiciones "conditionsDict"
            if conditionsDict[i] != '*':  # Si es * ignora el campo. Lo quita del string SQL para seleccionar todos
                if 'not null' in str(conditionsDict[i]).lower():
                    strWhere += strAnd + f"{i}" + f' IS NOT NULL '
                elif conditionsDict[i] is None or 'null' in str(conditionsDict[i]).lower():
                    strWhere += strAnd + f"{i}" + f' IS NULL '
                else:
                    strWhere += strAnd + f"{i}" + (' IN ' if '(' in str(conditionsDict[i]) else ' = ') \
                                + f"{conditionsDict[i]}"
                strAnd = ' AND '
    strWhere = strWhere if strWhere != where else where  # Si no se pasa ninguna condicion, asume * (elimina ' WHERE ')

    if sDate or eDate:
        if isinstance(sDate, datetime):
            dtStartDate = sDate
        else:
            try:
                dtStartDate = datetime.strptime(sDate, fDateTime)  # Convierte primero a datetime para validar la fecha.
            except(TypeError, ValueError):
                dtStartDate = None
        if isinstance(eDate, datetime):
            dtEndDate = eDate
        else:
            try:
                dtEndDate = datetime.strptime(eDate, fDateTime)    # Convierte primero a datetime para validar la fecha.
            except(TypeError, ValueError):
                dtEndDate = None

        if dtStartDate and dtEndDate and dtStartDate > dtEndDate:
            dtStartDate = None  # si startDate > endDate_str, ignora startDate
        startDate_str = dtStartDate.strftime(fDateTime) if dtStartDate else ''  # startDate_str:  'YYYY-MM-DD HH:MM:SS'
        endDate_str = dtEndDate.strftime(fDateTime) if dtEndDate else ''        #   endDate_str: 'YYYY-MM-DD HH:MM:SS'
    else:
        startDate_str = endDate_str = ''

    strAndDate = ''
    condFldName = str(condFldName).strip() if condFldName else None
    dbFldName = getFldName(tblName, condFldName) if condFldName else ''
    if dbFldName and strError not in dbFldName:
        # if 'where' not in strAndDate.lower():
        #     strAndDate += ' WHERE'
        strAndDate += f' WHERE {strAnd} "{dbFldName}" >= "{startDate_str}" ' if startDate_str != '' else ''
        strAnd = '' if strAndDate == '' else ' AND '                      #  and not kwargs
        strAndDate += f' {strAnd} "{dbFldName}" <= "{endDate_str}" ' if endDate_str != '' else ''

    strSQL = strSelect + strFrom + strWhere + strAndDate
    # print(f'{moduleName()}({lineNum()}).{callerFunction(namesOnly=True, getCallers=False)} - strSQL: {strSQL}')
    return dbRead(tblName, strSQL)  # Retorna objeto de tipo DataTable





# def getRecords00(tblName, sDate=None, eDate=None, condFldName=None, *args, **kwargs):        # TODO: NUEVA->Running.
#     """     Implementation 21Sep22: removes Get FIRST, get LAST records. Parses date periods only.
#     @param sDate: start Date.
#     @param eDate: end Date. sDate = eDate = None -> Pulls all records matching the other conditions passed.
#            Other values of sDate,eDate: treated as dates to pull records selecting by condFldName (must be DATE field)
#     @param tblName: DB table keyname (tblCaravanas, tblDataPersonasDatos, tblDataTMTransasccionesMonetarias, etc)
#     @param condFldName: fieldName de condicion. Ej. MAX('fldDate')
#     @param args: "SELECT" fields for SQL String. *: All fields in _tblName
#     @param kwargs: Conditions {fName:fldValue1, fldName2: fldValue2, }. If '' or None are passed as values,
#     returns all records
#     @return: DataTable object.
#     """
#     # ----------------------- 1) Parsing *argsSelect para StrSelect -----------------------------#
#     tblName = str(tblName).strip()
#     dbTblName = getTblName(tblName)
#     if strError in dbTblName:
#         print(f'{dbTblName} - Function/Method:{callerFunction()}')
#         return dbTblName  # Sale si es tabla no valida
#     # Si sValue == eValue =='' -> Toma ULTIMO REGISTRO para CondFldName, mas las demas condiciones pasadas en **kwargs.
#     if sDate == eDate == '':
#         groupOp = 'MAX'
#     elif sDate == eDate == 0:
#         groupOp = 'MIN'
#     else:
#         groupOp = None
#     # strSQLSelect1Table chequea validez de tblWrite y de campos *argsSelect
#     strSelect = strSQLSelect1Table(tblName, groupOp, condFldName, 0, *args)
#     if strError in strSelect:
#         retValue = f'ERR_INP: {strSelect} - Function/Method:{callerFunction()}'
#         krnl_logger.info(retValue)
#         print(retValue)
#         return retValue
#     strFrom = f' FROM "{dbTblName}"'
#
#     # ----------------------- 2) Parsing de **kwargs -> Diccionario de Condiciones -----------------------------#
#     strAnd = ''
#     where = ' WHERE ' if kwargs else ''
#     strWhere = where
#     conditionsDict = strSQLConditions1Table(tblName, **kwargs)
#     if conditionsDict:
#         for i in conditionsDict:  # Genera string de condiciones AND con el diccionario de condiciones "conditionsDict"
#             if conditionsDict[i] != '*':  # Si es * ignora el campo. Lo quita del string SQL para seleccionar todos
#                 if 'not null' in str(conditionsDict[i]).lower():
#                     strWhere += strAnd + f"{i}" + f' IS NOT NULL '
#                 elif conditionsDict[i] is None or 'null' in str(conditionsDict[i]).lower():
#                     strWhere += strAnd + f"{i}" + f' IS NULL '
#                 else:
#                     strWhere += strAnd + f"{i}" + (' IN ' if '(' in str(conditionsDict[i]) else ' = ') \
#                                 + f"{conditionsDict[i]}"
#                 strAnd = ' AND '
#     strWhere = strWhere if strWhere != where else where  # Si no se pasa ninguna condicion, asume * (elimina ' WHERE ')
#
#     if sDate or eDate:
#         if isinstance(sDate, datetime):
#             dtStartDate = sDate
#         else:
#             try:
#                 dtStartDate = datetime.strptime(sDate, fDateTime)  # Convierte primero a datetime para validar la fecha.
#             except(TypeError, ValueError):
#                 dtStartDate = None
#         # if eDate != '':
#         if isinstance(eDate, datetime):
#             dtEndDate = eDate
#         else:
#             try:
#                 dtEndDate = datetime.strptime(eDate, fDateTime)    # Convierte primero a datetime para validar la fecha.
#             except(TypeError, ValueError):
#                 dtEndDate = None
#
#         if dtStartDate and dtEndDate and dtStartDate > dtEndDate:
#             dtStartDate = None  # si startDate > endDate_str, ignora startDate
#         startDate_str = dtStartDate.strftime(fDateTime) if dtStartDate else ''  # startDate_str:  'YYYY-MM-DD HH:MM:SS'
#         endDate_str = dtEndDate.strftime(fDateTime) if dtEndDate else ''        #   endDate_str: 'YYYY-MM-DD HH:MM:SS'
#     else:
#         startDate_str = endDate_str = ''
#
#     strAndDate = ''
#
#     condFldName = str(condFldName).strip() if condFldName else None
#     dbFldName = getFldName(tblName, condFldName) if condFldName else ''
#     if dbFldName and strError not in dbFldName:
#         strAndDate += f' {strAnd} "{dbFldName}" >= "{startDate_str}" ' if startDate_str != '' else ''
#         strAnd = '' if strAndDate == '' else ' AND '                      #  and not kwargs
#         strAndDate += f' {strAnd} "{dbFldName}" <= "{endDate_str}" ' if endDate_str != '' else ''
#
#     strSQL = strSelect + strFrom + strWhere + strAndDate
#     # print(f'{moduleName()}({lineNum()}).{callerFunction(namesOnly=True, getCallers=False)} - strSQL: {strSQL}')
#     return dbRead(tblName, strSQL)  # Retorna objeto de tipo DataTable


def delRecord(tblName=None, idRecord=None, **kwargs):
    """ Deletes a record from DB.
    @return: True if success or errorCode (str)
    """
    # Ante cualquier error, ejecuta funcion desde aqui (el mismo thread). Por las dudas, ejecuta tambien si es None
    # TODO: Incorporar codigo de actualizacion de memory data (usando locks) si se borra un record sincronizado con
    #  memoria.->verificar que la tabla este en dataInMemoryTable y que el idRecord a borrar sea el del dato en memoria.
    #  Usar flag MemData p/ definir que valor setear en memoria al borrar el record actual (debe ser el record inmediato
    # anterior)
    # qryObj = SQLiteQuery.getObject()
    retValue = None
    if tblName and idRecord:
        tblName = str(tblName).strip()
        dbTblName = getTblName(tblName)
        if strError in dbTblName:
            retValue = f'ERR_Inp: InvalidArgument: {tblName}'
    else:
        retValue = f'ERR_INP_InvalidArgument: Table {tblName} or record {idRecord} invalid or missing.'

    if retValue is None:
        strDelete = f' DELETE FROM "{dbTblName}" WHERE "{dbTblName}"."{getFldName(tblName, "fldID")}" = "{idRecord}"'
        retValue = writeObj.execute_sql(strDelete)
    if isinstance(retValue, str):
        retValue = f'ERR_DB_Delete Operation Failed: {retValue}'
        db_logger.error(retValue)

    return retValue


def close_db_writes():
    AsyncBuffer.flush_all()
    SqliteQueueDatabase.stop_all_writers()
    return None


# Las funciones de abajo no pueden salir de este modulo por ahora (circular import errors)

def setupArgs(tblName: str, *args, **kwargs):
    """
    Creates a DataTable object for tblName and writes all valid parameters found in *args and **kwargs.
    Fields values passed in **kwargs are assigned ONLY if corresponding fields in tables passed *args are None,'','NULL'
    @param tblName: valid table Name
    @param args: DataTable objects
    @param kwargs: obj_data to populate fields that are blank (None) in tables passed in *args
    @return: DataTable Object with arguments passed. dataList in DataTable val if no valid arguments found.
             Returns errorCode (str) if tblName is not valid
    """
    tblName = tblName.strip()
    if strError in getTblName(tblName):
        retValue = f'ERR_INP_InvalidArgument:  Table Name: {tblName} - {moduleName()}({lineNum()}) - {callerFunction()}'
        return retValue

    tblArgs = DataTable(tblName)
    if args:
        try:
            for table in args:  # Asigna valores de *args (si se pasaron) a tablas tblRA y tblTransact
                if isinstance(table, DataTable) and table.dataLen and table.tblName == tblArgs.tblName:
                    for j in range(table.dataLen):
                        dicto = table.unpackItem(j)  # Inicializa multiples registros en dataList (si se pasa mas de 1)
                        tblArgs.setVal(j, **dicto)  # Escribe obj_data de args en dataList[j] (1 o mas registros)
        except (TypeError, ValueError, IndexError, KeyError, NameError):
            krnl_logger.info(f'ERR_INP_Invalid Argument - {moduleName()}({lineNum()})-{callerFunction()}. '
                             f'Table: {tblName}')
    # Asigna datos de kwargs a tblArgs. Values de kwargs se escriben en _dataList SOLO si esos campos en *args son None
    if kwargs:
        _ = getFldName(tblName, '*', 1)
        commonKeys = set(kwargs).intersection(_)  # set(kwargs) arma un set con todos los keys de kwargs.
        # kwargsParsed = {k: kwargs[k] for k in commonKeys}  # retorna solo campos presentes en tabla tblName
        for i in commonKeys:
            if type(kwargs[i]) is dict:         # ignora los diccionarios. No son un tipo valido para esta funcion.
                pass
            else:
                if type(kwargs[i]) not in (list, tuple, set):
                    kwargs[i] = [kwargs[i], ]
                for j in range(len(kwargs[i])):                  # j es Index de _dataList, i es fldName
                    if not tblArgs.getVal(j, i):
                        tblArgs.setVal(j, i, kwargs[i][j])      # Escribe solo si kwargs['fldName'] = None, '', NULL
    return tblArgs


def dbRead(tblName: str, strSQL: str, mode=0):  # tblName necesario para armar la estructura de retorno TODO(cmt): NUEVA
    """
    Reads records from DB using argument strSQL. strSQL must be valid, with access to 1 table only.
    mode: 0(Default): returns DataTable Object  -> THIS IS THE MORE EFFICIENT WAY TO PROCESS DATA
          1: returns list of dictionaries [{fldName1:value1, fldName2:value2, }, {fldName1:value3, fldName2: value4, },]
    @return: mode 0: Object of class DataTable with all the obj_data queried. (Default)
             mode 1: List of dictionaries [{fld1:val1, fld2:val2, }, {fld3:val2, fld1:val4, fld2:val5,}, ]. Each
             dictionary maps to a record (row) from DB and goes to a record item in DataTable.
    """
    qryObj = SQLiteQuery()  # TODO(cmt): Adquiere queryObj del thread desde donde se llama a dbRead(). MUY IMPORTANTE!
    dataList = []
    keyFieldNames = []  # keynames de los campos presentes en _dataList
    tblName = tblName.strip()  # Nombre de la tabla
    reverseFldNamesDict = {v: k for k, v in getFldName(tblName, '*', 1).items()}   # dict {dbFldName: fldName}
    cur = qryObj.execute(strSQL)   # TODO(cmt): Acceso a DB. Convierte strings a datetime via PARSE_DECLTYPES
    if not isinstance(cur, str):
        # IMPORTANTE: acceder via cur.description asegura que los indices de fldNames y valores se correspondan.
        dbFieldNames = [j[0] for j in cur.description]  # Solo campos leidos de DB se incluyen en la DataTable
        keyFieldNames = tuple(map(reverseFldNamesDict.get, dbFieldNames))

        rows = cur.fetchall()               # TODO(cmt): lectura de registros.
        if not rows:            # No hay datos: Retorna tabla vacia, PERO CON keyFieldNames inicializados.
            return DataTable(tblName, dataList, keyFieldNames)  # [dataList, ]

    else:       # No hay datos: Retorna tabla vacia, keyFieldNames=dbFieldNames=[]
        retValue = DataTable(tblName, dataList, keyFieldNames)
        krnl_logger.error(f'ERR_DBAccess dbRead(): {cur} - {callerFunction()}')
        return retValue  # Loggea error y retorna tabla vacia

    if not mode:        # Convierte los records de rows en listas para hacerlos modificables y retorna DataTable
        return DataTable(tblName, [list(j) for j in rows], keyFieldNames)
    else:
        rowLen = len(keyFieldNames)
        return [{keyFieldNames[i]: j[i] for i in range(rowLen)} for j in rows]


def getParameterID(tableName: str, paramName: str):
    """
    Gets parameter ID for paramName
    @param: paramName (str). Parameter name from tables in parameterTablesList
    @return: parameter ID (fldID) for parameter with paramName or None if none found
    """
    parameterTablesList = ['tblAnimalesParametrosNombres', 'tblSysParametrosNombres']
    retValue = None
    tblName = tableName.strip()
    if tblName in parameterTablesList:
        temp = getRecords(tblName, '', '', None, '*')
        namesList = temp.getCol('fldName')
        namesList = [removeAccents(i) for i in namesList]
        parsedName = removeAccents(paramName)
        if parsedName in namesList:
            for j, name in enumerate(namesList):
                if name == parsedName:
                    paramID = temp.getVal(j, 'fldID')
                    retValue = paramID
                    break
    return retValue


def fetchAnimalParameterValue(paramName: str, paramType='general'):
    """
    Gets the last parameter from table tblDataAnimalesParametrosGenerales
    @param paramName: Parameter Name (str) from selected table
    @param paramType: general: Parameter from [Data Animales Parametros Generales]
                      individual: Parameter from [Data Animales Parametros Individuales]
    @return: Active parameter(last recorded val for paramName)
    """
    retValue = None
    paramType = str(paramType).strip().lower()
    if paramType.startswith('gen'):                      # TIENE que empezar con 'gen'
        table = 'tblDataAnimalesParametrosGenerales'
    elif paramType.startswith('indiv'):                   # TIENE que empezar con 'indiv'
        table = 'tblDataAnimalesActividadParametrosIndividuales'
    else:
        table = 'tblDataAnimalesParametrosGenerales'
    paramID = getParameterID('tblAnimalesParametrosNombres', paramName)
    if paramID > 0:
        temp = getRecords(table, '', '', 'fldDate', '*', fldFK_NombreParametro=paramID)
        if temp.dataLen and temp.dataList[0]:
            retValue = temp.getVal(0, 'fldParameterValue')
    return retValue


init_database()         # Creates all Triggers and Indices in db file.

