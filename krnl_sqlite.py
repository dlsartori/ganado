import sqlite3
from krnl_exceptions import *
import threading
from threading import Lock
from collections import defaultdict
from krnl_config import strError, callerFunction, lineNum, moduleName, uidCh, MAIN_DB_NAME, db_logger, \
    CLIENT_MIN_INDEX, CLIENT_MAX_INDEX, NEXT_BATCH_MIN_INDEX, NEXT_BATCH_MAX_INDEX, print, DISMISS_PRINT

# tblNotFoundErrMsg = 'ERR_Sys_TableNotFound'

SQLiteDBError = sqlite3.DatabaseError
SQLiteError = sqlite3.Error
SYS_TABLES_NAME = '_sys_Tables'
SYS_FIELDS_NAME = '_sys_Fields'
lock = Lock()

class SQLiteQuery(object):      # 1 solo objecto db _conn por thread. Usa thread data
    """
    *** This class is to be used to create QUERY-ONLY objects. DO NOT WRITE to DB with these objects. Use
    SqliteQueueDatabase for writing to avoid database blocking. ***
    DB Access Model:
       - SQLiteQuery objects are a pool of DB connections to perform READ operations concurrently from different threads
       - SSqliteQueueDatabase objects are unique objects (singleton class) used to WRITE to DB, using a queue logic.

    Returns an SQLiteQuery object to access DB. If a _conn is already available returns it or use, otherwise
    creates a new _conn. Returns the first _conn available (in_transaction=False) found in list of connections
    for the calling thread
    @param: force_new -> True: forces creation of a new DB _conn and returns the new _conn. Connections created with
    'force_new' are not managed by this class. Must be closed by the creator.
    """
    __slots__ = ('__conn', '__threadID', '__thread_name', '__dict__')
    __MAX_CONNS_PER_THREAD = 100
    __queryObjectsPool = defaultdict(list)  # Registra queryObject y thread al que pertenece el obj:{threadID: [self, ]}
    __newInstance = []  # Lista LIFO para avisar a __init__ que es new instance. TODO(cmt): Lista p/ habilitar re-entry.
    __lock = Lock()

    def __new__(cls, **kwargs):  # Override de new para crear objeto nuevo solo si el thread no tiene un definido
        callingThread = threading.current_thread().ident
        # TODO(cmt): Si es force_new -> NO SE REGISTRA el objeto en el pool, es exclusivo del que lo crea
        if kwargs.get('force_new') is True:
            instance = super().__new__(cls)
            if len(cls.__queryObjectsPool.get(callingThread, [])) > SQLiteQuery.__MAX_CONNS_PER_THREAD:
                db_logger.warning(f'{moduleName()}({lineNum()}) - {callerFunction()}: 'f'ERR_SYS_DBAccess: '
                                   f'Connections for {callingThread} exceeded {SQLiteQuery.__MAX_CONNS_PER_THREAD}.')
            with cls.__lock:
                cls.__newInstance.append(True)      # Esta lista permite re-entry al codigo de __new__() e __init__()
            return instance

        # Here, an available _conn is procured.
        objList = cls.__queryObjectsPool.get(callingThread, [])
        obj = next((j for j in objList if not j.conn.in_transaction), None)
        if obj:
            return obj
        # En este punto, no se encontraron queryObj libres. Crea uno mas para este thread. __new__()lo agrega a Pool
        instance = super().__new__(cls)   # Crea objeto si el numero este dentro del limite de objs per thread
        with cls.__lock:
            cls.__queryObjectsPool[callingThread].append(instance)
            cls.__newInstance.append(True)
        return instance

    def __init__(self, db_name=None, *, check_same_thread=True, timeout=4.0, detect_types=0, **kwargs):
        """
        Opens a SQLiteQuery _conn and returns a valid cursor. This object becomes the DB handler.
        # The line of code assigning sqlite3.Row to the row_factory of _conn creates what some people call a
        # 'dictionary cursor', - instead of tuples it starts returning 'dictionary' rows after fetchall or fetchone.
        @param kwargs:
        """
        super().__init__()
        # Lista __newInstance permite re-entry de distintos threads
        if self.__newInstance and self.__newInstance[-1] is True:
            with self.__lock:
                self.__newInstance.pop()  # Elimina ultimo elemento.__newInstance es LIFO p/ que funcione correctamente.
            # detect_types para tratar de convertir datetime automaticamente. Por default a TODAS las conexiones.
            self.__conn = self.connCreate(dbName=db_name, check_same_thread=check_same_thread, timeout=timeout,
                                          detect_types=detect_types | sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                                          **kwargs)
            self.__threadID = threading.current_thread().ident
            self.__thread_name = threading.current_thread().name
            db_logger.info(f'=====> SQLiteQuery Constructor: Connection thread is {threading.current_thread().ident} '
                             f'- ThreadRegister: {self.__queryObjectsPool} - {callerFunction()}')
            # self.__idxLowerLimit = CLIENT_MIN_INDEX        # Indices minimo y maximo para crear registros en esta DB
            # self.__idxUpperLimit = CLIENT_MAX_INDEX
            # self.__idxLowerLimit_Next = NEXT_BATCH_MIN_INDEX
            # self.__idxUpperLimit_Next = NEXT_BATCH_MAX_INDEX


    @staticmethod
    def connCreate(dbName='', *, check_same_thread=True, detect_types=0, isolation_level=None, timeout=0.0, cach_stmt=0,
                   uri=False, **kwargs):  # kwargs a todas por ahora, para flexibilidad
        """Creates a DB connection """
        dbName = dbName.strip() if dbName else MAIN_DB_NAME
        try:
            retValue = sqlite3.connect(dbName, check_same_thread=check_same_thread, detect_types=detect_types,
                                isolation_level=isolation_level, timeout=timeout, cached_statements=cach_stmt, uri=uri)
        except(sqlite3.Error, sqlite3.DatabaseError) as e:
            retValue = f'ERR_DB_Cannot create connection: {e} - {callerFunction(getCallers=True)}'
            raise DBAccessError(f'{retValue}')
        finally:
            pass
        return retValue

    @classmethod
    def getObjectsPool(cls):
        return cls.__queryObjectsPool

    @classmethod
    def getObject(cls):
        """ Returns a SQLite query object associated to the thread that makes a call to perform a DB operation.
        """
        # Toma el thread id desde donde se inicio la llamada a setRecord() y busca lista de conexiones de ese thread.
        objList = SQLiteQuery.getObjectsPool().get(threading.current_thread().ident, [])
        obj = next((j for j in objList if not j.conn.in_transaction), None)
        return obj if obj else SQLiteQuery()  # Si no hay queryObj libres crea uno mas para este thread

    @classmethod
    def checkThreadSafety(cls):
        """ Returns SQLite build Threading Safety level: 0, 1, 2 (int). None if the call fails """
        con = sqlite3.connect(":memory:")
        val = con.execute("select * from pragma_compile_options where compile_options like 'THREADSAFE=%' ").fetchall()
        if val and len(val) > 0:
            safeStr = val[0][0].lower().strip()
            _ = safeStr.replace('threadsafe=', '')
            return int(_)
        return None

    def getCursor(self):
        if self.__conn:
            return self.__conn.cursor()

    @property
    def conn(self):
        return self.__conn

    @property
    def callingThread(self):
        return threading.current_thread().ident

    @property
    def threadID(self):
        return self.__threadID

    @property
    def threadName(self):
        return self.__thread_name

    def __del__(self):  # Cerrar conexion db, remover objeto del pool de conexiones.
        if self.__conn and self.__threadID == threading.current_thread().ident:
            try:
                self.execute('PRAGMA OPTIMIZE; ')
                self.connClose()
                self.__conn = None
                # Busca conexiones del calling thread
                if next((j for j in self.getObjectsPool().get(threading.current_thread().ident) if j == self), None):
                    with self.__lock:
                        self.__queryObjectsPool[self.__threadID].remove(self)
                    val = f'========> {self.__class__.__name__}.__del__(): Deleted obj={id(self)} / Thread: {self.__threadID}'
                    db_logger.info(val)
                    print(val, dismiss_print=DISMISS_PRINT)
            except (StopIteration, ValueError, sqlite3.Error) as e:
                retValue = f'ERR_DBClose. Cannot close connection or object not found. Error: {e}'
                db_logger.error(retValue)
                raise DBAccessError(f'{retValue} - {callerFunction(getCallers=True)}')


    # @timerWrapper(iterations=50)   # range: 300 - 2300 usec (50 iterations)
    def execute(self, strSQL='', params='', *, tbl=None, fldID_idx=None):
        """ Executes strSQL. Returns a cursor object or errorCode (str). This function for QUERY/DB Reads ONLY!!
            Implements re-entry handling code to support re-entry from other threads in the time between the execution
            of get_max_id() and the actual write to DB via the execute() command.
        """
        if strSQL:
            if tbl is not None and fldID_idx is not None:  # entra aqui solo en INSERT. NO SE USA INSERT aqui...
                # TODO(cmt) Sacha-implementacion de escritura con SQLiteQuery. NO FUNCIONA CORRECTAMENTE: Usa locks para
                #  obtener max_index correctamente PERO NO EVITA "database locked" errors si si intenta escribir en DB
                #  cuando hay otra escritura en curso. Este bloque NO es usado ya que SQLiteQuery() objects se usan
                #  solo para lectura. Se escribe con SqliteQueueDatabase.
                params = list(params)
                # with self.__lock:    # __lock asegura que durante reentrada 2 tbl_idx no terminen con los mismos valores
                #     tbl_idx = self.__indicesDict.get(tbl)       # tbl_idx = None si la tabla no existe en __indicesDict
                #     if tbl_idx is None:
                #         tbl_idx = [None, 0, 0]   # Inicializa estruct. tbl_idx si tbl no existe aun en __indicesDict
                #     tbl_idx[2] += 1                     # Incrementa reentryCounter.
                #     self.__indicesDict[tbl] = tbl_idx   # Hace disponible reentryCounter para otros threads

                max_idx = self._get_max_id(tbl)
                if isinstance(max_idx, str):
                    max_idx = f'ERR_SQLiteQuery {callerFunction()} Table Name error: {max_idx}'
                    db_logger.error(max_idx)
                    raise DBAccessError(f'DatabaseError Exception.{max_idx}; strSQL: {strSQL}')

                params[fldID_idx] = max_idx + 1     # asigna a fldID max index de tbl + 1
                # if tbl_idx[0] is None or tbl_idx[0] == max_id:
                #     tbl_idx[1] = tbl_idx[2]  # Si ya hay 1 idx en __indicesDict (es reentrada) actualiza index counter
                # tbl_idx[0] = max_id
                # params[fldID_idx] = tbl_idx[0] + tbl_idx[1]  # Actualiza fldID en params.
                # with self.__lock:  # lock porque la actualizacion de los campos de __indicesDict[tbl] DEBE ser atomica
                # self.__indicesDict[tbl] = tbl_idx   # Documentacion dice que esta operacion ES atomica (D[x] = y)
                # print(f'****** On entry __indicesDict = {self.__indicesDict[tbl]}', dismiss_status=DISMISS_PRINT)

            with self.__conn:
                db_logger.debug(f'SQLiteQuery received query: {strSQL}')
                try:
                    cur = self.__conn.execute(strSQL, params)
                except (sqlite3.Error, DBAccessError, Exception) as e:
                    cur = f'ERR_SQLiteQuery {callerFunction()} - error: {e}'
                    self.__conn.rollback()
                    db_logger.error(f'{cur}; strSQL: {strSQL}')
                    self.__conn.execute('PRAGMA OPTIMIZE; ')
                    raise DBAccessError(f'DatabaseError Exception.{cur}; strSQL: {strSQL}')

            return cur
        return None



    # 30May23: Implementa locks para controlar re-entry a este codigo.  # DEPRECATED. DO NOT USE.
    # def _get_max_id(self, tbl, *, row_count=1) -> int:      # This _get_max_id GETS re-entries from other threads.
    #     dbTblName = getTblName(tbl, 0)
    #     if dbTblName.startswith(strError):
    #         return dbTblName         # Sale si hay error en tblName, retorna string con error.
    #
    #     strMaxId = f"SELECT IFNULL(MAX(ROWID),{self.__idxLowerLimit}) FROM [{dbTblName}] WHERE " \
    #                f"ROWID >= {self.__idxLowerLimit} AND ROWID < {self.__idxUpperLimit}; "
    #     try:
    #         self.__lock.acquire()
    #         cur = self.__conn.execute(strMaxId)
    #         # idx = cur.fetchone()[0]
    #     except (sqlite3.Error, sqlite3.DatabaseError):
    #         cur = False
    #     finally:
    #         self.__lock.release()
    #
    #     if cur is False:                # Si tbl es WITHOUT ROWID -> utiliza dbFldName en vez de ROWID.
    #         dbFldName = getFldName(tbl, 'fldID')
    #         strMaxId = f"SELECT IFNULL(MAX([{dbFldName}]),{self.__idxLowerLimit}) FROM [{dbTblName}] WHERE " \
    #                    f"[{dbFldName}] >= {self.__idxLowerLimit} AND [{dbFldName}] < {self.__idxUpperLimit}; "
    #         try:
    #             self.__lock.acquire()
    #             cur = self.__conn.execute(strMaxId)
    #             # idx = cur.fetchone()[0]
    #         except (sqlite3.Error, sqlite3.DatabaseError):
    #             cur = False
    #         finally:
    #             self.__lock.release()
    #     if cur:
    #         idx = cur.fetchone()[0]
    #     else:
    #         idx = None
    #
    #     if idx is not None:
    #         if idx + row_count > self.__idxUpperLimit:  # TODO: ver si corresponde  ">" o ">="
    #             with self.__lock:
    #                 self.__idxLowerLimit = self.__idxLowerLimit_Next  # Pasa al siguente batch asignado.
    #                 self.__idxUpperLimit = self.__idxUpperLimit_Next  # Tabla [Index Batches] asignados a cada dbName
    #                 idx = self.__idxLowerLimit
    #         return idx
    #
    #     return f'ERR_DBAccess: Cannot read max index for table {tbl}'


    def connClose(self, *, optimize=False):
        # 1. Verify what type of cleanup and closures must be done (incl. backup dumps) before closing.
        try:
            if optimize:
                self.execute('PRAGMA OPTIMIZE; ')
            self.__conn.close()
        except sqlite3.Error as e:
            retValue = f'ERR_DBClose: {e} - Cannot close connection.'
            raise DBAccessError(f'{retValue} - {callerFunction(getCallers=True)}')


    def initialize(self):
        cur = self.execute('PRAGMA JOURNAL_MODE = WAL; ')  # Setear JOURNAL_MODE=WAL al iniciar el sistema (performance)
        _ = self.execute('PRAGMA OPTIMIZE; ')
        if isinstance(cur, str) or not str(cur.fetchone()[0]).lower().__contains__('wal'):  # JOURNAL_MODE queda en disco
            db_logger.info('PRAGMA setting failed. WAL journal_mode not enabled')
            cur = f'ERR_DBWrite: PRAGMA setting failed. WAL journal_mode not enabled'
        return cur

# ============================================ End SQLiteQuery class ============================================= #

# Main Thread (FrontEnd) database accesss object:       # TODO: Pasar luego esta conexion al main().
def createDBConn():
    obj = SQLiteQuery(check_same_thread=True)  # Front end query object. Registers itself in queryObjectsPool
    if isinstance(obj, str):
        obj = f'ERR_DBAccess: Cannot create _conn to DataBase..'
        print(f'{moduleName()}({lineNum()}): {obj} - {callerFunction()}')
        db_logger.critical(f'DB Critical Error!. {moduleName()}({lineNum()}): {obj} - {callerFunction()}')
        exit(-1)
    return obj

queryObj = createDBConn()


# ------------ SQLiteQuery associated Functions. TODO: Do NOT move from here --------------

# def getMaxId(tbl: str):  # TODO(cmt): Funcion NO SOPORTA re-entrada. No usar en entornos multithreading
#     """ Gets the recordID of the last record of a table.
#         Implements re-entry management because the function can be called by different threads at the same time.
#         This function is mostly deprecated. Table max_index is obtained by Class Cursor functions. DO NOT USE to
#         obtain write indexes.
#     """
#     if isinstance(tbl, str):
#         qryObj = SQLiteQuery.getObject()  # queryObject con conexion creada en el mismo thread que hace la llamada
#         dbFldName = getFldName(tbl, 'fldID')
#         strSQL = f"SELECT IFNULL(MAX([{dbFldName}]),{getMaxId.idxLowerLimit}) FROM [{getTblName(tbl)}] WHERE " \
#                  f"[{dbFldName}] >= {getMaxId.idxLowerLimit} AND [{dbFldName}] < {getMaxId.idxUpperLimit}; "
#         retCur = qryObj.execute(strSQL)
#         if isinstance(retCur, str):
#             return retCur
#         max_index = retCur.fetchone()[0]        # Tiene que ir a chequear si max_index == getMaxId.idxUpperLimit
#         if max_index == getMaxId.idxUpperLimit:
#             getMaxId.idxLowerLimit = NEXT_BATCH_MIN_INDEX  # Pasa al siguente batch asignado. Estos vienen de DB
#             getMaxId.idxUpperLimit = NEXT_BATCH_MAX_INDEX  # de DB: tbl [Index Batches] asignados a cada dbName
#         # print(f'reentryDict[{tbl}] = {reentryDict[tbl]} / thread: {threading.current_thread()}')
#         return max_index
#     return None
# getMaxId.idxLowerLimit = CLIENT_MIN_INDEX        # Estos valores vienen de DB. Se setean al arrancar el sistema.
# getMaxId.idxUpperLimit = CLIENT_MAX_INDEX


# def getMaxId00(tbl: str, reentryDict=None):  # TODO(cmt): Para ser usada en conjunto con setRecord() que actualice reentryDict.
#     """ Gets the recordID of the last record of a table.
#         Implements re-entry management because the function can be called by different threads at the same time.
#     """
#     if isinstance(tbl, str):
#         qryObj = SQLiteQuery.getObject()  # queryObject con conexion creada en el mismo thread que hace la llamada
#         dbFldName = getFldName(tbl, 'fldID')
#         strSQL = f"SELECT IFNULL(MAX([{dbFldName}]),{getMaxId.idxLowerLimit}) FROM [{getTblName(tbl)}] WHERE " \
#                  f"[{dbFldName}] >= {getMaxId.idxLowerLimit} AND [{dbFldName}] < {getMaxId.idxUpperLimit}; "
#         # strSQL = f"SELECT IFNULL(MAX(ROWID), 0) FROM [{getTblName(tbl)}]; "  # Este comando puede dar valor incorrecto
#         if reentryDict is None:      # Si no se pasa reentryDict -> es tabla con autoincrement. Ejecuta directamente.
#             print(f'ENTRO AL BLOQUE SIN reentryDict: {callerFunction(getCallers=True)}')
#             retCur = qryObj.execute(strSQL)
#             if isinstance(retCur, str):
#                 return retCur
#             max_index = retCur.fetchone()[0]        # Tiene que ir a chequear si max_index == getMaxId.idxUpperLimit
#
#         else:
#             retCur = qryObj.execute(strSQL)                   # (Vamos a ver si es cierto...)
#             if not isinstance(retCur, sqlite3.Cursor):
#                 return retCur
#             with lock:  # TODO(cmt): Operacion ES ATOMICA, para evitar que distintos threads reciban el mismo max_index
#                 counter = reentryDict.get(tbl, None)        # reentryDict = {tblName: counter, }
#                 counter = 0 if counter is None else counter + 1
#                 reentryDict[tbl] = counter
#             max_index = retCur.fetchone()[0] + counter
#
#         if max_index == getMaxId.idxUpperLimit:
#             getMaxId.idxLowerLimit = NEXT_BATCH_MIN_INDEX  # Pasa al siguente batch asignado. Estos vienen de DB
#             getMaxId.idxUpperLimit = NEXT_BATCH_MAX_INDEX  # de DB: tbl [Index Batches] asignados a cada dbName
#         # print(f'reentryDict[{tbl}] = {reentryDict[tbl]} / thread: {threading.current_thread()}')
#         return max_index
#     return None
# getMaxId.idxLowerLimit = CLIENT_MIN_INDEX        # Estos valores vienen de DB. Se setean al arrancar el sistema.
# getMaxId.idxUpperLimit = CLIENT_MAX_INDEX


def reloadTables():
    qryObj = SQLiteQuery.getObject()  # Obtiene query object para el Thread llamador.
    strSQL = f"SELECT Table_Key_Name, Table_Name, ID_Table, Bitmask_Table, Method_Name FROM '{SYS_TABLES_NAME}'"
    try:
        rows = qryObj.execute(strSQL).fetchall()  # fetchall() -> (tblName, tblIndex, isAutoIncrement, isWITHOUTROWID, table_bitmask, Method_Name)
        if rows:
            retValue = None
        else:
            retValue = f'ERR_DB_Access: cannot read from {SYS_TABLES_NAME} - {callerFunction(2, getCallers=True)}'
            db_logger.error(f'{retValue}')
    except (sqlite3.Error, sqlite3.DatabaseError) as e:
        retValue = f'ERR_DBRead: {e} - {callerFunction()}'  # DEBE estar ERR_ en los strings de error
        db_logger.error(retValue)

    if retValue is not None:
        return retValue
    else:
        # mode = 0 -> Retorna dbTblName
        # mode!= 0 -> Retorna tupla (dbTblName, tblIndex, pkAutoIncrement(1/0), isROWID(1/0))
        getTblName.__sysTablesCopy = {rows[j][0]: [rows[j][1], rows[j][2], None, None, rows[j][3], rows[j][4]]
                                      for j in range(len(rows))}

        # Agrega campos de identificacion de AutoIncrement y WITHOUT ROWID para cada Tabla.
        for t in getTblName.__sysTablesCopy:
            try:
                strPragma = f'PRAGMA TABLE_INFO("{getTblName.__sysTablesCopy[t][0]}")'
                cursor = qryObj.execute(strPragma)
                tblData = cursor.fetchall()
                colNames = [j[0] for j in cursor.description]
                pkIndex, typeIndex = colNames.index('pk'), colNames.index('type')
                pkFieldsIndices = [tblData.index(j) for j in tblData if
                                   j[pkIndex]]  # Lista de index de PK de cada tbl: 1 indice por cada columna PK
                if len(pkFieldsIndices) == 1 and str(tblData[pkFieldsIndices[0]][typeIndex]).upper() == 'INTEGER':  #
                    getTblName.__sysTablesCopy[t][2] = 1  # 3er Campo de tupla: 1 ->AutoIncrement (DO NOT REUSE Indices)
                else:
                    getTblName.__sysTablesCopy[t][2] = 0  # 3er Campo de tupla: 0 -> No es AutoIncrement (REUSE Indices)

                # Define si es WITHOUT ROWID para actualizar AutoIncrement y WITHOUT_ROWID
                strPragma = f'PRAGMA INDEX_INFO("{getTblName.__sysTablesCopy[t][0]}")'
                tblData = qryObj.execute(strPragma).fetchall()
                if len(tblData):
                    getTblName.__sysTablesCopy[t][2] = 0  # Cuando tabla es WITHOUT ROWID
                    getTblName.__sysTablesCopy[t][3] = 1  # Es WITHOUT ROWID -> __sysTables[t][3]=1 -> None NO autoincrementa
                else:
                    getTblName.__sysTablesCopy[t][3] = 0  # No es WITHOUT ROWID -> __sysTables[t][3]=0 (Autoincrementa)
            except (sqlite3.Error, sqlite3.DatabaseError) as e:
                getTblName.__sysTablesCopy[t][2] = getTblName.__sysTablesCopy[t][3] = None
                db_logger.error(f'Cannot load data from {SYS_TABLES_NAME}. Error: {e}')
                raise sqlite3.DatabaseError(f'Cannot load data from {SYS_TABLES_NAME}. Error: {e}')

            # print(f'tblData({t}) / {colNames}: {pkFieldsIndices} /Auto Incr.:{getTblName.__sysTables[t][2]}')
    with lock:
        getTblName.__sysTables = getTblName.__sysTablesCopy.copy()      # solo copia diccionarios dentro del lock.
    return True

    # ---------------------------------------------- End reloadTables ----------------------------------------

def getTblName(tbl: str = '', mode=0, *, reload=False, db_table_name=None):
    """
    Gets the name of a table by its keyname
            mode: O -> dbTblName (str)
                  1 -> (dbTblName, tblIndex, isAutoIncrement, isWithoutROWID, tbl_bitmask, Method_Name) (tuple)
                  db_table_name: returns tblName for the db_table_name provided. Ignores the other settings.
    @return: tableName (str), tuple (dbTblName, tblIndex, AutoIncrement, isWITHOUTROWID, tbl_bitmask) or errorCode (strError)
    """
    if reload:
        getTblName.__reloadTables = True
    if getTblName.__reloadTables:
        _ = reloadTables()
        if isinstance(_, str):
            db_logger.error(f'reloadTables() failed!.')
            return _ if mode != 1 else _, None, None, None, None, None
        getTblName.__reloadTables = False
        db_logger.info(f'reloadTables() function called. return = {_}')

    if db_table_name:
        # Reverse lookup: Busca tblName a partir de dbTblName
        return next((k for k in getTblName.__sysTables if getTblName.__sysTables[k][0].lower() == db_table_name.lower()),
                        "ERR_INP_Invalid table name.")

    if tbl:
        tbl = tbl.strip()
        if tbl[:3].lower() != 'tbl' and tbl != '*':      # Va '*' por las dudas (futuras extensiones a tablas)
            retValue = f'ERR_INP_InvalidArgument: {tbl}'
            return retValue if mode != 1 else retValue, None, None, None, None, None

        if tbl in getTblName.__sysTables:
            if mode == 1:
                retValue = tuple(getTblName.__sysTables[tbl])
            else:
                retValue = getTblName.__sysTables[tbl][0]
            return retValue

        else:   # tblName not found: Primero re-carga tabla en Memoria y busca tabla no encontrada en __sysTables dict
            _ = reloadTables()
            if isinstance(_, str):
                db_logger.error(f'reloadTables() failed!. error: {_}')
                return _ if mode != 1 else _, None, None, None, None, None

            getTblName.__reloadTables = False
            db_logger.info(f'reloadFields() function called.')
            if tbl in getTblName.__sysTables:
                retValue = getTblName.__sysTables[tbl][0] if not mode else tuple(getTblName.__sysTables[tbl])
            else:
                retValue = f'ERR_INP_TableNotFound {tbl} - {callerFunction(2, getCallers=True, namesOnly=True)}'
                db_logger.info(retValue)
            return retValue
    else:
        retValue = f'ERR_INP_InvalidArgument: {tbl}'
        db_logger.info(retValue)
    return retValue if mode != 1 else retValue, None, None, None, None, None

# Static variables for getTblName
getTblName.__sysTables = {}            # Diccionario con {tblName: (dbTblName, tblIndex, AutoIncrement, isWITHOUTROWID)}
getTblName.__sysTablesCopy = {}         # used to access __sysTables while on reload.
getTblName.__reloadTables = True       # reload flag.


def reloadFields():
    """ True: Success / str: errorCode to return back to callers """
    qryObj = SQLiteQuery.getObject()  # Adquiere conexion a  DB, dentro del thread desde donde se llama.
    strSQL = f"SELECT ID_Table, Field_Key_Name, Field_Name, ID_Field, Table_Key_Name, Excluded_Field, Compare_Index" \
             f" FROM '{SYS_FIELDS_NAME}' "
    retVal = None
    try:
        rows = qryObj.execute(strSQL).fetchall()  # (ID_Table, Field_Key_Name, Field_Name, ID_Field, Table_Key_Name)
        if not rows:
            retVal = f'ERR_INP_TableNotFound {SYS_FIELDS_NAME} - {callerFunction()}'
            db_logger.warning(f'{retVal}')
    except (sqlite3.Error, sqlite3.DatabaseError) as e:
        retVal = f'ERR_DBRead: {e} - {callerFunction()}'
        db_logger.error(retVal)
    if retVal:
        return retVal  # Sale si hubo error.

    tblNameList = {j[4] for j in rows}      # set con tableNames de todas las tablas del sistema
    for i in tblNameList:
        tempFldList = [j for j in rows if j[4] == i]
        # Arma dict __sysFields = {tblNameList:{fldName1:(dbFldName, fldIndex, Compare_Index), },}
        getFldName.__sysFieldsCopy[i] = {tempFldList[k][1]: (tempFldList[k][2], tempFldList[k][3], tempFldList[k][6])
                                           for k in range(len(tempFldList))}
        with lock:
            getFldName.__sysFields = getFldName.__sysFieldsCopy.copy()
    return True
    # -------------------------------  Fin reloadFields()  ---------------------------------#


def getFldName(tbl=None, fld='*', mode=0, *, reload=False, db_table_name=None):
    """
    Gets the name of a field by the keynames of a table and a field
    tbl : table keyname (str)
    @param: db_table_name: if passed, ignores tbl. Only works with mode 1.
    fld : field keyname(str)
    mode:    0: Field Name (str: 1 field name - list: multiple field names)
             1: {Field Keyname: Field DBName,} (dict)
             2: {Field Keyname: [Field DBName, Field_Index, Compare_Index],} (dict)
             3: {Field Keyname: ID_Field,} (dict)
    Returns: string: 1 Field; string: ERR_ for Error; list []: Multiple fields
    """
    global __fldNameCounter
    __fldNameCounter += 1        # Contador de ejecucion de esta funcion. Debugging purposes only.
    if reload:
        getFldName.__reloadFields = True

    if getFldName.__reloadFields:
        retValue = reloadFields()
        if isinstance(retValue, (str, type(None))):
            db_logger.info('reloadFields() call failed!.')
            return retValue
        else:
            getFldName.__reloadFields = False           # Atomic operation, or so they say...
            db_logger.info('reloadFields() function called.')

    if db_table_name:
        # Reverse lookup: Busca tblName a partir de dbTblName
        tbl = next((k for k in getTblName.__sysTables if getTblName.__sysTables[k][0].lower() == db_table_name.lower()),
                   '')
        mode = 1
        fld = '*'         # This works only with mode 1 and fld='*'. Ignores any fldNames passed.
    fld = str(fld).strip()
    if fld.lower().startswith('fld') or fld.startswith('*'):  # fldKeyNames son 'fld' o '*'
        tblName = tbl.strip()
        if tblName not in getFldName.__sysFields:
            _ = reloadTables()
            db_logger.info(f'reloadTables() function called. return = {_}')
            _ = reloadFields()
            db_logger.info(f'reloadFields() function called. return = {_}')

        if tblName not in getFldName.__sysFields:
            retValue = f'ERR_INP_TableNotFound: {tbl}'  # Sale si tblName no esta en diccionario __sysFields.
            db_logger.info(retValue)
            return retValue
        else:
            tblDict = getFldName.__sysFields[tblName]       # tblDict = {fldName: (dbFldName, fldIndex), }
            if fld == '*':
                if not mode:
                    retValue = [tblDict[j][0] for j in tblDict]         # List [dbFldName, ]
                elif mode == 1:
                    retValue = {k: tblDict[k][0] for k in tblDict}      # Dict {fldName: dbFldName, }
                elif mode == 2:
                    retValue = tblDict                      # Dict {fldName: (dbFldName, fldIndex, Compare_Index), }
                else:
                    retValue = {k: tblDict[k][1] for k in tblDict}      # Dict {fldName: fldIndex, }
                return retValue
            else:  # Se selecciono 1 solo campo. Lo busca; si no lo encuentra va a recargar tabla de Fields
                if fld in tblDict:
                    # tblDict={fldName: (dbFldName, fldIndex, Compare_Index)}
                    retValue = tblDict[fld][0] if not mode else tblDict[fld]  # (dbFldName, fldIndex, Compare_Index)
                    return retValue
                else:           # No se encontro fieldName, va a regargar tabla de Fields
                    retValue = reloadFields()
                    db_logger.info(f'reloadFields() function called. return = {retValue}')
                    if isinstance(retValue, (str, type(None))):
                        return retValue
                    else:
                        getFldName.__reloadFields = False
                    tblDict = getFldName.__sysFields[tblName]
                    if fld in tblDict:
                        retValue = tblDict[fld][0] if not mode else tblDict[fld]
                    else:
                        retValue = f'ERR_INP_FieldNotFound: {fld} - {callerFunction(2, getcallers=True,namesOnly=True)}'
                        # db_logger.warning(f'NEW getFldName retValue:{retValue}.')
    else:
        retValue = f'ERR_INP_InvalidArgument: {fld}'
        db_logger.info(retValue)
    return retValue

# Static variables for getFldName
# __sysFields = {tblName: {fldName1:(dbFldName, fldIndex), fldName2:(dbFldName, fldIndex), }, }
getFldName.__sysFields = {}
getFldName.__sysFieldsCopy = {}         # used to access __sysFields while on reload.
getFldName.__reloadFields = True          # reload flag.
__fldNameCounter = 0     # Con el atributo getFldName. -> no se puede importar/exportar. Esta se usa en test_threading


def set_reloadFields():
    getFldName.__reloadFields = True
    return None


def getFldCompare(name1: str, name2: str):
    """ Returns Compare_Index if fld1 is comparable to fld2 (they share the same Compare_Index value).
    @param name1: tblName1.fldName1 (str)
    @param name2: tblName2.fldName2 (str)
    @return Compare_Index from [_sys_Fields] if fld1 and fld2 share Compare_Index values. Otherwise False.
    """
    if any(not isinstance(j, str) for j in (name1, name2)):
        return False

    split_name1 = name1.split(".")
    split_name2 = name2.split(".")
    tbl1, fld1 = (split_name1[0], split_name1[1]) if len(split_name1) >= 2 else (None, None)
    tbl2, fld2 = (split_name2[0], split_name2[1]) if len(split_name2) >= 2 else (None, None)
    if any(not i for i in (tbl1, fld1, tbl2, fld2)):        # not i computes to True for None, 0, (), '', {}, [].
        return False
    fldName1 = getFldName(tbl1, fld1, 1)    # retrieves tuple (dbFldName, fldIndex, Compare_Index) for tbl1.fldName1
    fldName2 = getFldName(tbl2, fld2, 1)    # retrieves tuple (dbFldName, fldIndex, Compare_Index) for tbl2.fldName2
    fld1CompIndex = fldName1[2] if isinstance(fldName1, (list, tuple)) else None
    fld2CompIndex = fldName2[2] if isinstance(fldName2, (list, tuple)) else None
    if fld1CompIndex is not None and fld2CompIndex is not None:
        if isinstance(fld1CompIndex, int) and isinstance(fld2CompIndex, int):
            return fld1CompIndex if fld1CompIndex == fld2CompIndex else False
        # elif any(isinstance(j, (tuple, list, set)) for j in (fld1CompIndex, fld2CompIndex)):
        else:
            compVals = [set(j) if isinstance(j, (list,tuple,set,dict)) else {j} for j in (fld1CompIndex, fld2CompIndex)]
            return compVals[0].issubset(compVals[1]) or compVals[1].issubset(compVals[0])
    return False



def getTableInfo(tbl=None, *, db_table_name=None):
    """ Returns column info for tbl in dictionary form
        @param tbl: table name
        @param db_table_name: if passed, ignores tbl and pulls data for db_table_name.
        @return: {'name': 'fldName', 'type': 'fldType', 'notnull': 0/1, 'dflt_value': defValue, 'pk': 0/1}
        @return: get_fld_names=True -> {fldName: dbFldName, }
        """
    # cols = ['cid', 'name', 'type', 'notnull', 'dflt value', 'pk']. cid is Index of the field in that table.
    if db_table_name:
        dbTblName = db_table_name
    elif tbl:
        dbTblName = getTblName(tbl)
    else:
        return {}
    if dbTblName.startswith(strError):
        return f'ERR_INP_Invalid Argument: {tbl}'

    cur = queryObj.execute(f' PRAGMA TABLE_INFO("{dbTblName}")')
    if isinstance(cur, str):
        retValue = f'ERR_SYS_DBAccess. Cannot read from table {dbTblName}. Error: {cur}.'
        db_logger.error(retValue)
        return retValue

    cols = [j[0].lower() for j in cur.description]
    rows = cur.fetchall()
    name_idx = cols.index('name')
    retValue = {rows[j][name_idx]: {cols[i]: rows[j][i] for i in range(1, len(cols))} for j in range(len(rows))} \
                if rows else {}
    return retValue



def createUID(tbl: str, fld: str = ''):
    """
    Returns field UID. A concatenated string of the form 'tblName + uidChar + fldName'
    @param tbl: Valid table name
    @param fld: Valid field name. None or '*' return all fields in the table
    @return: fldUID (str) or tuple (fldUID, ) if fld parameter is '*'. errorCode(str) if invalid parameters.
    """
    fldName = fld.strip()
    # Por ahora este if de abajo no va, para permitir generar UID para campos ad-hoc que NO comienzan con 'fld'.
    # if fldName.lower().startswith('fld') or fldName.startswith('*'):  # fldKeyNames son 'fld' o '*'
    tblName = tbl.strip()
    if tblName not in getTblName.__sysTables:
        return f'ERR_INP_TableNotFound: {tbl}'
    else:
        if not fld or fldName.startswith('*'):
            tblDict = getFldName.__sysFields[tblName]
            return tuple([tblName + uidCh + j for j in tblDict])
        else:
            return tblName + uidCh + fldName


