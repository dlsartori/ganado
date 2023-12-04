# import weakref
import sqlite3
from krnl_exceptions import DBAccessError
from krnl_config import db_logger, krnl_logger, callerFunction,  singleton, MAIN_DB_NAME, strError, MAIN_DB_ID, \
    CLIENT_MIN_INDEX, CLIENT_MAX_INDEX, NEXT_BATCH_MIN_INDEX, NEXT_BATCH_MAX_INDEX, print, DISMISS_PRINT, connCreate, \
    DATASTREAM_DB_NAME, tables_and_binding_objects, tables_and_methods, DB_REPLICATE
from random import randrange

from krnl_sqlite import getFldName, getTblName, SQLiteQuery, set_reloadFields
from threading import Event, Lock, Thread
from krnl_abstract_base_classes import AbstractAsyncBaseClass
# from sqlite_ext import SqliteExtDatabase          # Not used for now. SqliteExtDatabase parent class not found.
try:
    from Queue import Queue, PriorityQueue
except ImportError:
    from queue import Queue, PriorityQueue
try:
    import gevent
    from gevent import Greenlet as GThread
    from gevent.event import Event as GEvent
    from gevent.local import local as greenlet_local
    from gevent.queue import Queue as GQueue
except ImportError:
    GThread = GQueue = GEvent = None

SENTINEL = object()     # Vamos a ver como funciona esto...

_upload_exempted = ['tbl_Upload_To_Server', ]  # list of database tables that must not be uploaded to server.

def update_fldUPDATE(fldUPDATE_dict: dict, db_fld_name, fld_value):
    """ returns a python dictionary ready for JSON serialization. Callback for sqlite UPDATE Trigger.
    @param fldUPDATE_dict: Dictionary originated from JSON, read from fldUPDATE field.
    @param db_fld_name: fldName to add to dictionary
    @param fld_value: field value.
    @return: dictionary to store in fldUPDATE as JSON.
     """
    if isinstance(fldUPDATE_dict, dict):
        fldUPDATE_dict.update({db_fld_name: fld_value})
    return fldUPDATE_dict


class ResultTimeout(Exception):
    pass

class WriterPaused(Exception):
    pass

class ShutdownException(Exception):
    pass

class AsyncCursor(object):
    """ Creates instances of cursor objects with SQL statements to be executed and, upon execution, store
    the data retrieved in a 'result' variable.
        1. The cursors are stored on a queue for execution.
        2. A Writer class object pulls the objects from the queue and executes the SQL statements via sqlite3 module.
            The results are stored back in the cursor object via the set_result method and an Event is set to flag the
            retrieval methods that the results are available to the caller.
        3. On request, the Cursor methods provide the results if available, or wait() until available and return them.
    """
    __slots__ = ('sql', 'params', 'commit', 'timeout', '_event', '_cursor', '_exc', '_idx', '_rows', '_ready', 'tbl',
                 'fldID_idx', 'executemany')

    def __init__(self, event, sql, params, commit, timeout, tbl=None, fldID_idx=None, executemany=False):
        self._event = event
        self.sql = sql
        self.params = params
        self.commit = commit
        self.timeout = timeout
        self._cursor = self._exc = self._idx = self._rows = None
        self._ready = False
        self.tbl = tbl
        self.fldID_idx = fldID_idx
        self.executemany = executemany

    def set_result(self, cursor, exc=None):     # , *, items_written=None):
        self._cursor = cursor
        self._exc = exc
        self._idx = 0
        self._rows = cursor.fetchall() if exc is None else []
        self._event.set()
        return self

    def _wait(self, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        if not self._event.wait(timeout=timeout) and timeout:
            raise ResultTimeout('results not ready, timed out.')
        if self._exc is not None:
            raise self._exc
        self._ready = True

    def __iter__(self):
        if not self._ready:
            self._wait()
        if self._exc is not None:
            raise self._exc
        return self

    def next(self):
        if not self._ready:
            self._wait()
        try:
            obj = self._rows[self._idx]
        except IndexError:
            raise StopIteration
        else:
            self._idx += 1
            return obj
    __next__ = next

    @property
    def lastrowid(self):
        if not self._ready:
            self._wait()
        return self._cursor.lastrowid

    @property
    def rowcount(self):
        if not self._ready:
            self._wait()
        return self._cursor.rowcount

    # @property
    # def items_written(self):
    #     if not self._ready:
    #         self._wait()
    #     return self._items_written

    @property
    def description(self):
        return self._cursor.description

    def close(self):
        self._cursor.close()

    def fetchall(self):
        return list(self)       # Iterating implies waiting until populated.

    def fetchone(self):
        if not self._ready:
            self._wait()
        try:
            return next(self)
        except StopIteration:
            return None

SHUTDOWN = StopIteration
PAUSE = object()
UNPAUSE = object()

# @singleton  # NO MORE @singleton because there can be multiple writer-objects for multiple DBs.
class Writer(object):
    __slots__ = ('database', 'queue')

    def __init__(self, db_SqliteQueueDatabase, queue):
        self.database = db_SqliteQueueDatabase            # database es un objeto de clase SqliteQueueDatabase
        self.queue = queue

    def run(self):
        conn = self.database.connection()
        while True:
            try:
                if conn is None:  # Paused.
                    print(f'aaaahhh: conn is None!. Callers: {callerFunction(getCallers=True)}')
                    if self.wait_unpause():
                        conn = self.database.connection()
                else:
                    conn = self.loop(conn)
            except ShutdownException:
                db_logger.info('writer received shutdown request, exiting.')
                if conn is not None:
                    self.database._close(conn)
                    self.database.reset()           # Hace conn = None
                return

    def wait_unpause(self):
        obj = self.queue.get()
        if obj is UNPAUSE:
            db_logger.info('writer unpaused - reconnecting to database.')
            return True
        elif obj is SHUTDOWN:
            raise ShutdownException()
        elif obj is PAUSE:
            db_logger.error('writer received pause, but is already paused.')
        else:
            obj.set_result(None, WriterPaused())
            db_logger.warning('writer paused, not handling %s', obj)

    def loop(self, conn):
        obj = self.queue.get()          # TODO(cmt): This line waits here until an item is available in queue!
        if isinstance(obj, AsyncCursor):
            self.execute(obj)
        elif obj is PAUSE:
            db_logger.info('writer paused - closing database _conn.')
            self.database._close(conn)
            self.database.reset()            # TODO(cmt): metodos internos no encontrados. _close fue re-escrita
            return
        elif obj is UNPAUSE:
            db_logger.error('writer received unpause, but is already running.')
        elif obj is SHUTDOWN:
            raise ShutdownException()
        else:
            db_logger.error('writer received unsupported object: %s', type(obj))
        return conn

    def execute(self, obj):
        db_logger.debug('received query %s', obj.sql)
        try:
            cursor = self.database._execute(obj.sql, obj.params, obj.commit, tbl=obj.tbl, fldID_idx=obj.fldID_idx,
                                            executemany=obj.executemany)
        except (sqlite3.DatabaseError, sqlite3.Error, DBAccessError, Exception) as execute_err:
            cursor = None
            exc = execute_err  # python3 is so fucking lame.
        else:
            exc = None
        # if obj.executemany:
        #     return obj.set_result(cursor[0], exc, items_written=cursor[1]) # Pasa los fldIDs creados por executemany()
        return obj.set_result(cursor, exc)


# TODO(cmt): DEBE ser singleton, si no, la cosa se tranca...Por el acceso simultaneo a get_max_id().
class SqliteQueueDatabase(AbstractAsyncBaseClass):       #  class SqliteQueueDatabase(SqliteExtDatabase)
    """
    Implements interface to sqlite3 methods in a single-threaded, sequential access to sqlite3 functions, using queue.
    DB Access Model:
       - SQLiteQuery objects are a pool of DB connections to perform READ operations concurrently from different threads
       - SSqliteQueueDatabase objects are unique objects (singleton class) used to WRITE to DB, using a queue logic and
       writing from a single thread.
    """
    __WAL_MODE_ERROR_MESSAGE = ('SQLite must be configured to use WAL (Write-Ahread Logging) '
                                'journal mode. WAL mode allows readers to continue '
                                ' to access the database while one connection writes to it.')
    # All operations involving write-access to the DB file, which therefore need to be put() in the write queue.
    __writeOperations = ('create', 'insert', 'replace', 'drop', 'update', 'delete')  # linea mia.
    __lock = Lock()      # Se asigna un lock general, que hace falta abajo...
    __sync_db = False
    __SQLite_MAX_ROWS = 9223372036854775807   # Value from sqlite documentation Theoretical software limit: 2**64
    __instance = None
    __initialized = False
    __instances = {}        # {instance_object: initialized(True/False), }

    # def __new__(cls, *args, **kwargs):  # Original __new__()
    #     """  Right way to do singletons. Allows for isinstance, type checks. """
    #     if not cls.__instance:                          # Crea Objeto (solo si no existia)
    #         cls.__instance = super().__new__(cls)       # super(SqliteQueueDatabase, cls).__new__(cls)
    #     return cls.__instance  # Retorna objeto al llamador


    def __new__(cls, *args, **kwargs):  # __new__ func. to create SqliteQueueDatabase objs for multiple database names.
        """Creates 1 SqliteQueueDatabase object for each database passed. Stores them in __instances dict, enabling
        work with multiple databases by all threads in the system via this 'serializer' database access object.
        After creation, always returns the same instance for each of the databases passed, as in a Singleton.
        __instances dict works as a pool of serializer-objects: 1 object for each database.
        """
        for o in cls.__instances:
            if o.dbName == args[0]:     # args[0] is database argument.
                return o                # If an object with the same database name exists, returns that object.
        o = super().__new__(cls)
        cls.__instances[o] = False      # Sets initialized flag to False, for __init__() to execute the 1st time only.
        return o

    def __init__(self, database, *args, use_gevent=False, autostart=False, queue_max_size=None, results_timeout=None,
                 sync_db_across_devices=False, **kwargs):
        # if self.__initialized:                # These 3 commented lines of code used with Original __new__() above.
        #     return
        # self.__initialized = True

        if self.__instances.get(self):
            return
        self.__instances[self] = True       # Sets initialized value in __instances dict to avoid re-initialization.

        kwargs['check_same_thread'] = False
        self.__sync_db = sync_db_across_devices
        # Ensure that journal_mode is WAL. This value is passed to the parent class constructor below.
        pragmas = self._validate_journal_mode(kwargs.pop('pragmas', None))

        # TODO(cmt): Create DB Connection. Saves connection parameters.
        self._database = database
        self._timeout = kwargs.get('timeout', 0)
        self._isolation_level = kwargs.get('isolation_level', None)
        self._detect_types = kwargs.get('detect_types', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self._cached_statements = kwargs.get('cached_statements', 0)
        self._check_same_thread = kwargs.get('check_same_thread', False)        # Debe ser False para correr async??
        self._uri = kwargs.get('uri', None)
        self._factory = kwargs.get('factory', None)
        self._conn = self.connection(database, timeout=self._timeout, isolation_level=self._isolation_level,
                                     detect_types=self._detect_types, cached_statements=self._cached_statements,
                                     check_same_thread=self._check_same_thread, uri=self._uri, factory=self._factory)
        self._autostart = autostart
        self._results_timeout = results_timeout
        self._is_stopped = True
        self._writer = None
        # ------------------------------------ Added code -------------------
        self.__idxLowerLimit = CLIENT_MIN_INDEX if self.__sync_db else 1
        self.__idxUpperLimit = CLIENT_MAX_INDEX if self.__sync_db else self.__SQLite_MAX_ROWS
        self.__idxLowerLimit_Next = NEXT_BATCH_MIN_INDEX if self.__sync_db else 1
        self.__idxUpperLimit_Next = NEXT_BATCH_MAX_INDEX if self.__sync_db else self.__SQLite_MAX_ROWS
        # ---------------------------------------------------------

        # TODO: Ojo con esto. Aqui funciona, pero en el codigo original NO se hace el seteo a WAL.
        cursor = self._conn.cursor()  # Ahora los pragmas, que vienen como un diccionario.
        jm = cursor.execute(f'PRAGMA JOURNAL_MODE; ').fetchone()
        if 'wal' not in str(jm).lower() and pragmas:
            for k in pragmas:
                sql_pragma = f' PRAGMA "{k}" = {pragmas[k]}; '
                cursor.execute(sql_pragma)  # Ejecuta directamente, sin mandar al queue.
        cursor.close()

        # Get different objects depending on the threading implementation.
        self._thread_helper = self.get_thread_implementation(use_gevent)(queue_max_size)  # ThreadHelper(queue_max_size)

        super().__init__()
        # Create the writer thread, optionally starting it.
        self._create_write_queue()
        if self._autostart:
            self.start()

    @property
    def dbName(self):
        return self._database

    # TODO(cmt): Ad-hoc implementation of connection()
    def connection(self, database=None, timeout=0.0, detect_types=0, isolation_level=None, check_same_thread=True,
                                     factory=None, cached_statements=0, uri=False):
        database = database or self._database
        # Detect Types: JSON, TIMESTAMP columns (PARSE_DECLTYPES). Internal processing in sqlite3 via hook calls.
        detect_types = detect_types | sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        factory = factory or self._factory
        if factory:
            conn = sqlite3.connect(database, timeout=timeout, detect_types=detect_types,
                                         isolation_level=isolation_level, check_same_thread=check_same_thread,
                                         factory=factory, cached_statements=cached_statements, uri=uri)
        else:
            conn = sqlite3.connect(database, timeout=timeout, detect_types=detect_types,
                                         isolation_level=isolation_level, cached_statements=cached_statements,
                                         check_same_thread=check_same_thread, uri=uri)
        return conn


    # TODO(cmt): Ad-hoc implementation of _execute()
    # The last_insert_rowid() function returns the ROWID of the last row inserted by the database connection which
    # invoked the function. The last_insert_rowid() SQL function is a wrapper around the sqlite3_last_insert_rowid()
    # C/C++ interface function.
    def _execute(self, sql, params='', commit=True, *, tbl='', fldID_idx=None, executemany=False):
        """                TODO(cmt) ***** Este metodo llama a sqlite3 execute() OR executemany() *****
        IMPORTANTE: Escribe datos de retorno en params (valor de fldID generados por la llamada a executemany() ).
        Logica de uso de tbl, fldID_idx: cuando se pasan (!=None) la func. _execute() corre get_max_id y obtiene el
        indice mas alto de la tabla tbl, y lo agrega a los parametros para ejecutar el INSERT.
        Si falta alguno de los 2, se pasa el strSQL tal como viene a ejecutarse.
        @param sql: strSQL
        @param params: parametros a pasar a execute() o executemany(). Es [] para execute() o [[]] para executemany
        @param commit: No usado
        @param tbl: tblName
        @param fldID_idx: indice de fldID en lista de params.
        """

        """ params_list es una REFERENCIA a params. Se usa este hecho para actualizar params con LOS fldID generados por
        executemany() y pasarlos a la funcion llamadora, para que genere los records a guardar en _Upload_To_Server. """
        params_list = (params, ) if any(not isinstance(j, (list, tuple)) for j in params) else params
        if tbl and fldID_idx is not None:
            # max_id = self.__get_max_id(db_tbl_name=dbTblName, row_count=len(params))  # None si __get_max_id() falla.
            if 'update' not in sql.lower():         # Solo entra a setear fldID si NO es UPDATE.
                if self.__sync_db:
                    dbTblName, tblIndex, selfIncrement, withoutROWID, bitmask, methodName = getTblName(tbl, 1)
                    # selfIncrement means the table has INTEGER PRIMARY KEY and increments when ROWID=None is passed.
                    max_id = self.__get_max_id(db_tbl_name=dbTblName, row_count=len(params_list))  # None si get_max_id falla
                    # Asigna valor a fldID para crear registro nuevo. Si __get_max_id() falla, va por autoincremento.
                    for count, j in enumerate(params_list):
                        j[fldID_idx] = max_id+1+count if (max_id is not None and tbl not in _upload_exempted) else None
                # else:
                #     # Cuando __sync_db=False => fldID=None -> fuerza autoincrement de la tabla. Respeta valor si hay.
                #     for count, j in enumerate(params_list):
                #         j[fldID_idx] = None if (withoutROWID is False or tbl in _upload_exempted)\
                #                             else (max_id + 1 + count if max_id is not None else None)
                # TODO(cmt): Actualiza param para que func. llamadora acceda a los fldID generados por executemany()
                # if any(not isinstance(j, (list, tuple)) for j in params):
                #     params = params_list[0]  # Esto debiera ser todo lo que se necesita.... Ma' ver????
                # else:
                #     params = params_list   # Esta linea NO DEBIERA necesitarse.TODO: CHEQUEAR si hace falta.

        with self._conn:        # Doing this because of the commit()s to be performed. Need to delve deeper into it.
            try:
                if not executemany:
                    cur = self._conn.execute(sql, params)        # TODO(cmt): Actual call to sqlite3.execute()
                    # The cur.rowcount below works ONLY because a fetch()/fetchall() was executed in AsyncCursor code.
                    if cur.rowcount <= 0:  # Valida records escritos. Si hay error, genera Exception.
                        raise sqlite3.DatabaseError(f'ERR_DBAccess: setRecords()-execute() call failed. Data could'
                                                    f' not be written. sql={sql}.')
                else:
                    cur = self._conn.executemany(sql, params_list)    # TODO(cmt): Actual call to sqlite3.executemany()
                    # The cur.rowcount below works ONLY because a fetch()/fetchall() was executed in AsyncCursor code.
                    if cur.rowcount != len(params_list):  # Valida records escritos. Si hay error, genera Exception.
                        raise sqlite3.DatabaseError(f'ERR_DBAccess: setRecords()-executemany() call failed. Data could'
                                                    f' not be written. sql={sql}.')
            except (sqlite3.Error, sqlite3.DatabaseError, sqlite3.OperationalError, Exception) as e:
                self._conn.rollback()
                cur = f'ERR_DBAccess: AsyncCursor _execute() error: {e}.'
                db_logger.critical(cur)
                print(cur, dismiss_print=DISMISS_PRINT)
                raise DBAccessError(f'SQLiteQueueDataBase Exception: {cur}')      # NO eliminar este raise !!
        return cur          # type(cur) = str when returning write error.


    # ------------------------------------------ Added Code ---------------------------------------------

    # TODO(cmt): re-entry should NOT be an issue with this function as access to it is 'serialized' by the write queue.
    #  Hence, locks shouldn't be required. This assumes that __get_max_id is called only from SqliteQueueDatabase class.
    #  There is a BIG time distance between fetching an idx and writing a record to DB->This method CANNOT BE REENTRANT!
    def __get_max_id(self, tbl=None, *, row_count=1, db_tbl_name=''):
        """
        Returns MAXID for Table and WITHOUT_ROWID for Table if requested (get_without_rowid=True)
        @param tbl: Table Name.
        @param row_count: 1 default (!=1 when used with executemany())
        @param db_tbl_name: DB table name passed instead of plain tblName. If passed, takes precedence over tbl.
        @return: max_id (int) for table. None if function fails and a valid max_id cannot be obtained.
        """
        if db_tbl_name:
            dbTblName = db_tbl_name         # TODO(cmt): OJO: NO CHECKS MADE on db_tbl_name. Must be correct!!
        else:
            dbTblName = getTblName(tbl, 0)
            if strError in dbTblName:
                return None

        # upload-exempted usan todo el rango de indices (desde 1). No escriben en bloques como el resto.
        low_lim = self.__idxLowerLimit if tbl not in _upload_exempted else 1
        upp_lim = self.__idxUpperLimit if tbl not in _upload_exempted else self.__SQLite_MAX_ROWS
        # string para without_rowid=False  (100% de los casos, debiera ser)
        strMaxId = f'SELECT IFNULL(MAX(ROWID),{low_lim}) FROM [{dbTblName}]' + \
                   f' WHERE ROWID >= {low_lim} AND ROWID <= {upp_lim};' if tbl not in _upload_exempted else '; '

        # strMaxId1 = f'SELECT IFNULL(MAX(ROWID),{low_lim}) FROM [{dbTblName}] WHERE ' \
        #            f'ROWID >= {low_lim} AND ROWID <= {upp_lim};'

        # with self.__lock:
        try:        # La ejecucion de este try permite sincronizar la columna oculta ROWID con la fldID declarada.
            cur = self._conn.execute(strMaxId)
            idx = cur.fetchone()[0]
            if self.__sync_db and tbl not in _upload_exempted and idx + row_count > self.__idxUpperLimit:
                self.__idxLowerLimit = self.__idxLowerLimit_Next  # Pasa al siguente batch asignado.
                self.__idxUpperLimit = self.__idxUpperLimit_Next  # Tabla [Index Batches] asignados a cada dbName
                idx = self.__idxLowerLimit - 1        # TODO(cmt): Esta asignacion funciona tambien para executemany()
        except (sqlite3.Error, sqlite3.DatabaseError):
            idx = None

        if idx is None:             # si tbl es WITHOUT ROWID, debe usar dbFldName en vez de ROWID.
            fldID_dbName = getFldName(tbl, 'fldID')
            if strError in fldID_dbName:
                return None         # Retorna None si hay error en obtencion de dbFldName.
            # string para without_rowid=True  (Solo tablas especificamente creadas como WITHOUT ROWID. NO debiera haber)
            strMaxId = f'SELECT IFNULL(MAX([{fldID_dbName}]),{low_lim}) FROM [{dbTblName}]' +\
                       f' WHERE [{fldID_dbName}] >= {low_lim} AND [{fldID_dbName}] <= {upp_lim};' \
                        if tbl not in _upload_exempted else '; '

            # strMaxId2 = f'SELECT IFNULL(MAX([{fldID_dbName}]),{low_lim}) FROM [{dbTblName}] WHERE ' \
            #            f'[{fldID_dbName}] >= {low_lim} AND [{fldID_dbName}] <= {upp_lim};'

            try:     # Este try se ejecuta si tbl NO es WITHOUT ROWID. (No debiera haber ninguna de esas en DB)
                cur = self._conn.execute(strMaxId)
                idx = cur.fetchone()[0]
                if self.__sync_db and tbl not in _upload_exempted and idx + row_count > self.__idxUpperLimit:
                    self.__idxLowerLimit = self.__idxLowerLimit_Next  # Pasa al siguente batch asignado.
                    self.__idxUpperLimit = self.__idxUpperLimit_Next  # Tabla [Index Batches] asignados a cada dbName
                    idx = self.__idxLowerLimit - 1
            except (sqlite3.Error, sqlite3.DatabaseError):
                pass    # idx = None

        return idx


    def _close(self, conn):
        try:
            _ = conn.execute('PRAGMA OPTIMIZE; ')       # Se asigna para esperar la ejecucion del PRAGMA.
            conn.close()
        except sqlite3.Error as e:
            print(f'Exception. Error in _close(): {e}', dismiss_print=DISMISS_PRINT)
            db_logger.error(f'{db_logger.name}: ERR_DBAccess. Error code: {e}')
            raise sqlite3.DatabaseError(f'Exception: error in _close(). Error code: {e}')

    def reset(self):
        self._conn = None
        # self.closed = True
        # self.ctx = []
        # self.transactions = []

    def get_thread_implementation(self, use_gevent):
        return GreenletHelper if use_gevent else ThreadHelper

    def _create_write_queue(self):
        self._write_queue = self._thread_helper.queue()

    def queue_size(self):
        return self._write_queue.qsize()

    def _validate_journal_mode(self, pragmas=None):
        if not pragmas:
            return {'journal_mode': 'wal'}
        if not isinstance(pragmas, dict):
            pragmas = dict((k.lower(), v) for (k, v) in pragmas)
        if pragmas.get('journal_mode', 'wal').lower() != 'wal':
            raise ValueError(self.__WAL_MODE_ERROR_MESSAGE)
        pragmas['journal_mode'] = 'wal'
        return pragmas


    # @timerWrapper(iterations=30)          # 40 - 80 usec avg for 30 iterations.
    def execute_sql(self, sql, params='', commit=False, timeout=None, *, tbl=None, fldID_idx=None):
        """ execute method for external calls. Puts the cursor object on the queue to be processed by the writer
            This is the interface call for setRecord() and setRecords() methods.
            In order to run sqlite3.executemany(), set argument executemany=True.
        """
        if sql.lower().startswith('select'):
            return self._execute(sql, params)
        if any(j in sql.lower() for j in self.__writeOperations):
            # Operacion de escritura -> crea AsyncCursor y lo pone en queue
            cursor = AsyncCursor(
                event=self._thread_helper.event(),
                sql=sql,
                params=params,
                commit=commit,  # commit is False for SQLite, according to the docs.
                timeout=self._results_timeout if timeout is None else timeout,
                tbl=tbl, fldID_idx=fldID_idx,
                executemany=False
            )
            # print(f' AsyncCursor init. {tbl}, fldID_idx={fldID_idx}')
            self._write_queue.put(cursor)
            return cursor
        else:
            if all(j in sql.lower().strip() for j in ('pragma', 'journal_mode')):
                sql = 'PRAGMA JOURNAL_MODE; '  # Won't set JOURNAL_MODE as db access breaks if JM != WAL. Only queries.
            try:
                qryObj = SQLiteQuery()          # Busca un qryObj del thread llamador (Pool de connections).
                return qryObj.execute(sql, params)  # Esta linea sera reemplazada.
            except sqlite3.Error as e:
                # Sale con Exception si el thread del caller no es el mismo que aquel en que la conexion fue creada,
                # o aparece cualquier otro error en la ejecucion del string sql.
                db_logger.error(f'Exception in {self.__class__.__name__}.execute_sql(). Error code: {e}')
                raise sqlite3.DatabaseError(f'Exception in {self.__class__.__name__}.execute_sql(). Error code: {e}')


    def executemany_sql(self, sql, params='', commit=False, timeout=None, *, tbl=None, fldID_idx=None):
        """ execute method for external calls. Puts the cursor object on the queue to be processed by the writer
                    This is the interface call for setRecord() and setRecords() methods.
                    In order to run sqlite3.executemany(), set argument executemany=True.
                """
        # if commit is SENTINEL:        # This line doesn't make sense in this context. Left only for reference...
        # if not any(sql.lower().__contains__(j) for j in self.__writeOperations):       # TODO(cmt): linea mia.:
        if any(j in sql.lower() for j in self.__writeOperations):
            # Operacion de escritura -> crea AsyncCursor y lo pone en queue
            cursor = AsyncCursor(
                event=self._thread_helper.event(),
                sql=sql,
                params=params,
                commit=commit,  # commit is False for SQLite, according to the docs.
                timeout=self._results_timeout if timeout is None else timeout,
                tbl=tbl, fldID_idx=fldID_idx,
                executemany=True
            )
            # print(f' AsyncCursor init. {tbl}, fldID_idx={fldID_idx}')
            self._write_queue.put(cursor)
            return cursor
        else:
            if all(j in sql.lower().strip() for j in ('pragma', 'journal_mode')):
                sql = 'PRAGMA JOURNAL_MODE; '                   # Won't set JOURNAL_MODE for now. Only queries.
            try:
                qryObj = SQLiteQuery()  # Busca un qryObj del thread llamador (Pool de connections).
                return qryObj.execute(sql, params)  # Esta linea sera reemplazada.
            except sqlite3.Error as e:
                # Sale con Exception si el thread del caller no es el mismo que aquel en que la conexion fue creada,
                # o aparece cualquier otro error en la ejecucion del string sql.
                db_logger.error(f'Exception in {self.__class__.__name__}.execute_sql(). Error code: {e}')
                raise sqlite3.DatabaseError(f'Exception in {self.__class__.__name__}.execute_sql(). Error code: {e}')


    def start(self):
        with self.__lock:
            if not self._is_stopped:
                return False        # Si el writer esta andando (_is_stopped=False), la llamada a start() retorna False.

            def write_spooler():
                writer = Writer(self, self._write_queue)  # Crea el objeto writer. Pasa el objeto AsyncCursor y el queue
                writer.run()               # Con esta llamada entra al loop de extraccion de datos del queue

            self._writer = self._thread_helper.thread(write_spooler)  # write_spooler es el target del thread.
            self._writer.start()                                # TODO(cmt): Aqui arranca el writer thread.
            self._is_stopped = False
            return True

    def stop(self):
        db_logger.info('db writer environment stop requested.')
        # print(f'AHHHHHHHHHHHHHHHHHHH I AM THE DB WRITER. I AM STOPPING NOW...')
        with self.__lock:
            if self._is_stopped:  # buffer_writers_stop_events is set -> inserta SHUTDOWN en queue.
                return False

            if self._database in self.buffer_writers_stop_events:
                #  TODO(cmt): self.buffer_writers_stop_events[self._database].clear() NO DEBE IR AQUI porque event esta
                #   ligado SOLAMENTE al estado run/stop de AsyncBuffers y Event set()/clear() se setean desde ahi.
                if not self.buffer_writers_stop_events[self._database].is_set():
                    # if buffer_writers_stop_events NOT set there are still open buffers writing to db. Cannot shutdown.
                    db_logger.info('Other db-accessing threads are still alive and writing data to the database.'
                                   ' Please close all other threads first.')
                    # TODO: remove this line below after testing is done.
                    krnl_logger.info('Other threads are still alive and writing data to the database.'
                                     ' Please terminate all other database-accessing threads first.')
                    return False

            krnl_logger.info(f'stop(): passing for {self._database} async write object.')
            self._write_queue.put(SHUTDOWN)
            self._is_stopped = True
        self._writer.join()     # join() fuerza al thread que lanzo el db writer a esperar que se procese SHUTDOWN.
        return True


    def is_stopped(self):
        with self.__lock:
            return self._is_stopped

    @classmethod
    def stop_all_writers(cls):          # Stops all objects found in dict cls.__instances.
        for o in cls.__instances:
            if not o.is_stopped():
                o.stop()
        return None

    def pause(self):
        with self.__lock:
            self._write_queue.put(PAUSE)

    def unpause(self):
        with self.__lock:
            self._write_queue.put(UNPAUSE)

    def __del__(self):
        if not self.is_stopped():
            _ = self.execute_sql('PRAGMA OPTIMIZE; ')       # Asignacion para forzar a esperar resultado del PRAGMA
            # _ = self.execute_sql('PRAGMA JOURNAL_MODE = DELETE; ')  # TODO(cmt): Esta linea da error
            self._close(self._conn)
            db_logger.info(f'{self.__class__.__name__}.__del__(): Removed obj={id(self)}.', end=' ')
            print(f'{self.__class__.__name__}.__del__(): Removed obj={id(self)}.', end=' ',
                  dismiss_print=DISMISS_PRINT)
            print(f' ...And also closing connection.', dismiss_print=DISMISS_PRINT)
        else:
            print('\n', dismiss_print=DISMISS_PRINT)

    def __unsupported__(self, *args, **kwargs):
        raise ValueError('This method is not supported by %r.' % type(self))
    atomic = transaction = savepoint = __unsupported__


class ThreadHelper(object):
    __slots__ = ('queue_max_size',)

    def __init__(self, queue_max_size=None):
        self.queue_max_size = queue_max_size if isinstance(queue_max_size, int) else None

    def event(self): return Event()                 # Crea un objeto Event y lo retorna.

    def queue(self, max_size=None):                 # Crea un objeto Queue y lo retorna.
        max_size = max_size if isinstance(max_size, int) else self.queue_max_size
        return Queue(maxsize=max_size or 0)

    def thread(self, fn, *args, **kwargs):          # Crea un objeto Thread y lo retorna.
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.daemon = True   # daemon=True -> __main__ NO espera a que este thread termine si no se hace join().
        return thread

class GreenletHelper(ThreadHelper):
    __slots__ = ()

    def event(self): return GEvent()

    def queue(self, max_size=None):
        max_size = max_size if max_size is not None else self.queue_max_size
        return GQueue(maxsize=max_size or 0)

    def thread(self, fn, *args, **kwargs):
        def wrap(*a, **k):
            gevent.sleep()
            return fn(*a, **k)
        return GThread(wrap, *args, **kwargs)

# --------------------------------------------- END AsyncCursor --------------------------------------------------- #


def init_db_replication_triggers():
    """
    Reads data from _sys_Fields table and sets INSERT and UPDATE Triggers for all rows with 'Method_Name' != None.
    @return: None
    """
    con = connCreate()
    sql = "SELECT ID_Table, Table_Name, Table_Key_Name, Method_Name FROM '_sys_Tables' WHERE Method_Name IS NOT NULL; "
    cur = con.execute(sql)
    tbl_list = cur.fetchall()
    trigger_tables_dict = {}  # {dbTblName: Method_Name,} Method_Name: method to process table replication across nodes.
    if tbl_list:
        trigger_tables_dict = {tbl_list[i][1]: tbl_list[i][3] for i in range(len(tbl_list)) if
                               isinstance(tbl_list[i][3], str)}
    if trigger_tables_dict:
        for k in trigger_tables_dict:
            flds_dict = getFldName(db_table_name=k)     # Gets FULL field structure as dict {fldName: dbFldName, }
            for f in ('fldUPDATE', 'fldDB_ID', 'fldPushUpload', 'fldBitmask', 'fldTimeStampSync'):
                if f in flds_dict:
                    flds_dict.pop(f, None)
            update_fields_list = list(flds_dict.values())
            update_str = trigger_on_update(k, update_fields_list)       # returns None if table name not found.
            if update_str:
                con.execute(update_str)
            insert_str = trigger_on_insert_delete(k)
            if insert_str:
                con.execute(insert_str)
    con.close()
    retValue = trigger_tables_dict
    return retValue


def trigger_on_update(tblName: str, col_list=None):
    """ Trigger on UPDATE: for fields selected in col_list increments value of field Record UPDATE on every UPDATE. """
    dbTblName = getTblName(tblName)
    if strError in str(dbTblName):
        if strError in getTblName(db_table_name=tblName.strip()):
            return None
        else:
            dbTblName = tblName

    if col_list and isinstance(col_list, (tuple, list)):
        col_list_str = 'OF '
        for idx, col in enumerate(col_list):
            col_list_str += f'"{col}"' + (', ' if idx != len(col_list)-1 else '')
    else:
        col_list_str = ''

    con = connCreate()
    cur = con.execute("SELECT DB_Table_Name FROM '_sys_Trigger_Tables'; ")
    tbl_list = [j[0] for j in cur.fetchall() if j] or []     # fetchall() returns [(tbl1,), (tbl2,), (tbl3), ]
    fldNames = getFldName(db_table_name=dbTblName)      # returns {fldName: dbFldName, }
    if "Record UPDATE" not in fldNames.values() and dbTblName not in tbl_list:
        return None

    return f'CREATE TRIGGER IF NOT EXISTS "Trigger_{dbTblName}_UPDATE" AFTER UPDATE {col_list_str}' \
           f' ON "{dbTblName}" FOR EACH ROW BEGIN ' + \
           (f' UPDATE "{dbTblName}" SET "Record UPDATE" = IFNULL(old."Record UPDATE",' 
            f' {randrange(1000, 5000) * randrange(100, 1000)}) + 1 WHERE ROWID=new.ROWID;'
            if "Record UPDATE" in fldNames.values() else '') + \
           (f' UPDATE _sys_Trigger_Tables SET TimeStamp=DATETIME("now","localtime"), Last_Updated_By="{MAIN_DB_ID}"'
            f' WHERE DB_Table_Name="{dbTblName}";'
            if dbTblName in tbl_list else '') + \
           f' END; '

    # return f'CREATE TRIGGER IF NOT EXISTS "Trigger_{dbTblName}_UPDATE" AFTER UPDATE {col_list_str}' \
    #        f' ON "{dbTblName}" FOR EACH ROW BEGIN ' + \
    #        (f' UPDATE "{dbTblName}" SET "Record UPDATE" = IFNULL(old."Record UPDATE",'
    #         f' {randrange(1000, 5000) * randrange(100, 1000)}) + 1 WHERE ROWID=new.ROWID;'
    #         if "Record UPDATE" in fldNames.values() else '') + \
    #        (f' UPDATE _sys_Trigger_Tables SET TimeStamp=DATETIME("now","localtime") WHERE DB_Table_Name="{dbTblName}";' \
    #             if dbTblName in tbl_list else '') + \
    #        f' END; '

    # return f'CREATE TRIGGER IF NOT EXISTS "Trigger_{dbTblName}_UPDATE" AFTER UPDATE {col_list_str}' \
    #        f' ON "{dbTblName}" FOR EACH ROW BEGIN ' + \
    #        (f' UPDATE "{dbTblName}" SET "Record UPDATE" = IFNULL(old."Record UPDATE",0) + 1 WHERE ROWID=new.ROWID;'
    #         if "Record UPDATE" in fldNames.values() else '') + \
    #        f' UPDATE _sys_Trigger_Tables SET TimeStamp=DATETIME("now","localtime") WHERE DB_Table_Name="{dbTblName}";' \
    #        + f' END; '


def trigger_on_insert_delete(tblName: str, operation='insert'):
    """ Updates field TimeStamp in table _sys_Trigger_Tables after INSERT, UPDATE or DELETE on table tblName
    @param tblName: valid table name to create INSERT or DELETE Trigger for.
    @param operation: INSERT, DELETE
    """
    if str(operation).lower() not in ('insert', 'delete', 'del'):
        return 'ERR_INP_Invalid Arguments: Operation not valid (operation must be INSERT or DELETE).'
    dbTblName = getTblName(tblName)
    if strError in str(dbTblName):
        if isinstance(tblName, str):
            dbTblName = tblName.strip()
        else:
            return None

    con = connCreate()
    cur = con.execute("SELECT DB_Table_Name FROM '_sys_Trigger_Tables'; ")
    tbl_list = [j[0] for j in cur.fetchall() if j] or []     # fetchall() returns [(tbl1,), (tbl2,), (tbl3), ]
    operation = 'INSERT' if 'ins' in operation else 'DELETE'
    if operation != "INSERT" and dbTblName not in tbl_list:
        return None
    return f'CREATE TRIGGER IF NOT EXISTS "Trigger_{dbTblName}_{operation}" AFTER {operation} ON "{dbTblName}"' \
           f' FOR EACH ROW BEGIN' + \
           (f' UPDATE "{dbTblName}" SET DB_ID=(SELECT DB_ID FROM _sys_db_id LIMIT 1) WHERE ROWID=new.ROWID; '
            if operation == 'INSERT' else '') + \
           (f' UPDATE _sys_Trigger_Tables SET TimeStamp=DATETIME("now","localtime"), Last_Updated_By="{MAIN_DB_ID}"'
            f' WHERE DB_Table_Name="{dbTblName}" AND new.DB_ID != "{MAIN_DB_ID}";'
             if dbTblName in tbl_list else '') + \
            f' END; '

    # return f'CREATE TRIGGER IF NOT EXISTS "Trigger_{dbTblName}_{operation}" AFTER {operation} ON "{dbTblName}"' \
    #        f' FOR EACH ROW BEGIN' + \
    #        (f' UPDATE "{dbTblName}" SET DB_ID=(SELECT DB_ID FROM _sys_db_id LIMIT 1) WHERE ROWID=new.ROWID; '
    #         if operation == 'INSERT' else '') + \
    #        (f' UPDATE _sys_Trigger_Tables SET TimeStamp=DATETIME("now","localtime") WHERE DB_Table_Name="{dbTblName}";' \
    #             if dbTblName in tbl_list else '') + \
    #        f' END; '


def setFldCompareIndex(val: int = None, *, field_list=()):      # Must define function here to be able to use writeObj.
    """
    Sets val (int) as the Compare_Index value in _sys_Fields table for the items listed in field_list.
    @param val:
    @param field_list: (str). Fields list of the form "tblName.fldName".
    @return: True if success (with at least 1 table.field item) or False if nothing is written to _sys_Fields tab.e
    """
    if not isinstance(val, int) or not field_list or not hasattr(field_list, "__iter__"):
        return False
    update_flag = False
    for j in field_list:
        tbl_fld = j.split(".")
        tbl, fld = (tbl_fld[0], tbl_fld[1]) if len(tbl_fld) >= 2 else (None, None)
        if tbl and fld:
            # Validates tbl and fld.
            dbFldName = getFldName(tbl, fld)
            dbTblName = getTblName(tbl)
            if strError in dbTblName or strError in dbFldName:
                continue
            sql = f'UPDATE "_sys_Fields" SET "Compare_Index"={val} WHERE Table_Key_Name={tbl} AND Field_Key_Name={fld}'
            cur = writeObj.execute_sql(sql)       # Send data to be written to db using the sequential write spooler.
            if not update_flag and cur.rowcount > 0:
                update_flag = True
    if update_flag:
        set_reloadFields()                           # Flags system for Fields dictionary to be re-loaded to memory.
        return True
    return False


def init_database():
    if DB_REPLICATE and 'DB_INITIALIZED' not in globals():
        trigger_tables = init_db_replication_triggers()
        print(f'krnl_db_access.py(836): INSERT/UPDATE Triggers created for: {trigger_tables}')
        print(f'krnl_db_access.py(837): tables_and_binding_objs: {tables_and_binding_objects}\n'
              f'tables_and_methods:{tables_and_methods}.')
        globals()['DB_INITIALIZED'] = True
    return None


# Write Obj. para TODAS las escrituras en DB del sistema
writeObj = SqliteQueueDatabase(MAIN_DB_NAME, autostart=True, sync_db_across_devices=False)
# Todas las llamadas posteriores a SqliteQueueDatabase() para crear un objeto, retornara este mismo objeto (singleton)
# OJO: si hubiese mas de un obj writeObj creados por distintos threads, aparecen errores "database locked" (comprobado!)
# Se pueden crear eventualmente multiples writeObjects para multiples archivos DB (1 writeObject por cada archivo DB).

writeObj_DS = SqliteQueueDatabase(DATASTREAM_DB_NAME, autostart=True, sync_db_across_devices=False)























# Source code for reference only: SqliteExtDatabase just does a bunch of initializations and passes args, kwars up,
# apparently to create the db _conn ->

# class SqliteExtDatabase(SqliteDatabase):
#     def __init__(self, database, c_extensions=None, rank_functions=True,
#                  hash_functions=False, regexp_function=False,
#                  bloomfilter=False, json_contains=False, *args, **kwargs):
#         super(SqliteExtDatabase, self).__init__(database, *args, **kwargs)
#         self._row_factory = None
#
#         if c_extensions and not CYTHON_SQLITE_EXTENSIONS:
#             raise ImproperlyConfigured('SqliteExtDatabase initialized with '
#                                        'C extensions, but shared library was '
#                                        'not found!')
#         prefer_c = CYTHON_SQLITE_EXTENSIONS and (c_extensions is not False)
#         if rank_functions:
#             if prefer_c:
#                 register_rank_functions(self)
#             else:
#                 self.register_function(bm25, 'fts_bm25')
#                 self.register_function(rank, 'fts_rank')
#                 self.register_function(bm25, 'fts_bm25f')  # Fall back to bm25.
#                 self.register_function(bm25, 'fts_lucene')
#         if hash_functions:
#             if not prefer_c:
#                 raise ValueError('C extension required to register hash '
#                                  'functions.')
#             register_hash_functions(self)
#         if regexp_function:
#             self.register_function(_sqlite_regexp, 'regexp', 2)
#         if bloomfilter:
#             if not prefer_c:
#                 raise ValueError('C extension required to use bloomfilter.')
#             register_bloomfilter(self)
#         if json_contains:
#             self.register_function(_json_contains, 'json_contains')
#
#         self._c_extensions = prefer_c
#
#     def _add_conn_hooks(self, conn):
#         super(SqliteExtDatabase, self)._add_conn_hooks(conn)
#         if self._row_factory:
#             conn.row_factory = self._row_factory
#
#     def row_factory(self, fn):
#         self._row_factory = fn