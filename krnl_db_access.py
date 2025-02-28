# import weakref
# from __future__ import annotations
import itertools
import sqlite3
from collections.abc import Iterable
import pandas as pd
import numpy as np          # used to split dataframes in chunks for writing to db.
from uuid import uuid4

from krnl_exceptions import DBAccessError
from krnl_config import db_logger, krnl_logger, callerFunction,  singleton, MAIN_DB_NAME, NR_DB_NAME, \
    strError, TERMINAL_ID, print, DISMISS_PRINT, DATASTREAM_DB_NAME, DB_REPLICATE, fDateTime, PANDAS_WRT_CHUNK_SIZE, \
    PANDAS_READ_CHUNK_SIZE
# from random import randrange
from krnl_db_query import _DataBase, db_main, db_nr, SQLiteQuery, getFldName, getTblName, tables_in_query, \
    DBAccessSemaphore, AccessSerializer
from threading import Event, Lock, Thread
from krnl_abstract_base_classes import AbstractAsyncBaseClass
                                                            # Serializer lock for db access on a per-table basis.
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
    __slots__ = ('sql', 'params', 'commit', 'timeout', '_event', '_cursor', '_exc', '_idx', '_rows', '_ready', '_tbl',
                 'executemany')

    def __init__(self, event, sql, params, commit, timeout, tbl=None, fldID_idx=None, executemany=False):
        self._event = event
        self.sql = sql
        self.params = params
        self.commit = commit
        self.timeout = timeout
        self._cursor = self._exc = self._idx = self._rows = None
        self._ready = False
        self._tbl = tbl
        # self.fldID_idx = fldID_idx
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
                    db_logger.info(f'aaaahhh: conn is None!. Callers: {callerFunction(getCallers=True)}')
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
        obj = self.queue.get()          # TODO(cmt): The thread sits here until an item is available in queue!
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
            cursor = self.database._execute(obj.sql, obj.params, obj.commit, executemany=obj.executemany,
                                            tbl_name=obj._tbl)
        except (sqlite3.DatabaseError, sqlite3.Error, DBAccessError, Exception) as execute_err:
            cursor = None
            exc = execute_err  # python3 is so fucking lame.
        else:
            exc = None
        # if obj.executemany:
        #     return obj.set_result(cursor[0], exc, items_written=cursor[1]) # Pasa los fldIDs creados por executemany()
        return obj.set_result(cursor, exc)


# TODO(cmt): DEBE ser singleton, si no, la cosa se tranca..
class SqliteQueueDatabase(AbstractAsyncBaseClass):       #  class SqliteQueueDatabase(SqliteExtDatabase)
    """
    Implements interface to sqlite3 methods in a single-threaded, sequential access to sqlite3 functions, using queue.
    DB Access Model:
       - SQLiteQuery objects are a pool of DB connections to perform READ operations concurrently from different threads
       - SSqliteQueueDatabase objects are unique objects (singleton class) used to WRITE to DB, using a queue logic and
       writing from a single thread.
    This class opens all connections in 'rw' mode.
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
        """Creates 1 SqliteQueueDatabase object and 1 only for each database used. Stores them in __instances dict,
        enabling work with multiple databases by all threads in the system via this 'serializer' database access object.
        After creation, always returns the same instance for each of the databases passed, as in a Singleton.
        __instances dict works as a pool of serializer-objects: 1 object for each database.
        """
        for o in cls.__instances:
            if o.dbName == kwargs.get('db_name'):    # , MAIN_DB_NAME):     # kwargs['db_name'] is database argument.
                return o                # If an object with the same database name exists, returns that object.
        o = super().__new__(cls)
        cls.__instances[o] = False      # Sets initialized flag to False, for __init__() to execute the 1st time only.
        return o

    def __init__(self, *, db_name=None, use_gevent=False, autostart=True, queue_max_size=None, results_timeout=None,
                 sync_db_across_devices=False, **kwargs):
        if self.__instances.get(self, None):
            return

        self.__instances[self] = True       # Sets initialized value in __instances dict to avoid re-initialization.
        kwargs['check_same_thread'] = False
        self.__sync_db = sync_db_across_devices
        # Ensure that journal_mode is WAL. This value is passed to the parent class constructor below.
        pragmas = self._validate_journal_mode(kwargs.pop('pragmas', None))

        # TODO(cmt): Create DB Connection. Saves connection parameters.
        self._database = db_name
        self._timeout = kwargs.get('timeout', 0)
        self._isolation_level = kwargs.get('isolation_level', None)
        self._detect_types = kwargs.get('detect_types', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self._cached_statements = kwargs.get('cached_statements', 0)
        self._check_same_thread = kwargs.get('check_same_thread', False)        # Debe ser False para correr async??
        self._uri = kwargs.get('uri', True)  # Must be True to be able to pass database as f'file:{database}?mode=rw'
        self._factory = kwargs.get('factory', None)
        try:
            self._conn = self.connection(self._database, timeout=self._timeout, isolation_level=self._isolation_level,
                                         detect_types=self._detect_types, cached_statements=self._cached_statements,
                                         check_same_thread=self._check_same_thread, uri=self._uri, factory=self._factory)
        except (sqlite3.Error, sqlite3.OperationalError) as e:
            val = f'ERR_DB_Cannot create connection:{e} - {callerFunction(getCallers=True)}. Database file: {db_name}.'
            self._conn = None
            db_logger.error(val)
            raise DBAccessError(f'{val}')
        else:
            self._autostart = autostart
            self._results_timeout = results_timeout
            self._is_stopped = True
            self._writer = None

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
    db_name = dbName

    # @property
    # def db_slock(self):
    #     return self.__db_access_lock

    # TODO(cmt): Ad-hoc implementation of connection()
    def connection(self, database=None, timeout=0.0, detect_types=0, isolation_level=None, check_same_thread=True,
                                     factory=None, cached_statements=0, uri=None):     # uri=True for mode=rw
        database = database or self._database
        # Detect Types: JSON, TIMESTAMP columns (PARSE_DECLTYPES). Internal processing in sqlite3 via hook calls.
        detect_types = detect_types | sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        factory = factory or self._factory
        if factory:
            conn = sqlite3.connect(f'file:{database}?mode=rw', timeout=timeout, detect_types=detect_types,
                                         isolation_level=isolation_level, check_same_thread=check_same_thread,
                                         factory=factory, cached_statements=cached_statements, uri=True)
        # f'file:{database}?mode=rw'
        else:
            conn = sqlite3.connect(f'file:{database}?mode=rw', timeout=timeout, detect_types=detect_types,
                                         isolation_level=isolation_level, cached_statements=cached_statements,
                                         check_same_thread=check_same_thread, uri=True)
        return conn


    # TODO(cmt): Ad-hoc implementation of _execute()
    # sqlite3.last_insert_rowid() function returns the ROWID of the last row inserted by the database connection which
    # invoked the function. The last_insert_rowid() SQL function is a wrapper around the sqlite3_last_insert_rowid()
    # C/C++ interface function.
    def _execute(self, sql, params='', commit=True, *, executemany=False, tbl_name=None):
        """                TODO(cmt) ***** Este metodo llama a sqlite3 execute() OR executemany() *****
        IMPORTANTE: Escribe datos de retorno en params (valor de fldID generados por la llamada a executemany() ).
        Logica de uso de tbl, fldID_idx: cuando se pasan (!=None) la func. _execute() corre get_max_id y obtiene el
        indice mas alto de la tabla tbl, y lo agrega a los parametros para ejecutar el INSERT.
        Si falta alguno de los 2, se pasa el strSQL tal como viene a ejecutarse.
        @param sql: strSQL
        @param params: parametros a pasar a execute() o executemany(). Es [] para execute() o [[]] para executemany
        @param commit: No usado
        @param tbl_name: table name (starting with 'tbl').
        """
        # params_list es una REFERENCIA a params. Se usa este hecho para actualizar params con LOS fldID generados por
        # executemany() y pasarlos a la funcion llamadora, para que genere los records a guardar en _Upload_To_Server.
        params_list = (params, ) if any(not isinstance(j, (list, tuple)) for j in params) else params
        with self._conn:        # Doing this because of the commit() on __exit__() from the context mgr.
            if tbl_name:        # Some sql statements (PRAGMA, etc.) may not carry tbl_name.
                semaphore = DBAccessSemaphore(tbl_name, db_name=self.db_name)
                # print(f'---------Locks dict({len(tbl_lock._get_resources_dict())}): {tbl_lock._get_resources_dict()}')
                semaphore.acquire(timeout=1)  # Passes on 1st call to acquire(). All subsequent calls on tbl_name wait.
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
                        raise sqlite3.DatabaseError(f'ERR_DBAccess: setrecords()/executemany() call failed. Data could'
                                                    f' not be written. sql={sql}.')
            except (sqlite3.Error, sqlite3.DatabaseError, sqlite3.OperationalError, Exception) as e:
                self._conn.rollback()
                cur = f'ERR_DBAccess: AsyncCursor _execute() error: {e}.'
                db_logger.critical(cur)
                if tbl_name:
                    #     self.__db_access_lock.release(tbl_name=tbl_name)
                    semaphore.release()
                raise sqlite3.DatabaseError(f'SQLiteQueueDataBase Exception: {cur}')      # NO eliminar este raise !!

            if tbl_name:
                semaphore.release()      # Notifies waiting thread that it can now access tbl_name.

        return cur          # type(cur) = str when returning write error.


    # ------------------------------------------ Added Code ---------------------------------------------

    # TODO(cmt): 29-Dec-23 (Mexico) -> FUNCTION IS DEPRECATED. NO LONGER NEEDED as of 4.1.9
    # TODO(cmt): re-entry should NOT be an issue with this function as access to it is 'serialized' by the write queue.
    #  Hence, locks shouldn't be required. This assumes that __get_max_id is called only from SqliteQueueDatabase class.
    #  There is a BIG time distance between fetching an idx and writing a record to DB->This method CANNOT BE REENTRANT!
    # def __get_max_id(self, tbl=None, *, row_count=1, db_tbl_name=''):
    #     """
    #     Returns MAXID for Table and WITHOUT_ROWID for Table if requested (get_without_rowid=True)
    #     @param tbl: Table Name.
    #     @param row_count: 1 default (!=1 when used with executemany())
    #     @param db_tbl_name: DB table name passed instead of plain tblName. If passed, takes precedence over tbl.
    #     @return: max_id (int) for table. None if function fails and a valid max_id cannot be obtained.
    #     """
    #     if db_tbl_name:
    #         dbTblName = db_tbl_name         # TODO(cmt): OJO: NO CHECKS MADE on db_tbl_name. Must be correct!!
    #     else:
    #         dbTblName = getTblName(tbl, 0)
    #         if strError in dbTblName:
    #             return None
    #
    #     # upload-exempted usan todo el rango de indices (desde 1). No escriben en bloques como el resto.
    #     low_lim = self.__idxLowerLimit if tbl not in _upload_exempted else 1
    #     upp_lim = self.__idxUpperLimit if tbl not in _upload_exempted else self.__SQLite_MAX_ROWS
    #     # string para without_rowid=False  (100% de los casos, debiera ser)
    #     strMaxId = f'SELECT IFNULL(MAX(ROWID),{low_lim}) FROM [{dbTblName}]' + \
    #                f' WHERE ROWID >= {low_lim} AND ROWID <= {upp_lim};' if tbl not in _upload_exempted else '; '
    #
    #     # strMaxId1 = f'SELECT IFNULL(MAX(ROWID),{low_lim}) FROM [{dbTblName}] WHERE ' \
    #     #            f'ROWID >= {low_lim} AND ROWID <= {upp_lim};'
    #
    #     # with self.__lock:
    #     try:        # La ejecucion de este try permite sincronizar la columna oculta ROWID con la fldID declarada.
    #         cur = self._conn.execute(strMaxId)
    #         idx = cur.fetchone()[0]
    #         if self.__sync_db and tbl not in _upload_exempted and idx + row_count > self.__idxUpperLimit:
    #             self.__idxLowerLimit = self.__idxLowerLimit_Next  # Pasa al siguente batch asignado.
    #             self.__idxUpperLimit = self.__idxUpperLimit_Next  # Tabla [Index Batches] asignados a cada dbName
    #             idx = self.__idxLowerLimit - 1        # TODO(cmt): Esta asignacion funciona tambien para executemany()
    #     except (sqlite3.Error, sqlite3.DatabaseError):
    #         idx = None
    #
    #     if idx is None:             # si tbl es WITHOUT ROWID, debe usar dbFldName en vez de ROWID.
    #         fldID_dbName = getFldName(tbl, 'fldID')
    #         if strError in fldID_dbName:
    #             return None         # Retorna None si hay error en obtencion de dbFldName.
    #         # string para without_rowid=True  (Solo tablas especificamente creadas como WITHOUT ROWID. NO debiera haber)
    #         strMaxId = f'SELECT IFNULL(MAX([{fldID_dbName}]),{low_lim}) FROM [{dbTblName}]' +\
    #                    f' WHERE [{fldID_dbName}] >= {low_lim} AND [{fldID_dbName}] <= {upp_lim};' \
    #                     if tbl not in _upload_exempted else '; '
    #
    #         # strMaxId2 = f'SELECT IFNULL(MAX([{fldID_dbName}]),{low_lim}) FROM [{dbTblName}] WHERE ' \
    #         #            f'[{fldID_dbName}] >= {low_lim} AND [{fldID_dbName}] <= {upp_lim};'
    #
    #         try:     # Este try se ejecuta si tbl NO es WITHOUT ROWID. (No debiera haber ninguna de esas en DB)
    #             cur = self._conn.execute(strMaxId)
    #             idx = cur.fetchone()[0]
    #             if self.__sync_db and tbl not in _upload_exempted and idx + row_count > self.__idxUpperLimit:
    #                 self.__idxLowerLimit = self.__idxLowerLimit_Next  # Pasa al siguente batch asignado.
    #                 self.__idxUpperLimit = self.__idxUpperLimit_Next  # Tabla [Index Batches] asignados a cada dbName
    #                 idx = self.__idxLowerLimit - 1
    #         except (sqlite3.Error, sqlite3.DatabaseError):
    #             pass    # idx = None
    #
    #     return idx


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
    def execute_sql(self, sql, params='', commit=False, timeout=None, tbl_name=None):
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
                tbl=tbl_name,  # tbl used to setup the access serializer lock.
                # fldID_idx=fldID_idx,
                executemany=False
            )
            # print(f' AsyncCursor init. {tbl}, fldID_idx={fldID_idx}')
            self._write_queue.put(cursor)           # Sends cursor to writer thread, por sequential writes to db.
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


    def executemany_sql(self, sql, params='', commit=False, timeout=None, tbl_name=None):
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
                commit=commit,              # commit is False for SQLite, according to the docs.
                timeout=self._results_timeout if timeout is None else timeout,
                tbl=tbl_name,  # tbl used to setup the access serializer lock.
                # fldID_idx=fldID_idx,
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
            krnl_logger.info(f'stop(): shutting down {self._database} async write object.')
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


def setrecords(chunks: pd.DataFrame | Iterable = None, tbl_name: str = None, *, db_name=None) -> Iterable:
    """
    Writes pandas dataframes to db, queuing the data in the SQLiteQueueDatabase queue for asynchronous execution.
    Splits each dataframe into 2: UPDATE dataframes (rows with fldID = None) and INSERT dataframes (fldID >= 0)
    Executes the writes for all non-empty dataframes.
    @param tbl_name: table key name (starts with "tbl").
    @param chunks: pd.DataFrame object or Iterable.
    @param db_name: Database name where to perform the write (str). Default: None -> resolves to MAIN_DB_NAME.
    @return: sqlite.Cursor tuple of sqlit3.cursors for dataframes written, (cur1, cur2, ).
            Returns empty tuple if nothing is written.
    """
    db_name = db_name if db_name in _DataBase.active_databases() else MAIN_DB_NAME
    write_obj = SqliteQueueDatabase(db_name=db_name)
    if tbl_name:
        db_tbl_name = getTblName(db_table_name=tbl_name, db_name=db_name)
    else:
        # uses data in dataframe to pull table name.
        if isinstance(chunks, pd.DataFrame):
            tbl_name = chunks.db.tbl_name
            db_tbl_name = chunks.db.db_tbl_name
        else:
            ittr = itertools.tee(chunks, 2)
            chunks = ittr[0]
            temp = next(ittr[1])
            tbl_name = temp.db.tbl_name
            db_tbl_name = temp.db.db_tbl_name

    field_names = getFldName(tbl_name, '*', mode=1, db_name=db_name)     # {fldName: dbFldName, }
    if strError in db_tbl_name or not isinstance(field_names, dict):
        raise ValueError(f'ERR_ValueError - setrecords(): invalid database table name: {str(tbl_name)}. '
                         f'Valid table key name required.')
    hidden_cols = SQLiteQuery().execute(f'SELECT name FROM pragma_table_xinfo("{db_tbl_name}") '
                                        f'WHERE hidden > 0; ').fetchall()
    if isinstance(chunks, pd.DataFrame):
        chunks = (chunks, )

    if isinstance(chunks, Iterable):
        for chunk in chunks:
            # # TODO: Some erroneus data fed into dataframe to test UPDATE / INSERT operations. Remove after testing.
            # chunk.loc[0:4, field_names['fldID']] = (None, None, None, None, None)
            # chunk.loc[0:4, field_names['fldObjectUID']] = [uuid4().hex for j in range(5)]

            # 1.  Remove hidden/generated cols to avoid sqlite write errors.
            for col in chunk.columns:
                for c in hidden_cols:
                    if field_names[col] in c:
                        chunk.drop(columns=col, inplace=True)    # removes the hidden/generated col from df.

            # 2. Convert all NaN values to None, to pass to sqlite3.
            chunk.fillna(np.nan).replace([np.nan], [None], inplace=True)

            # 3. sqlite3 does not support numpy.int64. conversion is done via a converter in config.py

            # 4. Remove rows with invalid fldID values (fldID values not int and not None).
            # TODO: Avoid this check. It may be expensive in terms of execution time. Use constraint in db file.
            # chunk = chunk[chunk[field_names['fldID']].apply(lambda x: (isinstance(x, int) or x is None))]

            # 5. Convert all pd.TimeStamp columns to str (sqlite3 converter cannot handle pd.TimeStamp).
            for col in chunk.columns:  # Converts all pd.TimeStamp objects to string.
                if 'datetime' in chunk.dtypes[col].__class__.__name__.lower():
                    chunk[col] = pd.Series(chunk[col].dt.to_pydatetime(), dtype=object)    # uses TimeStamp Series dt Accessor.

            cols = tuple([field_names[k] for k in chunk.columns])  # Must retain column names order.
            # Checks if fldID col exists and if so -> all rows with fldID != None are set for UPDATE operations, while
            # rows with fldID = None are selected for INSERT operations.
            # Splits each chunk dataframe into 2: insert and update, and runs executemany for all dataframes created.
            if 'fldID' in chunk.columns:
                # Creates INSERT and UPDATE dataframes based in fldID value for each row.
                df_update = chunk[chunk['fldID'] >= 0]     # None, nulls evals to False. >=0 eval to True
                df_insert = chunk[~(chunk['fldID'] >= 0)]  # Negate to obtain the NULLs rows for INSERT.

                # FYI only: dataframe resulting from the difference between 2 dataframes (ALL dataframe data compared).
                # df_insert = chunk[~chunk.apply(tuple, 1).isin(df_update.apply(tuple, 1))]

                # FYI only: dataframe resulting from the difference between 2 dataframes, based on column differences.
                # df_insert = chunk[~chunk[chunk.field_names['fldID']].isin(df_update[chunk.field_names['fldID']])]
            else:
                # fldID column not passed in DataFrame -> it's all INSERT rows.
                df_insert = chunk
                df_update = chunk.iloc[0:0]         # No UPDATE rows: Creates empty update dataframe.

            # cols = tuple([field_names[k] for k in chunk.columns])       # Must retain column names order!!
            wrt_dict = {}
            if not df_insert.empty:                                 # Do INSERTs first
                qMarks = ' (' + (len(df_insert.columns) * ' ?,')[:-1] + ') '
                sql_insert = f'INSERT INTO "{db_tbl_name}" ' + str(cols) + ' VALUES' + qMarks + '; '
                if len(df_insert) > PANDAS_WRT_CHUNK_SIZE:
                    df_insert = np.array_split(df_insert, len(df_insert) // PANDAS_WRT_CHUNK_SIZE)
                else:
                    df_insert = (df_insert, )
                wrt_dict[sql_insert] = df_insert             # write dictionary to send data to writer thread.
                # print(f'sql INSERT: {sql_insert}')

            if not df_update.empty:                                 # UPDATES go second.
                sql_update = f'UPDATE "{db_tbl_name}" SET '
                for i in cols:
                    sql_update += f'"{i}"=?, '
                sql_update = sql_update[:-2] + f' WHERE "{field_names["fldID"]}"=? ; '
                # Inserts a duplicate fldID column at the end of the dataframe for the "WHERE fldID = ? "...
                df_update.insert(len(df_update.columns), 'Duplicate_fldID', df_update.loc[:, 'fldID'])
                if len(df_update) > PANDAS_WRT_CHUNK_SIZE:
                    df_update = np.array_split(df_update, len(df_update) // PANDAS_WRT_CHUNK_SIZE)  # func returns list.
                else:
                    df_update = (df_update, )
                wrt_dict[sql_update] = df_update             # write dictionary to send data to writer thread.

            cur_list = []
            for sql, dataframes in wrt_dict.items():        # writes only non-empty dataframes.
                for frame in dataframes:
                    params = frame.values.tolist()
                    # Puts 1 write cursor object in write-queue for dataframe created, and 1 return cur in cur_list.
                    cur = write_obj.executemany_sql(sql, params, tbl_name=tbl_name)
                    cur_list.append(cur)

            # Returns sqlite3 list of cursors based on # of df written. Check cur.rowcount to validate writes.
            # return cur_list[0] if len(cur_list) == 1 else tuple(cur_list)
            return tuple(cur_list)
    else:
        raise TypeError(f'ERR_TypeError: df_setrecords(). Invalid type for {chunks}. Must be iterable or DataFrame.')



# def df_setrecords00(chunks: pd.DataFrame | Iterable) -> Iterable:
#     """
#     Writes pandas dataframes to db, queuing the data in the SQLiteQueueDatabase queue for asynchronous execution.
#     Splits each dataframe into 2: UPDATE dataframes (rows with fldID = None) and INSERT dataframes (fldID >= 0)
#     Executes the writes for all non-empty dataframes.
#     @param chunks: pd.DataFrame object or Iterable.
#     @return: (cur1, cur2) -> Tuple with sqlit3.cursor(s)   #  chunk.loc[0:1, chunk.field_names['fldID']] = None, None
#             Returns empty tuple if nothing is written.
#     """
#     if isinstance(chunks, pd.DataFrame):
#         chunks = (chunks, )
#
#     if isinstance(chunks, Iterable):
#         for chunk in chunks:
#             # Checks if fldID col exists and if so, if it is populated: all rows with fldID != None are for UPDATES. All
#             # fldID = None are for INSERT operations.
#             # Splits each chunk dataframe into 2: insert and update, and runs executemany for all dataframes created.
#             if hasattr(chunk, 'field_names'):
#                 # Writes only dataframes that have a db table structure.
#                 chunk.loc[0:4, chunk.field_names['fldID']] = [None] * 5  # TODO: Some INSERTs. Remove after testing.
#                 chunk.loc[0:4, chunk.field_names['fldObjectUID']] = [uuid4().hex for j in range(5)]
#
#                 db_tbl_name = getTblName(chunk.tbl_name)
#                 if strError in db_tbl_name:
#                     raise ValueError(f'ERR_Invalid Argument setrecords(): Invalid db table name {chunk.tbl_name}.')
#                 wrtbl_cols = getFldName(chunk.tbl_name, '*', mode=1,  exclude_hidden_generated=True)
#                 if strError in wrtbl_cols:
#                     raise ValueError(f'ERR_DBAccess setrecords(): cannot read _sys_Fields table.')
#                 # Cleans dataframe of any sqlite generated / hidden columns (removes the hidden/generated cols).
#                 for col in chunk.columns:
#                     if col not in wrtbl_cols.values():
#                         chunk.drop(columns=col, inplace=True)
#
#                 if 'fldID' in chunk.field_names:
#                     # Creates INSERT and UPDATE dataframes based in fldID value for each row.
#                     df_update = chunk[chunk[chunk.field_names['fldID']] >= 0]  # None evals to False. non-ints->throw error
#                     df_insert = chunk[~(chunk[chunk.field_names['fldID']] >= 0)]
#
#                     # FYI only: df resulting from the difference between 2 dataframes (ALL dataframe data compared).
#                     # df_insert = chunk[~chunk.apply(tuple, 1).isin(df_update.apply(tuple, 1))]
#
#                     # FYI only: df difference between 2 dataframes, base on column differences.
#                     # df_insert = chunk[~chunk[chunk.field_names['fldID']].isin(df_update[chunk.field_names['fldID']])]
#
#                     # print(f'df_insert:\n {df_insert}')
#                     # print(f'df_update:\n {df_update}')
#                 else:
#                     # fldID column not passed in DataFrame -> it's all INSERT rows.
#                     df_insert = chunk
#                     df_update = chunk.iloc[0:0]         # Creates empty update dataframe, for testing condition below.
#
#                 wrt_dict = {}
#                 if not df_update.empty:
#                     sql_update = f'UPDATE "{db_tbl_name}" SET '
#                     for i in df_update.columns:
#                         sql_update += f'"{i}"=?, '
#                     sql_update = sql_update[:-2] + f' WHERE "{chunk.field_names["fldID"]}"=? ; '
#                     # Inserts a duplicate fldID column at the end of the dataframe for the "WHERE fldID = ? "...
#                     df_update.insert(len(df_update.columns), 'Duplicate_fldID',
#                                      df_update.loc[:, chunk.field_names['fldID']])
#                     # df_update['Duplicate_fldID'] = df_update.loc[:, chunk.field_names['fldID']]
#                     # print(f'sql UPDATE: {sql_update}')
#                     wrt_dict[sql_update] = df_update             # write dictionary to send data to writer thread.
#
#                 if not df_insert.empty:
#                     qMarks = ' (' + (len(df_insert.columns) * ' ?,')[:-1] + ') '
#                     sql_insert = f'INSERT INTO "{db_tbl_name}" ' + str(tuple(df_insert.columns)) + \
#                                  ' VALUES' + qMarks + '; '
#                     wrt_dict[sql_insert] = df_insert             # write dictionary to send data to writer thread.
#                     # print(f'sql INSERT: {sql_insert}')
#
#                 cur_list = []
#                 for sql, df in wrt_dict.items():        # writes only non-empty dataframes.
#                     for col in df.columns:
#                         if any(dt in df.dtypes[col].__class__.__name__ for dt in ('date', 'time', 'datetime')):
#                             df[col] = df[col].dt.strftime(fDateTime)    # Converts all pd.TimeStamp objects to string.
#                     params = df.values.tolist()
#                     # Puts 1 write cursor object in write-queue for dataframe created, and 1 return cur in cur_list.
#                     cur = writeObj.executemany_sql(sql, params)
#                     cur_list.append(cur)
#
#                 # Returns list of cursors based on df splits performed. Must check cur.rowcount to test write success.
#                 return tuple(cur_list)
#             raise ValueError(f'ERR_Value: setrecords(). Malformed DataFrame. Cannot be written to database.')
#         raise TypeError(f'ERR_Type: setrecords(). Invalid type for {chunks}. Must be iterable or DataFrame.')


# Write object para TODAS las escrituras en DB del sistema (1 and only 1 writeObj per open database).
writeObj = SqliteQueueDatabase(db_name=MAIN_DB_NAME, autostart=True, sync_db_across_devices=False)
# Todas las llamadas posteriores a SqliteQueueDatabase() para crear un objeto para la base de datos MAIN_DB_NAME
# retornara este mismo objeto (singleton para cada db abierta). Si hubiese mas de un obj writeObj creados por distintos
# threads para una misma db, aparecen errores "database locked" (comprobado!)
# Se pueden crear eventualmente multiples writeObjects para multiples DB (1 writeObject por cada DB abierta).
# writeObj_ds = SqliteQueueDatabase(DATASTREAM_DB_NAME, autostart=True, sync_db_across_devices=False)
# writeObj_nr = SqliteQueueDatabase(NR_DB_NAME, autostart=True, sync_db_across_devices=False)



def init_database():
    if DB_REPLICATE and 'DB_INITIALIZED' not in globals():
        # trigger_tables = init_db_replication_triggers()
        # print(f'krnl_db_access.py(836): INSERT/UPDATE Triggers created for: {trigger_tables}')
        # print(f'krnl_db_access.py(837): tables_and_binding_objs: {tables_and_binding_objects}\n'
        #       f'tables_and_methods:{tables_and_methods}.')
        globals()['DB_INITIALIZED'] = True
    return None

def setFldCompareIndex(val: int = None, *, db_obj=None, field_list=()):
    """
    Sets val (int) as the Compare_Index value in _sys_Fields table for the items listed in field_list.
    @param db_obj: Database object to update Compare Indices table on.
    @param val:
    @param field_list: (str). Fields list of the form "tblName.fldName".
    @return: True if success (with at least 1 table.field item) or False if nothing is written to _sys_Fields table
    """
    db_obj = db_obj if isinstance(db_obj, _DataBase) else db_main
    if not isinstance(val, int) or not field_list or not hasattr(field_list, "__iter__"):
        return False
    update_flag = False
    dbName = db_obj.dbName or MAIN_DB_NAME
    for j in field_list:
        tbl_fld = j.split(".")
        tbl, fld = (tbl_fld[0], tbl_fld[1]) if len(tbl_fld) >= 2 else (None, None)
        if tbl and fld:
            # Validates tbl and fld.
            dbFldName = getFldName(tbl, fld, db_obj=db_obj)
            dbTblName = getTblName(tbl, db_obj=db_obj)
            if strError in dbTblName or strError in dbFldName:
                continue
            sql = f'UPDATE "_sys_Fields" SET "Compare_Index"={val} WHERE Table_Key_Name={tbl} AND Field_Key_Name={fld}'
            cur = writeObj.execute_sql(sql, tbl_name='_sys_Fields')  # Send data to be written to db using the sequential write spooler.
            if not update_flag and cur.rowcount > 0:
                update_flag = True
    if update_flag:
        db_obj.set_reloadFields()  # Flags system for Fields dictionary to be re-loaded into memory.
        return True
    return False


