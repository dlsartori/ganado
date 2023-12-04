import os
import logging
from logging import Logger
import typing
from money import Money
from logging.handlers import RotatingFileHandler
import functools
import builtins
from uuid import UUID, uuid4, uuid1
from datetime import datetime, timedelta
from time import time, monotonic, perf_counter, perf_counter_ns
import re
import inspect
from inspect import currentframe
from sys import _getframe, modules, setswitchinterval, getswitchinterval, argv
import ntplib
import json
from uuid import UUID
from json import JSONEncoder, JSONDecodeError
from krnl_exceptions import DBAccessError
import sqlite3
from decimal import Decimal
from deepdiff import DeepDiff
# PRINT_MAX_LEN = 160
# VERBOSE = 0             # Parametro para habilitar/deshabilitar avisos a pantalla de ciertas funciones

fDateTime = "%Y-%m-%d %H:%M:%S.%f"   # Este formato se usa en todos los casos, fDate, fTime no -> son obsoletos.
strError = 'ERR_'

THREAD_SWITCH_INTERVAL = getswitchinterval()   # 5 msec work way much better than 10 msec. Leave this alone..
# TODO(cmt): thread switch_interval (default=0.005) is used by BufferWriter class to prioritize thread execution.
setswitchinterval(THREAD_SWITCH_INTERVAL)

MAIN_DB_NAME = "GanadoSQLite.db"
DATASTREAM_DB_NAME = "GanadoSQLite_DS.db"
MAIN_DB_ID = None                 # Initialized by baptize_db() function.
MAX_GOBACK_DAYS = 721              # Days to go back in time to pull records from specific tables.
SQLite_MAX_ROWS = 9223372036854775807   # Value from sqlite documentation Theoretical software limit: 2**64
CLIENT_MIN_INDEX = 0              # Seteado por el Server al registrar y habilitar el Client. Estos valores vienen de DB
CLIENT_MAX_INDEX = 268435456      # 2^28: Max Index available to this client to create new records. Set by the Server.
NEXT_BATCH_MIN_INDEX = 268435456 + 1   # Guarda datos de 1 batch adicional por continuidad. Estos valores vienen de DB
NEXT_BATCH_MAX_INDEX = NEXT_BATCH_MIN_INDEX + 268435456         # Estos valores vienen de DB

# DB_SYNC = False          # Flags DB synchronization across devices.
BIT_UPLOAD = 1          # Record Bitmask definitions
BIT_SYNC = 2

tables_and_binding_objects = {'Animales': 'Animal', 'Caravanas': 'Tag', 'Personas': 'Person', 'Geo Entidades': 'Geo',
                              'Dispositivos': 'Device', 'Animales Registro De Actividades': 'Animal',
                              'Animales Registro De Actividades Programadas': 'Animal',
                              'Caravanas Registro De Actividades': 'Tag',
                              'Dispositivos Registro De Actividades': 'Device',
                              'Dispositivos Registro De Actividades Programadas': 'Device',
                              'Personas Registro De Actividades': 'Person',
                              'Personas Registro De Actividades Programadas': 'Person'}

ERROR = object()
VOID = object()
NULL = object()

class ShutdownException(Exception):
    __message = 'ShutdownException Message!'
    def __init__(self, message=None, errors=None):
        super().__init__(message or self.__message)
        self.errors = errors

""" 
How logging works:
Internally, messages are turned into LogRecord objects and routed to a Handler object registered for this krnl_logger. 
The handler will then use a Formatter to turn the LogRecord into a string and emit that string.
In general a module should emit log messages as a best practice and should not configure how those messages are handled. 
That is the responsibility of the application.
"""

FRONT_END_LOGGER = 'fe_logger'
KERNEL_LOGGER = 'krnl_logger'
BACKGROUND_LOGGER = 'bkgd_logger'
DB_LOGGER = 'db_logger'
CONSOLE_LOGGER = 'con_logger'
URL_LOGGER = 'url_logger'

sessionActiveUser = 1           # Usuario Activo de la sesion
activityEnableDisabled = 0
activityEnableBasic = 2         # Todos estos niveles a definir
activityEnableIntermediate = 4
activityEnableAdvanced = 6
activityEnableFull = 10         # Full enable para actividades. 0 -> Activity disabled.

uidCh = '__'  # Used in Signature and Notifications to create unique field names. Chars MUST be ok with use in DB Names.
len_uidCh = len(uidCh)
oprCh = '__opr'     # Particle added to "fldName" fields in DataTables to store operators strings belonging to "fldName"
len_oprCh = len(oprCh)
fabCh = 'fab#'      # Particle used to define system-created (ad-hoc) "fieldnames". TODO(cmt): this is not be needed..
len_fabCh = len(fabCh)
sigCh = '-'         # Particle used to create signature: '_tblRAPName + sigCh + idActivityRAP'
len_sigCh = len(sigCh)

# ------------------------------------ CODE DEVELOPMENT PARAMETERS ------------------------------------------------- #

def parse_cmd_line(arg, arg_list=()):
    """ Checks for param in command line (passed via argv or via arg_list) and returns param value.
    @param arg: argument to check for (str)
    @param arg_list: Iterable to use for parsing alternatively to argv (tuple, list or set)
    @return: value assigned to param (str). If value converts to float, returns float(value).
             if value is 'True' or 'False' returns True/False
             None if param is not found in command line or val not provided.
    """
    if not isinstance(arg, str):
        return None

    arg_list = arg_list if arg_list and isinstance(arg_list, (list, tuple, set)) else argv
    if arg_list and isinstance(arg_list, (list, tuple, set)):
        arg = arg.strip().lower()
        for j in arg_list:
            if arg in j.lower().strip():
                char = next((i for i in j if i in ('=', ':', ' ')), None)     # valid separators: '=', ':', ' '.
                if char is None:
                    val = None
                else:
                    try:
                        val = j[j.find(char) + 1:].strip()
                    except IndexError:
                        val = None
                    else:
                        if val.lower() == 'false':
                            val = False
                        elif val.lower() == 'true':
                            val = True
                        else:
                            try:
                                val = float(val)
                            except (TypeError, ValueError):
                                pass
                return val
    return None

DEBUG_MODE = parse_cmd_line('debug') if parse_cmd_line('debug') is not None else True
DISMISS_PRINT = parse_cmd_line('dismiss_print') or False
USE_DAYS_MULT = parse_cmd_line('days_mult') or False
DB_REPLICATE = parse_cmd_line('db_replicate') or True       # Enables database replication by default

SPD = 3600 * 24                 # Seconds per day
SPH = 3600                     # Seconds per hour
DAYS_MULT = 60  # Dias/segundo.TODO: Modifica EntityObj.updateTimeOut(),krnl_bovine_activity.computeCategory(). SOLO ESO!
__simulationYears = 3
sleepAmount = __simulationYears * 365 / DAYS_MULT
MIN_TIMEOUT_DAYS = 1         # 1 day
MAX_TIMEOUT_DAYS = 50 * 365    # Maximum Timeout time: 50 years.
INTERVAL_MINUTE = 60
INTERVAL_HOUR = 3600
INTERVAL_DAY = 3600 * 24
INTERVAL_WEEK = INTERVAL_DAY * 7

# -------------------------------------END CODE DEVELOPMENT PARAMETERS -------------------------------------------- #


BKGD_WRITE_ATTEMPTS = 3       # Numero de intentos de escritura en DB desde background, antes de abortar la operacion

def moduleName():
    return str(os.path.basename(__file__))

def lineNum():
    return currentframe().f_back.f_lineno

"""                                            **** LOGGING ****
How logging works:
Internally, messages are turned into LogRecord objects and routed to a Handler object registered for this krnl_logger. 
The handler will then use a Formatter to turn the LogRecord into a string and emit that string.
In general a module should emit log messages as a best practice and should NOT configure how those messages are handled. 
That is the responsibility of the application.
"""
def getLogger(*, loggername=None, logfile=None, targets_levels=None, urls='', error_file=False, debug=False) -> Logger:
    """ Returns Logger with 2 or more  handlers: Console handler set to INFO level or lower, File handlers set to
    levels defined in target_levels dictionary.
    All files created in dict1 folder (./)
    @param debug: True -> Include a debug file with all DEBUG messages.
    @param error_file: True: creates a separate file for warnings and errors to ease debugging of errors.
    @param targets_levels: {'output_stream': logging_level, } . Ex: {'console':logging.INFO, 'url': logging.WARNING}
    @param urls: List of URLs to broadcast the log messages to.
    """
    logLevels = (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL)
    defaultLogLevel = logging.INFO
    outputList = ('console', 'file', 'url', 'stdout', 'prn')
    loggerName = str(loggername).strip() if loggername else __name__
    logger = logging.getLogger(loggerName)
    targets_levels_parsed = {}
    if isinstance(targets_levels, str):
        targets_levels = (targets_levels, )
    if hasattr(targets_levels, '__iter__'):
        for k in targets_levels:
            if isinstance(k, str) and k.lower().strip() in outputList:
                targets_levels_parsed[k.lower().strip()] = targets_levels.get(k) if isinstance(targets_levels, dict) \
                                                           and targets_levels.get(k) in logLevels else defaultLogLevel
    if not targets_levels_parsed:
        targets_levels_parsed = {'console': logging.INFO}
    logger.setLevel(min(targets_levels_parsed.values()) if not debug else logging.DEBUG)  # Sets level for the logger.
    print(f'Logger Name: {loggerName} / Targets & Levels: {targets_levels_parsed} / Debug mode: {debug}')

    fileName = logfile.strip() if logfile and isinstance(logfile, str) else loggerName
    fFormat = logging.Formatter(f'%(asctime)s - %(name)s: [%(filename)s.%(funcName)s:%(lineno)d] - '
                                f''f'%(levelname)s - %(message)s')
    debugFormat = logging.Formatter(f'%(asctime)s - %(name)s: [%(filename)s.%(funcName)s: %(lineno)d] %(levelname)s'
                                    f' - %(message)s')

    # Create handlers, set handlers' levels
    if 'console' in targets_levels_parsed:
        cHandler = logging.StreamHandler()      # Console handler
        cHandler.setLevel(targets_levels_parsed.get('console'))
        cFormat = logging.Formatter(f'%(name)s: [%(filename)s.%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s')
        if targets_levels_parsed['console'] == logging.DEBUG:
            cHandler.setFormatter(debugFormat)      # Set the format for the handler
        else:
            cHandler.setFormatter(cFormat)
        logger.addHandler(cHandler)         # Append handlers to the db_logger

    if 'file' in targets_levels_parsed:
        logFileName = './' + fileName + '.log'
        fHandlers = [RotatingFileHandler(logFileName, maxBytes=1000000, backupCount=50)]  # Crea 1 handler. 500MB data
        if targets_levels_parsed['file'] == logging.DEBUG:
            [j.setFormatter(debugFormat) for j in fHandlers]      # Set the format for the handler
        else:
            [j.setFormatter(fFormat) for j in fHandlers]
        [j.setLevel(targets_levels_parsed['file']) for j in fHandlers]      # Sets levels for file handlers
        # Append handlers to the db_logger
        _ = [logger.addHandler(j) for j in fHandlers]      # addHandler returns nothing.

    if error_file is True:
        errorFileName = './' + fileName + '_error_file.log'
        fHandlers = [RotatingFileHandler(errorFileName, maxBytes=1000000, backupCount=50)]  # Crea 1 handler. 500MB data
        [j.setLevel(logging.WARNING) for j in fHandlers]      # TODO(cmt): por ahora incluye WARNINGS ademas de ERRORS.
        [j.setFormatter(fFormat) for j in fHandlers]
        # Append handlers to the error log file
        _ = [logger.addHandler(j) for j in fHandlers]      # addHandler returns nothing.

    if debug is True:
        debugFileName = './' + fileName + '_debug.log'
        fHandlers = [RotatingFileHandler(debugFileName, maxBytes=1000000, backupCount=50)]  # Crea 1 handler. 500MB data
        [j.setLevel(logging.DEBUG) for j in fHandlers]      # TODO(cmt): por ahora incluye WARNINGS ademas de ERRORS.
        [j.setFormatter(debugFormat) for j in fHandlers]
        # Append handlers to the error log file
        _ = [logger.addHandler(j) for j in fHandlers]      # addHandler returns nothing.

    if 'url' in targets_levels_parsed:
        pass

    if 'stdout' in targets_levels_parsed:
        pass

    return logger

# Logger for kernel code
krnl_logger = getLogger(loggername=KERNEL_LOGGER, targets_levels={'file': logging.INFO, 'console': logging.INFO},
                        logfile=None, error_file=True, debug=DEBUG_MODE)
krnl_logger.info('Starting up krnl_logger...')

# Logger for all Front End (UI) code
fe_logger = getLogger(loggername=FRONT_END_LOGGER, targets_levels={'file': logging.INFO, 'console': logging.INFO},
                      logfile=None, debug=DEBUG_MODE)
fe_logger.info('Starting up fe_logger...')

# Logger for DB modules specific to DB reads/write (krnl_sqlite.py, krnl_db_access.py). Part of the kernel, but stil...
db_logger = getLogger(loggername=DB_LOGGER, targets_levels={'file': logging.INFO, 'console': logging.WARNING},
                      logfile=None, error_file=True, debug=DEBUG_MODE)
db_logger.info('Starting up db_logger...')

bkgd_logger = getLogger(loggername=BACKGROUND_LOGGER, targets_levels={'file': logging.INFO, 'console': logging.WARNING},
                        error_file=True, logfile=None)
bkgd_logger.info('Starting up bkgd_logger...')

# Logger for urls used to broadcast log info (for future development)
url_logger = getLogger(loggername=URL_LOGGER, targets_levels='url', logfile=None)
# url_logger.info('Starting up url_logger...')


def callerFunction(depth=1, **kwargs):
    """
    @param depth: 0->callerFunction; 1->Caller of this Function/ActivityMethod (what we need); 2->Caller of the Caller; etc.
    @param
                   if upperIndex > stack depth -> returns all callers, up to main module, based in includeMain argument.
    @param kwargs: 'includeMain': Includes main Module, otherwise excludes it from return list.
                   'namesOnly': returns bare name string (without Function/Name string)
                   'getCallers': True -> returns list of caller functions: This function and the Parent caller,
                   up to upperIndex. Default: False
    @return: [caller function, caller of the caller, ...] (tuple)
    """
    depth = depth if depth >= 0 else 1
    getCallers = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('getcaller')), True)
    if getCallers is True:
        upperIndex = len(inspect.stack(0)) - (1 if not kwargs.get('includeMain') else 0)
        retValue = [str(_getframe(j).f_code.co_name).strip() + ('()' + (' ->' if j > depth+1 else ''))
                    for j in range(upperIndex, max(depth, 0), -1)]
    else:
        retValue = _getframe(depth).f_code.co_name
    retValue = str(retValue).replace("'", "").replace("[", "").replace("]", "").replace(",", "") + ''
    namesOnly = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('namesonly')), True)
    retValue = retValue if namesOnly is not False else f'Function/ActivityMethod: {retValue}'
    return retValue



# ------------------  DB ACCESS functions for this module. Not used anymore, but leave just in case ------------------ #

def connCreate(dbName='', *, check_same_thread=True, detect_types=0, isolation_level=None, timeout=0.0, cach_stmt=0,
               uri=False):  # kwargs a todas por ahora, para flexibilidad
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

def execute(strSQL='', params='', *, conn=None):
    """ Executes strSQL. Returns a cursor object or errorCode (str). This function for QUERY/DB Reads ONLY!!
        Implements re-entry handling code to support re-entry from other threads in the time between the execution
        of get_max_id() and the actual write to DB via the execute() command.
    """
    if strSQL and conn:
        params = list(params)
        with conn:
            db_logger.debug(f'SQLiteQuery received query: {strSQL}')
            try:
                cur = conn.execute(strSQL, params)
            except (sqlite3.Error, DBAccessError, Exception) as e:
                cur = f'ERR_SQLiteQuery {callerFunction()} error: {e} - )'
                conn.rollback()
                db_logger.error(f'{cur}; strSQL: {strSQL}')
                raise DBAccessError(f'DatabaseError Exception.{cur}; strSQL: {strSQL}')
        return cur

def exec_sql(*, db_name: str = None, sql: str = None, params=''):
    """
    Reads records from DB using argument strSQL. strSQL must be valid, with access to 1 table only.
    returns: (dbFieldNames, data_rows[list of lists])
    """
    db_name = db_name or MAIN_DB_NAME
    try:
        conn = connCreate(db_name)   # TODO(cmt): Adquiere conn del thread que llama a dbRead(). MUY IMPORTANTE!
        cur = conn.execute(sql, params)   # TODO(cmt): Acceso a DB. Convierte strings a datetime via PARSE_DECLTYPES
    except (sqlite3.Error, sqlite3.DatabaseError, sqlite3.OperationalError) as e:
        krnl_logger.error(f'ERR_DBAccess exec_sql()- sql={sql}. Error: {e}')
        raise sqlite3.DatabaseError(f'ERR_DBAccess exec_sql()- sql={sql}. Error:{e}')

    if isinstance(cur, sqlite3.Cursor):
        # IMPORTANTE: acceder via cur.description asegura que los indices de fldNames y valores se correspondan.
        try:
            dbFieldNames = [j[0] for j in cur.description]  # Solo campos leidos de DB se incluyen en la DataTable
        except (TypeError, ValueError, IndexError):
            dbFieldNames = []
        rows = cur.fetchall()               # TODO(cmt): lectura de registros. rows is [(),] (list of tuples)
        conn.close()
    else:       # Error: Retorna tabla vacia, keyFieldNames=dbFieldNames=[]
        krnl_logger.error(f'ERR_DBAccess dbRead0(): {cur} - {callerFunction()}')
        raise sqlite3.DatabaseError(f'ERR_DBAccess dbRead0(): {cur} - {callerFunction()}')
    return dbFieldNames, rows

# -------------------------------------------- End DB Access functions -------------------------------------------- #

# ------------------------------------------ DB Initialization Activities ----------------------------------------- #
def baptize_db(db_name=None):
    """ Reads the db UID value from table _sys_db_id. Returns the value. If table _sys_db_id doesn't exist, creates it
    and assigns a UID to the database that will be the database id for as long as it exists.
    @return: database UID (str). """
    db_name = db_name if db_name and isinstance(db_name, str) else MAIN_DB_NAME
    exec_sql(db_name=db_name, sql="CREATE TABLE IF NOT EXISTS _sys_db_id (ID_ROW INTEGER PRIMARY KEY, "
                                  "DB_ID TEXT NOT NULL, DB_Name TEXT); ")
    fNames, rows = exec_sql(db_name=db_name, sql="SELECT ROWID, DB_ID FROM _sys_db_id LIMIT 1")
    if rows:
        # Trigger used to prevent updates of DB_ID in the VERY UNLIKELY case that uuid1()-generated ROWIDs are repeated.
        trigger_sql = f'CREATE TRIGGER IF NOT EXISTS IGNORE_UPDATES BEFORE UPDATE ON _sys_db_id FOR EACH ROW BEGIN ' \
                      f'SELECT RAISE(IGNORE) WHERE old.ROWID=={rows[0][0]}; END; '
        exec_sql(db_name=db_name, sql=trigger_sql)
        return str(rows[0][1])

    db_uid = uuid4().hex
    rowid = uuid1().int >> 96  # Creates a unique rowid to avoid overwrites by the replication (when rowid is the same)
    while rowid >= SQLite_MAX_ROWS:
        rowid = uuid1().int >> 64       # if unique rowid >= Max SQLITE row, tries again in while loop.
    exec_sql(db_name=db_name, sql=f'INSERT INTO _sys_db_id (ROWID, DB_ID, DB_Name) VALUES ({rowid},"{db_uid}",{db_name})')
    trigger_sql = f'CREATE TRIGGER IF NOT EXISTS IGNORE_UPDATES BEFORE UPDATE ON _sys_db_id FOR EACH ROW BEGIN ' \
                  f'SELECT RAISE(IGNORE) WHERE old.ROWID=={rowid}; END; '
    exec_sql(db_name=db_name, sql=trigger_sql)
    return db_uid

def create_replication_dicts():
    fldNames, rows = exec_sql(sql="SELECT DB_Table_Name, Object_Name, TimeStamp FROM _sys_Trigger_Tables; ")
    if rows:
        tbls_binding_objects = {r[0]: r[1] for r in rows}  # {DB_Table_Name: Class_Name, } Used to for db replication.
        tbls_last_time_stamps = {r[0]: r[2] for r in rows}  # {DB_Table_Name: last_updated_by, }
    else:
        tbls_binding_objects = {}   # binding_objects are the objects that execute the Method below. Usually a class.
        tbls_last_time_stamps = {}
                                            # if None, Method executes by itself, without any kind of binding object.
    fldNames, rows = exec_sql(sql="SELECT Table_Name, Method_Name FROM _sys_Tables WHERE Method_Name IS NOT NULL; ")
    if rows:
        tbls_methods = {r[0]: r[1] for r in rows}  # {DB_Table_Name: Method_Name, }. Used to manage db replication.
    else:
        tbls_methods = {}
    return tbls_binding_objects, tbls_methods, tbls_last_time_stamps

tables_and_binding_objects, tables_and_methods, db_time_stamps = create_replication_dicts()
MAIN_DB_ID = baptize_db(MAIN_DB_NAME)

# ======================================= Critical Time settings for system operation =================================

SYSTEM_SERVER_TOLERANCE_SECS = 10  # 10 seconds tolerance. If tolerance is in range, picks system time() as ref time.
def getReferenceTime(*, server=None, db_table='_sys_Time_Reference', offset=0.0):
    """
    First: Reads reference time from DB table.
    If that fails, returns time in seconds from NTP Server or time from time() function of NTP call fails
    """
    t0 = perf_counter()
    monotonic()
    _ = perf_counter()
    execution_perf_counter = _ - t0     # To account for the execution time of the perf_counter() calls
    ADJUST_FACTOR = 0.8
    client = ntplib.NTPClient()
    ref_seconds = mono_base_seconds = 0
    sys_ref_seconds = time()
    sys_startTime = perf_counter()
    sys_monoStart = monotonic()
    try:
        # Linea response= genera error si no hay internet link (socket.gaierror: [Errno 11001] getaddrinfo failed)
        response = client.request(server, version=4)
        startTime = perf_counter()
        monoStart = monotonic()
        _ = perf_counter()
        execTime = _ - startTime
        ref_seconds = response.tx_time  # response.tx_time in theory equal to time() output if computer clock is ok
        mono_base_seconds = monoStart + execTime * ADJUST_FACTOR - execution_perf_counter
        msg = f'response.tx_time: {response.tx_time}, {datetime.fromtimestamp(response.tx_time)}; ' \
              f'time():{sys_ref_seconds} / utc_time: {datetime.utcfromtimestamp(response.tx_time)} / execTime={execTime}'
        print(msg)  # dismiss_print=DISMISS_PRINT NO va aqui..
        krnl_logger.info(msg)
    except Exception as e:
        error_msg = f'Error {e} while retrieving time from server {server}. Returning secs from time() instead.'
        print(error_msg)
        krnl_logger.warning(error_msg)
        # TODO: Here, try to connect to DB server for reference time.
    finally:
        sys_execTime = perf_counter() - sys_startTime
        sys_mono_base_seconds = sys_monoStart + sys_execTime * ADJUST_FACTOR - execution_perf_counter
        if ref_seconds and mono_base_seconds:
            difference = abs(sys_ref_seconds - ref_seconds)
            if difference <= SYSTEM_SERVER_TOLERANCE_SECS:
                krnl_logger.info(f'Server-System difference: {difference}. Using system time as time reference.')
                return sys_ref_seconds, sys_mono_base_seconds
            krnl_logger.info(f'Server-System difference={difference} secs. exceeds limit. System time may be inaccurate.'
                             f' Using Server time as time reference.')
            return ref_seconds, mono_base_seconds
        krnl_logger.info(f'No response from server {server}. Using system time as time references')
        return sys_ref_seconds, sys_mono_base_seconds


def get_ref_time():
    """ returns reference time in seconds since epoch. From NTP query first. if NTP query fails, from time() """
    return getReferenceTime(server="pool.ntp.org")  # returns: system_start_seconds, mono_base_seconds

SYS_START_SEC, MONO_BASE_SECONDS = get_ref_time()  # base time for system startup. Used to reference monotonic() reads.
print(f'&&&&------------------------   SYS_START_SEC: {SYS_START_SEC}, MONO_BASE_SECONDS: {MONO_BASE_SECONDS}')


def time_mt(mode=None):
    """Monotonic Time: Replacement for time(). Returns number of seconds since epoch using monotonic().
    Implemented to reduce calls to time() which is affected by time changes in the system clock.
    mode = 0: returns time since epoch in seconds (float)
    mode !=0: returns time since epoch as a datetime object.
    """
    if not mode:
        return monotonic() - MONO_BASE_SECONDS + SYS_START_SEC
    return datetime.fromtimestamp(monotonic() - MONO_BASE_SECONDS + SYS_START_SEC)

a = time_mt()
c = time()
print(f'&&&&------------------------ time_mt(SERVER): {a}  / time(): {a}')
print(f'&&&&------------------------ Difference time_mt(SERVER) - time():{c - a} secs.')

# =================================================================================================================== #

#                                       sqlite3 types Adapters and Converters

sqlite3.register_adapter(Decimal, lambda d: str(d))  # Decimal to str to avoid rounding errors caused by float casting.
sqlite3.register_converter("DECTEXT", lambda d: Decimal(d.decode('ascii')))  # DECTEXT exclusivo de columnas con Montos
# Note: TIMESTAMP name has converter/adapter pair already embedded in sqlite3. All columns with name TIMESTAMP are
# processed transparently and automatically between datetime object and str.

# Convierte data de tipo JSON a su iterator original (list,dict,etc) para todas las columnas de nombre 'JSON' en DB.
# Para que esto ande, al abrir una conexion hacer detect_types=PARSE_DECLTYPES. Es la opcion por default en __init__()
def convert_json(json_data):
    try:
        return json.loads(json_data.decode(), object_hook=json_converter)
    except JSONDecodeError:
        raise DBAccessError('JSON conversion error. Could not convert %s' % json_data)

sqlite3.register_converter('JSON', convert_json)

def json_converter(x):     # Ya que json pasa los int keys a str, hay que reconvertir a int al cargarlos desde DB
    # TODO(cmt): Convierte en int los dict keys convertibles a int. TENER EN CUENTA y NO USAR '24','151',etc. (esto es
    #  numeros en formato str como keys en los json dicts. Con esta restriccion, anda lindo esto, che....
    #  Tambien convierte UUID validos a str.
    if isinstance(x, dict):
        aux_dict = {}
        for k in x:
            try:
                aux_dict[int(k)] = x.get(k)
            except ValueError:
                aux_dict[k] = x.get(k)
        return aux_dict
    return x

# ================================================================================================================= #

#                                 Comparison functions used by ProgActivities class and functions.

def compare(val1, val2):
    """ Checks if val1 is equal or is in val2, based on the type of val2.
    @param val1: single value
    @param val2: value or structure
    """
    if isinstance(val2, (list, tuple)):
        try:
            return val1 in val2
        except(TypeError, ValueError, AttributeError):
            return False
    elif isinstance(val2, str):
        try:
            return removeAccents(val1) in removeAccents(val2)
        except(TypeError, ValueError, AttributeError):
            return False
    elif isinstance(val2, set):
        try:
            return not(bool(set(val1).difference(val2)))   # True si val1 esta incluido (todos sus elementos) en val2.
        except(TypeError, ValueError, AttributeError):
            return False
    elif isinstance(val2, dict):
        if not isinstance(val1, dict):
            try:
                val1 = dict((k, v) for (k, v) in val1)
            except(TypeError, AttributeError, KeyError, ValueError):
                return False
        try:
            result = [k in val2 and v == val2[k] for (k, v) in val1.items()]
            return all(j is True for j in result) or val1 == val2
        except(TypeError, AttributeError, KeyError, ValueError):
            return False
    else:
        return val1 == val2 if type(val1) == type(val2) else False


def compare_range(comp_val, ref_val, low_val=VOID, high_val=VOID, *, exclusive=False):
    """
    Compares comp_val to a value with lower and upper limits (Deviation) passed in data_list.
    Rules (assumes comp_val is always passed):
    1) No values passed -> returns False
    2) ref_val != None. All rest is None (1 value passed) -> compares as ==
    3) ref_val OR low_val != None -> ABSOLUTE COMPARISON (NO reference value).
        3.1) (None, low_val) -> comp_val <= low_val
        3.2) (ref_val, None) -> comp_val >= ref_val
    4) ref_val, low_val, high_val != None -> RELATIVE COMPARISON (using reference value).
        ref_val -low_val <= comp_val <= ref_val+high_val
    @param exclusive: True: use <, >.   False: use <=, >=.
    @return:
    """
    if comp_val is None and ref_val is None:
        return False
    if low_val is VOID and high_val is VOID:            # 1 value passed. Compares as ==.
        return comp_val == ref_val if type(comp_val) == type(ref_val) else False
    elif high_val is VOID:                              # 2 valueS passed. Compares as <= or >=.
        if ref_val is None:             # (comp_val, None, low_val) => chequea comp_val <= low_val
            if low_val is None:
                return False
            return comp_val < low_val if exclusive else comp_val <= low_val
        elif low_val is None:           # (comp_val, ref_val, None) => chequea comp_val >= ref_val
            if comp_val is None:
                return False
            return comp_val > ref_val if exclusive else comp_val >= ref_val
        else:
            if comp_val is None:
                return False
            try:
                low_dev = min(ref_val, low_val)
                high_dev = max(ref_val, low_val)
            except (TypeError, ValueError):
                return False
            return low_dev < comp_val < high_dev if exclusive else low_dev <= comp_val <= high_dev
    else:                                          # All 3 values passed. Compares as ref-low <= comp_val <= ref+high.
        if any(j is None for j in (ref_val, low_val, high_val)):
            return False
        try:
            low_dev = ref_val - low_val or 0
            high_dev = ref_val + high_val or 0
        except (TypeError, ValueError):
            return False
        return low_dev < comp_val < high_dev if exclusive else low_dev <= comp_val <= high_dev


def pa_match_execution_date(exec_date, d2: dict):    # d1 from executed Activity (self); d2 from Prog. Activity (obj)
    """
    Checks whether programmedDate-fldWindowLowerLimit <= execution_date <= programmedDate+fldWindowUpperLimit
    @param exec_date: Activity execution date (datetime obj)
    @param d2: dictionary with fldProgrammedDate, fldWindowLowerLimit, fldWindowUpperLimit from a Programmed Activity
    @return: True/False
    """
    programmedDate = d2.get('fldProgrammedDate')
    windowLowerLimit = d2.get('fldWindowLowerLimit')
    windowUpperLimit = max(d2.get('fldWindowUpperLimit'), d2.get('fldDaysToExpire', 0))

    if any(not isinstance(j, datetime) for j in (programmedDate, exec_date)) or \
            any(not isinstance(j, (int, float)) for j in (windowLowerLimit, windowUpperLimit)):
        return False
    return programmedDate - timedelta(days=windowLowerLimit) <= exec_date <=  \
           programmedDate + timedelta(days=windowUpperLimit)


def nested_dict_iterator_gen(dicto):   # TODO(cmt): generator function (usa yield). Esta es la que se usa en el sistema.
    """ This function accepts a nested dictionary as argument and iterate over all values of nested dictionaries
        Returns lists of dictionaries (unnested)
    """
    for key, value in dicto.items():
        if isinstance(value, dict):                         # Check if value is of dict type
            for aux_val in nested_dict_iterator_gen(value):  # run recursively. Search for more values that are dict
                if isinstance(aux_val, dict):  # and any(isinstance(j, dict) for j in aux_val.values()):
                    # print(f'JIJIJI. aux_val es dict={aux_val}', dismiss_print=DISMISS_PRINT)
                    yield aux_val               # yield solo de values que contengan diccionarios anidados
                # else:
                #     print(f'JEJEJE. POR AQUI PASE, y aux_val NO ES dict={aux_val}...', dismiss_print=DISMISS_PRINT)
        else:
            yield {key: value}     #  yield de todos los dicts que NO contienen diccionarios en sus values.



# ============================================ SYSTEM DECORATORS =================================================== #
# singleton NO ES una funcion normal. Por ser un decorator, se ejecuta de manera especial solo durante la inicializacion
def singleton(cls):         # TODO(cmt): Class Decorator function -> All activities and handlers are singletons.
    """ Makes a class a Singleton class (1 instance only) - OJO: SIEMPRE retorna un instance.  ==> NO SIRVE PARA LLAMAR
    CLASS/STATIC METHODS NI PARA HACER TYPE CHECKS (isinstance, type, is) TAL COMO ESTA
    Para un full-fledged singleton implementation, hacer override de __new__() dentro de la definicion de la clase.
    (ver SQLiteQueueDatabase class)
    """
    @functools.wraps(cls)
    def wrapper_singleton(*args, **kwargs):
        if not wrapper_singleton.instance:
            wrapper_singleton.instance = cls(*args, **kwargs)       # Crea Objeto (solo si no existia)
        return wrapper_singleton.instance                           # Retorna objeto al llamador
    wrapper_singleton.instance = None
    return wrapper_singleton


def timerWrapper(iterations=1, verbose=False):
    """ Wrapper implemented to be able to pass parameters (iterations, etc) to timerDecorator(). """
    def timerDecorator(func):  # Timer wrapper para medir tiempo de ejecucion de funciones de background.
        """ Timer @decorator to time execution of function calls. """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = perf_counter_ns()  # perf_counter -> Resolucion = 0.466 usec.
            retValue = func(*args, **kwargs)  # retValue here is the the Total # of animals processed by the func call
            end_time = perf_counter_ns()
            run_time = (end_time - start_time)  # time in nanoseconds
            timerDecorator.__timerSum += run_time
            timerDecorator.__iterCounter += 1
            if timerDecorator.__iterCounter % timerDecorator.__loopIterations == 0:
                avgTime = timerDecorator.__timerSum / timerDecorator.__loopIterations / 1000
                print(f"\n@@@@@@ Timing Function {func.__name__!r} ---> Avg. Execution "
                      f"time({timerDecorator.__loopIterations} iterations): {avgTime:.3f} microseconds", end='')
                print(f' -- Calls: {callerFunction(getCallers=True)}', dismiss_print=DISMISS_PRINT)
                if type(retValue) in (int, float) and verbose:
                    print(f"\n/// Total Objects processed: {retValue} - Estimated process time per 1000 objects: "
                          f"{(avgTime / 1000000) * 1000:.2f} secs.", dismiss_print=DISMISS_PRINT)
                timerDecorator.__iterCounter = 0
                timerDecorator.__timerSum = 0
            return retValue

        # Asignaciones de abajo se ejecutan una unica vez al inicializar. Son atributos de timerDecorator.
        timerDecorator.__timerSum = 0.0
        timerDecorator.__iterCounter = 0
        timerDecorator.__loopIterations = iterations if type(iterations) is int and iterations > 0 else 1
        return wrapper
    return timerDecorator

# print() decorator to enable/disable print() calls via system parameter DISMISS_PRINT
def printWrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

orig_print = builtins.print                       # Renombra funcion print() original (definida en builtins.py)
@printWrapper
def print(*args, dismiss_print=False, **kwargs):  # Este printWrapper lo usan los modulos que importen print de aqui
    if dismiss_print is True:
        return None                               # Porque builtins.print() retorna None.
    return orig_print(*args, **kwargs)

# -------------------------------------------------- END DECORATORS ------------------------------------------------ #

def datetime_from_kwargs(defaultDate=None, **kwargs):
    """
    Parses kwargs for parameter 'date'. Returns date if present and valid, otherwise returns defaultDate.
    If defaultDate is not valid, returns None.
    If parameter 'date' is not present in kwargs (date not passed) or date is not valid, returns None.
    All objects are datetime objects.
    @param defaultDate: datetime object
    @param kwargs:
    @return: datetime object with parsed date. None if 'date' is not passed or date is not valid.
    """
    eventDate = next((kwargs[j] for j in kwargs if 'date' in j.lower()), '')
    if eventDate:
        if not isinstance(eventDate, datetime):
            try:
                eventDate = datetime.strptime(eventDate, fDateTime)
            except(TypeError, ValueError, AttributeError):
                eventDate = defaultDate if isinstance(defaultDate, datetime) else None
    else:
        eventDate = None
    return eventDate

def getEventDate(*, tblDate=None, defaultVal=None, **kwargs):
    """ returns datetime object by querying tblDate and kwargs. Assigns eventDate based on set priorities
    @param defaultVal: datetime obj
    @param tblDate: datetime obj or datetime in string format.
    @return: datetime object.
    """
    eventDate = tblDate
    if not isinstance(eventDate, datetime):
        try:
            eventDate = datetime.strptime(eventDate, fDateTime)     # 1ro: eventDate de tblData('fldDate')
        except(ValueError, TypeError, AttributeError, NameError):
            eventDate = datetime_from_kwargs(**kwargs)              # 2do: eventDate de kwargs
            if not eventDate:
                defaultVal = defaultVal if isinstance(defaultVal, datetime) else datetime.fromtimestamp(time_mt())
        eventDate = eventDate if eventDate else defaultVal           # 3ro: eventDate = defaultValue
    return eventDate

def trunc_datetime(dt: datetime, trunc_val=None):
    """
    @param dt: datetime object
    @param trunc_val: 'day', 'hour', 'minute', 'second' (str)
    @return: truncated datetime object or original dt if dt is not a datetime object.
    """
    truncateDict = {None: 0, 'second': 1, 'minute': 2, 'hour': 3, 'day': 4}  # Truncate for datetime objects.
    try:
        return datetime(dt.year, dt.month,
                        dt.day if truncateDict.get(trunc_val, 0) <= truncateDict['day'] else 0,
                        dt.hour if truncateDict.get(trunc_val, 0) <= truncateDict['hour'] else 0,
                        dt.minute if truncateDict.get(trunc_val, 0) <= truncateDict['minute'] else 0,
                        dt.second if truncateDict.get(trunc_val, 0) <= truncateDict['second'] else 0, 0)
    except (TypeError, AttributeError, ValueError):
        return dt


def removeAccents(input_str: str, *, str_to_lower=True):
    """
    Removes common accent characters. Converts to all lowercase if str_to_lower=True (Default)
    This is the standard to check for strings and names.
    Uses: regex.
    """
    if isinstance(input_str, str):
        new = input_str.strip()
        if str_to_lower is False:
            new = re.sub(r'[àáâãäå]'.upper(), 'A', new)
            new = re.sub(r'[èéêë]'.upper(), 'E', new)
            new = re.sub(r'[ìíîï]'.upper(), 'I', new)
            new = re.sub(r'[òóôõö]'.upper(), 'O', new)
            new = re.sub(r'[ùúûü]'.upper(), 'U', new)
        else:
            new = new.lower()

        new = re.sub(r'[àáâãäå]', 'a', new)
        new = re.sub(r'[èéêë]', 'e', new)
        new = re.sub(r'[ìíîï]', 'i', new)
        new = re.sub(r'[òóôõö]', 'o', new)
        new = re.sub(r'[ùúûü]', 'u', new)               # ver si usar ü o no
        return new
    return input_str

def kwargsStrip(**kwargs):
    """
    Strips all trailing and leading blanks from key names in kwargs. Strips 1 level of kwargs (NO NESTED kwargs).

    @return: **kwargs (dict) with all leading and trailing blanks stripped from key names.
    """
    retDict = {}
    for k in kwargs:
        try:
            retDict[str(k).strip()] = kwargs[k]             # Hace strip() solo de keys tipo str.
        except (TypeError, ValueError, AttributeError):
            retDict[k] = kwargs[k]
    return retDict

def in_between_nums(num0=None, *, lower_limit=None, upper_limit=None, exclusive=False):
    """
    Compares 3 numbers (int, float or complex). True if num0 is between lower_limit and upper_limit.
    @param lower_limit:
    @param upper_limit:
    @param exclusive: True: uses < ; False: uses <=
    @param num0:  number to compare between lower_limit and upper_limit,
    @return: True/False. All types not in (int, float, complex) result in False.
    """
    if not isinstance(num0, (int, float, complex, datetime)):
        return False             # Retorna False  si no se provee num0.
    lowLimit = lower_limit if isinstance(lower_limit, (int, float, complex, datetime)) else None
    uppLimit = upper_limit if isinstance(upper_limit, (int, float, complex, datetime)) else None
    if lowLimit is None and uppLimit is None:
        return False
    comp1 = (lowLimit < num0 if exclusive else lowLimit <= num0) if lowLimit is not None else True
    comp2 = (num0 < uppLimit if exclusive else num0 <= uppLimit) if uppLimit is not None else True
    return comp1 and comp2

def valiDate(str_date: str, defaultVal=None):
    """
    Returns string date equal to strDate if strDate is a valid date. Otherwise returns defaultVal.
    @param str_date: string to validate
    @param defaultVal: Value to return if strDate is not a valid date. Can be anything.
    @return: strDate (datetime object) if strDate is valid, defaultVal is strDate is not a valid date.
             Returns str_date if not str_date
    """
    try:
        return datetime.strptime(str_date, fDateTime)
    except (ValueError, TypeError):
        return defaultVal


# ########################################## Used code down to here. The rest is not used #############################




def tblNameFromUID(fldUID: str):
    try:
        if all(fldUID.strip().__contains__(j) for j in ('tbl', uidCh)):
            sep = fldUID.find(uidCh)
            if sep > 0:
                return fldUID[:sep]
        return None
    except(AttributeError, NameError):
        return None


def fldNameFromUID(fldUID: str):
    try:
        if all(fldUID.strip().__contains__(j) for j in ('tbl', uidCh)):
            sep = fldUID.find(uidCh)
            if sep > 0:
                return fldUID[sep+len_uidCh:]
        return fldUID
    except (AttributeError, NameError):
        return fldUID


# -------------------------------- Funcion para ejecutar codigo pasado como string ---------------------------------- #
def eval_expression(input_string, allowed_names):
    # print(f'Locals: {locals()}')
    try:
        code = compile(input_string, "<string>", "eval")
    # except(SyntaxError, TypeError, AttributeError, EOFError, KeyError, SystemError, IndentationError,
    #        OSError, NameError, ZeroDivisionError, UnboundLocalError, ImportError, FloatingPointError):
    except Exception:
        print(f'ERR_Sys: Compile error. {moduleName()}({lineNum()}) - {callerFunction()}', dismiss_print=DISMISS_PRINT)
        return False
    for name in code.co_names:
        if name not in allowed_names.values():
            raise NameError(f'Use of {name} not allowed')
    retValue = eval(code, {"__builtins__": {}}, allowed_names)
    return retValue

# ================================================================================================================= #


# Seteo basico de args para DeepDiff. Necesario para dictCompare().
deepDiff_args = {'ignore_string_case': True, 'ignore_string_type_changes': True, 'ignore_numeric_type_changes': True,
                    'ignore_order': True, 'truncate_datetime': 'day', 'verbose_level': 1}

def dictCompare(dict1: dict, dict2: dict, *, compare_args=None):
    """Diffs to dict using DeepDiff and returning the changes ready to print
    dict1 is expanded with keys from new, with their respective values.
    Returns a tuple of added, removed and updated
    """
    # dict1.update({k: dict1[k] for k in dict2})        #  Original: dict1 = {k: dict1[k] for k in dict2}
    compare_args = compare_args if isinstance(compare_args, dict) else {}
    d = DeepDiff(dict1, dict2, **compare_args)
    added = {}                                      # added: absent in dict1 (execute), present in dict2 (paFields)
    removed = set()                                 # removed: present in dict1 (execute), absent in dict2 (paFields)
    changed = d.get("values_changed", dict())       # changed: present in dict1 and dict2: Values and/or types changed.
    for key_change, change in d.get("type_changes", dict()).items():
        if change["new_value"] == change["old_value"]:    # str vs unicode type changes
            continue
        else:
            changed[key_change] = change
    for key in ["dictionary_item_added", "iterable_item_added", "attribute_added", "set_item_added"]:
        if d.get(key, None):
            added[key] = d.get(key)
        # added = added.union(d.get(key, set()))
    for key in ["dictionary_item_removed", "iterable_item_removed", "attribute_removed", "set_item_removed"]:
        removed = removed.union(d.get(key, set()))
    return added, removed, changed


def nested_dict_iterator_gen2(dicto):  # Este es el que se usa para implementar las comparaciones DeepDiff. No usado.
    """ This function accepts a nested dictionary as argument and iterate over all values of nested dictionaries """
    # Iterate over all key-value pairs of dict argument
    for key, value in dicto.items():
        if isinstance(value, dict):                         # Check if value is of dict type
            for kv_pair in nested_dict_iterator_gen2(value):  # If value is dict then iterate over all its values
                yield {key: (*kv_pair,)}
        else:
            yield {key: value}   # If value is not dict type then yield the value

reservedDictKeys = ['new_value', 'old_value']  # Valores usados internamente por DeepDiff que se tienen que discriminar



# --------------------------------------------- End of file ---------------------------------------------------------#

