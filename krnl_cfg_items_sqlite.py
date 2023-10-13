# import os
# import logging
# import sqlite3
# from logging import Logger
# from logging.handlers import RotatingFileHandler
# # from PyQt5.QtSql import QSqlQuery, QSqlDatabase
#
# from krnl_exceptions import *
# # from PyQt5.QtWidgets import QFileDialog, QApplication
# # from PyQt5.QtCore import Qt, QDate, QVariant, QSettings, QPoint
# from datetime import datetime
# import re
# import inspect
# from inspect import currentframe
# from sys import _getframe, modules
#
# VERBOSE = 1  # Parametro para habilitar/deshabilitar avisos a pantalla de ciertas funciones
# tblNotFoundErrMsg = 'ERR_Sys_TableNotFound'
# fldNotFoundErrMsg = 'ERR_Sys_FieldNotFound'
# dbErrorDisplay = 'result().lastError().databaseText()'
# obj_dtError = datetime(1, 1, 1, 0, 0, 0, 0)
# obj_dtBlank = datetime(1, 1, 2, 0, 0, 0, 0)
# PRINT_MAX_LEN = 160
# fDate = "%Y-%m-%d"
# fTime = "%H:%M:%S:%f"
# fDateTime = "%Y-%m-%d %H:%M:%S:%f"
# str_dtError = '0001-01-01 00:00:00:000000'  # Used to return Invalid Date condition '0001-01-01 00:00:00:000000'
# str_dtBlank = '0001-01-02 00:00:00:00000'  # Used to return None or Blank Date condition
#
# strError = 'ERR_'
# sessionActiveUser = 1  # Usuario Activo de la sesion
# activityEnableDisabled = 0
# activityEnableBasic = 2  # Todos estos niveles a definir
# activityEnableIntermediate = 4
# activityEnableAdvanced = 6
# activityEnableFull = 10  # Full enable para actividades. 0 -> Activity disabled.
# uidCh = '__'  # Used in Signature and Notifications to create unique field names. Chars MUST be ok with use in DB Names.
# oprCh = '__opr'  # Particle added to "fldName" fields in DataTables to store operators strings belonging to "fldName"
# nones = (None, 'None', '', 'NULL', 'null', 'Null')
# lower_nones = ('none', '', '0', 'false', 'n', 'no', 'null')
# LOGGING_LEVEL_CON = logging.WARNING
# # LOGGING_LEVEL_FILE = logging.ERROR
# LOGGING_LEVEL_STDOUT = logging.WARNING
# LOGGING_LEVEL_STDERR = logging.ERROR
# LOGGING_LEVEL_CRITICAL = logging.CRITICAL
# LOGGING_LEVEL_ERROR = logging.ERROR
# LOGGING_LEVEL_WARNING = logging.WARNING
# LOGGING_LEVEL_INFO = logging.INFO
# LOGGING_LEVEL_DEBUG = logging.DEBUG
# LOGGING_DEFAULT_FILENAME = 'logGanado.log'
# NOT_DEFINED = 'not defined'
#
# DAYS_MULT = 60  # Dias/segundo.TODO: Modifica EntityObj.updateTimeOut(),krnl_bovine_activity.computeCategory(). SOLO ESO!
# __simulationYears = 3
# sleepAmount = __simulationYears * 365 / DAYS_MULT
#
# MIN_TIMEOUT_DAYS = 1  # 1 day
# MAX_TIMEOUT_DAYS = 50 * 365  # Maximum Timeout time: 50 years.
# INTERVAL_MINUTE = 60
# INTERVAL_HOUR = 3600
# INTERVAL_DAY = 3600 * 24
# INTERVAL_WEEK = INTERVAL_DAY * 7
#
#
# def moduleName():
#     return str(os.path.basename(__file__))
#
#
# def lineNum():
#     return currentframe().f_back.f_lineno

""" 
How logging works:
Internally, messages are turned into LogRecord objects and routed to a Handler object registered for this krnl_logger. 
The handler will then use a Formatter to turn the LogRecord into a string and emit that string.
In general a module should emit log messages as a best practice and should NOT configure how those messages are handled. 
That is the responsibility of the application.
"""


# def getLogger(*args, **kwargs) -> Logger:
#     """ Returns Logger with 2 handlers: Console handler set to INFO level, File handler set to WARNING level
#     All files created in dict1 folder (./)
#     @param kwargs: 'loggerName' name assigned to the Logger object (Default: basic_logger)
#                    'logFileName' log file name in dict1 directory. Default=LOGGING_DEFAULT_FILENAME.
#                    'debug'=True -> Creates and additional Rotating File Handler for disk write with DEBUG level.
#     """
#     debugMode = False if str(kwargs.get('debug')).strip().lower() in lower_nones else True
#     loggerName = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('loggername')), 'basic_logger')
#     db_logger = logging.getLogger(loggerName)
#     logFileName = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('logfile') or
#                         str(j).lower().__contains__('filename')), './' + loggerName + '.log')
#
#     # Create handlers, set levels
#     cHandler = logging.StreamHandler()
#     fHandlers = [RotatingFileHandler(logFileName, maxBytes=1000000, backupCount=50)]  # Crea 1 handler. 500MB of data
#
#     cHandler.setLevel(logging.INFO)
#     [j.setLevel(logging.WARNING) for j in fHandlers]
#
#     # Creates formatters and adds them to handlers
#     cFormat = logging.Formatter(f'%(name)s: [%(filename)s.%(funcName)s: %(lineno)d] - %(levelname)s - %(message)s')
#     fFormat = logging.Formatter(f'%(asctime)s - %(name)s: [%(filename)s.%(funcName)s: %(lineno)d] - '
#                                 f'%(levelname)s - %(message)s')
#     cHandler.setFormatter(cFormat)
#     [j.setFormatter(fFormat) for j in fHandlers]
#
#     # Append handlers to the db_logger
#     db_logger.addHandler(cHandler)
#     [db_logger.addHandler(j) for j in fHandlers]  # addHandler returns nothing.
#
#     if debugMode:
#         dFormat = logging.Formatter(f'%(asctime)s - %(name)s: [%(filename)s.%(funcName)s: %(lineno)d] %(levelname)s -'
#                                     f' %(message)s')
#         debugHandlers = [RotatingFileHandler(loggerName + '_' + 'debug.log', maxBytes=1000000, backupCount=5)]
#         [j.setLevel(logging.DEBUG) for j in debugHandlers]
#         [j.setFormatter(dFormat) for j in debugHandlers]
#         [db_logger.addHandler(j) for j in debugHandlers]
#         # print(f'THESE ARE MY DEBUG HANDLERS:\n')
#         # for j in debugHandlers:
#         #     print(f'{j} / len(debugHandlers): {len(debugHandlers)}')
#     db_logger.setLevel(min([j.level for j in db_logger.handlers]))
#     return db_logger
#
#
# # krnl_logger = getLogger(loggerName='krnl_logger', level=logging.INFO, output=('file', 'console'))
# # krnl_logger.info('Starting up krnl_logger...')
# # bkgd_logger = getLogger(loggername='bkgd_logger', debug=True)
#
# def createDbConn(strDbName: str, strConn: str):
#     """
#
#     :rtype: (object, bool)
#
#     """
#     dbConnCreated = False
#     if QSqlDatabase.contains(strConn):
#         db = QSqlDatabase.database(strConn)
#     else:
#         db = QSqlDatabase.addDatabase("QSQLITE", strConn)
#         db.setDatabaseName(strDbName)
#         # TODO: capturar errores de conexión - devolver ERR_Sys_DBConnectionError
#         db.open()
#         dbConnCreated = True
#     return [db, dbConnCreated]
#
#
# def createDbConnQt(strDbName: str, strConn: str):
#     """
#     Returns a QSqlDatabase object (QSQLITE driver)
#
#     Connection is created if it doesn't exist
#
#     Parameters
#     ----------
#     strDbName : str
#         the SQLiteQuery database file path
#     strConn : str
#         the _conn string
#
#     Returns
#     -------
#     object
#         QSqlDatabase object
#     bool
#         True if _conn was created
#         False if _conn exists
#
#     :rtype: (object, bool)
#
#     """
#     dbConnCreated = False
#     if QSqlDatabase.contains(strConn):
#         db = QSqlDatabase.database(strConn)
#     else:
#         db = QSqlDatabase.addDatabase("QSQLITE", strConn)
#         db.setDatabaseName(strDbName)
#         # TODO: capturar errores de conexión - devolver ERR_Sys_DBConnectionError
#         db.open()
#         dbConnCreated = True
#     return [db, dbConnCreated]
#
#
#
# def getArg(key: str = None, *, defaultVal=None, lower=None, exact_match=True, **kwargs):
#     """
#     Parses kwargs searching for 'key'. Returns the value associated to key or defaultVal if key is not found.
#     @param exact_match: True: exact match of key. False returns true if key 'is contained' in the dictionary keys.
#     @param lower:
#     @param defaultVal: Value returned if key is not found in kwargs dict.
#     @param key:
#     @param kwargs: kwargs to parse
#
#     @return:
#     """
#     if exact_match:
#         if not lower:
#             return kwargs.get(key, defaultVal)
#         else:
#             return next((kwargs[j] for j in kwargs if removeAccents(j) == removeAccents(key)), defaultVal)
#
#     if lower:
#         retValue = next((kwargs[j] for j in kwargs if removeAccents(str(j)).__contains__(removeAccents(str(key)))),
#                         defaultVal)
#     else:
#         retValue = next((kwargs[j] for j in kwargs if str(j).__contains__(key)), defaultVal)
#     return retValue
#
#
# def removeAccents(input_str):
#     """
#     Removes common accent characters. Converts to all lowercase. This is the standard to check for strings and names.
#     Uses: regex.
#     """
#     new = input_str.strip().lower()
#     new = re.sub(r'[àáâãäå]', 'a', new)
#     # new = re.sub(r'[àáâãäå]'.upper(), 'A', new)
#     new = re.sub(r'[èéêë]', 'e', new)
#     # new = re.sub(r'[èéêë]'.upper(), 'E', new)
#     new = re.sub(r'[ìíîï]', 'i', new)
#     # new = re.sub(r'[ìíîï]'.upper(), 'I', new)
#     new = re.sub(r'[òóôõö]', 'o', new)
#     # new = re.sub(r'[òóôõö]'.upper(), 'O', new)
#     new = re.sub(r'[ùúûü]', 'u', new)  # ver si usar ü o no
#     # new = re.sub(r'[ùúûü]'.upper(), 'U', new)
#     return new
#
#
# def createDT(strDT: str):
#     """
#     Creates datetime object from argument string COMING FROM A DB READ. To be used also with use input.
#     @param strDT: String in format YYYY-MM-DD
#     @return: Valid input string -> datetime object representing argument values
#              Non valid input string -> obj_dtError
#              Blank ('' or None) -> obj_dtBlank
#     """
#     zeroPaddingTime = '00:00:00:000000'
#     lenDT = 26
#     lenT = 15
#
#     if strDT == '' or strDT is None:  # '', None = Blank / 0: reservado para indicar FIRST RECORD. FALTA IMPLEMENTAR
#         return obj_dtBlank  # Objeto datetime que indica '' o None.
#     else:  # Esto es el createDTShort
#         strDT = str(strDT).strip()
#         if 0 < strDT.find(' ') < 10 or len(strDT) < lenDT:  # Procesa si se paso fecha "corta" (ej. 2020/1/1)
#             strDate = strDT.replace('/', '-', strDT.count('/'))
#             strDate = strDate.replace(' ', '-', 2 - strDate.count('-'))
#             strDate = strDate[:min(10, (strDate.find(" ") + 1 if strDate.find(" ") > 0 else 10))]
#             yr = strDate[:strDate.find('-')]
#             mon = strDate[strDate.find('-') + 1:strDate.find('-', strDate.find('-') + 1)]
#             day = strDate[strDate.find('-', strDate.find('-') + 1) + 1:len(strDate)]
#             # print(f'strDate: {strDate} / strYear: {yr} /  strMonth: {mon}  / strDay: {day} ')
#             strTime = strDT[-(len(strDT) - len(strDate)):].strip() if len(strDate) != len(strDT) else zeroPaddingTime
#             # print(f'strTime INICIAL: {strTime} / len(strDT):{len(strDT)} / len(strDate):{len(strDate)}')
#             strTime = strTime + zeroPaddingTime[-(lenT - len(strTime)):] if len(strTime) < lenT else strTime[:lenT]
#             hour = strTime[:2]
#             minut = strTime[3:5]
#             sec = strTime[6:8]
#             usec = strTime[9:]
#             # print(f'strTime: {strTime} / hour:{hour}; min:{minut}; sec:{sec}; usec:{usec}')
#         else:  # Asume string formateado YYYY-MM-DD HH:MM:SS:NNNNNN
#             strDT = strDT[:lenDT]
#             yr = strDT[:4]
#             mon = strDT[5:7]
#             day = strDT[8:10]
#             hour = strDT[11:13]
#             minut = strDT[14:16]
#             sec = strDT[17:19]
#             usec = strDT[20:]
#
#         try:
#             yr = int(yr)
#             mon = int(mon)
#             day = int(day)
#             hour = int(hour)
#             minut = min(int(minut), 59)
#             sec = min(int(sec), 59)
#             usec = int(usec)
#             dt = datetime(yr, mon, day, hour, minut, sec, usec)
#         except (TypeError, ValueError, NameError, IndexError):
#             dt = obj_dtError
#     # print(f'OBJECT dt is: {dt}')
#     return dt
#
# # --------------------------------------------- End of file ---------------------------------------------------------#
#
