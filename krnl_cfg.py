#!/usr/bin/env python
# -*- coding: utf-8 -*-d
# https://github.com/dlsartori/ganado

# import linecache
# import sys
# import os
# import functools
# from datetime import datetime, timedelta
# from json import loads, dumps
# from krnl_cfg_items import *
# from krnl_sqlite import getTblName, getFldName
# from krnl_exceptions import *
# from sys import modules
# from typing import TYPE_CHECKING
# # if TYPE_CHECKING:

# from krnl_dataTable import DataTable
# from pydantic import BaseModel, validator, typing, ValidationError

# STDOUT_HNDL = sys.stdout        # Handle de los dispositivos standard.
# STDIN_HNDL = sys.stdin
# ERROR_HNDL = sys.stderr
# dobSystemMask = '22:22:22:222222'
# nones = (None, 'None', '', 'NULL', 'null', 'Null')
# nos = ('no', 'No', 'N', 'NO', 'n')
# TAGS_ASSIGNED_TO = 'subclass'          # 'class' o 'subclass'
# LOCK_TIMEOUT = 4          # Timeout in seconds for all Locks
# RLOCK_TIMEOUT = 4         # Timeout in seconds for all RLocks

# enabled = True          # Usado en ...
# disabled = False

# sysDictTables = {}
# sysDictFields = {}
# dataBufferWrt = {str: []}  # Buffer de datos de escritura: {tblName (str); []: Record de valores a escribir}


# def moduleName():
#     modlStr = str(modules[__name__])
#     return modlStr[modlStr.find('module')+len('module'):modlStr.find('from')].replace("'", '').strip()


# def moduleName():
#     return str(os.path.basename(__file__))






# def exec_sql(tblName, strSQL: str, mode=0):  # _tblName necesario para armar la estructura de retorno
#     """
#     Reads records from DB using argument strSQL. strSQL must be valid, with access to 1 table only.
#     mode: 0(Default): returns DataTable Object  -> THIS IS THE MORE EFFICIENT WAY TO PROCESS DATA
#           1: returns list of dictionaries [{fldName1:value1, fldName2:value2, }, {fldName1:value3, fldName2: value4, }, ]
#     @return: mode 0: Object of class DataTable with all the obj_data queried. (Default)
#              mode 1: List of dictionaries [{fld1:val1, fld2:val2, }, {fld3:val2, fld1:val4, fld2:val5,}, ]. Each
#              dictionary maps to a record (row) from DB and goes to a record item in DataTable.
#     """
#     dataList = []
#     dbFieldNames = []  # DB Names de los campso en keyFieldNames.
#     keyFieldNames = []  # keynames de los campos presentes en _dataList
#     tblName = tblName.strip()  # Nombre de la tabla
#     fieldNamesAll = getFldName(tblName, '*', 1)  # {fName:fldDBName}. mode=1 -> Solo campos de DB
#     # [db, dbGanadoConnCreated] = createDbConn("GanadoSQLite.db", "GanadoConnection")
#     query = QSqlQuery(strSQL, db)
#     if query.next():
#         for i in range(query.record().count()):  # o presentes en la estructura de Tabla (queryObj Field Names)
#             dbFieldNames.append(query.record().fieldName(i))
#         query.previous()
#     else:
#         retValue = DataTable(tblName, [dataList, ], keyFieldNames, dbFieldNames)  #  No hay datos: Retorna tabla vacia
#         # if dbGanadoConnCreated:
#         #     db.connClose()
#         #     db.removeDatabase("GanadoSQLite.db")
#         return retValue
#
#         # Asigna key field names a lista keyFieldNames en el mismo orden de los campos extraidos en dbFieldNames
#     for j in range(len(dbFieldNames)):
#         for i in fieldNamesAll:
#             if fieldNamesAll[i] == dbFieldNames[j]:
#                 keyFieldNames.append(i)
#                 break
#
#     if query.next():  # Recorre registro para tomar valores. Genera un diccionario por cada Record.
#         query.previous()
#         while query.next():
#             rowDict = {}
#             rowList = []
#             for i in range(query.record().count()):
#                 fldValue = query.value(i)
#                 if fldValue and type(fldValue) is str:   # Chequea si es string y si es string, si tiene json encoding.
#                     if fldValue.__contains__(']') or fldValue.__contains__('}'):     # TODO: Revisar esta verificacion
#                         try:
#                             fldValue = loads(fldValue)
#                         except ValueError:
#                             pass            # Si loads() falla (no es json encoding), deja fldValue en su valor inicial
#                 if mode == 0:
#                     rowList.append(fldValue)  # Puebla lista con datos de un record de DB.
#                 else:
#                     rowDict[dbFieldNames[i]] = fldValue  # Loop para poblar dict. con datos de 1 record de DB
#             dataList.append(rowList) if mode == 0 else dataList.append(rowDict)  # Retorna lista de diccionarios. 1 diccionario por cada registro en DB.
#         if mode == 0:
#             retValue = DataTable(tblName, dataList, keyFieldNames, dbFieldNames)  # Retorna objeto de tipo DataTable
#             # print(f'dbRead(lineNum()) OUTPUT: {retValue} // len(list): {retValue.dataLen}')
#         else:
#             retValue = dataList
#     else:
#         retValue = DataTable(tblName, [dataList, ], keyFieldNames, dbFieldNames)
#     # if dbGanadoConnCreated:
#     #     db.connClose()
#     #     db.removeDatabase("GanadoSQLite.db")
#
#     return retValue
#


# def strSQLDeleteRecord(tblName, idRecord):
#     """
#     Crea string SQL para intentar borrrado  del registro idRecord en tabla _tblName. Para usar con dbConnection ya creada
#     @param tblName:
#     @param idRecord:
#     @return: string Delete o False si no se pudo crear string Delete
#     """
#     retValue = None
#     tblName = str(tblName).strip()
#     dbTblName = getTblName(tblName)
#     if idRecord > 0 and dbTblName.find(strError) < 0:
#         retValue = f' DELETE FROM "{dbTblName}" WHERE "{dbTblName}"."{getFldName(tblName,"fldID")}" = "{idRecord}"'
#     return retValue


# def getNow(strFormat: str):  # Retorna tiempo del sistema en un string de formato strFormat
#     return datetime.strftime(datetime.now(), strFormat)



# def kwargsParseNames00(tblName, leMode=0, **kwargs):        # TODO(cmt): FUNCION ORIGINAL. NO MODIFICAR !!
#     """
#     Generates and returns a Dictionary of dictionaries, one dictionary for each tableName passed. Form:
#     losDicts[tblName1] = {fName:fldValue,}, losDicts[tblName2]= {fName:fldValue,}, ...
#     Intended for use for passing and returning parameters in multiple tables
#     @param leMode: 0: Pass all. Checks Table names but not fldNames names. All field names stripped and returned.
#                  1: Only DB Fields. Filters only fldNames that are valid DB Field Names.
#     @param tblName: a table name.
#     @param kwargs: if tblWrite is not provided or not valid: Each dictionary is
#                         {tableName : {fieldName1:fieldValue1, fieldName2:fieldValue2,...},} for the corresponding table.
#         If tblWrite is provided **kwargs is of the form fieldName1=fieldValue1, fieldName2=fieldValue2,...
#     Non-valid names are ignored. If key hdrFields are repeated in a dictionary, the last instance is used.
#     If no valid names are found, returns and val dictionary.
#
#     @return: losDicts{} : Dictionary of dictionaries with tblNames as keys and values are dictionaries {fName:fldValue}
#     """
#     losDicts = {}
#     tblName = str(tblName).strip()
#     if not getTblName(tblName).__contains__(strError):  # Si no hay error (Nombre de tabla es valido)
#         for i in kwargs:
#             f = str(i).strip()  # elimina leading & trailing blanks
#             if leMode == 0:       # mode=0 -> Pasa todos los campos. Necesario para parsear campos generados por queries
#                 losDicts[f] = kwargs[i]
#             else:  # mode != 0 -> # Verifica que field name sea un campo valido en DB. Si no es, ignora.
#                 if not getFldName(tblName, f).__contains__(strError):
#                     losDicts[f] = kwargs[i]
#     return losDicts
#


# def getIdListByFieldValue(tbl: str, fld: str, value):
#     # Usage:
#     #     id = getIdListByFieldValue('Caravanas', 'Numero Caravana', 402)
#     # [db, dbGanadoConnCreated] = createDbConn("GanadoSQLite.db", "GanadoConnection")
#     result = []
#     strSQL = f'SELECT * FROM "{tbl}" WHERE "{fld}" = {value}'
#     query = QSqlQuery(strSQL, db)
#     if query.next():
#         query.previous()
#         while query.next():
#             result.append(query.value(0))
#     # if dbGanadoConnCreated:
#     #     db.connClose()
#     #     db.removeDatabase("GanadoSQLite.db")
#     return result
#
#     # app = QApplication(sys.argv)
#



