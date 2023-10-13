import sys
from datetime import datetime
import inspect
from krnl_sqlite import *
import time
from time import sleep
from krnl_config import fDateTime
from krnl_cfg import getNow
import functools
from custom_types import DataTable, dbRead
from krnl_parsing_functions import setRecord, getRecords


if __name__ == '__main__':
    conn = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()
    t = datetime(2045, 1, 1, 12, 1, 1)
    fecha = '1900-01-01'
    cur.execute('CREATE TABLE foo(bar TIMESTAMP, fecha DATE)')
    cur.execute("INSERT INTO foo(bar, fecha) VALUES (?,?)", (t, fecha))
    # cur.execute("INSERT INTO foo(bar) VALUES (?)", ('2022-09-11 22:58:58',))
    cur.execute("SELECT * FROM foo")            # AQUI INVOCA LAS FUNCIONES DE CONVERSION!!
    data = cur.fetchall()
    data = list(zip(*data))
    print(f'Result INSERT: {data}')

    # Ciclo de update.
    tbl = 'foo'
    values = (datetime.now(), fecha)
    strSQL = f' UPDATE {tbl} SET bar=? WHERE fecha=? '
    print(f'strSQL *** UPDATE ***: {strSQL}')
    cur.execute(strSQL, values)
    cur.execute("SELECT * FROM foo")
    data1 = cur.fetchall()
    data1 = list(zip(*data1))
    print(f'Result UPDATE: {data1}')

    #               CODIGO DE datetime convertes en api2.py
    # def convert_date(val):
    #     return datetime.date(*map(int, val.split(b"-")))
    #
    #
    # def convert_timestamp(val):
    #     datepart, timepart = val.split(b" ")
    #     year, month, day = map(int, datepart.split(b"-"))
    #     timepart_full = timepart.split(b".")
    #     hours, minutes, seconds = map(int, timepart_full[0].split(b":"))
    #     if len(timepart_full) == 2:
    #         microseconds = int('{:0<6.6}'.format(timepart_full[1].decode()))
    #     else:
    #         microseconds = 0
    #
    #     val = datetime.datetime(year, month, day, hours, minutes, seconds, microseconds)
    #     return val

    queryObj = SQLiteQuery()  # Se abre conexion en krnl_sqlite. Esta linea no debiera crea una nueva.

    # t = getTblName('tblAnimales')
    # t1 = getTblName('tblCaravanas')
    # fields = getFldName('tblAnimales', '*', 1)
    # print(f'Fields: {fields}')
    # fld = getFldName('tblCaravanas', 'fldID')
    # print(fld)
    # print(getMaxId('tblAnimales'))
    strSQL = f"SELECT * FROM 'Animales';"
    table = dbRead('tblAnimales', strSQL)
    print(f'DataList dbRead Original: Len:{table.dataLen} - Record Len: {len(table.dataList[0])} / {table.dataList}')
    lista1 = table.getVal(7, 'fldDate')
    lista2 = table.getVal00(7, 'fldDate')
    print(f'Lista getVal   : {lista1}')
    print(f'Lista getValOld: {lista2}')
    print(f'getVal(*): {table.getVal(5, "*")}')
    RR = True if table.unpackItem(5) == table.getVal(5, "*") else False
    print(f'getVal"fldDate": {table.getVal(5, "fldDate")}')
    # is_not = (0, None, False, [], (), {})
    # print(f'IS NOT{is_not} : {[j for j in is_not if not j]}\n')

    eventDate = datetime.now()
    recordUpdate = 5
    table.setVal(recordUpdate, fldDate=eventDate, fldID=recordUpdate)
    print(f'Unpacking: {table.unpackItem(5)}.\nNow going to UPDATE record[{recordUpdate}]...')
    setRecord(table.tblName, **table.unpackItem(5))
    tablita = getRecords(table.tblName, '', '', None, '*')
    fecha = tablita
    print(f'Record just written (record[{recordUpdate}]) is: {tablita.unpackItem(recordUpdate)}')
    t = tablita.getVal(recordUpdate, "fldDate")
    print(f'fldDate({recordUpdate}): {t} / {type(t)} ')

    # print(f'threadSafety: {queryObj.checkThreadSafety()}\n')
    print(f'epoch in this system (gmtime function): {time.gmtime(0)}')

