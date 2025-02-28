# from datetime import datetime
# import inspect
# import time
from time import sleep
from krnl_config import time_mt
# import functools
# from krnl_cfg import moduleName, callerFunction
# from operator import attrgetter
# import time
from krnl_custom_types import getRecords, setRecord, DataTable
from krnl_db_query import SQLiteQuery
from functools import reduce
import itertools

if __name__ == '__main__':

    queryObj = SQLiteQuery()
    # tbl = 'tblAnimalesActividadesProgramadasTriggers'
    tbl = 'tblDataAnimalesActividadInventario'
    temp = getRecords('tblAnimales', '', '', None, '*', fldID=(5, 7, 12, 15))
    # set((1,2,6,12))->Por algun motivo JSONEncoder no va bien con sets -> Arreglado en JSONEncoderSerializable
    jsonData = {1, 2, 25, 12, 25, 7, 4, 25}  # (temp.uids, *temp.dataList)
    print(f'Data Struct to convert to JSON: {jsonData}')
    temp1 = DataTable(tbl)                                                         # , fldCondicionesTrigger=jsonData
    # temp1.setVal(0, fldID=None, fldTimeStamp=time_mt('datetime'))
    # temp1.setVal(1, fldID=None, fldTimeStamp=time_mt('datetime'), fldCondicionesTrigger=jsonData)
    # temp1.setVal(2, fldID=None, fldTimeStamp=time_mt('datetime'), fldCondicionesTrigger=jsonData)
    temp1.setVal(0, fldID=None, fldDate=time_mt('datetime'), fldFK_Actividad=30, fldMemData=1)
    temp1.setVal(1, fldID=None, fldDate=time_mt('datetime'), fldFK_Actividad=31, fldMemData=1)
    temp1.setVal(2, fldID=None, fldDate=time_mt('datetime'), fldFK_Actividad=32, fldMemData=1)
    print(f'temp1(0): {temp1.unpackItem(1)}')
    # for j in range(100):  # Lazo para temporizar setRecords() sin writeQueue.





    id_fldID = setRecord(tbl, **temp1.unpackItem(1))

    # INSERT INTO "Data Animales Actividad Inventario" ('ID_Data Inventario','ID_Actividad','Fecha Evento') VALUES
    # ((SELECT IFNULL(MAX(ROWID), 0)+1 FROM "Data Animales Actividad Inventario"), 4, '2022-04-25 12:12:12.121212')

    # setRecTuple = temp1.setRecords()
    # id_fldID = setRecTuple[1] if setRecTuple[1] else temp1.getVal(0, 'fldID')
    # print(f'setRecords() returns: {setRecTuple}')
    # print(f'Decoding:')

    temp = getRecords(tbl, '', '', None, '*', fldID=id_fldID)
    print(f'Retrieved Dict: {temp.unpackItem(0)}')
    JSONFldDecoded = temp.getVal(0, 'fldCondicionesTrigger')
    dataList = list(JSONFldDecoded) if hasattr(JSONFldDecoded, '__iter__') else []
    print(f'JSON Decoded List: {JSONFldDecoded} \n type(JSONFldDecoded): {type(JSONFldDecoded)}')


    # dataList.remove(25)       # Test: Remueve repetidos eliminando el primero que encuentra en la lista
    # print(f'DataList remove 1 25: {dataList}')
    # dataList.remove(25)
    # print(f'DataList remove 2 25: {dataList}')
    # dataList.remove(25)
    # print(f'DataList remove 3 25: {dataList}')
