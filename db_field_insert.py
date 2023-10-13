from time import sleep
from krnl_cfg import *
# import functools
from custom_types import DataTable, getRecords, setDBData
# from krnl_cfg import moduleName, callerFunction
# from operator import attrgetter
# from json import dump, dumps, loads, load
from krnl_parsing_functions import writeObj, setRecord, getFldName, createUID, fldNameFromUID, tblNameFromUID
from krnl_sqlite import SQLiteQuery

if __name__ == '__main__':

    queryObj = SQLiteQuery()

    # Generacion de Campo PushUpload / BitMask
    # tblPopList1 = ('_sys_Tables', '_sys_Fields', 'Sys Versiones DB', 'Unidades Tipos', 'Unidades Sistemas',
    #               'Sys Errores Codigos', 'Unidades Nombres', 'Animales Sanidad Actividades Del Tratamiento TEMPLATE',
    #               'Sys Registro De Sistema', 'Geo Region Provincia - NO USADA', 'Geo Locaciones - NO USADA',
    #               'Geo Establecimientos - NO USADA', 'Geo Departamentos - NO USADA',
    #               'Geo Ciudades Pueblos Lugares - NO USADA',
    #               'Data Geo Entidades Coordenadas', 'Animales Registros De Cria - RETIRED', 'Actividades Signatures')
    #
    # # strSQL = f"SELECT Table_Key_Name, Table_Name, ID_Table FROM [_sys_Tables]"
    # strSQL = f"SELECT Table_Name FROM [_sys_Tables]"
    # cur = queryObj.execute(strSQL)
    # tblNameList = cur.fetchall()
    # tblNameList = [j[0] for j in tblNameList]
    # for i in tblPopList1:
    #     tblNameList.remove(i)
    # print(f'table List: {len(tblNameList)} / {tblNameList}')

    # Codigo para insertar campo Bitmask en las tablas de DB seleccionadas
    # for j in tblNameList:
    #     strAlterTable = f'ALTER TABLE "{j}" ADD PushUpload JSON; '
    #     cur = queryObj.execute(strAlterTable)
    # Codigo para insertar campo Bitmask en las tablas de DB seleccionadas
    # for j in tblNameList:
    #     strAlterTable = f'ALTER TABLE "{j}" ADD Bitmask INTEGER; '
    #     cur = queryObj.execute(strAlterTable)

    # Generacion de Campo TimeStamp
    tblPopList2 = ('_sys_Tables', '_sys_Fields', 'Sys Versiones DB', 'Unidades Tipos', 'Unidades Sistemas',
                  'Sys Errores Codigos', 'Unidades Nombres', 'Animales Sanidad Actividades Del Tratamiento TEMPLATE',
                  'Geo Region Provincia - NO USADA', 'Geo Locaciones - NO USADA',
                  'Geo Establecimientos - NO USADA', 'Geo Departamentos - NO USADA', 'Geo Ciudades Pueblos Lugares - NO USADA',
                  'Animales Registros De Cria - RETIRED', 'Actividades Signatures',
                    'Animales Alimento Nombres',
                    'Animales Actividades Programadas Triggers',
                    'Caravanas Registro De Actividades',
                    'Data Animales Parametros Generales',
                    'Data Listas Elementos',
                    'Data MoneyActivity Bancos Link CuentasPersonas',
                    'Dispositivos',
                    'Dispositivos Registro De Actividades',
                    'Link Animales Actividades Programadas',
                    'Registro De Notificaciones',
                    'Geo Entidades',
                    'Listas',
                    'Personas',
                    'Personas Registro De Actividades',
                    'Proyectos',
                    'Dispositivos Registro Datastream',
                    'Data Personas Disparadores Operandos',
                    'Data Personas Disparadores Parametros',
                    'Dispositivos Identificadores',
                    'Animales Registro De Actividades',
                    'MoneyActivity Registro De Actividades',
                    'Listas Registro De Actividades',
                    'Caravanas Registro DataStream',
                    'Dispositivos Registro De Actividades Programadas',
                    'Animales AP Secuencias',
                    'Animales Registro De Actividades Programadas'

                   )

    strSQL = f"SELECT Table_Name FROM [_sys_Tables]"
    cur = queryObj.execute(strSQL)
    tblNameList = cur.fetchall()
    tblNameList = [j[0] for j in tblNameList]
    for i in tblPopList2:
        try:
            tblNameList.remove(i)
        except Exception:
            print(f'Not in tableList: {i}', end='; ')
    print(f'table List: {len(tblNameList)} / {tblNameList}')

    # # Codigo para insertar campo TimeStamp en todas las tablas de DB
    # for j in tblNameList:
    #     strAlterTable = f'ALTER TABLE "{j}" ADD TimeStamp TIMESTAMP; '
    #     cur = queryObj.execute(strAlterTable)


    # i = 0
    # while i < 30:
    #     print(i % 10, end=', ')
    #     i += 1          # Lacito para generar acceso circular a lista [0 a 9]
