from krnl_config import time_mt
from custom_types import DataTable, getRecords, getTblName, getFldName

if __name__ == '__main__':

    # timeStamp = time_mt('dt')
    # userID = sessionActiveUser

    # Crea tabla vacia usando constructor. Asi se crean las tablas para llenarlas luego de datos provenientes de la UI.
    tblTags = DataTable('tblCaravanas')

    # Crea tablas leyendo de DB
    tblAnimales = getRecords('tblAnimales', '', '', None, '*', fldMF='F')     # DataTable con todos los animales Hembra
    print(f'\n>>>Animales')
    print(f'Table DB Name:  {tblAnimales.dbTblName}, Table Name: {tblAnimales.tblName}')
    print(f'dataLen (cantidad de registros en lista dataList): {tblAnimales.dataLen}')
    print(f'Cantidad de Campos de {tblAnimales.tblName}: {tblAnimales.fldNamesLen}')
    leField = 'fldDOB'
    print(f'Get name for {leField}, please: {tblAnimales.getDBFldName(leField)}. -- Now, get the index for {leField}'
          f' in the Fields List: {tblAnimales.getFldIndex(leField)}')
    print(f'      fldNames: {tblAnimales.fldNames} \nDB field Names: {tblAnimales.dbFldNames}')
    print(f'dataList (Lista con todos los registros en DataTable): {tblAnimales.dataList}')
    print(f'\nComo "desempacar" un registro de la DataTable (Item 0): {tblAnimales.unpackItem(0)}')
    print(f'Leer un valor: Record #4 (OJO: registros comienzan en 0), campo Raza: {tblAnimales.getVal(4, "fldFK_Raza")}')
    print(f'\nSetear un valor en tabla Caravanas: {tblTags.setVal(0, fldID=8, fldTagNumber="ABC789", fldFK_Color=7)}')
    print(f'Leer Tag Number antes seteado: {tblTags.getVal(0, "fldTagNumber")}')
    print(f'Ahora, desempaca un campo de tabla Caravanas a Dict: {tblTags.unpackItem(0)}')
    print(f'Unpack() retorna tambien Dict con DB Field Names: {tblTags.unpackItem(0, 1)}')
    print(f'\nAhora usamos time_mt -> En segundos: {time_mt()} / Como objeto datetime: {time_mt("datetime")}')



    # Prueba pack/unpack de tablas. Descomentar para ver lo que pasa (si queres..)
    # tabla1 = getRecords(tblTags.tblName, '', '', None, '*')     # Crea dataTable usando el nombre de otra dataTable
    # packedDict = tabla1.packTable()
    # print(f'\npackedDict={packedDict}')
    # unpackedTemp = DataTable.unpackTable(packedDict)
    # print(f'unPackedTemp: {unpackedTemp}')
    # print(f'Unpacked Table fields: {unpackedTemp.fldNames}\ndataList={unpackedTemp.dataList}')

