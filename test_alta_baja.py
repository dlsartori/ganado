from krnl_config import *
from krnl_tag import Tag
from krnl_custom_types import getRecords, DataTable, close_db_writes
from datetime import datetime
from krnl_object_instantiation import loadItemsFromDB
from krnl_geo_new import Geo
from krnl_bovine import Bovine
try:
    import psutil           # These 2 used to query total memory used by application.
    import os
except ImportError:
    pass

def moduleName():
    return str(os.path.basename(__file__))

if __name__ == '__main__':
    tblDataCategoryName = 'tblDataAnimalesCategorias'
    tblDataStatusName = 'tblDataAnimalesActividadStatus'
    tblPersonasName = 'tblPersonas'
    tblLocalizationName = 'tblDataAnimalesActividadLocalizacion'
    tblAnimals = DataTable('tblAnimales')
    print(f'                                             ----------------------------  Test de alta() ---------'
          f'---------------------')
    USE_DAYS_MULT = False

    shortList = (0, 1, 4, 8, 11, 18, 27, 32, 130, 172, 210, 244, 280, 398, 41, 61, 92, 363, 368, 372)
    bovines = loadItemsFromDB(Bovine, items=shortList, init_tags=True)
    print(f'--- Initial bovines in register: {len(Bovine.getRegisterDict().keys())} / {Bovine.getRegisterDict().keys()}')
    print(f'--- Initial Tags in register: {len(Tag.getRegisterDict().keys())} / {Tag.getRegisterDict().keys()}')
    newTag = Tag.generateTag()  # generateTag() es una funcion auxiliar p/ crear un tag. Usar solo durante el desarrollo
    tblAnimals.setVal(0, fldMF='M', fldFK_ClaseDeAnimal=1, fldFlagCastrado=1, fldFK_Raza=5, fldDateExit='',
                      fldDOB=datetime.strptime('2022-08-11 00:01:01.222222', fDateTime))
    tblCategory = DataTable(tblDataCategoryName, fldFK_Categoria=8,
                            fldDate=datetime.strptime('2022-08-11 00:01:01.222222', fDateTime))  # 8: Novillo
    tblStatus = DataTable(tblDataStatusName)
    # tblStatus = DataTable(tblDataStatusName, fldFK_Status=1, fldDate=getNow(fDateTime))
    tblOwners = getRecords(tblPersonasName, '', '', None, '*', fldLastName=['Sartori', 'Gonzalez/Buyatti'])
    tblDataOwners = DataTable('tblDataAnimalesActividadPersonas')

    tblLocalization = DataTable(tblLocalizationName, fldFK_Localizacion=Geo.getUID('El Ñandu -Lote 1'))  # 547:Lote1-El Nandu
    for j in range(tblOwners.dataLen):
        tblDataOwners.setVal(j, fldFK_Persona=tblOwners.getVal(j, 'fldID'), fldPercentageOwnership=1/tblOwners.dataLen,
                             fldComment=tblOwners.getVal(j, 'fldLastName') + ', ' + tblOwners.getVal(j, 'fldName'))
    print(f'Owners: {tblDataOwners.unpackItem(0)}')

    bovines[0].inventory.set(date='2023-11-01 15:00:00')

    newAnimal = Bovine.alta('Nacimiento', tblAnimals, tblCategory, tblStatus, tblDataOwners, tblLocalization,
                                      tags=newTag)
    print(f'--- New Animal REGULAR - ID: {newAnimal.ID}  / Dict: {newAnimal.__dict__}')

    # Animal Dummy
    # 10# print(f'New Animal DUMMY - ID: {newAnimal.getID}  / Data: {newAnimal.__dict__}')

    print(f'--- Final bovines in register: {len(Bovine.getRegisterDict().keys())} / {Bovine.getRegisterDict().keys()}')
    print(f'--- Final Tags in register: {len(Tag.getRegisterDict().keys())} / {Tag.getRegisterDict().keys()}')
    try:
        process = psutil.Process(os.getpid())
        print(f'\n+++++++++++++++++++++  Total memory used by process (MB): {process.memory_info().rss/(1024 * 1024)}.')
    except (AttributeError, NameError):
        pass



    print(f'\n\n                                   ################# Baja de Animal {newAnimal.ID} ###################')

    idActividadRA = newAnimal.baja('Venta')


    close_db_writes()       # Flushes all buffers, writes all data to DB and suspends db write operations.

    stop = 6



