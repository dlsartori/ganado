from krnl_config import *
from krnl_tag import Tag
from krnl_custom_types import getrecords, close_db_writes, getFldName
from datetime import datetime
import pandas as pd
from krnl_object_instantiation import loadItemsFromDB
from krnl_geo import Geo
from krnl_bovine import Bovine
from krnl_tag import Tag
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
    dfAnimals = pd.DataFrame.db.create('tblAnimales')
    print(f'                                             ----------------------------  Test de alta() ---------'
          f'---------------------')
    USE_DAYS_MULT = False

    shortList = (0, 1, 4, 8, 11, 18, 27, 32, 130, 172, 210, 244, 280, 398, 41, 61, 92, 363, 368, 372)
    bovines = loadItemsFromDB(Bovine, items=shortList, init_tags=True)
    # print(f'--- Initial bovines in register: {len(Bovine.getRegisterDict().keys())} / {Bovine.getRegisterDict().keys()}')
    # print(f'--- Initial Tags in register: {len(Tag.getRegisterDict().keys())} / {Tag.getRegisterDict().keys()}')
    newTag = Bovine._myTagClass().generateTag()  # generateTag() es una funcion auxiliar p/ crear un tag. Usar solo durante el desarrollo

    dfAnimals.loc[0, ('fldMF', 'fldFK_ClaseDeAnimal', 'fldFlagCastrado', 'fldFK_Raza', 'fldDateExit', 'fldDOB')] = \
                     ('M', 1, 1, 5, '', datetime.strptime('2022-08-11 00:01:01.222222', fDateTime))
    dfCategory = pd.DataFrame.db.create(tblDataCategoryName)
    dfCategory.loc[0, ('fldFK_Categoria', 'fldDate')] = (8, datetime.strptime('2022-08-11 00:01:01.222222', fDateTime))
    # tblCategory = DataTable(tblDataCategoryName, fldFK_Categoria=8,
    #                         fldDate=datetime.strptime('2022-08-11 00:01:01.222222', fDateTime))  # 8: Novillo
    # tblStatus = DataTable(tblDataStatusName)
    # tblStatus = DataTable(tblDataStatusName, fldFK_Status=1, fldDate=getNow(fDateTime))
    dfStatus = pd.DataFrame.db.create(tblDataStatusName)
    dfStatus.loc[0, ('fldFK_Status', 'fldDate')] = (1, time_mt('dt'))
    dfOwners = getrecords(tblPersonasName, '*', where_str=f'WHERE "{getFldName(tblPersonasName, "fldLastName")}" '
                                                          f'IN ("Sartori", "Gonzalez/Buyatti")')
    tblDataOwners = pd.DataFrame.db.create('tblDataAnimalesActividadPersonas')

    dfLocalization = pd.DataFrame.db.create(tblLocalizationName)
    dfLocalization.loc[0, 'fldFK_Localizacion'] = Geo.getUID('El Ñandu -Lote 1')  # 547:Lote1-El Nandu

    for j, row in dfOwners.iterrows():
        tblDataOwners.loc[j, ('fldFK_Persona', 'fldPercentageOwnership', 'fldComment')] = \
            (dfOwners.loc[j, 'fldID'], 1/len(dfOwners.index), dfOwners.loc[j, 'fldLastName'] + ', ' +
             dfOwners.loc[j, 'fldName'])
    print(f'Owners: {dfOwners.loc[0].to_dict()}')

    bovines[0].inventory.set(date='2024-03-22 15:00:00')

    df_animals = pd.DataFrame.db.create('tblAnimales')
    df_animals.loc[0, ('fldMF', 'fldFK_ClaseDeAnimal', 'fldFlagCastrado', 'fldFK_Raza', 'fldDateExit', 'fldDOB',
                       'fldMode')] = ('M', 1, 1, 5, '', datetime.strptime('2022-08-11 00:01:01.222222', fDateTime),
                                      'regular')


    # newAnimal = Bovine.alta('Nacimiento', tblAnimals, tblCategory, tblStatus, tblDataOwners, tblLocalization,
    #                                   tags=newTag)

    newAnimal = Bovine.alta('Nacimiento', df_animals, dfCategory, dfStatus, dfOwners, dfLocalization, tags=newTag)
    print(f'--- New Animal REGULAR - ID: {newAnimal.ID}  / Dict: {newAnimal.__dict__}')

    # Animal Dummy
    # 10# print(f'New Animal DUMMY - ID: {newAnimal.getID}  / Data: {newAnimal.__dict__}')

    print(f'--- Final bovines in register: {[o.recordID for o in bovines]}')
    # print(f'--- Final Tags in register: '
    #       f'{[row.fldID for frame in Bovine._myTagClass().obj_dataframe() for j, row in frame.iterrows()]}')
    try:
        process = psutil.Process(os.getpid())
        print(f'\n++++++++++++++  Total memory used by process (MB): {process.memory_info().rss/(1024 * 1024)}.\n')
    except (AttributeError, NameError):
        pass


    # print(f'\n\n                            ################# Baja de Animal {newAnimal.ID} ###################')

    # idActividadRA = newAnimal.baja('Venta')


    close_db_writes()       # Flushes all buffers, writes all data to DB and suspends db write operations.

    stop = 6



