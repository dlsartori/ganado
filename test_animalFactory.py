from krnl_cfg import lineNum, callerFunction, os, krnl_logger
from os import path
from krnl_custom_types import DataTable, getRecords
from krnl_tag import Tag
from krnl_tag_bovine import TagBovine
from krnl_tag_caprine import TagCaprine
from krnl_object_instantiation import altaAnimal, __bajaAnimal, animalCreator
from krnl_bovine import Bovine, Animal
# from datetime import datetime
# from typing import List, Optional
# from pydantic import BaseModel


def moduleName():
    return str(path.basename(__file__))

if __name__ == "__main__":
    tblDataCategoryName = 'tblDataAnimalesCategorias'
    tblDataStatusName = 'tblDataAnimalesActividadStatus'
    tblPersonasName = 'tblPersonas'
    tblLocalizationName = 'tblDataAnimalesActividadLocalizacion'
    tblAnimals = DataTable('tblAnimales')
    print(f'\n                                                 ----------------------------  Test de alta() ---------'
          f'---------------------')
    animalClassName = Bovine   # .animalKind()      # 'Vacuno'. Va a crear objetos Bovine y tags TagBovine con BovineFactory

                            # TODO(cmt): El Abstract Factory funca lindo...
    newTag = animalCreator.tag_creator(animalClassName, generateTag=True)
    if isinstance(newTag, str):
        raise TypeError(f'{krnl_logger.error(newTag)}')
    newTag.localization.set(localization=549)
    print(f'{newTag.localization.get(mode="val")}')
    tblAnimals.setVal(0, fldMF='M', fldFK_ClaseDeAnimal=1, fldFlagCastrado=0, fldFK_Raza=5, fldDateExit='')
    tblCategory = DataTable(tblDataCategoryName, fldFK_Categoria=8, fldDate='2022-08-11 00:01:01.123456')  # 8: Novillo
    tblStatus = DataTable(tblDataStatusName)
    tblOwners = getRecords(tblPersonasName, '', '', None, '*', fldLastName=('Sartori', 'González/Buyatti',
                                                                               'Gonzalez/Buyatti'))
    tblDataOwners = DataTable('tblDataAnimalesActividadPersonas')
    tblLocalization = DataTable(tblLocalizationName, fldFK_Localizacion=547)  # 547: Lote 1 El Nandu

    for j in range(tblOwners.dataLen):
        tblDataOwners.setVal(j, fldFK_Persona=tblOwners.getVal(j, 'fldID'),
                             fldPercentageOwnership=1 / tblOwners.dataLen,
                             fldComment=tblOwners.getVal(j, 'fldLastName') + ', ' + tblOwners.getVal(j, 'fldName'))

    # print(f'obj_data owners: {tblDataOwners.dataList}')
    newAnimal = altaAnimal('Vacuno', 'Nacimiento', tblAnimals, tblCategory, tblStatus, tblDataOwners, tblLocalization,
                           tags=[newTag, ])

    # if isinstance(newAnimal, str):
    #     exit(1)

    print(f'New Animal: {newAnimal.mode.upper()} - ID: {newAnimal.getID}  / Data: {newAnimal.__dict__}')

    # print(f'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDUMY Animal')
    # dummyAnimal = perform('Vacuno', 'Dummy', tblAnimals, tblCategory, tblLocalization, tblDataOwners)
    # print(f'New Animal DUMMY - ID: {newAnimal.getID}  / Data: {newAnimal.__dict__}')

    newAnimal.inventory.set()
    print(f'newAnimal.inventory.get(): {newAnimal.inventory.get()}')
    newAnimal.status.set(status='En Stock')
    cat = newAnimal.category.get()
    newAnimal.category.set(category=cat, enforce=True)

    print(f'  \n                      ((((((((((((((((((((((((((( Baja de Animal )))))))))))))))))))))))))))))))))))')
    idActividadRA = __bajaAnimal(newAnimal, 'Venta')


    # print(f'                         ((((((((((( animalitos ))))))))))))))')
    # print(f'isType(newAnimal): {isType(newAnimal, DataTable)} / type(newAnimal): {type(newAnimal)} / name(DataTable): {Bovine.__name__}')
