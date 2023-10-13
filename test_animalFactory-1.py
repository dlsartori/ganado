from krnl_cfg import lineNum, callerFunction, os, getRecordsNew, krnl_logger, isType, lower_nones
from os import path
from krnl_argumentTable import DataTable
from krnl_tag import Tag
from krnl_tag_bovine import TagBovine
from krnl_tag_caprine import TagCaprine
from krnl_object_instantiation import altaAnimal, __bajaAnimal, animalCreator, loadItemsFromDB
from krnl_bovine import Bovine, Animal
from krnl_person import Person
from krnl_animal_activity import AnimalActivity
# from datetime import datetime
# from typing import List, Optional
# from pydantic import BaseModel


def moduleName():
    return str(path.basename(__file__))

if __name__ == "__main__":
    print(f'\n                                                 ----------------------------  Test de alta() ---------'
          f'---------------------')

    # bovineList = [0, 1, 4, 8, 11, 18, 27, 32, 41, 61, 92, 120, 130, 172, 210, 244, 280, 398]
    shortList = (0, 1, 4, 8, 11, 18, 27, 32, 130, 172, 210, 244, 280, 398)  # ID=0 no existe.
    bovines = loadItemsFromDB(Bovine, *shortList, initializeTags=True)  # Abstract Factory funca lindo aqui...
    people = loadItemsFromDB(Person)

    # tblOwners = getRecords(tblPersonasName, '', '', None, '*', fldLastName=('Sartori', 'González/Buyatti',
    #                                                                            'Gonzalez/Buyatti'))
    # tblDataOwners = DataTable('tblDataAnimalesActividadPersonas')
    # tblLocalization = DataTable(tblLocalizationName, fldFK_Localizacion=547)  # 547: Lote 1 El Nandu
    # for j in range(tblOwners.dataLen):
    #     tblDataOwners.setVal(j, fldFK_Persona=tblOwners.getVal(j, 'fldID'),
    #                          fldPercentageOwnership=1 / tblOwners.dataLen,
    #                          fldComment=tblOwners.getVal(j, 'fldLastName') + ', ' + tblOwners.getVal(j, 'fldName'))
    # print(f'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDUMY Animal')
    # dummyAnimal = perform('Vacuno', 'Dummy', tblAnimals, tblCategory, tblLocalization, tblDataOwners)
    # print(f'New Animal DUMMY - ID: {newAnimal.getID}  / Data: {newAnimal.__dict__}')

    newAnimal = bovines[4]
    newAnimal.inventory.set()
    print(f'Animal {newAnimal.ID}.inventory.get(): {newAnimal.inventory.get()}')
    print(f'Animal {newAnimal.ID}.category.get(): {newAnimal.category.get()}')
    stat = 'En Stock'
    print(f'Animal {newAnimal.ID}.status.set({stat}):  {newAnimal.status.set(status=stat)}')
    print(f'Animal {newAnimal.ID}.category.get(): {newAnimal.category.get()}')
    cat = 8
    print(f'Animal {newAnimal.ID}.category.set({cat}): {newAnimal.category.set(category=cat, enforce=True)}')
    print(f'Animal {newAnimal.ID}.category.get({cat}): {newAnimal.category.get()}')
    print(f'{[j.getElements for j in people]}')
    for j in bovines:
        dicto = j.IDFromTagNum('8330')    # Retorna {animalObj: [tagObj, ]} OJO: el value es SIEMPRE una lista de Tags
        for key in dicto:                 # IDFromTagNum retorna objetos para mayor flexibilidad.
            print(f'OBJID: {key.ID} - {key.__class__.__name__} / '
                  f'TagID: {[dicto[key][j].ID for j in range(len(dicto[key]))]} - {dicto[key][0].__class__.__name__}')
            break
        break

    # print(f'  \n                      ((((((((((((((((((((((((((( Baja de Animal )))))))))))))))))))))))))))))))))))')
    # idActividadRA = perform(newAnimal, 'Venta')


