from krnl_tm import *
from krnl_cfg import *
from krnl_entityObject import *
from krnl_tag import *
from krnl_abstract_class_animal import *
from krnl_bovine import *
from krnl_geo_new import handlerGeo
from inspect import currentframe, getframeinfo
import concurrent.futures                   # habilita ThreadPoolExecutor

if __name__ == '__main__':
    # createUtilsTable()
    # Codigo standard para creacion de Caravanas, Animales, etc.
    tagsVector = getRecords('tblCaravanas', '', '', None, '*')      # fldTagNumber=['8330', '8423', '15', '8258', '98', '101', '4']
    tag = []
    for j in range(tagsVector.dataLen):
        tempTag = Tag(**tagsVector.unpackItem(j))
        if tempTag.isValid and (tempTag.exitYN == 0 or tempTag.exitYN is None):
            tag.append(tempTag)
        # print(f'getElements: {getElements} // {tag[j]}  // {tag[j].__ID}')
    print(f'TEST.PY({lineNum()})Total Tags created: {len(tag)}')

    tag[15].inventory.set()    # Llamada tipica a una funcion/actvidad con parametros Default
    k = 3
    lastInv = tag[k].inventory.get('2015-04-11', '2022-12-12')
    if lastInv is not None:
        print(f'TEST.PY({lineNum()}) - Last Inventory tag[{tag[k].tagNumber}]: len:{lastInv.dataLen} // {lastInv.dataList}')
    tag[9].status.set(status='Comisionada')
    bb = tag[9].status.__getattribute__('obj.__outerAttr')
    cc = tag[9].status.get(mode='fullRecord').unpackItem(0)
    print(f'TEST.PY({lineNum()}) getattribute(obj.__outerAttr).tagNumber = {bb.tagNumber} // statuts.get(): {cc}')

    bovines = []
    bovineList = [0, 1, 4, 8, 11, 18, 27, 32, 41, 61, 92, 120, 130, 172, 210, 244, 280, 398]
    aniDataTable = getRecords('tblAnimales', '', '', None, '*')
    tblRA_Desasignados = getRecords('tblAnimalesRegistroDeActividades', '', '', None, 'fldID', fldFK_NombreActividad=
                                        Animal.getActivitiesDict()['Caravaneo - Desasignar'])
    for j in bovineList:
        # a = bovineDataTable.unpackItem(j)
        # print(f'@@ TEST.PY - ANIMAL: {a} ')
        animalObj = Bovine(**aniDataTable.unpackItem(j))
        if animalObj.isValid and not animalObj.exitYN:
            animalObj.tags.initializeTags(tblRA_Desasignados)
            bovines.append(animalObj)
    print(f'TEST.PY Total Bovines created: {len(bovines)}  //  Bovines: {bovines}')
    # for j in bovines:
    #     print(f'AnimalID: {j.getID} / TagID: {j.myTagIDs}')
    # print(f'tag Register Dict: {Tag.getRegisterDict()}')

    bovines[8].status.set(status='Timeout')
    # bovines[7].tm.set(fldFK_ActividadTM=10032)
    bovines[7].tm.get()
    print(f'TEST.PY({lineNum()}) Bovine.getTotalAnimals Function: {Bovine.getTotalAnimals()} ')
    boviLocaliz = bovines[7].localization.get().unpackItem(0)
    print(f'TEST.PY({lineNum()}) bovine[{7}] Localization: {boviLocaliz}')

    # Chequeo de Categorias
    for j in range(len(bovines)):
        print(f'vacuno[{j}]: {bovines[j].category.getCurrentValue("Name")} / Original dob: {bovines[j].dob} / '
              f'New dob: {datetime.strftime(bovines[j].generateDOB(bovines[j].category.getCurrentValue("Name")), fDateTime)}')
              # f'isInstance: {isinstance(bovines[j], Animal)}')
              # f'  - Class Hierarchy: {str(bovines[j].__class__.mro())}')

    # print(f'AND THIS IS THE GENERIC BOVINE!!: {genericBovine.__dict__}')
    print(f'AND THIS IS Bovine Objects Table: {bovines[0].tblObjName()} / id(obj): {id(bovines[0])} obj: {bovines[0].self}')
