from krnl_cfg import *
from custom_types import DataTable, getRecords, setupArgs
from krnl_sqlite import SQLiteQuery, getTableInfo
# from krnl_cfg import moduleName, callerFunction
# from operator import attrgetter
from krnl_parsing_functions import writeObj, setRecord
from krnl_object_instantiation import loadItemsFromDB
from krnl_bovine import Bovine
# from krnl_bovine_activity import BovineActivity
# from krnl_animal import Animal
from time import sleep
from krnl_animal_activity import AnimalActivity, ProgActivityAnimal
from krnl_abstract_class_prog_activity import ActivityTrigger
from krnl_geo_new import handlerGeo, Geo
# from krnl_abstract_class_activity import Activity
# from fibooks import info, balance_sheet, income_statement, excel_parser, other

flds = ['fldID', 'fldDate', 'fldFK_NombreActividad', 'fldFK_ClaseDeAnimal', 'fldPAData', 'fldWindowLowerLimit',
       'fldDiasParaEjecucion', 'fldWindowUpperLimit', 'fldDaysToAlert', 'fldDaysToExpire', 'fldComment']
def generateDataProgramacion(**kwargs):
    tblName = 'tblDataProgramacionDeActividades'
    temp = getRecords(tblName, '', '', None, '*', fldID=1)
    temp.setVal(0, **kwargs)
    temp.setRecords()


if __name__ == '__main__':

    timeStamp = time_mt('dt')
    userID = sessionActiveUser
    tblDataProg = DataTable('tblDataProgramacionDeActividades')
    tblTriggers = DataTable('tblAnimalesActividadesProgramadasTriggers')
    tblLinkPA = DataTable('tblLinkAnimalesActividadesProgramadas')
    # Prueba pack/unpack de tablas.
    # temp = getRecords(tblTriggers.tblName, '', '', None, '*')
    # packedDict = temp.packTable()
    # print(f'packedDict={packedDict}')
    # unpackedTemp = DataTable.unpackTable(packedDict)
    # print(f'unPackedTemp: {unpackedTemp}')
    # print(f'Unpacked Table fields: {unpackedTemp.fldNames}\ndataList={unpackedTemp.dataList}')
    # print(f'*** Are these 2 equal?: deepDiff: {DeepDiff(temp, unpackedTemp)} / ==: {temp == unpackedTemp}')

    shortList = (1, 4, 8, 11, 18, 27, 32, 130, 172, 210, 244, 280, 398, 41, 61, 92, 120)  # ID=0 no existe.
    bovines = loadItemsFromDB(Bovine, shortList, initializeTags=True)  # Abstract Factory funca lindo aqui...
    Geo.initialize()
    # for k in handlerGeo.getGeoEntities():
    #     print(f'{k}: {handlerGeo.getGeoEntities()[k]}')

    ret = ProgActivityAnimal.loadTriggers()
    print(f'loadTriggers retValue: {ret}')
    for j in ProgActivityAnimal.getTriggerRegisterDict():
        print(f'{j}: {ProgActivityAnimal.getTriggerRegisterDict()[j].triggerFields}\n'
              f'dataProgDict={ProgActivityAnimal.getTriggerRegisterDict()[j].dataProgDict}')
    triggersList = ProgActivityAnimal.getTriggerRegisterDict().values()
    # for bov in bovines:
    #     for trigger in triggersList:
    #         bov.pa.assignTrigger(trigger)     # NO DEBIERA SER NECESARIO ASIGNAR TRIGGERS A OBJETOS INDIVIDUALES...
    #
    # print(f'*** In the end: Animal Triggers are: {ProgActivityAnimal.getTriggerRegisterDict()}')
    # ret = None
    if ret is None:     # Crea algunos triggers si la tabla esta vacia.
        # Programacion y Trigger para Castracion
        tblDataProg.setVal(0, fldDate=datetime(2022, 10, 20, 11, 30, 0, 15), fldTimeStamp=timeStamp,  fldDaysToAlert=30,
                           fldDiasParaEjecucion=180, fldWindowLowerLimit=150, fldWindowUpperLimit=220,
                           fldDaysToExpire=270, fldFK_UserID=userID, fldFK_ClaseDeAnimal=1, fldFK_Actividad=4)
        tblDataProg.setVal(fldPAData={'tblAnimales__fldMF': 'M', 'tblAnimales__fldFlagCastrado': 0,
                                      'tblDataAnimalesActividadSanidadAplicaciones__fldComment':
                                          'TEST: Data Programacion para Castracion-El Ñandú',
                                      'tblAnimales__fldFK_NombreActividad': 4, 'tblAnimales__fldFK_ClaseDeAnimal': 1})

        # Test setRecord(). INSERT
        idx = setRecord(tblDataProg.tblName, **tblDataProg.unpackItem(0))
        print(f'idx INSERT={idx}')

        tblTriggers.setVal(0, fldCondicionesTrigger={'tblDataAnimalesActividadLocalizacion__fldFK_Localizacion': 545,
                           'tblAnimales__fldMF': 'M'}, fldDescription='Castracion - El Nandu')
        tblTriggers.setVal(0, fldFK_ActividadDisparadora=6, fldFK_ActividadProgramada=4, fldTimeStamp=timeStamp,
                           fldFlag=1, fldFK_UserID=userID, fldFK_DataProgramacion=idx)
        print(f'Triggers: {tblTriggers.unpackItem(0)} \nData Programacion: {tblDataProg.unpackItem(0)}')

        tblTriggers.setRecords()
        trigger = ProgActivityAnimal.createTrigger(tblTriggers, tblDataProg)
        ProgActivityAnimal.registerTrigger(trigger)

        # Programacion y Trigger Brucelosis. Dias para Ejecucion: 210 desde DOB.
        tblDataProg.setVal(1, fldDate=datetime(2022, 10, 20, 11, 30, 0, 151515), fldTimeStamp=timeStamp,
                           fldDiasParaEjecucion=210, fldWindowLowerLimit=180, fldWindowUpperLimit=240, fldDaysToExpire=330,
                           fldDaysToAlert=30, fldFK_UserID=userID, fldFK_ClaseDeAnimal=1, fldFK_Actividad=40)
        tblDataProg.setVal(1, fldPAData={'tblAnimales__fldMF': 'F',
                                'tblDataAnimalesActividadSanidadAplicaciones__fldActiveIngredient': 'BrumicinaTruch',
                                'tblDataAnimalesActividadSanidadAplicaciones__fldFK_FormaDeSanidad': 1,
                                'tblDataAnimalesActividadSanidadAplicaciones__fldComment': 'No More Brucella',
                                         'minAgeDays': 3*30, 'maxAgeDays': 9*30
                                         })
        idx = setRecord(tblDataProg.tblName, **tblDataProg.unpackItem(1))
        print(f'idx INSERT Brucelosis={idx}')


        tblTriggers.setVal(0, fldCondicionesTrigger={'tblDataAnimalesActividadLocalizacion__fldFK_Localizacion': 36,
                                                     'tblAnimales__fldMF': 'M'}, fldDescription='Brucelosis - Santa Fe')
        tblTriggers.setVal(0, fldFK_ActividadDisparadora=1, fldTimeStamp=timeStamp, fldFlag=1, fldFK_UserID=userID,
                           fldFK_DataProgramacion=idx)
        print(f'Triggers: {tblTriggers.unpackItem(0)} \nData Programacion: {tblDataProg.unpackItem(1)}')
        tblTriggers.setRecords()
        paObject = ProgActivityAnimal('Actividad Programada')
        trigger = paObject.createTrigger(tblTriggers, tblDataProg)
        paObject.registerTrigger(trigger)
        print(f'Programmed Activity (paObject) es: {paObject.activityID}')

    print(f'triggerRegisterDict: {ProgActivityAnimal.getTriggerRegisterDict()}\ngetswitchinterval={sys.getswitchinterval()}')

    trg1, trg2 = 2, 1
    trigg1 = ProgActivityAnimal.getTriggerRegisterDict()[trg1]
    # cond1Dict = trigg1.triggerFields
    trigg2 = ProgActivityAnimal.getTriggerRegisterDict()[trg2]
    # cond2Dict = trigg2.triggerFields
    # sleep(0.3)
    # dDiff = DeepDiff(cond1Dict, cond2Dict, truncate_datetime='day')
    # print(f'%%%%%%%%%%%%% This is Deepdiff: {dDiff.to_dict()}')
    krnl_logger.info(f'--------------------  Now comparing triggers {trg1} and {trg2} ---------------------------\n')
    trigg2.compareTriggers(trigg1, excluded_fields=('fldID',))

    # RAPTable = getRecords('tblAnimalesRegistroDeActividadesProgramadas', '', '', None, '*', fldFlag=(1, 2))
    # print(f'\n TRIGGER ASSIGNMENTS: ')
    # for j in bovines:
    #     print(f'{j.ID}-{j.category.get(id_type="name")}: {str([i.description for i in j.myTriggers])} -')

    print(f'Stopping write spooler...', end=' ')
    writeObj.stop()         # El join() ejecutado por stop() hace correr tambien SQLiteQuery.__del__












def setProgActivity(*args):

    dictRAP = {'tblAnimalesRegistroDeActividadesProgramadas':
                   {'fldFlag': None, 'fldFK_ClaseDeAnimal': None, 'fldFK_Trigger': None, 'fldID': None,
                    'fldFK_NombreActividad': None, 'fldTimeStamp': None, 'fldFK_DataProgramacion': {},
                    'fldComment': None, 'fldFK_UserID': None, 'fldPushUpload': None, 'fldBitmask': 0}}

    dictLinkPA = {'tblLinkAnimalesActividadesProgramadas':
                      {'fldBaseDate': None, 'fldTimeStamp': None, 'fldFK_ActividadDeCierre': None, 'fldID': None,
                       'fldFK_Actividad': None, 'fldFK': None, 'fldComment': None, 'fldInstanciaTratamiento': None,
                       'fldCounter': None, 'fldPushUpload': None, 'fldBitmask': None
                       }}

    dictDataProg = {'tblDataProgramacionDeActividades':
                        {'fldDiasParaEjecucion': None, 'fldID': None, 'fldDate': None, 'fldDaysToAlert': None,
                         'fldWindowUpperLimit': None, 'fldDaysToExpire': None, 'fldFK_UserID': None, 'fldComment': '',
                         'fldWindowLowerLimit': None, 'fldPAData': None, 'fldPushUpload': None, 'fldBitmask': 0,
                         'fldTimeStamp': None}}

    dictSequences = {'tbAnimalesAPSecuencias':
                         {'fldID': None, 'fldTimeStamp': None, 'fldFK_Tratamiento': None, 'fldFlag': None,
                          'fldComment': '', 'fldFK_UserID': None, 'fldPushUpload': None, 'fldBitmask': None }}

    dictSequenceActivities = {'tblAnimalesAPSecuenciasActividades':
                                  {'fldID': None, 'fldFK_Secuencia': None, 'fldDate': None, 'fldFK_Actividad': None,
                                   'fldFK_DataProgramacion': None, 'fldOrdinal': None, 'fldDiasParaEjecucion': None,
                                   'fldFK_Tratamiento': None, 'fldComment': '', 'fldFK_UserID': None}}
    dictTreatments = {'tblAnimalesSanidadTratamientos':
                          {'fldID': None, 'fldFK_NombreDeTratamiento': None,
                           'fldFK_TratamientoSubcategoria': None, 'fldComment': None, 'fldFlag': None, 'fldFK': None,
                           'fldFK_UserID': None, 'fldPushUpload': None, 'fldBitmask': None, 'fldTimeStamp': None}}   # fldFK: ID_Categoria

    tblDataProg = DataTable('tblDataProgramacionDeActividades')
    tblTriggers = DataTable('tblAnimalesActividadesProgramadasTriggers')
    tblLinkPA = DataTable('tblLinkAnimalesActividadesProgramadas')
    tblRAP = DataTable('tblAnimalesRegistroDeActividadesProgramadas')













    # i = 0
    # while i < 30:
    #     print(i % 10, end=', ')
    #     i += 1          # Lacito para generar acceso circular a lista [0 a 9]
