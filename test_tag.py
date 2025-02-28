# import faulthandler
import threading
from krnl_config import *
from datetime import timedelta
from krnl_custom_types import DataTable, getRecords, setRecord, close_db_writes, getColors
from krnl_object_instantiation import loadItemsFromDB
from krnl_abstract_class_animal import Animal
from krnl_bovine import Bovine
from krnl_tag import Tag

flds = ["fldID", "fldDate", "fldFK_NombreActividad", "fldFK_ClaseDeAnimal", "fldPAData", "fldWindowLowerLimit",
        "fldDiasParaEjecucion", "fldWindowUpperLimit", "fldDaysToAlert", "fldDaysToExpire", "fldComment"]
def generateDataProgramacion(**kwargs):
    tblName = "tblDataProgramacionDeActividades"
    temp = getRecords(tblName, "", "", None, "*", fldID=1)
    temp.setVal(0, **kwargs)
    temp.setRecords()

# --------------------------------------------------------------------------------------------------------------- #
def setRAPDict(*, activity_id, event_date=None, data_prog=None, claseDeAnimal=1, comment='', flag=1):
    dicto = {'fldID': None,
             'fldFK_Actividad': activity_id,
             'fldFlag': flag,
             'fldFK_DataProgramacion': data_prog,
             'fldFK_ClaseDeAnimal': claseDeAnimal,
             'fldTimeStamp': event_date,
             'fldComment': comment,           # Agregar descripcion de la PA en Comment
             'fldBitmask': 3
             }
    return dicto

def setLinkPADict(*, activityID=None, prog_date=None, items=(), closing_activity=None):
    if isinstance(items, str):
        items = [items, ]
    elif not isinstance(items, (list, tuple)):
        items = ()
    dictList = []
    for j in items:
        dicto = {'fldID': None,
                 'fldFK': j,
                 'fldFK_Actividad': activityID,
                 'fldProgrammedDate': prog_date,
                 'fldFK_ActividadDeCierre': closing_activity,
                 'fldBitmask': 3
                 }
        dictList.append(dicto)
    return dictList


def setDataProgDict(*, InstanciaDeSecuencia=None, activityID=None, fldAgeDays=None, AgeDaysDeviation=None,
                    event_date=None, paDataDict=None, paDataDictCreate=None, claseDeAnimal=1, daysToAlert=None,
                    secuencia=None, lowerLimit=None, upperLimit=None, idDataProg=None, daysToExpire=None):
    if not paDataDict:
        paDataDict = {}             # fldPAData dict in database
    dicto = {'fldID': None,
             # 'fldInstanciaDeSecuencia': InstanciaDeSecuencia,
             # 'fldFK_Secuencia': secuencia,
             'fldPAData': paDataDict,
             'fldPADataCreacion': paDataDictCreate,
             # 'fldFK_Localizacion': localizacion,
             'fldFK_ClaseDeAnimal': claseDeAnimal,
             'age': fldAgeDays,
             'fldDate': event_date,
             'fldAgeDaysDeviation': AgeDaysDeviation,
             'fldWindowLowerLimit': lowerLimit,
             'fldWindowUpperLimit': upperLimit,
             'fldDaysToExpire': daysToExpire,
             'fldFK_DataProgramacion': idDataProg,
             'fldTimeStamp': event_date,
             'fldDaysToAlert': daysToAlert,
             'fldBitmask': 3            # TODO: Upload + sync -> Si la PA es generada por sistema, setar fldBitmask=1
             }
    return dicto


def paSetDataInventory(*, prog_date=None, items=(), pa_dict=None, pa_dict_create=None, tbl_data_prog=None, tbl_RAP=None,
                       tbl_link_pa=None):
    """ returns 3 DataTable Objects: paData, RAP, linkPA, in that order, to feed the """
    activity = 10
    if not isinstance(pa_dict, dict):
        pa_dict = {}
    tbl_data_prog.setVal(0, **setDataProgDict(lowerLimit=30, upperLimit=30, daysToExpire=60, daysToAlert=5,
                                              paDataDictCreate=pa_dict_create, paDataDict=pa_dict))
    tbl_RAP.setVal(0, **setRAPDict(activity_id=activity, comment='Inventario Inventado 4.1.7'))
    if not prog_date:
        prog_date = time_mt('date_time') + timedelta(days=60)           # programmed 60 days from now.
    itemsDict = setLinkPADict(activityID=activity, prog_date=prog_date, items=items)
    for count, dicto in enumerate(itemsDict):
        tbl_link_pa.setVal(count, **dicto)
    # print(f'EEEEEEEEEEEy dataList: {tbl_link_pa.dataList}')
    return list((activity, tbl_data_prog, tbl_RAP, tbl_link_pa))
# --------------------------------------------------------------------------------------------------------------- #

def tit_count(self, *, tit_number=None):    # TODO: MUST pass self as 1st argument for compatibility.
    """
    Externally defined function to add functionality to a specific GenericActivity object ('weaning', 'castration', etc)
    @param self: Generic Activity object instance (passed to this function via GenericActivity.method_wrapper()).
    @return: None
    """
    if not isinstance(tit_number, int):
        return None
    print(f'Reported number of tits for {self.outerObject}: {tit_number}.')
    return None


if __name__ == "__main__":
    # faulthandler.enable()

    supershort = (363, 368, 372)
    shortList = (1, 4, 5, 11, 18, 27, 32, 130, 172, 210, 244, 280, 363, 368, 372, 92, 120)  # ID=0 no existe.
    # TODO(cmt): OJO! loadItemsFromDB NO genera la lista bovines en el mismo orden en que los IDs estan en shortlist.
    bovines = loadItemsFromDB(Bovine, items=shortList, init_tags=True)  # Abstract Factory funca lindo aqui...
    print(f'ACTIVE UIDs: {[j.ID for j in bovines]}')
    for idx, j in enumerate(bovines):
        print(f'index{idx} : {j}')

    # inventory_obj() is redefined (decorated) as a classmethod whose __call__() method returns cls so as to pass cls
    # as 1st argument to get() method.
    inv_uid = Bovine.inventory_classmethod().get(uid=bovines[5].ID)     # 27 in shortList
    print(f'Inventory_obj call for {bovines[12].recordID}: {inv_uid}\n\n')

    # close_db_writes()  # Flushes all buffers, writes all data to DB and suspends db write operations.
    # exit(0)

    number = Tag.get_input()            # input(f'Tag Number for new tag: ')
    # color = input(f'Tag Color: ')
    # color = color.title()
    colors_dict = getColors()        # {'color_name': (colorID, colorHex), }
    # color = colors_dict[color][0] if color.lower().strip() in [j.lower() for j in colors_dict] else 22
    animalClassID = input(f'Animal Type (Bovine: 1, Caprine: 2, etc.): ')
    animalClassID = int(animalClassID)
    animal_class = next((k for k, v in Animal.getAnimalClasses().items() if v == animalClassID), None)
    if animal_class:
        identifier = Tag.create_identifier(elements=(number, Tag._tagIdentifierChar, animal_class.__name__))
        tag_dict = Tag.identifier_get_user_confirmation(identifier)
        tag_class = animal_class._myTagClass()
        if tag_class is None:
            raise ValueError(f'{moduleName()}-{lineNum()}. ERR_SYS: Assignment error for Tag Class: None value passed.')

        tag_obj = tag_class.alta(**tag_dict)
        tag_obj.inventory.get()
        tag_obj.status.get()
        a = Tag.status_classmethod().get_mem_dataframes()
        b = Tag.get_tags_in_use()
        print(f'created Tag: {tag_obj.getElements}')
        rec = Tag.tag_by_number('RRR33')

        bovines[0].tags.assignTags(comm_type='Comision', tag_list=(tag_obj, ))
        bovines[0].tags.deassignTags(tag_list=(tag_obj,))
        bovines[0].category.set(category='novillo')
        tag_obj.baja()
    # TODO(cmt): Test creating some PA from dictionary data.
    print(f'argv: {argv}')
    print(f'USE_DAYS_MULT: {USE_DAYS_MULT}; DISMISS_PRINT: {DISMISS_PRINT}')


    # inventoryList = (244, 120, 92, 372, 368, 280, 210, 172, 130, 32, 18, 4)
    # # Not creating new progActivities here. Goes to set inventories for selected objects and close ProgActivities.
    # horita = time_mt('datetime')
    # print('                                  ######################## Localizations: ########################')
    # for j in bovines:
    #     if j.recordID in inventoryList:
    #         j.inventory.set(execution_date=horita + timedelta(days=95),
    #                         execute_fields={'age': None, 'localization': None})
    #         j.status.get()
    #         # j.weaning.set()
    #         print(f'                                    ### {j}: {j.localization.get().name}')

    print(f'~~~~~~~~~~~~~~~~~~~ Total number of threads running: {threading.active_count()}')
    close_db_writes()    # Flushes all buffers, writes all data to DB and suspends db write operations.




