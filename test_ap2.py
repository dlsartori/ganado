# import faulthandler
import threading
from krnl_config import *
from datetime import timedelta
from krnl_custom_types import DataTable, getRecords, setRecord, close_db_writes, dbRead
# from krnl_db_access import writeObj
from krnl_object_instantiation import loadItemsFromDB
from krnl_bovine import Bovine
from krnl_abstract_class_animal import Animal       # Pa' ejecutar paCreateActivity()
from krnl_abstract_class_activity import Activity
from krnl_tag_activity import TagActivity
from krnl_animal_activity import AnimalActivity, InventoryActivityAnimal, ProgActivityAnimal, GenericActivityAnimal, StatusActivityAnimal
from krnl_geo import Geo
# from threading import current_thread

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
    timeStamp = time_mt("dt")
    userID = sessionActiveUser
    tblDataProg = DataTable("tblDataProgramacionDeActividades")
    tblTriggers = DataTable("tblAnimalesActividadesProgramadasTriggers")
    tblLinkPA = DataTable("tblLinkAnimalesActividadesProgramadas")
    tblRAP = DataTable("tblAnimalesRegistroDeActividadesProgramadas")

    print(f'\nAnimal Activities defined: {[j.__name__ for j in AnimalActivity.get_class_register()]}')
    print(f'Tag Activities defined: {[j.__name__ for j in TagActivity.get_class_register()]}\n')
    supershort = (363, 368, 372)
    shortList = (1, 4, 5, 11, 18, 27, 32, 130, 172, 210, 244, 280, 363, 368, 372, 92, 120)  # ID=0 no existe.
    # TODO(cmt): OJO! loadItemsFromDB NO genera la lista bovines en el mismo orden en que los IDs estan en shortlist.
    bovines = loadItemsFromDB(Bovine, items=shortList, init_tags=True)  # Abstract Factory funca lindo aqui...
    print(f'ACTIVE UIDs: {[j.ID for j in bovines]}')
    for idx, j in enumerate(bovines):
        print(f'index{idx} : {j}')

    # Items with inventory date != 0 and not present in bovines.
    not_found = {'22ff143d684c4848843f4d2fe82992f4', '6808e3d6169d40fdbb2a1187e4ba4297', '0d9b48c92d7f4328b87a19fef2f213d7', '4308cbf21f744864bc5276223da37fcc'}
    not_found_fldID = [41, 8, 61, 398]
    # Check measurement() property/method.
    # bovines[0].measurement.set(meas_name='peso', meas_value=230, meas_units='kg')
    # bovines[0].measurement.get(meas_name='PESO')
    # exit(0)

    # Checks registration and execution of externally-defined functions that are added to the GenericActivity objects.
    Animal.GenActivity_register_func(property_name='weaning', func_object=tit_count)
    bovines[0].weaning.tit_count(tit_number=15)
    # bovines[0].inventory._pop_outerAttr_key(current_thread().ident)
    bovines[1].weaning.set(val=time_mt('dt'))  # Usa val porque es GenericActivity.
    bovines[1].weaning.get()
    inv0 = bovines[0].inventory.get()
    print(f'Inventory for {bovines[0].recordID}: {inv0}')
    # inventory_obj is an Instance of InventoryActivity class that implements __call__() to access outerObject.
    bovines[12].inventory.set(date=datetime.now() + timedelta(days=120))

    # inventory_obj() is a classmethod whose __call__() method returns cls, so as to pass cls as 1st argument to get().
    inv_uid = Bovine.inventory_classmethod().get(uid=bovines[12].ID)
    print(f'Inventory_obj call for {bovines[12].recordID}: {inv_uid}')

    bovines[1].status.set(status=3)
    bovines[1].tact.set(val=1)      # Esta esta funcando bien (10-Jan-24). Usa val porque es GenericActivity.
    a = bovines[1].tact.get()       # Esta parece que tambien.

    categ_problem_objs = (37, 68, 101, 172, 184, 191)
    tbl_problems = dbRead('tblAnimales', f'SELECT * from "Animales" WHERE "ID_Animal" IN {str(categ_problem_objs)}; ')
    problem_uids = tbl_problems.getCol('fldObjectUID')
    for uid in problem_uids:
        obj = Bovine.getObject(uid)
        if 'm' in obj.mf:
            obj.category.set(category=6)


    # close_db_writes()  # Flushes all buffers, writes all data to DB and suspends db write operations.
    # exit(0)


    # TODO(cmt): Test creating some PA from dictionary data.
    create_pa = parse_cmd_line('pa_create') or False
    print(f'argv: {argv}')
    print(f'USE_DAYS_MULT: {USE_DAYS_MULT}; DISMISS_PRINT: {DISMISS_PRINT}; create_pa: {create_pa}')
    inventoryList = (244, 120, 92, 372, 368, 280, 210, 172, 130, 32, 18, 4)

    geo_obj = next((g for g in Geo.getObject("El Ñandú") if removeAccents(g.name) in removeAccents("El Ñandú")), None)
    if create_pa:     # [180, None] checks _comp_val >= 180.
        paDict = {"fldMF": "m", "fldComment": "Test Inventario- El Ñandú", "age": [180, None],
                  "fldFK_Localizacion": geo_obj.ID}     # "42990b601cec4ddb9d85bfb94cda2e29"
        # Generates data set for items with recordID event (excludes odds)
        paDataset = paSetDataInventory(prog_date=time_mt('date_time') + timedelta(days=60), tbl_RAP=tblRAP,
                                       items=[j.ID for j in bovines if j. recordID in shortList and not j.recordID & 1],
                                        tbl_data_prog=tblDataProg, tbl_link_pa=tblLinkPA, pa_dict=paDict)
        # progDate = time_mt('datetime') + timedelta(days=60)
        # items_dict = {j: progDate for j in bovines if j.recordID in inventoryList}   fldFK_Localizacion
        paInventory = Bovine.paCreateActivity(paDataset.pop(0), *paDataset)
    else:
        # Not creating new progActivities here. Goes to set inventories for selected objects and close ProgActivities.
        horita = time_mt('datetime')
        print('                                  ######################## Localizations: ########################')
        for j in bovines:
            if j.recordID in inventoryList:
                j.inventory.set(execution_date=horita + timedelta(days=95),
                                execute_fields={'age': None, 'localization': None})
                j.status.get()
                # j.weaning.set()
                print(f'                                    ### {j}: {j.localization.get().name}')

    print(f'~~~~~~~~~~~~~~~~~~~ Total number of threads running: {threading.active_count()}')
    close_db_writes()    # Flushes all buffers, writes all data to DB and suspends db write operations.




# ----------------------------------------------------------------------------------------------------------------- #

# aplicacionSanidadDict = {
#                         'fldID': None,  # used to pull data from table [Data Animales Sanidad Aplicaciones Parametros]
#                         'fldFK_Actividad': None,
#                         'fldFK_FormaDeSanidad': None,
#                         'fldDate': None,
#                         'fldComment': None,
#                         }
#
# aplicacionSanidadParams = {
#                             'fldActiveIngredient': None,            # Principio Activo
#                             'fldName': None,                        # Product name
#                             'fldConcentration': None,               # Concentration in %
#                             'fldQuantity': None,                        # Amount of product applied in Units
#                             'fldFK_Unidad': None,                   # units for fldQuantity (gr, cc, etc)
#                             'fldComment': None,
#                            }
#


# # Castracion
# fldPAData1 = {"tblAnimales__fldMF": "M", "tblAnimales__fldFlagCastrado": 0,
#               "maxAgeDays": 365, "minAgeDays": 210,
#               # "tblDataProgramacionDeActividades__fldFK_NombreActividad": 4,
#               "tblDataAnimalesActividadSanidadAplicaciones__fldComment": "TEST: Data Programacion CASTRACION -El Ñandú",
#               # "tblDataAnimalesActividadLocalizacion__fldFK_Localizacion": 545,
#               # "tblAnimales__fldFK_ClaseDeAnimal": 1
#               }
#
# # Brucelosis
# fldPAData2 = {"maxAgeDays": 270, "minAgeDays": 90, "tblAnimales__fldMF": "F",
#               "tblDataAnimalesActividadSanidadAplicaciones__fldActiveIngredient": "BrumicinaTruch",
#               "tblDataAnimalesActividadSanidadAplicaciones__fldComment": "TEST: Data Programacion BRUCELOSIS -El Ñandú",
#               "tblDataAnimalesActividadSanidadAplicaciones__fldFK_FormaDeSanidad": 1
#               # "tblDataProgramacionDeActividades__fldFK_NombreActividad": 40,
#               # "tblDataAnimalesActividadLocalizacion__fldFK_Localizacion": 545,
#               # "tblAnimales__fldFK_ClaseDeAnimal": 1
#               }
#
# # Tristeza
# fldPAData3 = {"minAgeDays": 180,
#               "tblDataAnimalesActividadSanidadAplicaciones__fldActiveIngredient": "Ivermectina",
#               "tblDataAnimalesActividadSanidadAplicaciones__fldFK_FormaDeSanidad": 1,
#               "tblDataAnimalesActividadSanidadAplicaciones__fldComment": "TEST: Data Programacion TRISTEZA -El Ñandú",
#               # "tblDataAnimalesActividadLocalizacion__fldFK_Localizacion": 545,
#               # "tblAnimales__fldFK_ClaseDeAnimal": 1
#               }
#
# # Destete
# fldPAData4 = {"maxAgeDays": 720, "minAgeDays": 210,
#               "tblDataAnimalesActividadDestete__fldComment": "TEST: Data Programacion DESTETE - El Ñandú",
#               # "tblDataAnimalesActividadSanidadAplicaciones__fldFK_NombreActividad": 6,
#               # "tblDataAnimalesActividadLocalizacion__fldFK_Localizacion": 545,
#               # "tblAnimales__fldFK_ClaseDeAnimal": 1
#               }
#
#

    # for i in range(30):
    #     print(i % 10, end=", ")         # Lacito para generar acceso circular a lista [0 a 9]

