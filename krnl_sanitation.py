# from krnl_entityObject import EntityObject
import os
from krnl_config import activityEnableFull, lineNum, singleton, time_mt, sessionActiveUser, callerFunction, \
    datetime_from_kwargs, fDateTime, krnl_logger, db_logger
from krnl_custom_types import setupArgs, DataTable, getRecords, setRecord, delRecord
from krnl_animal_activity import AnimalActivity
# from krnl_animal import Animal
# from krnl_person import Person
# from krnl_device import Device
# from datetime import datetime


def moduleName():
    return str(os.path.basename(__file__))


class AnimalSanitation(AnimalActivity):

    __abstract_class = True     # instructs __init_subclass__ not to include in register.
    __subclass_register = set()  # Stores Tag subclasses -> {class_obj, }

    # __init_subclass() is used to register dynamically added Sanitation subclasses when definitions in new modules
    # are added to the system.
    # This code executes after the subclasses complete their creation code, WITHOUT any object instantiation. Beleza.
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if getattr(cls, '_' + cls.__name__ + '__add_to_register', None):
            cls.register_class()

    @classmethod
    def register_class(cls):
        cls.__subclass_register.add(cls)

    @classmethod
    def __unregister_class(cls):  # Hidden method(). unregistering a class only managed with proper privileges.
        cls.__subclass_register.discard(cls)

    @classmethod
    def get_sub_classes(cls):
        return cls.__subclass_register.copy()


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


@singleton
class Application(AnimalSanitation):
    __tblDataName = 'tblDataAnimalesActividadSanidadAplicaciones'
    __activityName = 'Sanidad - Aplicacion'
    __method_name = 'application'
    __short_name = 'applic'

    def __init__(self, *args, **kwargs):
        isValid = True
        # kwargs['_tblDataName'] = self.__tblDataName
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        super().__init__(isValid, self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    def set(self, *args, execute_fields=None, excluded_fields=(), **kwargs):
        """
        Sets Sanidad-Aplicacion obj_data for ID_Animal. Writes tblRA,tblLink,tblDataAnimalesActividadSanidadAplicaciones
        @param execute_fields: dict. Data related to the execution of the Activity
        @param excluded_fields: tuple. List of fields to exclude from comparison.
        @param args: DataTable objects. Tables tblRA, tblDataAnimalesActividadSanidadAplicaciones
        @param kwargs: dictionary. ONLY for tabla tblDataAnimalesActividadSanidadAplicaciones arguments
        @return: idActividadRA or ERR_ string.
       """
        tblRA = setupArgs(self.__tblRAName, *args)
        tblLink = setupArgs(self.__tblLinkName, *args)
        tblData = setupArgs(self.__tblDataName, *args, **kwargs)
        tableList = [j for j in args if isinstance(j, DataTable)]   # Lista consolidada de TODAS las tablas modificadas.
        if not tblData.getVal(0, 'fldActiveIngredient'):
            retValue = f'ERR_UI_InvalidArgument - Principio Activo. {moduleName()}({lineNum()})'
            return retValue  # Sale si no se indica Principio Activo.

        if self.isValid and self.outerObject.validateActivity(self._activityName):
            comment = tblData.getVal(0, 'fldComment', '') + f' - Aplicacion: {tblData.getVal(0, "fldActiveIngredient")}'
            tblData.setVal(0, fldComment=f'tagNumber: {self.outerObject.myTagIDs} + {comment}')

            idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)
            if isinstance(idActividadRA, str):
                retValue = idActividadRA
                krnl_logger.info(f'{retValue} - {moduleName()}({lineNum()}) / {callerFunction()}')
                return retValue                         # Sale c/error si no pudo escribir DB
            retValue = idActividadRA
            if self.doInventory(**kwargs):
                self.outerObject.inventory.set(*args, **kwargs)

            if isinstance(retValue, int) and self._supportsPA:
                excluded_fields = set(excluded_fields) if isinstance(excluded_fields, (list,set,tuple,dict)) else set()
                execute_date = self.outerObject._lastStatus[1]  # Gets the internal execution date for lastInventory.
                if isinstance(retValue, int) and self._supportsPA:
                    executeFields = self.activityExecuteFields(execution_date=execute_date,
                                                               status=self.outerObject.lastStatus)
                    if execute_fields and isinstance(execute_fields, dict):
                        executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
                    self._paMatchAndClose(retValue, execute_fields=executeFields, excluded_fields=excluded_fields,
                                          **kwargs)  # TODO(cmt): This call is executed asynchronously by another thread
                    # Updates cols in tblLink, so that external nodes can access Execute Data, Excluded Fields.
                    fldID_Link = tblLink.getVal(0, 'fldID')  # tblLink argument is updated by
                    if fldID_Link:
                        setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
                                  fldExcludedFields=excluded_fields)




            # Verificacion y cierre de Actividad Programada (AP) si esta se conforma en una actividad de cierre valida.
            executionParams = self._packActivityParams(*tableList)
            if executionParams:
                # 1. Toma AP que tengan el mismo nombre a la ejecutada.
                eventDate = tblData.getVal(0, 'fldDate')  # Toma estos 2 valores retornados de _createActivityRA()
                idActivity = tblRA.getVal(0, 'fldFK_NombreActividad')
                if hasattr(self.outerObject, 'animalClassID'):
                    animalClassID = self.outerObject.animalClassID
                    temp = getRecords(self.__tblRAPName, '', '', None, '*', fldFK_NombreActividad=idActivity,
                                      fldFK_ClaseDeAnimal=animalClassID)
                else:  # Si no es Animal, falla try de arriba-> filtra por NombreActividad
                    temp = getRecords(self.__tblRAPName, '', '', None, '*', fldFK_NombreActividad=idActivity)
                if temp.dataLen:
                    dataProgIDList = temp.getCol('fldFK_DataProgramacion')  # Lista con  ID Data Programacion
                    apListToClose = []    # Lista de ID_Actividad a ser cerradas
                    temp1 = getRecords(self.__tblProgActivitiesName, '', '', None, '*', fldID=dataProgIDList)
                    if temp1.dataLen:
                        paDataIndex = temp1.getFldIndex('fldPAData')
                        lowLimIndex = [temp1.getFldIndex('fldWindowLowerLimit')]
                        uppLimIndex = [temp1.getFldIndex('fldWindowUpperLimit')]
                        for j in temp1.dataList:
                            progDate = None     # Viene de tabla tblLinkPA, campo 'fldDateProgrammed'
                            lowerLim = j[lowLimIndex]
                            upperLim = j[uppLimIndex]
                            if not lowerLim <= eventDate <= upperLim:
                                continue   # No cumple condicion de fecha de cierre, va a la proxima AP de la lista
                            # Aqui compara diccionarios Data Actividad Programada con executionParams.
                            dataProgDict = j[paDataIndex]
                            matchResult = self.match(executionParams, dataProgDict, compare=('fldActiveIngredient', ))
                            if matchResult:
                                apListToClose.append(temp.getVal(j, 'fldID'))
                        for i in apListToClose:
                            pass
                            # 1. Update Closure in __tblLinkAP
                            # 2. Update Closure in __tblDataAPStatus

        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - Application.set()'
            print(f'{moduleName()}({lineNum()}  - {retValue}')

        return retValue

    def get(self):
        pass

    def prog(self):
        pass


