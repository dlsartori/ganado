# from krnl_entityObject import EntityObject
import os
from krnl_config import activityEnableFull, lineNum, singleton, time_mt, sessionActiveUser, callerFunction, \
    datetime_from_kwargs, fDateTime
from custom_types import setupArgs, DataTable, getRecords, setRecord, delRecord
from krnl_animal_activity import AnimalActivity
# from krnl_animal import Animal
# from krnl_person import Person
# from krnl_device import Device
# from datetime import datetime


def moduleName():
    return str(os.path.basename(__file__))


class AnimalHealth(AnimalActivity):

    __abstract_class = True     # instructs __init_subclass__ not to include in register.

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


@singleton
class Application(AnimalHealth):
    __tblDataName = 'tblDataAnimalesActividadSanidadAplicaciones'
    __activityName = 'Sanidad - Aplicacion'
    __method_name = 'application'

    def __init__(self, *args, **kwargs):
        isValid = True
        # kwargs['_tblDataName'] = self.__tblDataName
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        super().__init__(isValid, self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    def set(self, *args, **kwargs):
        """
        Sets Sanidad-Aplicacion obj_data for ID_Animal. Writes tblRA,tblLink,tblDataAnimalesActividadSanidadAplicaciones
       @param idActividadRA: ID_Actividad in RA. Si passed,there's no need to write on tblRA or tblLink
       @param args: DataTable objects. Tables tblRA, tblDataAnimalesActividadSanidadAplicaciones
       @param kwargs: dictionary. ONLY for tabla tblDataAnimalesActividadSanidadAplicaciones arguments
       @return:
       """
        tblRA = setupArgs(self.__tblRAName, *args)
        tblLink = setupArgs(self.__tblLinkName, *args)
        tblData = setupArgs(self.__tblDataName, *args, **kwargs)
        tableList = [j for j in args if isinstance(j, DataTable)]   # Lista consolidada de TODAS las tablas modificadas.
        if not tblData.getVal(0, 'fldActiveIngredient'):
            retValue = f'ERR_UI_InvalidArgument - Principio Activo. {moduleName()}({lineNum()})'
            print(f'{retValue}')
            return retValue  # Sale si no se indica Principio Activo.

        if self.isValid and self.outerObject.validateActivity(self.__activityName):
            comment = tblData.getVal(0, 'fldComment')
            comment = (comment + ' ' if comment else '') + f'- Aplicacion: {tblData.getVal(0, "fldActiveIngredient")}'
            tblData.setVal(0, fldComment=f'tagNumber: {self.outerObject.myTagIDs} + {comment}')

            idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)
            if isinstance(idActividadRA, str):
                retValue = idActividadRA
                print(f'{retValue} - {moduleName()}({lineNum()}) / {callerFunction()}')
                return retValue                         # Sale c/error si no pudo escribir DB
            retValue = idActividadRA
            if self._doInventory(**kwargs):
                self.outerObject.inventory.set(*args, **kwargs)

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


