from krnl_tag_animal import *
from krnl_config import os, lineNum, callerFunction, sessionActiveUser
from os import path
from krnl_custom_types import DataTable,setRecord, delRecord
from random import randrange


def moduleName():
    return str(path.basename(__file__))


class TagCaprine(TagAnimal):
    _kindOfAnimalID = 2  # TODO(cmt): 1:'Vacuno', 2:'Caprino', 3:'Ovino', 4:'Porcino', 5:'Equino'.

    # Duplication indices dict in every subclass to make searches faster.
    _active_uids_dict = {}  # {fldObjectUID: fld_Duplication_Index}  --> fld_Duplication_Index IS an object UID.
    _active_duplication_index_dict = {}  # {fld_Duplication_Index: set(fldObjectUID, dupl_uid1, dupl_uid2, ), }

    def __init__(self, **kwargs):
        # if kwargs.get('fldID') in self.__registerDict:
        #     kwargs['repeatID'] = True                   # Signals tag ID already exists to constructor in Tag.
        super().__init__(**kwargs)



    # __registerDict = {}  # {tagID: tagObject} Registro de Tags, para evitar duplicacion.
    #
    # @classmethod
    # def getRegisterDict(cls):
    #     return cls.__registerDict
    #
    # @classmethod
    # def register(cls, obj):  # NO HAY CHEQUEOS. obj debe ser valido
    #     try:
    #         cls.__registerDict[obj.ID] = obj  # TODO-Design criteria: Si se repite key, sobreescribe con nuevo valor
    #     except (NameError, KeyError, IndexError, ValueError, TypeError):
    #         raise KeyError(f'ERR_INP_KeyError: {moduleName()}({lineNum()}) - {callerFunction()}')
    #     return obj
    #
    # @classmethod
    # def unRegister(cls, obj):  # Remueve obj .__registerDict
    #     """
    #     Removes object from .__registerDict
    #     @param obj: object to remove/pop
    #     @return: removed object if successful. False if fails.
    #     """
    #     retValue = cls.__registerDict.pop(obj.getID, False)  # Retorna False si no encuentra el objeto
    #     return retValue
    #
    # @property
    # def isRegistered(self):
    #     retValue = True if self.getID in self.__registerDict else False
    #     return retValue

    @classmethod
    def generateTag(cls, **kwargs):
        """
        Generates a new tag. Used only for testing purposes.
        @param kwargs: tagnumber-> Tag Number. if not provided, a unique tag number is generated.
                       technology -> Standard, RFID, etc.
                       marks -> Number of marks (Default = 1)
                       tagType, tagColor, tagFormat
                       tagStatusDict -> Tag Status. Default=1 (Alta)
                       writeDB=True -> writes all required obj_data to DB tables (Caravanas, Caravanas Status, etc)
        @return: Tag Object
        """
        # fldIDTag = getMaxId('tblCaravanas') + 1
        tagNumber = str(next((j for j in kwargs if str(j).lower().__contains__('tagnumber')),
                             randrange(10000, 99999))) + '-RND'
        aux = 0
        while tagNumber in cls.getRegisterDict() and aux < 9990:
            tagNumber = str(randrange(10000, 99999)) + '-RND'
            aux += 1
        if aux >= 9990:
            retValue = f'ERR_Sys: Cannot assign Tag Number. Tag not created.'
            print(f'{retValue}')
            return retValue

        __tblRAName = 'tblCaravanasRegistroDeActividades'
        __tblObjName = 'tblCaravanas'  # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
        __tblLinkName = 'tblLinkCaravanasActividades'
        __tblStatusName = 'tblDataCaravanasStatus'
        tblRA = DataTable(__tblRAName)  # Tabla Registro De Actividades
        tblObject = DataTable(__tblObjName)  # Tabla "Objeto": tblCaravanas, tblAnimales, etc.
        tblLink = DataTable(__tblLinkName)  # Sin argumentos: se crean TODOS los campos de la tabla; _dataList=[]
        tblStatus = DataTable(__tblStatusName)

        technology = next((j for j in kwargs if str(j).lower().__contains__('techno')), 'Standard')
        marks = next((j for j in kwargs if str(j).lower().__contains__('mark')), 1)
        tagColor = next((j for j in kwargs if str(j).lower().__contains__('color')), 'Blanco')
        tagType = next((j for j in kwargs if str(j).lower().__contains__('tagtype')), 'General')
        tagFormat = next((j for j in kwargs if str(j).lower().__contains__('tagformat')), 'Tarjeta')
        tagStatus = next((j for j in kwargs if str(j).lower().__contains__('status')), 'Alta')
        tagStatus = tagStatus if tagStatus in cls.getStatusDict() else 'Alta'
        new_tag = TagCaprine(fldID=0, fldTagNumber=tagNumber, fldFK_TagTechnology=technology,
                            fldFK_Color=tagColor, fldFK_TagType=tagType, fldTagMarkQuantity=marks,
                            fldFK_TagFormat=tagFormat, fldFK_UserID=sessionActiveUser)
        idTag = setRecord('tblCaravanas', **new_tag.getElements)
        new_tag.ID = idTag  # Actualiza Tag ID con valor obtenido de setRecord()
        cls.register(new_tag)  # TODO: SIEMPRE registrar Tag (al igual que Animal). __init__() NO registra los objetos.
        print(f'TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT Caravana Nueva: {type(new_tag)} / {new_tag.getElements} ')

        tblRA.setVal(0, fldFK_NombreActividad=cls.getActivitiesDict()['Alta'], fldFK_UserID=sessionActiveUser)
        idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))  # Crea registro en tblRA
        tblRA.setVal(0, fldID=idActividadRA, fldComment=f'Generated Tag. ID: {new_tag.ID} / {new_tag.tagNumber}')
        tblLink.setVal(0,
                       fldComment=f'{callerFunction()}. Tag ID: {new_tag.ID} / {new_tag.tagNumber}')  # fldFK_Actividad=idActividadRA,
        # _ = setRecord(tblLink.tblName, **tblLink.unpackItem(0))

        new_tag.status.set(tblRA, tblLink, tblStatus, status=tagStatus)
        new_tag.localization.set(tblRA, tblLink, idLocalization=545)

        retValue = new_tag

        return retValue