from krnl_tag_animal import *
from krnl_config import os, lineNum, callerFunction, sessionActiveUser
from custom_types import DataTable, setRecord, delRecord
from random import randrange


def moduleName():
    return str(os.path.basename(__file__))


class TagBovine(TagAnimal):
    """
    Implements class Tag for specific Animal Class (Bovine, Porcine, etc).
    For now, __registerDict and its associated methods are moved here (Aug-22)
    """

    def __init__(self, **kwargs):
        if kwargs.get('fldID') in self.__registerDict:
            kwargs['repeatID'] = True       #       Signals repeat Tag ID to avoid overwriting.
        super().__init__(**kwargs)

    __registerDict = {}  # {tagID: tagObject} Registro de Tags, para evitar duplicacion.

    @classmethod
    def getRegisterDict(cls):
        return cls.__registerDict

    def register(self):  # NO HAY CHEQUEOS. obj debe ser valido
        try:
            self.__registerDict[self.ID] = self  # TODO-Design criteria: Si se repite key, sobreescribe con nuevo valor
        except (NameError, KeyError, IndexError, ValueError, TypeError):
            raise KeyError(f'ERR_INP_KeyError: {moduleName()}({lineNum()}) - {callerFunction()}')
        return self

    # obj es Objeto de tipo Tag. NO HAY CHEQUEOS. obj debe ser valido
    def unRegister(self):  # Remueve obj .__registerDict
        """
        Removes object from .__registerDict
        @param obj: object to remove/pop
        @return: removed object if successful. False if fails.
        """
        retValue = self.__registerDict.pop(self.getID, False)  # Retorna False si no encuentra el objeto
        return retValue

    @property
    def isRegistered(self):
        retValue = True if self.getID in self.__registerDict else False
        return retValue

    @classmethod
    def generateTag(cls, **kwargs):  # TODO: Copia de este metodo va en TODAS las subclases TagCaprine, TagPorcine, etc.
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
        # tblObject = DataTable(__tblObjectsName)  # Tabla "Objeto": tblCaravanas, tblAnimales, etc.
        tblLink = DataTable(__tblLinkName)  # Sin argumentos: se crean TODOS los campos de la tabla; _dataList=[]
        tblStatus = DataTable(__tblStatusName)

        technology = next((j for j in kwargs if str(j).lower().__contains__('techno')), 'Standard')
        marks = next((j for j in kwargs if str(j).lower().__contains__('mark')), 1)
        tagColor = next((j for j in kwargs if str(j).lower().__contains__('color')), 'Blanco')
        tagType = next((j for j in kwargs if str(j).lower().__contains__('tagtype')), 'General')
        tagFormat = next((j for j in kwargs if str(j).lower().__contains__('tagformat')), 'Tarjeta')
        tagStatus = next((j for j in kwargs if str(j).lower().__contains__('status')), 'Alta')
        tagStatus = tagStatus if tagStatus in cls.getStatusDict() else 'Alta'
        new_tag = Tag(fldID=0, fldTagNumber=tagNumber, fldFK_TecnologiaDeCaravana=technology,
                      fldFK_Color=tagColor, fldFK_TipoDeCaravana=tagType, fldTagMarkQuantity=marks,
                      fldFK_FormatoDeCaravana=tagFormat, fldFK_UserID=sessionActiveUser)
        idTag = setRecord('tblCaravanas', **new_tag.getElements)
        new_tag.ID = idTag  # Actualiza Tag ID con valor obtenido de setRecord()
        cls.register(new_tag)  # TODO: SIEMPRE registrar Tag (al igual que Animal). __init__() NO registra los objetos.
        print(f'TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT Caravana Nueva: {type(new_tag)} / {new_tag.getElements} ')

        tblRA.setVal(0, fldFK_NombreActividad=cls.getActivitiesDict()['Alta'], fldFK_UserID=sessionActiveUser)
        idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))  # Crea registro en tblRA
        tblRA.setVal(0, fldID=idActividadRA, fldComment=f'Generated Tag. ID: {new_tag.ID} / {new_tag.tagNumber}')
        tblLink.setVal(0, fldComment=f'{callerFunction()}. Tag ID: {new_tag.ID} / {new_tag.tagNumber}')
        # fldFK_Actividad=idActividadRA
        new_tag.status.set(tblRA, tblLink, tblStatus, status=tagStatus)
        new_tag.localization.set(tblRA, tblLink, idLocalization=545)
        retValue = new_tag
        return retValue


