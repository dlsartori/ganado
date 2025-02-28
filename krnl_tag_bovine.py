from krnl_tag_animal import *
from krnl_config import os, lineNum, callerFunction, sessionActiveUser
from krnl_custom_types import DataTable, setRecord, delRecord
from uuid import uuid4

def moduleName():
    return str(os.path.basename(__file__))


class TagBovine(TagAnimal):
    """                         TODO: DEPRECATED. NOW USING class factory func _create_tag_subclass()
    Implements class Tag for specific Animal Class (Bovine, Porcine, etc).
    For now, __registerDict and its associated methods are moved here (Aug-22)
    """
    _kindOfObjectID = 1  # TODO(cmt): 1:'Vacuno', 2:'Caprino', 3:'Ovino', 4:'Porcino', 5:'Equino'.
    # Duplication indices dict in every subclass to make searches faster.
    # Updated by _init_uid_dict() method, defined in Tag.
    _active_uids_dict = {}  # {fldObjectUID: fld_Duplication_Index}  --> fld_Duplication_Index IS an object UID.
    _duplic_index_checksum = 0   # sum of _active_uids_df.values() to detect changes and update _active_uids_df.
    _active_duplication_index_dict = {}  # {fld_Duplication_Index: (fldObjectUID, dupl_uid1, dupl_uid2, ), }
    __identfiers_dict = {}   # {fldObjectUID: fldIdentificadores} -> dict to access identifiers without reading from DB.

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

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
        # tagNumber = str(next((j for j in kwargs if 'tagnumber' in str(j).lower()), randrange(10000, 99999))) + '-RND'
        tag_uid = uuid4()
        tagNumber = 'RND-' + str(tag_uid)
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

        new_tag = cls(fldID=0, fldTagNumber=tagNumber, fldFK_TagTechnology=technology, fldObjectUID=tag_uid,
                      fldFK_Color=tagColor, fldFK_TagType=tagType, fldTagMarkQuantity=marks,
                      fldFK_TagFormat=tagFormat, fldFK_UserID=sessionActiveUser, fldAssignedToClass='Bovine',
                      fldAssignedToUID=tag_uid)
        idTag = setRecord('tblCaravanas', **new_tag.getElements)
        cls.register(new_tag)
        print(f'TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT Caravana Nueva: {type(new_tag)} / {new_tag.getElements} ')

        tblRA.setVal(0, fldFK_NombreActividad=cls.getActivitiesDict()['Alta'], fldFK_UserID=sessionActiveUser)
        idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))  # Crea registro en tblRA
        tblRA.setVal(0, fldID=idActividadRA, fldComment=f'Generated Tag. ID: {new_tag.ID} / {new_tag.tagNumber}')
        tblLink.setVal(0, fldComment=f'{callerFunction()}. Tag ID: {new_tag.ID} / {new_tag.tagNumber}',
                       fldFK_Actividad=idActividadRA)
        new_tag.status.set(tblRA, tblLink, tblStatus, status=tagStatus)
        new_tag.localization.set(tblRA, tblLink, idLocalization=545)
        retValue = new_tag
        return retValue


