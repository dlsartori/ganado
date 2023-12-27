from krnl_abstract_class_activity import *
from krnl_geo_new import Geo


def moduleName():
    return str(os.path.basename(__file__))


class TagActivity(Activity):     # Abstract Class (no lleva ningun instance attributte). NO se instancia.
    # __abstract_class = True
    _activity_class_register = set()        # Accessed from Activity class.

    def __call__(self, caller_object=None, *args, **kwargs):
        """
        @param caller_object: instance of Bovine, Caprine, etc., that invokes the Activity
        @param args:
        @param kwargs:
        @return: Activity object invoking __call__()
        """
        # item_obj=None above is important to allow to call fget() like that, without having to pass parameters.
        # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {(item_object, *args)}; kwargs: {kwargs}')
        self.outerObject = caller_object  # item_object es instance de Animal, Tag, etc. NO PUEDE SER class por ahora.
        return self

    # Class Attributes: Tablas que son usadas por todas las instancias de InventoryActivityAnimal
    __tblRAName = 'tblCaravanasRegistroDeActividades'
    __tblObjName = 'tblCaravanas'  # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
    __tblLinkName = 'tblLinkCaravanasActividades'

    temp = getRecords('tblCaravanasActividadesNombres', '', '', None, 'fldID', 'fldName', 'fldFlag', 'fldFlagPA')
    __activityID = []
    __activityName = []
    _isInventoryActivity = {}
    _supportsPA = {}
    for j in range(temp.dataLen):
        __activityID.append(temp.dataList[j][0])
        __activityName.append(temp.dataList[j][1])
        _isInventoryActivity[temp.dataList[j][1]] = temp.dataList[j][2]
        if bool(temp.dataList[j][3]):
            _supportsPA[temp.dataList[j][1]] = temp.dataList[j][0]
    _activitiesDict = dict(zip(__activityName, __activityID))  # tagActivities = {__activityName: __activityID, }.

    __tblDataInventoryName = 'tblDataCaravanasInventario'
    __tblDataStatusName = 'tblDataCaravanasStatus'
    __tblObjectsName = 'tblCaravanas'

    def __init__(self, activityName=None, *args, tbl_data_name=None, **kwargs):
        # Agrega tablas especificas de Animales para pasar a Activity.
        activityID = self._activitiesDict.get(activityName)
        invActivity = self._isInventoryActivity.get(activityName)
        enableActivity = kwargs.get('__activity_enable_mode', activityEnableFull)
        isValid = True
        if kwargs.get('supportsPA') is None:
            # Si no hay override desde abajo, setea al valor de __supportsPA{} para ese __activityName.
            kwargs['supportsPA'] = bool(self._supportsPA.get(activityName, False))
        super().__init__(isValid, activityName, activityID, invActivity, enableActivity, self.__tblRAName, *args,
                         tblDataName=tbl_data_name, tblObjectsName=self.__tblObjectsName, **kwargs)

    __classExcludedFieldsClose = {"fldProgrammedDate", "fldWindowLowerLimit", "fldWindowUpperLimit", "fldFK_Secuencia",
                                  "fldDaysToAlert", '"fldDaysToExpire"'}
    __classExcludedFieldsCreate = {"fldPADataCreacion"}

    __activityExcludedFieldsClose = {}  # {activityID: (excluded_fields, ) }
    __activityExcludedFieldsCreate = {}  # {activityID: (excluded_fields, ) }

    @classmethod  # TODO(cmt): Main two methods to access excluded_fields
    def getActivityExcludedFieldsClose(cls, activityName=None):
        return cls.__activityExcludedFieldsClose.get(activityName, set())

    @classmethod
    def getActivityExcludedFieldsCreate(cls, activityName=None):
        return cls.__activityExcludedFieldsCreate.get(activityName, set())

    @classmethod
    def getTblObjectsName(cls):
        return cls.__tblObjectsName

    @property
    def activities(self):
        return self._activitiesDict

    @classmethod
    def getActivitiesDict(cls):
        return cls._activitiesDict

    @classmethod
    def getSupportsPADict(cls):
        return cls.__supportsPA

    __isInventoryActivity = dict(zip(__activityName, _isInventoryActivity))

    @staticmethod
    def getInventoryActivity():
        return TagActivity.__isInventoryActivity

    @property
    def tblDataInventoryName(self):
        return self.__tblDataInventoryName

    @property
    def tblDataStatusName(self):
        return self.__tblDataStatusName

    @property
    def tblObjectsName(self):
        return self.__tblObjectsName

    # Diccionario de Tag.Status
    temp = getRecords('tblCaravanasStatus', '', '', None, 'fldID', 'fldName', 'fldFlag')
    __tagStatusDict = {}  # TODO(cmt) Estructura: {statusName: [statusID, activeYN]}
    for j in range(temp.dataLen):
        __tagStatusDict[str(temp.dataList[j][1])] = [int(temp.dataList[j][0]), int(temp.dataList[j][2])]

    @property
    def statusDict(self):
        return self.__tagStatusDict

    @classmethod
    def getStatusDict(cls):
        return cls.__tagStatusDict

# --------------------------------------------- Fin Class TagActivity ------------------------------------------- #

@singleton
class InventoryActivityTag(TagActivity):
    __tblDataName = 'tblDataCaravanasInventario'
    __activityName = 'Inventario'
    __method_name = 'inventory'
    _short_name = 'invent'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False
        kwargs['decoratorName'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    def set(self, *args: DataTable, **kwargs):  # UI debera generar DataTables para pasar parametros.
        """
        Inserts Inventory obj_data for the object "obj" in the DB.
        __outerAttr: Tag __ID para la que se realiza la Actividad. Lo setea el metodo inventario().
        @param args: list of DataTable objects, with all the tables and fields to be written to DB.
                No checks performed. The function doing the write will discard and ignore all non valid arguments
        @return: 0->ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        retValue, event_date = self._setInventory(*args, **kwargs)
        return retValue


    def get(self, sDate='', eDate='', *args, **kwargs):
        """
        Returns ALL records in table Data Inventario between sValue and eValue. sValue=eValue ='' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param args: DataTables
        @param kwargs: mode='fullRecord' -> Returns last Record
        @param eDate: See @param sDate.
        @param kwargos: mode='value' -> Returns value from DB. If no mode or mode='memory' returns value from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """

        retValue = self._getInventory(sDate, eDate, *args, **kwargs)
        return retValue

@singleton
class StatusActivityTag(TagActivity):
    __tblDataName = 'tblDataCaravanasStatus'
    __permittedDict = {1: [2, 3, 4, 5, 6], 2: [2, 3, 4, 5, 6], 3: [2, 3, 4, 5, 6], 4: [4, 5, 6], 5: [5], 6: [1, 3, 4],
                       7: [1, 2], 'None': [1, 2], None: [1, 2]}
    __activityName = 'Status'
    __method_name = 'status'
    _short_name = 'status'  # Used by Activity._paCreate(), Activity.__isClosingActivity()


    @classmethod
    def permittedFrom(cls):  # Lista de Status permitidos para cada Status de Tag, a partir de status inicial(From)
        return cls.__permittedDict

    def __init__(self, *args, **kwargs):                      # TODO: Cargar argumentos desde DB y validar.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def set(self, *args: DataTable, **kwargs):
        """
        Inserts Status obj_data for the object "obj" in the DB.
        __outerAttr: Tag __ID para la que se realiza la Actividad. Lo setea el metodo inventario().
        @param status: string. Mandatory. Status to set the Object to.
        @param args: list of DataTable objects, with tables and fields to be written to DB with setDBData Function
        @param kwargs: Arguments passed to [Data Animales Actividad Status] table
                No checks performed. The function doing the write will discard and ignore all non valid arguments
                'isProg' = True/False -> Activity is Programmed Activity, or not.
                'recordInventory'=True/False -> Overrides Activity setting of variable _isInventoryActivity
        @return: ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        retValue = self._setStatus(*args, **kwargs)
        if self._doInventory(**kwargs):
            self.outerObject.inventory.set()

        return retValue


    def get(self, sDate='', eDate='',  **kwargs):
        """
        Returns records in table Data Status between sValue and eValue. If sValue = eValue = '' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode=val -> Returns val only from DB. If no mode, returns value from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        retValue = self._getStatus(sDate, eDate, **kwargs)
        return retValue

@singleton
class LocalizationActivityTag(TagActivity):
    __tblDataName = 'tblDataCaravanasLocalizacion'
    __activityName = 'Localizacion'
    __method_name = 'localization'
    _short_name = 'locali'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False
        kwargs['decoratorName'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def set(self, *args: DataTable, localization=None, **kwargs):
        """
        Inserts LocalizationActivityTag obj_data for the object "obj" in the DB.
        __outerAttr: TagObject para la que se realiza la Actividad. Lo setea el metodo inventario().
        @param localization: Geo object | str. Sets Localization in short form (without passing full DataTables)
        @param args: list of DataTable objects, with all the tables and fields to be written to DB.
                No checks performed. The function doing the write will discard and ignore all non valid arguments
        @param kwargs: recordInventory: 0->Do NOT call _setInventory() / 1: Call _setInventory(),
                        idLocalization = valid LocalizationActivityAnimal val, from table GeoEntidades
        @return: 0->ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        kwargs['localization'] = localization
        retValue = self._setLocalization(*args, **kwargs)
        if not isinstance(retValue, str):
            if self._doInventory(**kwargs):
                _ = self.outerObject.inventory.set()
        return retValue


    def get(self, sDate='', eDate='', *args, **kwargs):
        """
       Returns records in table Data LocalizationActivityAnimal between sValue and eValue.
       If sValue = eValue = '' -> Last record
       @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                       sDate='0' or eDate='0': First Record
                       Otherwise: records between sDate and eDate
       @param eDate: See @param sDate.
       @param kwargs: mode='value' -> Returns value from DB. If no mode or mode='memory' returns value from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        retValue = self._getLocalization(sDate, eDate, *args, **kwargs)
        return retValue

@singleton
class CommissionActivityTag(TagActivity):
    # Class Attributes: Tablas que son usadas por todas las instancias de CommissionActivityTag
    __activityName = 'Comision'
    __method_name = 'commission'
    _short_name = 'commis'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    __tblRAName = 'tblCaravanasRegistroDeActividades'
    __tblObjName = 'tblCaravanas'
    __tblLinkName = 'tblLinkCaravanasActividades'
    __tblDataName = 'tblDataCaravanasStatus'
    __tblRA = DataTable(__tblRAName)  # Tabla Registro De Actividades
    __tblObject = DataTable(__tblObjName)  # Tabla "Objeto": tblCaravanas, tblAnimales, etc.
    __tblLink = DataTable(__tblLinkName)  # Sin argumentos: se crean TODOS los campos de la tabla; _dataList=[]
    __tblData = DataTable(__tblDataName)  # Data Inventario, Data AltaActivity, Data Localizacion, etc.

    def __init__(self, *args, **kwargs):
        if not self.__tblRA.isValid or not self.__tblLink.isValid:
            krnl_logger.warning(f'ERR_Sys_CannotCreateObject: Tag.Commission')
            return
        else:
            kwargs['supportsPA'] = False
            kwargs['decoratorName'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    @property
    def isValid(self):
        return self.__isValidFlag


    def set(self, *args: DataTable, **kwargs):
        """
        Tag Commissioning. Inserts records in DB tables. Also executes sub-activity on the tag.
        @param args: list of DataTable objects, with all the tables and fields to be written to DB.
                No checks performed. The function doing the writes will ignore all non-valid arguments.
        @param kwargs:
               tagCommissionType:  Values: 'Comision' (Default), 'Comision - Reemplazo', 'Comision - Reemision'.
        @return: 0->ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        tagStatus = 'Comisionada'
        tagActivity = next((removeAccents(kwargs[j]) for j in kwargs if 'tagcomm' in str(j).lower()), 'Comision')
        tagActivity = tagActivity if tagActivity in TagActivity.getActivitiesDict() else 'Comision'
        if 'emplazo' in tagActivity:
            activityID = TagActivity.getActivitiesDict()['Comision - Reemplazo']
            tagStatus = 'Reemplazada'
        elif 'emision' in tagActivity or 'emission' in tagActivity:
            activityID = TagActivity.getActivitiesDict()['Comision - Reemision']
        else:
            activityID = TagActivity.getActivitiesDict()['Comision']

        tblRA = setupArgs(self.__tblRAName, *args)
        tblLink = setupArgs(self.__tblLinkName, *args)
        # tblData = setupArgs(self.__tblDataName, *args, **kwargs)
        activityID = activityID if tblRA.getVal(0, 'fldFK_NombreActividad') is None \
            else tblRA.getVal(0, 'fldFK_NombreActividad')
        tblRA.setVal(0, fldFK_NombreActividad=activityID)
        idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))
        if isinstance(idActividadRA, str):                   # str: Hubo error de escritura
            retValue = f'ERR_DB_WriteError - {idActividadRA}  - {callerFunction(getCallers=True)}'
            print(f'krnl_tag.py({lineNum()} - {retValue}')
        else:
            tblLink.setVal(0, fldFK_Actividad=idActividadRA, fldFK=self.outerObject.ID)
            retValue = self.outerObject.status.set(tagStatus, tblRA, tblLink, status=tagStatus)
        return retValue


    def get(self, sDate='', eDate=''):
        """
        Returns ALL records in table Data Caravanas RA between sValue and eValue. sValue=eValue ='' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @return: Object DataTable
        """
        retValue = None
        activityList = [TagActivity.getActivitiesDict()['Comision'],
                        TagActivity.getActivitiesDict()['Comision - Reemplazo'],
                        TagActivity.getActivitiesDict()['Comision - Reemision']]
        if self._isValid:
            # qryTable = getRecords(obj.__tblRAName, '', '', None, '*', fldFK_NombreActividad=activityList)
            qryTable = self._getRecordsLinkTables(self.__tblRA, self.__tblLink, None, activity_list=('Comision',
                                                 'Comision - Reemplazo', 'Comision - Reemision'))
            if qryTable.dataLen:
                if qryTable.dataLen <= 1:
                    retValue = qryTable  # qryTable tiene 1 solo registro (Ultimo o Primero)

                else:
                    retTable = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
                    retValue = retTable
                # print(f'TAG({lineNum()}) retTable: {retValue}')
        return retValue

    def unset(self, *args: DataTable, **kwargs):
        """
        Decommissioning of Tags
        @param args: list of DataTable objects, with all the tables and fields to be written to DB.
                No checks performed. The function doing the write will discard and ignore all non valid arguments
        @param kwargs:
               tagStatus: status for decommissioned tag.
               Permitted values: 'Decomisionada' (Default), 'Baja', 'Reemplazada', 'Extraviada'
        @return: 0->ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        activityName = 'Decomision'
        activityID = self.outerObject.getActivitiesDict()[activityName]
        validStatus = ('decomisionada', 'reemplazada', 'perform', 'extraviada')
        tagStatus = 'Decomisionada'
        eventDate = time_mt('datetime')
        tagStatus = next((kwargs[j].strip() for j in kwargs if str(j).lower().__contains__('status')
                              and str(kwargs[j]).strip().lower() in validStatus), tagStatus)
        kwargs['fldFK_Status'] = self.statusDict[tagStatus][0]
        kwargs['fldDate'] = kwargs.get('fldDate', eventDate)
        kwargs['fldComment'] = kwargs.get('fldComment', '') + f'{activityName}. Tag:{self.outerObject.ID}'
        tblRA = setupArgs(self.__tblRAName, *args)
        tblLink = setupArgs(self.__tblLinkName, *args)
        tblData = setupArgs(self.__tblDataName, *args, **kwargs)
        activityID = activityID or tblRA.getVal(0, 'fldFK_NombreActividad')
        tblRA.setVal(0, fldFK_NombreActividad=activityID)
        tblLink.setVal(0, fldFK_Actividad=tblRA.getVal(0, 'fldID'), fldFK=self.outerObject.ID)

        retValue = self._createActivityRA(tblRA, tblLink, tblData, *args, tblDataName=self.__tblDataName, **kwargs)
        if isinstance(retValue, str):                   # str: Hubo error de escritura
            retValue = f'ERR_DB_WriteError - {retValue} - {callerFunction()}'
            print(f'{moduleName()}({lineNum()}) - {retValue}')
        else:
            _ = self.outerObject.status.set(tagStatus, tblRA, tblLink, status=tagStatus)  # Setea status de tag c/ idActividad de RA
        return retValue

