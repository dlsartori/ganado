# from __future__ import annotations
import pandas as pd
from krnl_abstract_class_activity import *
from krnl_geo import Geo
import collections
from uuid import uuid4

def moduleName():
    return str(os.path.basename(__file__))


class TagActivity(Activity):     # Abstract Class (no lleva ningun instance attributte). NO se instancia.
    _activity_class_register = {}        # Accessed from Activity class.
    _class_supports_pa = False          # None of the derived Activity classes support ProgActivities.
    # Lists all Activity classes that support memory data, for access and initialization.
    _memory_data_classes = set()  # Initialized on creation of Activity classes. Defined here to include all Activities.


    def __call__(self, caller_object=None):
        """
        @param caller_object: instance of Bovine, Caprine, etc., that invokes the Activity
        @return: Activity object.
        """
        # caller_object=None above is important to allow to call fget() like that, without having to pass parameters.
        # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {(caller_object, *args)}; kwargs: {kwargs}')
        self.outerObject = caller_object  # item_object es instance de Animal, Tag, etc. NO PUEDE SER class por ahora.
        return self

    # sql0 = 'SELECT "Link Animales Actividades"."ID_Animal", MAX("Data Animales Actividad Inventario"."Fecha Evento") ' \
    #        'AS "fldDate", "Data Animales Actividad Inventario"."ID_Actividad" FROM "Link Animales Actividades" ' \
    #        'INNER JOIN "Data Animales Actividad Inventario" ON "Link Animales Actividades"."ID_Actividad" == ' \
    #        '"Data Animales Actividad Inventario"."ID_Actividad" GROUP BY "Link Animales Actividades"."ID_Animal" ' \
    #        'HAVING "ID_Animal" IN ("c18112cadc6f48f6b873367df13b869b", "98239ad28b5a46bead4d1eb6316c2054", ' \
    #        '"223f76a1f92b47c38c2003f74d522799"); '


    # Class Attributes: Tablas que son usadas por todas las instancias de InventoryActivityAnimal
    _tblRAName = 'tblCaravanasRegistroDeActividades'
    _tblObjName = 'tblCaravanas'  # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
    _tblLinkName = 'tblLinkCaravanasActividades'

    tempdf = getrecords('tblCaravanasActividadesNombres', 'fldID', 'fldName', 'fldFlag', 'fldFlagPA')
    __activityID = []
    __activityName = []
    __inventoryActivitiesDict = {}
    _supportsPADict = {}
    for j in tempdf.index:
        __activityID.append(tempdf.loc[j, 'fldID'])
        __activityName.append(tempdf.loc[j, 'fldName'])
        __inventoryActivitiesDict[tempdf.loc[j, 'fldName']] = tempdf.loc[j, 'fldFlag']
        if bool(tempdf.loc[j, 'fldFlagPA']):
            _supportsPADict[tempdf.loc[j, 'fldName']] = tempdf.loc[j, 'fldID']
    _activitiesDict = dict(zip(tempdf['fldName'], tempdf['fldID']))  # tagActivities = {__activityName: __activityID, }.
    __inventoryActivityDict = dict(zip(tempdf['fldName'], __inventoryActivitiesDict))
    del tempdf


    __tblDataInventoryName = 'tblDataCaravanasInventario'
    __tblDataStatusName = 'tblDataCaravanasStatus'
    __tblObjectsName = 'tblCaravanas'


    def __init__(self, activityName=None, *args, tbl_data_name=None, **kwargs):
        # Agrega tablas especificas de Animales para pasar a Activity.
        activityID = self._activitiesDict.get(activityName)
        invActivity = self.__inventoryActivitiesDict.get(activityName)
        enableActivity = kwargs.get('__activity_enable_mode', activityEnableFull)
        isValid = True
        if kwargs.get('supportsPA') is None:
            # Si no hay override desde abajo, setea al valor de _supportsPADict{} para ese __activityName.
            kwargs['supportsPA'] = bool(self._supportsPADict.get(activityName, False))
        super().__init__(isValid, activityName, activityID, invActivity, enableActivity, self._tblRAName, *args,
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
    # tblObjectsName = getTblObjectsName

    @classmethod
    def getTblRAName(cls):
        return cls._tblRAName
    # tblRAName = getTblRAName

    @classmethod
    def getTblLinkName(cls):
        return cls._tblLinkName
    # tblLinkName = getTblLinkName

    @property
    def activities(self):
        return self._activitiesDict

    @classmethod
    def getActivitiesDict(cls):
        return cls._activitiesDict

    @classmethod
    def getSupportsPADict(cls):
        return cls._supportsPADict


    @staticmethod
    def getInventoryActivityDict():
        return TagActivity.__inventoryActivityDict

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
    Status = collections.namedtuple("Status", ['fldid', 'active'])
    temp = getrecords('tblCaravanasStatus', 'fldID', 'fldName', 'fldFlag')      # {statusName: [statusID, activeYN]}
    records_lst = []
    for r in zip(temp['fldID'], temp['fldFlag']):
        records_lst.append(Status(*r))
    __tagStatusDict = dict(zip(temp['fldName'], records_lst))       # {fldName: (status.fldid, status.active), }
    del temp

    @property
    def statusDict(self):
        return self.__tagStatusDict

    @classmethod
    def getStatusDict(cls):
        return cls.__tagStatusDict      # {statusName: (statusID, Active(1/0), )}

# --------------------------------------------- End Class TagActivity ------------------------------------------- #



@singleton
class InventoryActivityTag(TagActivity):
    __tblDataName = 'tblDataCaravanasInventario'
    __activityName = 'Inventario'
    __method_name = 'inventory'
    _short_name = 'invent'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getTblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

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
        retValue = self._setInventory(*args, **kwargs)
        return retValue


    def get(self, *args, **kwargs):
        """
        Returns ALL records in table Data Inventario between sValue and eValue. sValue=eValue ='' -> Last record
        @param args: DataFrames
        @param kwargs: full_record=True -> Returns last Record in full
        @param kwargs: mode='value' -> Returns value from DB. If no mode or mode='memory' returns value from Memory.
        @return: Object DataFrame with information from queried table or statusID (int) if mode=val
        """

        retValue = self._getInventory(*args, **kwargs)
        return retValue

@singleton
class StatusActivityTag(TagActivity):
    __tblDataName = 'tblDataCaravanasStatus'
    __permittedDict = {1: [2, 3, 4, 5, 6, None], 2: [1, 3, 4, 5, 6], 3: [2, 3, 4, 5, 6], 4: [4, 5, 6, None],
                       5: [5], 6: [1, 3, 4], 'None': [1, 2], None: [1, 2]}
    __activityName = 'Status'
    __method_name = 'status'
    _short_name = 'status'  # Used by Activity._paCreate(), Activity.__isClosingActivity()
    _slock_activity_mem_data = AccessSerializer()  # Used to manage concurrent access to memory, per-Activity basis.
    # Local to StatusActivityTag. Initialized in EntityObject. Must be dunder because class must be single-out.
    # __local_active_uids_dict --> Activity.__init_subclass__() creates dict of classes that implement mem_data
    # __local_active_uids_dict - {cls: mem_dataframe, } with cls = Bovine, Caprine, etc or TagBovine, TagCaprine, etc.
    __local_active_uids_dict = {}  # {uid: MemoryData fields, }. Local to InventoryActivity. Initialized in EntityObject
    _mem_data_params = {'field_names': ('fldFK_Status', 'fldDate', 'fldFK_Actividad')}

    @classmethod
    def get_local_uids_dict(cls):
        return cls.__local_active_uids_dict


    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    @classmethod
    def permittedFrom(cls):  # Lista de Status permitidos para cada Status de Tag, a partir de status inicial(From)
        return cls.__permittedDict

    def __init__(self, *args, **kwargs):                      # TODO: Cargar argumentos desde DB y validar.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def set(self, *args: pd.DataFrame, **kwargs):
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
        return retValue


    def get(self,  **kwargs):
        """
        Returns records in table Data Status between sValue and eValue. If sValue = eValue = '' -> Last record
        @param kwargs: mode=val -> Returns val only from DB. If no mode, returns value from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        retValue = self._getStatus(**kwargs)
        return retValue


@singleton
class AltaActivityTag(TagActivity):
    """Implements perform() method to create new Tags in the system.
    The call must be made by classes TagBovine, TagCaprine, etc.
    Callable class: invoking its instances with () automatically executes __call__() defined here.
    """
    __tblDataName = 'tblDataCaravanasAltaBaja'
    __activityName = 'Alta'
    __method_name = 'alta'  # Used in creatorActivity to create the callable property. @properties don't do perform()
    __classmethod = True   # This Activity is to be called only by classes (Tag instances cannot call. Not defined).
    __one_time_activity = True      # Don't have a use for this yet.


    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False
        kwargs['decorator'] = self.__method_name
        kwargs['one_time_activity'] = self.__one_time_activity

        # # Registers activity_name to prevent re-instantiation in GenericActivity.
        # self.definedActivities()[self.__activityName] = self
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)

    # def __call__(self, caller_object=None):   # caller_object is self from TagBovine, TagCaprine class: a tag object.
    #     self.outerObject = caller_object
    #     # TODO(cmt): Fancy use of inner func, using the enclosure from __call__ to call __baja() with args, kwargs.
    #     # This is possible only for activities with 1 and only 1 method defined. Alta, Baja for now.
    #     def inner(*args, **kwargs):
    #         return self.__alta(*args, **kwargs)
    #     return inner

    # Overrides __call__() in TagActivity. This one uses *args, **kwargs.
    def __call__(self, caller_object=None, *args, **kwargs):
        # caller_object is class TagBovine, TagCaprine class: a Tag subclass.
        self.outerObject = caller_object
        return self.__alta(*args, **kwargs)


    # @Activity.activity_tasks(after=(Activity.doInventory,))
    def __alta(self, *args, **kwargs):
        """     ********** NO CHECKS MADE ON kwargs. ALL DATA MUST BE VALIDATED BY THE UI functions ***********
        Creates a tag object of type passed in outerObject. Inserts a DB record in tblCaravanas with all the tag info.
        This is a class function -> self.outerObject is a class, NOT an Activity object.
        @param args:
        @param kwargs: All parameters required to create a tag, including tag number and tag uid.
        @return: Tag-subclass object (TagStandardBovine, TagStandardCaprine, TagRFIDBovine, etc.). None if fails.
        """
        tblDataName = 'tblDataCaravanasAltaBaja'
        activityID = self.getActivitiesDict()['Alta']
        uid = kwargs.get('fldObjectUID', None)
        if not uid:
            kwargs['fldObjectUID'] = uuid4().hex
        dfObj = pd.DataFrame.db.create(self.getTblObjectsName())
        dfRA = pd.DataFrame.db.create(self.getTblRAName())
        dfLink = pd.DataFrame.db.create(self.getTblLinkName())
        dfData = pd.DataFrame.db.create(tblDataName)
        eventDate = time_mt('dt')
        tag_items = tuple(kwargs.items())

        dfObj.loc[0, [j[0] for j in tag_items]] = [j[1] for j in tag_items]
        tag_record = setRecord(dfObj.db.tbl_name, **kwargs)
        if isinstance(tag_record, str):
            krnl_logger.error(f'ERR_DBAccess: cannot write Tag record object to db. Tag not created. e={tag_record}.')
            return None
        dfObj.loc[0, 'fldID'] = tag_record     # Sets record id in tblObj DataTable
        dfRA.loc[0, ('fldFK_NombreActividad', 'fldTimeStamp', 'fldFK_UserID')] = \
                                    (activityID, eventDate, sessionActiveUser)
        dfLink.loc[0, 'fldFK'] = kwargs.get('fldObjectUID')
        dfData.loc[0, 'fldAltaBaja'] = activityID
        idActividadRA = self._createActivityRA(dfRA, dfLink, dfData, uid=kwargs.get('fldObjectUID'))
        if isinstance(tag_record, str):
            krnl_logger.error(f'ERR_DBAccess: cannot write Tag record for {dfRA.db.tbl_name}. e={idActividadRA}.')
            return None
        # No entry created in the Status table for now. isAvailable() call on the object will yield "Available".
        tag_obj_class = self.outerObject._tagObjectsClasses.get(kwargs.get('fldFK_TagTechnology').lower(), None)
        if tag_obj_class is None:
            raise ValueError(f'{moduleName()}-{lineNum()}. ERR_SYS: Assignment error for Tag Class: None value passed.')

        obj = tag_obj_class(**dfObj.loc[0].to_dict())      # using this dict will include the recordID value.
        # Sets identifier value for object (it's a computed value in tblCaravanas that's not present in kwargs).
        obj.setIdentifiers(obj.create_identifier(elements=(obj.tagNumber, obj._tagIdentifierChar, obj.assignedToClass)))

        return obj                                              # Returns tag object.


@singleton
class BajaActivityTag(TagActivity):
    """Implements baja for Tags in the system.
    Callable class: invoking its instances with () automatically executes __call__() defined here.
    """
    __tblDataName = 'tblDataCaravanasAltaBaja'
    __activityName = 'Baja'
    __method_name = 'baja'  # Used in activityCreator() to create the callable property.
    __one_time_activity = True      # Don't have a use for this yet.

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False
        kwargs['decorator'] = self.__method_name
        kwargs['one_time_activity'] = self.__one_time_activity

        # # Registers activity_name to prevent re-instantiation in GenericActivity.
        # self.definedActivities()[self.__activityName] = self
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)


    def __call__(self, caller_object=None):     # caller_object is self from TagBovine, TagCaprine class: a tag object.
        self.outerObject = caller_object

        # TODO(cmt): Fancy use of inner func, using the enclosure from __call__ to call __baja() with args, kwargs.
        # This is practicable only for activities with 1 and only 1 method defined. Baja for now.
        # And also for activities that implement @property (called by instances only). The inner function code DOES NOT
        # work for classmethods. -> Perhaps because @property uses fget() method ??? TODO: Check on this.
        def inner(*args, **kwargs):
            return self.__baja(*args, **kwargs)
        return inner


    def __baja(self, *args, **kwargs):
        """     ********** NO CHECKS MADE ON kwargs. ALL DATA MUST BE VALIDATED BY THE UI functions ***********
        Sets tag as Inactive in tblCaravanas and creates a Baja Activity for the tag. Tag is no longer available for use
        @param kwargs:
        @return: True: Baja excuted. False: Baja not executed (tag is already in baja status). str: Error reading db.
        """
        activityID = self.getActivitiesDict()['Baja']
        status_baja = self.getStatusDict()['Baja'][0]
        dfRA = pd.DataFrame.db.create(self.getTblRAName())
        dfLink = pd.DataFrame.db.create(self.getTblLinkName())
        dfData = pd.DataFrame.db.create(self.__tblDataName)
        eventDate = time_mt('dt')
        tag = self.outerObject

        if tag.status.get() != status_baja:
            dfRA.loc[0, ('fldFK_NombreActividad', 'fldTimeStamp')] = (activityID, eventDate)
            dfLink.loc[0, 'fldFK'] = tag.ID
            dfData.loc[0, 'fldAltaBaja'] = activityID
            retValue = self._createActivityRA(dfRA, dfLink, dfData)
            if isinstance(retValue, str):
                db_logger.error(f'ERR_DBAccess: error reading {dfRA.db.tbl_name}. e= {retValue}.')
                return retValue
            tag.status.set(dfRA, dfLink, status='baja')
            _ = setRecord(self.tblObjectsName, fldID=tag.recordID, fldDateExit=eventDate.timestamp())
            # if self.doInventory(**kwargs):
            #     _ = self.outerObject.inventory.set(dfRA, dfLink, date=eventDate)
            return True
        return False

@singleton
class LocalizationActivityTag(TagActivity):
    __tblDataName = 'tblDataCaravanasLocalizacion'
    __activityName = 'Localizacion'
    __method_name = 'localization'
    _short_name = 'locali'              # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False
        kwargs['decoratorName'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    # This is NOT an Inventory activity (for now at least) - @Activity.activity_tasks(after=(Activity.doInventory,))
    def set(self, *args: pd.DataFrame, localization=None, **kwargs):
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
        return self._setLocalization(*args, **kwargs)


    def get(self, *args, full_record=False,  **kwargs):
        """
       Returns records in table Data LocalizationActivityAnimal between sValue and eValue.
        @param full_record: True -> Returns dict with full database row.
       @param kwargs: mode='value' -> Returns value from DB. If no mode or mode='memory' returns value from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        retValue = self._getLocalization(*args, full_record=full_record, **kwargs)
        return retValue

@singleton
class CommissionActivityTag(TagActivity):
    # Class Attributes: Tablas que son usadas por todas las instancias de CommissionActivityTag
    __activityName = 'Comision'
    __method_name = 'commission'
    _short_name = 'commis'  # Used by Activity._paCreate(), Activity.__isClosingActivity()
    _PHATT_Activity = True     # This Activity records Phyisically Attaching a Tag to an object (in Caravanas table)
    __tblDataName = 'tblDataCaravanasComision'
    __tblStatusName = 'tblDataCaravanasStatus'

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False
        kwargs['decoratorName'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    @property
    def isValid(self):
        return self.__isValidFlag

    @Activity.activity_tasks(after=(Activity.doInventory, ))
    def set(self, *args: DataTable, comm_type: str = None, assigned_to_class=None, **kwargs):
        """
        Tag Commissioning. Inserts records in DB tables. Also executes sub-activity on the tag.
        @param assigned_to_class: MANDATORY. Object class name to which the tag is assigned (Bovine, Caprine, etc.)
        @param comm_type: str. Values: 'Comision' (Default), 'Comision - Reemplazo', 'Comision - Reemision'.
        @param args: list of DataTable objects, with all the tables and fields to be written to DB.
                No checks performed. The function doing the writes will ignore all non-valid arguments.
        @param kwargs:
               tagCommissionType:  Values: 'Comision' (Default), 'Comision - Reemplazo', 'Comision - Reemision'.
        @return: ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        tagStatus = 'Comisionada'
        tagActivity = comm_type or next((removeAccents(kwargs[j]) for j in kwargs if 'tagcomm' in str(j).lower()), 'Comision')
        tagActivity = tagActivity.lower() if tagActivity in TagActivity.getActivitiesDict() else 'Comision'
        if 'emplazo' in tagActivity or 'replac' in tagActivity:
            activityID = TagActivity.getActivitiesDict()['Comision - Reemplazo']
            tagStatus = 'Reemplazada'
        elif 'emision' in tagActivity or 'emission' in tagActivity:
            activityID = TagActivity.getActivitiesDict()['Comision - Reemision']
        else:
            activityID = TagActivity.getActivitiesDict()['Comision']
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self.__tblDataName, default=None)
        args = list(args)
        if dfRA is None:
            dfRA = pd.DataFrame.db.create(self._tblRAName)
            args.append(dfRA)
        if dfLink is None:
            dfLink = pd.DataFrame.db.create(self._tblLinkName)
            args.append(dfLink)
        if dfData is None:
            dfData = pd.DataFrame.db.create(self.tblDataName())
            args.append(dfData)

        # Discards dates in the future for timeStamp, eventDate.
        time_now = time_mt('dt')
        timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if len(dfRA.index) and 'fldTimeStamp' in dfRA.columns
                        else time_now, time_now)
        eventDate = min(getEventDate(tblDate=dfData.loc[0, 'fldDate'] if not dfData.empty and 'fldDate' in dfData
                                                        else 0, defaultVal=timeStamp, **kwargs), timeStamp)
        dfRA.loc[0, ('fldFK_NombreActividad', 'fldTimeStamp')] = (activityID, timeStamp)
        dfData.loc[0, ('fldDate', 'fldFK_ComDecom')] = (eventDate, activityID)

        retValue = self._createActivityRA(dfRA, dfLink, dfData)     # initializes dfRA.loc[0, 'fldID']
        if isinstance(retValue, str):                   # str: Hubo error de escritura
            retValue = f'ERR_DBAccess - {retValue}  - {callerFunction(getCallers=True)}'
            krnl_logger.error(retValue)
            return retValue

        dfLink.loc[0, ('fldFK_Actividad', 'fldFK')] = (retValue, self.outerObject.ID)
        # Setting status will define the return value for isAvailable.
        _ = self.outerObject.status.set(dfRA, dfLink, status=tagStatus)  # uses idActividadRA from previous Activ.

        # TODO(cmt): Updates fldAssignedTo and PHATT field in tblCaravanas. This is the only place where this should be
        #  done and is the ONLY action required from Python to drive the Physically-Attached logic.
        PHATT = (getattr(self, '_PHATT_Activity', False) is True) * 1        # Resolves to 1/0
        _ = setRecord(self.tblObjectsName, fldID=self.outerObject.recordID, fldPhysicallyAttached=PHATT,
                       fldAssignedToClass=assigned_to_class)

        return retValue


    # def get(self, sDate='', eDate=''):
    #     """
    #     @param sDate: No args: Last Record; sDate=eDate='': Last Record;
    #                     sDate='0' or eDate='0': First Record
    #                     Otherwise: records between sDate and eDate
    #     @param eDate: See @param sDate.
    #     @return: DataTable object or None if error reading db.
    #     """
    #     if self._isValid:
    #         qryTable = self._getRecordsLinkTables(self.__tblRA, self.__tblLink, None, activity_list=('Comision',
    #                                               'Comision - Reemplazo', 'Comision - Reemision'))
    #         if isinstance(qryTable, DataTable) and qryTable:
    #             if qryTable.dataLen <= 1:        # qryTable tiene 1 solo registro (Ultimo o Primero)
    #                 return qryTable
    #             else:
    #                 return qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
    #             # print(f'TAG({lineNum()}) retTable: {retValue}')
    #     return None


    def unset(self, *args: DataTable, status=None, **kwargs):
        """
        Decommissioning of Tags
        @param status: status (str) to set the uncommissioned tag to (decomisionada, reemplazada, extraviada, etc.).
        @param args: list of DataTable objects, with all the tables and fields to be written to DB.
                No checks performed. The function doing the write will discard and ignore all non valid arguments
        @param kwargs:
               tagStatus: status for decommissioned tag.
               Permitted values: 'Decomisionada' (Default), 'Baja', 'Reemplazada', 'Extraviada'
        @return: 0->ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        activityName = 'Decomision'
        activityID = self.outerObject.getActivitiesDict()[activityName]
        validStatus = ('decomisionada', 'reemplazada', 'baja', 'extraviada')
        tagStatus = 'Decomisionada'
        tagStatus = status if status.strip().lower() in validStatus else tagStatus
        tagStatus = self.statusDict[tagStatus][0]
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self.__tblDataName, default=None)
        args = list(args)
        if dfRA is None:
            dfRA = pd.DataFrame.db.create(self._tblRAName)
            args.append(dfRA)
        if dfLink is None:
            dfLink = pd.DataFrame.db.create(self._tblLinkName)
            args.append(dfLink)
        if dfData is None:
            dfData = pd.DataFrame.db.create(self.tblDataName())
            args.append(dfData)

        # Discards dates in the future for timeStamp, eventDate.
        time_now = time_mt('dt')
        timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if not dfRA.empty and 'fldTimeStamp' in dfRA and
                                                       pd.notnull(dfRA.loc[0, 'fldTimeStamp']) else time_now, time_now)
        date = dfData.loc[0, 'fldDate'] if not dfData.empty and 'fldDate' in dfData and \
                                           pd.notnull(dfData.loc[0, 'fldDate']) else None
        eventDate = min(getEventDate(tblDate=date, defaultVal=timeStamp, **kwargs), timeStamp)
        dfRA.loc[0, ('fldFK_NombreActividad', 'fldTimeStamp')] = (activityID, timeStamp)
        dfData.loc[0, ('fldDate', 'fldFK_ComDecom')] = (eventDate, activityID)

        retValue = self._createActivityRA(dfRA, dfLink, dfData, *args, **kwargs)
        if isinstance(retValue, str):                   # str: Hubo error de escritura
            retValue = f'ERR_DBAccess - {retValue} - {callerFunction()}'
            krnl_logger.error(retValue)
        else:
            _ = self.outerObject.status.set(dfRA, dfLink, status=tagStatus)  # Setea status de tag c/ idActividad de RA

        # Don't run doInventory: self._activityID is set to Commission and will yield the result for Commission.
        # Decommission is NOT an inventory activity (for now)

        return retValue

    # ========================================= END TagActivity Class =============================================== #



