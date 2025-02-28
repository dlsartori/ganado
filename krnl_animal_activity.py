import pandas as pd

from krnl_abstract_class_activity import *
from krnl_abstract_class_prog_activity import ProgActivity
import uuid
from krnl_geo import Geo
from krnl_tag import Tag
from krnl_tag_bovine import TagAnimal, TagBovine
from krnl_person import Person
from krnl_custom_types import setupArgs, setRecord, delRecord, getTblName, getFldName, AbstractMemoryData
from krnl_config import krnl_logger, compare_range
# from collections.abc import Iterable
import collections.abc
import threading

def moduleName():
    return str(os.path.basename(__file__))


class AnimalActivity(Activity):     # Abstract Class (no lleva ningun instance attributte). NO se instancia.
    # __abstract_class = True     # Activity runs the __init_subclass() function to initialize all subclasses.
    # dict used with Activity classes. Accessed from Activity class. Stores AnimalActivity class objects.
    # Used to create Activity instance objects and to bind, when applicable, ActivityClasses with Object classes.
    _activity_class_register = {}  # {ActivityClass: ObjectClass | None, }

    # Lists all Activity classes that support memory data, for access and initialization.
    _memory_data_classes = set()  # Initialized on creation of Activity classes. Defined here to include all Activities.

    def __call__(self, caller_obj=None):
        """            *** This method is inherited and can be accessed from derived classes. Cool!! ****
        __call__ is invoked when an instance is followed by parenthesis (instance_classmethod()) or when a property is
        defined on the instance, and the property is invoked (inventory.get(): in this case __call__ is invoked for the
        obj associated with inventory property before making the get() call. Then, outerObject is properly initialized.)
        @param caller_obj: instance of Bovine, etc. that invokes the Activity. How this binding works is explained at
        https://stackoverflow.com/questions/57125769/when-the-python-call-method-gets-extra-first-argument
        @return: Activity obj invoking __call__() or None. The meaning of None is relevant to call without outerObject.
        """
        # caller_obj=None above is important to allow to call fget() like that, without having to pass dummy parameters.
        # print(f'\n>>>>>>>>>>>>>>>>>>>> {self._decoratorName}.__call__() params - args: {(caller_obj.ID, *args)}; '
        #       f'kwargs: {kwargs}')
        self.outerObject = caller_obj  # caller_obj es instance de Animal, Tag, etc, o Class cuando llama classmethod.

        # 10Aug24: The key issue is that called functions HAVE TO BE inside this block, or caller_obj cannot be bound
        # inside those functions.
        # But there is no way to know, right here, which method in Activity will be called after __call__() returns.
        # So using closures in this context is not viable. Must stick with self.outerObject.
        # print(f'_call closure id: {self.__call__.__closure__}')

        return self


    # Class Attributes: Tablas que son usadas por todas las instancias de InventoryActivityAnimal
    __tblRAName = 'tblAnimalesRegistroDeActividades'
    __tblRADBName = getTblName(__tblRAName)
    __tblObjectsName = 'tblAnimales'     # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
    __tblObjectDBName = getTblName(__tblObjectsName)
    __tblLinkName = 'tblLinkAnimalesActividades'
    __tblLinkDBName = getTblName(__tblLinkName)
    __tblDataCategoryName = 'tblDataAnimalesCategorias'
    __tblProgStatusName = 'tblDataActividadesProgramadasStatus'       # __tblPADataStatusName
    __tblProgActivitiesName = 'tblDataProgramacionDeActividades'
    __tblPATriggersName = 'tblAnimalesActividadesProgramadasTriggers'


    @classmethod
    def tblObjDBName(cls):
        return cls.__tblObjectDBName

    @classmethod
    def tblRADBName(cls):
        return cls.__tblRADBName

    @classmethod
    def tblRAName(cls):
        return cls.__tblRAName

    @classmethod
    def tblObjName(cls):
        return cls.__tblObjectsName

    @classmethod
    def tblLinkName(cls):
        return cls.__tblLinkName

    @classmethod
    def tblLinkDBName(cls):
        return cls.__tblLinkDBName

    @classmethod
    def getTblObjectsName(cls):
        return cls.__tblObjectsName

    @classmethod
    def getTblRAName(cls):
        return cls.__tblRAName

    @classmethod
    def getTblLinkName(cls):
        return cls.__tblLinkName


    tempdf = getrecords('tblAnimalesClases', 'fldID', 'fldAnimalClass')
    __animalKinds = dict(zip(tempdf['fldAnimalClass'], tempdf['fldID']))  # {Clase De Animal (str): animalClassID}
    del tempdf

    @classmethod
    def getAnimalKinds(cls):  # Repeat func. Needed here for convenience, to parameterize definition of Animal classes.
        return cls.__animalKinds  # {'Bovine': 1, 'Caprine':2, 'Ovine':3, etc } -> leido de DB.

    tempdf = getrecords('tblAnimalesActividadesNombres', 'fldID', 'fldName', 'fldFlag', 'fldFK_ClaseDeAnimal',
                        'fldFlagPA', 'fldPAExcludedFields', 'fldDecoratorName', 'fldTableAndFields', 'fldFilterField',
                        'fldAdHocMethods')
    _activitiesDict = {}           # {Nombre Actividad: ID_Actividad, }
    __inventoryActivitiesDict = {}      # {Nombre Actividad: isInventory (True/False), }
    _activitiesForMyClass = {}
    _genericActivitiesDict = {}  # {Nombre Actividad: (activityID,decoratorName,tblName,qryFieldName,filterField), }
    _supportsPADict = {}          # {activity_name: activityID, } con getSupportsPADict=True
    __paRegisterDict = {}  # {self: ActivityID} Register de las Actividades Programadas ACTIVAS de class AnimalActivity


    # Variables para logica de manejo de objetos repetidos/duplicados.
    _fldID_list = []  # List of all active records pulled by getRecords() from tblAnimales.
    _fldID_exit_list = []  # List of all all records with fldExitDate > 0, updated in each call to _processDuplicates().
    _object_fldUPDATE_dict = {}  # {fldID: fldUPDATE_counter, }        Keeps a dict of uuid values of fldUPDATE fields
    # _RA_fldUPDATE_dict = {}  # {fldID: fldUPDATE_counter, }      # TODO: ver si este hace falta.
    # _RAP_fldUPDATE_dict = {}  # {fldID: fldUPDATE_counter, }      # TODO: ver si este hace falta.
    temp0df = getrecords(__tblRAName, 'fldID')
    if len(temp0df.index):
        # Initializes _fldID_list with all fldIDs for Triggers that detect INSERT operations.
        _fldID_list = temp0df['fldID'].tolist()

    __classExcludedFieldsClose = {"fldWindowLowerLimit", "fldWindowUpperLimit", "fldFK_Secuencia",
                                  "fldDaysToAlert", "fldDaysToExpire"}.union(Activity._getBaseExcludedFields())
    __classExcludedFieldsCreate = {"fldPADataCreacion"}.union(Activity._getBaseExcludedFields())
    _activityExcludedFieldsClose = {}  # {activityID: (excluded_fields, ) }
    __activityExcludedFieldsCreate = {}  # {activityID: (excluded_fields, ) }

    # Creates dictionary with GenericActivity data. Each item in the dictionary will generate 1 GenericActivity object.
    for i, row in tempdf.iterrows():
        d = row.to_dict()
        if d['fldTableAndFields'] and d['fldDecoratorName'] and "." in d['fldTableAndFields']:
            tbl_flds = d['fldTableAndFields'].split(".")
            _genericActivitiesDict[d['fldName']] = (d['fldID'], d['fldDecoratorName'], tbl_flds[0], tbl_flds[1],
                                                     d['fldFilterField'] or 'fldDate', d['fldAdHocMethods'])
    print(f'--------- {moduleName()}.{lineNum()}----- _genericActivitiesDict: {_genericActivitiesDict}',
          dismiss_print=DISMISS_PRINT)


    @classmethod                                    # TODO(cmt): Main two methods to access excluded_fields
    def getActivityExcludedFieldsClose(cls, activity_name=None):
        return cls._activityExcludedFieldsClose.get(activity_name, set())

    @classmethod
    def getActivityExcludedFieldsCreate(cls, activity_name=None):
        return cls.__activityExcludedFieldsCreate.get(activity_name, set())

    # Como funciona: aqui se registran todos los objetos que se crean a partir de class Activity especificas, como
    # @singleton. Entonces si se pasa un activity_name ya definido a GenericActivity() este retorna el objeto singleton
    # ya existente.
    __definedActivities = {}  # {'activity_name': activityObj}. Used to avoid duplication with GenericActivity objects.
    @classmethod
    def definedActivities(cls):
        return cls.__definedActivities  # {'activity_name': activityObj}.Used to avoid duplication with GenericActivity.

    @classmethod
    def getSupportsPADict(cls):
        return cls._supportsPADict    # {ActivityName: __activityID, } con getSupportsPADict=True

    # ('fldID', 'fldName', 'fldFlag', 'fldFK_ClaseDeAnimal', 'fldFlagPA', 'fldPAExcludedFields', 'fldDecoratorName',
    #      'fldTableAndFields', 'fldFilterField', 'fldAdHocMethods')

    for i, row in tempdf.iterrows():
        _activitiesDict[row['fldName']] = row['fldID']            # {Nombre Actividad: ID_Actividad, }
        __inventoryActivitiesDict[row['fldID']] = row['fldFlag']  # {Nombre Actividad: Inv. Activity (1/0),}
        _activitiesForMyClass[row['fldName']] = row['fldFK_ClaseDeAnimal']  # {Nombre Actividad: AnimalClass, }
        if bool(row['fldFlagPA']):
            _supportsPADict[row['fldName']] = row['fldID']  # {ActivityName: __activityID, }, supportPA=True
        # {Nombre Actividad: excluded_fields,}
        _activityExcludedFieldsClose[row['fldName']] = \
            set(row['fldPAExcludedFields'] or set()).union(__classExcludedFieldsClose)
    del tempdf
    del temp0df


    def __new__(cls, *args, **kwargs):
        if cls is AnimalActivity:
            krnl_logger.error(f"ERR_TypeError: {cls.__name__} cannot be instantiated, please use a subclass")
            raise TypeError(f'ERR_TypeError: class {cls} cannot be instantiated. Please, use one of its subclasses.')
        return super().__new__(cls)

    def __init__(self, activityName=None, *args, activity_enable=activityEnableFull, tbl_data_name='',
                 excluded_fields_close=(), **kwargs):
        activityID = self._activitiesDict.get(activityName)
        invActivity = self.__inventoryActivitiesDict.get(activityName)
        kwargs['tblProgStatusName'] = self.__tblProgStatusName
        kwargs['tblProgActivitiesName'] = self.__tblProgActivitiesName
        kwargs['excluded_fields'] = self.getActivityExcludedFieldsClose(activity_name=activityName) or set() .\
            union(excluded_fields_close)
        isValid = True
        if kwargs.get('supportsPA', None) is None:
            # IMPORTANT SETTING: Si no hay override desde abajo, setea al valor de _supportsPA{} para ese __activityName
            kwargs['supportsPA'] = bool(self._supportsPADict.get(activityName, False))

        super().__init__(isValid, activityName, activityID, invActivity, activity_enable, self.__tblRAName, *args,
                         tblDataName=tbl_data_name, tblObjectsName=self.__tblObjectsName,  **kwargs)     #


    @classmethod
    def getActivitiesDict(cls):
        return cls._activitiesDict           # {Nombre Actividad: ID_Actividad, }

    @property
    def activities(self):  # Este @property es necesario para las llamadas a obj.outerObject.
        return self._activitiesDict          # {Nombre Actividad: ID_Actividad, }

    @classmethod
    def getInventoryActivitiesDict(cls):   # {Nombre Actividad: isInventory (1/0), }
        return cls.__inventoryActivitiesDict

    @classmethod
    def tblDataCategoryName(cls):
        return cls.__tblDataCategoryName

    @staticmethod
    def getPARegisterDict():           # TODO(cmt): AnimalActivity, ProgActivityAnimal directly linked.
        return ProgActivityAnimal.getPARegisterDict()       # {paObj: ActivityID}

    def activityExecuteFields(self, **kwargs) -> dict:
        """ Function used to create the executeFields dict to be passed to _paMatchAndClose() method.
        Defines all Execution fields to compare against Close fields in ProgActivities and determine if self is a
        Closing Activity for the ProgActivities it is checked against.
        @return: dict with updated execution values for the call. ONLY REQUIRED FIELDS MUST BE INCLUDED. NOTHING ELSE!!!
        """
        kwargs.update({'fldFK_ClaseDeAnimal': self.outerObject.animalClassID()})
        return kwargs
                # 'execution_date': execution_date,
                # 'ID': self.outerObject.ID,
                # 'fldAgeDays': self.outerObject.age.get(),
                # 'fldFK_Localizacion': self.outerObject.lastLocalization,
                # 'fldFK_Raza': self.outerObject.animalRace,
                # 'fldMF': self.outerObject.mf,
                # 'fldFK_Categoria': [self.outerObject.lastCategoryID]
               #  })

    # ------------ Funciones de Categoria, generales para TODAS las clases de Animal. Vamos empezando por aqui,... ---#

    # NEW VERSION
    # @Activity.activity_tasks(after=(Activity.doInventory, Activity.paScheduler))
    def _setCategory(self, *args, excluded_fields=None, execute_fields=None, **kwargs):
        """
        @param args: DataTable objects with obj_data to write to DB
        @param kwargs:  'enforce'=True: forces the Category val irrespective of __statusPermittedDict conditions
                         'categoryID': Category number to set, if Category number is not passed via tblData
        @return: idActividadRA if record created.
                 None if record not created (same category)
                 errorCode (str) if invalid category
        """
        outerObj = self.outerObject
        if not outerObj or type(outerObj) is type:          # Only operates with objects. No good with classes.
            raise TypeError(f'_setCategory() cannot be called by a class ({outerObj}). '
                            f'Only to be called by an Activity object.')
        dfRA = next((j for j in args if j.db.tbl_name == self._tblRAName), pd.DataFrame.db.create(self._tblRAName))
        dfData = next((j for j in args if j.db.tbl_name == self._tblDataName),pd.DataFrame.db.create(self._tblDataName))

        # Procesa Categoria Y valida categoryID: Todo este mambo es para aceptar categorias como nombre y/o como ID
        categoryID = dfData.loc[0, 'fldFK_Categoria'] if len(dfData.index) > 0 else None
        if categoryID not in outerObj.getCategories().values():  # Class access to attr. to generalize to obj or class.
            categID = removeAccents(next((kwargs[j] for j in kwargs if 'cat' in j.lower()), None))
            categoryID = categID if categID in outerObj.getCategories().values() else \
                next((v for k, v in outerObj.getCategories().items() if removeAccents(str(k)) == categID), None)
        if categoryID is None:
            retValue = f'ERR_INP_ArgumentsNotValid: Category not valid or missing: {str(categoryID)} - ' \
                       f'{callerFunction(getCallers=True)}.'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue

        # Discards dates in the future for timeStamp, eventDate.
        time_now = time_mt('dt')
        timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if len(dfRA.index) else time_now, time_now)
        eventDate = min(getEventDate(tblDate=dfData.loc[0, 'fldDate'] if len(dfData.index) else None,
                                     defaultVal=timeStamp, **kwargs), timeStamp)
        dfData.loc[0, 'fldDate'] = eventDate
        dfRA.loc[0, 'fldTimeStamp'] = timeStamp
        # No prob with this call on category.get(), TODO(cmt): -> as long as set_category=False to avoid recursion.
        lastCategory = outerObj.category.get(mode='memory', set_category=False)

        # Aqui abajo, self accede al diccionario __permittedFrom de la clase de Animal correcta.
        enforce = next((bool(v) for k, v in kwargs.items() if 'enforce' in k.lower()), None)
        # Flag to invoke PA creation method when conditions for this Activity change.
        if categoryID and (categoryID in self.permittedDict()[lastCategory] or enforce):
            # Situation here: category itself is NOT a ProgActivity (categories CANNOT be programmed). HOWEVER, a
            # category change MAY trigger the creation of one or more ProgActivity records for outerObject.
            # flagExecInstance flags just that.
            flagExecInstance = (lastCategory != categoryID) if self.classSupportsPA() else False

            # tblLink = next((j for j in args if j.tblName == self._tblLinkName), DataTable(self._tblLinkName))
            dfLink = next((j for j in args if j.db.tbl_name == self._tblLinkName),
                           pd.DataFrame.db.create(self._tblLinkName))
            activityID = dfRA.loc[0, 'fldFK_NombreActividad'] if not dfRA.empty else None
            activityID = activityID if activityID else self._activityID
            dfRA.loc[0, 'fldFK_NombreActividad'] = activityID
            dfData.loc[0, 'fldFK_Categoria'] = categoryID
            idActividadRA = self._createActivityRA(dfRA, dfLink, dfData, **kwargs)
            retValue = idActividadRA
            if isinstance(retValue, int):
                dfRA.loc[0, 'fldID'] = idActividadRA  # TODO: This value is passed back to paScheduler()
                # Sets the proper castration value for _flagCastrado based on the category being set.
                castration_date = self.outerObject.castration.get()
                castration_date = (castration_date or 1) if categoryID in self.categ_castrated() else 0
                # Here it only modifies _fldFlagCastrado in DB and the system. DOES NOT INSERT A NEW Castration Activity
                # Can't use castration.set() here 'cause will produce an infinite recursion loop.
                outerObj._fldFlagCastrado = castration_date
                _ = setRecord(self.tblObjName(), fldID=self.outerObject.recordID,
                              fldFlagCastrado=outerObj._fldFlagCastrado)

                if self.supports_mem_data():
                    memVal = self.get_mem_data()  # TODO: Do NOT use query_func=_getCategory here to avoid endless loop.
                    if not memVal or memVal.get('fldDate', 0) < eventDate:
                        # ('fldDate', 'fldFK_Actividad', 'fldFK_Categoria', 'fldDOB', 'fldMF')
                        values = (eventDate, idActividadRA, categoryID, self.outerObject.dob, self.outerObject.mf)
                        # vals = {k: v for k, v in dfData.iloc[0].to_dict().items() if k in
                        #         self._mem_data_params['field_names']}
                        vals = dict(zip(self._mem_data_params['field_names'], values))
                        self.set_mem_data(vals)

                # Verificar si hay hay que cerrar alguna(s) PA con con esta actividad como "cerradora".
                # Minimum required at Activity level to drive the ProgActivity logic. The rest is done by paScheduler().
                # This code is specific for the Activity (executeFields). idActividadRA just created and excluded_fields
                # are also required to be passed on.
                # if self._supportsPA:
                #     executeFields = self.activityExecuteFields(execution_date=eventDate)
                #     if execute_fields and isinstance(execute_fields, dict):
                #         executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
                #     # Pretty contorted recursive dictionary to be able to pass data back to paScheduler() func.
                #     kwargs['returned_params'].update({'idActividadRA': idActividadRA, 'execute_fields': executeFields,
                #                                        'excluded_fields': excluded_fields})  # for paScheduler()

                if flagExecInstance:
                    # When conditions have changed, checks if the new conditions call for the creation of a new
                    # ProgActivities on the object.
                    self._paCreateExecInstance(outer_obj=self.outerObject)     # This one also goes to a queue.

                # if self.doInventory(**kwargs):
                #     _ = self.outerObject.inventory.set(tblRA, tblLink, date=max(timeStamp, eventDate))

            else:
                retValue = f'ERR_Sys_Object not valid or activity not defined: {retValue}- {callerFunction()}'
                krnl_logger.info(retValue)
                print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)

        elif categoryID == lastCategory:
            retValue = True   # -> True si no hay cambio de Categoria, porque no se genero idActividadRA.
        else:
            retValue = f'ERR_Sys_Category Problem - ID: {self.outerObject.ID}({self.outerObject.recordID}) ' \
                       f'current Category: {lastCategory}; required Category:{categoryID}; ' \
                       f'permittedDict[{lastCategory}]: {self.permittedDict()}'
            krnl_logger.warning(f'{moduleName()}({lineNum()}  - {retValue}')
            print(f'{moduleName()}({lineNum()}  - {retValue}', dismiss_print=DISMISS_PRINT)
            retValue = False

        return retValue


    def _getCategory(self, *args, mode='mem', id_type='id', uid=None, full_record=False, set_category=False,
                     all_records=False, **kwargs):
        """
        Returns records in table Data Animales Categorias between sValue and eValue. sValue=eValue ='' ->Last record
        If the last update of the value (Category in this case) is less than 1 day old, returns value from memory.

        @param event_date: True -> returns the date lastCategoryID was set (_lastCategoryID[1])
        @param uid: when passed, Animal uid to compute category based on age.
        @param set_category: True:executes _setCategory() method to update category in db and memory_data. Default=False
        @param all_records: returns full datataframe read from db.
        @param kwargs: mode='value' -> Returns value from DB.
                       id_type='name'-> Category Name; id_type='id' or no id_type -> Category ID.
        @return: CategoryID (int) | CategoryName (str) | last Category record (DataFrame) | multiple records (DataFrame)
        """
        fldName = 'fldFK_Categoria'
        id_type = id_type or ''     # id_type=name returns categoryName, otherwise returns categoryID.
        modeArg = mode or 'value'
        latest_rec = None
        # outerObject is an instance of Bovine, Caprine, etc., OR class Bovine, Caprine, etc.
        outer_object = self.outerObject
        # Todo(cmt): Tries memory data first. Reduces drastically db access. _getCategory() MUST support re-entry!!!
        tmp = self.get_mem_data(query_func=self._getCategory, uid=uid)  # tmp is the full data record for uid.
        query_df = None
        if 'mem' in modeArg and isinstance(tmp, dict):
            initial_cat = tmp.get(fldName)
            computedCategory = self._compute(initial_category=initial_cat, uid=uid)
            if initial_cat not in self.category_names():
                krnl_logger.error(f'ERR_SYS: category is not valid!!: {initial_cat}.')
                return None
            if computedCategory == initial_cat:
                if not full_record:
                    return self.category_names()[initial_cat] if 'name' in id_type else initial_cat
                return tmp      # Returns full data dictionary.
        else:
            query_df = self._get_link_records(uid=uid, **kwargs)
            if not query_df.empty:
                # TODO: FIX THE CASE OF EMPTY query_df.
                # Assumes a dataframe with more than 1 record and pulls the record with latest fldDate.
                latest_rec = query_df.loc[query_df['fldDate'] == query_df['fldDate'].max()].iloc[0]
                initial_cat = latest_rec.loc[fldName] if not latest_rec.empty else None
                # Checks against existing category and re-compute if they are different.
                computedCategory = self._compute(initial_category=initial_cat, uid=uid)  # Computes without calling set()
                latest_rec.fldFK_Categoria = computedCategory
            else:
                krnl_logger.error(f'ERR_ValueError: {uid} not present in {self._tblDataName}.')
                return None

        if initial_cat != computedCategory:
            if set_category:
                # Updates newly computed category to database and memory data when set_category=True.
                dfRA, dfLink, dfData = self.set3frames(dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                                       dfData_name=self._tblDataName)
                eventDate = time_mt('dt')
                dfRA.loc[0, ('fldID', 'fldFK_NombreActividad')] = (None, self._activityID)  # fldID=None->force new rec
                dfLink.loc[0, ('fldID', 'fldFK')] = (None, getattr(outer_object, 'ID', uid))
                dfData.loc[0, ('fldID', 'fldFK_Categoria', 'fldDate', 'fldComment')] = \
                               (None, computedCategory, eventDate, 'Category set from _getCategory().')
                idActividadRA = self._createActivityRA(dfRA, dfLink, dfData, uid=uid, **kwargs)  # Sets new category.
                if isinstance(idActividadRA, int):
                    # Now, sets memory_data for category change.
                    # mem_rec = self.get_mem_data(query_func=self._getCategory, uid=uid)  # Reads existing record first
                    # if isinstance(mem_rec, dict):
                    mem_rec = {fldName: computedCategory, 'fldFK_Actividad': idActividadRA, 'fldDate': eventDate}
                    self.set_mem_data(mem_rec, uid=uid)
                else:
                    krnl_logger.error(f'ERR_DBAccess: could not update Animal Category in database. Computed '
                                      f'category not saved.')
        if all_records:
            if query_df:
                return query_df
            else:
                full_record = True

        if full_record:
            retValue = latest_rec.to_dict()   # Returns full dictionary of tblData fields.
        elif 'name' in id_type.lower():
            retValue = self.category_names()[computedCategory]       # Returns categoryName.
        else:       # id_type set to anything other than 'name' results in returning categoryID.
            retValue = computedCategory  # returns computed category if success or original category if call fails.
        return retValue


    @property
    def activityID(self):
        return self._activityID

    @property
    def activityName(self):
        return self._activityName

    @classmethod
    def tblDataProgramacionName(cls):
        return cls.__tblProgActivitiesName



    # def _getCategory00(self, sDate='', eDate='', *args, mode='mem', full_record=False, **kwargs):
    #     """
    #     Returns records in table Data Animales Categorias between sValue and eValue. sValue=eValue ='' ->Last record
    #     If the last update of the value (Category in this case) is less than 1 day old, returns value from memory.
    #     @param sDate: No args: Last Record; sDate=eDate='': Last Record;
    #                     sDate='0' or eDate='0': First Record
    #                     Otherwise: records between sDate and eDate
    #     @param eDate: See @param sDate.
    #     @param event_date: True -> returns the date lastCategoryID was set (_lastCategoryID[1])
    #     @param kwargs: mode='value' -> Returns value from DB.
    #                    id_type='name'-> Category Name; id_type='id' or no id_type -> Category ID.
    #     @return: Object DataTable with information from queried table or statusID (int) if mode=val
    #     """
    #     fldName = 'fldFK_Categoria'
    #     id_type = kwargs.get('id_type') or None  # id_type=name returns categoryName, otherwise returns categoryID.
    #     retValue = None
    #     # Todo esto para aprovechar los datos en memoria y evitar accesos a DB.
    #     outer_obj = self.outerObject  # This activity can be invoked by the Activity obj itself (no outerObject)
    #     modeArg = mode
    #
    #     if 'mem' in modeArg:
    #         # Todo(cmt): get_mem_data reduces drastically db access. _getCategory() MUST support re-entry!!!
    #         tmp = self.get_mem_data(query_func=self._getCategory).get(fldName) if not full_record else \
    #             self.get_mem_data(query_func=self._getCategory)
    #         if isinstance(tmp, (int, dict)):
    #             return tmp
    #         else:
    #             modeArg = 'value'
    #
    #     tblRA = setupArgs(self._tblRAName, *args)  # __tblRAName viene de class Activity
    #     tblLink = setupArgs(self._tblLinkName, *args)  # __tblLinkName viene de class Activity
    #     tblData = setupArgs(self._tblDataName, *args, **kwargs)  # __tblDataName seteado INDIVIDUALMENTE.
    #     qryTable = self._getRecordsLinkTables(tblRA, tblLink, tblData)
    #     if isinstance(qryTable, DataTable):
    #         if qryTable.dataLen <= 1:
    #             result = qryTable
    #         else:
    #             result = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
    #         if full_record is True:
    #             return result.unpackItem(0)
    #
    #         categID = result.getVal(0, fldName)
    #         # Checks against existing category and re-compute if they are different.
    #         computedCategory = outer_obj.category._compute(initial_category=categID) if outer_obj else \
    #             self._compute(initial_category=categID)
    #         if categID != computedCategory:
    #             # Must update newly computed category to database and memory data.
    #             tblRA.setVal(0, fldID=None)  # Clears fldID to force creation of a new record.
    #             tblLink.setVal(0, fldID=None, fldFK=None)  # All other parameters should be OK in tables.
    #             tblData.setVal(0, fldID=None, fldFK_Categoria=computedCategory,
    #                            fldComment='Category set from get().')
    #             tblRA.setVal(0, fldFK_NombreActividad=self._activityID)
    #             idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)  # Sets the new category.
    #             if not isinstance(idActividadRA, int):
    #                 krnl_logger.error(f'ERR_DBAccess: could not update Category tables in database.')
    #                 return computedCategory  # Could not update database, returns computed category.
    #             categID = computedCategory  # Updates to new computedCategory only on success, otherwise keeps old value
    #
    #         if 'val' in modeArg.lower():
    #             if not id_type or 'id' in id_type.lower():
    #                 retValue = categID  # returns computed category if success or original category if call fails.
    #             else:
    #                 # Returns categoryName.
    #                 # retValue = next((j for j in outer_obj.categories if outer_obj.categories[j] == categID), None)
    #                 retValue = next((j for j in self._activities_dict if self._activities_dict[j] == categID), None)
    #         else:
    #             retValue = result  # Retorna DataTable con registros. Support this for now. Later, we'll see...
    #     return retValue



# --------------------------------------------- Fin Class AnimalActivity --------------------------------------------- #




# TODO ========= GenericActivityAnimal NOT SUPPORTED FOR NOW WE'LL SEE LATER . ========= #
class GenericActivityAnimal(AnimalActivity):                # GenericActivity Class for Animal.
    """
    Enables the creation of multiple AnimalActivity objects, sparing the creation of a full Activity Class when simple
    getter/setter functions are needed.
    Data required to fully initialize an object: activityName, activityID, tblData, query field in the form
    "tblName.fldName" and filter_fld_name. These 4 items are read from [Animales Actividades Nombres] table.
    Instance objects are created in Animal class as a @property.
    ***** Additional external methods can be attached to individual objects via the register_method() method *****
    """
    __method_name = 'generic_activity'  # Defines __method_name to be included in Activity._activity_class_register
    # methods not to be deleted / unregistered.
    __reserved_names = ('get', 'set', 'register_method', 'unregister_method', 'methods_wrapper',
                        '_createGenericActivityObjects')
    __initialized = False

    def __new__(cls, activName, *args, **kwargs):  # Override para crear objeto nuevo solo si Activity no esta definida
        if activName not in cls.getActivitiesDict():
            raise NameError(f'ERR_INP_Invalid Activity: {activName}. Activity not created.')
        if activName in cls.definedActivities():
            cls.__initialized = True                      # Flag para evitar re-inicializacion de la instancia.
            return cls.definedActivities()[activName]     # Retorna objeto Activity ya creado, si existe.
        else:
            instance = super().__new__(cls)  # Crea objeto nuevo si el numero este dentro del limite de objs per thread
            cls.__initialized = False  # Flag para evitar re-inicializacion de la instancia.
            return instance


    def __init__(self, activName, *args, decorator_name=None, tbl_data_name='', qry_fld_names='*',
                 filter_field_name='fldDate', one_shot=False, excluded_fields=(), **kwargs):
        """ All fields names to query with get() must be passed as qry_fld_names.
        @param one_shot: True -> Activity is to be execute ONLY ONCE on each outerObject.
        All field names to write/update with set must be set in DataTables passed as args to set().
        """
        if self.__initialized or not tbl_data_name:
            self.__initialized = False      # Resetea flag de inicializacion para el proximo objeto que se vaya a crear.
            return                          # Evita re-inicializar si objeto existe desde antes.

        self.__qryFldName = qry_fld_names   # Field Name to be queried for Activity in get() method. 1 field only.
        self.__filterFldname = filter_field_name
        self.__oneShot = one_shot        # TODO(cmt): identify one-time only activities (weaning, castracion, etc).
        super().__init__(activName, *args, activity_enable=activityEnableFull, tbl_data_name=tbl_data_name,
                         excluded_fields=excluded_fields, decorator_name=decorator_name, **kwargs)
        self.definedActivities()[self._activityName] = self

        # Now sets any existing Ad Hoc methods (their names listed in the db record) for the GenericActivity object.
        methods_dict = next((v for k, v in kwargs.items() if 'adhoc_methods' in k.lower()), None)
        if methods_dict and isinstance(methods_dict, dict):
            # methods listed in AnimalesActividadesNombres.AdHoc_Methods field MUST be defined in GenericActivity class.
            for k in methods_dict:
                method = getattr(self, methods_dict[k], None)
                if hasattr(method, '__self__'):
                    method = method.__func__
                if callable(method) or hasattr(method, '__func__'):
                    if k in self.__reserved_names:
                        # To "override" en existing method (defined in __reserved_names), implements a decorator call
                        decorated = method(self, getattr(self, k).__func__)
                        setattr(self, k, decorated)
                    else:
                        # If method is not in reserved_names, implements a regular call.
                        # method passed to register_method() MUST be unbound.
                        self.register_method(method_obj=method, method_name=k)


    @classmethod
    def _createGenericActivityObjects(cls, **kwargs):
        """ Returns list of GenericActivityAnimal objects to be initialized in Animal.__init_subclass(). """
        retList = []
        for k in cls._genericActivitiesDict:
            if any(not j for j in (cls._genericActivitiesDict[k][1], cls._genericActivitiesDict[k][2],
                                   cls._genericActivitiesDict[k][3])):
                continue
            try:
                obj = cls(k, decorator_name=cls._genericActivitiesDict[k][1],
                          tbl_data_name=cls._genericActivitiesDict[k][2], qry_fld_names=cls._genericActivitiesDict[k][3],
                          filter_field_name=cls._genericActivitiesDict[k][4] or 'fldDate',
                          adhoc_methods=cls._genericActivitiesDict[k][5] or None,
                          one_shot=False, excluded_fields=(), **kwargs)
            except (TypeError, AttributeError, ValueError, NameError, SyntaxError):
                pass
            else:
                retList.append(obj)
        return retList


    def set(self, *args:DataTable, val=None, pa_force_close=False, execute_fields=None, excluded_fields=None, **kwargs):
        """ Creates an Activity Record. Sets data in tblRA, tblLink, tblData tables for corresponding activity object.
            Regular form: populate all pertinent fields in tblData. Pass as DataTable argument.
            Short form (sets 1 value only): use parameter val to set value of qryFldName.
            @param val: value to set qryFieldName to (short form of function call) if no data is passed in tblData.
        """
        excluded_fields = set(excluded_fields) if isinstance(execute_fields, (list, set, tuple)) else set()
        args = [j for j in args if isinstance(j, DataTable)]
        tblRA = next((j for j in args if j.tblName == self._tblRAName), DataTable(self._tblRAName))
        tblLink = next((j for j in args if j.tblName == self._tblLinkName), DataTable(self._tblLinkName))
        tblData = next((j for j in args if j.tblName == self._tblDataName), DataTable(self._tblDataName, **kwargs))

        # Support for "short" form: if no records are provided in tblData, sets ONLY the query_field in tblData.
        if not tblData.dataLen:
            if val and self.__qryFldName != '*':
                tblData.setVal(0, **{self.__qryFldName: val})
            else:
                return False        # No data provided for all fields ('*') -> Exits with error, without writing to db.

        if self._isValid:  # and self.outerObject.validateActivity(self.__activityName):
            # Priority: 1: fldDate(tblData); 2: kwargs(valid datetime, with key containing 'date'); 3: defaultVal.
            defaultVal = val if self.__qryFldName == 'fldDate' and isinstance(val, datetime) else time_mt('dt')
            eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), defaultVal=defaultVal, **kwargs)
            tblRA.setVal(0, fldFK_NombreActividad=self._activityID)
            tblLink.setVal(0, fldFK=self.outerObject.ID)
            tblData.setVal(0, fldDate=eventDate)
            # if self.outerObject.supportsMemData:  # parametros para actualizar variables en memoria se pasan en
            #     tblData.setVal(0, fldMemData=1)  # Activa flag Memory Data en el registro que se escribe en DB
            idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)
            retValue = idActividadRA

            # TODO: Verificar si hay hay que cerrar alguna(s) PA con con esta actividad como closing Activity.
            # if self._supportsPA:
            if self._supportsPA:
                executeFields = self.activityExecuteFields(execution_date=tblData.getVal(0, 'fldDate'))
                if isinstance(execute_fields, dict):
                    executeFields.update(execute_fields)      # execute_fields adicionales, si se pasan.
                self._paMatchAndClose(idActividadRA, execute_fields=executeFields, excluded_fields=excluded_fields,
                                        force_close=pa_force_close)
                # Updates cols in tblLink, so that other nodes can access Execute Data, Excluded Fields.
                fldID_Link = tblLink.getVal(0, 'fldID')
                if fldID_Link:
                    setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
                              fldExcludedFields=excluded_fields)
        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - {callerFunction(getCallers=True)}'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}  - {retValue}', dismiss_print=DISMISS_PRINT)
        return retValue

    def get(self, *args: DataTable, sDate='', eDate='', **kwargs):
        """
        Returns values for qryFldName in tblData between sValue and eValue.
        sValue = eValue = '' -> Value from last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @return: value or list of values corresponding to qryFldName.
        """
        args = set([j for j in args if isinstance(j, DataTable)])
        retValue = None
        tblRA = setupArgs(self._tblRAName, *args)  # __tblRAName viene de class Activity
        tblLink = setupArgs(self._tblLinkName, *args)  # __tblLinkName viene de class Activity
        tblData = setupArgs(self._tblDataName, *args, **kwargs)  # __tblDataName viene de class Activity
        if self.__qryFldName not in tblData.fldNames + ['*', ]:
            self.__qryFldName = '*'         # chequeo basico de qryFldName. Si no es valido, setea a '*'.
        tblLink.setVal(0, fldFK=self.outerObject.ID, fldFK_Actividad=self._activityID)
        qryTable = self._getRecordsLinkTables(tblRA, tblLink, tblData, **kwargs)
        if isinstance(qryTable, DataTable):
            if qryTable.dataLen <= 1:
                result = qryTable
            else:
                result = qryTable.getIntervalRecords(self.__filterFldname, sDate, eDate, 1)  # mode=1: Date field.
            if not result.dataLen:
                retValue = {}
            elif result.dataLen == 1:
                retValue = result.getVal(0, self.__qryFldName, None)    # 1 Field: Value, multiple fields: dict
            else:
                if self.__qryFldName == '*':
                    retValue = [result.unpackItem(j) for j in range(result.dataLen)]       # List of dicts
                else:
                    retValue = result.getCol(self.__qryFldName)                            # List of values
        return retValue


    def getTblNames(self):
        """Returns _tblRAName, _tblLinkName, _tblDataName in that order """
        return self._tblRAName, self._tblLinkName, self._tblDataName

    def register_method(self, method_obj=None, *, method_name=None):
        """
        Registers method_obj (an external function) as a method in GenericActivity class. After registration, method_obj
        can be accessed by its name as a regular attribute of GenericActivity.
        *** IMPORTANT: method_obj MUST define self as its first positional argument. Ex: my_func(self, *args, **kwargs).
        Also IMPORTANT: if method_obj is already defined, any new call to register_method() with the same method_obj
        will OVERRIDE the existing attribute. This is by design to allow the implementation of updates to all registered
        attributes.
        @param method_name: When provided, uses name to set the attribute. Needed for overriding already defined
        methods (set, get).
        @param method_obj: callable (a function name)
        @return: True if registration successful. None if otherwise.
        """
        if not callable(method_obj):
            return None
        if method_name and isinstance(method_name, str):
            name = method_name
        else:
            name = method_obj.__name__
        setattr(self, name, self.methods_wrapper(method_obj))  # sets decorated method as the callable.
        return True


    def methods_wrapper(self, func):
        """
        Used to pass self (a GenericActivity instance) to func so that self, self.outerObject are available to func.
        This wrapper is designed mainly to incorporate in the class functions defined externally to GenericActivity.
        @param func: wrapped function.
        *** IMPORTANT: func MUST define self as its first positional argument. Ex: my_func(self, *args, **kwargs). ***
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(self, *args, **kwargs)
        return wrapper

    def unregister_method(self, method_name: str):
        """
        Deletes GenericActivity attribute method_name if it exists and if it's not a reserved name.
        @param method_name: callable. Class method to delete.
        @return: None
        """
        if hasattr(self, method_name) and callable(getattr(self, method_name)):
            if method_name in self.__reserved_names:
                return None             # ignores reserved method names
            delattr(self, method_name)
        return None


    def _tact_set(self, func):
        """ Ad hoc implementation of set() for Tact Activity. Uses a wrapper (decorator) function to implement the
        override of functions already existing in GenericActivity class.
        TODO: For now, these instance-specific functions go here for ease of implementation. Later, they can be placed
        elsewhere and be passed to __init__() for the initialization of the respective objects.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if 'f' not in self.outerObject.mf:
                return f'ERR_INP_Invalid object for tact Activity.'   # Since it's Tact, exits if Animal is not female.
            ret = func(self, *args, **kwargs)
            if isinstance(ret, int) and bool(kwargs.get('val')) and \
                    'vaca' not in self.outerObject.category.get(id_type='name').lower():
                # Updates Animal Category if tact result is positive and category for outerObject is not yet 'vaca'.
                self.outerObject.category.set(category='vaca')
            return ret
        return wrapper

# ---------------------------------------------- End Generic Activity ----------------------------------------------- #



@singleton
class InventoryActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadInventario'
    __activityName = 'Inventario'
    _short_name = 'invent'        # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    # TODO: __method_name could be read from DB. Already in [Actividades Nombres]. Does it make sense?
    __method_name = 'inventory'  # Used in ActivityMethod to create InventoryActivity object and the callable property.
    # Params Used in default memory_data functions and class, in Activity.
    _mem_data_params = {'field_names': ('fldDate', 'fldFK_Actividad', 'fldDaysToTimeout'), 'inventory': True}
    # Local to InventoryActivity. Initialized in EntityObject. Must be dunder because class must be singled-out.
    __local_active_uids_dict = {}    # {object_class: {uid: MemoryData object, }, }.
    _slock_activity_mem_data = AccessSerializer()  # Used to manage concurrent access to memory, per-Activity basis.
    __dict_updating = False          # Flag to manage concurrent access (by different threads) to local dictionary.

    # def __call__(self, caller=None, *args, **kwargs):
    #     self._caller = caller
    #     return self

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True     # Inventory does support PA.
        kwargs['decorator'] = self.__method_name
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)

    def set(self, *args: DataTable, execute_fields=None, excluded_fields=(), **kwargs):
        """
        Inserts Inventory obj_data for the object "obj" in the DB.
         @param execute_fields: dict. Data related to the execution of the Activity
        @param excluded_fields: tuple. List of fields to exclude from comparison.
        @param args: list of DataTable objects, with all the tables and fields to be written to DB.
                No checks performed. The function doing the write will discard and ignore all non valid arguments
        @param kwargs:  'date': eventDate (dates passed through DataTable take precedence over this setting via kwargs)
                        Other kwargs arguments are passed to [Data Animales Actividad Inventario] table
                       operatorsDict: Dictionary {fldName: operator, } to use in match() method to search for
                       Programmed Activities matching this Activity.
        @return: 0->ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        # Must add tblLink if not passed, needed to update tblLink record with fldExecuteData, fldExcludedFields.
        dfRA = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblRAName), None)
        dfLink = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblLinkName), None)
        dfData = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self.tblDataName()), None)
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

        retValue = self._setInventory(*args, execute_fields, excluded_fields, **kwargs)

        # if isinstance(retValue, int):
        #     if self._supportsPA:
        #         excluded_fields = set(excluded_fields) if isinstance(excluded_fields, (list, set, tuple, dict)) else set()
        #         excluded_fields.update(self.getActivityExcludedFieldsClose(self.__activityName))
        #         executeFields = self.activityExecuteFields(execution_date=self.outerObject.inventory.get())
        #         if execute_fields and isinstance(execute_fields, dict):
        #             executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
        #         self._paMatchAndClose(retValue, execute_fields=executeFields, excluded_fields=excluded_fields)
        #         # Updates cols in tblLink, so that external nodes can access Execute Data, Excluded Fields.
        #         fldID_Link = tblLink.getVal(0, 'fldID')    # Uses fact that tblLink data is updated by _setInventory().
        #         if fldID_Link:
        #             setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
        #                       fldExcludedFields=excluded_fields)

        # TODO: For now, this one doesn't trigger creation of PA instances in tblLinkPA. We'll see later...
        return retValue

    def get(self, *args, mode='mem', full_record=False, uid=None, all_records=None, **kwargs):
        """
        Returns Inventory value (default), dictionary or DataFrame, based on parameter selection.
        @param mode: str. memory | value: Pulls data from memory or from database.
        @param uid: uid for which to run the query. If not passed, uses self.outerObject.
        @param full_record: Returns a dictionary. Last database record, based on value of fldDate field.
        @param all_records: Returns full pandas DataFrame with all rows pulled from the query.
        @return: datetime | dict | DataFrame. None if no data found.
        """
        return self._getInventory(*args, mode=mode, full_record=full_record, uid=uid, all_records=all_records, **kwargs)

    def check_timeout(self, *, uid=None):     # TODO(cmt): Uses only memory data. Called by an InventoryActivity object.
        """ Checks for object timeout
        @return: True if time elapsed since last inventory exceeds object's days_to_timeout value.
        """
        return self._check_timeout(uid=uid)


    @classmethod                        # InventoryActivity.
    def _memory_data_init_last_rec(cls, obj_class, active_uids: set, *, max_col=None, **kwargs):
        #                **** TODO(cmt): This function is designed to be run in its own thread! ****
        """ Initializer for memory data that keeps LAST available database RECORD (last inventory, last status, etc.).
        Overrides func in base class. Needed to be able to append fldDaysToTimeout data to inventory memory_data.
        Called from EntityObject __init_subclass__(). cls._memory_data_classes are initialized at this call.
        It reads all required data from database, creates MemoryData objects and initializes the local dictionary
        cls.__local_active_uids_dict = {obj_class: {uid: MemoryData obj, }, }
        Also copies the created dictionary to all parent classes that implement __local_active_uids_dict.
        This is required to operate memory_data logic consistently and maintain data integrity.
        Returns LAST values for fldNames passed (LAST meaning: db values associated with the last (highest) value of
        fldDate field in the data table, or the LAST value corresponding to fldName if fldDate is not defined for
        the table).
        @param obj_class: class to be used as key for __local_active_uids_dict.
        @return: None
        """
        uid_fld_name = "fldObjectUID"
        tbl_obj_fld_names = ('fldObjectUID', 'fldDaysToTimeout')
        tbl_obj = cls.tblObjName()
        local_uids_dict = getattr(cls, '_' + cls.__name__ + '__local_active_uids_dict', None)
        if local_uids_dict is not None:    # This is None for classes that don't support_mem_data(). If so, exits.
            if not isinstance(max_col, str):
                max_col = 'fldDate'  # Default column to pull max date.
            if obj_class not in local_uids_dict:
                local_uids_dict[obj_class] = pd.DataFrame([], columns=list(cls._mem_data_params['field_names']))

            # Check if any cls has _animalClassID attribute defined (Bovine, Caprine, etc. will have it).
            # Loads 1 dataframe at a time for: classes with _animalClassID == cls._animalClassID or for classes with
            # _animalClassID not defined (AnimalActivity, TagActivity, DeviceActivity classes).
            # not hasattr('_animalClassID') -> InventoryActivity, StatusActivity, etc. (not defined for specific
            # animal classes.
            if not hasattr(cls, '_animalClassID') or cls.animalClassID() == obj_class.animalClassID():
                # fldDaysToTimeout belongs to a different table. Must be dealt with separately (down below...)
                tblData_fields = getFldName(cls.tblDataName(), '*', mode=1)     # {fldName: dbFldName, }
                flds = [f for f in cls._mem_data_params['field_names'] if f in tblData_fields]
                # Now, get fldDaysToTimeout for active_uids.
                uid_db_fld_name = getFldName(tbl_obj, uid_fld_name)
                df_loaded = cls._load_memory_data_last_rec(tbl=cls.tblDataName(), keys=set(active_uids), flds=flds,
                                                           max_col=max_col)
                obj_fld_names_str = ", ".join([f'"{getFldName(tbl_obj, j)}"' for j in tbl_obj_fld_names])
                sql_obj = 'SELECT ' + obj_fld_names_str + f' FROM "{getTblName(tbl_obj)}" WHERE "{uid_db_fld_name}" ' \
                                                      f'IN {str(tuple(active_uids))}; '
                df_obj = pd.read_sql_query(sql_obj, SQLiteQuery().conn)  # df_obj cols: (fldObjectUID, fldDaysToTimeout)
                if df_obj.empty or df_loaded.empty:
                    return

                index = 'fldFK'  # Index to set on df_loaded.
                df_loaded.set_index(index, inplace=True)  # df_loaded comes from tblLink, hence 'fldFK'
                df_obj.set_index('fldObjectUID', inplace=True)  # df_obj comes from tblObject hence 'fldObjectUID'
                joined = df_loaded.join(df_obj)
                # Renames cols to use all names in _mem_data_params. Some names are changed by the sql_read decorator.
                dfcols = joined.columns.tolist()
                if any(cname not in cls._mem_data_params['field_names'] for cname in dfcols):
                    if len(dfcols) != len(cls._mem_data_params['field_names']):
                        raise AttributeError(f'ERR_Attribute: DataFrame in {cls.__name__} does not match Memory Data '
                                             f'template. Aborting Memory Data initialization for Activity.')
                    joined.rename(columns=dict(zip(joined.columns.to_list(), cls._mem_data_params['field_names'])),
                                  inplace=True)

                # Now resolves potential duplicate records in the dataframes picking the uids with the latest fldDate.
                df_obj_class = local_uids_dict[obj_class]
                if not df_obj_class.empty:
                    merged = pd.merge(df_obj_class, joined, how='outer')
                    # Keeps the record with highest fldDate (hopefully!)
                    local_uids_dict[obj_class] = merged.groupby(index)[max_col].max()
                else:
                    local_uids_dict[obj_class] = joined
                dicto = {"uid": local_uids_dict[obj_class].iloc[0].to_dict()}  # Printing Category mem_data.
                print(f'MEMORY DATA for {cls.__name__}: {len(local_uids_dict[obj_class])} uid items. Item form: '
                      f'{dicto}', dismiss_print=DISMISS_PRINT)
        return


@singleton
class StatusActivityAnimal(AnimalActivity):
    __instance = None           # Singleton: Unique instance defined for the class.
    __initialized = False       # To avoid multiple (possible incomplete) initialization of the same instance.
    __tblDataName = 'tblDataAnimalesActividadStatus'
    __statusPermittedDict = {'1': [2, 3, 4], '2': [1, 3, 4], '3': [1, 2, 4], '4': [4], '0': [1, 2, 3],'None': [1, 2, 3],
                             None: [1, 2, 3], 1: [2, 3, 4], 2: [1, 3, 4], 3: [1, 2, 4], 4: [4], 0: [1, 2, 3]}
    __activityName = 'Status'
    __method_name = 'status'
    _short_name = 'status'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    _slock_activity_mem_data = AccessSerializer()  # Used to manage concurrent access to memory, per-Activity basis.
    # Local to InventoryActivity. Initialized in EntityObject. Must be dunder because class must be single-out.

    # __local_active_uids_dict used by Activity.__init_subclass__(): creates dict of classes that implement mem_data
    __local_active_uids_dict = {}  # {uid: MemoryData object, }. Local to InventoryActivity. Initialized in EntityObject
    _mem_data_params = {'field_names': ('fldFK_Status', 'fldDate', 'fldFK_Actividad')}

    @classmethod
    def get_local_uids_dict(cls):
        return cls.__local_active_uids_dict


    def __new__(cls, *args, **kwargs):  # TODO(cmt): Right way to do singletons. Allows for isinstance(), type checks.
        if not cls.__instance:
            cls.__instance = super().__new__(cls)  # Crea Objeto (solo si no existia)
        return cls.__instance  # Retorna objeto al llamador

    def __init__(self, *args, **kwargs):
        if self.__initialized:
            return              # Si ya fue inicializado, sale.
        self.__initialized = True
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = False
        kwargs['decoratorName'] = self.__method_name
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)

    def permittedFrom(self):  # Lista de Status permitidos para Animal, a partir de a status inicial(From)
        return self.__statusPermittedDict


    def set(self, *args: DataTable, status=None, execute_fields=None, excluded_fields=(), **kwargs):
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
        dfRA = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblRAName), None)
        dfLink = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblLinkName), None)
        dfData = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self.tblDataName()), None)
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

        retValue = self._setStatus(*args, status=status, **kwargs)


        # if isinstance(retValue, int):        # it don't make any sense to program status changes (at least for now)
        #     if self._supportsPA:
        #         excluded_fields = set(excluded_fields) if isinstance(excluded_fields, (list, set, tuple, dict)) else set()
        #         execute_date = self.outerObject._lastStatus[1]  # Gets the internal execution date for lastInventory.
        #         if isinstance(retValue, int) and self._supportsPA:
        #             executeFields = self.activityExecuteFields(execution_date=execute_date,
        #                                                      status=self.outerObject.lastStatus)
        #             if execute_fields and isinstance(execute_fields, dict):
        #                 executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
        #             self._paMatchAndClose(retValue, execute_fields=executeFields, excluded_fields=excluded_fields,
        #                                   **kwargs)  # TODO(cmt): This call is executed asynchronously by another thread
        #             # Updates record in tblLink, so that external nodes can access Execute Data, Excluded Fields.
        #             fldID_Link = tblLink.getVal(0, 'fldID')  # tblLink argument is updated by _setStatus().
        #             if fldID_Link:
        #                 setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
        #                           fldExcludedFields=excluded_fields)
        return retValue


    def get(self, *args, mode='mem', full_record=False, uid=None, all_records=None, **kwargs):
        """
        Returns Status value (default), dictionary or DataFrame, based on parameter selection.
        @param mode: str. memory | value: Pulls data from memory or from database.
        @param uid: uid for which to run the query. If not passed, uses self.outerObject.
        @param full_record: Returns a dictionary. Last database record, based on value of fldDate field.
        @param all_records: Returns full pandas DataFrame with all rows pulled from the query.
        @return: datetime | dict | DataFrame. None if no data found.
        """
        return self._getStatus(*args, mode=mode, full_record=full_record, uid=uid, all_records=all_records, **kwargs)


    def comp(self, val):
        try:
            return self.outerObject.lastStatus == val
        except(TypeError, ValueError):
            return False


@singleton
class LocalizationActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadLocalizacion'
    __activityName = 'Localizacion'
    __method_name = 'localization'
    _short_name = 'locali'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    # __local_active_uids_dict used by Activity.__init_subclass__(): creates dict of classes that implement mem_data
    # __local_active_uids_dict to return _support_mem_data status (True/False)
    __local_active_uids_dict = {}  # pd.DataFrame. Local to InventoryActivity. Initialized in EntityObject
    _mem_data_params = {'field_names': ('fldFK_Localizacion', 'fldDate', 'fldFK_Actividad')}
    _slock_activity_mem_data = AccessSerializer()  # Used to manage concurrent access to memory, per-Activity basis.


    def __init__(self, *args, **kwargs):
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = False
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def set(self, *args: DataTable, localization: Geo = None, execute_fields=None, excluded_fields=None, **kwargs):
        """
        Inserts LocalizationActivityAnimal obj_data for the object "obj" in the DB.
        __outerAttr: TagObject para la que se realiza la Actividad. Lo setea el metodo inventario().
        @param localization: Geo object to set LocalizationActivity in short form (without passing full DataFrames)
        @param args: list of DataTable objects, with all the tables and fields to be written to DB.
                No checks performed. The function doing the write will discard and ignore all non valid arguments
        @param kwargs: recordInventory: 0->Do NOT call _setInventory() / 1: Call _setInventory(),
                        idLocalization = valid LocalizationActivityAnimal val, from table GeoEntidades
        @return: 0->ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        dfRA = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblRAName), None)
        dfLink = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblLinkName), None)
        dfData = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self.tblDataName()), None)
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

        kwargs['localization'] = localization
        retValue = self._setLocalization(*args, execute_fields, excluded_fields, **kwargs)

        return retValue

    def get(self, *args, mode='mem', full_record=False, uid=None, all_records=None, **kwargs):
        """
       Returns records in table Data LocalizationActivityAnimal.
       Returns last Localization value (default), dictionary or DataFrame, based on parameter selection.
        @param mode: str. memory | value: Pulls data from memory or from database.
        @param uid: uid for which to run the query. If not passed, uses self.outerObject.
        @param full_record: Returns a dictionary. Last database record, based on value of fldDate field.
        @param all_records: Returns full pandas DataFrame with all rows pulled from the query.
        @return: last Localization | dict | DataFrame. None if no data is found.
        """
        retValue = self._getLocalization(*args, mode=mode, full_record=full_record, uid=uid, all_records=all_records,
                                         **kwargs)
        return retValue

    def comp(self, val):
        """Returns True if outer_obj within self is contained (geographically) in val"""
        if isinstance(val, str):
            val = Geo.getObject(val)
        if isinstance(val, Geo):
            return self.get().contained_in(val)
        return False


@singleton
class CastrationActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadCastracion'
    __activityName = 'Castracion'
    __method_name = 'castration'
    _short_name = 'castra'  # Activity short name. 6 chars. Used by Activity._paCreate(), Activity.__isClosingActivity()
    __one_time_activity = True

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    def __init__(self, *args, **kwargs):
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        # Flag to signal only-once activities (castracion, destete, salida, etc.)
        kwargs['one_time_activity'] = self.__one_time_activity
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def set(self, *args, date=None):
        """
        Sets castration date for Animal. Updates tables: Animales, Animales Registro De Actividades,
        Link Animales Actividades, Data Animales Castracion
        @param date: datetime. 0, False, None or '' -> Date not known (However castration is recorded as done).
        @return: flagCastrado (int) or ERR_ (string)
        """
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self.tblDataName(), default=None)
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
        eventDate = date
        time_now = time_mt('dt')
        if eventDate:
            if not isinstance(eventDate, (int, float)):
                # Discards dates in the future for executed activities (ProgActivities are another class Activity objs.)
                eventDate = min(date.timestamp() if isinstance(eventDate, datetime) else 1, time_now)
        else:
            eventDate = None        # Castration date not passed. Still Castration is recorded as done.

        outer_obj = self.outerObject
        outer_obj.isCastrated = True        # TODO(cmt): eventDate es variable local, para aceptar re-entry.
        dfObjects = pd.DataFrame.db.create(self.tblObjName())
        dfObjects.loc[0, ('fldFlagCastrado', 'fldFK_UserID', 'fldID')] = (1, sessionActiveUser, outer_obj.recordID)
        _ = setRecord(dfObjects.db.tbl_name, **dfObjects.iloc[0].to_dict())  # Setea _flagCastrado en tblAnimales

        dfData.loc[0, ('fldDate', 'fldFlagCastrado')] = (eventDate, 1)
        activityID = AnimalActivity.getActivitiesDict().get(self.__activityName)
        timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if not dfRA.empty else time_now, time_now)
        dfRA.loc[0, ('fldFK_NombreActividad', 'fldTimeStamp')] = (activityID, timeStamp)
        _ = self._createActivityRA(dfRA, dfLink, dfData)
        if isinstance(_, str):
            krnl_logger.error(f'ERR_DBAccess. Cannot read from tbl {dfData.db.tbl_name}. Error: {_}')
            return _

        # Sets new category based on castration and animal age.
        if outer_obj.age.get() <= self.outerObject._myActivityClass.ageLimit('novillito'):
            outer_obj.category.set(category='novillito')
        else:
            outer_obj.category.set(category='novillo')

        # if self.doInventory():
        #     _ = self.outerObject.inventory.set(tblRA, tblLink, date=max(timeStamp.timestamp(), eventDate))

        return outer_obj._fldFlagCastrado



    def get(self):
        return self.outerObject._fldFlagCastrado


@singleton
class AgeActivityAnimal(AnimalActivity):
    """ Age class is defined in order to standardize the logic in ProgActivities, using age as a condition in the same
     way as other Activities that are also conditions """
    _implements__call__ = True      # Flags that Activity implements __call__(), hence it's callable.
    __tblDataName = 'tblAnimales'
    __activityName = 'Edad'
    __method_name = 'age'
    _short_name = 'age'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    def __init__(self, *args, **kwargs):
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = False
        kwargs['decoratorName'] = self.__method_name
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)

    # Cannot implement this __call__ if __call__() is defined in Parent Class
    # def __call__(self, *args, **kwargs):                   # With this, can call as: animalObj.age()
    #     return self.outerObject._age(**kwargs)

    def get(self, **kwargs):                        # Defined for compatibility of syntax with other Activities.
        return self.outerObject._age(**kwargs)      # With this, can call as: animalObj.age.get()

    def comp(self, data_list: list, *, exclusive=False):
        """
        @param data_list: data_list[0]:reference_age; data_list[1]:lowDevi; data_list[2]:hiDevi
        if lowDevi or highDevi == -1, comparison to be made is age() > or >= val, with no lowDevi or hiDevi values.
        @param exclusive: True: use <, >.   False: use <=, >=.
        @return:
        """
        return compare_range(self.outerObject.age.get(), *data_list, exclusive=exclusive)


@singleton
class MeasurementActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadMedicion'
    __activityName = 'Medicion'
    __method_name = 'measurement'
    _short_name = 'measur'     # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    temp = getRecords('tblAnimalesMedicionesNombres', '', '', None, '*')
    if not isinstance(temp, DataTable):
        raise (DBAccessError, 'ERR_DBAccess: cannot read table [Animales Mediciones Nombres]. Exiting.')
    _measurementsDict = {}   # {fldName: ID_Nombre Medicion, }
    for j in range(temp.dataLen):
        d = temp.unpackItem(j)
        _measurementsDict[d['fldName']] = d['fldID']
    _measurementsDict_lower = {k.lower(): v for k, v in _measurementsDict.items()}

    def __init__(self, *args, **kwargs):
        isValid = True
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

        self.__tblRA = setupArgs(self._tblRAName, *args)
        self.__tblLink = setupArgs(self._tblLinkName, *args)
        self.__tblData = setupArgs(self.__tblDataName, *args, **kwargs)


    # @Activity.activity_tasks(after=(Activity.doInventory, Activity.paScheduler))
    def set(self, *args: pd.DataFrame, meas_name: str = '', meas_value=None, meas_units: str = '', excluded_fields=None,
            execute_fields=None, **kwargs):         # execute_fields passed directly to decorator @activity_tasks()
        """
        Creates a record in [Data Animales Actividad Mediciones]. Creates records in [Animales Registro De Actividades]
        and [Link Animales Actividades] if required.
        @param meas_name: Measurement (str). Temperature, Weight, etc. Must be a valid name.
        @param meas_value: Measurement value (BLOB).
        @param meas_units: Measurement unit (str). Must be a valid unit name (Kilogramo, Libra) or acronym (kg, lb)
        @param args: DataTables.
        @param kwargs: Arguments passed to [Data Animales Actividad MoneyActivity] table.
        @return: idActividadRA: Success / errorCode (str) on error
        """
        meas_name = meas_name.lower() if isinstance(meas_name, str) else ''
        meas_units = meas_units.lower() if isinstance(meas_units, str) else ''
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self.tblDataName(), default='dataframe')
        args = list(args)
        args.append(dfRA)
        args.append(dfLink)
        args.append(dfData)

        m_id = dfData.loc[0, 'fldFK_NombreMedicion'] if not dfData.empty else self._measurementsDict_lower.get(meas_name)
        try:
            m_unit_id = dfData.loc[0, 'fldFK_Unidad'] if not dfData.empty else \
                (self._unitsDict_lower.get(meas_units)[0] if meas_units in self._unitsDict_lower else
                 next((self._unitsDict_lower.get(k)[0] for k in self._unitsDict_lower if meas_units in
                       self._unitsDict_lower.get(k)[1]), None))
        except IndexError:
            m_unit_id = None
        m_value = dfData.loc[0, 'fldValue'] if not dfData.empty else meas_value
        if m_unit_id not in [j[0] for j in list(self._unitsDict.values())]:
            return f'ERR_INP_Argument(s): Invalid or missing units {meas_units}.'
        if m_id not in self._measurementsDict.values():
            return f'ERR_INP_Argument(s): Invalid or missing measurement name {meas_name}({m_id}).'
        if m_value is None or m_value == VOID:          # 0, '' are valid measurement values.
            return f'ERR_INP_Argument(s): Measurement value missing.'

        if dfData.empty:
            dfData.loc[0, ('fldFK_NombreMedicion',  'fldValue', 'fldFK_Unidad')] = (m_id, m_value, m_unit_id)
        dfRA.loc[0, 'fldFK_NombreActividad'] = self._activityID
        dfLink.loc[0, 'fldExcludedFields'] = excluded_fields
        retValue = self._createActivityRA(dfRA, dfLink, dfData)
        if isinstance(retValue, str):
            krnl_logger.error(f'ERR_DBAccess. Cannot read from tbl {dfData.db.tbl_name}. Error: {retValue}')
            return retValue

        # Updates kwargs for paScheduler() execution.
        dfData.loc[0, 'fldFK_NombreMedicion'] = m_id
        dfRA.loc[0, 'fldID'] = retValue
        kwargs.update({'idActividadRA': retValue, 'm_id': m_id})


        # if self._supportsPA:
        #     excluded_fields = set(excluded_fields) if isinstance(excluded_fields, (list, set, tuple, dict)) else set()
        #     excluded_fields.update(self.getActivityExcludedFieldsClose(self._activityName))
        #     executeFields = self.activityExecuteFields(execution_date=tblData.getVal(0, 'fldDate'),
        #                                              fldFK_NombreMedicion=m_id)
        #     if execute_fields and isinstance(execute_fields, dict):
        #         executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
        #     self._paMatchAndClose(retValue, execute_fields=executeFields, excluded_fields=excluded_fields)
        #     # Updates fldExecuteData, fldExcludedFields in tblLink so that this data is replicated to other nodes.
        #     fldID_Link = tblLink.getVal(0, 'fldID')  # Uses fact that tblLink arg. is updated by _createActivityRA().
        #     if fldID_Link:
        #         setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
        #                   fldExcludedFields=excluded_fields)

        # if self.doInventory(**kwargs):
        #     _ = self.outerObject.inventory.set()

        return retValue


    def get(self, *args, mode='mem', full_record=False, uid=None, all_records=None, meas_name: str = '', **kwargs):
        """
        Returns data from table [Data Mediciones] between sValue and eValue. sValue=eValue ='' -> Last record
        @param sDate: No args -> Last Record;  sDate=eDate='' -> Last Record;
                        sDate='0' OR eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param meas_name: Measurement (str). Temperature, Weight, etc. Must be a valid name.
        @param args: tblDataAnimalesActividadMedicion can be provided. Optional.
        @param kwargs: mode=datatable->Returns DataTable with full record.  mode='fullRecord' -> Returns last Record
        in full.
        @return: {'name': meas_name, 'value': mease_val, 'units': meas_units} for measurement. meas_name, meas_units are
        strings ("temperatura", "degc", "peso", "ton", "kg", etc.).
        DataFrame object with 1 or more records.
        {} if nothing's found.
        """
        retValue = {}
        meas_name = meas_name.lower() if isinstance(meas_name, str) else ''
        tblData = next((j for j in args if isinstance(j, DataTable) and j.tblName == self.__tblDataName),
                       DataTable(self.__tblDataName, **kwargs))
        m_id = tblData.getVal(0, 'fldFK_NombreMedicion') if tblData.dataLen else self._measurementsDict_lower.get(meas_name)
        if m_id not in self._measurementsDict.values():
            return f'ERR_INP_Argument(s): Invalid or missing measurement name {m_id}.'
        if self._isValid and self.outerObject.validateActivity(self._activityName):
            # qryTable = getRecords(self.__tblDataName, sDate, eDate, None, '*', fldFK_NombreMedicion=m_id)
            query_df = getrecords(self.__tblDataName, '*',
                                  where_str=f'WHERE "{getFldName(self.__tblDataName, "fldFK_NombreMedicion")}"=={m_id}')
            if not query_df.empty:
                return query_df
                # filteredTable = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
                # if filteredTable.dataLen <= 1:   # qryTable tiene 1 solo registro (Ultimo o Primero)
                #     retValue = {'name': next((k for k in self._measurementsDict if self._measurementsDict[k] ==
                #                               filteredTable.getVal(0, 'fldFK_NombreMedicion')), None),
                #                 'value': filteredTable.getVal(0, 'fldValue', VOID),
                #                 'units': next((k for k in self._unitsDict if self._unitsDict[k][0] ==
                #                                filteredTable.getVal(0, 'fldFK_Unidad')), None)
                #                 }
                #     print(f'{moduleName()}({lineNum()}) retValue: {retValue}', dismiss_print=DISMISS_PRINT)
                # else:
                #     # Returns DataTable with multiple records.
                #     retValue = filteredTable
        return retValue


    def comp(self, meas_name: str, meas_unit: str, meas_value=None):  # Comp works with single values for now (27Sep23).
        lastVal = self.get(meas_name=meas_name)
        return bool(lastVal and lastVal.get('name').lower() == str(meas_name).lower() and
                    lastVal.get('units').lower() == str(meas_unit).lower() and
                    lastVal.get('value') == meas_value)


@singleton
class ParturitionActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadParicion'
    __activityName = 'Paricion'
    __method_name = 'parturition'
    _short_name = 'partur'  # Activity short name. 6 chars. Used by Activity._paCreate(), Activity.__isClosingActivity()
    __one_time_activity = False

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    def __init__(self, *args, **kwargs):
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = False        # En principio, no se puede programar una paricion. Cesarea si.
        kwargs['decorator'] = self.__method_name
        # Flag to signal only-once activities (castracion, destete, salida, etc.)
        kwargs['one_time_activity'] = self.__one_time_activity
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    def set(self, *args, event_date=None, parturition_status=1, execute_fields=None, excluded_fields=()):
        """
        Sets parturition date for Animal. Updates tables: Animales, Animales Registro De Actividades,
        Link Animales Actividades, Data Animales Actividad Paricion
        @param parturition_status: int
            0: Malparturition, stillborn, abortion.
            1: Born alive. Normal delivery.
            2: Born alive. C-section.
        @param event_date: datetime
        @return: idActivityRA of created activity (int) or error code (str)
        """
        if 'f' not in self.outerObject.mf:
            return f'ERR_INP_Invalid object for parturition.'   # Exits if Animal is not female.
        if not event_date or not isinstance(event_date, datetime):
            event_date = None   # Si no se pasa event_date valido, deja que lo setee _createActivityRA()

        # tblObjects = DataTable(self.tblObjName())
        # tblRA = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblRAName), None)
        # tblLink = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblLinkName), None)
        # tblData = next((j for j in args if isinstance(j, DataTable) and j.tblName == self.tblDataName()), None)
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self.tblDataName(), default='dataframe')
        args = list(args)
        args.append(dfRA)
        args.append(dfLink)
        args.append(dfData)

        dfData.loc[0, ('fldDate', 'fldFK_ResultadoDeParicion')] = (event_date, parturition_status)
        c_section = (parturition_status == 2)
        # activityID = AnimalActivity.getActivitiesDict().get(self.__activityName)
        dfRA.loc[0, 'fldFK_NombreActividad'] = self._activityID
        retValue = self._createActivityRA(dfRA, dfLink, dfData)
        if isinstance(retValue, str):
            krnl_logger.error(f'ERR_DBAccess. Cannot read from tbl {dfData.db.tbl_name}. Error: {retValue}')
            return retValue
        # Sets new category if this was the animal's 1st parturition.
        if 'vaca' not in self.outerObject.category.get(id_type='name').lower():
            self.outerObject.category.set(category='vaca')

        if isinstance(retValue, int) and c_section:     # c-sections can be a ProgActivity. Normal delivery can't.
            excluded_fields = set(excluded_fields) if isinstance(excluded_fields, (list, set, tuple, dict)) else set()
            excluded_fields.update(self.getActivityExcludedFieldsClose(self.__activityName))
            executeFields = self.activityExecuteFields(execution_date=self.outerObject.inventory.get())
            if execute_fields and isinstance(execute_fields, dict):
                executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
            self._paMatchAndClose(retValue, execute_fields=executeFields, excluded_fields=excluded_fields)
            # Updates cols in tblLink, so that external nodes can access Execute Data, Excluded Fields.
            fldID_Link = dfLink.loc[0, 'fldID']   # Uses fact that tblLink argument is updated by _setInventory().
            if fldID_Link:
                setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
                          fldExcludedFields=excluded_fields)
        return retValue

    # TODO: AMMEND alta() including mother data to incorporate parturition/birth activities.

    def get(self):
        """ Return a DataTable with all parturition records for the animal (Stored in the Multiple db table). Each
        record keeps the full data of each parturition for the Animal.
         @return: DataTable with parturition data. Empty DataTable if no parturitions recorded for animal.
         Error string if error in reading from DB.
         """
        return self._get_link_records(activity_list=(self._activityID, ))


@singleton
class TMActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadTM'
    __tblMontosName = 'tblDataTMMontos'
    __activityName = 'Actividad MoneyActivity'
    __method_name = 'tm'
    _short_name = 'tm'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    def __init__(self, *args, **kwargs):
        isValid = True
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    # @Activity.activity_tasks(after=(Activity.doInventory, Activity.paScheduler))
    def set(self, *args: DataTable, **kwargs):
        """
        Inserts record "idActividadTM" in [Data Animales Actividad MoneyActivity]. Creates records in
        [Animales Registro De Actividades] and [Link Animales Actividades] if required.
        @param idActividadTM: val to insert in table [Data Animales Actividad MoneyActivity], in field fldFK_ActividadTM
        @param idActividadRA: val to insert in table [Data Animales Actividad MoneyActivity], in field fldFK_Actividad
        @param args: DataTables. Only [Animales Registro De Actividades] and [Data Animales Actividad MoneyActivity] are parsed
        @param kwargs: Arguments passed to [Data Animales Actividad MoneyActivity] table.
        @return: idActividadRA: Success / errorCode (str) on error
        """
        dfRA, dfLink, dfData = self.set3frames(dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self.tblDataName(), default='dataframe')
        args = list(args)
        args.append(dfRA)
        args.append(dfLink)
        args.append(dfData)
        return self._setTM(*args, **kwargs)


    def get(self, *args, full_record=False, uid=None, all_records=None, **kwargs):
        """
        Returns records in table [Data MoneyActivity Montos] between sValue and eValue. sValue=eValue ='' -> Last record
        @param kwargs: mode=datatable->Returns DataTable with full record.  mode='fullRecord' -> Returns last Record
        in full.
        @return: DataFrame with multiple records for uid | dictionary with latest record, with information
        from MoneyActivity Montos | {} -> Nothing found.
        """
        retValue = None
        if self._isValid and self.outerObject.validateActivity(self.__activityName):
            dfRA, dfLink, dfData = self.set3frames(dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                                   dfData_name=self.tblDataName(), default='dataframe')
            args = list(args)
            args.append(dfRA)
            args.append(dfLink)
            args.append(dfData)
            tmActivityRecords = self._get_link_records(uid=uid)
            colAnimalActivity = tmActivityRecords['fldFK_ActividadTM'].tolist()  # Recs c/ ID_ActividadTM de ID_Animal
            if len(colAnimalActivity) > 0:
                colAniActivity_str = str(tuple(colAnimalActivity)) if len(colAnimalActivity) > 1 \
                    else str(tuple(colAnimalActivity)).replace(',', '')
                query_df = getrecords(self.__tblMontosName, '*',
                    where_str=f'WHERE "{getFldName(self.__tblMontosName,"fldFK_Actividad")}" IN {colAniActivity_str}')
                if not query_df.empty:
                    if all_records:
                        return query_df
                    else:
                        latest_rec = query_df.loc[query_df['fldDate'] == query_df['fldDate'].max()].iloc[0]
                        return latest_rec
            else:
                retValue = {}     # Retorna DataTable blank (sin datos)
        return retValue

    # def comp(self, val):
    #     pass


@singleton
class TagActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadCaravanas'
    __tblObjectsName = 'tblCaravanas'
    __tblObjectDBName = getTblName(__tblObjectsName)
    __activityName = 'Caravaneo'
    __method_name = 'tags'
    _short_name = 'carava'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    def __init__(self, *args, **kwargs):
        isValid = True
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def initializeTags(self, *args, **kwargs):      # NEW: 'per individual tag' tag creation.
        """
        Assigns all active tags to ID_Animal (self), pulling the tags (previously assigned to the Animal)
        from [Data Animales Actividades Caravanas]. If tags are not found, leaves myTags as an empty set.
        args: DataFrame with info needed to process Tags (1 row per tag). Passed as args to minimize DB reads.
        @return: True if all OK; None: DB read error; False if no tags assigned (TODO: Action must be taken by UI!)
        """
        if not self.outerObject.validateActivity(self._activityName):
            retValue = f'ERR_SYS: ActivityNotDefined - {self.__activityName}.'
            krnl_logger(f'{retValue}')
            return None
        tag_uids = self.outerObject.getIdentifiers()  # list of tag UIDs in __identifiers attr. for Animal, Device, etc.
        objList = []

        # Gets a TagAnimal class to access the proper tagTechDicitionary and pull the Tag class from there.
        tag_animal_class = self.outerObject._myTagClass()  # tag_animal_class is TagBovine, TagCaprine, etc.
        for i in tag_uids:
            obj = tag_animal_class.getObject(i)   # obj is type TagStandardBovine, TagStandardCaprine, TagRFIDMachine.
            if isinstance(obj, tag_animal_class):
                objList.append(obj)         # TODO: VER DE SETEAR Tag Status aqui.
        if objList:
            self.outerObject.setMyTags(*objList)
            # print(f'...Now setting tags: {self.outerObject.myTagIDs}')
            return True
        return False                # No tags found. UI must inform the user that the Animal is being used with no tags!



    # Animal: Caravaneo / Caravanas: Comision, Reemplazo, Reemision
    @Activity.activity_tasks(after=(Activity.doInventory,))
    def assignTags(self, *args: pd.DataFrame, comm_type='Comision', tag_list=None, **kwargs):
        # 04Aug24: Implements use of obj_dataframe() as an iterator. Fixes bugs.
        """
        Assigns Tags in *args to Animal. Updates tables in DB. This is a physical assignment of tags (PHATT) activity.
        Commissions all tags via the Tag.commission() method, which sets the Physically-Attached attribute for the tag.
        @param tag_list: iterable with Tag objects or tag uids (str) to assign to Animal (list | tuple)
        @param comm_type: 'Comision'(Default), 'Reemplazo', 'Reemision'. sets the Activity for Tag (str)
        @param args: DataTables to use in operation (tblRA for idActividadRA, tblLink for fldIDLink, etc)
        @param kwargs: tags=[Tag, ]. List of Tag Objects to assign (1 or more)
               - set_tag_inventory: False -> Avois duplicating tag inventories (Tag Inventory is set by commission.set)
        @return: idActividad (int) or errorCode (str)
        """
        activityName = 'Caravaneo'
        if not self._isValid or not self.outerObject.validateActivity(activityName):
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotValid - {callerFunction()}.'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue

        # kwargs.update({'set_tag_inventory': False})   # Instructs _setInventory not to record inventory for tags. TODO: see if this works!!

        # tag_list may carry 3 flavors: Tag objects, Tag UIDs and plain numbers (str). The code below addressed them all
        tags = list(tag_list) if isinstance(tag_list, collections.abc.Iterable) else [tag_list, ]
        tag_objs = [t for t in tags if isinstance(t, Tag) and t.isAvailable]      # 1. List of Tag objects
        tag_uids = []
        tag_class = self.outerObject._myTagClass()  # Pulls the proper tag class for tag creation. (TagBovine, etc.)
        for t in tags.copy():
            if isinstance(t, str):
                try:
                    tag_uids.append(uuid.UUID(t).hex)           # Appends all tags with valid UUID format.
                except(TypeError, SyntaxError, AttributeError, ValueError):
                    continue
                else:
                    tags.remove(t)
        tag_numbers = [t for t in tags if isinstance(t, str)]  # Only strings left are numbers. UIDs were removed above.
        tag_identifs = [Tag.create_identifier(elements=(num, Tag._tagIdentifierChar, self.__class__.__name__))
                        for num in tag_numbers]     # Identifier: "123-Bovine", "777-Caprine", etc.

        # Split tag_identifiers between those that exist in node and those that don't.
        identifs_in_node = {}                               # identifs_in_node: {identifier_str: tag_uid}
        identifs_not_in_node = {}
        for identif in tag_identifs:
            for tags_df in tag_class.obj_dataframe():
                if identif in tags_df['fldIdentificadores']:
                    # TODO: check if this assignment is of the correct type.
                    identifs_in_node[identif] = \
                        tags_df.loc[tags_df['fldIdentificadores'] == identif, 'fldObjectUID'].tolist()[0]
                else:
                    identifs_not_in_node[identif] = None

        # Request confirmation of non-existing tags
        for identif in identifs_not_in_node:
            dicto = Tag.identifier_get_user_confirmation(identif)  # Returns dict if confirmed, {} if NOT confirmed.
            if dicto:
                # creates new tag in database and creates tag object
                tag_obj = tag_class.alta(**dicto)
                identifs_in_node[identif] = tag_obj  # If confirmed creates new tag obj and adds it to identifs_in_node.

        # At this point identifs_in_node.values() is made of tag uids and tag objects.Must create objects from uids left
        for k, v in identifs_in_node.items():
            if isinstance(v, str):
                # Creates object from uid and update identifs_in_node dict.
                tag_obj = tag_class.getObject(uid=v)      # tag_class.getObject() pulls the right Tag class for tech.
                if isinstance(tag_obj, tag_class):
                    identifs_in_node[k] = tag_obj    # Final form of this identifs_in_node: {__identifier: tag object}

        # Appends any tags passed as tag objects.
        for t in tag_objs:
            identifs_in_node.update({t.getIdentifiers(): t})        # getIdentifiers() -> returns str ('8330-Bovine')

        # And here, identifs_in_node is fully made of {identifier: tag_obj, }. Goes to commission available tags.
        tagActivity = removeAccents(comm_type).title()  # Capitalizes all words. That's the standard in the db.
        tagActivity = tagActivity if tagActivity in Tag.getActivitiesDict().values() else 'Comision'
        # Setea Actividad para Caravanas. MISMA Actividad para todos los tags
        if 'emplaz' in tagActivity or 'eplace' in tagActivity:
            tagActivity = 'Comision - Reemplazo'
            tagStatus = 'Reemplazada'           # TODO: This part not yet implemented. For now, activity=Comision.
        elif 'emision' in tagActivity or 'emission' in tagActivity:
            tagActivity = 'Comision - Reemision'
            tagStatus = 'Comisionada'
        else:
            tagStatus = 'Comisionada'

        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self.tblDataName(), default='dataframe')

        time_now = time_mt('dt')        # Discards dates in the future for timeStamp, eventDate.
        timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if not dfRA.empty and 'fldTimeStamp' in dfRA else time_now, time_now)
        eventDate = min(getEventDate(tblDate=dfData.loc[0, 'fldDate'] if not dfData.empty and 'fldDate' in dfData
                                            else 0, defaultVal=timeStamp, **kwargs), timeStamp)
        dfData.loc[0, 'fldDate'] = eventDate

        # Commissions all the available tags.
        # self.outerObject.tags_read.clear()  # resets tags_read list.
        for t in identifs_in_node.values():
            if t.isAvailable:
                t.commission.set(activityName=tagActivity, assigned_to_class=self.outerObject.__class__.__name__)
                self.outerObject.setMyTags(t)  # registra Tag en el array de Tags del Animal. Actualiza __identifiers.

                # Attribute with list of tags physically read in the activity, used to set inventory for Tags.
                self.outerObject.tags_read.append(t)

        # Assigns all commissioned tags to Animal object
        # 1. Updates Animales."Identificadores" field.
        df_identifs = pd.read_sql_query(f'SELECT "{getFldName(self.tblObjName(), "fldIdentificadores")}" FROM '
                                        f'"{self.tblObjDBName()}" WHERE "UID_Objeto" == "{self.outerObject.ID}"; ',
                                        SQLiteQuery().conn)
        identifs = set(df_identifs.loc[0, "fldIdentificadores"]) if \
                       isinstance(df_identifs.loc[0, "fldIdentificadores"], collections.abc.Iterable) else set()
        tagIDs = self.outerObject.myTagIDs
        identifs.update(tagIDs)
        if identifs != (set(df_identifs.loc[0, "fldIdentificadores"]) if
                        isinstance(df_identifs.loc[0, "fldIdentificadores"], collections.abc.Iterable) else set()):
            _ = setRecord(self.tblObjName(), fldID=self.outerObject.recordID, fldIdentificadores=identifs)
            self.outerObject._init_uid_dicts()      # Reloads object dataframe with new fldIdentificadores data.

        # 2. Creates assignTag Activity for Animal: Updates db tables tblRA, tblLink, tblData
        # tblLink: 1 record per Animal being operated on (1 in this case).
        # tblData: 1 record per tag being assigned. MULTIPLE RECORDS in this case.
        # Updates tblDataAnimalesCaravanas: 1 row per tag assigned to the Animal.Same idActividadRA from assign Activity
        for j, t in enumerate(self.outerObject.tags_read):
            # appends rows(s) to dfData
            dfData.loc[j, ('fldFK_Caravana', 'fldComment', 'fldDate')] = \
                (t.ID, f'Actividad: {activityName}. Tag ID: {t.tagNumber} / {t.ID}.', eventDate)
            # Setea datos de tag en tbl del outerObject al que se asigna la Caravana
        # TODO: This call counts on dfRA, dfLink fully setup by _createActivityRA(). CHECK OPERATION (04Aug24)
        # TODO (24Aug24): REMOVE _tags_read from object's attribute, make it a function local variable in needed.
        idActividadRA = self._createActivityRA(dfRA, dfLink, dfData)
        self.outerObject.tags_read.clear()     # Clears _tags_read list after it's done with it.
        if isinstance(idActividadRA, str):
            retValue = f'ERR_DBAccess: Cannot read table {dfRA.db.tbl_name}'
            krnl_logger.error(retValue, stack_info=True, exc_info=True)
            return retValue
        retValue = idActividadRA
        # dfRA.loc[0, 'fldID'] = idActividadRA
        return retValue



    # def assignTags03(self, *args: DataTable, comm_type='Comision', tag_list=None, **kwargs):
    #     """
    #     Assigns Tags in *args to Animal. Updates tables in DB.
    #     Commissions all tags via the Tag.commission() method, which sets the Physically-Attached attribute for the tag.
    #     @param tag_list: iterable with Tag objects or tag uids (str) to assign to Animal (list | tuple)
    #     @param comm_type: 'Comision'(Default), 'Reemplazo', 'Reemision'. sets the Activity for Tag (str)
    #     @param args: DataTables to use in operation (tblRA for idActividadRA, tblLink for fldIDLink, etc)
    #     @param kwargs: tags=[Tag, ]. List of Tag Objects to assign (1 or more)
    #            - set_tag_inventory: False -> Avois duplicating tag inventories (Tag Inventory is set by commission.set)
    #     @return: idActividad (int) or errorCode (str)
    #     """
    #     activityName = 'Caravaneo'
    #     if not self._isValid or not self.outerObject.validateActivity(activityName):
    #         retValue = f'ERR_Sys_ObjectNotValid or ActivityNotValid - {callerFunction()}.'
    #         krnl_logger.info(retValue)
    #         print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
    #         return retValue
    #
    #     # tag_list may carry 3 flavors: Tag objects, Tag UIDs and plain numbers. The code below addressed them all.
    #     tags = tag_list if hasattr(tag_list, '__iter__') else [tag_list, ]
    #     tag_objs = [t for t in tags if isinstance(t, Tag) and t.isAvailable]      # List of Tag objects
    #     tag_uids = []
    #     tag_class = self.outerObject._myTagClass()  # Pulls the proper tag class for tag creation. (TagBovine, etc.)
    #     obj_df = tag_class.obj_dataframe()
    #     for t in tags.copy():
    #         if isinstance(t, str):
    #             try:
    #                 tag_uids.append(uuid.UUID(t).hex)
    #             except(TypeError, SyntaxError, AttributeError, ValueError):
    #                 continue
    #             else:
    #                 tags.remove(t)
    #     tag_numbers = [t for t in tags if isinstance(t, str)]  # Only strings left are numbers. UIDs were removed above.
    #     tag_identifs = [Tag.create_identifier(elements=(num, Tag._tagIdentifierChar, self.__class__.__name__))
    #                     for num in tag_numbers]     # Identifier: "123-Bovine", "777-Caprine", etc.
    #     # Split tag_identifiers between those that exist in node and those that don't.
    #     existing_ids = {}                               # existing_ids here: {__identifier: tag_uid}
    #     non_existing_ids = {}
    #     for t in tag_identifs:
    #         if t in obj_df['fldIdentificadores']:       # .tolist() # Tag.get_identifiers_dict():
    #             # TODO: check if this assignment is of the correct type.
    #             existing_ids[t] = obj_df.loc[obj_df['fldIdentificadores'] == t, 'fldObjectUID']
    #         else:
    #             non_existing_ids[t] = None
    #
    #     # Request confirmation of non-existing tags
    #     for identif in non_existing_ids:
    #         dicto = Tag.identifier_get_user_confirmation(identif)  # Returns dict if confirmed, {} if NOT confirmed.
    #         if dicto:
    #             # creates new tag in database and creates tag object
    #             tag_obj = tag_class.alta(**dicto)
    #             existing_ids[identif] = tag_obj  # If confirmed, creates new tag and adds tag_obj to existing_ids.
    #
    #     # At this point existing_ids.values() is made of tag uids and tag objects. Must create objects from uids left.
    #     for k, v in existing_ids.items():
    #         if isinstance(v, str):
    #             # Creates object from uid and update existing_ids dict.
    #             tag_obj = tag_class.getObject(uid=v)      # tag_class.getObject() pulls the right Tag class for tech.
    #             if isinstance(tag_obj, tag_class):
    #                 existing_ids[k] = tag_obj             # Final form of this existing_ids: {__identifier: tag object}
    #
    #     # Adds any tags passed as tag objects.
    #     for t in tag_objs:
    #         existing_ids.update({t.getIdentifiers(): t})        # getIdentifiers() -> returns a uid string.
    #
    #     # And here, existing_ids is fully made of {identifier: tag_obj, }. Goes to commission available tags.
    #     tagActivity = removeAccents(comm_type).title()  # Capitalizes all words. That's the standard in the db.
    #     tagActivity = tagActivity if tagActivity in Tag.getActivitiesDict().values() else 'Comision'
    #     # Setea Actividad para Caravanas. MISMA Actividad para todos los tags
    #     if 'emplaz' in tagActivity or 'eplace' in tagActivity:
    #         tagActivity = 'Comision - Reemplazo'
    #         tagStatus = 'Reemplazada'           # TODO: This part not yet implemented. For now, activity=Comision.
    #     elif 'emision' in tagActivity or 'emission' in tagActivity:
    #         tagActivity = 'Comision - Reemision'
    #         tagStatus = 'Comisionada'
    #     else:
    #         tagStatus = 'Comisionada'
    #
    #     dfRA = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblRAName),
    #                  pd.DataFrame.db.create(self._tblRAName))
    #     dfLink = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblLinkName),
    #                    pd.DataFrame.db.create(self._tblLinkName))
    #     dfData = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self.tblDataName()),
    #                    pd.DataFrame.db.create(self.tblDataName()))
    #     time_now = time_mt('dt')        # Discards dates in the future for timeStamp, eventDate.
    #     timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] or time_now, time_now)
    #     eventDate = min(getEventDate(tblDate=dfData.loc[0, 'fldDate'], defaultVal=timeStamp, **kwargs), timeStamp)
    #     dfData.loc[0, 'fldDate'] = eventDate
    #
    #     # Commissions all the available tags.
    #     self.outerObject._tags_read.clear()  # resets tags_read list.
    #     for t in existing_ids.values():
    #         if t.isAvailable:
    #             t.commission.set(activityName=tagActivity, assigned_to_class=self.outerObject.__class__.__name__)
    #             self.outerObject.setMyTags(t)  # registra Tag en el array de Tags del Animal. Actualiza __identifiers.
    #             # Attribute with list of tags physically read in the activity, used to set inventory for Tags.
    #             self.outerObject._tags_read.append(t)
    #
    #     # Assigns all commissioned tags to Animal object
    #     # 1. Updates Animales."Identificadores" field.
    #     df_identifs = pd.read_sql_query(f'SELECT {getFldName(self.tblObjName(), "fldIdentificadores")} FROM '
    #                                         f'{self.tblObjDBName()} WHERE "UID_Objeto" == "{self.outerObject.ID}"; ')
    #     identifs = set(df_identifs.loc[0, "fldIdentificadores"])
    #     tagIDs = self.outerObject.myTagIDs  # convierte lista a json para escribir en DB.
    #     identifs.update(tagIDs)
    #     if identifs != set(df_identifs.loc[0, "fldIdentificadores"]):
    #         _ = setRecord(self.tblObjName(), fldID=self.outerObject.recordID, fldIdentificadores=identifs)
    #
    #     # 2. Creates assignTag Activity for Animal: Updates tblRA, tblLink, tblData
    #
    #     # tblLink: 1 record per Animal being operated on (1 in this case).
    #     # tblData: 1 record per tag being assigned. MULTIPLE RECORDS in this case.
    #     # Updates tblDataAnimalesCaravanas: 1 row per tag assigned to the Animal.Same idActividadRA from assign Activity
    #     for t in self.outerObject._tags_read:
    #         dfData.loc[len(dfData.index), 'fldFK_Caravana', 'fldComment'] = \
    #             [t.ID, f'Actividad: {activityName}. Tag ID: {t.tagNumber} / {t.ID}.']
    #         # Setea datos de tag en tbl del outerObject al que se asigna la Caravana
    #     idActividadRA = self._createActivityRA(dfRA, dfLink, dfData)
    #     if isinstance(idActividadRA, str):
    #         retValue = f'ERR_DBAccess: Cannot read table {dfRA.db.tbl_name}'
    #         krnl_logger.error(retValue, stack_info=True, exc_info=True)
    #         return retValue
    #     retValue = idActividadRA
    #     return retValue


    # @Activity.activity_tasks(after=(Activity.doInventory,))  NO hacer inventory aqui. Se confunden las actividades
    # por haber solo una actividad de Caravaneo implementada como Actividad.
    def deassignTags(self, *args: pd.DataFrame, tag_list=None, **kwargs):         # Caravaneo - Desasignar (ID=71)
        # 04Aug24: Implements iterator management for obj_dataframe()
        """
        Deassigns Tags from Animal. if Status for deassigned Tags is not specified, status=Decomisionada.
        @param args: Tags array to assign to Animal
        @param tag_list: Tag objects to deassign from tagged object.
        @param kwargs: tagStatus=val -> Value to set on 'deassigned' tags. val must be in activitiesPermittedStatus.
                       Default: 'Decomisionada'
        @return:
        """
        activityName = 'Caravaneo - Desasignar'         # Defined in db but not implemented as an Activity class.
        kwargs['activity_name'] = activityName
        tags = tag_list or []
        if not self._isValid or not self.outerObject.validateActivity(activityName):
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotValid - {callerFunction()}()'
            print(f'{moduleName()}({lineNum()}) - {retValue}')
            return retValue

        tags = tags if hasattr(tags, '__iter__') and not isinstance(tags, str) else list((tags, ))
        tags = [j for j in tags if isinstance(j, Tag)]
        if tags:
            dfRA, dfLink, dfData = self.set3frames(dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                                   dfData_name=self.tblDataName())
            time_now = time_mt('dt')  # Discards dates in the future for timeStamp, eventDate.
            timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] if not dfRA.empty and 'fldTimeStamp' in dfRA and
                                                        pd.notnull(dfRA.loc[0, 'fldTimeStamp']) else time_now, time_now)
            date = dfData.loc[0, 'fldDate'] if not dfData.empty and 'fldDate' in dfData and \
                                               pd.notnull(dfData.loc[0, 'fldDate']) else None
            eventDate = min(getEventDate(tblDate=date, defaultVal=timeStamp, **kwargs), timeStamp)
            dfData.reset_index()
            dfData.loc[0, 'fldDate'] = eventDate
            for idx, t in enumerate(tags):
                # dfData MUST BE EMPTY for this to work.
                # if pd.isnull(dfData.iloc[idx, dfData.columns.get_loc('fldFK_Caravana')]):
                dfData.iloc[idx, dfData.columns.get_loc('fldFK_Caravana')] = t.ID    # Multiple records in tblData.

            idActividadRA = self._createActivityRA(dfRA, dfLink, dfData)
            if isinstance(idActividadRA, str):
                retValue = f'ERR_DB_Access - Func/Method: TagActivity.deassign()'
                krnl_logger.error(retValue)
                return retValue

            initial_identifs = self.outerObject.getIdentifiers()
            for t in tags:
                self.outerObject.popMyTags(t)           # retira el Tag del Diccionario __myTags
                tag_status = kwargs.get('status', '') or 'Decomisionada'
                t.commission.unset(status=tag_status, date=eventDate, **kwargs)  # this ain't an Inventory Activity.

            new_identifs = self.outerObject.getIdentifiers()        # This is a tuple.
            if set(initial_identifs) != set(new_identifs):
                _ = setRecord(self.tblObjName(), fldID=self.outerObject.recordID, fldIdentificadores=new_identifs)
                self.outerObject._init_uid_dicts()  # Reloads object dataframe with new fldIdentificadores data.

            retValue = idActividadRA
            return retValue

        return None


    # def deassignTags01(self, *args: pd.DataFrame, tag_list=None, **kwargs):         # Caravaneo - Desasignar (ID=71)
    #     """
    #     Deassigns Tags from Animal. if Status for deassigned Tags is not specified, status=Decomisionada.
    #     @param args: Tags array to assign to Animal
    #     @param tag_list: Tag objects to deassign from tagged object.
    #     @param kwargs: tagStatus=val -> Value to set on 'deassigned' tags. val must be in activitiesPermittedStatus.
    #                    Default: 'Decomisionada'
    #     @return:
    #     """
    #     activityName = 'Caravaneo - Desasignar'         # Defined in db but not implemented as an Activity class.
    #     kwargs['activity_name'] = activityName
    #     tags = tag_list or []
    #     if not self._isValid or not self.outerObject.validateActivity(activityName):
    #         retValue = f'ERR_Sys_ObjectNotValid or ActivityNotValid - {callerFunction()}()'
    #         print(f'{moduleName()}({lineNum()}) - {retValue}')
    #         return retValue
    #
    #     tags = tags if hasattr(tags, '__iter__') else list(tags)
    #     tags = [j for j in tags if isinstance(j, Tag)]
    #     if tags:
    #         dfRA = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblRAName),
    #                     pd.DataFrame.db.create(self._tblRAName))
    #         dfLink = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self._tblLinkName),
    #                       pd.DataFrame.db.create(self._tblLinkName))
    #         dfData = next((j for j in args if isinstance(j, pd.DataFrame) and j.db.tbl_name == self.tblDataName()),
    #                       pd.DataFrame.db.create(self.tblDataName()))
    #         time_now = time_mt('dt')  # Discards dates in the future for timeStamp, eventDate.
    #         timeStamp = min(dfRA.loc[0, 'fldTimeStamp'] or time_now, time_now)
    #         eventDate = min(getEventDate(tblDate=dfData.loc[0, 'fldDate'], defaultVal=timeStamp, **kwargs), timeStamp)
    #         dfData.loc[0, 'fldDate'] = eventDate
    #
    #         for idx, t in enumerate(tags):
    #             if pd.isnull(dfData.iloc[idx, 'fldFK_Caravana']):
    #                 dfData.iloc[idx, 'fldFK_Caravana'] = t.ID         # Multiple records in tblData.
    #
    #         idActividadRA = self._createActivityRA(dfRA, dfLink, dfData)
    #         if isinstance(idActividadRA, str):
    #             retValue = f'ERR_DB_Access - Func/Method: TagActivity.deassign()'
    #             krnl_logger.error(retValue)
    #             return retValue
    #
    #         for t in tags:
    #             if isinstance(t, Tag):
    #                 self.outerObject.popMyTags(t)           # retira el Tag del Diccionario __myTags
    #                 tag_status = kwargs.get('status', '') or 'Decomisionada'
    #                 t.commission.unset(status=tag_status, date=eventDate, **kwargs)  # this ain't an Inventory Activity.
    #
    #         retValue = idActividadRA
    #         return retValue
    #     return None



    @classmethod
    def isTagAssigned(cls, tag):           # cls is Bovine, Caprine, etc.
        """
        Informs whether tag is assigned to an object of type cls, by assembling a Tag identifier that includes cls.
        Uses that identifer to search tag in the Tag._identifiers_dict. Returns the status of that tag if found.
        @param tag: tagNumber, Tag UID (str) or Tag object.
        @return: True: Tag is assigned to object of class cls; False: Tag is available; None: Tag not found.
        """
        if isinstance(tag, str):
            tagID = None
            try:
                tagID = uuid.UUID(tag).hex
            except (SyntaxError, TypeError, AttributeError, ValueError):
                # Tag passed as a tag number. Must create tag identifier
                identifier = Tag.create_identifier(elements=(tag.strip(), Tag._tagIdentifierChar, cls.__name__))
                for df in cls._myTagClass().obj_dataframe():        # _myTagClass is TagBovine, TagCaprine, etc.
                    try:
                        tagID = df.loc[df['fldIdentificadores'].isin([identifier]), 'fldObjectUID'].iloc[0]
                        break
                    except IndexError:
                        continue
            if tagID:
                # A valid Tag uid here or None: creates Tag object or returns None.
                tagObj = Tag.getObject(tagID)
            else:
                tagObj = None

        elif isinstance(tag, Tag):
            tagObj = tag
        else:
            tagObj = None

        if tagObj:
            return not tagObj.isAvailable       # True: Assigned / False: Not assigned.
        return None


#  TODO(cmt): Esta clase NO DEBE SER singleton: Se crea un objeto por cada Actividad Programada definida para Animales.
#   NO tiene asociado ningun @property en krnl_abstract_class_animal.py
class ProgActivityAnimal(ProgActivity):
    __tblDataName = 'tblDataAnimalesActividadesProgramadasStatus'
    __tblRAName = 'tblAnimalesRegistroDeActividades'
    __paRegisterDict = {}  # {self: ActivityID} Register de las Actividades Programadas ACTIVAS de class AnimalActivity
    __triggerRegisterDict = {}  # {idTrigger: triggerObj}. 1 triggerRegisterDict para todas las ProgActivities.

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    # List of activities with ProgActivity defined and supported. Used in recordActivity() method.
    __paDict = AnimalActivity.getSupportsPADict()         # {ActivityName: __activityID, } con getSupportsPADict=True

    @classmethod
    def progActivitiesDict(cls):
        return cls.__paDict     # {__activityName: __activityID, }

    # Tablas Actividades Programadas - Class attributes. NO se definen en parent class.
    __tblProgActivitiesName = 'tblDataProgramacionDeActividades'  # misma para todas las clases de objetos.
    __tblRAPName = ProgActivity.tblRANames().get(__tblRAName, '')
    __tblLinkPAName = ProgActivity.progTables()[__tblRAPName][0] if __tblRAPName else ''
    __tblPATriggersName = ProgActivity.progTables()[__tblRAPName][1] if __tblRAPName else ''
    __tblPADataStatusName = ProgActivity.progTables()[__tblRAPName][2] if __tblRAPName else ''
    __tblPASequencesName = ProgActivity.progTables()[__tblRAPName][3] if __tblRAPName else ''
    __tblPASequenceActivitiesName = ProgActivity.progTables()[__tblRAPName][4] if __tblRAPName else ''
    __AnimalProgActivitiesPermittedStatus = {1: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 2: [3, 5, 6, 7, 8, 9, 10, 11, 12],
                                             3: [6, 7, 9, 10, 11], 4: [], 5: [], 6: [], 7: [], 8: [], 9: [],
                                             10: [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12],
                                             11: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 12: []}

    @classmethod
    def progActivitiesPermittedStatus(cls):
        return cls.__AnimalProgActivitiesPermittedStatus

    @classmethod
    def tblRAPName(cls) -> str:
        return cls.__tblRAPName

    @classmethod
    def tblLinkPAName(cls) -> str:
        return cls.__tblLinkPAName

    @classmethod
    def tblPADataStatusName(cls) -> str:
        return cls.__tblPADataStatusName

    @classmethod
    def tblTriggersName(cls) -> str:
        return cls.__tblPATriggersName

    @classmethod
    def tblDataProgramacionName(cls) -> str:  # Must be implemented in subclasses.
        return cls.__tblProgActivitiesName

    def __init__(self, ID, isValid=True, activityID=None, enableActivity=activityEnableFull, *args, prog_data_dict=None,
                 RAP_dict=None, json_dict_create=None, json_dict_close=None, **kwargs):
        super().__init__(ID, isValid, activityID, enableActivity, *args, prog_data_dict=prog_data_dict,
                         RAP_dict=RAP_dict, json_dict_create=json_dict_create, json_dict_close=json_dict_close,**kwargs)

    @classmethod
    def paCreateActivity(cls, activity_id=None, *args: DataTable, enable_activity=activityEnableFull, **kwargs):
        """ Creates a Animal Programmed Activity (PA) and registers the created object in __registerDict.
        Records data in the RAP, Data Programacion, tblLinkPA in database.
        TO BE CALLED from Animal.paCreateActivity().
        *** tblRAP, tblLinkPA, tblDataProg MUST BE POPULATED IN FULL.In particular object items (fldFK in tblLinKPA) ***
        @return: ProgActivity Object. ERR_ (str) if class is not valid or if recordActivity() finished with errors.
        """
        obj = super()._paCreateActivity(activity_id, *args, enable_activity, **kwargs)
        # if isinstance(obj, cls):
        #     obj.paRegisterActivity()
        return obj

    def paRegisterActivity(self):
        if self._isValid:
            self.__paRegisterDict[self] = self.activityID         # {paObj: __activityID, }
            # 1st Attempt: Actualiza aqui el dict. Object ID viene de targetObjects
            # self.getAuxDictLinkPA()[self].update(self.targetObjects). ESTO NO SE USA MAS. DEPRECADO.

    def paUnregisterActivity(self):
        # self.getAuxDictLinkPA().pop(self, None)
        return self.__paRegisterDict.pop(self, None)

    # Class methods: Attached to the Activity Class (ProgActivityAnimal, ProgActivityDevice, ProgActivityPerson, etc)
    @classmethod
    def getPARegisterDict(cls):
        return cls.__paRegisterDict         # {paObj: ActivityID}


    @classmethod
    def loadFromDB(cls):
        objList = super().loadFromDB()
        if isinstance(objList, str):
            krnl_logger.error(f'ERR_DB_Access Error: Programmed Activities for class {cls.__name__} not loaded: {objList}')
            return f'ERR_DB_Access Error: Programmed Activities for class {cls.__name__} not loaded: {objList}'
        _ = [cls.paRegisterActivity(j) for j in objList]  # {paObj: __activityID, } Registers objects in class dict.
        return bool(_)

    # @classmethod
    # def loadFromDB00(cls):
    #     objList = super().loadFromDB()
    #     if isinstance(objList, str):
    #         krnl_logger.error(
    #             f'ERR_DB_Access Error: Programmed Activities for class {cls.__name__} not loaded: {objList}')
    #         return f'ERR_DB_Access Error: Programmed Activities for class {cls.__name__} not loaded: {objList}'
    #     _ = [cls.paRegisterActivity(j) for j in objList]  # {paObj: __activityID, } Registers objects in class dict.
    #     return bool(_)


    def updateStatus(self, status=None, *args, **kwargs):
        """                     IMPORTANT: Accessed by background threads.
        Sets / updates programmed activities Status in table [Data Actividades Programadas Status].
        Activity for which status is to be set (fldID in Registro De Actividades Programadas) MUST be passed in args
        as part of a DataTable struct.
        This function only sets the status to passed val and closes a programmed activity when requested.
        @param status: status val if not provided in tblDataStatus DataTable
        @return: recordID (int) if successful or errorCode (str)
        """
        # TODO: ACTUALIZAR AQUI __auxDictLinkPA = {}  # {idObj: (fldID1, fldID2, ), }
        retValue = super().updateStatus(status, *args, **kwargs)  # llama a super() p/ tareas comunes a todas las clases
        return retValue

# ----------------------------------------------- END Class Code --------------------------------------------------- #
ProgActivityAnimal.loadFromDB()         # Loads progActivities from DB and registers objects in registerDict.
# ProgActivityAnimal.loadTriggers()        # Loads Triggers from DB and registers objects in registerDict.

# ============================================== End class ProgActivityAnimal =================================== #


@singleton
class PersonActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadPersonas'
    __activityName = 'Personas'
    __method_name = 'person'
    _short_name = 'person'                  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    def __init__(self, *args, **kwargs):
        isValid = True
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    # @Activity.dynamic_attr_wrapper
    def get(self, *args, activity_id=None, uid=None, **kwargs):
        """
        Returns records from [Data Animales Actividad Personas] table.
        @param activity_id: int | list | tuple. ActivityID for activities to use as filters.
        @param uid: uid | list | tuple. Object uids for which to pull data.
       @return: DataFrame object with information.
       """
        if self._isValid and self.outerObject.validateActivity(self.__activityName):
            activity_list = (activity_id,) if isinstance(activity_id, int) else activity_id
            return self._get_link_records(activity_list=activity_list, uid=uid)
        return None



    def set(self, *args: pd.DataFrame, person=None, execute_fields=None, excluded_fields=None, **kwargs):
        """     # TODO 04Aug24: This function needs EXTENSIVE testing.
        Inserts ID_Persona obj_data for the object "obj" in the DB, along with ownership percentage over Object.
        __outerAttr: TagObject para la que se realiza la Actividad. Lo setea el metodo inventario().
        For convenience, can process multiple [Data Personas] tables with multiple records in each of the tables.
        To preserve consistency, a call to set() MUST DEFINE OWNERS FOR 100% of the object Ownership: Must set
        to Inactive all previous ownership records for the Object and leave Active the newly inserted records.
        For this, high level functions MUST DEFINE ALL Persons and Percent so that total percentage = 100% for all
        set() operations. Must pass this information in DataTable form. set() will remove all previous owner records
        to inactive (OVERWRITE->fldFlag=0) and set the new records as Active, for a total of 100% ownership.
        @param person:
        @param person: (uid), ID active in the system. Used when assigning only 1 person, with 100% ownership.
        @param args: list of DataTable objects, with all the tables and fields to be written to DB. Used when
                multiple persons are to be assigned to the obj Object.
                No checks performed. The function will discard and ignore all non-valid arguments
        @param kwargs: Use to pass additional obj_data for table [Data Animales Actividad Personas]
                       idPerson=(int) Assignes ID_Persona with 100% ownership (Simplified input)
        @return: ID_Actividad (Registro De Actividades, int) if success; errorCode (str) on error; None for nonValid
        """
        # TODO: Checks to be performed by high level-> All idPerson valid, active, level=1; Sum of % ownership=100%
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=self._tblRAName, dfLink_name=self._tblLinkName,
                                               dfData_name=self.__tblDataName)
        if not dfData.empty:
            idPerson = ()
        elif isinstance(person, (list, tuple, set)):
            idPerson = person
        elif isinstance(person, str):
            idPerson = (person, )
        else:
            raise ValueError(f'{self.__class__.__name__}.set(): ValueError. Invalid argument person.')

        if self._isValid and self.outerObject.validateActivity(self._activityName):
            idPersonPercent = {}          # {Person uid: fldPercentageOwnership, }
            activityID = dfRA.loc[0, 'fldFK_NombreActividad'] if not dfRA.empty and 'fldFK_NombreActividad' in dfRA.columns else None
            if pd.isnull(activityID):
                activityID = self._activityID

            dfRA.iloc[0, dfRA.columns.get_loc('fldFK_NombreActividad')] = activityID
            # Genera lista de owners originales del animal, existentes ANTES de asignar los owners de esta llamada.
            # SOLO filtrar por getID en temp0, NO usar ID_Actividad porque el seteo de ownership se puede
            # hacer con cualquier __activityID. Lo mismo aplica para cualquier otro filtro de este tipo.
            strwhr0 = f'WHERE "{getFldName(self._tblLinkName, "fldFK")}" == "{self.outerObject.ID}"'
            ser0 = getrecords(self._tblLinkName, 'fldFK_Actividad', where_str=strwhr0)['fldFK_Actividad']
            objectActivitiesCol = tuple(ser0.tolist())
            objectActivities_str = str(objectActivitiesCol) if len(objectActivitiesCol) > 1 else \
                str(objectActivitiesCol).replace(',', '')
            strwhr1 = f'WHERE "{getFldName(self.__tblDataName, "fldFlag")}" == 1 ' + \
                      f'AND "{getFldName(self.__tblDataName, "fldFK_Actividad")}" IN {objectActivities_str}' \
                      if len(objectActivitiesCol) > 0 else ';'
            dftemp = getrecords(self.__tblDataName, 'fldID', 'fldFK_Persona', 'fldPercentageOwnership',
                                where_str=strwhr1)

            personRecords = pd.DataFrame.db.create(self.__tblDataName)
            if dfData.empty:               # if not args:
                for j, pers in enumerate(idPerson):        # p MUST BE A VALID Person uid.
                    dfData.loc[j, ('fldFK_Persona', 'fldPercentageOwnership', 'fldFlag')] = (pers, 1.0, 1)
                    idPersonPercent[idPerson] = 1.0     # 100% ownership
                    personRecords.loc[len(personRecords.index)] = dfData.loc[j]
            else:
                for idx in dfData.index:
                    personRecords.loc[len(personRecords.index)] = dfData.loc[idx]
                    idPersonPercent[dfData.loc[idx, 'fldFK_Persona']] = float(dfData.loc[idx, 'fldPercentageOwnership'])
                    dfData.loc[idx, 'fldFlag'] = 1        # Sets as active. Is it really needed??

            print(f'PEERRRRRRRRRRSON! {moduleName()}({lineNum()}) personRecords={personRecords.to_dict()}',
                  dismiss_print=DISMISS_PRINT)
            # objPersonList = [Person.getRegisterDict()[i] for i in idPersonPercent
            #                      if i in Person.getRegisterDict() and Person.getRegisterDict()[i].level == 1]
            objPersonList = []
            for df in Person.obj_dataframe():       # TODO 04Aug24: IMPLEMENT DataFrames for Person class.
                # objPersonList.append(df[df['fldObjectUID'].isin(list(idPersonPercent.keys()))]['fldObjectUID'].
                #                      apply(Person.getObject).tolist()[0])
                objPersonList.extend(df[df['fldObjectUID'].isin(list(idPersonPercent.keys()))]['fldObjectUID'].tolist())

            if objPersonList:       # A list of Owners' uids.
                # objPersonList = [Person.getObject(j) for j in objPersonList]
                ownershipTotal = 0
                ownershipTotal += sum([v for k, v in idPersonPercent.items() if k in objPersonList])
                # Chequea que % Ownership sea 100% para todos los owners validos
                if ownershipTotal != 1.00:
                    retValue = f'ERR_INP_InvalidArgument - {callerFunction()}: Total Percentage Ownership for ' \
                               f'Object {self.outerObject.getID} must equal 100%. Exiting...'
                    krnl_logger.info(retValue)
                    raise ValueError(f'{moduleName()}({lineNum()} - {retValue})')

                print(f'PEERRRRRRRRRRSON! {moduleName()}({lineNum()}) Person(s): {objPersonList} - '
                      f'TOTAL OWNERSHIP={ownershipTotal}', dismiss_print=DISMISS_PRINT)
                personIDCol = personRecords['fldFK_Persona'].tolist()
                dfData_new = pd.DataFrame.db.create(dfData.db.tbl_name, data=personRecords.to_dict())

                print(f'PEERRRRRRRRRRSON! {moduleName()}({lineNum()}) newTblData={dfData_new.values}',
                      dismiss_print=DISMISS_PRINT)
                if not dfData_new.empty:
                    if not dftemp.empty:
                        dftemp['fldFlag'] = 0       # setea fldFlag=0 para los owners previos.
                    setrecords(dftemp)
                    retValue = self._setPerson(dfRA, dfLink, dfData_new, execute_fields=execute_fields,
                                               excluded_fields=excluded_fields)
                else:
                    retValue = f'ERR_INP_PersonNotValid - {callerFunction()}. NO PERSON...'
                    print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            else:
                retValue = f'ERR_INP_PersonNotValid - {callerFunction()}. Person ID:{idPerson}'
                print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - {callerFunction()}. Person ID:{idPerson}'
            print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)

        return retValue



    # def set00(self, *args: DataTable, execute_fields=None, excluded_fields=None, **kwargs):     # OUTDATED. DEPRECATE.
    #     """
    #     Inserts ID_Persona obj_data for the object "obj" in the DB, along with ownership percentage over Object.
    #     __outerAttr: TagObject para la que se realiza la Actividad. Lo setea el metodo inventario().
    #     For convenience, can process multiple [Data Personas] tables with multiple records in each of the tables.
    #     To preserve consistency, a call to set() MUST DEFINE OWNERS FOR 100% of the object Ownership: Must set
    #     to Inactive all previous ownership records for the Object and leave Active the newly inserted records.
    #     For this, high level functions MUST DEFINE ALL Persons and Percent so that total percentage = 100% for all
    #     set() operations. Must pass this information in DataTable form. set() will remove all previous owner records
    #     to inactive (OVERWRITE->fldFlag=0) and set the new records as Active, for a total of 100% ownership.
    #     @param idPerson: ID_Persona, active in the system. Used when assigning only 1 person, with 100% ownership.
    #     @param args: list of DataTable objects, with all the tables and fields to be written to DB. Used when
    #             multiple persons are to be assigned to the obj Object.
    #             No checks performed. The function will discard and ignore all non-valid arguments
    #     @param kwargs: Use to pass additional obj_data for table [Data Animales Actividad Personas]
    #                    idPerson=(int) Assignes ID_Persona with 100% ownership (Simplified input)
    #     @return: ID_Actividad (Registro De Actividades, int) if success; errorCode (str) on error; None for nonValid
    #     """
    #     # TODO: Checks to be performed by high level-> All idPerson valid, active, level=1; Sum of % ownership=100%
    #     tblData = setupArgs(self.__tblDataName, *args, **kwargs)
    #     idPerson = next((kwargs[j] for j in kwargs if 'person' in str(j).lower()), None)
    #     if self._isValid and self.outerObject.validateActivity(self._activityName):
    #         tblRA = setupArgs(self._tblRAName, *args)
    #         tblLink = setupArgs(self._tblLinkName, *args)
    #         idPersonDict = {}          # {ID Persona: fldPercentageOwnership, }
    #         personRecords = DataTable(tblData.tblName)
    #         activityID = self._activityID if tblRA.getVal(0, 'fldFK_NombreActividad') is None \
    #             else tblRA.getVal(0, 'fldFK_NombreActividad')
    #         tblRA.setVal(0, fldFK_NombreActividad=activityID)
    #         # Genera lista de owners originales del animal, existentes ANTES de asignar los owners de esta llamada.
    #         # SOLO filtrar por getID en temp0, NO usar ID_Actividad porque el seteo de ownership se puede
    #         # hacer con cualquier __activityID. Lo mismo aplica para cualquier otro filtro de este tipo.
    #         temp0 = getRecords(tblLink.tblName, '', '', None, 'fldFK_Actividad', 'fldFK', fldFK=self.outerObject.ID)
    #         objectActivitiesCol = temp0.getCol('fldFK_Actividad')
    #         temp = getRecords(tblData.tblName, '', '', None, 'fldID', 'fldFK_Persona', 'fldPercentageOwnership',
    #                           fldFlag=1, fldFK_Actividad=objectActivitiesCol)
    #         print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) - initialActiveRecords={temp.dataList}',
    #               dismiss_print=DISMISS_PRINT)
    #         # percentageCol = temp.getCol('fldPercentageOwnership')
    #         # initialOwnershipPercent = float(sum(percentageCol))
    #         # print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) percentCol: {percentageCol} / '
    #         #       f'intialPercentage={initialOwnershipPercent}')
    #
    #         if not args:
    #             tblData.setVal(0, fldFK_Persona=idPerson, fldPercentageOwnership=1.0, fldFlag=1)
    #             idPersonDict[idPerson] = 1.0     # 100% ownership
    #             personRecords.appendRecord(**tblRA.unpackItem(0))
    #         else:
    #             for t in args:
    #                 if t.tblName == tblData.tblName and t.dataLen and len(t.dataList[0]) > 0:
    #                     for j in range(t.dataLen):
    #                         idPersonDict[t.getVal(j, 'fldFK_Persona')] = float(t.getVal(j, 'fldPercentageOwnership'))
    #                         t.setVal(j, fldFlag=1)
    #                         personRecords.appendRecord(**t.unpackItem(j))  # DataTable con info de Record de personas
    #
    #         print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) personRecords={personRecords.dataList}',
    #               dismiss_print=DISMISS_PRINT)
    #         objPersonList = [Person.getRegisterDict()[i] for i in idPersonDict
    #                              if i in Person.getRegisterDict() and Person.getRegisterDict()[i].level == 1]
    #         if objPersonList:
    #             ownershipTotal = 0
    #             for j in objPersonList:     # Chequea que % Ownership sea 100% para todos los owners validos
    #                 ownershipTotal += next((idPersonDict[g] for g in idPersonDict if g == j.getID), 0)
    #             if ownershipTotal != 1.00:
    #                 retValue = f'ERR_INP_InvalidArgument - {callerFunction()}: Total Percentage Ownership for ' \
    #                            f'Object {self.outerObject.getID} must equal 100%. Exiting...'
    #                 krnl_logger.info(retValue)
    #                 # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
    #                 raise RuntimeError(f'{moduleName()}({lineNum()} - {retValue})')
    #
    #             print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) Person(s): {objPersonList} - '
    #                   f'TOTAL OWNERSHIP={ownershipTotal}', dismiss_print=DISMISS_PRINT)
    #             newTblData = DataTable(tblData.tblName)
    #             personIDCol = personRecords.getCol('fldFK_Persona')
    #             for obj in objPersonList:
    #                 for j in range(len(personIDCol)):
    #                     if obj.ID == personIDCol[j]:
    #                         newTblData.appendRecord(**personRecords.unpackItem(j))
    #             print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) newTblData={newTblData.dataList}',
    #                   dismiss_print=DISMISS_PRINT)
    #             if newTblData.dataLen:
    #                 if temp.dataLen and temp.dataList[0]:
    #                     for j in range(temp.dataLen):       # setea fldFlag=0 para los owners previos.
    #                         temp.setVal(j, fldFlag=0)
    #                     _ = tblData.setRecords()
    #                 retValue = self._setPerson(tblRA, tblLink, newTblData, execute_fields, excluded_fields)
    #             else:
    #                 retValue = f'ERR_INP_PersonNotValid - {callerFunction()}. NO PERSON...'
    #                 print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
    #         else:
    #             retValue = f'ERR_INP_PersonNotValid - {callerFunction()}. Person ID:{idPerson}'
    #             print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
    #     else:
    #         retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - {callerFunction()}. Person ID:{idPerson}'
    #         print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)
    #     # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
    #     return retValue

    # @Activity.dynamic_attr_wrapper
    def getOwners(self):
        """
        Gets all active owners for AnimalID, so that sum of partial ownerships gives 100%.
        @return: {ID_Persona: percentageOwnership}, {} if None.
        """
        retDict = {}                # {ID_Persona: percentageOwnership}
        return



@singleton
class MaintenanceActivityAnimal(AnimalActivity):
    __tblDataName = 'tblAnimales'       # TODO: not sure which Data table to use here...
    __activityName = 'Mantenimiento'
    __method_name = 'maintenance'

    def __init__(self, *args, **kwargs):
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = False
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    def paCleanup(self, *args: DataTable, **kwargs):
        """
        Closes all expired progActivities for the outer object (Animal). RUNS IN THE BACKRGROUND.

        @return: None
        """
        pass

# -------------------------------- FIN CLASES ANIMALACTIVITY. SIGUEN FUNCIONES SUELTAS ------------------------------ #

