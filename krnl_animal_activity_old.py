from krnl_abstract_class_activity import *
from krnl_abstract_class_prog_activity import ProgActivity
from krnl_geo import Geo
from krnl_tag import Tag
from krnl_tag_bovine import TagAnimal, TagBovine
from krnl_person import Person
from krnl_custom_types import setupArgs, setRecord, delRecord
from krnl_config import krnl_logger, compare_range
from krnl_db_query import getTblName, getFldName
import threading

def moduleName():
    return str(os.path.basename(__file__))


class AnimalActivity(Activity):     # Abstract Class (no lleva ningun instance attributte). NO se instancia.
    __abstract_class = True     # Activity runs the __init_subclass() function to initialize all subclasses.

    # Accessed from Activity class. Stores AnimalActivity class objects.
    _activity_class_register = set()    # Classes used to create instance objects for each AnimalActivity.

    def __call__(self, caller_object=None):
        """            *** This method is inherited and can be accessed from derived classes. Cool!! ****
        __call__ is invoked when an instance is followed by parenthesis (instance_obj()) or when a property is defined
        on the instance, and the property is invoked (inventory.get(): in this case __call__ is invoked for the object
        associated with inventory property before making the get() call. Then, outerObject is properly initialized.)
        @param caller_object: instance of Bovine, etc. that invokes the Activity. How this binding works is shown at
        https://stackoverflow.com/questions/57125769/when-the-python-call-method-gets-extra-first-argument

        @return: Activity object invoking __call__()
        """
        # item_obj=None above is important to allow to call fget() like that, without having to pass dummy parameters.
        # print(f'\n>>>>>>>>>>>>>>>>>>>> {self._decoratorName}.__call__() params - args: {(caller_object.ID, *args)}; '
        #       f'kwargs: {kwargs}')
        self.outerObject = caller_object  # item_object es instance de Animal, Tag, etc. NO PUEDE SER class.
        return self

    # Class Attributes: Tablas que son usadas por todas las instancias de InventoryActivityAnimal
    __tblRAName = 'tblAnimalesRegistroDeActividades'
    __tblRADBName = getTblName(__tblRAName)
    __tblObjectsName = 'tblAnimales'     # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
    __tblObjectDBName = getTblName(__tblObjectsName)
    __tblLinkName = 'tblLinkAnimalesActividades'
    __tblDataCategoryName = 'tblDataAnimalesCategorias'
    __tblProgStatusName = 'tblDataActividadesProgramadasStatus'       # __tblPADataStatusName
    __tblProgActivitiesName = 'tblDataProgramacionDeActividades'
    __tblPATriggersName = 'tblAnimalesActividadesProgramadasTriggers'
    __lock = Lock()

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


    temp = getRecords('tblAnimalesActividadesNombres', '', '', None, 'fldID', 'fldName', 'fldFlag',
                      'fldFK_ClaseDeAnimal', 'fldFlagPA', 'fldPAExcludedFields', 'fldDecoratorName','fldTableAndFields',
                      'fldFilterField')
    _activitiesDict = {}           # {Nombre Actividad: ID_Actividad, }
    _isInventoryActivity = {}
    _activitiesForMyClass = {}
    _genericActivitiesDict = {}  # {Nombre Actividad: (activityID,decoratorName,tblName,qryFieldName,filterField), }
    __supportPADict = {}          # {activity_name: activityID, } con getSupportsPADict=True
    __paRegisterDict = {}  # {self: ActivityID} Register de las Actividades Programadas ACTIVAS de class AnimalActivity

    # Variables para logica de manejo de objetos repetidos/duplicados.
    _fldID_list = []  # List of all active records pulled by getRecords() from tblAnimales.
    _fldID_exit_list = []  # List of all all records with fldExitDate > 0, updated in each call to _processDuplicates().
    _object_fldUPDATE_dict = {}  # {fldID: fldUPDATE_counter, }        Keeps a dict of uuid values of fldUPDATE fields
    # _RA_fldUPDATE_dict = {}  # {fldID: fldUPDATE_counter, }      # TODO: ver si este hace falta.
    # _RAP_fldUPDATE_dict = {}  # {fldID: fldUPDATE_counter, }      # TODO: ver si este hace falta.
    temp0 = getRecords(__tblRAName, '', '', None, 'fldID')
    if isinstance(temp0, DataTable) and temp0.dataLen:
        # Initializes _fldID_list with all fldIDs for Triggers that detect INSERT operations.
        _fldID_list = temp0.getCol('fldID')

    __classExcludedFieldsClose = {"fldWindowLowerLimit", "fldWindowUpperLimit", "fldFK_Secuencia",
                                  "fldDaysToAlert", "fldDaysToExpire"}.union(Activity._getBaseExcludedFields())
    __classExcludedFieldsCreate = {"fldPADataCreacion"}.union(Activity._getBaseExcludedFields())

    _activityExcludedFieldsClose = {}  # {activityID: (excluded_fields, ) }
    __activityExcludedFieldsCreate = {}  # {activityID: (excluded_fields, ) }

    # Creates dictionary with GenericActivity data. Each item in the dictionary will generate 1 GenericActivity object.
    for j in range(temp.dataLen):
        d = temp.unpackItem(j)
        if d['fldTableAndFields'] and d['fldDecoratorName'] and "." in d['fldTableAndFields']:
            tbl_flds = d['fldTableAndFields'].split(".")
            _genericActivitiesDict[d['fldName']] = (d['fldID'], d['fldDecoratorName'], tbl_flds[0], tbl_flds[1],
                                                     d['fldFilterField'] or 'fldDate')
    print(f'------------{moduleName()}.{lineNum()}-- _genericActivitiesDict: {_genericActivitiesDict}',
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
    def getSupportPADict(cls):
        return cls.__supportPADict    # {ActivityName: __activityID, } con getSupportsPADict=True

    for j in range(temp.dataLen):
        _activitiesDict[temp.dataList[j][1]] = temp.dataList[j][0]       # {Nombre Actividad: ID_Actividad, }
        _isInventoryActivity[temp.dataList[j][1]] = temp.dataList[j][2]  # {Nombre Actividad:_isInventoryActivity(1/0),}
        _activitiesForMyClass[temp.dataList[j][1]] = temp.dataList[j][3]  # {Nombre Actividad: AnimalClass, }
        if bool(temp.dataList[j][4]):
            __supportPADict[temp.dataList[j][1]] = temp.dataList[j][0]  # {ActivityName: __activityID, }, supportPA=True

        # {Nombre Actividad: excluded_fields,}
        _activityExcludedFieldsClose[temp.dataList[j][1]] = set(temp.dataList[j][5] or set()).union(__classExcludedFieldsClose)

    del temp
    del temp0


    def __new__(cls, *args, **kwargs):
        if cls is AnimalActivity:
            krnl_logger.error(f"ERR_TypeError: {cls.__name__} cannot be instantiated, please use a subclass")
            raise TypeError(f'ERR_TypeError: class {cls} cannot be instantiated. Please, use one of its subclasses.')
        return super().__new__(cls)

    def __init__(self, activityName=None, *args, activity_enable=activityEnableFull, tbl_data_name='',
                 excluded_fields_close=(), **kwargs):
        activityID = self._activitiesDict.get(activityName)
        invActivity = self._isInventoryActivity.get(activityName)
        kwargs['tblProgStatusName'] = self.__tblProgStatusName
        kwargs['tblProgActivitiesName'] = self.__tblProgActivitiesName
        kwargs['excluded_fields'] = self.getActivityExcludedFieldsClose(activity_name=activityName) or set() .\
            union(excluded_fields_close)
        isValid = True
        if kwargs.get('supportsPA', None) is None:
            # IMPORTANT SETTING: Si no hay override desde abajo, setea al valor de _supportsPA{} para ese __activityName
            kwargs['supportsPA'] = bool(self.__supportPADict.get(activityName, False))

        super().__init__(isValid, activityName, activityID, invActivity, activity_enable, self.__tblRAName, *args,
                         tblDataName=tbl_data_name, tblObjectsName=self.__tblObjectsName,  **kwargs)     #

        # # TODO(cmt): __outerAttr => ONLY 1 object per Activity and per Thread (recursion with a different object for
        # #  SAME activity and SAME Thread is not supported). Used by all re-entrant methods (that could be called by
        # #  different threads: Activity.__call__, _setInventory, _setStatus, _setLocaliz, _getRecordLinkTables, etc.).
        # self.__outerAttr = {}  # Atributo DINAMICO. {threadID: outerObject, }

    @classmethod
    def getActivitiesDict(cls):
        return cls._activitiesDict           # {Nombre Actividad: ID_Actividad, }

    @property
    def activities(self):  # Este @property es necesario para las llamadas a obj.outerObject.
        return self._activitiesDict          # {Nombre Actividad: ID_Actividad, }

    @classmethod
    def getInventoryActivitiesDict(cls):
        return cls._isInventoryActivity

    @classmethod
    def tblDataCategoryName(cls):
        return cls.__tblDataCategoryName

    @staticmethod
    def getPARegisterDict():           # TODO(cmt): AnimalActivity, ProgActivityAnimal directly linked.
        return ProgActivityAnimal.getPARegisterDict()       # {paObj: ActivityID}

    def _classExecuteFields(self, **kwargs) -> dict:
        """ Function used to create the executeFields dict to be passed to _paMatchAndClose() method.
        Defines all Execution fields to compare against Close fields in ProgActivities and determine if self is a
        Closing Activity for the ProgActivities it is checked against.
        @return: dict with updated execution values for the call. ONLY REQUIRED FIELDS MUST BE INCLUDED. NOTHING ELSE!!!
        """
        kwargs.update({'fldFK_ClaseDeAnimal': self.outerObject.animalClassID()})
        return kwargs
                # 'execution_date': execution_date,
                # 'fldFK_ClaseDeAnimal': self.outerObject.animalClassID,
                # 'ID': self.outerObject.ID,
                # 'fldAgeDays': self.outerObject.age.get(),
                # 'fldFK_Localizacion': self.outerObject.lastLocalization,
                # 'fldFK_Raza': self.outerObject.animalRace,
                # 'fldMF': self.outerObject.mf,
                # 'fldFK_Categoria': [self.outerObject.lastCategoryID]
               #  })

    # ------------ Funciones de Categoria, generales para TODAS las clases de Animal. Vamos empezando por aqui,... ---#

    def _setCategory(self, *args, excluded_fields=None, execute_fields=None, **kwargs):
        """
        @param args: DataTable objects with obj_data to write to DB
        @param enforce: =True: forces the Category val irrespective of __statusPermittedDict conditions
        @param category: Category number to set, if Category number is not passed via tblData
        @return: idActividadRA if record created.
                 None if record not created (same category)
                 errorCode (str) if invalid category
        """
        outerObj = self.outerObject
        tblData = next((j for j in args if j.tblName == self._tblDataName), DataTable(self._tblDataName))
        # Procesa Categoria, valida categoryID: Todo este mambo es para aceptar categorias como nombre y/o como ID
        categoryID = tblData.getVal(0, 'fldFK_Categoria')
        if categoryID not in outerObj.categories.values():
            category = next((bool(kwargs[j]) for j in kwargs if 'category' in j.lower()), '')
            categID = removeAccents(category)       # Here, a categoryName may have been passed. Must check for that.
            categoryID = categID if categID in outerObj.categories.values() else \
                next((v for k, v in outerObj.categories.items() if removeAccents(str(k))
                      == categID), None)
        if categoryID is None:
            retValue = f'ERR_INP_ArgumentsNotValid: Category not valid or missing: {tblData.getVal(0, "fldFK_Categoria")} ' \
                       f'{callerFunction(getCallers=True)}'
            # krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue

        # Prioridad eventDate:    1: fldDate(tblData); 2: kwargs(fecha valida); 3: timeStamp.
        timeStamp = time_mt('datetime')
        eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), timeStamp=timeStamp, **kwargs)
        tblData.setVal(0, fldDate=eventDate)
        lastCategory = self.outerObject.category.get()  # No problem with this recursive call on category Activity.
        # Aqui abajo, self accede al diccionario __permittedFrom de la clase de Animal correcta.
        enforce = next((bool(kwargs[j]) for j in kwargs if 'enforce' in j.lower()), None)
        if categoryID and (categoryID in self.permittedFrom()[f"{lastCategory}"] or enforce):
            # 1st clears "ternero"/"ternera": these 2 categories are by date only and cannot be set for an animal with
            # age out of range for ternero/ternera.
            if categoryID in [k for k, v in self.categ_names.items() if 'terner' in v.lower()]:
                if outerObj.age.get() > self.getAgeLimits().get(categoryID, 0):
                    return lastCategory  # Ignores category change if trying to set ternero/ternera with too high an age

            flagExecInstance = (lastCategory != categoryID)  # Flag to invoke PA creation method.
            tblRA = next((j for j in args if j.tblName == self._tblRAName), DataTable(self._tblRAName))
            tblLink = next((j for j in args if j.tblName == self._tblLinkName), DataTable(self._tblLinkName))
            activityID = tblRA.getVal(0, 'fldFK_NombreActividad')
            activityID = activityID if activityID else self._activityID
            tblRA.setVal(0, fldFK_NombreActividad=activityID)
            tblData.setVal(0, fldFK_Categoria=categoryID, fldComment='')
            idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)
            retValue = idActividadRA

            if isinstance(retValue, int):
                # THIS CODE DEPRECATED SINCE THE ABANDONMENT OF lastCategory memory data.
                # """ En caso e reentry solo escribira en memoria el valor de eventDate mas alto disponible """
                # with self.__lock:  # lock: the whole block must be atomic to ensure integrity of what's written to mem.
                #     if self.outerObject.lastCategoryID:
                #         if self.outerObject._lastCategoryID[1] < eventDate:
                #             # Writes items to memory list only if data in memory is from a date EARLIER than eventDate.
                #             self.outerObject.lastCategoryID = [categoryID, eventDate]
                #     else:
                #         self.outerObject.lastCategoryID = [categoryID, eventDate]  # Should be atomic (I hope...)

                # Sets the proper castration value for _flagCastrado based on the category being set.
                if outerObj.mf == 'm':
                    castration_date = eventDate if categoryID in self.categ_castrated() else 0
                    self.outerObject.castration.set(event_date=castration_date)


                # Verificar si hay hay que cerrar alguna(s) PA con con esta actividad como "cerradora".
                if self._supportsPA:
                    excluded_fields = set(excluded_fields) if isinstance(excluded_fields,
                                                                         (list, set, tuple, dict)) else set()
                    if isinstance(retValue, int) and self._supportsPA:
                        executeFields = self._classExecuteFields(execution_date=eventDate,
                                                                 category=self.outerObject.lastCategoryID)
                        if execute_fields and isinstance(execute_fields, dict):
                            executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
                        self._paMatchAndClose(retValue, execute_fields=executeFields, excluded_fields=excluded_fields,
                                              **kwargs)   # This call is executed asynchronously by another thread!!
                        # Updates cols in tblLink, so that external nodes can access Execute Data, Excluded Fields.
                        fldID_Link = tblLink.getVal(0, 'fldID')
                        if fldID_Link:
                            setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
                                      fldExcludedFields=excluded_fields)

                        print(f'\n_________This thread is: {threading.current_thread().name}____________ _'
                              f'setCategory _matchAndClose. outerObject: {self.outerObject.ID}')

                if flagExecInstance:
                    # This Activity itself is a Programmed Activity
                    self._paCreateExecInstance(outer_obj=self.outerObject)     # This one also goes to a queue.
            else:
                retValue = f'ERR_Sys_Object not valid or activity not defined: {retValue}- {callerFunction()}'
                krnl_logger.info(retValue)
                print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)

        elif categoryID == lastCategory:
            retValue = True   # -> True si no hay cambio de Categoria, porque no se genero idActividadRA.
        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - ID: {self.outerObject.ID} current Category:' \
                       f'{self.outerObject.lastCategoryID}; required Category:{categoryID}'
            krnl_logger.warning(f'{moduleName()}({lineNum()}  - {retValue}')
            print(f'{moduleName()}({lineNum()}  - {retValue}', dismiss_print=DISMISS_PRINT)

        return retValue

    def _getCategory(self, sDate='', eDate='', *args, event_date=False, **kwargs):
        """
        Returns records in table Data Animales Categorias between sValue and eValue. sValue=eValue ='' ->Last record
        If the last update of the value (Category in this case) is less than 1 day old, returns value from memory.
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param event_date: True -> returns the date lastCategoryID was set (_lastCategoryID[1])
        @param kwargs: mode='value' -> Returns value from DB.
                       id_type='name'-> Category Name; id_type='id' or no id_type -> Category ID.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        fldName = 'fldFK_Categoria'
        id_type = kwargs.get('id_type') or None
        retValue = None
        t = time_mt('datetime')
        # Todo esto para aprovechar los datos en memoria y evitar accesos a DB.
        outer_obj = self.outerObject
        modeArg = 'value'
        tblRA = setupArgs(self._tblRAName, *args)  # __tblRAName viene de class Activity
        tblLink = setupArgs(self._tblLinkName, *args)  # __tblLinkName viene de class Activity
        tblData = setupArgs(self._tblDataName, *args, **kwargs)  # __tblDataName seteado INDIVIDUALMENTE.
        qryTable = self._getRecordsLinkTables(tblRA, tblLink, tblData)
        if isinstance(qryTable, DataTable):
            if qryTable.dataLen <= 1:
                result = qryTable
            else:
                result = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
            if event_date is True:
                return result.getVal(0, 'fldDate')

            categID = result.getVal(0, fldName)
            if 'val' in modeArg.lower():
                # Retorna PRIMER o ULTIMO Registro en funcion de valores de sDate, eDate.
                if not id_type or 'id' in id_type.lower():
                    # Checks against existing category and re-compute if they are different.
                    computedCategory = self.outerObject.category.compute(initial_category=categID)
                    if categID != computedCategory:
                        #  TODO: Sets only categID in tblData for this write to DB. CHECK HOW IT WORKS!
                        tblRA.setVal(0, fldID=None)     # Clears fldID to force creation of a new record.
                        tblLink.setVal(0, fldID=None, fldFK=None)   # All other parameters should be OK in tables.
                        tblData.setVal(0, fldID=None, fldFK_Categoria=computedCategory,
                                       fldComment='Category set from get().')
                        tblRA.setVal(0, fldFK_NombreActividad=self._activityID)
                        idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)     # Sets the new category.
                        if isinstance(idActividadRA, int):
                            categID = computedCategory
                    retValue = categID  # returns computed category if success or original category if call fails.
                else:
                    # Returns categoryName.
                    retValue = next((j for j in outer_obj.categories if outer_obj.categories[j] == categID), None)
            else:
                retValue = result   # Retorna DataTable con registros.
        return retValue




    # def _getCategory01(self, sDate='', eDate='', *args, event_date=False, **kwargs):
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
    #     modeArg = kwargs.get('mode') or 'mem'
    #     id_type = kwargs.get('id_type')
    #     retValue = None
    #     t = time_mt('datetime')
    #     # Todo esto para aprovechar los datos en memoria y evitar accesos a DB.
    #     outer_obj = self.outerObject
    #     # if outer_obj.supportsMemData and (not modeArg or 'mem' in modeArg.lower()) and \
    #     #         t - outer_obj._lastCategoryID[1] <= timedelta(minutes=15):
    #     #     # If data was pulled less than 15 minutes ago, returns memory data.
    #     #     if not id_type or 'id' in id_type.lower():  #  Returns data from memory (categoryID)
    #     #         retValue = outer_obj.lastCategoryID if not event_date else outer_obj._lastCategoryID[1]
    #     #     else:
    #     #         retValue = next((j for j in outer_obj.categories if outer_obj.categories[j] ==
    #     #                          outer_obj.lastCategoryID), None)   # Returns data from memory (categoryName)
    #     #     return retValue
    #
    #     modeArg = 'value'
    #     tblRA = setupArgs(self._tblRAName, *args)  # __tblRAName viene de class Activity
    #     tblLink = setupArgs(self._tblLinkName, *args)  # __tblLinkName viene de class Activity
    #     tblData = setupArgs(self._tblDataName, *args, **kwargs)  # __tblDataName seteado INDIVIDUALMENTE.
    #     qryTable = self._getRecordsLinkTables(tblRA, tblLink, tblData)
    #     if isinstance(qryTable, DataTable):
    #         if qryTable.dataLen <= 1:
    #             result = qryTable
    #         else:
    #             result = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
    #         if event_date is True:
    #             return result.getVal(0, 'fldDate')
    #
    #         categID = result.getVal(0, fldName)
    #         # outer_obj.lastCategoryID = [categID, t]     # Sets values in memory for compute() to work.
    #         # categID = outer_obj.lastCategoryID      # Gets the latest category, after compute() updated it.
    #         if 'val' in modeArg.lower():
    #             # Retorna PRIMER o ULTIMO Registro en funcion de valores de sDate, eDate.
    #             if not id_type or 'id' in id_type.lower():
    #                 retValue = categID
    #             else:
    #                 retValue = next((j for j in outer_obj.categories if outer_obj.categories[j] ==
    #                                  categID), None)
    #         else:
    #             retValue = result   # Retorna DataTable con registros.
    #     return retValue
    #



    # def _getCategory00(self, sDate='', eDate='', *args, event_date=False, **kwargs):
    #     """
    #     Returns records in table Data Animales Categorias between sValue and eValue. sValue=eValue ='' ->Last record
    #     @param sDate: No args: Last Record; sDate=eDate='': Last Record;
    #                     sDate='0' or eDate='0': First Record
    #                     Otherwise: records between sDate and eDate
    #     @param eDate: See @param sDate.
    #     @param kwargs: mode='value' -> Returns value from DB.
    #                    id_type='name'-> Category Name; id_type='id' or no id_type -> Category ID. All from memory value.
    #     @return: Object DataTable with information from queried table or statusID (int) if mode=val
    #     """
    #     fldName = 'fldFK_Categoria'
    #     modeArg = kwargs.get('mode')
    #     id_type = kwargs.get('id_type')
    #     retValue = None
    #
    #     # Todo esto para aprovechar los datos en memoria y evitar accesos a DB.
    #     if self.outerObject.supportsMemData and (not modeArg or 'mem' in modeArg.lower()):
    #         if not id_type or 'id' in id_type.lower():  #  Returns data from memory (categoryID)
    #             retValue = self.outerObject.lastCategoryID if not event_date else self.outerObject._lastCategoryID[1]
    #         else:
    #             retValue = next((j for j in self.outerObject.categories if self.outerObject.categories[j] ==
    #                              self.outerObject.lastCategoryID), None)   # Returns data from memory (categoryName)
    #         return retValue
    #
    #     modeArg = 'value'
    #     tblRA = setupArgs(self._tblRAName, *args)  # __tblRAName viene de class Activity
    #     tblLink = setupArgs(self._tblLinkName, *args)  # __tblLinkName viene de class Activity
    #     tblData = setupArgs(self._tblDataName, *args, **kwargs)  # __tblDataName seteado INDIVIDUALMENTE.
    #     qryTable = self._getRecordsLinkTables(tblRA, tblLink, tblData)
    #     if isinstance(qryTable, DataTable):
    #         if qryTable.dataLen <= 1:
    #             result = qryTable
    #         else:
    #             result = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
    #         if event_date is True:
    #             return result.getVal(0, 'fldDate')
    #
    #         categID = result.getVal(0, fldName)
    #         if 'val' in modeArg.lower():
    #             # Retorna PRIMER o ULTIMO Registro en funcion de valores de sDate, eDate.
    #             if not id_type or 'id' in id_type.lower():
    #                 retValue = categID
    #             else:
    #                 retValue = next((j for j in self.outerObject.categories if self.outerObject.categories[j] ==
    #                                  categID), None)
    #         else:
    #             retValue = result   # Retorna DataTable con registros.
    #     return retValue

    @property
    def activityID(self):
        return self._activityID

    @property
    def activityName(self):
        return self._activityName

    @classmethod
    def tblDataProgramacionName(cls):
        return cls.__tblProgActivitiesName






    # @classmethod
    # def processReplicated00(cls):  # Old version. ALL THIS IS ALREADY REPLACED BY Animales trigger AFTER INSERT.!!
    #     """             ******  Run periodically as IntervalTimer func. ******
    #                     ****** This code should (hopefully) execute in LESS than 5 msec (switchinterval).   ******
    #     Used to execute logic for detection and management of INSERTed, UPDATEd and duplicate objects.
    #     Defined for Animal, Tag, Person, Geo.
    #     Checks for additions to tblAnimales from external sources (replication from other nodes) for Valid and
    #     Active objects. Updates _fldID_list, _object_fldUPDATE_dict
    #     @return: True if update operation succeeds, False if reading tblAnimales from db fails.
    #     """
    #     sql = f'SELECT * from "{cls.tblObjDBName()}" WHERE "Salida YN" == 0 OR "Salida YN" IS NULL; '
    #     temp = dbRead(cls.tblObjName(), sql)
    #     if not isinstance(temp, DataTable) or not temp:
    #         return False
    #
    #     # 1. INSERT -> Checks for INSERTED new Records verifying the status of the last-stored fldID col and locally
    #     # added records then, process repeats/duplicates.
    #     krnl_logger.info(f"---------------- INSIDE {cls.__class__.__name__}._processDuplicates()!! -------------------")
    #     pulled_fldIDCol = temp.getCol('fldID')
    #     newRecords = set(pulled_fldIDCol).difference(cls._fldID_list)
    #     if newRecords:
    #         newRecords = list(newRecords)
    #         pulledIdentifiersCol = temp.getCol('fldTagNumber')  # List of lists. Each of the lists contains UIDs.
    #         fullIdentifiers = []  # List of ALL identifiers from temp table, to check for repeat objects.
    #         for lst in pulledIdentifiersCol:
    #             if isinstance(lst, (list, tuple, set)):
    #                 fullIdentifiers.extend(lst)
    #             else:
    #                 fullIdentifiers.append(lst)
    #         fullIdentifiers = set(fullIdentifiers)
    #         # TODO(cmt): here runs the logic for duplicates resolution for each of the uids in newRecords.
    #         for j in newRecords:  # newRecords: ONLY records NOT found in _fldID_list from previous call.
    #             # TODO: Pick the right cls (Bovine, Caprine, etc).
    #             obj_dict = temp.unpackItem(pulled_fldIDCol.index(j))
    #             objClass = Tag
    #             obj = objClass(**obj_dict)
    #
    #             # If record is repeat (at least one of its identifiers is found in cls._identifiers), updates repeat
    #             # records for databases that might have created repeats. Changes are propagated by the replicator.
    #             identifs = obj.tagNumber  # get_identifiers_dict() returns set of identifiers UIDs.
    #
    #             """ Checks for duplicate/repeat objects: Must determine which one is the Original and set the record's
    #             fldObjectUID field with the UUID of the Original object and fldExitDate to flag it's a duplicate.
    #             """
    #             # Note: the search for common identifiers and the logic of using timeStamp assures there is ALWAYS
    #             # an Original object to fall back to, to ensure integrity of the database operations.
    #             if fullIdentifiers.intersection(identifs):
    #                 # TODO(cmt): Here detected duplicates: Assigns Original and duplicates based on myTimeStamp.
    #                 for o in objClass.getRegisterDict().values():
    #                     if o.tagNumber == identifs:
    #                         if o.myTimeStamp <= obj.myTimeStamp:
    #                             original = o
    #                             duplicate = obj
    #                         else:
    #                             original = obj
    #                             duplicate = o
    #                         original.tagNumber = duplicate.tagNumber  # for data integrity.
    #                         setRecord(cls.tblObjName(), fldID=duplicate.recordID, fldObjectUID=original.ID,
    #                                   fldExitDate=time_mt('datetime'))
    #                         setRecord(cls.tblObjName(), fldID=original.recordID, fldTagNumber=original.tagNumber)
    #                         break
    #             elif obj_dict.get('fldTerminal_ID') != TERMINAL_ID:
    #                 # If record is not duplicate and comes from another node, adds it to __registerDict.
    #                 obj.register()
    #
    #     # 2. UPDATE - Checks for UPDATED records modified in other nodes and replicated to this database. Checks are
    #     # performed based on value of fldUPDATE field (a dictionary) in each record.
    #     # The check for the node generating the UPDATE is done here in order to avoid unnecessary setting values twice.
    #     # UPDATEDict = {temp.getVal(j, 'fldID'): temp.getVal(j, 'fldUPDATE') for j in range(temp.dataLen) if
    #     #               temp.getVal(j, 'fldUPDATE')}  # Creates dict only with non-NULL (populated) items.
    #     UPDATEDict = {}
    #     for j in range(temp.dataLen):
    #         if temp.getVal(j, 'fldUPDATE'):
    #             d1 = temp.unpackItem(j)
    #             UPDATEDict[d1['fldID']] = d1['fldUPDATE']
    #     changed = {k: UPDATEDict[k] for k in UPDATEDict if k not in cls._object_fldUPDATE_dict or
    #                (k in cls._object_fldUPDATE_dict and UPDATEDict[k] != cls._object_fldUPDATE_dict[k])}
    #     if changed:  # changed = {fldID: fldUPDATE(int), }
    #         for k in changed:  # updates all records in local database with records updated by other nodes.
    #             # Update memory structures here: __registerDict, exitDate, etc, based on passed fldNames, values.
    #             changedRecord = temp.unpackItem(fldID=k)
    #             objClass = Tag
    #             if objClass:
    #                 obj = next((o for o in objClass.getRegisterDict().values() if o.recordID == k), None)
    #                 if obj:
    #                     # TODO(cmt): UPDATEs obj specific attributes with values from read db record.
    #                     obj.updateAttributes(**changedRecord)
    #                     if changedRecord.get('fldObjectUID', 0) != obj.ID:
    #                         obj.setID(changedRecord['fldObjectUID'])
    #                     if changedRecord.get('fldDateExit', 0) != obj.exitDate:
    #                         obj.setExitDate(changedRecord['fldDateExit'])
    #                         obj.isActive = False
    #                         obj.unregister()
    #             # Updates _object_fldUPDATE_dict (of the form {fldID: UUID(str), }) to latest values.
    #             cls._object_fldUPDATE_dict[k] = changed[k]
    #
    #     # 3. BAJA / DELETE -> For DELETEd Records and records with fldExitDate !=0, removes object from __registerDict.
    #     temp1 = getRecords(cls.tblObjName(), '', '', None, '*')
    #     if not isinstance(temp, DataTable):
    #         return False
    #     # Removes from __registerDict Tag with fldExitDate (exit_recs) and DELETE (deleted_recs) executed in other nodes
    #     all_recs = temp1.getCol('fldID')
    #     exit_recs = set(all_recs).difference(pulled_fldIDCol)  # records with fldExitDate != 0.
    #     remove_recs = exit_recs.difference(cls._fldID_exit_list)  # Compara con lista de exit Records ya procesados.
    #     deleted_recs = set(cls._fldID_list).difference(pulled_fldIDCol)
    #     remove_recs = remove_recs.union(deleted_recs) or []
    #     for i in remove_recs:
    #         obj = next((o for o in cls.getRegisterDict().values() if o.recordID == i), None)
    #         if obj:
    #             obj.unregister()
    #
    #     # Updates list of fldID and list of records with fldExitDate > 0 (Animales con Salida).
    #     cls._fldID_list = pulled_fldIDCol.copy()  # [o.recordID for o in cls.getRegisterDict().values()]
    #     cls._fldID_exit_list = exit_recs.copy()
    #     return True

# --------------------------------------------- Fin Class AnimalActivity --------------------------------------------- #


# ========= GenericActivity Class used to... ========= #
class GenericActivityAnimal(AnimalActivity):                # GenericActivity Class for Animal.
    """
    Enables the creation of multiple AnimalActivity objects, sparing the creation of a full Activity Class when simple
    getter/setter functions are needed.
    Data required to fully initialize an object: activityName, activityID, tblData and query field in the form
    "tblName.fldName", and filter_fld_name. These 4 items read from [Animales Actividades Nombres] table.
    Instance objects are created in Animal class as @property.
    ***** Additional external methods can be attached to individual objects via the register_method() method *****
    """
    __method_name = 'generic_activity'  # Defines __method_name to be included in Activity._activity_class_register
    # methods not to be deleted / unregistered.
    __reserved_names = ('get', 'set', 'register_method', 'unregister_method','methods_wrapper','_createGenericActivityObjects')
    __initialized = False

    def __new__(cls, activName, *args, **kwargs):  # Override para crear objeto nuevo solo si Activity no esta definida
        if activName not in cls.getActivitiesDict():
            raise NameError(f'ERR_INP_Invalid Activity: {activName}. Activity not created.')
        if activName in cls.definedActivities():
            cls.__initialized = True                      # Flag para evitar re-inicializacion de la instancia.
            return cls.definedActivities()[activName]     # Retorna objeto Activity ya creado, si existe.
        else:
            instance = super().__new__(cls)  # Crea objeto si el numero este dentro del limite de objs per thread
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

        self.__qryFldName = qry_fld_names   # Field Name to be queried for Activity in get() method. Only 1 field.
        self.__filterFldname = filter_field_name
        self.__oneShot = one_shot        # TODO(cmt): identify one-time only activities (weaning, castracion, etc).
        super().__init__(activName, *args, activity_enable=activityEnableFull, tbl_data_name=tbl_data_name,
                         excluded_fields=excluded_fields, decorator_name=decorator_name, **kwargs)
        self.definedActivities()[self._activityName] = self


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

        # Support for "short" form: if no records in tblData, set ONLY the query_field in tblData.
        if not tblData.dataLen:
            if val and self.__qryFldName != '*':
                tblData.setVal(0, **{self.__qryFldName: val})
            else:
                return False

        if self._isValid:  # and self.outerObject.validateActivity(self.__activityName):
            timeStamp = time_mt('datetime')  # Prioridad: 1: fldDate(tblData); 2: kwargs(fecha valida); 3: timeStamp.
            eventDate = getEventDate(tblDate=tblData.getVal(0, 'fldDate'), timeStamp=timeStamp, **kwargs)
            tblRA.setVal(0, fldFK_NombreActividad=self._activityID)
            tblLink.setVal(0, fldFK=self.outerObject.ID)
            tblData.setVal(0, fldDate=eventDate)  # Mismo valor para DB y para memoria.
            if self.outerObject.supportsMemData:  # parametros para actualizar variables en memoria se pasan en
                tblData.setVal(0, fldMemData=1)  # Activa flag Memory Data en el registro que se escribe en DB
            idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, **kwargs)
            retValue = idActividadRA

            # TODO: Verificar si hay hay que cerrar alguna(s) PA con con esta actividad como closing Activity.
            if self._supportsPA:
                if self._supportsPA:
                    executeFields = self._classExecuteFields(execution_date=tblData.getVal(0, 'fldDate'))
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
        qryTable = self._getRecordsLinkTables(tblRA, tblLink, tblData)
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

    def register_method(self, method_obj=None):
        """
        Registers method_obj (an external function) as a method in GenericActivity class. After registration, method_obj
        can be accessed by its name as a regular attribute of GenericActivity.
        *** IMPORTANT: method_obj MUST define self as its first positional argument. Ex: my_func(self, *args, **kwargs).
        Also IMPORTANT: if method_obj is already defined, any new call to register_method() with the same method_obj
        will OVERRIDE the existing attribute. This is by design to allow the implementation of updates to all registered
        attributes.
        @param method_obj: callable (a function name)
        @return: True if registration successful. None if otherwise.
        """
        if not callable(method_obj):
            return None
        setattr(self, method_obj.__name__, self.methods_wrapper(method_obj))  # sets decorated method as the callable.
        return True

    def methods_wrapper(self, func):
        """
        Used to pass self (a GenericActivity instance) to func so that self, self.outerObject are available to func.
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
            if method_name.strip() in self.__reserved_names:
                return None             # ignores reserved method names
            delattr(self, method_name)
        return None

@singleton
class InventoryActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadInventario'
    __activityName = 'Inventario'
    _short_name = 'invent'        # Used by Activity._paCreate(), Activity.__isClosingActivity()
    # TODO: __method_name could be read from DB. Already in [Actividades Nombres]. Does it make sense?
    __method_name = 'inventory'  # Used in ActivityMethod to create the InventoryActivity object and the callable property.

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True     # Inventory does support PA.
        kwargs['decorator'] = self.__method_name
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)

    # def __call__(self, caller_object, *args, **kwargs):
    #     # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {(item_object, *args)}; kwargs: {kwargs}')
    #     self.outerObject = caller_object  # item_object es instance de Animal, Tag, etc. NO PUEDE SER class
    #     return self  # porque el 'self' de __call__ es un @property y no hace bind to classes

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
        tblLink = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblLinkName), None)
        if tblLink is None:
            args = list(args)
            tblLink = DataTable(self._tblLinkName)
            args.append(tblLink)
        retValue = self._setInventory(*args, **kwargs)

        if isinstance(retValue, int) and self._supportsPA:
            excluded_fields = set(excluded_fields) if isinstance(excluded_fields, (list, set, tuple, dict)) else set()
            excluded_fields.update(self.getActivityExcludedFieldsClose(self.__activityName))
            executeFields = self._classExecuteFields(execution_date=self.outerObject.inventory.get())
            if execute_fields and isinstance(execute_fields, dict):
                executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
            self._paMatchAndClose(retValue, execute_fields=executeFields, excluded_fields=excluded_fields)
            # Updates cols in tblLink, so that external nodes can access Execute Data, Excluded Fields.
            fldID_Link = tblLink.getVal(0, 'fldID')    # Uses fact that tblLink argument is updated by _setInventory().
            if fldID_Link:
                setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
                          fldExcludedFields=excluded_fields)

            # print(f'({lineNum()}) returning from _paMatchAndClose() in Inventory set(). outerAttr still at {self._outerAttr()}')

        # TODO: For now, this one doesn't trigger creation of PA instances in tblLinkPA. We'll see later...
        return retValue

    def get(self, sDate='', eDate='', *args, **kwargs):
        """
        Returns ALL records in table Data Inventario between sValue and eValue. sValue=eValue ='' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode='value' -> Returns last value from DB. If no mode or mode='memory' returns value from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        return self._getInventory(sDate, eDate, *args, **kwargs)


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
    def __new__(cls, *args, **kwargs):  # TODO(cmt): Right way to do singletons. Allows for isinstance, type checks.
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
        tblLink = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblLinkName), None)
        if tblLink is None:
            args = list(args)
            tblLink = DataTable(self.__tblLinkName)
            args.append(tblLink)
        retValue = self._setStatus(*args, status=status, **kwargs)
        if isinstance(retValue, int):
            if self.doInventory(**kwargs):
                _ = self.outerObject.inventory.set()
            if self._supportsPA:
                excluded_fields = set(excluded_fields) if isinstance(excluded_fields, (list, set, tuple, dict)) else set()
                execute_date = self.outerObject._lastStatus[1]  # Gets the internal execution date for lastInventory.
                if isinstance(retValue, int) and self._supportsPA:
                    executeFields = self._classExecuteFields(execution_date=execute_date,
                                                             status=self.outerObject.lastStatus)
                    if execute_fields and isinstance(execute_fields, dict):
                        executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
                    self._paMatchAndClose(retValue, execute_fields=executeFields, excluded_fields=excluded_fields,
                                          **kwargs)  # TODO(cmt): This call is executed asynchronously by another thread
                    # Updates cols in tblLink, so that external nodes can access Execute Data, Excluded Fields.
                    fldID_Link = tblLink.getVal(0, 'fldID')  # tblLink argument is updated by _setStatus().
                    if fldID_Link:
                        setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
                                  fldExcludedFields=excluded_fields)
        return retValue


    def get(self, sDate='', eDate='', *args, **kwargs):
        """
        Returns records in table Data Status between sValue and eValue. If sValue = eValue = '' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode=val -> Returns val only from DB. If no mode, returns value from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        return self._getStatus(sDate, eDate, *args, **kwargs)


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


    def __init__(self, *args, **kwargs):
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = False
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def set(self, *args: DataTable, localization: Geo = None, **kwargs):      # TODO: CORREGIR ESTE METODO, usando objetos Geo
        """
        Inserts LocalizationActivityAnimal obj_data for the object "obj" in the DB.
        __outerAttr: TagObject para la que se realiza la Actividad. Lo setea el metodo inventario().
        @param localization: Geo object to set LocalizationActivity in short form (without passing full DataTables)
        @param date: Event Date (str)
        @param args: list of DataTable objects, with all the tables and fields to be written to DB.
                No checks performed. The function doing the write will discard and ignore all non valid arguments
        @param kwargs: recordInventory: 0->Do NOT call _setInventory() / 1: Call _setInventory(),
                        idLocalization = valid LocalizationActivityAnimal val, from table GeoEntidades
        @return: 0->ID_Actividad (Registro De Actividades) if success; errorCode (str) on error; None for nonValid
        """
        kwargs['localization'] = localization
        retValue = self._setLocalization(*args, **kwargs)
        if not isinstance(retValue, str):
            if self.doInventory(**kwargs):
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

    def __init__(self, *args, **kwargs):
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        # Flag to signal only-once activities (castracion, destete, salida, etc.)
        kwargs['one_time_activity'] = self.__one_time_activity
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def set(self, *args, event_date=0):
        """
        Sets castration date for Animal. Updates tables: Animales, Animales Registro De Actividades,
        Link Animales Actividades, Data Animales Castracion
        @param event_date: datetime or  0, False, None or '' -> Castration = NO
                                      Valid Date -> Castration = YES with Date / 1 -> Castration = YES without Date.
        @return: flagCastrado (datetime, int) or ERR_ (string)
        """
        # Si es 0, False, None: _fldFlagCastrado = 0 (No castrado)
        # Fecha valida -> _fldFlagCastrado=datetime obj. En cualquier otro caso: 1 -> Castrado,pero no se conoce Fecha
        if self.outerObject.isCastrated:
            return self.outerObject.isCastrated
        # elif event_date and not isinstance(event_date, datetime):
        #     # Si ya esta castrado o si no es fecha valida y no es cero/None, sale sin setear nada.
        #     return f'ERR_INP_Invalid Argument(s) castration: {event_date}'

        with self.__lock:
            self.outerObject.isCastrated = event_date if isinstance(event_date, datetime) else bool(event_date) * 1
        tblObjects = DataTable(self.__tblObjectsName)
        tblObjects.setVal(0,fldFlagCastrado=self.outerObject.isCastrated, fldFK_UserID=sessionActiveUser, fldID=self.ID)
        _ = setRecord(tblObjects.tblName, **tblObjects.unpackItem(0))  # Setea _flagCastrado en tblAnimales
        tblRA = DataTable(self._tblRAName, *args)
        tblLink = DataTable(self._tblLinkName, *args)
        tblData = DataTable(self.__tblDataName, *args, fldDate=self.outerObject.isCastrated)
        activityID = AnimalActivity.getActivitiesDict().get(self.__activityName)
        tblRA.setVal(0, fldFK_NombreActividad=activityID)
        _ = self._createActivityRA(tblRA, tblLink, tblData)
        if isinstance(_, str):
            krnl_logger.error(f'ERR_DBAccess. Cannot read from tbl {tblData.tblName}. Error: {_}')
            return _
        return self.outerObject._flagCastrado

    def get(self):
        return self.outerObject._flagCastrado


@singleton
class AgeActivityAnimal(AnimalActivity):
    """ Age class is defined in order to standardize the logic in ProgActivities, using age as a condition in the same
     way as other Activities that are also conditions """
    _implements__call__ = True      # Flags that Activity implements __call__(), hence it's callable.
    __tblDataName = 'tblAnimales'
    __activityName = 'Edad'
    __method_name = 'age'
    _short_name = 'age'  # Used by Activity._paCreate(), Activity.__isClosingActivity()
    # __operators = {'lt': lt, 'le': le, 'gt': gt, 'ge': ge}      # TODO: For later implementation

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

    def set(self, *args: DataTable, meas_name: str = '', meas_value=None, meas_units: str = '', excluded_fields=None,
            execute_fields=None, **kwargs):
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

        tblRA = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblRAName),
                       DataTable(self._tblRAName))
        # Must add tblLink if not passed, needed to update tblLink record with fldExecuteData, fldExcludedFields.
        tblLink = next((j for j in args if isinstance(j, DataTable) and j.tblName == self._tblLinkName),
                       DataTable(self._tblLinkName))
        tblData = next((j for j in args if isinstance(j, DataTable) and j.tblName == self.__tblDataName),
                       DataTable(self.__tblDataName, **kwargs))
        m_id = tblData.getVal(0, 'fldFK_NombreMedicion') if tblData.dataLen else \
            self._measurementsDict_lower.get(meas_name)
        try:
            m_unit_id = tblData.getVal(0, 'fldFK_Unidad') if tblData.dataLen else \
                (self._unitsDict_lower.get(meas_units)[0] if meas_units in self._unitsDict_lower else
                 next((self._unitsDict_lower.get(k)[0] for k in self._unitsDict_lower if meas_units in
                       self._unitsDict_lower.get(k)[1]), None))
        except IndexError:
            m_unit_id = None
        m_value = tblData.getVal(0, 'fldValue') if tblData.dataLen else meas_value
        if m_unit_id not in [j[0] for j in list(self._unitsDict.values())]:
            return f'ERR_INP_Argument(s): Invalid or missing units {meas_units}.'
        if m_id not in self._measurementsDict.values():
            return f'ERR_INP_Argument(s): Invalid or missing measurement name {meas_name}({m_id}).'
        if m_value is None or m_value == VOID:          # 0, '' are valid measurement values.
            return f'ERR_INP_Argument(s): Measurement value missing.'

        if not tblData.dataLen:
            tblData.setVal(0, fldFK_NombreMedicion=m_id,  fldValue=m_value, fldFK_Unidad=m_unit_id)
        tblRA.setVal(0, fldFK_NombreActividad=self._activityID)
        tblLink.setVal(0, fldExcludedFields=excluded_fields)
        retValue = self._createActivityRA(tblRA, tblLink, tblData)
        if isinstance(retValue, str):
            krnl_logger.error(f'ERR_DBAccess. Cannot read from tbl {tblData.tblName}. Error: {retValue}')
            return retValue

        if self._supportsPA:
            excluded_fields = set(excluded_fields) if isinstance(excluded_fields, (list, set, tuple, dict)) else set()
            excluded_fields.update(self.getActivityExcludedFieldsClose(self._activityName))
            executeFields = self._classExecuteFields(execution_date=tblData.getVal(0, 'fldDate'),
                                                     fldFK_NombreMedicion=m_id)
            if execute_fields and isinstance(execute_fields, dict):
                executeFields.update(execute_fields)  # execute_fields adicionales, si se pasan.
            self._paMatchAndClose(retValue, execute_fields=executeFields, excluded_fields=excluded_fields)
            # Updates fldExecuteData, fldExcludedFields in tblLink so that this data is replicated to other nodes.
            fldID_Link = tblLink.getVal(0, 'fldID')  # Uses fact that tblLink arg. is updated by _createActivityRA().
            if fldID_Link:
                setRecord(self._tblLinkName, fldID=fldID_Link, fldExecuteData=executeFields,
                          fldExcludedFields=excluded_fields)
        if self.doInventory(**kwargs):
            _ = self.outerObject.inventory.set()

        return retValue


    def get(self, *args: DataTable, sDate='', eDate='', meas_name: str = '', **kwargs):
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
        DataTable object if multiple records must be returned.
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
            qryTable = getRecords(self.__tblDataName, sDate, eDate, None, '*', fldFK_NombreMedicion=m_id)
            if isinstance(qryTable, DataTable):
                if qryTable.dataLen:
                    filteredTable = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
                    if filteredTable.dataLen <= 1:   # qryTable tiene 1 solo registro (Ultimo o Primero)
                        retValue = {'name': next((k for k in self._measurementsDict if self._measurementsDict[k] ==
                                                  filteredTable.getVal(0, 'fldFK_NombreMedicion')), None),
                                    'value': filteredTable.getVal(0, 'fldValue', VOID),
                                    'units': next((k for k in self._unitsDict if self._unitsDict[k][0] ==
                                                   filteredTable.getVal(0, 'fldFK_Unidad')), None)
                                    }
                        print(f'{moduleName()}({lineNum()}) retValue: {retValue}', dismiss_print=DISMISS_PRINT)
                    else:
                        # Returns DataTable with multiple records.
                        retValue = filteredTable
        return retValue


    def comp(self, meas_name: str, meas_unit: str, meas_value=None):  # Comp works with single values for now (27Sep23).
        lastVal = self.get(meas_name=meas_name)
        return bool(lastVal and lastVal.get('name').lower() == str(meas_name).lower() and
                    lastVal.get('units').lower() == str(meas_unit).lower() and
                    lastVal.get('value') == meas_value)


@singleton
class TMActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadTM'
    __tblMontosName = 'tblDataTMMontos'
    __activityName = 'Actividad MoneyActivity'
    __method_name = 'tm'
    _short_name = 'tm'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

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
        self.__tblMontos = setupArgs(self.__tblMontosName, *args)


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
        retValue = self._setTM(*args, **kwargs)
        if not isinstance(retValue, str):
            if self.doInventory(**kwargs):
                _ = self.outerObject.inventory.set()
        return retValue

    def get(self, sDate='', eDate='', *args, **kwargs):
        """
        Returns records in table [Data MoneyActivity Montos] between sValue and eValue. sValue=eValue ='' -> Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode=datatable->Returns DataTable with full record.  mode='fullRecord' -> Returns last Record
        in full.
        @return: Object DataTable with information from MoneyActivity Montos. Blank (val) object DataTable if nothing's found
        """
        retValue = None
        if self._isValid and self.outerObject.validateActivity(self.__activityName):
            tmActivityRecords = self._getRecordsLinkTables(self.__tblRA, self.__tblLink, self.__tblData)
            colAnimalActivity = tmActivityRecords.getCol('fldFK_ActividadTM')  # Recs c/ ID_ActividadTM de ID_Animal
            if len(colAnimalActivity) > 0:
                qryTable = getRecords(self.__tblMontosName, '', '', None, '*', fldFK_Actividad=colAnimalActivity)
                if qryTable.dataLen:
                    if qryTable.dataLen <= 1:
                        retValue = qryTable  # qryTable tiene 1 solo registro (Ultimo o Primero)
                    else:
                        retValue = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
                    print(f'{moduleName()}({lineNum()}) retTable: {retValue.fldNames} // Data: {retValue.dataList}',
                          dismiss_print=DISMISS_PRINT)
            else:
                retValue = self.__tblMontos     # Retorna DataTable blank (sin datos)
        return retValue

    # def comp(self, val):
    #     pass


@singleton
class TagActivityAnimal(AnimalActivity):
    __tblDataName = 'tblDataAnimalesActividadCaravanas'
    __tblObjectsName = 'tblCaravanas'
    __activityName = 'Caravaneo'
    __method_name = 'tags'
    _short_name = 'carava'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    def __init__(self, *args, **kwargs):
        isValid = True
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

    @staticmethod
    def __tagRegister():                # TODO: Tag.getRegisterDict() IS DEPRECATED. DO NOT USE!
        """ Returns full Tag registerDict. Link to class Tag data structures.  Internal use only (cannot call with
        @property tags)  """
        return Tag.getRegisterDict()


    def initializeTags(self, *args, **kwargs):
        """
        Assigns all active tags to ID_Animal (self), pulling the tags (previously assigned to the Animal)
        from [Data Animales Actividades Caravanas]. If tags are not found, leaves myTags as empty set.
        args: DataTables with info needed to process Tags. Passed as args to minimize DB reads.
            args[0]: Desasignados from RA: list of fldID of Activity #25 (Caravaneo - Desasignar)
        @return: None if all OK. ERR_ (str) if error.
        """
        if not self.outerObject.validateActivity(self._activityName):
            retValue = f'ERR_Sys_ActivityNotDefined - {callerFunction()}()'
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return None

        tagIDs = self.outerObject.get_identifiers_dict()      # list of tag UIDs
        objList = []
        for i in tagIDs:
            obj = Tag.getObject(i)
            if isinstance(obj, Tag):
                objList.append(obj)
        if objList:
            self.outerObject.setMyTags(*objList)
            # print(f'...Now setting tags: {self.outerObject.myTagIDs}')
            return True
        return None


    # def initializeTags00(self, *args, **kwargs):        # Old version
    #     """
    #     Assigns all active tags to ID_Animal (self), pulling the tags (previously assigned to the Animal)
    #     from [Data Animales Actividades Caravanas]. If tags are not found, leaves myTags as empty set.
    #     args: DataTables with info needed to process Tags. Passed as args to minimize DB reads.
    #         args[0]: Desasignados from RA: list of fldID of Activity #25 (Caravaneo - Desasignar)
    #     @return: None if all OK. ERR_ (str) if error.
    #     """
    #     if not self._isValid:
    #         retValue = f'ERR_Sys_ObjectNotValid - {callerFunction()}()'
    #         print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
    #         return retValue
    #     elif not self.outerObject.validateActivity(self._activityName):
    #         retValue = f'ERR_Sys_ActivityNotDefined - {callerFunction()}()'
    #         print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
    #         return retValue
    #
    #     tagObjects = [o for o in Tag.getRegisterDict().values() if o.ID in self.outerObject.get_identifiers_dict()]
    #     if len(tagObjects) != len(self.outerObject.get_identifiers_dict()):
    #         # TODO(cmt): "Work with what's available" here:
    #         # Some identifiers are not loaded as Tag objects in Tag.registerDict(). Try loading from Caravanas table
    #         # in case that memory-updating routines haven't processed the data just yet.
    #         Tag.processReplicated()     # This one should be re-entrant. It must be.
    #
    #     if tagObjects:
    #         self.outerObject.setMyTags(*tagObjects)
    #         # print(f'...Now setting tags: {[t.tagNumber for t in tagObjects]}')
    #         return True
    #
    #
    #     # Old code, for when __identifiers is not set in self.outerObject.
    #     dataRADesasignados = args[0] if args else \
    #         getRecords('tblAnimalesRegistroDeActividades', '', '', None, "*",
    #                    fldFK_NombreActividad=self.activities['Caravaneo - Desasignar'])
    #     __tblRA = setupArgs(self._tblRAName, *args)
    #     __tblLink = setupArgs(self._tblLinkName, *args)
    #     __tblData = setupArgs(self.__tblDataName, *args, **kwargs)
    #     # animalClassName = self.outerObject.animalClassName
    #
    #     # Registros de Asignacion/Desasignacion de Caravanas para el idAnimal
    #     animalTagRecords = self._getRecordsLinkTables(__tblRA, __tblLink, __tblData, activity_list=('Alta', 'Caravaneo',
    #                                                  'Caravaneo - Desasignar'))
    #     skipList = []  # fldID de Registros que se deben excluir por Desasignacion de Caravanas.
    #     idCaravanaList = []  # Lista de ID_Caravana que se deben asignar al Animal
    #     if dataRADesasignados.dataLen:
    #         for i in range(animalTagRecords.dataLen):  # Puebla skipList con idActividad de Caravanas desasignadas
    #             if animalTagRecords.getVal(i, 'fldFK_Actividad') in dataRADesasignados.dataList:
    #                 for j in range(animalTagRecords.dataLen):
    #                     if animalTagRecords.getVal(j, 'fldFK_Caravana') == animalTagRecords.getVal(i, 'fldFK_Caravana'):        # TODO: Aqui puede comparar UUIDs directamente.
    #                         skipList.append(animalTagRecords.getVal(j, 'fldFK_Actividad'))
    #                 skipList.append(animalTagRecords.getVal(i, 'fldFK_Actividad'))
    #
    #     for i in range(animalTagRecords.dataLen):  # PUebla idCaravanaList solo con Tags activos
    #         if animalTagRecords.getVal(i, 'fldFK_Actividad') is not None and \
    #                 animalTagRecords.getVal(i, 'fldFK_Actividad') not in skipList:
    #             idCaravanaList.append(animalTagRecords.getVal(i, 'fldFK_Caravana'))  # agrega ID_Caravana (UUID)
    #
    #     # Asigna Tags al Animal: Busca en Tag.__registerDict y si no esta ahi, crea la caravana.
    #     retValue = None
    #     if idCaravanaList:
    #         # idCaravanaList = [f'"{j}"' for j in idCaravanaList]
    #         tagRecords = getRecords('tblCaravanas', '', '', None, '*', fldObjectUID=idCaravanaList)
    #         if isinstance(tagRecords, str):
    #             retValue = f'ERR_Sys_CannotInitializeTag {idCaravanaList}'
    #             krnl_logger.error(retValue)
    #             return retValue
    #         else:
    #             idCaravanaList = tagRecords.getCol('fldObjectUID')  # OJO: getCol ignora (salta) registros vacios de _dataList
    #         for j in range(len(idCaravanaList)):
    #             if idCaravanaList[j] in self.__tagRegister():  # __registerDict Tag Class correspondiente
    #                 self.outerObject.setMyTags(self.__tagRegister()[idCaravanaList[j]])
    #             else:  # Si Tag no existe en TagRegisterDict la crea buscando el ID de la caravana en DB
    #                 tempTag = Tag(**tagRecords.unpackItem(j))  # Crea tag con datos de record fldID=idCaravanaList[j]
    #                 # Si existe ya un tag con el mismo UID para la clase de objeto outerObject, da WARNING y sale.
    #                 if any(self.__tagRegister()[j].ID == tempTag.ID and self.__tagRegister()[j].assignedToClass ==
    #                        tempTag.assignedToClass and not self.__tagRegister()[j].isAvailable
    #                        for j in self.__tagRegister()):
    #                     retValue = f'INFO_CannotAssignTag: Tag UID {tempTag.tagNumber} / {tempTag.ID} already in use. '\
    #                                f'Tag was not assigned to Object {self.outerObject.ID}'
    #                     krnl_logger.warn(retValue, stack_info=True)
    #                 else:
    #                     if tempTag.assignedToClass != self.outerObject.__class__.__name__:
    #                         _ = setRecord(self.__tblObjectsName, fldID=tempTag.recordID,
    #                                       fldAssignedToClass=self.outerObject.__class__.__name__)  # TODO:HAS to be name
    #                         if isinstance(_, str):
    #                             retValue = f'ERR_DBWrite: Cannot write table {self.__tblObjectsName}'
    #                             krnl_logger.error(retValue, stack_info=True)
    #                             # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
    #                             return retValue
    #                         else:
    #                             tempTag.assignedToClass = self.outerObject.__class__.__name__
    #                     tempTag.register()
    #                     self.outerObject.setMyTags(tempTag)
    #     return retValue


    # Animal: Caravaneo / Caravanas: Comision, Reemplazo, Reemision
    def assignTags(self, *args: DataTable, **kwargs):
        """
        Assigns Tags in *args to Animal. Updates tables in DB.
        @param args: DataTables to use in operation (tblRA for idActividadRA, tblLink for fldIDLink, etc)
        @param kwargs: tags=[Tag, ]. List of Tag Objects to assign (1 or more)
                        tagCommissionType='Comision'(Default), 'Reemplazo', 'Reemision'. sets the Activity for Tag
        @return: idActividad (int) or errorCode (str)
        """
        activityName = 'Caravaneo'
        activityID = self.activities[activityName]
        if not self._isValid or not self.outerObject.validateActivity(activityName):
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotValid - {callerFunction()}()'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue

        tags = kwargs.get('tags', [])
        tags = tags if hasattr(tags, '__iter__') else [tags, ]
        tags = [j for j in tags if isinstance(j, Tag)]
        if tags:
            __tblRA = setupArgs(self._tblRAName, *args)
            __tblLink = setupArgs(self._tblLinkName, *args)
            __tblData = setupArgs(self.__tblDataName, *args, **kwargs)
            activityID = activityID or __tblRA.getVal(0, 'fldFK_NombreActividad')
            __tblRA.setVal(0, fldFK_NombreActividad=activityID)

            # Valida que todos los tag IDs esten en tabla tblCaravanas
            tagRecords = getRecords(self.__tblObjectsName, '', '', None, '*', fldObjectUID=tuple([t.ID for t in tags]))
            if isinstance(tagRecords, str):
                retValue = f'ERR_DBRread: Cannot read table {self.__tblObjectsName}'
                krnl_logger.error(retValue, stack_info=True, exc_info=True)
                # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
                return retValue
            idCol = tagRecords.getCol('fldObjectUID')

            wrtTables = []
            permittedTagAssignStatus = (1, 3, None, '')  # Alta:1, Decomisionada:3
            tagActivity = next((kwargs[j] for j in kwargs if 'commission' in str(j).lower()
                                or 'comision' in str(j).lower()), 'Comision')
            tagActivity = removeAccents(tagActivity) if tagActivity in Tag.getActivitiesDict() else 'Comision'
            tagStatus = 'Comisionada'
            # Setea Actividad para Caravanas. MISMA Actividad para todos los tags
            if 'emplazo' in tagActivity:
                tagActivity = 'Comision - Reemplazo'
                tagStatus = 'Reemplazada'
            elif 'emision' in tagActivity or 'emission' in tagActivity:
                tagActivity = 'Comision - Reemision'
                tagStatus = 'Comisionada'
            else:
                pass

            timeStamp = time_mt('datetime')
            eventDate = timeStamp
            userID = __tblRA.getVal(0, 'fldFK_UserID') if __tblRA.getVal(0, 'fldFK_UserID') is not None else \
                sessionActiveUser
            # Escritura basica de tablas para insertar el tag en la tabla de Caravanas del Animal
            __tblRA.setVal(0, fldFK_NombreActividad=activityID, fldTimeStamp=timeStamp, fldFK_UserID=userID)
            idActividadRA = setRecord(__tblRA.tblName, **__tblRA.unpackItem(0))
            if isinstance(idActividadRA, str):
                retValue = f'ERR_DB_WriteError: {idActividadRA} - Func/Method: MyTags.assignTags()'
                krnl_logger.error(retValue)
                print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
                # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
                return retValue
            # Setea valores en tabla Link
            commentLink = __tblLink.getVal(0, 'fldComment') if __tblLink.getVal(0, 'fldComment') else "" + \
                                                            f'Activity: {activityName} / {timeStamp}'
            __tblLink.setVal(0, fldFK_Actividad=idActividadRA, fldFK=self.outerObject.getID, fldComment=commentLink)
            __tblLink.undoOnError = True  # Se deben deshacer escrituras previas si falla escritura
            wrtTables.append(__tblLink)  # Si no se paso idActividad RA, escribe tabla Link

            for t in tags:
                if isinstance(t, Tag) and t.status.get() in permittedTagAssignStatus:
                    if t.assignedToClass != self.outerObject.__class__.__name__:
                        # Actualiza tblCaravanas con fldAssignedToClass=Nombre de clase del objeto al que se asigna t
                        if t.ID in idCol:
                            _ = setRecord(self.__tblObjectsName, fldID=t.recordID,
                                          fldAssignedToClass=self.outerObject.__class__.__name__,
                                          fldAssignedToUID=self.outerObject.ID)
                            if isinstance(_, str):
                                retValue = f'ERR_DBWrite: Cannot write table {self.__tblObjectsName}'
                                krnl_logger.error(retValue, stack_info=True)
                                continue   # Continua a procesar el siguiente tag.
                            else:
                                t.assignedToClass = self.outerObject.__class__.__name__
                                # Setea datos de tag en tbl del outerObject al que se asigna la Caravana
                                self.outerObject.setIdentifier(t.ID)
                        else:
                            retValue = f'ERR_Sys: Tag Number {t.tagNumber} / ID={t.ID} not found in DB'
                            krnl_logger.error(retValue, stack_info=True)
                            continue   # Continua a procesar el siguiente tag.

                    self.outerObject.setMyTags(t)  # registra Tag en el array de Tags del Animal
                    t.commission.set(activityName=tagActivity)  # Comisiona y Setea status de
                    # Setea valores en tabla Data Actividad Caravanas y escribe
            tagIDs = self.outerObject.myTagIDs          # convierte lista a json para escribir en DB.
            __tblData.appendRecord(fldFK_Actividad=idActividadRA, fldFK_Caravana=tagIDs, fldDate=eventDate,
                                   fldComment=f'Actividad: {activityName}. Tag ID: {tagIDs}')

            wrtTables.append(__tblData)
            if __tblData.dataLen:
                retValue = [setRecord(j.tblName, **j.unpackItem(0)) for j in wrtTables]
            else:
                retValue = f'ERR_INP_Empty tag string.'
                krnl_logger.info(retValue, stack_info=True)
                return retValue
            # Si hay error de escritura en tblLink o tblData, hace undo de tblRA y sale con error.
            if any(isinstance(j, str) for j in retValue):
                if __tblRA.getVal(0, 'fldID') is None:
                    _ = delRecord(__tblRA.tblName, idActividadRA)
                retValue = 'ERR_DB_WriteError.' + f' {moduleName()}({lineNum()})- {callerFunction()}'
                krnl_logger.error(retValue)
                print(f'Deleting record {idActividadRA} / Table: {__tblRA.tblName} / retValue: {retValue}',
                      dismiss_print=DISMISS_PRINT)
            else:
                retValue = idActividadRA
                if self.doInventory(**kwargs):
                    self.outerObject.inventory.set(*args, **kwargs)
            # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
            return retValue
        return None

    def deassignTags(self, *args: DataTable, **kwargs):         # Caravaneo - Desasignar (ID=71)
        """
        DeAssigns Tags in *args from Animal. if Status for deassigned Tags is not specified, status=Decomisionada.
        @param args: Tags array to assign to Animal
        @param kwargs: tags=[Tag]. Tag Objects to deassign (1 or more)
                       tagStatus=val -> Value to set on 'deassigned' tags. val must be in progActivitiesPermittedStatus.
                       Default: 'Baja'
        @return:
        """
        activityName = 'Caravaneo - Desasignar'
        wrtTables = []

        tags = kwargs.get('tags', [])
        if not self._isValid or not self.outerObject.validateActivity(activityName):
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotValid - {callerFunction()}()'
            print(f'{moduleName()}({lineNum()}) - {retValue}')
            # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
            return retValue

        tags = tags if hasattr(tags, '__iter__') else list(tags)
        tags = [j for j in tags if isinstance(j, Tag)]
        if tags:
            tblRA = setupArgs(self._tblRAName, *args)
            tblLink = setupArgs(self._tblLinkName, *args)
            tblData = setupArgs(self._tblDataName, *args)
            activityID = tblRA.getVal(0, 'fldFK_NombreActividad') or self._activityID
            tblRA.setVal(0, fldFK_NombreActividad=activityID)
            # reqStatus = next((j for j in kwargs if str(j).lower().__contains__('tagstatus') and
            #                  str(kwargs[j]).lower() in ('decomisionada', 'reemplazada')), 'Baja')
            retValue = None
            timeStamp = time_mt('datetime')
            eventDate = timeStamp
            userID = sessionActiveUser
            tblRA.setVal(0, fldFK_NombreActividad=activityID, fldTimeStamp=timeStamp, fldFK_UserID=userID)
            idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))
            if isinstance(idActividadRA, str):
                retValue = f'ERR_DB_WriteError - Func/Method: MyTags.assignTags()'
                krnl_logger.error(retValue)
                print(f'{moduleName()}({lineNum()})  - {retValue}', dismiss_print=DISMISS_PRINT)
                # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
                return retValue
            # Setea valores en tabla Link
            tblLink.setVal(0, fldFK_Actividad=idActividadRA, fldFK=self.outerObject.ID,
                           fldComment=f'Activity: {activityName}')
            tblLink.undoOnError = True  # Se deben deshacer escrituras previas si falla escritura
            wrtTables.append(tblLink)

            for t in tags:
                if isinstance(t, Tag):
                    self.outerObject.popMyTags(t)           # retira el Tag del Diccionario __myTags
                    self.outerObject.removeIdentifer(t.ID)    # quita de self.__identifiers y del record en Animales.
                    t.commission.unset(**kwargs)    # Decomisiona tag y Setea status
                    tblData.appendRecord(fldFK_Actividad=idActividadRA, fldDate=eventDate, fldFK_Caravana=t.ID,
                        fldComment=tblData.getVal(0, 'fldComment', '')+f'{activityName}. Tag ID:{[t.ID for t in tags]}')


            wrtTables.append(tblData)
            errorStr = None
            if tblData.dataLen:
                retValue = [j.setRecords() for j in wrtTables]    # Escribe Tablas
            else:
                errorStr = f'ERR_INP_Empty tag string. {moduleName()}({lineNum()}) - {callerFunction()}'
                krnl_logger.info(errorStr)
                print(f'{errorStr}', dismiss_print=DISMISS_PRINT)
            # Si hay error de escritura en tblLink o tblData, hace undo de tblRA y sale con error.
            if any(val in (0, None) for val in retValue) or errorStr:
                if tblRA.getVal(0, 'fldID') is None:
                    _ = delRecord(tblRA.tblName, idActividadRA)
                retValue = f'ERR_DB_WriteError - Function/Method: deassignTags()'
                krnl_logger.error(f'{moduleName()}({lineNum()}) - {retValue} - '
                                  f'Deleting record {idActividadRA} / Table: {tblRA.tblName}')
                print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
                print(f'Deleting record {idActividadRA} / Table: {tblRA.tblName}', dismiss_print=DISMISS_PRINT)
            else:
                retValue = idActividadRA
                if self.doInventory(**kwargs):
                    self.outerObject.inventory.set()
            return retValue

    @classmethod
    def isTagAssigned(cls, tag):           # cls is Bovine, Caprine, etc.
        """
        Informs whether tag is assigned to an object of type cls (by returning the object tag is tied to).
        If tag is not found in the first pass, reloads ALL tags for all objects from db to account for potential
        database updates due to replication.
        @param tag: tagNumber, Tag UID (str) or Tag object.
        @return: object is tag is assigned to an object. None if tag is not assigned or doesn't exist
        CAUTION: Object may be an object of class cls or a different class as under certain circumstances Tag objects
        are pulled from Tag __registerDict.
        """
        tagID = None
        if isinstance(tag, str):
            try:
                tagID = UUID(tag).hex
            except (SyntaxError, TypeError, AttributeError, ValueError):
                tagObj = Tag.tagFromNum(tag)
            else:
                tagObj = next((o for o in Tag.getRegisterDict().values() if o.ID == tagID), None)
        elif isinstance(tag, Tag):
            tagObj = tag
        else:
            tagObj = None

        if tagObj:
            return tagObj.isAssigned()

        # Reloads tags for all cls Objects from database.
        if not tagID:
            tagID = next((k for k in Tag.getRegisterDict() if Tag.getRegisterDict()[k].tagNumber == removeAccents(tag)),
                         None)
            if not tagID:
                return None

        obj = None
        for o in cls.getRegisterDict():  # This loop can take a while to run...
            o.initializeTags()
            if tagID in o.myTagIDs:
                obj = o
                # don't break here!. Allow for initializeTags() to run for ALL objects in registerDict, to update all.
        return obj



#  TODO(cmt): Esta clase NO DEBE SER singleton: Se crea un objeto por cada Actividad Programada definida para Animales.
#   NO tiene asociado ningun @property en krnl_abstract_class_animal.py
class ProgActivityAnimal(ProgActivity):
    __tblDataName = 'tblDataAnimalesActividadesProgramadasStatus'
    __tblRAName = 'tblAnimalesRegistroDeActividades'
    __paRegisterDict = {}  # {self: ActivityID} Register de las Actividades Programadas ACTIVAS de class AnimalActivity
    __triggerRegisterDict = {}  # {idTrigger: triggerObj}. 1 triggerRegisterDict para todas las ProgActivities.

    # List of activities with ProgActivity defined and supported. Used in recordActivity() method.
    __paDict = AnimalActivity.getSupportPADict()         # {ActivityName: __activityID, } con getSupportsPADict=True

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

    @classmethod
    def loadFromDB00(cls):
        objList = super().loadFromDB()
        if isinstance(objList, str):
            krnl_logger.error(
                f'ERR_DB_Access Error: Programmed Activities for class {cls.__name__} not loaded: {objList}')
            return f'ERR_DB_Access Error: Programmed Activities for class {cls.__name__} not loaded: {objList}'
        _ = [cls.paRegisterActivity(j) for j in objList]  # {paObj: __activityID, } Registers objects in class dict.
        return bool(_)


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
    _short_name = 'person'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    def __init__(self, *args, **kwargs):
        isValid = True
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = True
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)

        self.__tblRA = setupArgs(self._tblRAName, *args)  # Tabla Registro De Actividades
        self.__tblLink = DataTable(self._tblLinkName)  # Sin argumentos: se crean TODOS los campos de la tabla;
        self.__tblData = DataTable(self._tblDataName)  # Data Inventario, Data Localizacion, etc.

    # @Activity.dynamic_attr_wrapper
    def get(self, sDate='', eDate='', **kwargs):
        """
       Returns records in table Data Personas between sValue and eValue. If sValue = eValue = '' -> Last record
       @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                       sDate='0' or eDate='0': First Record
                       Otherwise: records between sDate and eDate
       @param eDate: See @param sDate.
       @param kwargs: mode=datatable->Returns DataTable with full record.  mode='fullRecord' -> Returns last Record,
        in full.
       @return: Object DataTable with information from [Data Animales Actividad Personas]
       """
        retValue = None
        if self._isValid and self.outerObject.validateActivity(self.__activityName):
            qryTable = self._getRecordsLinkTables(self.__tblRA, self.__tblLink, self.__tblData)
            if qryTable.dataLen:
                if qryTable.dataLen <= 1:
                    retValue = qryTable  # qryTable tiene 1 solo registro (Ultimo o Primero)
                else:
                    retTable = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
                    retValue = retTable
        # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
        return retValue


    def set(self, *args: DataTable, **kwargs):
        """
        Inserts ID_Persona obj_data for the object "obj" in the DB, along with ownership percentage over Object.
        __outerAttr: TagObject para la que se realiza la Actividad. Lo setea el metodo inventario().
        For convenience, can process multiple [Data Personas] tables with multiple records in each of the tables.
        To preserve consistency, a call to set() MUST DEFINE OWNERS FOR 100% of the object Ownership: Must set
        to Inactive all previous ownership records for the Object and leave Active the newly inserted records.
        For this, high level functions MUST DEFINE ALL Persons and Percent so that total percentage = 100% for all
        set() operations. Must pass this information in DataTable form. set() will remove all previous owner records
        to inactive (OVERWRITE->fldFlag=0) and set the new records as Active, for a total of 100% ownership.
        @param idPerson: ID_Persona, active in the system. Used when assigning only 1 person, with 100% ownership.
        @param args: list of DataTable objects, with all the tables and fields to be written to DB. Used when
                multiple persons are to be assigned to the obj Object.
                No checks performed. The function will discard and ignore all non-valid arguments
        @param kwargs: Use to pass additional obj_data for table [Data Animales Actividad Personas]
                       idPerson=(int) Assignes ID_Persona with 100% ownership (Simplified input)
        @return: ID_Actividad (Registro De Actividades, int) if success; errorCode (str) on error; None for nonValid
        """
        # Checks to be performed by high level. All idPerson valid, active, level=1; Sum of % ownership=100%
        tblData = setupArgs(self.__tblDataName, *args, **kwargs)
        idPerson = next((kwargs[j] for j in kwargs if 'person' in str(j).lower()), None)
        if self._isValid and self.outerObject.validateActivity(self._activityName):
            tblRA = setupArgs(self._tblRAName, *args)
            tblLink = setupArgs(self._tblLinkName, *args)
            idPersonDict = {}          # {ID Persona: fldPercentageOwnership, }
            personRecords = DataTable(tblData.tblName)
            activityID = self._activityID if tblRA.getVal(0, 'fldFK_NombreActividad') is None \
                else tblRA.getVal(0, 'fldFK_NombreActividad')
            tblRA.setVal(0, fldFK_NombreActividad=activityID)
            # Genera lista de owners originales del animal, existentes ANTES de asignar los owners de esta llamada.
            # SOLO filtrar por getID en temp0, NO usar ID_Actividad porque el seteo de ownership se puede
            # hacer con cualquier __activityID. Lo mismo aplica para cualquier otro filtro de este tipo.
            temp0 = getRecords(tblLink.tblName, '', '', None, 'fldFK_Actividad', 'fldFK', fldFK=self.outerObject.ID)
            objectActivitiesCol = temp0.getCol('fldFK_Actividad')
            temp = getRecords(tblData.tblName, '', '', None, 'fldID', 'fldFK_Persona', 'fldPercentageOwnership',
                              fldFlag=1, fldFK_Actividad=objectActivitiesCol)
            print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) - initialActiveRecords={temp.dataList}',
                  dismiss_print=DISMISS_PRINT)
            # percentageCol = temp.getCol('fldPercentageOwnership')
            # initialOwnershipPercent = float(sum(percentageCol))
            # print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) percentCol: {percentageCol} / '
            #       f'intialPercentage={initialOwnershipPercent}')

            if not args:
                tblData.setVal(0, fldFK_Persona=idPerson, fldPercentageOwnership=1.0, fldFlag=1)
                idPersonDict[idPerson] = 1.0     # 100% ownership
                personRecords.appendRecord(**tblRA.unpackItem(0))
            else:
                for t in args:
                    if t.tblName == tblData.tblName and t.dataLen and len(t.dataList[0]) > 0:
                        for j in range(t.dataLen):
                            idPersonDict[t.getVal(j, 'fldFK_Persona')] = float(t.getVal(j, 'fldPercentageOwnership'))
                            t.setVal(j, fldFlag=1)
                            personRecords.appendRecord(**t.unpackItem(j))  # DataTable con info de Record de personas

            print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) personRecords={personRecords.dataList}',
                  dismiss_print=DISMISS_PRINT)
            objPersonList = [Person.getRegisterDict()[i] for i in idPersonDict
                                 if i in Person.getRegisterDict() and Person.getRegisterDict()[i].level == 1]
            if objPersonList:
                ownershipTotal = 0
                for j in objPersonList:     # Chequea que % Ownership sea 100% para todos los owners validos
                    ownershipTotal += next((idPersonDict[g] for g in idPersonDict if g == j.getID), 0)
                if ownershipTotal != 1.00:
                    retValue = f'ERR_INP_InvalidArgument - {callerFunction()}: Total Percentage Ownership for ' \
                               f'Object {self.outerObject.getID} must equal 100%. Exiting...'
                    krnl_logger.info(retValue)
                    # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
                    raise RuntimeError(f'{moduleName()}({lineNum()} - {retValue})')

                print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) Person(s): {objPersonList} - '
                      f'TOTAL OWNERSHIP={ownershipTotal}', dismiss_print=DISMISS_PRINT)
                newTblData = DataTable(tblData.tblName)
                personIDCol = personRecords.getCol('fldFK_Persona')
                for obj in objPersonList:
                    for j in range(len(personIDCol)):
                        if obj.ID == personIDCol[j]:
                            newTblData.appendRecord(**personRecords.unpackItem(j))
                print(f'BBBBRRRRRRRRRR! {moduleName()}({lineNum()}) newTblData={newTblData.dataList}',
                      dismiss_print=DISMISS_PRINT)
                if newTblData.dataLen:
                    if temp.dataLen and temp.dataList[0]:
                        for j in range(temp.dataLen):       # setea fldFlag=0 para los owners previos.
                            temp.setVal(j, fldFlag=0)
                        _ = tblData.setRecords()
                    retValue = self._setPerson(tblRA, tblLink, newTblData)
                else:
                    retValue = f'ERR_INP_PersonNotValid - {callerFunction()}. NO PERSON...'
                    print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            else:
                retValue = f'ERR_INP_PersonNotValid - {callerFunction()}. Person ID:{idPerson}'
                print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
        else:
            retValue = f'ERR_Sys_ObjectNotValid or ActivityNotDefined - {callerFunction()}. Person ID:{idPerson}'
            print(f'{moduleName()}({lineNum()} - {retValue}', dismiss_print=DISMISS_PRINT)
        # self.outerObject = None  # Removes object from list. Enables recursion/reentry.
        return retValue

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

