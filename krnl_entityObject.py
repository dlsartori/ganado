import sqlite3

from krnl_custom_types import *
from krnl_config import time_mt, DAYS_MULT, USE_DAYS_MULT, TERMINAL_ID
from krnl_db_access import _create_db_trigger

# import pydantic           ### pydantic not used anymore for now (Nov-23)
# from pydantic import (BaseModel, validator, typing, ValidationError, constr, conint, conlist, PositiveInt,
#                       NegativeInt, PositiveFloat)

def moduleName():
    return str(os.path.basename(__file__))

# Initializes set used to cleanup __outerAttr dictionaries in Activity objects.
if '_classes_with_Activity' not in globals():
    globals()['_classes_with_Activity'] = set()

class EntityObject(object):
    __slots__ = ('__ID', '__isValidFlag', '__isActiveFlag', '__supportsMemoryData', '_lastInventory',
                 '_lastStatus', '_lastLocalization', '_lastCategoryID', '__daysToTimeout', '__timeoutFlag')

    tblSysParams = getRecords('tblSysDataParametrosGenerales', '', '', None, '*')
    indexDefaultTO = tblSysParams.getCol('fldName').index('Dias Para Timeout')
    __defaultDaysToTO = 0  # int(tblSysParams.getVal(indexDefaultTO, 'fldParameterValue'))
    __defaultDaysToTO = __defaultDaysToTO if __defaultDaysToTO > 0 else 366

    # Runs _creatorActivityObjects() method to create Activity Objects and the @property methods to invoke their code.
    # Populates tables_and_methods, tables_and_binding_objects to use in IntervalTimer threads.
    def __init_subclass__(cls, **kwargs):
        super.__init_subclass__()
        if hasattr(cls, '_activityObjList'):  # Si tiene definidos _activityObjList, _myActivityClass ejecuta funcion.
            cls._creatorActivityObjects()     # Crea objectos Activity y sus @property names a ser usados en el codigo.
            if cls._activityObjList:
                # _classes_with_Activity used to clean __outerAttr dictionaries: removes entries for killed threads.
                globals()['_classes_with_Activity'].add(cls)
                # print(f'&&&&&&&& Classes with Activity: {globals()["_classes_with_Activity"]} - {moduleName()}-{lineNum()}')

        # TODO: THESE ARE THE NEW TRIGGERS. Initializes all db triggers defined for class cls, if any.
        # Name mangling required here to prevent inheritance from resolving to wrong data.
        # Creates triggers defined for class.
        triggers_list = getattr(cls, '_' + cls.__name__ + '__db_triggers_list', None)
        if triggers_list:
            for j in triggers_list:
                if isinstance(j, (list, tuple)) and len(j) == 2:  # j=(trigger_name, trigger_processing_func (callable))
                    _ = DBTrigger(trig_name=j[0], process_func=j[1], calling_obj=cls)

                # Old, deprecated way of generating triggers.
                # if triggers_dict:  # Name mangling required here to prevent inheritance from resolving to wrong data.
                #     for trig in triggers_dict:
                #         if 'repl' in trig.type:
                #             classes_with_replication[trig] = cls  # {trigger_obj: calling object for method, }
                #         elif 'dupl' in trig.type:
                #             classes_with_duplication[trig] = cls
                #         else:
                #             pass

        # # TODO: THESE ARE THE NEW TRIGGERS. Initializes all db triggers defined for class cls, if any.
        # triggers_dict = getattr(cls, '_' + cls.__name__ + '__db_triggers_list', None)
        # if triggers_dict:  # Name mangling required here to prevent inheritance from resolving to wrong data.
        #     for item in triggers_dict:
        #         if callable(item):
        #             trigger_str = item(cls)
        #             item_name = item.__name__
        #         elif hasattr(item, '__func__'):
        #             trigger_str = item.__func__(cls)
        #             item_name = item.__func__.__name__
        #         elif isinstance(item, str):
        #             trigger_str = item
        #             item_name = item
        #         else:
        #             trigger_str = None
        #             item_name = ''
        #         if trigger_str:
        #             _create_db_trigger(trigger_str)  # Runs ALL triggers defined for cls.
        #             # classes_with_replication = {id(trigger_generator): [cls, processing_function(callable)], }
        #             if 'repl' in item_name.lower():
        #                 classes_with_replication[id(item)] = (cls, triggers_dict[item])
        #             elif 'dupl' in item_name.lower():
        #                 classes_with_duplication[id(item)] = (cls, triggers_dict[item])
        #         else:
        #             pass


        if hasattr(cls, 'registerKindOfAnimal'):
            if cls.getClassesEnabled()[cls.getAnimalClassID()]:
                cls.registerKindOfAnimal()

        # Initializes uid_dicts in the respective subclasses. Used to manage duplication in Animales,Caravanas,Geo, etc.
        try:
            # This way of calling is works ONLY because _init_uid_dicts is defined at the lowest level of inheritance.
            # (Bovine, Caprine, etc) and NOT in their parent classes.
            cls._init_uid_dicts()       # executes only when _init_uid_dicts is defined
            # If passes, initializes dictionary {class: Duplication_method, } for all classes that implement duplication
            # classes_with_duplication[cls] = cls._processDuplicates
        except AttributeError:
            pass                        # Otherwise, igonres.

        # TODO(cmt): IMPORTANT! this one MUST GO AFTER _init_uid_dicts(), as it uses __active_uids_dict data.
        func = getattr(cls, '_' + cls.__name__ + '__init_memory_data', None)  # func obj is return already bound to cls.
        if func:
            func()    # Dict {uid: {last_inventory: val, }, }  for now...
        return None

    # TODO(cmt): The whole point of this method is to be called with the right _activityObjList, _myActivityClass
    #   attributes based on cls, so that it is defined in one place only (here).
    @classmethod
    def _creatorActivityObjects(cls):
        """ Creates Animal Activity Objects (Instances of ActividadInventarioAnimal, ActividadLocalizacionAnimal, etc)
        by calling cls().
        Also creates class methods using ActivityFactory class and assigns them as @property objects to the classes.
        These are the properties used in the code (inventory, status, category, tags, etc).
        Called from __init_subclass__() method in EntityObject class, WHEN THE CLASS IS BEING CREATED.
        """
        if cls._activityObjList:
            return None  # this function must only run once.

        cls._creator_run = True  # Never run this code again.
        missingList = []
        # Creates InventoryAnimalActivity, StatusAnimalActivity, MoneyActivity singleton objects. Puts them in a list.
        # __class_register is a list that holds all subclasses with a valid __method_name for _myActivityClass.
        for c in cls._myActivityClass.get_class_register():
            # Separate processing for Generic Activity objects as this is class spawning multiple Activity objects.
            # Processing GenericActivityAnimal first in the 'if' enables a Generic Activity being overriden.
            # 'GenericActivityDevice', 'GenericActivityPerson' --> Future development of generic activities.
            if c.__name__ in ('GenericActivityAnimal', 'GenericActivityDevice', 'GenericActivityPerson'):
                genericActivityObjects = c._createGenericActivityObjects()
                if genericActivityObjects:
                    cls._activityObjList.extend(genericActivityObjects)
            else:
                try:
                    cls._activityObjList.append(c())  # Creates Activity object and appends to Activity object List.
                except (AttributeError, TypeError, NameError) as e:
                    krnl_logger.error(f'ERR_SYS_Error in Activity object creation {callerFunction(getCallers=True)}'
                                      f'({lineNum()}): class {c} - error: {e}')
                    missingList.append(c)

        # Loops _activityObjList to create all callable properties (inventory, status, etc.) as attributes in cls.
        for j in cls._activityObjList:  # List of Activty objects (InventoryAnimalActivity, StatusAnimalActivity, etc)
            # j are callable instances (their classes implement __call__())
            if j._decoratorName:  # se salta los None. Solo crea metodos cuando __method_name esta definido en la clase.
                try:
                    # Creates a property (inventory, status, etc., defined by _decoratorName) out of the Activity object
                    # which, by implementing __call__() allows to initialize outerObject when the property is invoked.
                    """ property(j) below IS CRITICAL in order to execute as "inventory.get()": the property calls its
                    # fget() method and this in turns executes __call__() in the object instance assigned to property.
                    # Otherwise it would have to be "inventory().get()" (__call__() being invoked by the instance name 
                    # followed by parenthesis). """
                    setattr(cls, j._decoratorName, property(j))
                    #  setattr(cls, j._decoratorName, property(ActivityMethod(j)))      # Old way of setting property.
                    # ActivityMethod class no longer used since making Activity Class callable via __call__()
                except (AttributeError, TypeError, NameError):
                    pass
        d = {j._decoratorName: j.__class__.__name__ for j in cls._activityObjList}
        print(f'Activity Objects for {cls.__name__} are: {d}', dismiss_print=DISMISS_PRINT)
        return None

        #                   ### Original code for the loop, using ActivityMethod class TODO: DEPRECATED! ###
        # Loops _activityObjList to create all callable properties (inventory, status, etc.) as attributes in cls.
        # for j in cls._activityObjList:  # List of Activty objects (InventoryAnimalActivity, StatusAnimalActivity, etc)
        #     if j._decoratorName:  # se salta los None. Solo crea metodos cuando __method_name esta definido en la clase.
        #         try:
        #             # Creates a callable ActivityMethod object out of Activity object j, converts the callable object to
        #             # property and creates a cls attribute with name (inventory, status, etc) defined by _decoratorName.
        #             setattr(cls, j._decoratorName, property(ActivityMethod(j)))
        #         except (AttributeError, TypeError, NameError):
        #             pass
        # d = {j._decoratorName: j.__class__.__name__ for j in cls._activityObjList}
        # print(f'Activity Objects for {cls.__name__} are: {d}', dismiss_print=DISMISS_PRINT)
        # return None

    @classmethod
    def _get_duplicate_uids(cls, uid: str = None, *, all_duplicates=False):
        """For a given uid, returns the Earliest Duplicate Record if only 1 record exists or a tuple of Duplicate
        records associated to the uid.
        For reference, the dictionaries are defined as follows:
                _active_uids_dict = {uid: _Duplication_Index, }
                _active_duplication_index_dict = {_Duplication_Index: [_Duplication_Index, dupl_uid1, dupl_uid2, ], }
        @param all_duplicates: True: returns all uids linked by a Duplication_Index value.
                False: returns Earliest Duplicate uid. (Default).
        @return: Earliest Duplicate uid (tuple) or list of duplicate uids (tuple). () if none found.
        """
        if uid:
            try:
                uid = UUID(uid.strip())
            except (SyntaxError, AttributeError, TypeError):
                return ()
            duplication_index = cls._active_uids_dict.get(uid)    # Pulls the right _active_uids_dict for cls.
            if duplication_index:
                if all_duplicates:
                    if duplication_index in cls._active_duplication_index_dict:
                        uid_list = cls._active_duplication_index_dict[duplication_index]
                        return tuple(uid_list)
                return tuple([duplication_index, ])
        return ()



    @staticmethod
    def ActivityCleanup(thread_id):
        """
        Traverses all Activity objects listed in _activityObjList and removes the entry for thread_id in the __outerAttr
        dictionary for all objects present.
        Called by the code that kills thread_id thread.
        This is to prevent the __outerAttr from growing out of control as threads are started and killed.
        @param thread_id:
        @return: None
        """
        if "_classes_with_Activity" in globals():
            for cls in globals()["_classes_with_Activity"]:
                if hasattr(cls, '_activityObjList') and cls._activityObjList:
                    for o in cls._activityObjList:
                        if o._pop_outerAttr_key(thread_id):
                            print(f'...Removing thread_id {thread_id} for {cls.__name__}.{o._decoratorName}.'
                                  f' - {moduleName()}-{lineNum()}.', dismiss_print=DISMISS_PRINT)
        return None


    def __init__(self, ID_Obj, isValid, isActive, *args, **kwargs):
        if isinstance(ID_Obj, (int, str)):
            self.__ID = ID_Obj          # recordID es ID_Animal, ID_Caravana, ID_Persona, ID_Dispositivo, ID_Item
            self.__isValidFlag = isValid or True          # Activo / No Activo de tabla Data Status
            # !=None para Animales, None para el resto de los objetos.
            self.__isActiveFlag = isValid * isActive  # Active es True/False SOLO si __isValidFlag = True
            self.__supportsMemoryData = next((kwargs[j] for j in kwargs if j.lower().startswith('memdata')), None) or False
            self._lastInventory = next((kwargs[j] for j in kwargs if j.lower().startswith('lastinvent')), None) # LISTA!!
            self._lastStatus = next((kwargs[j] for j in kwargs if j.lower().startswith('laststatus')), None)  # LISTA!!
            self._lastLocalization = next((kwargs[j] for j in kwargs if j.lower().startswith('lastlocaliz')), None) # LISTA!!
            self._lastCategoryID = next((kwargs[j] for j in kwargs if j.lower().startswith('lastcategory')), None) # LISTA!!
            self.__daysToTimeout = next((kwargs[j] for j in kwargs if j.lower().startswith('daystotimeout')),
                                        EntityObject.__defaultDaysToTO)  # Periodo en dias para Timeout
            self.__timeoutFlag = False      # True cuando (now()-self__lastInventory).days > obj.__daysToTimeout
        else:
            # Objeto isvalid = False si no se pasan los recordID necesarios.
            self.__isValidFlag = False
            self.__isActiveFlag = False
        super().__init__()

    @property
    def isValid(self):    # Retorna estado valido/no valido de un objeto. Objeto NO valido cuando falla escritura en DB.
        return self.__isValidFlag

    @isValid.setter
    def isValid(self, state):
        """
        Sets __isValidFlag for an object
        @param state: True or False. state MUST BE a valid val, otherwise __isValidFlag remains unchanged.
        @return: NADA, as per Python requirements.
        """
        self.__isValidFlag = bool(state)
        self.__isActiveFlag = self.__isValidFlag * self.__isActiveFlag  # Si isValid es 0, isActive debe hacerse 0.

    @property
    def isActive(self):
        return self.__isActiveFlag

    @isActive.setter
    def isActive(self, state):
        """
        Sets __isActiveFlag for an object. if __isValidFlag == 0, __isActiveFlag will be set to 0
        @param state: True or False. state MUST BE a valid val, otherwise __isActiveFlag remains unchanged.
        @return: NADA, as per Python requirements.
        """
        self.__isActiveFlag = 0 if not state else 1 * self.__isValidFlag

    @property
    def getID(self):            # Se mantiene por compatibilidad. Usar @property ID.
        return self.__ID if self.__isValidFlag is not False else None

    @property
    def ID(self):
        return self.__ID if self.__isValidFlag is not False else None


    def setID(self, val):
        """
        Sets the UUID field for object. Used to update UUID value when resolving repeat records/objects.
        @return: False if val is not a valid UUID.
        """
        try:
            _ = UUID(val)
        except(SyntaxError, TypeError, ValueError):
            return False
        else:
            self.__ID = str(val.hex)
            return True

    def check_timeout(self, val):       # TODO: THIS FUNCTION DOES NOT GO HERE. CHANGE TO USING _memory_data
        """ Checks for timeout and sets the timeout flag is more than __defaultDAystoTO have elapsed.
        """
        if val and isinstance(val, datetime):
            if USE_DAYS_MULT:
                self.__timeoutFlag = (time_mt()-val.timestamp()) * DAYS_MULT >= self.__defaultDaysToTO
            else:
                self.__timeoutFlag = (time_mt('datetime')-val).days >= self.__defaultDaysToTO
            return True
        return False

    @classmethod
    def loadActiveRecords(cls, *, added_conditions=''):
        """Loads all Active records from DB for cls.
        @param added_conditions: string starting with extra conditions to refine active records query.
               Should start with "AND", "OR" or other valid logic operators.
        @return: DataTable with all active records. Meant to save memory by not creating __registerDict dictionaries.
        error string (str) if fails.
        """
        try:
            tbl = cls.tblObjName()       # Accesses cls.__tblObjName if exists, otherwise throws AttributeError except.
        except AttributeError:
            return f"ERR_DBAccess: Cannot access {cls} Objects table. Invalid name or Objects table does not exist."
        else:
            sql = f'SELECT * FROM {getTblName(tbl)} WHERE "Salida YN"=0 OR "Salida YN" IS NULL ' + added_conditions+';'
            temp = dbRead(tbl, sql)  # temp is a DataTable object.

            if not isinstance(temp, DataTable):
                return f"ERR_DBAccess: {temp}."

            # if hasattr(cls, '_fldID_list'):         # TODO: this may be deprecated as of 4.1.9. See to remove.
            #     cls._fldID_list = temp.getCol('fldID')  # Initializes fldID_list for _processDuplicates() to work..
            return temp

    @property
    def supportsMemData(self):
        return self.__supportsMemoryData

    # @property
    # def lastInventory(self):
    #     if self.__supportsMemoryData and self._lastInventory:         # Retorna Fecha del Ultimo inventario
    #         return self._lastInventory[0]
    #     return None
    #
    # @lastInventory.setter
    # def lastInventory(self, val):
    #     if self.__supportsMemoryData:
    #         self._lastInventory = val            # TODO(cmt): val = [varValue(datetime), eventTime(datetime)]
    #     if val:
    #         if USE_DAYS_MULT:
    #             self.__timeoutFlag = (time_mt()-val[0].timestamp()) * DAYS_MULT >= self.__defaultDaysToTO
    #         else:
    #             self.__timeoutFlag = (time_mt('datetime')-val[0]).days >= self.__defaultDaysToTO


    @property
    def lastStatus(self):
        if self.__supportsMemoryData and self._lastStatus:
            return self._lastStatus[0]     # Retorna Status actual del Objeto
        return None

    @lastStatus.setter
    def lastStatus(self, val):
        if self.__supportsMemoryData:
            self._lastStatus = val          # val = [varValue(status), eventTime(datetime)]

    @property
    def lastLocalization(self):
        if self.__supportsMemoryData and self._lastLocalization:
            return self._lastLocalization[0]
        return None

    @lastLocalization.setter
    def lastLocalization(self, val):
        if self.__supportsMemoryData:
            self._lastLocalization = val         # val = [varValue(Geo Object), eventTime(datetime)]

    @property
    def lastCategoryID(self):
        if self.__supportsMemoryData and self._lastCategoryID:
            return self._lastCategoryID[0]       # val = [varValue(categoryID), eventTime(datetime)]
        return None

    @lastCategoryID.setter
    def lastCategoryID(self, val):
        if self.__supportsMemoryData:
            self._lastCategoryID = val           # val = [varValue(categoryID), eventTime(datetime)]


    @property
    def timeout(self):
        return self.__timeoutFlag

    @timeout.setter
    def timeout(self, val):
        self.__timeoutFlag = bool(val)

    @property
    def daysToTimeout(self):
        return self.__daysToTimeout

    @daysToTimeout.setter
    def daysToTimeout(self, val):
        try:
            self.__daysToTimeout = val if val > 0 else 365
        except TypeError:
            self.__daysToTimeout = 366


    @classmethod
    def getObject(cls, obj_id):             # TODO: Override this function in specific classes.
        """
               Loads 1 Object of class cls from DB. Animal IDs provided in args. If no args, loads all animals.
               In all cases pulls the record for obj_uid with MIN("FechaHora Registro"). This is to implement a consistent
               logic to handle duplicate records.
               Function is duplicates-aware for all classes.
               @param obj_id: UID for object to load. If none passed, searches tags (if tags are defined.
               @param cls: Bovine, Caprine, Person, Tag, etc.   -> OJO!: Class Object, NOT str.
               @param kwargs:
               @return: object of class cls associated with obj_uid, or errorCode (str) if error.
                   IMPORTANT: returned object may be active or inactive.
               """
        try:
            if cls.__name__ in ('Animal', 'AssetItem', 'Asset', 'EntityObject'):
                return f'INFO_Inp_InvalidArgument {cls}. Class is not instantiable.'
        except (TypeError, AttributeError, NameError):
            return f'INFO_Inp_InvalidArgument {cls}. Class is not instantiable.'

        # if obj_id:
        #     try:
        #         obj_uid = UUID(obj_id)
        #     except(SyntaxError, TypeError, ValueError):
        #         return f'ERR_INP_Invalid or malformed UID for object of class {cls.__name__}. Nothing loaded.'
        return




    @classmethod
    def loadObjectByUID(cls, *, obj_uid=None, **kwargs):
        """
        Loads 1 Animal of class cls from DB. Animal IDs provided in args. If no args, loads all animals.
        In all cases pulls the record for obj_uid with MIN("FechaHora Registro"). This is to implement a consistent
        logic to handle duplicate records.
        @param obj_uid: UID for object to load. If none passed, searches tags (if tags are defined.
        @param cls: Bovine, Caprine, Person, Tag, etc.   -> OJO!: Class Object, NOT str.
        @param kwargs:
        @return: object of class cls associated with obj_uid, or errorCode (str) if error.
            IMPORTANT: returned object may be active or inactive.
        """
        try:
            if cls.__name__ in ('Animal', 'AssetItem', 'Asset', 'EntityObject'):
                return f'ERR_INP_InvalidArgument {cls}. Class is not instantiable.'
        except (TypeError, AttributeError, NameError):
            return f'ERR_INP_InvalidArgument {cls}. Class is not instantiable.'

        if obj_uid:
            try:
                obj_uid = UUID(obj_uid)
            except(SyntaxError, TypeError, ValueError):
                return f'ERR_INP_Invalid or malformed UID for object of class {cls.__name__}. Nothing loaded.'

        try:
            tbl = cls.tblObjName()       # Accesses cls.__tblObjName if exists, otherwise throws AttributeError except.
        except AttributeError:
            return f"ERR_DBAccess: Cannot access {cls} Objects table. Invalid name or Objects table does not exist."
        else:
            conditions_str = f' AND UID_Objeto={obj_uid}' + \
                             (f' AND "ID_Clase De Animal"={cls.classID()}' if hasattr(cls, 'classID') else '')
            sql = f'SELECT MIN("FechaHora Registro") FROM {getTblName(tbl)} WHERE "Salida YN"=0 OR "Salida YN" IS NULL '\
                  + conditions_str + ';'
            itemsTable = dbRead(tbl, sql)  # temp is a DataTable object.

            if not isinstance(itemsTable, DataTable):
                return f"ERR_DBAccess: {itemsTable}."

        # if hasattr(cls, 'classID'):
        #     itemsTable = getRecords(cls.tblObjName(), '', '', None, '*',       # cls pulls the right tblObName
        #                             fldFK_ClaseDeAnimal=cls.classID(), fldDateExit=0, fldObjectUID=obj_uid)  #Animales
        # else:
        #     itemsTable = getRecords(cls.tblObjName(), '', '', None, '*', fldObjectUID=obj_uid)  # Todos los demas

        if isinstance(itemsTable, str):
            return itemsTable               # Error reading table. Exits.

        if not itemsTable.dataList:
            return f'UID {obj_uid} not found in table {itemsTable.tblName}. Nothing loaded.'

        # Pulls ProgActivities for object
        try:
            paClass = cls.getPAClass()
        except (AttributeError, NameError):
            paClass = None  # paClass no definida para cls.
        RAP_col = []
        if paClass:
            # Pulls RAP records ONLY belonging to this node (fldTerminal_ID=TERMINAL_ID). Skips all replicated records.
            RAPRecords = getRecords(cls.tblRAPName(), '', '', None, 'fldID', fldTerminal_ID=TERMINAL_ID)
            if isinstance(RAPRecords, DataTable) and RAPRecords.dataLen:
                RAP_col = tuple(RAPRecords.getCol('fldID'))

        itemObj = cls(**itemsTable.unpackItem(0))  # LLAMA a __init__(): Crea Obj. Inicializa lastInv,lastStatus,etc
        if itemObj.isValid and not itemObj.exitYN:
            # itemObj.register()  # Registra objecto en __registerDict. NO LONGER NEEDED (v 4.1.9)
            if paClass:  # Registers progActivity in myProgActivities dict if ProgActivities are defined for object.
                # Populates myProgActivities dict ONLY with ProgActivities generated by the node (DB_ID = TERMINAL_ID).
                if RAP_col:  # Solo ProgActivities abiertas (sin cierre).
                    linkPARecords = getRecords(cls.tblLinkPAName(), '', '', None, '*', fldFK=itemObj.ID,
                                               fldFK_ActividadDeCierre=None, fldFK_Actividad=RAP_col)
                else:
                    linkPARecords = getRecords(cls.tblLinkPAName(), '', '', None, '*', fldFK=itemObj.ID,
                                               fldFK_ActividadDeCierre=None)  # Solo ProgActivities sin cierre.
                if isinstance(linkPARecords, DataTable):
                    fldIDCol = linkPARecords.getCol('fldID')
                    # Truco p/ incluir fldFK_ActividadDeCierre=0 porque parsing de getRecords() no implementa por ahora
                    temp = getRecords(cls.tblLinkPAName(), '', '', None, '*', fldFK=itemObj.ID,
                                      fldFK_ActividadDeCierre=0)
                    if isinstance(temp, DataTable) and temp.dataLen:
                        for i in range(temp.dataLen):
                            if temp.getVal(i, 'fldID') not in fldIDCol and temp.getVal(i, 'fldFK_Actividad') in RAP_col:
                                linkPARecords.appendRecord(**temp.unpackItem(i))
                    if linkPARecords.dataLen:
                        paObjList = [o for o in paClass.getPARegisterDict() if o.ID in
                                     linkPARecords.getCol('fldFK_Actividad')]
                        if paObjList:
                            for o in paObjList:
                                itemObj.registerProgActivity(o)  # myProgActivities: {paObj: __activityID}
                elif isinstance(linkPARecords, str):
                    krnl_logger.warning(
                        f'ERR_DBReadError: cannot read from table {cls.tblLinkPAName()}. Error: {linkPARecords}')

            if hasattr(itemObj, 'identifiers'):
                pass        # Process any updates of tags here.


            # if init_tags and itemObj.__class__ in Animal.getAnimalClasses():
            #     itemObj.tags.initializeTags(tblRA_Desasignados)
            #     print(f'@@@@@@ {moduleName()} - bovine[{itemObj}] Category read from DB: {itemObj.category.get()}',
            #           end=' -- ', dismiss_print=DISMISS_PRINT)


                itemObj.category.compute()
                print(f'After Category Update: {itemObj.category.get()}', dismiss_print=DISMISS_PRINT)
            # itemObj.inventory.get(mode='value')
            # itemObj.status.get(mode='value')

        # if itemObj.__class__.__name__ == 'Animal':
        #     itemObj.updateTimeout_bkgd()  # NO hace falta esto. Solo para testear.


    def updateTimeout(self, **kwargs):
            """
            Timeout arguments may vary as they are assigned INDIVIDUALLY - they are an Instance Property.
            Function called by default daily.
            @return: idObject if in Timeout or False if timeout not reached. None: in Timeout, but self already passed up.
            """
            # if self.lastInventory:

            lastInventory = self.get_memory_data().get('last_inventory', None)
            if lastInventory:
                if USE_DAYS_MULT:    # TODO(cmt): if para utilizar formula correcta cuando se usa mutiplicador de dias
                    daysSinceLastInv = int((time_mt()-lastInventory.timestamp()) * DAYS_MULT)
                    # print(f'HOLA------ ESTAMOS en updateTimeout()!!')
                else:
                    daysSinceLastInv = int((time_mt('datetime') - lastInventory).days)  # Esta es la correcta

                krnl_logger.debug(f'=====> Animal: {self.ID}, lastInv: {lastInventory} / daysSinceLastInv:{daysSinceLastInv}')

                if daysSinceLastInv > self.__daysToTimeout:
                    self.__timeoutFlag = True
                    return [self, lastInventory, daysSinceLastInv]   # TODO: ARREGLAR al terminar el debugging (retornar True/False)
                    # return True
                else:
                    self.__timeoutFlag = False
                    krnl_logger.debug(f'=====> NO HAY TIMEOUT!! Animal: {self.ID}, daystoTimeOut: {self.__daysToTimeout} ')
                    # return [self, (self.lastInventory, int(daysSinceLastInv))]
            # else:
            #     krnl_logger.debug(f'=====> NO HAY lastInventory!! Animal: {self.ID}, lastInv: {self.lastInventory} ')
            return None
