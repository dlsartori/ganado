from __future__ import annotations
# import inspect
from krnl_custom_types import *
from krnl_config import time_mt, DAYS_MULT, USE_DAYS_MULT, TERMINAL_ID
from krnl_abstract_class_activity import Activity

def moduleName():
    return str(os.path.basename(__file__))

# Initializes set used to cleanup __outerAttr dictionaries in Activity objects.
if '_classes_with_Activity' not in globals():
    globals()['_classes_with_Activity'] = set()



class EntityObject(object):
    __slots__ = ('__ID', '_isValidFlag', '_isActiveFlag', '__supportsMemoryData', '_lastInventory',
                 '_lastStatus', '_lastLocalization', '_lastCategoryID', '_daysToTimeout', '__timeoutFlag')

    tblSysParams = getRecords('tblSysDataParametrosGenerales', '', '', None, '*')
    indexDefaultTO = tblSysParams.getCol('fldName').index('Dias Para Timeout')
    __defaultDaysToTO = 0  # int(tblSysParams.getVal(indexDefaultTO, 'fldParameterValue'))
    __defaultDaysToTO = __defaultDaysToTO if __defaultDaysToTO > 0 else 366

    # Runs _creatorActivityObjects() method to create Activity Objects and the @property methods to invoke their code.
    # Populates tables_and_methods, tables_and_binding_objects to use in IntervalTimer threads.
    def __init_subclass__(cls, **kwargs):
        super.__init_subclass__()
        if hasattr(cls, '_activityObjList'):  # Si tiene definidos _activityObjList, _myActivityClass ejecuta codigo.
            cls._creatorActivityObjects()     # Crea objectos Activity y sus @property names a ser usados en el codigo.
            if cls._activityObjList:
                # _classes_with_Activity is used to clean __outerAttr dictionaries: removes entries for killed threads.
                globals()['_classes_with_Activity'].add(cls)
                # print(f'&&&& Classes with Activity: {globals()["_classes_with_Activity"]}-{moduleName()}-{lineNum()}')

        # TODO(cmt): THESE ARE THE NEW TRIGGERS. Initializes all db triggers defined for class cls, if any.
        # Name mangling required here to prevent inheritance from resolving to wrong data.
        # Creates triggers defined for class.
        triggers_list = getattr(cls, '_' + cls.__name__ + '__db_triggers_list', None)
        if triggers_list:
            for j in triggers_list:
                if isinstance(j, (list, tuple)) and len(j) == 2:  # j=(trigger_name, trigger_processing_func (callable))
                    _ = DBTrigger(trig_name=j[0], process_func=j[1], calling_obj=cls)
            print(f'CREATED Triggers for {cls}: {str(tuple([tr.name for tr in DBTrigger.get_trigger_register()]))}. ')
        if hasattr(cls, 'registerKindOfAnimal'):
            if cls.getClassesEnabled()[cls.getAnimalClassID()]:  # cls is Bovine, Caprine, etc. Enabled Animal classes.
                cls.registerKindOfAnimal()          # Sets Animal.getanimalclasses()[cls] = _kindOfAnimalID
        if getattr(cls, '_' + cls.__name__ + '__uses_tags', None):
            # Registers Tag class for cls Animal class that __uses_tags (TagBovine for Bovine, etc.)
            cls.createTagSubClass()
            # cls.registerTagSubClass()

        # Initializes _active_uid_dict in the respective subclasses. Used to manage duplication in Animales,Caravanas,
        # Geo, Device, etc. Name mangling needed to access ONLY classes that define _active_uids_df dictionaries.
        if getattr(cls, '_active_uids_df', None) == {}:
            # Calls ONLY for classes with _active_uids_df defined (Bovine, TagBovine, etc) and NOT for parent classes.
            # Also initializes dictionary {class: Duplication_index, } for all classes that implement duplication
            # classes_with_duplication[cls] = cls._processDuplicates
            # try:
            cls._init_uid_dicts()       # executes only for classes that define _init_uid_dicts.

            # MemoryData dicts initialization below. Create MemoryData objects and load data from database.
            # TODO(cmt): IMPORTANT! this one MUST GO AFTER _init_uid_dicts(), as it uses _active_uids_df data.
            # Updates local_active_uids dictionaries in Activity classes that __supports_mem_data.
            # Does ALL memory_data db reads and init here for those Activities.
            # classes that define __local_uids_dict.
            mem_data_classes = cls._myActivityClass._memory_data_classes if hasattr(cls, '_myActivityClass') else None

            # TODO(cmt): Now I see the light: There's no need to wait on dictionary initialization for mem_data
            #  Activities!!. This is because the logic that handles the mem_data dictionaries already covers dicts
            #  changing dynamically. So, if a certain uid is not found in the local dict by a _get_mem_data() or
            #  set_mem_data() call, the function will pull the data from db and will update the corresponding dict.
            #  With this, Activity._futures dict will still be populated during the intialization of EntityObjects,
            #  but is NOT used at the moment. Because of all this, the code commented below is no longer needed.
            #  THERE IS HOWEVER a race-condition scenario: set_mem_data() can add data to a dict BEFORE
            #  initialization completes. For these cases, uses wait_till_ready() to address concurrent access on a
            #  per-dictionary basis. The locking condition is checked both in set_mem_data() and in
            #  _memory_data_init_last_rec().

            if mem_data_classes:
                # New way of initializing: fires all _memory_data_init_last_rec() functions in separate threads.
                with ThreadPoolExecutor(max_workers=len(mem_data_classes) + 1) as executor:
                    for c in mem_data_classes:
                        # Initializes local uids dicts for c and parent classes.
                        # Activities DON'T need to wait for these threads to complete. They operate normallly.
                        active_uids = []
                        for df in cls.get_active_uids_iter():
                            active_uids.extend(df['fldObjectUID'].tolist())  # Get ALL uids in object dataframe.
                        Activity._futures[c].append(executor.submit(c._memory_data_init_last_rec, cls, active_uids))
                        # c._memory_data_init_last_rec(cls, active_uids)    # Non-threaded call.
                        # c.wait_till_ready()
                    f = [c for c in mem_data_classes if not c.is_ready()]
                    if f:  # Warns if any mem_data initializers are not yet finished.
                        krnl_logger.info(f'Activity {tuple(f)}  NOT READY while running thread launcher loop.')
                    print(f'\n============= Futures dict ({cls.__name__}): {Activity._futures}.')
                # print(f"MEMORY DATA UID DICTS: {getattr(c, '_' + c.__name__ + '__local_active_uids_dict', None)}")


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
                else:
                    cls._myActivityClass.get_class_register()[c] = None  # cls # Sets up {ActivityClass: Object class, }

        # print(f'^^^^^^^^^^ Activity Class Register is : {cls._myActivityClass.get_class_register()}', dismiss_print=DISMISS_PRINT)
        # Loops _activityObjList to create all callable properties (inventory, status, etc.) as attributes in cls.
        for j in cls._activityObjList:  # List of Activty objects (InventoryAnimalActivity, StatusAnimalActivity, etc)
            # j is a callable instance of an Activity (their classes implement __call__())
            # se salta los None. Solo crea metodos cuando __method_name esta definido en la clase.
            # TODO Important: for Activity objects with same __method_name, the LAST object to be processed in this for
            #  loop is the overriding object for that method name.
            if j._decoratorName:
                try:
                    # Creates a property (inventory, status, etc., defined by _decoratorName) out of the Activity object
                    # which, by implementing __call__(), allows to initialize outerObject when the property is invoked.
                    # See https://stackoverflow.com/questions/32855927/how-does-call-actually-work.
                    # https://eli.thegreenplace.net/2012/03/23/python-internals-how-callables-work/
                    """ property(j) below IS CRITICAL in order to execute as "inventory.get()": the property calls its
                    # fget() method and this in turns executes __call__() in the Activity object attached to property.
                    # Otherwise it would have to be "inventory().get()" (__call__() being invoked by the instance name 
                    # followed by parenthesis). """
                    if not hasattr(j, '_' + j.__class__.__name__ + '__classmethod'):
                        # Crea @property solo cuando cls NO es clase exclusiva classmethod (Alta, Baja, por ahora).
                        setattr(cls, j._decoratorName, property(j))         # @property for EO instances

                    """ 25-May-24: added _decoratorName_classmethod -> attribute that accesses Activity Object via an
                    Activity class (instead of an instance). This is necessary to use Bovine, Caprine, etc, to call
                    InventoryActivity, StatusActivity, CategoryActivity, etc. (using uids) and avoid getObject() calls.
                    Made possible because the Activity classes implement the __call__() method. """
                    setattr(cls, j._decoratorName + '_classmethod', classmethod(j))  # Make a classmethod out of Activity Object
                except (AttributeError, TypeError, NameError):
                    pass
        d = {j._decoratorName: j.__class__.__name__ for j in cls._activityObjList}
        print(f'Activity Objects for {cls.__name__} are: {d}', dismiss_print=DISMISS_PRINT)
        return None


    @classmethod
    def _get_duplicate_uids(cls, uid: UUID | str = None, *, all_duplicates=False) -> UUID | tuple | None:
        """For a given uid, returns the Earliest Duplicate Record (the Original) if only 1 record exists or a tuple of
        duplicate records associated to the uid.
        For reference, the dictionaries are defined as follows:
                _active_uids_df = {uid: _Duplication_Index, }   --> _Duplication_Index IS a uid.
                _active_duplication_index_dict = {_Duplication_Index: [fldObjectUID, dupl_uid1, dupl_uid2, ], }
        @param all_duplicates: True: returns all uids linked by a Duplication_Index value.
                False: returns Earliest Duplicate uid. (Default).
        @return: Original uid (UUID) or list of duplicate uids (tuple). None no duplicates exist for uid.

         _Duplication_Index is set by SQLite. Flags db records created by different nodes that refer
         to the same physical object. Duplicates are resolved between SQLite (triggers) and this function, by picking
         min(fldTimeStamp) (record 1st created) in all cases.
        """
        if uid:
            try:
                uid = uid.hex if isinstance(uid, UUID) else UUID(uid.strip()).hex    # converts to str.
            except (SyntaxError, AttributeError, TypeError, ValueError):
                return None
            duplication_index = None
            for df in cls.obj_dataframe():  # Careful, this is an iterator! Exhausted after 1st use.
                aux_series = df.loc[df['fldObjectUID'] == uid, 'fld_Duplication_Index']
                if len(aux_series.index) > 0:
                    duplication_index = aux_series.iloc[0]
                    break

            if pd.notnull(duplication_index):
                if all_duplicates:
                    return tuple(cls.obj_dupl_series()[duplication_index])
                return duplication_index        # Returns single value (a UUID or None)
        return None


    @classmethod
    def get_active_uids_iter(cls):
        """ OJO!: Returns iterator. All code accessing this function must iterate through the iterator. """
        if getattr(cls, 'obj_dataframe', None):
            # return cls.obj_dataframe()['fldOjbectUID'].tolist()
            return cls.obj_dataframe()              # OJO: obj_dataframe returns an iterator.
        return getattr(cls, '_active_uids_df', {})   # {animal_uid: duplication_index }


    @staticmethod
    def ActivityCleanup(thread_id):
        """
        Traverses all Activity objects listed in _activityObjList and removes the entry for thread_id in the __outerAttr
        dictionary for all objects present.
        Called by the code that kills the thread identified with thread_id.
        This is to prevent the __outerAttr dictionary from growing out of control as threads are started and killed.
        @param thread_id:
        @return: None
        """
        # _classes_with_Activity key keeps a list of all classes with Activities defined, in globals dict.
        if "_classes_with_Activity" in globals():
            for cls in globals()["_classes_with_Activity"]:
                if hasattr(cls, '_activityObjList') and cls._activityObjList:
                    for o in cls._activityObjList:
                        if o._pop_outerAttr_key(thread_id, None):
                            krnl_logger.info(f'--Removing thread_id {thread_id} for {cls.__name__}.{o._decoratorName}.')
        return None


    def __init__(self, ID_Obj, isValid, isActive, *args, **kwargs):
        if isinstance(ID_Obj, str):     # TODO(cmt): uids are str for all purposes. Converted to int only when needed.
            self._isValidFlag = isValid or True  # Activo / No Activo de tabla Data Status
            self.__ID = ID_Obj          # recordID es ID_Animal, ID_Caravana, ID_Persona, ID_Dispositivo, ID_Item
            # !=None para Animales, None para el resto de los objetos.
            self._isActiveFlag = isValid * isActive  # Active es True/False SOLO si _isValidFlag = True
            self._daysToTimeout = next((kwargs[j] for j in kwargs if 'timeout' in j.lower()),
                                       getattr(self, '_defaultDaysToTimeout', None)) or EntityObject.__defaultDaysToTO
        else:
            # Objeto isvalid = False si no se pasan los recordID necesarios.
            self._isValidFlag = False
            self._isActiveFlag = False
        super().__init__()

    @property
    def isValid(self):    # Retorna estado valido/no valido de un objeto. Objeto NO valido cuando falla escritura en DB.
        return self._isValidFlag

    @isValid.setter
    def isValid(self, state):
        """
        Sets _isValidFlag for an object
        @param state: True or False. state MUST BE a valid val, otherwise _isValidFlag remains unchanged.
        @return: NADA, as per Python requirements.
        """
        self._isValidFlag = bool(state)
        self._isActiveFlag = self._isValidFlag * self._isActiveFlag  # Si isValid es 0, isActive debe hacerse 0.

    @property
    def isActive(self):
        return self._isActiveFlag

    @isActive.setter
    def isActive(self, state):
        """
        Sets _isActiveFlag for an object. if _isValidFlag == 0, _isActiveFlag will be set to 0
        @param state: True or False. state MUST BE a valid val, otherwise _isActiveFlag remains unchanged.
        @return: NADA, as per Python requirements.
        """
        self._isActiveFlag = 0 if not state else 1 * self._isValidFlag

    @property
    def getID(self):            # Se mantiene por compatibilidad. Usar @property ID.
        return self.__ID if self._isValidFlag else None

    @property
    def ID(self):
        return self.__ID if self._isValidFlag else None

    @ID.setter
    def ID(self, val):
        """
        Sets the UUID field for object. Used to instantiate or to update UUID value when resolving repeat records.
        ******* fldObjectUID is treated as str for all purposes.Converts to UUID only when required.  *********
        """
        if isinstance(val, UUID):
            self.__ID = str(val)  # fldObjectUID is treated as str for all purposes.Converts to UUID only when required.
        else:
            try:
                val = UUID(val)
            except(TypeError, SyntaxError, ValueError):
                pass
            else:
                self.__ID = str(val)


    def setID(self, val):
        """               *********** Deprecated. Left here for compatibility ******************
        Sets the UUID field for object. Used to update UUID value when resolving repeat records/objects.
        ******* fldObjectUID is treated as str for all purposes.Converts to UUID only when required.  *********
        @return: False if val is not a valid UUID.
        """
        try:
            _ = UUID(val)
        except(SyntaxError, TypeError, ValueError):
            return False
        else:
            self.__ID = str(val.hex)
            return True

    # @classmethod
    # def get_identifiers_dict(cls):
    #     """ Returns the identifers dict for object subclasses that implement Identifiers. """
    #     return getattr(cls, '_identifiers_dict')  # {identifier: fldObjectUID  }


    # def update_identfiers_dict(self, identifs=None):
    #     if identifs is not None:
    #         try:                        # update() should be atomic
    #             self.get_identifiers_dict()[self.ID] = identifs  # {fldObjectUID: [__identifiers, ], }
    #         except AttributeError:
    #             pass

    # def pop_identifier(self, identif):
    #     """ Updates _identifiers_dict with new iterable after removing one item from __identifiers.
    #     For "immutable" identifiers (Tag, Geo) -> Do nothing.
    #     MEANT TO BE USED BY deassign() function.
    #     """
    #     identifiers = getattr(self, '_' + self.__class__.__name__ + '__identifiers', None)
    #     if identifiers and isinstance(identifiers, set):
    #         identifiers.discard(identif)
    #     elif isinstance(identifiers, list) and identif in identifiers:
    #         identifiers.remove(identif)
    #     else:
    #         return
    #     try:
    #         self.get_identifiers_dict()[self.ID] = identifiers   # New identfiers set. assignment should be atomic
    #     except AttributeError:
    #         pass
    #

    # def pop_identifier00(self, identif):
    #     """ Updates _identifiers_dict with new iterable after removing one item from __identifiers.
    #     For "immutable" identifiers (Tag, Geo) -> Do nothing.
    #     MEANT TO BE USED BY deassign() function.
    #     """
    #     identifiers = getattr(self, '_' + self.__class__.__name__ + '__identifiers', None)
    #     if identifiers and isinstance(identifiers, set):
    #         identifiers.discard(identif)
    #     elif isinstance(identifiers, list) and identif in identifiers:
    #         identifiers.remove(identif)
    #     else:
    #         return
    #     try:
    #         self.get_identifiers_dict()[self.ID] = identifiers  # New identfiers set. assignment should be atomic
    #     except AttributeError:
    #         pass
    #

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
            return temp


    @property
    def daysToTimeout(self):
        return self._daysToTimeout

    @daysToTimeout.setter
    def daysToTimeout(self, val):
        try:
            self._daysToTimeout = val if (isinstance(val, (int, float)) and val > 0) else 365
        except TypeError:
            self._daysToTimeout = 366
        finally:
            # timeout is now a database value and a memory_data value.
            self.inventory.set_and_rec_mem_data(self.daysToTimeout, tbl_name=self.tblObjName(), fld_name='fldDaysToTimeout')


    @classmethod
    def getObject(cls, obj_id):             # TODO: Override this function in specific classes.
        """
               Loads 1 Object of class cls from DB. Animal IDs provided in args. If no args, loads all animals.
               In all cases pulls the record for obj_uid with MIN("FechaHora Registro"). This is to implement a consistent
               logic to handle duplicate records.
               Function is duplicates-aware for all classes.
               @param obj_id: UID for object to load. If none passed, searches tags (if tags are defined.
               @param cls: Bovine, Caprine, Person, Tag, etc.   -> OJO!: Class Object, NOT str.
               @return: object of class cls associated with obj_uid, or errorCode (str) if error.
                   IMPORTANT: returned object may be active or inactive.
               """
        try:
            if cls.__name__ in ('Animal', 'AssetItem', 'Asset', 'EntityObject', 'Bird', 'Mammal'):
                return f'INFO_Inp_InvalidArgument {cls}. Class is not instantiable.'
        except (TypeError, AttributeError, NameError):
            return f'INFO_Inp_InvalidArgument {cls}. Class is not instantiable.'


    # @classmethod
    # def loadObjectByUID(cls, *, obj_uid=None, **kwargs):        # TODO: DEPRECATE. DO NOT USE.
    #     """
    #     Loads 1 Animal of class cls from DB. Animal IDs provided in args. If no args, loads all animals.
    #     In all cases pulls the record for obj_uid with MIN("FechaHora Registro"). This is to implement a consistent
    #     logic to handle duplicate records.
    #     @param obj_uid: UID for object to load. If none passed, searches tags (if tags are defined.
    #     @param cls: Bovine, Caprine, Person, Tag, etc.   -> OJO!: Class Object, NOT str.
    #     @param kwargs:
    #     @return: object of class cls associated with obj_uid, or errorCode (str) if error.
    #         IMPORTANT: returned object may be active or inactive.
    #     """
    #     try:
    #         if cls.__name__ in ('Animal', 'AssetItem', 'Asset', 'EntityObject'):
    #             return f'ERR_INP_InvalidArgument {cls}. Class is not instantiable.'
    #     except (TypeError, AttributeError, NameError):
    #         return f'ERR_INP_InvalidArgument {cls}. Class is not instantiable.'
    #
    #     if obj_uid:
    #         try:
    #             obj_uid = UUID(obj_uid)
    #         except(SyntaxError, TypeError, ValueError):
    #             return f'ERR_INP_Invalid or malformed UID for object of class {cls.__name__}. Nothing loaded.'
    #
    #     try:
    #         tbl = cls.tblObjName()       # Accesses cls.__tblObjName if exists, otherwise throws AttributeError except.
    #     except AttributeError:
    #         return f"ERR_DBAccess: Cannot access {cls} Objects table. Invalid name or Objects table does not exist."
    #     else:
    #         conditions_str = f' AND UID_Objeto={obj_uid}' + \
    #                          (f' AND "ID_Clase De Animal"={cls.classID()}' if hasattr(cls, 'classID') else '')
    #         sql = f'SELECT MIN("FechaHora Registro") FROM {getTblName(tbl)} WHERE "Salida YN"=0 OR "Salida YN" IS NULL '\
    #               + conditions_str + ';'
    #         itemsTable = dbRead(tbl, sql)  # temp is a DataTable object.
    #
    #         if not isinstance(itemsTable, DataTable):
    #             return f"ERR_DBAccess: {itemsTable}."
    #
    #     if isinstance(itemsTable, str):
    #         return itemsTable               # Error reading table. Exits.
    #     if not itemsTable.dataList:
    #         return f'UID {obj_uid} not found in table {itemsTable.tblName}. Nothing loaded.'
    #
    #     # Pulls ProgActivities for object
    #     try:
    #         paClass = cls.getPAClass()
    #     except (AttributeError, NameError):
    #         paClass = None  # paClass no definida para cls.
    #     RAP_col = []
    #     if paClass:
    #         # Pulls RAP records ONLY belonging to this node (fldTerminal_ID=TERMINAL_ID). Skips all replicated records.
    #         RAPRecords = getRecords(cls.tblRAPName(), '', '', None, 'fldID', fldTerminal_ID=TERMINAL_ID)
    #         if isinstance(RAPRecords, DataTable) and RAPRecords.dataLen:
    #             RAP_col = tuple(RAPRecords.getCol('fldID'))
    #
    #     itemObj = cls(**itemsTable.unpackItem(0))  # LLAMA a __init__(): Crea Obj. Inicializa lastInv,lastStatus,etc
    #     if itemObj.isValid and not itemObj.exitYN:
    #         # itemObj.register()  # Registra objecto en __registerDict. NO LONGER NEEDED (v 4.1.9)
    #         if paClass:  # Registers progActivity in myProgActivities dict if ProgActivities are defined for object.
    #             # Populates myProgActivities dict ONLY with ProgActivities generated by the node (DB_ID = TERMINAL_ID).
    #             if RAP_col:  # Solo ProgActivities abiertas (sin cierre).
    #                 linkPARecords = getRecords(cls.tblLinkPAName(), '', '', None, '*', fldFK=itemObj.ID,
    #                                            fldFK_ActividadDeCierre=None, fldFK_Actividad=RAP_col)
    #             else:
    #                 linkPARecords = getRecords(cls.tblLinkPAName(), '', '', None, '*', fldFK=itemObj.ID,
    #                                            fldFK_ActividadDeCierre=None)  # Solo ProgActivities sin cierre.
    #             if isinstance(linkPARecords, DataTable):
    #                 fldIDCol = linkPARecords.getCol('fldID')
    #                 # Truco p/ incluir fldFK_ActividadDeCierre=0 porque parsing de getRecords() no implementa por ahora
    #                 temp = getRecords(cls.tblLinkPAName(), '', '', None, '*', fldFK=itemObj.ID,
    #                                   fldFK_ActividadDeCierre=0)
    #                 if isinstance(temp, DataTable) and temp.dataLen:
    #                     for i in range(temp.dataLen):
    #                         if temp.getVal(i, 'fldID') not in fldIDCol and temp.getVal(i, 'fldFK_Actividad') in RAP_col:
    #                             linkPARecords.appendRecord(**temp.unpackItem(i))
    #                 if linkPARecords.dataLen:
    #                     paObjList = [o for o in paClass.getPARegisterDict() if o.ID in
    #                                  linkPARecords.getCol('fldFK_Actividad')]
    #                     if paObjList:
    #                         for o in paObjList:
    #                             itemObj.registerProgActivity(o)  # myProgActivities: {paObj: __activityID}
    #             elif isinstance(linkPARecords, str):
    #                 krnl_logger.warning(
    #                     f'ERR_DBReadError: cannot read from table {cls.tblLinkPAName()}. Error: {linkPARecords}')
    #
    #         if hasattr(itemObj, 'identifiers'):
    #             pass        # Process any updates of tags here.
    #
    #             present_categ = itemObj.category.get(set_category=True)
    #
    #             last_inv = itemObj.inventory.get()
    #             last_status = itemObj.status.get()
    #             print(f'After Category Update: {present_categ}; last_inv: {last_inv}; status: {last_status}. ',
    #                   dismiss_print=DISMISS_PRINT)
    #     # if itemObj.__class__.__name__ == 'Animal':
    #     #     itemObj.updateTimeout_bkgd()  # NO hace falta esto. Solo para testear.
    #
    #
    #
