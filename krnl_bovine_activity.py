from krnl_animal_activity import *
import inspect


baseexecute = {'execution_date', 'ID', 'fldFK_ClaseDeAnimal', 'fldAgeDays', 'lastLocalization', 'animalRace', 'mf',
                    'lastCategoryID'}

class BovineActivity(AnimalActivity):       # This is an abstract class.
    # TODO: HARDWIRED VALUE. See how to fix this. Used to set up memory_data dictionaries.
    # One way to parameterize this is to use __class__.__name__ and pull the relevant particle from name ('bovine')
    # Animal class for which this class is ActivityClass.
    # @staticmethod
    # def class_name():
    #     return __qualname__

    _animalClassID = None
    # next((v for k, v in AnimalActivity.getAnimalKinds().items() if k in __qualname__.lower()), None)
    for k, v in AnimalActivity.getAnimalKinds().items():
        if __qualname__.lower().startswith(k.lower()):       # MUST use ==, to single out bovine from ovine.
            _animalClassID = v
            break

    if _animalClassID is None:
        raise NameError(f'ERR_Class name: class {__qualname__} cannot be associated to a valid Animal class.')

    __abstract_class = True         # Para no registrarse en class_register
    __invariant_categories = {}  # {categoryID: categoryName, } Vaca, Novillo,Vaca CUT,etc: Categories that don't change
    __tblObjectsName = 'tblAnimalesCategorias'

    # dict used with Activity classes. Accessed from Activity class. Stores AnimalActivity class objects.
    # Used to create Activity instance objects and to bind, when applicable, ActivityClasses with Object classes.
    _activity_class_register = {}  # {ActivityClass: ObjectClass | None, }

    @classmethod
    def animalClassID(cls):
        return cls._animalClassID

    # temp = dbRead(__tblObjectsName, f'SELECT * FROM "{getTblName(__tblObjectsName)}" '
    #                                 f'WHERE "{getFldName(__tblObjectsName, "fldFK_ClaseDeAnimal")}" == '
    #                                 f'{_animalClassID} AND "{getFldName(__tblObjectsName, "fldMF")}" > 0; ')

    tempdf = pd.read_sql_query(f'SELECT * FROM "{getTblName(__tblObjectsName)}" '
                                    f'WHERE "{getFldName(__tblObjectsName, "fldFK_ClaseDeAnimal")}" == '
                                    f'{_animalClassID} AND "{getFldName(__tblObjectsName, "fldMF")}" > 0; ',
                                    SQLiteQuery().conn)

    __categ_by_weight = {2: True, 3: True, 5: True, 6: True, 7: True}  # Category changes enabled by weigth limits.
    # Category changes enabled by parturition (delivery of 1st calf)
    __categ_by_pregnancy_parturition = {2: True, 3: True}  # Upon 1st pregnancy, ternera or vaquillona move to vaca.

    # Caution: bool(nan) -> True / bool(pd.NA) -> Fails with TypeError.
    categ_names = dict(zip(tempdf['fldID'], tempdf['fldName']))         # {categoryID: categoryName, }
    __categ_mf = dict(zip(tempdf['fldID'], tempdf['fldMF']))            # {categoryID: m | f, }
    # {categoryID: True/False for castrated condition of categoryID, }
    __categ_castrated = dict(zip(tempdf['fldID'],                       # {categoryID: Castrated(True/False), }
                                 tempdf['fldFlagCastrado'].fillna(np.nan).replace([np.nan], [None]).apply(bool)))
    __ageLimits = dict(zip(tempdf[tempdf['fldLimitDays'] > 0]['fldID'],
                           tempdf[tempdf['fldLimitDays'] > 0]['fldLimitDays']))     # {categoryID: limit_days, }

    # print(f'categories: {categ_names}')
    # print(f'__ageLimits: {__ageLimits}')
    print(f'__categ_mf: {__categ_mf}\n__categ_castrated: {__categ_castrated}')
    # print(f'__categ_by_weight: {__categ_by_weight}\n__categ_by_parturition: {__categ_by_pregnancy_parturition}')
    del tempdf

    tempdf = pd.read_sql_query(f'SELECT * FROM "{getTblName(__tblObjectsName)}" '
                                    f'WHERE "{getFldName(__tblObjectsName, "fldFK_ClaseDeAnimal")}" == '
                                    f'{_animalClassID} AND "{getFldName(__tblObjectsName, "fldInvariant")}" > 0; ',
                               SQLiteQuery().conn)

    # Si tempdf is empty, se deja vacio __invariant_categories: Todas las categorias se computan.
    if not tempdf.empty:
        # cols = temp.getCols('fldID', 'fldName')
        __invariant_categories = dict(zip(tempdf['fldID'].tolist(), tempdf['fldName'].tolist()))
    del tempdf


    def __new__(cls, *args, **kwargs):
        if cls is BovineActivity:
            krnl_logger.error(f"ERR_TypeError: {cls.__name__} cannot be instantiated, please use a subclass")
            raise TypeError(f'ERR_TypeError: class {cls} cannot be instantiated. Please, use one of its subclasses.')
        return super().__new__(cls)


    def __init__(self, activityName, *args, tbl_data_name=None, **kwargs):
        super().__init__(activityName, *args, tbl_data_name=tbl_data_name, **kwargs)

    @classmethod
    def category_names(cls):         # {categoryID: categoryName,  }
        return cls.categ_names

    @classmethod
    def categ_castrated(cls):        # {categoryID: castration_ability,  }
        return cls.__categ_castrated

    @classmethod
    def categ_by_weight(cls):
        return cls.__categ_by_weight

    @classmethod
    def categ_mf(cls):
        return cls.__categ_mf

    @classmethod
    def categ_by_pregnancy_parturition(cls):
        return cls.__categ_by_pregnancy_parturition

    @classmethod
    def get_invariant_categories(cls):
        return cls.__invariant_categories  # {categoryID: categoryName, }. Used on CategoryActivity._compute()

    @classmethod
    def getAgeLimits(cls):  # {categoryID: limit_days, }
        return cls.__ageLimits

    @classmethod
    def ageLimit(cls, categ):
        """ Returns the age limit for category val, if exists
        @param categ: int or str. Animal categoryID or categoryName.
        @return: ageLimit for val (int).
        """
        if isinstance(categ, str):
            return next((cls.__ageLimits.get(k) for k in cls.categ_names if categ.lower().strip() in
                         cls.categ_names.get(k).lower()), None)  # Returns none if is not a valid key
        return cls.__ageLimits.get(categ, None)  # Returns none if is not a valid key


# TODO: FALTA Inicializar __activityName, __activityID, invActivity, signatureYN, desde DB

@singleton
class CategoryActivity(BovineActivity):
    __tblDataName = 'tblDataAnimalesCategorias'
    __tblObjectsName = 'tblAnimalesCategorias'
    __activityName = 'Categoria'
    __method_name = 'category'
    _short_name = 'catego'  # Used by Activity._paCreate(), Activity.__isClosingActivity()
    _activities_dict = {}  # {fldNombreActividad: fldID_Actividad, }. Initialized from Bovine class.

    # Params Used in default memory_data functions and class, in Activity.
    _mem_data_params = {'field_names': ('fldDate', 'fldFK_Actividad', 'fldFK_Categoria', 'fldDOB', 'fldMF')}
    # dict is Local to this Activity. Initialized in EntityObject. Must be dunder because class must be singled-out.
    __local_active_uids_dict = {}  # {object_class: {uid: MemoryData object, }, }.
    _slock_activity_mem_data = AccessSerializer()  # Used to manage concurrent access to memory, per-Activity basis.

    @classmethod
    def tblDataName(cls):
        return cls.__tblDataName

    @classmethod
    def getActivityName(cls):
        return cls.__activityName

    __permittedDict = {2: [3, 4, 12], 3: [4, 12],  4: [12], 5: [6, 7, 8, 9, 11], 6: [7, 8, 9, 11],
                       7: [8], 8: [8], 9: [8, 9, 11], 10: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                       'None': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], None: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}

    @classmethod
    def permittedDict(cls):
        return cls.__permittedDict

    __base_excluded_fields = ('ID', 'lastLocalization', 'animalRace')     # Campos de baseExecuteFields a ignorar en esta actividad.


    def __init__(self, *args, **kwargs):
        # Registers activity_name to prevent re-instantiation in GenericActivity. Also useful in _load_memory_data_last_rec() func
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = False            # No PA for category activity.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def permittedFrom(self):  # Lista de Status permitidos para Animal, a partir de a status inicial(From)
        return self.__permittedDict

    def _baseExcludedFields(self):  # Lista de Status permitidos para Animal, a partir de a status inicial(From)
        return self.__base_excluded_fields

    @classmethod                # CategoryActivity.
    def _memory_data_init_last_rec(cls, obj_class, active_uids: set, *, max_col=None, **kwargs):
        #                **** TODO(cmt): This function is designed to be run in its own thread! ****
        """ Initializer for Category memory data that keeps LAST available database RECORD.
        Overrides base method in Activity class. Re-uses some of its code.
        TODO(cmt): Loads last categories from database and DOES NOT UPDATE category values (doesn't compute new
         categories). This is by design, to save time. Then categories will be computed as required by the running code.
        Called from EntityObject __init_subclass__(). cls._memory_data_classes are initialized at this call.
        cls is InventoryActivity, StatusActivity, LocalizationActivity, BovineActivity.CategoryActivity, etc.
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
        tbl_obj = cls.tblObjName()
        # TODO: Fields names additional to those from Activity's tblData. fldObjectUID is a Link field. MUST be present!
        tbl_obj_fld_names = ('fldObjectUID', 'fldDOB', 'fldMF')     # ('fldDate', 'fldFK_Actividad', 'fldFK_Categoria', 'fldDOB', 'fldMF')
        uid_fld_name = next((j for j in tbl_obj_fld_names if 'objectuid' in j.lower()), None)
        uid_db_fld_name = getFldName(tbl_obj, uid_fld_name)
        local_uids_dict = getattr(cls, '_' + cls.__name__ + '__local_active_uids_dict', None)

        if local_uids_dict is not None:        # This is None for classes that don't support_mem_data(). If so, exits.
            if not isinstance(max_col, str):
                max_col = 'fldDate'  # Default column to pull max date.
            if obj_class not in local_uids_dict:
                local_uids_dict[obj_class] = pd.DataFrame([], columns=list(cls._mem_data_params['field_names']))
            if not hasattr(cls, '_animalClassID') or cls.animalClassID() == obj_class.animalClassID():
                # fldDOB, fldMF belong to a different table. Must be dealt with separately (down below...)
                tblData_fields = getFldName(cls.tblDataName(), '*', mode=1)  # {fldName: dbFldName, }
                flds = [f for f in cls._mem_data_params['field_names'] if f in tblData_fields]  # Required, as _mem_data_params may hold fields from multiple tables.
                # loaded_dict an initial_category repository for all Animals. New categories are NOT COMPUTED here!
                # Then, with these initial categories, new ones will be computed on the fly, as required.
                # dicto is of the form {uid: tuple_of_values, }
                df_loaded = cls._load_memory_data_last_rec(tbl=cls.tblDataName(), keys=set(active_uids), flds=flds,
                                                           max_col=max_col)
                obj_fld_names_str = ", ".join([f'"{getFldName(tbl_obj, j)}"' for j in tbl_obj_fld_names])
                sql = 'SELECT ' + obj_fld_names_str + f' FROM "{getTblName(tbl_obj)}" WHERE "{uid_db_fld_name}" ' \
                                                  f'IN {str(tuple(active_uids))}; '
                df_obj = pd.read_sql_query(sql, SQLiteQuery().conn)     # df_obj columns: (fldObjectUID, fldDOB, fldMF)
                if df_obj.empty or df_loaded.empty:
                    return

                index = 'fldFK'     # Index to set on df_loaded.
                df_loaded.set_index(index, inplace=True)      # df_loaded comes from tblLink, hence 'fldFK'
                df_obj.set_index('fldObjectUID', inplace=True)  # df_obj comes from tblObject hence 'fldObjectUID'
                joined = df_loaded.join(df_obj)
                # Renames cols to use all names in _mem_data_params. Some names are changed by the sql_read decorator.
                dfcols = joined.columns.tolist()
                if any(cname not in cls._mem_data_params['field_names'] for cname in dfcols):
                    if len(dfcols) != len(cls._mem_data_params['field_names']):
                        raise AttributeError(f'ERR_MemData Init: DataFrame columns do not match Memory Data template.'
                                             f' Aborting Memory Data initialization for Activity {cls.__name__}.')
                    joined.rename(columns=dict(zip(joined.columns.to_list(), cls._mem_data_params['field_names'])),
                                  inplace=True)
                # joined_dict = df_loaded.join(df_obj).agg(tuple, axis=1).to_dict()

                # Now resolves potential duplicate records in the dataframes picking the uids with the latest fldDate.
                df_obj_class = local_uids_dict[obj_class]
                if not df_obj_class.empty:
                    merged = pd.merge(df_obj_class, joined, how='outer')
                    local_uids_dict[obj_class] = merged.groupby(index)[max_col].max()       # Keeps the record with highest fldDate (hopefully!)
                else:
                    local_uids_dict[obj_class] = joined
                dicto = {"uid": local_uids_dict[obj_class].iloc[0].to_dict()}      # Printing Category mem_data.
                print(f'MEMORY DATA for {cls.__name__}: {len(local_uids_dict[obj_class])} uid items. Item form: '
                      f'{dicto}', dismiss_print=DISMISS_PRINT)
            return


    def set(self, *args, category='', enforce=False,  **kwargs):
        """     TODO(cmt): This method should be deprecated: A no-action function kept only for overall compatibility.
        @param enforce: True-> forces the Category val irrespective of __statusPermittedDict conditions
        @param category:  string. 'Toro', 'Ternero', 'Vaquillona', etc.
        @param args: DataTable objects with obj_data to write to DB
        @param kwargs:  'enforce'=True: forces the Category val irrespective of __statusPermittedDict conditions
                         'categoryID': Category number to set, if Category number is not passed via tblData
        @return: True if success; False or None if fail
        """
        kwargs['category'] = category
        kwargs['enforce'] = enforce
        retValue = self._setCategory(*args, **kwargs)
        # retValue = isinstance(retValue, int)

        return retValue


    def get(self, *args, mode='mem', id_type='id', uid=None, full_record=False, set_category=False, all_records=False):
        """
        Returns records in table Data Animales Categorias between sValue and eValue. sValue=eValue ='' ->Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode='value' -> Returns value from DB. If no mode or mode='memory' returns value from Memory.
               id_type='name'-> Category Name; id_type='id' or no id_type -> Category ID.
               uid = Animal uid when NOT called via an outerObject.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        return self._getCategory(*args, mode=mode, id_type=id_type, uid=uid, full_record=full_record,
                                 set_category=set_category, all_records=all_records)


    def _compute(self, *, initial_category=None, enforce_computation=False, verbose=False, uid=None, set_category=False,
                  **kwargs):
        """ *** IMPORTANT: _lastCategoryID in memory NO LONGER USED as of 4.1.9.
            *** Writes to DB only when category_set=True  ***
        If USE_DAYS_MULT global parameter is set to True, multiplies age by DAYS_MULT value.
        computes value and RETURNS results ***
        IMPORTANT: One specific method for each Animal Class.
        @param set_category: If True, calls category.set() when there's a category change. Default: False.
        @param initial_category: Mandatory (int). categoryID used as starting point for computation of new category.
        @param enforce_computation: Forces computation of category, regardless of __invariant status.
        @param uid: Animal uid to generate Animal object from DB.
        @param kwargs:
        @return: New categoryID (int) if category changed; initial_category (int) if unchanged. None if error.
        """
        # if not initial_category:
        #     initial_category = self.get(uid=uid)   # _getCategory() call -> Avoid calling _getCategory here for now...
        if initial_category not in self.category_names():
            e = f'ERR_SYS: Cannot get Category for animal {getattr(self, "outerObject.recordID", None) or uid}.'
            krnl_logger.warning(e)
            raise ValueError(f'{e}')           # Raises error if initial category cannot be determined.

        initialCategoryID = initial_category
        if initialCategoryID in self.get_invariant_categories() and not enforce_computation:    # __invariant_categories = {categoryID: categoryName}
            return initialCategoryID

        # Gets outerObj if func is called by an outer object or creates new object from db via getObject().
        # self.outerObject will be an instance Bovine, Caprine, etc., or the CLASS Bovine, Caprine, etc.
        outerObj = self.outerObject
        if not outerObj:
            return None

        mf = None
        if type(outerObj) is type:
            # Calling compute with class Bovine, Caprine, etc.
            local_uids_dict = self.get_local_uids_dict()[outerObj]
            mem_record = local_uids_dict[uid].record
            dob = mem_record.get('fldDOB', None)            # Gets dob date from memory
            mf = mem_record.get('fldMF', None).lower()      # gets mf data from memory
            if isinstance(dob, datetime):
                ageDays = int((time_mt() - dob.timestamp()))/SPD * (DAYS_MULT if USE_DAYS_MULT is True else 1)
            else:
                return initialCategoryID     # Cannot get a dob when outerObject is a class. Returns same category.
        else:
            ageDays = outerObj.age.get()

        # Get next categoryID ("after" or "above" initialCategoryID), by gender (MF).
        initialCategoryMF = self.categ_mf().get(initialCategoryID) or None
        if not initialCategoryMF:
            return initialCategoryID     # Returns initial category if error is found when pulling Male/Female status.

        ageLimitsByMF = {k: v for k, v in self.getAgeLimits().items() if initialCategoryMF in self.categ_mf()[k]}
        # Filters categories further by castrated status, narrowing the allowed categories in ageLimitsByMF
        if initialCategoryMF in self.categ_castrated():
            ageLimitsByMF = {k: v for k, v in ageLimitsByMF.items() if k in self.categ_castrated()}
        else:
            ageLimitsByMF = {k: v for k, v in ageLimitsByMF.items() if k not in self.categ_castrated()}

        if ageLimitsByMF:
            nextCategoryDict = {j: ageLimitsByMF[j] for j in ageLimitsByMF if ageLimitsByMF[j] <= ageDays}
        else:
            return initialCategoryID  # Returns initial category if no ageLimits are found (It's an invariant category).
        # next Category: the one with the largest ageLimit found. - Empty list -> no categories available: no change.
        nextCategoryID = max(nextCategoryDict, key=nextCategoryDict.get) if nextCategoryDict else initialCategoryID
        if nextCategoryID == initialCategoryID:
            return initialCategoryID             # Exits if no change in category.

        categoryID = initialCategoryID
        if not mf:
            mf = outerObj.mf.lower()    # pulls mf from Animal object.
        if 'f' in mf:
            if ageDays >= self.ageLimit(nextCategoryID):
                categoryID = nextCategoryID
        else:
            if ageDays >= self.ageLimit(nextCategoryID):
                categoryID = nextCategoryID

        # TODO(cmt): Gets an Animal object and writes the new category to db tables.
        if categoryID != initialCategoryID and set_category is True:
            obj = outerObj if type(outerObj) is not type else outerObj.getObject(uid)
            obj.category.set(category=categoryID)
        categoryName = self.category_names()[categoryID]

        # TODO: solo se debe almacenar outerObj.lastCategoryID en DB. Los otros 2, por ahora, para debugging.
        if type(outerObj) is not type:      # Executes below ONLY if outerObject is an Animal object (not a class).
            outerObj.getBkgdCategoryChanges()[outerObj] = (categoryName, f'age:{int(ageDays)}')
            if outerObj.categories[categoryName] != initialCategoryID and verbose:  # TODO: QUITAR ESTO DESPUES DE DEBUGGING
                print(f'%%%%%%%%%%%%%% Bovine._compute({lineNum()}): Here is the difference: New Category='
                      f'{outerObj.categories[categoryName]} / Initial Category: {initialCategoryID}')

        retValue = categoryID
        return retValue


