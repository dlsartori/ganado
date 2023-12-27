from krnl_animal_activity import *

baseexecute = {'execution_date', 'ID', 'fldFK_ClaseDeAnimal', 'fldAgeDays', 'lastLocalization', 'animalRace', 'mf',
                    'lastCategoryID'}

class BovineActivity(AnimalActivity):

    _animalClassID = 1         # TODO: HARDWIRED VALUE. See how to fix this.
    __abstract_class = True         # Para no registrarse en class_register

    def __new__(cls, *args, **kwargs):
        if cls is BovineActivity:
            krnl_logger.error(f"ERR_TypeError: {cls.__name__} cannot be instantiated, please use a subclass")
            raise TypeError(f'ERR_TypeError: class {cls} cannot be instantiated. Please, use one of its subclasses.')
        return super().__new__(cls)


    def __init__(self, activityName, *args, tbl_data_name=None, **kwargs):
        super().__init__(activityName, *args, tbl_data_name=tbl_data_name, **kwargs)

    # @classmethod
    # def loadTriggers(cls):
    #     return AnimalActivity.loadTriggers()

# TODO: FALTA Inicializar __activityName, __activityID, invActivity, signatureYN, desde DB

@singleton
class CategoryActivity(BovineActivity):
    __tblDataName = 'tblDataAnimalesCategorias'
    __tblObjectsName = 'tblAnimalesCategorias'
    __activityName = 'Categoria'
    __method_name = 'category'
    _short_name = 'catego'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    __permittedDict = {'2': [3, 4, 12], '3': [4, 12],  '4': [12], '5': [6, 7, 8, 9, 11], '6': [7, 8, 9, 11],
                       '7': [8], '8': [8], '9': [8, 9, 11], '10': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                       'None': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], None: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}
    __base_excluded_fields = ('ID', 'lastLocalization', 'animalRace')     # Campos de baseExecuteFields a ignorar en esta actividad.

    __invariant_categories = {}  # {categoryID: categoryName, } Vaca, Novillo,Vaca CUT,etc: Categories that don't change

    @classmethod
    def get_invariant_categories(cls):
        return cls.__invariant_categories    # {categoryID: categoryName, }. Used on CategoryActivity.compute()

    temp = dbRead(__tblObjectsName, f'SELECT * FROM "{getTblName(__tblObjectsName)}" '
                                 f'WHERE "{getFldName(__tblObjectsName, "fldFK_ClaseDeAnimal")}" == '
                                 f'{BovineActivity._animalClassID} AND '
                                 f'"{getFldName(__tblObjectsName, "fldInvariant")}" > 0; ')
    # Si falla dbRead(), se deja vacio __invariant_categories: Todas las categorias se computan.
    if temp and not isinstance(temp, str):
        cols = temp.getCols('fldID', 'fldName')
        __invariant_categories = dict(zip(cols[0], cols[1]))
    del temp


    def __init__(self, *args, **kwargs):
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        kwargs['supportsPA'] = False            # No PA for category activity.
        kwargs['decorator'] = self.__method_name
        # super().__init__(*args, tbl_data_name=self.__tblDataName, **kwargs)
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    def permittedFrom(self):  # Lista de Status permitidos para Animal, a partir de a status inicial(From)
        return self.__permittedDict


    def _baseExcludedFields(self):  # Lista de Status permitidos para Animal, a partir de a status inicial(From)
        return self.__base_excluded_fields


    def set(self, *args, category='', **kwargs):
        """     TODO(cmt): This method should be deprecated: A no-action function kept only for overall compatibility.
        @param category:  string. 'Toro', 'Ternero', 'Vaquillona', etc.
        @param args: DataTable objects with obj_data to write to DB
        @param kwargs:  'enforce'=True: forces the Category val irrespective of __statusPermittedDict conditions
                         'categoryID': Category number to set, if Category number is not passed via tblData
        @return: True if success; False if fail
        """
        kwargs['category'] = category
        retValue = self._setCategory(*args, **kwargs)
        retValue = isinstance(retValue, int)
        # if self._doInventory(**kwargs):
        #     _ = self.outerObject.inventory.set()
        return retValue


    def get(self, sDate=None, eDate=None, *args, **kwargs):
        """
        Returns records in table Data Animales Categorias between sValue and eValue. sValue=eValue ='' ->Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode='value' -> Returns value from DB. If no mode or mode='memory' returns value from Memory.
        @return: Object DataTable with information from queried table or statusID (int) if mode=val
        """
        # return self._getCategory(sDate, eDate, *args, **kwargs)
        return self.outerObject.category.compute()      # computes in real time, with last value of animal age().


    def compute(self, *, sys_update=False, enforce_computation=False, verbose=False, **kwargs):
        """ *** IMPORTANT: _lastCategoryID in memory NO LONGER USED as of 4.1.9.
            *** DOES NOT WRITE DATA TO DB, IT UPDATES __memory_data DICTIONARY  ***
        If USE_DAYS_MULT global parameter is set to True, multiplies age by DAYS_MULT value.
        computes value and RETURNS results ***
        IMPORTANT: One specific method for each Animal Class.
        @param enforce_computation: Forces computation of category, regardless of __invariant status.
        @param sys_update: Defines system-generated updates. OBSOLETE NOW. DO NOT USE.
        @param kwargs:
        @return: New categoryID (int) if category changed. None if category is unchanged.
        """
        outerObj = self.outerObject
        initialCategoryID = outerObj.get_memory_data().get('last_category') or None
        if initialCategoryID in self.__invariant_categories and not enforce_computation:    # __invariant_categories = {categoryID: categoryName}
            return initialCategoryID

        ageDays = outerObj.age.get() * (DAYS_MULT if USE_DAYS_MULT is True else 1)
        sysUpdate = sys_update
        if 'f' in outerObj.mf.lower():
            if ageDays < outerObj.ageLimit('ternera'):
                # if Bovine.__setVaquillonaByWeight:  # TODO:Cambiar Categoria por peso ->operaciones de pesaje,aqui no.
                #     if obj.getWeight() > == Bovine.__weightLimitTernera:
                #         category = 'vaquillona'
                # if obj.pregnancy.getTotal() == 1  # Si total Pariciones = 1, es Vaquillona.
                # category = 'vaquillona'
                categoryName = 'ternera'
            elif ageDays < outerObj.ageLimit('vaquillona'):
                categoryName = 'vaquillona'
            else:
                categoryName = 'vaca'
        else:
            if outerObj.isCastrated:
                categoryName = 'novillito' if ageDays < outerObj.ageLimit('novillito') else 'novillo'
            else:
                if ageDays < outerObj.ageLimit('ternero'):
                    categoryName = 'ternero'
                elif ageDays < outerObj.ageLimit('torito'):
                    categoryName = 'torito'
                else:
                    if ageDays > outerObj.ageLimit('novillito'):
                        if outerObj.novilloByAge():
                            categoryName = 'novillo'
                        else:
                            categoryName = 'toro'
                    else:
                        categoryName = 'novillito'
        categoryID = self.outerObject.categories[categoryName]

        # TODO(cmt): Updates memory_data dict here, so that it's transparent to get() and set()
        self.outerObject.get_memory_data()['last_category'] = categoryID

        # TODO: solo se debe almacenar outerObj.lastCategoryID en DB. Los otros 2, por ahora, para debugging.
        outerObj.getBkgdCategoryChanges()[outerObj] = (categoryName, f'age:{int(ageDays)}')

        if outerObj.categories[categoryName] != initialCategoryID and verbose:  # TODO: QUITAR ESTO DESPUES DE DEBUGGING
            print(f'%%%%%%%%%%%%%% Bovine.compute({lineNum()}): Here is the difference: New Category='
                  f'{outerObj.categories[categoryName]} / Initial Category: {initialCategoryID}')

            # tblCategory = DataTable(self.__tblDataName)
            # tblCategory.setVal(0, fldFK_Categoria=outerObj.categories[categoryName], fldDate=datetime.fromtimestamp(t),
            #                    fldModifiedBySystem=sysUpdate)
            #
            # retValue = self.set(tblCategory, **kwargs)    # Updates DB and memory value (EntityObject._lastCategoryID)
            # if isinstance(retValue, str):
            #     krnl_logger.error(retValue)
            #     retValue = None
            # else:
            #     # Returns obj to access all attributes for now. TODO: return outer_obj.categoryID in final version.
            #     retValue = outerObj
            retValue = initialCategoryID
        else:
            retValue = categoryID     # Category not updated. Nothing to do...
        return retValue




    def compute01(self, *, sys_update=False, **kwargs):
        """ *** IMPORTANT: _lastCategoryID in memory NO LONGER USED as of 4.1.9.
        computes value and RETURNS results ***
        IMPORTANT: One specific method for each Animal Class.
        If category changes, category.set() is called to update the new category both in DB and in memory.
        @param sys_update:
        @param kwargs: If USE_DAYS_MULT global parameter is set to True, multiplies age by (SPD * DAYS_MULT)
        @return: New categoryID (int) if category changed. None if category is unchanged.
        """
        outerObj = self.outerObject
        initialCategoryID = outerObj.category.get()
        if initialCategoryID in self.__invariant_categories:
            return initialCategoryID

        t = time_mt()     # Referencia de tiempo para determinar edad: edad = t-DOB - TODO: Eliminar (SPDAYS*DAYS_MULT)
        ageDays = outerObj.age.get() * (DAYS_MULT if USE_DAYS_MULT is True else 1)
        sysUpdate = sys_update
        if 'f' in outerObj.mf.lower():
            if ageDays < outerObj.ageLimit('Ternera'):
                # if Bovine.__setVaquillonaByWeight:  # TODO:Cambiar Categoria por peso ->operaciones de pesaje,aqui no.
                #     if obj.getWeight() > == Bovine.__weightLimitTernera:
                #         category = 'Vaquillona'
                # if obj.pregnancy.getTotal() == 1  # Si total Pariciones = 1, es Vaquillona.
                # category = 'Vaquillona'
                categoryName = 'Ternera'
            elif ageDays < outerObj.ageLimit('Vaquillona'):
                categoryName = 'Vaquillona'
            else:
                categoryName = 'Vaca'
        else:
            if outerObj.isCastrated:
                categoryName = 'Novillito' if ageDays < outerObj.ageLimit('Novillito') else 'Novillo'
            else:
                if ageDays < outerObj.ageLimit('Ternero'):
                    categoryName = 'Ternero'
                elif ageDays < outerObj.ageLimit('Torito'):
                    categoryName = 'Torito'
                else:
                    if ageDays > outerObj.ageLimit('Novillito'):
                        if outerObj.novilloByAge():
                            categoryName = 'Novillo'
                        else:
                            categoryName = 'Toro'
                    else:
                        categoryName = 'Novillito'

        # TODO: solo se debe almacenar outerObj.lastCategoryID en DB. Los otros 2, por ahora, para debugging.
        outerObj.getBkgdCategoryChanges()[outerObj] = (categoryName, f'age:{int(ageDays)}')

        if outerObj.categories[categoryName] != initialCategoryID:   # TODO: CORREGIR ESTO DESPUES DEL DEBUGGING
            print(f'%%%%%%%%%%%%%%%%% Here is the difference: categories[categoryName]='
                  f'{outerObj.categories[categoryName]} / last Category: {initialCategoryID}')

            tblCategory = DataTable(self.__tblDataName)
            tblCategory.setVal(0, fldFK_Categoria=outerObj.categories[categoryName], fldDate=datetime.fromtimestamp(t),
                               fldModifiedBySystem=sysUpdate)

            retValue = self.set(tblCategory, **kwargs)    # Updates DB and memory value (EntityObject._lastCategoryID)
            if isinstance(retValue, str):
                krnl_logger.error(retValue)
                retValue = None
            else:
                # Returns obj to access all attributes for now. TODO: return outer_obj.categoryID in final version.
                retValue = outerObj
        else:
            retValue = None     # Category not updated. Nothing to do...
        return retValue


    # def compute00(self, *, sys_update=False, **kwargs):
    #     """ *** IMPORTANT: _lastCategoryID in memory MUST BE current and valid for compute() to yield valid results ***
    #     Computes Category for Animal. IMPORTANT: One specific method for each Animal Class.
    #     If category changes, category.set() is called to update the new category both in DB and in memory.
    #     @param sys_update:
    #     @param kwargs: If USE_DAYS_MULT global parameter is set to True, multiplies age by (SPD * DAYS_MULT)
    #     @return: New categoryID (int) if category changed. None if category is unchanged.
    #     """
    #     outerObj = self.outerObject
    #     t = time_mt()     # Referencia de tiempo para determinar edad: edad = t-DOB - TODO: Eliminar (SPDAYS*DAYS_MULT)
    #     ageDays = outerObj.age.get() * (SPD * DAYS_MULT if USE_DAYS_MULT is True else 1)
    #     currentCategoryID = outerObj.lastCategoryID  # Gets value from memory. Can't use get() here (infinite recursion)
    #     sysUpdate = sys_update
    #     if 'f' in outerObj.mf.lower():
    #         if ageDays < outerObj.ageLimit('Ternera'):
    #             # if Bovine.__setVaquillonaByWeight:  # TODO:Cambiar Categoria por peso ->operaciones de pesaje,aqui no.
    #             #     if obj.getWeight() > == Bovine.__weightLimitTernera:
    #             #         category = 'Vaquillona'
    #             # if obj.pregnancy.getTotal() == 1  # Si total Pariciones = 1, es Vaquillona.
    #             # category = 'Vaquillona'
    #             categoryName = 'Ternera'
    #         elif ageDays < outerObj.ageLimit('Vaquillona'):
    #             categoryName = 'Vaquillona'
    #         else:
    #             categoryName = 'Vaca'
    #     else:
    #         if outerObj.isCastrated:
    #             categoryName = 'Novillito' if ageDays < outerObj.ageLimit('Novillito') else 'Novillo'
    #         else:
    #             if ageDays < outerObj.ageLimit('Ternero'):
    #                 categoryName = 'Ternero'
    #             elif ageDays < outerObj.ageLimit('Torito'):
    #                 categoryName = 'Torito'
    #             else:
    #                 if ageDays > outerObj.ageLimit('Novillito'):
    #                     if outerObj.novilloByAge():
    #                         categoryName = 'Novillo'
    #                     else:
    #                         categoryName = 'Toro'
    #                 else:
    #                     categoryName = 'Novillito'
    #
    #     # TODO: solo se debe almacenar outerObj.lastCategoryID en DB. Los otros 2, por ahora, para debugging.
    #     outerObj.getBkgdCategoryChanges()[outerObj] = (outerObj.lastCategoryID, categoryName, f'age:{int(ageDays)}')
    #
    #     if outerObj.categories[categoryName] != currentCategoryID:   # TODO: CORREGIR ESTO DESPUES DEL DEBUGGING
    #         print(f'%%%%%%%%%%%%%%%%% Here is the difference: categories[categoryName]='
    #               f'{outerObj.categories[categoryName]} / lastCategoryID: {outerObj.lastCategoryID}')
    #
    #         tblCategory = DataTable(self.__tblDataName)
    #         tblCategory.setVal(0, fldFK_Categoria=outerObj.categories[categoryName], fldDate=datetime.fromtimestamp(t),
    #                            fldModifiedBySystem=sysUpdate)
    #
    #         retValue = self.set(tblCategory, **kwargs)    # Updates DB and memory value (EntityObject._lastCategoryID)
    #         if isinstance(retValue, str):
    #             krnl_logger.error(retValue)
    #             retValue = None
    #         else:
    #             # Returns obj to access all attributes for now. TODO: return outer_obj.lastCategoriID in final version.
    #             retValue = outerObj
    #     else:
    #         retValue = None     # Category not updated. Nothing to do...
    #     return retValue
