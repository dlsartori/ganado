from krnl_animal_activity import *

baseexecute = {'execution_date', 'ID', 'fldFK_ClaseDeAnimal', 'fldAgeDays', 'lastLocalization', 'animalRace', 'mf',
                    'lastCategoryID'}

class BovineActivity(AnimalActivity):

    __abstract_class = True         # Para no registrarse en class_register

    def __new__(cls, *args, **kwargs):
        if cls is BovineActivity:
            krnl_logger.error(f"ERR_TypeError: {cls.__name__} cannot be instantiated, please use a subclass")
            raise TypeError(f'ERR_TypeError: class {cls} cannot be instantiated. Please, use one of its subclasses.')
        return super().__new__(cls)


    def __init__(self, activityName, *args, tbl_data_name=None, **kwargs):
        super().__init__(activityName, *args, tbl_data_name=tbl_data_name, **kwargs)

    @classmethod
    def loadTriggers(cls):
        return AnimalActivity.loadTriggers()

# TODO: FALTA Inicializar __activityName, __activityID, invActivity, signatureYN, desde DB

@singleton
class CategoryActivity(BovineActivity):
    __tblDataName = 'tblDataAnimalesCategorias'
    # __tblDataName = BovineActivity.tblDataCategoryName()
    __activityName = 'Categoria'
    __method_name = 'category'
    _short_name = 'catego'  # Used by Activity._paCreate(), Activity.__isClosingActivity()

    __permittedDict = {'2': [3, 4, 12], '3': [4, 12],  '4': [12], '5': [6, 7, 8, 9, 11], '6': [7, 8, 9, 11],
                       '7': [8], '8': [8], '9': [8, 9, 11], '10': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                       'None': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], None: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}
    __base_excluded_fields = ('ID', 'lastLocalization', 'animalRace')     # Campos de baseExecuteFields a ignorar en esta actividad.

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
        """
        @param category:  string. 'Toro', 'Ternero', 'Vaquillona', etc.
        @param args: DataTable objects with obj_data to write to DB
        @param kwargs:  'enforce'=True: forces the Category val irrespective of __statusPermittedDict conditions
                         'categoryID': Category number to set, if Category number is not passed via tblData
        @return: True if success; False if fail
        """
        kwargs['category'] = category
        retValue = self._setCategory(*args, **kwargs)
        retValue = isinstance(retValue, int)
        if self._doInventory(**kwargs):
            _ = self.outerObject.inventory.set()
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
        return self._getCategory(sDate, eDate, *args, **kwargs)


    def compute(self, *, sys_update=False, **kwargs):
        """ *** IMPORTANT: _lastCategoryID in memory MUST BE current and valid for compute() to yield valid results ***
        Computes Category for Animal. IMPORTANT: One specific method for each Animal Class.
        If category changes, category.set() is called to update the new category both in DB and in memory.
        @param sys_update:
        @param kwargs: If USE_DAYS_MULT global parameter is set to True, multiplies age by (SPD * DAYS_MULT)
        @return: New categoryID (int) if category changed. None if category is unchanged.
        """
        outerObj = self.outerObject
        t = time_mt()     # Referencia de tiempo para determinar edad: edad = t-DOB - TODO: Eliminar (SPDAYS*DAYS_MULT)
        ageDays = outerObj.age.get() * (SPD * DAYS_MULT if USE_DAYS_MULT is True else 1)
        currentCategoryID = outerObj.lastCategoryID  # Gets value from memory. Can't use get() here (infinite recursion)
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
        outerObj.getBkgdCategoryChanges()[outerObj] = (outerObj.lastCategoryID, categoryName, f'age:{int(ageDays)}')

        if outerObj.categories[categoryName] != currentCategoryID:   # TODO: CORREGIR ESTO DESPUES DEL DEBUGGING
            print(f'%%%%%%%%%%%%%%%%% Here is the difference: categories[categoryName]='
                  f'{outerObj.categories[categoryName]} / lastCategoryID: {outerObj.lastCategoryID}')

            tblCategory = DataTable(self.__tblDataName)
            tblCategory.setVal(0, fldFK_Categoria=outerObj.categories[categoryName], fldDate=datetime.fromtimestamp(t),
                               fldModifiedBySystem=sysUpdate)

            retValue = self.set(tblCategory, **kwargs)    # Updates DB and memory value (EntityObject._lastCategoryID)
            if isinstance(retValue, str):
                krnl_logger.error(retValue)
                retValue = None
            else:
                # Returns obj to access all attributes for now. TODO: return outer_obj.lastCategoriID in final version.
                retValue = outerObj
        else:
            retValue = None     # Category not updated. Nothing to do...
        return retValue

    # # @Activity.dynamic_attr_wrapper
    # def isBusy(self):
    #     """ Returns >0 if categoryUpdate() method is busy (in execution by another thread/module) """
    #     return self.isCategoryBusy


