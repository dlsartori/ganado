from krnl_abstract_class_animal import *
from krnl_tag import Tag
from krnl_config import time_mt
from datetime import datetime
from threading import Lock, Condition

                                                # CAPRINOS #


def moduleName():
    return str(os.path.basename(__file__))


class Config:
    arbitrary_types_allowed = True

# @pydantic.dataclasses.dataclass(config=Config)


class Caprine(Mammal):
    # __objClass = 6
    # __objType = 1
    __moduleRunning = 1   # Flag que indica si el modulo de la clase de Animal se esta ejecutando. TODO: Ver como setear
    _kindOfAnimalID = 2      # Caprino = 2
    __registerDict = {}  # {idAnimal: objAnimal}-> Registro General de Animales: Almacena Animales de la clase

    @classmethod
    def getRegisterDict(cls):
        return cls.__registerDict

    # @classmethod
    # def getKindOfAnimal(cls):
    #     return cls._kindOfAnimalID

    @classmethod
    def registerKindOfAnimal(cls):
        Animal.getAnimalClasses()[cls] = cls._kindOfAnimalID

    temp = getRecords('tblAnimalesActividadesNombres', '', '', None, 'fldID', 'fldName', 'fldFlag',
                         'fldFK_ClaseDeAnimal')
    __caprineActivitiesDict = {}  # __caprineActivitiesDict = {fldNombreActividad: fldID_Actividad,}
    __isInventoryActivity = {}          # _isInventoryActivity = {fldNombreActividad: _isInventoryActivity (1/0),}
    for j in range(temp.dataLen):
        if temp.dataList[j][3] == 0 or temp.dataList[j][3] == _kindOfAnimalID:
            # 0: Define en DB una Actividad que aplica a TODAS las clases de Animales
            __caprineActivitiesDict[temp.dataList[j][1]] = temp.dataList[j][0]
            __isInventoryActivity[temp.dataList[j][1]] = temp.dataList[j][2]

    def register(self):
        """
        Inserts Caprine Object and ID into Caprine.__registerDict
        @return:
        """
        Caprine.__registerDict[self.getID] = self
        return self.getID

    def unRegister(self):  # Remueve obj .__registerDict. A ver si funciona esto como metodo...
        """
        Removes object from .__registerDict
        @return: removed object ID if successful. False if fails.
        """
        return Caprine.__registerDict.pop(self.getID, False)  # Retorna False si no encuentra el objeto

    temp1 = getRecords('tblAnimalesCategorias', '', '', None, '*', fldFK_ClaseDeAnimal=_kindOfAnimalID)
    categories = {}         # {Category Name: ID_Categoria, }
    if temp1.dataLen:
        for j in range(temp1.dataLen):
            categories[temp1.getVal(j, 'fldName')] = temp1.getVal(j, 'fldID')

    @classmethod
    def getActivitiesDict(cls):
        return cls.__caprineActivitiesDict

    @property
    def activities(self):  # @property activities es necesario para las llamadas a obj.outerObject.
        return self.__caprineActivitiesDict


    @classmethod
    def getInventoryActivitiesDict(cls):
        return cls.__isInventoryActivity

    def __init__(self, *args, **kwargs):            # TODO: FIX THE VALIDATION CODE BELOW.
        # class ArgsCaprine(BaseModel):
        #     tags: list = []
        #     kwargs: dict
        #     # strip_str: constr(strip_whitespace=True)
        #     # lower_str = constr(to_lower=True)
        #
        #     @validator('tags', pre=True, each_item=True, allow_reuse=True)
        #     @classmethod
        #     def validate_tags(cls, val):  # valida tags
        #         val = val if hasattr(val, "__iter__") else list(val)  # Convierte val a lista si no lista
        #         val = [j for j in val if isinstance(j, Tag) is True]  # Valida tags
        #         # print(f'****************** VALIDATOR DE TAGS: {_}')
        #         return val
        #
        #     @classmethod
        #     @validator('kwargs', pre=False, each_item=True, allow_reuse=True)
        #     def validate_kwargs(cls, argsDict):
        #         # lower_keys = [str(j).strip().lower() for j in argsDict]
        #         if {'fldID', 'fldDOB'}.issubset(argsDict):
        #             myDOB = valiDate(next((argsDict[j] for j in argsDict if j.lower().__contains__('flddob')),None),None)
        #             if myDOB is not None:
        #                 myDOB = datetime.strptime(myDOB, fDateTime)
        #             castr = next((argsDict[j] for j in argsDict if j.lower().__contains__('flagcastrado')), 0)
        #             if castr in (0, 1):
        #                 argsDict['fldFlagCastrado'] = castr  # Si fldFlagCastrado no esta en kwargs, se crea y setea a 0
        #             else:
        #                 argsDict['fldFlagCastrado'] = valiDate(castr, 1)  # valida fecha. Si es incorrecta, setea 1
        #             mode = next((argsDict[j] for j in argsDict if j.lower().__contains__('fldmode')), 'regular')
        #
        #             if not myDOB:
        #                 raise ValueError(f'ERR_INP_InvalidArgument 1')
        #             if mode not in Animal.getAnimalModeDict():
        #                 raise ValueError(f'ERR_INP_InvalidArgument: Animal Mode {mode} not valid.')
        #
        #             else:
        #                 return argsDict
        #         else:
        #             raise ValueError(f'ERR_INP_InvalidArgument: ID_Animal and/or Fecha De Nacimiento are missing.')

        # user = ArgsCaprine(tags=args, kwargs=kwargs)

        try:
            # user = ArgsCaprine(tags=args, kwargs=kwargs)
            kwargs = self.validate_kwargs(kwargs)
            result = True
        except (TypeError, ValueError, ValidationError) as e:
            print(f'{e}')
            result = False

        if result is False:
            isValid = False  # Sale con __isValidFlag = False
            isActive = False     # No se puede crear un tag valido faltando alguno de estos argumentos.
            myID = False
            # mf = None                # Esta linea y abajo: por compatibilidad de argumentos solamente.
            self.__flagCastrado = None
            # animalClassID = None
            # animalRace = None
            # countMe = 0
            exitYN = False
            self.__comment = f'{moduleName()}({lineNum()}) - Invalid Animal Object - {callerFunction()}'
            self.__myTags = []
        else:
            myID = kwargs['fldID']

            # mf = kwargs['fldMF'].upper() if 'fldMF' in kwargs else None
            self.__flagCastrado = kwargs['fldFlagCastrado'] if 'fldFlagCastrado' in kwargs else None
            # animalClassID = kwargs['fldFK_ClaseDeAnimal'] if 'fldFK_ClaseDeAnimal' in kwargs else None
            # animalRace = kwargs['fldFK_Raza'] if 'fldFK_Raza' in kwargs else None
            # countMe = kwargs['fldCountMe'] if 'fldCountMe' in kwargs else 1
            exitYN = kwargs['fldDateExit'] if 'fldDateExit' in kwargs else None
            # fldDate = kwargs['fldDate'] if 'fldDate' in kwargs else None
            isValid = True
            isActive = True
            self.__comment = kwargs['fldComment'] if 'fldComment' in kwargs else ''
            kwargs['memdata'] = True  # Habilita datos en memoria (last_inventory data) en EntityObject
        super().__init__(*args, **kwargs)

        # Registra objeto (Bovine) en __registerDict, inicializa variable moduleRunning a 1. Si es generic
        # TODO: Para que la comparacion funcione, se debe tener el valor del ID generico ANTES de ejecutar por
        #  primera vez este constructor.
        #  @@@ Ver como arreglar esto @@@
        if not exitYN and myID not in Caprine.__registerDict:
            try:
                Caprine.__registerDict[myID] = self      # Registra objetos que no tengan salida y no esten en Dict.
            except (KeyError, ValueError, TypeError, IndexError, NameError):
                # Log Error for Animal.__ID: ERR_SYS_RegisterError
                pass

    @staticmethod
    def validate_kwargs(argsDict):
        # lower_keys = [str(j).strip().lower() for j in argsDict]
        if {'fldID', 'fldDOB'}.issubset(argsDict):
            myDOB = valiDate(next((argsDict[j] for j in argsDict if j.lower().__contains__('flddob')), None), None)
            if myDOB is not None:
                myDOB = datetime.strptime(myDOB, fDateTime)
            castr = next((argsDict[j] for j in argsDict if j.lower().__contains__('flagcastrado')), 0)
            if castr in (0, 1):
                argsDict['fldFlagCastrado'] = castr  # Si fldFlagCastrado no esta en kwargs, se crea y setea a 0
            else:
                argsDict['fldFlagCastrado'] = valiDate(castr, 1)  # valida fecha. Si es incorrecta, setea 1
            mode = next((argsDict[j] for j in argsDict if j.lower().__contains__('fldmode')), 'regular')

            if not myDOB:
                raise ValueError(f'ERR_INP_InvalidArgument 1')
            if mode not in Animal.getAnimalModeDict():
                raise ValueError(f'ERR_INP_InvalidArgument: Animal Mode {mode} not valid.')

            else:
                return argsDict
        else:
            raise ValueError(f'ERR_INP_InvalidArgument: ID_Animal and/or Fecha De Nacimiento are missing.')

    @classmethod
    def getTotalCount(cls, mode=1):          # Total Bovine Animals in __registerDict.
        """
        Counts total animals for given Animal Class.
        @param mode: 0: ALL ID_Animal in Animal.__registerDict for given class.
            `        1 (Default): Only ACTIVE Animals. Useful to count stocks.
        @return: Bovine animals count (int)
        """
        count = 0
        objList = cls.getRegisterDict().values()
        for obj in objList:
            if mode == 0:
                count += obj.countMe
            else:
                if obj.isActive:
                    count += obj.countMe
        return count


class CaprineActivity(AnimalActivity):

    __abstract_class = True  # Para no registrarse en class_register

    def __new__(cls, *args, **kwargs):
        if cls is CaprineActivity:
            krnl_logger.error(f"ERR_TypeError: {cls.__name__} cannot be instantiated, please use a subclass")
            raise TypeError(f'ERR_TypeError: class {cls} cannot be instantiated. Please, use one of its subclasses.')
        return super().__new__(cls)

    def __init__(self, activityName=None, *args, tbl_data_name=None, **kwargs):
        super().__init__(activityName, *args, tbl_data_name=tbl_data_name, **kwargs)


class CategoryActivity(CaprineActivity):
    __activityName = 'Categoria'            # self.definedActivities()[self.__activityName] = self
    __method_name = 'category'
    # Class Attributes: Tablas que son usadas por todas las instancias de InventoryActivityAnimal
    __tblRAName = 'tblAnimalesRegistroDeActividades'
    __tblObjectsName = 'tblAnimales'  # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
    __tblLinkName = 'tblLinkAnimalesActividades'
    __tblDataName = 'tblDataAnimalesCategorias'
    __tblDataInventoryName = 'tblDataAnimalesActividadInventario'
    __tblRA = DataTable(__tblRAName)  # Tabla Registro De Actividades
    __tblObject = DataTable(__tblObjectsName)  # Tabla "Objeto": tblCaravanas, tblAnimales, etc.
    __tblLink = DataTable(__tblLinkName)  # Sin argumentos: se crean TODOS los campos de la tabla; _dataList=[]
    __tblData = DataTable(__tblDataName)  # Data Categorias
    __tblDataInventory = DataTable(__tblDataInventoryName)  # Data Inventario

    __permittedDict = {'2': [3, 4, 12], '3': [4, 12],  '4': [12], '5': [6, 7, 8, 9, 11], '6': [7, 8, 9, 11],
                       '7': [8], '8': [8], '9': [8, 9, 11], '10': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                       'None': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], None: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}

    @classmethod
    def permittedFrom(cls):  # Lista de Status permitidos para Animal, a partir de a status inicial(From)
        return cls.__permittedDict

    def __init__(self, *args, **kwargs):
        if not self.__tblRA.isValid or not self.__tblLink.isValid or not self.__tblData.isValid:
            krnl_logger.warning(f'ERR_Sys_CannotCreateObject ({lineNum()}): {self.__class__.__name__}')
            return
        else:
            kwargs['supportsPA'] = False  # Inventory does support PA.
            kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, tbl_data_name=self.__tblDataName, **kwargs)


    # @Activity.dynamic_attr_wrapper
    def set(self, *args, category='', **kwargs):
        """
        @param categ_name:  string. 'Toro', 'Ternero', 'Vaquillona', etc.
        @param args: DataTable objects with obj_data to write to DB
        @param kwargs:  'enforce'=True: forces the Category val irrespective of __statusPermittedDict conditions
                         'categoryID': Category number to set, if Category number is not passed via tblData
        @return: True if success; False if fail
        """
        kwargs['category'] = category
        retValue = self._setCategory(*args, **kwargs)
        retValue = True if isinstance(retValue, int) else False
        if self._doInventory(**kwargs):
            _ = self.outerObject.inventory.set()
        return retValue






    # def set(self, *args, **kwargs):             # TODO: Verificar que category.set() funcione...
    #     """
    #     @param args: DataTable objects with obj_data to write to DB
    #     @param kwargs:  'enforce'=True: forces the Category val irrespective of __statusPermittedDict conditions
    #                      'categoryID': Category number to set, if Category number is not passed via tblData
    #     @return: True if success; False if fail
    #     """
    #     # lower_kwargs = {key.strip().lower(): kwargs[key] for key in kwargs} if len(kwargs) > 0 else {}
    #     tblData = setupArgs(self.__tblDataName, *args, **kwargs)
    #
    #     categID = next((int(kwargs[j]) for j in kwargs if str(j).lower().__contains__('categoryid')), None)
    #     categID = categID if categID in Caprine.categories.values() else None        # Valida categoryID
    #     enforce = next((int(kwargs[j]) for j in kwargs if str(j).lower().__contains__('enforce')), False)
    #
    #     if self.isValid and self.outerObject.validateActivity(self.__activityName):
    #         if 'fldFK_Categoria' in tblData.fldNames:       # Procesa Categoria de tblData
    #             categoryID = tblData.getVal(0, 'fldFK_Categoria') if tblData.getVal(0, 'fldFK_Categoria') \
    #                                         in Caprine.categories.values() else categID   # Valida categoryID
    #         else:
    #             categoryID = categID
    #         if categoryID is None:
    #             retValue = f'ERR_INP_ArgumentsNotValid: Category not valid or missing. Func/Method:category.set()'
    #             print(f'{moduleName()}({lineNum()} - {retValue}')
    #             return retValue
    #
    #         if categoryID is not None and (categoryID in self.__permittedDict[self.outerObject.lastCategoryID] or
    #                                        enforce not in (None, 0, False)):
    #             tblRA = setupArgs(self._tblRAName, *args)
    #             tblLink = setupArgs(self._geTblLinkName, *args)
    #             tblData.setVal(0, fldFK_Categoria=categoryID)
    #             lock = Lock()
    #             # with lock:               # Lock para actualizar tablas y EntityObject.lastCategoryID
    #             idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, *args, **kwargs)
    #             if isinstance(idActividadRA, int):
    #                 self.outerObject.lastCategoryID = categoryID        # Fin del bloque with. Libera lock
    #
    #             if isinstance(idActividadRA, str):  # str: Hubo error de escritura
    #                 retValue = f'ERR_DB_WriteError {idActividadRA} - {callerFunction()}'
    #                 print(f'{moduleName()}({lineNum()}) - {retValue}')
    #             else:
    #                 retValue = idActividadRA
    #         else:
    #             retValue = f'ERR_INP_InvalidArgument: Category ID: {categoryID} - {callerFunction()}'
    #             print(f'{moduleName()}({lineNum()})  - {retValue}')
    #     else:
    #         retValue = f'ERR_Sys_ObjectNotValid or ERR_INP_ActivityNotDefined - {callerFunction()}'
    #         print(f'{moduleName()}({lineNum()})  - {retValue}')
    #     return retValue

    # @Activity.dynamic_attr_wrapper
    def get(self, sDate='', eDate='', **kwargs):
        """
        Returns records in table Data Animales Categorias between sValue and eValue. sValue=eValue ='' ->Last record
        @param sDate: No args: Last Record; sDate=eDate='': Last Record;
                        sDate='0' or eDate='0': First Record
                        Otherwise: records between sDate and eDate
        @param eDate: See @param sDate.
        @param kwargs: mode=datatable->Returns DataTable with full record.  mode='fullRecord' -> Returns last Record
         in full.
        @return: Object DataTable with information from table queried table. None if activity name is not valid
        """
        fldName = 'fldFK_Categoria'
        modeArg = kwargs.get('mode')
        retValue = None
        if not modeArg or str(modeArg).lower().__contains__('mem'):
            # Todo esto para aprovechar los datos en memoria y evitar accesos a DB.
            retValue = self.outerObject.lastCategoryID  # Returns data from memory
        else:
            qryTable = self._getRecordsLinkTables(self.__tblRA, self.__tblLink, self.__tblData)
            if isinstance(qryTable, DataTable):
                if qryTable.dataLen <= 1:
                    result = qryTable
                else:
                    result = qryTable.getIntervalRecords('fldDate', sDate, eDate, 1)  # mode=1: Date field.
                if str(modeArg).lower().__contains__('val'):
                    # Retorna PRIMER o ULTIMO Registro en funcion de valores de sDate, eDate.
                    retValue = result.getVal(0, fldName)
                else:
                    # Retorna DataTable con registros.
                    retValue = result
        return retValue



    # @Activity.dynamic_attr_wrapper
    def getCurrentValue(self, mode='name'):
        """
        Returns dict1 Category for Animal as a 1-item dictionary or string, as per mode parameter
        @param mode: 'name': Category Name (str)
                     'idCategory': Category ID (int)
                     Anything else: {categoryName: categoryValue}
        @return: Category Name (str), Category Value (int) or {categoryName: categoryValue}. Error: None
        """
        retValue = {}
        temp = self.get().unpackItem(0)
        if len(temp) > 0:
            for j in self.outerObject.categories:
                if self.outerObject.categories[j] == temp['fldFK_Categoria']:
                    if str(mode).lower().strip().find('name') >= 0:
                        retValue = j
                    elif 0 <= str(mode).strip().lower().find('id') < 2:
                        retValue = temp['fldFK_Categoria']
                    else:
                        retValue[j] = temp['fldFK_Categoria']
                    break
        else:
            retValue = None
        return retValue



    def compute(self, val):
        """
        Updates Category for Animal. One specific method for each Animal Class.
        @return: idAnimal if category changed. None if category is unchanged.
        """
        # ageDays = (datetime.now() - self.outerObject.dob).days
        ageDays = (time_mt() + self.outerObject.dob) / SPD * DAYS_MULT
        if str(self.outerObject._fldMF).strip().upper().find('F') >= 0:
            if ageDays < self.outerObject.__edadLimiteTernera:
                # if Caprine.__setVaquillonaByWeight:  # Cambiar Categoria por peso:En operaciones de pesaje,aqui no.
                #     if obj.getWeight() > == Caprine.__weightLimitTernera:
                #         category = 'Vaquillona'
                # if obj.parturition.getTotal() == 1  # Si total Pariciones = 1, es Vaquillona.
                # category = 'Vaquillona'
                categoryName = 'Ternera'
            elif ageDays < self.outerObject.__edadLimiteVaquillona:
                categoryName = 'Vaquillona'
            else:
                categoryName = 'Vaca'
        else:
            if not self.outerObject.castration:
                categoryName = 'Novillito' if ageDays < self.outerObject.__edadLimiteNovillito else 'Novillo'
            else:
                if ageDays < self.outerObject.__edadLimiteTernero:
                    categoryName = 'Ternero'
                elif ageDays < self.outerObject.__edadLimiteTorito:
                    categoryName = 'Torito'
                elif ageDays > self.outerObject.__edadLimiteNovillito and self.outerObject.setNovilloByAge:
                    categoryName = 'Novillo'
                else:
                    categoryName = 'Toro'

        if self.outerObject.categories[categoryName] != self.outerObject.lastCategoryID:
            eventDate = time_mt('date_time')
            # eventDate = eventDate.replace(hour=22,minute=22,second=22,microsecond=222222) #System-created Category
            tblCategory = DataTable(self.__tblDataName)
            tblCategory.setVal(0, fldFK_Categoria=Caprine.categories[categoryName], fldDate=eventDate,
                               fldModifiedBySystem=1)
            self.set(tblCategory, enforce=0)
            retValue = self.outerObject.getID
        else:
            retValue = None
        return retValue

    @classmethod
    def updateCategories_bkgd(cls):
        """
        Background method to update category for Caprines. Called by krnl_threading module, executed in a Timer thread.
        Daemon Function.
        @return:
        """
        for j in cls.__registerDict.values():
            j.category.compute(verbose=True)

    @classmethod
    def updateTimeout_bkgd(cls):
        """
        Background method to update Timeout for Caprines. Called by krnl_threading module, executed in a Timer thread.
        Daemon Function.
        @return:
        """
        for k in cls.__registerDict.values():
            if not k.isDummy:        # Dummies no entran en Timeout.
                k.updateTimeout()    # Calls method updateTimeout() in entityObject. Same method for all EO subclasses

# TODO: Este metodo se debe ejecutar al lanzar el modulo de Caprinos. Ver como hacer esto!

# if Caprine.getClassesEnabled()[Caprine.getKindOfAnimal()]:
#     Caprine.registerKindOfAnimal()       # Registra tipo de Animal en diccionario en clase Animal.


# TODO: Leer ID de Animales/Objetos Genericos de DB (tablas Animales, Personas, Dispositivos, etc) tomar el ID Y crear
#  el objeto genericCaprine (y todos los demas) e inicializar correctamente.
# genericCaprine = Caprine(fldID=400, fldCountMe=0, fldDOB='1900-01-01', fldMF='M', fldFK_ClaseDeAnimal=1, fldMode='generic',
#                        fldName='Generic Caprine', fldComment='GENERIC Caprine', fldDate=getNow(fDateTime))
#
# genericCaprine.daysToTimeout = 10000000
# EntityObject.GENERIC_OBJECT_ID['Caprine'] = genericCaprine.getID     # Agrega el generico al dict de Objetos Genericos.


# Lista de todos los objetos Caprine, obtenidos de __registerDict en cada llamada a Caprines
# def caprine():
#     return Caprine.getRegisterDict().values()


