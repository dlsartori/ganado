import time
from krnl_abstract_class_animal import *
from krnl_bovine_activity import CategoryActivity, BovineActivity
# from krnl_tag import Tag
# from datetime import datetime
from threading import Event

                                                # Vacunos #
def moduleName():
    return str(os.path.basename(__file__))

class Config:
    arbitrary_types_allowed = True
# @pydantic.dataclasses.dataclass(config=Config)


class Bovine(Mammal):
    # __objClass = 5
    # __objType = 1
    __genericAnimalID = None
    __moduleRunning = 1   # Flag que indica si el modulo de la clase de Animal se esta ejecutando. TODO: Ver como setear
    _animalClassName = 'Vacuno'
    _kindOfAnimalID = 1             # TODO(cmt): 1:'Vacuno', 2:'Caprino', 3:'Ovino', 4:'Porcino', 5:'Equino'.

    __registerDict = {}  # {idAnimal: objAnimal}-> Registro General de Animales: Almacena Animales de la clase

    # Defining _activityObjList will call  _creatorActivityObjects() in EntityObject.__init_subclass__().
    _activityObjList = []  # List of Activity objects created by factory function.
    _myActivityClass = BovineActivity   # Will look for the property-creating classes starting in this class.

    __timeoutEvent = Event()  # Events for class Bovine to communicate with bkgd functions.
    __categoryEvent = Event()
    __allAssignedTags = {}  # dict {tagID: tagObject, }. All tags assigned to animals of a given class.

    # @classmethod
    # def getTagsList(cls):
    #     return cls.__allAssignedTags

    @classmethod
    def getAllAssignedTags(cls):  # TODO: must be implemented in all subclasses (Bovine, Caprine, etc.)
        return cls.__allAssignedTags

    @classmethod
    def getRegisterDict(cls):
        return cls.__registerDict  # {UIDAnimal: objAnimal}->Registro de Bovinos: Almacena Animales de la clase

    @classmethod
    def registerKindOfAnimal(cls):
        """ Registers class Bovine in __registeredClasses dict. Method is run by EntityObject.__init_subclass__()"""
        Animal.getAnimalClasses()[cls] = cls._kindOfAnimalID

    __bkgdAnimalTimeouts = []   # stores objects that had timeout changes made by background threads.
    __bkgdCategoryChanges = {}  # stores objects that had category changes made by bckgd threads: {obj: newCategory,}

    @classmethod
    def getBkgdAnimalTimeouts(cls):
        return cls.__bkgdAnimalTimeouts

    @classmethod
    def getBkgdCategoryChanges(cls):
        return cls.__bkgdCategoryChanges

    @classmethod
    def timeoutEvent(cls):
        return cls.__timeoutEvent

    @classmethod
    def categoryEvent(cls):
        return cls.__categoryEvent

    temp = getRecords('tblAnimalesActividadesNombres', '', '', None, 'fldID', 'fldName', 'fldFlag',
                         'fldFK_ClaseDeAnimal')
    __bovineActivitiesDict = {}  # __caprineActivitiesDict = {fldNombreActividad: fldID_Actividad,}
    __isInventoryActivity = {}          # _isInventoryActivity = {fldNombreActividad: _isInventoryActivity (1/0),}
    for j in range(temp.dataLen):
        if temp.dataList[j][3] == 0 or temp.dataList[j][3] == _kindOfAnimalID:
            # 0: Define en DB una Actividad que aplica a TODAS las clases de Animales
            __bovineActivitiesDict[temp.dataList[j][1]] = temp.dataList[j][0]
            __isInventoryActivity[temp.dataList[j][1]] = temp.dataList[j][2]

    @property
    def activities(self):           # @property activities es necesario para las llamadas a obj.outerObject.
        return self.__bovineActivitiesDict

    def register(self):
        """
        Inserts Bovine Object and ID into Bovine.__registerDict
        @param external: Signals whether the object being registered is locally created or replicated from another db.
        @return: Inserted object. None if one tries to register a Generic or External animal.
        """
        if self.mode.lower() in ('regular', 'substitute', 'dummy'):  # TODO(cmt): POR AHORA, generic y external no van
            self.__registerDict[self.ID] = self
            # if external and self.mode.lower() not in 'dummy':
            #     self._new_local_fldIDs.append(self.recordID)     # Used to drive the duplicate object detection logic.
            return self
        return None

    def unRegister(self):  # Remueve obj .__registerDict.
        """
        Removes object from .__registerDict
        @return: removed object ID if successful. None if object not found in dict.
        """
        try:
            self._fldID_list.remove(self.recordID)      # removes fldID from _fldID_list to keep integrity of structure.
        except ValueError:
            pass
        return self.__registerDict.pop(self.ID, None)  # Retorna False si no encuentra el objeto

    @classmethod
    def getActivitiesDict(cls):
        return cls.__bovineActivitiesDict

    @classmethod
    def getInventoryActivitiesDict(cls):
        return cls.__isInventoryActivity

    temp1 = getRecords('tblAnimalesCategorias', '', '', None, '*', fldFK_ClaseDeAnimal=_kindOfAnimalID)
    __categories = {}  # {Category Name: ID_Categoria, }
    if temp1.dataLen:
        for j in range(temp1.dataLen):
            __categories[temp1.getVal(j, 'fldName')] = temp1.getVal(j, 'fldID')

    @property
    def categories(self):
        return self.__categories             # {Category Name: ID_Categoria, }

    @classmethod
    def getCategories(cls):
        return cls.__categories              # {Category Name: ID_Categoria, }

    __edadLimiteTernera = fetchAnimalParameterValue('Edad Limite Ternera')
    __edadLimiteVaquillona = fetchAnimalParameterValue('Edad Limite Vaquillona')
    __edadLimiteVaca = fetchAnimalParameterValue('Edad Limite Vaca')
    __edadLimiteTernero = fetchAnimalParameterValue('Edad Limite Ternero')
    __edadLimiteTorito = fetchAnimalParameterValue('Edad Limite Torito')
    __edadLimiteNovillito = fetchAnimalParameterValue('Edad Limite Novillito')
    __edadLimiteNovillo = fetchAnimalParameterValue('Edad Limite Novillo')
    __edadLimiteToro = fetchAnimalParameterValue('Edad Limite Toro')
    __setNovilloByAge = 0  # TODO: leer este parametro de DB.
    __AGE_LIMIT_BOVINE = 20 * 365  # TODO: leer este parametro de DB.

    __ageLimits = {'Ternera': __edadLimiteTernera, 'Vaquillona': __edadLimiteVaquillona, 'Vaca': __edadLimiteVaca,
                   'Ternero': __edadLimiteTernero, 'Torito': __edadLimiteTorito, 'Novillito': __edadLimiteNovillito,
                   'Novillo': __edadLimiteNovillo, 'Toro': __edadLimiteToro, 'Bovine': __AGE_LIMIT_BOVINE,
                   'Vacuno': __AGE_LIMIT_BOVINE}

    @classmethod
    def getAgeLimits(cls):
        return cls.__ageLimits

    @classmethod
    def ageLimit(cls, val):
        return cls.__ageLimits.get(val)     # Returns none if is not a valid key

    @classmethod
    def novilloByAge(cls, val=None):
        """ getter/setter for this attribute"""
        if val is not None:
            cls.__setNovilloByAge = bool(val)
        return cls.__setNovilloByAge


    def __init__(self, *args, **kwargs):

        # class ArgsBovine(BaseModel):        # pydantic Class for argument parsing and validation
        #     kwargs: dict
        #     # strip_str: constr(strip_whitespace=True)
        #     # lower_str: constr(to_lower=True)
        #
        #     @classmethod
        #     # @field_validator('kwargs', '', check_fields=False)
        #     @validator('kwargs', pre=False, each_item=True, allow_reuse=True, check_fields=False)
        #     def validate_kwargs(cls, argsDict):
        #         myDOB = None
        #         myfldID = None
        #         # lower_keys = [str(j).strip().lower() for j in argsDict]
        #         if {'fldID', 'fldDOB'}.issubset(argsDict):
        #             # myfldID = next((argsDict[j] for j in argsDict if 'fldid' in j.lower()), None)
        #             myDOB = kwargs.get('fldDOB', '')
        #             if myDOB:
        #                 if not isinstance(myDOB, datetime):
        #                     try:
        #                         myDOB = datetime.strptime(myDOB, fDateTime)
        #                     except(TypeError, ValueError):
        #                         print(f'ERR_INP_Invalid argument DOB: {myDOB}')
        #                         raise ValueError(f'ERR_INP_InvalidArgument: Date of Birth {myDOB} - '
        #                                          f'{moduleName()}({lineNum()})')
        #             castr = next((argsDict[j] for j in argsDict if 'flagcastrado' in j.lower()), 0)
        #             if castr in (0, 1):
        #                 argsDict['fldFlagCastrado'] = castr  # Si fldFlagCastrado no esta en kwargs lo crea y setea a 0
        #             else:
        #                 argsDict['fldFlagCastrado'] = valiDate(castr, 1)  # valida fecha. Si es incorrecta, setea 1
        #             mode = str(next((kwargs[j] for j in kwargs if 'mode' in str(j).lower()), None)).lower()
        #             if mode not in Animal.getAnimalModeDict():
        #                 raise ValueError(f'ERR_UI_InvalidArgument: Animal Mode {mode} not valid. - '
        #                                  f'{moduleName()}({lineNum()})')
        #             else:
        #                 return argsDict
        #         else:
        #             raise ValueError(f'ERR_UI_InvalidArgument: ID_Animal and/or Animal DOB are missing. '
        #                              f'kwargs:{kwargs} / argsDict: {argsDict} / ID: {myfldID} / dob: {myDOB}')

        try:
            # user = ArgsBovine(kwargs=kwargs)
            kwargs = self.validate_arguments(kwargs)
        except (TypeError, ValueError) as e:
            krnl_logger.info(f'{e} - Object not created!.')
            del self  # Removes invalid/incomplete Bovine object
            return

        mode = next((str(kwargs[j]).lower().strip() for j in kwargs if 'mode' in str(j).lower()), None)
        if 'generic' in mode.lower() and self.__genericAnimalID is not None:
            retValue = 'ERR_Inp_InvalidObject: Generic Animals can only have one instance. Object not created'
            krnl_logger.error(retValue)
            raise ValueError(retValue)          # Solo 1 objeto Generic se puede crear.

        self.__flagCastrado = kwargs.get('fldFlagCastrado')
        self.__comment = kwargs.get('fldComment', '')
        kwargs['memdata'] = True  # Habilita datos en memoria (_getCategory, getStatus, _getInventory) en EntityObject

        super().__init__(*args, **kwargs)


    # __categoryObject = CategoryActivity()
    # @property
    # def category(self):
    #     self.__categoryObject.outerObject = self  # Pasa objeto Bovine para ser usado por set, get, etc.
    #     return self.__categoryObject     # Retorna Activity object pare acceder metodos en CategoryActivity

    @staticmethod
    def validate_arguments(argsDict):
        # Animal Category is not required here. It will be derived from M/F, Castration status and age for each object.
        # DOB required for Bovines. Other Animals may not require dob.
        dob = next((argsDict[k] for k in argsDict if 'flddob' in str(k).lower()), '')
        if not isinstance(dob, datetime):
            try:
                dob = datetime.strptime(dob, fDateTime)
            except(TypeError, ValueError):
                err_str = f'ERR_INP_Invalid or missing argument DOB: {dob}'
                print(err_str, dismiss_print=DISMISS_PRINT)
                raise ValueError(f'{err_str} - {moduleName()}({lineNum()})')

        key, castr = next(((k, argsDict[k]) for k in argsDict if 'castrad' in k.lower()), (None, 0))
        if castr in (0, 1, True, False, None):          # 1: Castrated, but castration date not known.
            argsDict[key] = bool(castr) * 1  # Si fldFlagCastrado no esta en kwargs, se crea y setea a 0
        else:
            argsDict[key] = valiDate(castr, 0)  # verifica si es fecha. Si fecha no es valida, asume NO Castrado (0).
        key, mode = next(((k.lower(), str(argsDict[k]).lower()) for k in argsDict if 'mode' in str(k).lower()),
                         (None, 'regular'))         # if mode not passed defaults to 'regular' Animal.
        if mode not in Animal.getAnimalModeDict():
            raise ValueError(f'ERR_UI_InvalidArgument: Animal Mode {mode} not valid. - '
                             f'{moduleName()}({lineNum()})')
        else:
            argsDict[key] = mode
            return argsDict


    @classmethod
    def defineCategory(cls, dob, mf: str, **kwargs):
        """
        Produces a Category for an Animal when DOB, MF are passed. IMPORTANT: One specific method for each Animal Class.
        NO DATATABLES PASSED or updated. It just updates the Animal category and returns the new value to the caller.
        kwargs: 'enforce'=True enforces category regardless of dob.
        @return: idAnimal if category changed. None if category is unchanged.
        """
        if isinstance(dob, (int, float)):
            pass
        elif isinstance(dob, str):
            dob = time.strptime(dob, '%Y-%m-%d %H:%M:%S.%f')
        elif isinstance(dob, datetime):
            dob = dob.timestamp()
        else:
            retValue = f'INFO_Inp_InvalidArguments: {dob}'
            krnl_logger.info(retValue, stack_info=True)
            return retValue

        ageDays = (time_mt() - dob) / SPD
        if ageDays > cls.ageLimit(Bovine.animalClass) and not kwargs.get('enforce'):
            retValue = f'INFO_Inp_InvalidArgument: Animal age ({ageDays/365} yrs) out of limits '
            krnl_logger.info(retValue, stack_info=True)     # enforce=True -> Ignora este control de edad.
            return retValue

        if mf.upper().__contains__('F'):
            if ageDays < cls.ageLimit('Ternera'):
                # if Bovine.__setVaquillonaByWeight:  # TODO: Cambiar Categoria por peso:En "MEDICIONES",aqui no.
                #     if obj.getWeight() > == cls.__weightLimitTernera:
                #         category = 'Vaquillona'
                # if obj.parturition.getTotal() == 1  # Si total Pariciones = 1, es Vaquillona.
                # category = 'Vaquillona'
                categoryName = 'Ternera'
            elif ageDays < cls.ageLimit('Vaquillona'):
                categoryName = 'Vaquillona'
            else:
                categoryName = 'Vaca'
        else:
            if not kwargs.get('castration'):
                categoryName = 'Novillito' if ageDays < Bovine.ageLimit('Novillito') else 'Novillo'
            else:
                if ageDays < cls.ageLimit('Ternero'):
                    categoryName = 'Ternero'
                elif ageDays < cls.ageLimit('Torito'):
                    categoryName = 'Torito'
                elif ageDays > cls.ageLimit('Novillito'):  # and self.outerObject.setNovilloByAge:
                    categoryName = 'Novillo'
                else:
                    categoryName = 'Toro'

        retValue = categoryName
        return retValue

# ---------------------------------------------------- End Bovine class --------------------------------------------- #

# if Bovine.getClassesEnabled()[Bovine.getAnimalClassID()]:
#     Bovine.registerKindOfAnimal()       # Registra tipo de Animal en diccionario en clase Animal.

# Lista de todos los objetos Bovine, obtenidos de __registerDict en cada llamada a bovines
def bovine():
    return list(Bovine.getRegisterDict().values())


