import datetime
import functools
import sqlite3
from datetime import timedelta
from uuid import UUID, uuid4
import re
from krnl_asset import Asset
from krnl_entityObject import *
from krnl_abstract_class_activity import Activity, activityEnableFull, sessionActiveUser, valiDate
# from krnl_abstract_method_factory import ActivityMethod
from krnl_abstract_class_prog_activity import ProgActivity
from krnl_config import krnl_logger, bkgd_logger, SPD, MAIN_DB_ID, exec_sql, MAX_GOBACK_DAYS
from krnl_animal_activity import AnimalActivity, InventoryActivityAnimal, StatusActivityAnimal, TagActivityAnimal, \
    LocalizationActivityAnimal, TMActivityAnimal, PersonActivityAnimal, ProgActivityAnimal, GenericActivityAnimal
from krnl_animal_activity_alta_baja import AltaActivityAnimal
# from krnl_health import Application
from krnl_tag import Tag
# from krnl_tag_bovine import TagBovine
# from krnl_tag_caprine import TagCaprine
# from krnl_person import Person
from custom_types import getRecords

def moduleName():
    return str(os.path.basename(__file__))

# Abstract Class: Cannot instantiate Animal. Only subclasses can.
class Animal(Asset):
    __slots__ = ('_fldDOB', '_fldMF', '_fldFlagCastrado', '_fldAnimalClass', '_fldFK_Raza', '__countMe', '__exitDate',
                 '_name', '_animalClassName', '_mode', '_conceptionType', '__surrogateMother', '_timeStamp',
                 '__myTags', '__comment', 'ageDays', '__identifiers')
    # __objClass = 3
    # __objType = 1
    __classesEnabled = {1: True, 2: False, 3: False, 4: False, 5: False}  # Class of Animals to register
    __MIN_REWIND_DAYS = 365        # Tiempo en Dias. Usado en generateDOB().


    # TODO(cmt): Defining _activityObjList will call  _creatorActivityObjects() in EntityObject.__init_subclass__().
    _activityObjList = []  # List of Activity objects created by factory function.
    _myActivityClass = AnimalActivity
    __tblObjectsName = 'tblAnimales'

    # __activityClasses = {}  # {animalKind(string): class, } -> Cada class de Activity() inserta aqui sus datos
    # {Animal mode: countMe Value, }
    __animalMode = {'regular': 1, 'substitute': 0, 'dummy': -1, 'external': 0, 'generic': 0}
    __conceptionTypes = ('natural', 'ia', 'inseminacion', 'inseminacion artificial', 'te', 'transferencia embrionaria')

    # Variables para logica de manejo de objetos repetidos/duplicados.
    _fldID_list = []  # List of all active records pulled by getRecords() from tblAnimales.
    _fldID_exit_list = []   # List of all all records with fldExitDate > 0, updated in each call to processReplicated().
    _object_fldUPDATE_dict = {}  # {fldID: fldUPDATE_lastUID, }        Keeps a dict of uuid values of fldUPDATE fields
    _RA_fldUPDATE_dict = {}  # {fldID: fldUPDATE_lastUID, }      # TODO: ver si este hace falta.
    _RAP_fldUPDATE_dict = {}  # {fldID: fldUPDATE_lastUID, }      # TODO: ver si este hace falta.
    temp0 = getRecords(__tblObjectsName, '', '', None, 'fldID',  'fldUPDATE', fldDateExit=0)
    if isinstance(temp0, DataTable) and temp0.dataLen:
        tempList = temp0.getCols('fldID', 'fldUPDATE')       # Initializes _object_fldUPDATE_dict with UPDATEd records.
        _fldID_list = tempList[0]                       # Initializes _fldID_list with all fldIDs from active Animals.
        _object_fldUPDATE_dict = dict(zip(tempList[0], tempList[1]))
        _object_fldUPDATE_dict = {k: v for (k, v) in _object_fldUPDATE_dict.items() if v is not None}
        temp1 = getRecords(__tblObjectsName, '', '', None, 'fldID')  # Full list of fldIDs, with and without ExitDate.
        if isinstance(temp1, DataTable) and temp1.dataLen:
            temp1_set = set(temp1.getCol('fldID'))
            _fldID_exit_list = list(temp1_set.difference(_fldID_list))


    @staticmethod
    def getClassesEnabled():
        return Animal.__classesEnabled

    @staticmethod
    def getAnimalModeDict():
        return Animal.__animalMode


    # GENERAL Set with all progActivities currently assigned to at least 1 animal in registerDict. For ease of search.
    # TODO(cmt): MUST be a list, as repeated objects must be managed. This is a collection of ALL the items
    #  in __myProgActivities for ALL objects defined. Don't have a use for it yet...
    __activeProgActivities = list()

    # All tags assigned to this class (Animal in this case

    @classmethod
    def getAllAssignedTags(cls):            # TODO: must be implemented in all subclasses (Bovine, Caprine, etc.)
        raise TypeError(f'ERR_Invalid Type {cls} for getAllAssignedTags.')
        return {}

    @classmethod
    def activeProgActivities(cls):
        return cls.__activeProgActivities

    # {Animal Kind: ID_Clase De Animal, }, ej: {'Vacuno': 1, 'Caprino': 2, }
    __animalKinds = {}
    temp1 = getRecords('tblAnimalesClases', '', '', None, 'fldID', 'fldAnimalClass', 'fldComment')
    for j in range(temp1.dataLen):
        __animalKinds[temp1.dataList[j][1]] = temp1.dataList[j][0]  # {Clase De Animal (nombre): animalClassID}

    @classmethod
    def getRegisterDict(cls):           # TODO: must be implemented in all Animal subclasses (Bovine, Caprine, etc.)
        return {}

    @staticmethod
    def getAnimalKinds():
        return Animal.__animalKinds         # {'Vacuno': 1, 'Caprino':2, 'Ovino':3, etc } -> leido de DB.

    # Dictionary with kinds of animals actually in use (only animal kinds for modules loaded: Bovine, Caprine, etc).
    # Populated during initialization of Animal Classes (Bovine, Caprine, etc),.
    # TODO(cmt): When a module shuts down the cleanup code MUST remove its Class (cls) from __registeredClasses.
    #  Entries and removals done from child classes (Bovine, Caprine, etc)
    __registeredClasses = {}    # {cls: kindOfAnimalID} -> {Bovine: 1, Caprine: 2, Ovine: 3, } Bovine is a class.

    @classmethod
    def getAnimalClasses(cls):
        return cls.__registeredClasses  # {cls: kindOfAnimalID}

    @classmethod
    def removeAnimalClass(cls):
        """ Removes entry from __registeredClasses.
        @return: cls or None if cls does not exist in dictionary """
        cls.__registeredClasses.pop(cls, None)

    @classmethod
    def getAnimalClassName(cls):        # animalKind: 'Vacuno', 'Ovino', 'Caprino', etc.
        """ Returns cls.__name__ corresponding to animalKind.
        @return: Animal Class Name ('Bovine', 'Caprine', etc). None if animalKind is not valid
        """
        return cls.__name__   # if cls.__name__ in Animal.getAnimalClasses() else None

    @classmethod
    def getAnimalClassID(cls):
        return cls._kindOfAnimalID              # Gets the right _kindOfAnimalID from subclasses.

    @classmethod
    def classID(cls):
        """
        @return:  Returns animalClassID (int)
        """
        return cls._kindOfAnimalID              # Gets the right _kindOfAnimalID from subclasses.

    @classmethod
    def className(cls):
        """
        @return:  Returns animalKind (str): 'Vacuno', 'Caprino', etc.
        """
        return cls._animalClassName             # Gets the right _animalClassName from subclasses.

    temp = getRecords('tblAnimalesActividadesNombres', '', '', None, 'fldID', 'fldName', 'fldFlag',
                         'fldFK_ClaseDeAnimal')
    __activitiesForMyClass = {}
    for j in range(temp.dataLen):
        __activitiesForMyClass[temp.dataList[j][1]] = temp.dataList[j][3]  # {Nombre Actividad: AnimalClass, }

    __tblDataInventoryName = 'tblDataAnimalesActividadInventario'
    __tblDataStatusName = 'tblDataAnimalesActividadStatus'
    __tblClassName = 'tblAnimalesClases'
    __tblDataCategoriesName = 'tblDataAnimalesCategorias'
    __tblObjName = 'tblAnimales'
    __tblRAName = 'tblAnimalesRegistroDeActividades'
    __tblLinkName = 'tblLinkAnimalesActividades'
    __tblDataLocalizationName = 'tblDataAnimalesActividadLocalizacion'
    __paClass = ProgActivityAnimal
    __tblLinkPAName = 'tblLinkAnimalesActividadesProgramadas'       # Usado en object_instantiation.loadItemsFromDB()
    __tblRAPName = 'tblAnimalesRegistroDeActividadesProgramadas'

    @classmethod
    def getPAClass(cls):
        return cls.__paClass

    # Tablas para leer de DB ultimo Inventario, Status, Localizacion y Categoria del Animal. Datos van a EntityObject
    tblDataInventory = getRecords(__tblDataInventoryName, '', '', None, '*')
    inventoryRAActivityCol = tblDataInventory.getCol('fldFK_Actividad') if type(tblDataInventory) is not str \
                                                                    and len(tblDataInventory.dataList[0]) > 0 else []
    # print(f'tblRAInventory: {inventoryRAActivityCol}')
    tblDataStatus = getRecords(__tblDataStatusName, '', '', None, '*')
    statusRAActivityCol = tblDataStatus.getCol('fldFK_Actividad') if type(tblDataStatus) is not str \
                                                           and len(tblDataStatus.dataList[0]) > 0 else []
    tblDataLocaliz = getRecords(__tblDataLocalizationName, '', '', None, '*')
    localizationRAActivityCol = tblDataLocaliz.getCol('fldFK_Actividad') if isinstance(tblDataLocaliz, DataTable) \
                                                                         and len(tblDataLocaliz.dataList[0]) > 0 else []
    tblDataCategory = getRecords(__tblDataCategoriesName, '', '', None, '*')
    # print(f'Categoria DataList: {tblDataCategory.dataList}')
    categoryRAActivityCol = tblDataCategory.getCol('fldFK_Actividad') if type(tblDataCategory) is not str \
                                                                      and len(tblDataCategory.dataList[0]) > 0 else []

    #  "42990b601cec4ddb9d85bfb94cda2e29" : El Nandu - Lote 1
    @classmethod
    def tblObjName(cls):
        return cls.__tblObjName

    @classmethod
    def tblRAName(cls):
        return cls.__tblRAName

    @classmethod
    def tblLinkName(cls):
        return cls.__tblLinkName

    @classmethod
    def tblRAPName(cls):
        return cls.__tblRAPName

    @classmethod
    def tblLinkPAName(cls):
        return cls.__tblLinkPAName


    def __new__(cls, *args, **kwargs):          # __new__() override prevents instantiation of Animal class.
        if cls is Animal:
            krnl_logger.error(f"ERR_SYS_Invalid type: {cls.__name__} cannot be instantiated, please use a subclass")
            raise TypeError(f"{cls.__name__} cannot be instantiated, please use a subclass")
        return super().__new__(cls)

    def __init__(self, *args, **kwargs):
        self.__myTags = set()
        dob = kwargs.get('fldDOB', None)
        myID = kwargs.get('fldObjectUID', None)
        self.__recordID = kwargs.get('fldID')
        try:
            _ = UUID(myID)
        except(TypeError, ValueError):
            raise ValueError(f'ERR_TypeError: invalid/malformed UUID {myID}. Animal object cannot be created.')
        if kwargs.get('fldMF', '').lower() not in ('m', 'f') or not kwargs.get('fldFK_ClaseDeAnimal') or not dob:
            raise TypeError('ERR_TypeError: Cannot create Animal object.')


        self._fldDOB = dob  # dob es datetime, por consistencia.
        # self._ageDaysDeviation = kwargs.get('fldAgeDaysDeviation', 0)       # What is this needed for??
        self._fldMF = kwargs['fldMF'].lower()
        self._timeStamp = kwargs.get('fldTimeStamp', None)    # Esta fecha se usa para gestionar objetos repetidos.
        self._fldFlagCastrado = int(kwargs.get('fldFlagCastrado')) if kwargs.get('fldFlagCastrado') else 0
        animalClassID = next((kwargs[j] for j in kwargs if 'animalclass' in j.lower() or 'clasedeanim' in j.lower())
                             , None)
        self._kindOfAnimalID = animalClassID if animalClassID in list(self.__animalKinds.values()) else None
        self._animalClassName = next(j for j in self.__animalKinds if self.__animalKinds[j] == self._kindOfAnimalID)
        self._fldFK_Raza = kwargs.get('fldFK_Raza')
        # fldDate (Fecha Evento) de tblAnimales. Se usa en manejo de Dummy Animals en altaMultiple(), perform()

        isValid = True
        isActive = True
        self._mode = kwargs.get('fldMode', 'regular')
        self._mode = self._mode if self._mode in self.__animalMode else 'regular'
        # countMe: Flag 1 / 0 para indicar si objeto se debe contar o no.
        #  1: Regular Animal;
        #  0: Substitute (Animal creado por Reemision de Caravana); External Animal; Generic Animal
        # -1: Dummy (Creado por perform de un Animal Substitute)
        self.__countMe = Animal.__animalMode[self._mode]

        # list of progActivities active for object. Loaded from DB and then updated by the system.
        # TODO(cmt): Each node initializes and loads ONLY ProgActivities created by this node in __myProgActivities.
        #  This is to implement a consistent ProgActivity closure across all nodes when all db records are replicated.
        self.__myProgActivities = {}  # Dict {paObj: __activityID}

        # TODO: inputs de name desde la UI se deben procesar con removeAccents() para comparar con obj._name
        self._name = removeAccents(kwargs.get('fldName', ''))
        self._conceptionType = kwargs.get('fldConceptionType', 'natural') \
            if kwargs.get('fldConceptionType', 'natural').lower() in self.__conceptionTypes else 'natural'
        self.__surrogateMother = kwargs.get('fldFK_MadreSubrogante', None)
        self.__comment = kwargs.get('fldComment', '')
        self.__exitDate = kwargs.get('fldDateExit') or 0           # DBKeyName: fldDateExit
        if self.__exitDate:                                    # TODO(cmt): exitDate puede ser: 0, 1, datetime Object.
            isActive = False
        # else:
        #     self.__exitDate = 0

        self.__identifiers = kwargs.get('fldIdentifiers') or set()
        if self.__identifiers:
            self.__identifiers = set(self.__identifiers)
            # print(f'self.__identifiers: {self.__identifiers}', dismiss_print=DISMISS_PRINT)

        # TODO: self.__tagRegisterDict Tag RegisterDict del tipo de tag usado por este animal.PASAR ESTO a funcion()
        if 'new_object' not in kwargs:      # to skip all this if the Animal is not being created from DB.
            # Setea lastInventory, lastStatus, lastLocalization, lastCategory y pasa a EntityObject.__init__()
            animalActivitiesLinkTbl = getRecords(self.tblLinkName(), '', '', None, '*', fldFK=myID)
            if isinstance(animalActivitiesLinkTbl, DataTable) and animalActivitiesLinkTbl.dataLen and \
                    len(animalActivitiesLinkTbl.dataList[0]) > 0:
                animalActivitiesCol = animalActivitiesLinkTbl.getCol('fldFK_Actividad')
                inventoryCol = [j for j in animalActivitiesCol if j in self.inventoryRAActivityCol]
                inventoryData = getRecords(self.__tblDataInventoryName, '', '', None, '*',
                                           fldFK_Actividad=inventoryCol)  #
                lastInventoryData = inventoryData.getIntervalRecords('fldDate', '', '', 1)  # Saca Record reciente
                # print(f'{moduleName()}({lineNum()}) ^^^^^^^^^^ lastInventoryDAta: {lastInventoryData}')
                lastInv = lastInventoryData.getVal(0, 'fldDate')
                lastInv = [lastInv, lastInv]
                # print(f'{moduleName()}({lineNum()}) - Last Inventory: {lastInventoryData.getVal(0, "fldDate")}')

                statusCol = [j for j in animalActivitiesCol if j in self.statusRAActivityCol]
                statusData = getRecords(self.__tblDataStatusName, '', '', None, '*',
                                        fldFK_Actividad=statusCol)  # DataTable con ultimo status
                lastStatusData = statusData.getIntervalRecords('fldDate', '', '', 1)    # Saca Record mas reciente
                lastStatus = [lastStatusData.getVal(0, 'fldFK_Status'), lastStatusData.getVal(0, 'fldDate')]

                localizationCol = [j for j in animalActivitiesCol if j in self.localizationRAActivityCol]
                # DataTable with last Localization for Animal. sqlite3 converter converts to int to Geo obj.
                localizData = getRecords(self.__tblDataLocalizationName, '', '', None, '*',
                                         fldFK_Actividad=localizationCol)
                lastLocalizData = localizData.getIntervalRecords('fldDate', '', '', 1)  # Saca Record mas reciente
                lastLocaliz = [lastLocalizData.getVal(0, 'fldFK_Localizacion'), lastLocalizData.getVal(0, 'fldDate')]
                # print(f'LLLLLLLLLLLLLLLLLLocalization: {lastLocaliz[0].name}')

                animalCategoryCol = [j for j in animalActivitiesCol if j in self.categoryRAActivityCol]
                categoryData = getRecords(self.__tblDataCategoriesName, '', '', None, '*',
                                          fldFK_Actividad=animalCategoryCol)  # DataTable con ultima Categ
                lastCategData = categoryData.getIntervalRecords('fldDate', '', '', 1)    # Saca Record mas reciente
                lastCateg = [lastCategData.getVal(0, 'fldFK_Categoria'), lastCategData.getVal(0, 'fldDate')]
            else:
                lastInv = lastStatus = lastLocaliz = lastCateg = []
                print(f'{moduleName()}({lineNum()}) - ***  OJO:   NO SE SETEARON VARIABLES DE MEMORIA!!!')
                krnl_logger.info(f'***  OJO: NO HAY DATOS de lastInv, lastStatus, lastLocaliz, lastCateg  EN DB.'
                                 f'Seteando lastInventory={lastInv}; lastStatus={lastStatus}; '
                                 f'lastLocalization={lastLocaliz}; lastCategoryID={lastCateg}')
        else:
            lastInv = lastStatus = lastLocaliz = lastCateg = []

        kwargs['lastInventory'] = lastInv
        kwargs['lastStatus'] = lastStatus
        kwargs['lastLocalization'] = lastLocaliz        # Aqui llega on objecto Geo o None
        kwargs['lastCategory'] = lastCateg
        super().__init__(myID, isValid, isActive, *args, **kwargs)   # Llama a Asset.__init__()

    # ---------------------------------------------------Fin __init__() ------------------------------------------ #


    def __repr__(self):                 # self.getCategories() gets the right __categories dictionary.
        return "[{}; Tags:{}; Age:{:.1f}]".format(self.categoryName(),
                                str([t.tagNumber for t in self.myTags]).replace("[", "").replace("]", "") or None,
                                self.age.get())         # age value defined at runtime.

    def updateAttributes(self, **kwargs):
        """ Updates object attributes with values passed in attr_dict. Values not passed leave that attribute unchanged.
        @return: None
        """
        if not kwargs:
            return None

        self._fldDOB = kwargs.get('fldDOB') or self._fldDOB
        self._fldMF = kwargs.get('fldMF').lower() or self._fldMF
        # self._fldTimeStamp = kwargs.get('fldTimeStamp')           # Este campo no se debe actualizar
        self._fldFlagCastrado = int(kwargs.get('fldFlagCastrado')) if kwargs.get('fldFlagCastrado') is not None else \
            self._fldFlagCastrado
        # animalClassID = next((kwargs[j] for j in kwargs if 'animalclass' in j.lower() or 'clasedeanim' in j.lower())
        #                      , None)
        # self._kindOfAnimalID = animalClassID if animalClassID in list(self.__animalKinds.values()) else None
        # self._animalClassName = next(j for j in self.__animalKinds if self.__animalKinds[j] == self._kindOfAnimalID)
        self._fldFK_Raza = kwargs.get('fldFK_Raza') or self._fldFK_Raza
        # isValid = True
        # isActive = True

        self._mode = kwargs.get('fldMode') or self._mode
        self._mode = self._mode if self._mode in self.__animalMode else 'regular'
        # countMe: Flag 1 / 0 para indicar si objeto se debe contar o no.
        #  1: Regular Animal;
        #  0: Substitute (Animal creado por Reemision de Caravana); External Animal; Generic Animal
        # -1: Dummy (Creado por perform de un Animal Substitute)
        self.__countMe = Animal.__animalMode[self._mode]

        # list of progActivities active for object. Loaded from DB and then updated by the system.
        # self.__myProgActivities = kwargs.get('fldProgActivities', {})  # Dict {paObj: __activityID}

        # TODO: inputs de name desde la UI se deben procesar con removeAccents() para comparar con obj._name
        self._name = removeAccents(kwargs.get('fldName', None)) or self._name
        self._conceptionType = kwargs.get('fldConceptionType', None) if kwargs.get('fldConceptionType', '').lower() \
                                                                in self.__conceptionTypes else self._conceptionType
        self.__surrogateMother = kwargs.get('fldFK_MadreSubrogante', None) or self.__surrogateMother
        self.__comment = kwargs.get('fldComment', '') or self.__comment

        # self.__exitDate = kwargs.get('fldDateExit') or 0  # DBKeyName: fldDateExit

        self.__identifiers = kwargs.get('fldIdentifiers') or self.__identifiers
        if self.__identifiers:
            self.popMyTags('*')
            self.setMyTags(*[Tag.getRegisterDict()[t] for t in Tag.getRegisterDict() if t in self.__identifiers])

        return None


    def categoryName(self):
        """ Returns category Name corresponding to self.lastCategoryID or None if category not defined.
        """                     # self.getCategories() pulls the right __categories dictionary.
        return next((k for k in self.getCategories() if self.getCategories()[k] == self.lastCategoryID), 'No Category.')

    @property
    def name(self):
        return str(tuple(self.myTagNumbers)).replace("(", "").replace(")", "")

    @classmethod
    def getObject(cls, name: str):
        """ Returns the Animal object associated to name.
        Name can be a UUID or a regular human-readable string (Tag Number for Animals).
        @return: Geo object if any found, None if name not found in getGeoEntities dict.
        """
        try:
            o = cls.getRegisterDict().get(name, None)   # cls.getRegisterDict() pulls the right dict for Animal class.
        except (SyntaxError, ValueError, TypeError):
            return None
        else:
            if not o:
                name = re.sub(r'[\\|/@#$%^*()=+¿?{}"\'<>,:;_]', ' ', name)  # '-' is a tag separator. It's part of name.
                name_words = [j for j in removeAccents(name).split(" ") if j]
                name_words = [j.replace(" ", "") for j in name_words]
                return next((cls.getRegisterDict()[k] for k in cls.getRegisterDict()
                             if any(word in removeAccents(cls.getRegisterDict()[k].name) for word in name_words)), None)
            return o

    @classmethod
    def getTotalAnimals(cls, mode=1):  # Total objects. __registerDict {ID: obj, } defined in Bovine, Caprine, etc.
        """
        Counts total animals for given Animal Class.
        @param mode: 0: ALL ID_Animal in cls.__registerDict for given class.
            `        1 (Default): Only ACTIVE Animals. Useful to count stocks.
        @return: Animal count for class cls (int)
        """
        objList = cls.getRegisterDict().values()    # TODO(cmt): cls resolves the proper __registerDict to retrieve
        count = sum([j.countMe for j in objList if j.isActive]) if mode else sum([j.countMe for j in objList])
        # count = 0
        # for obj in objList:
        #     if mode == 0:
        #         count += obj.countMe
        #     else:
        #         if obj.isActive:
        #             count += obj.countMe
        return count


    @classmethod
    def GenActivity_register_func(cls, *, property_name: str = None, func_object=None):
        """
        Registers a function as an attribute (method) of the Generic Activity object pointed to by property name.
        *** The registered method must be an attribute of the instance, NOT the GenericActivity class. ***
        Uses fget() to access the underlying object of the property, as the property is the only handle available.
        @param property_name: property (decorator) name
        @param func_object: method or function object to register for object attached to property_name.
        @return: None
        """
        if not isinstance(property_name, str) or not callable(func_object):
            return None
        try:
            # Pulls the object associated with property_name. by using fget (no parenthesis), the object is returned
            # WITHOUT invoking the object's __call__ method, which in this case is unnecessary.
            # fget() also could be used only because calling fget() returns the same object type as is stored by fget.
            activity_obj = getattr(cls, property_name).fget
        except (AttributeError, NameError):
            activity_obj = None
        if activity_obj:
            activity_obj.register_method(method_obj=func_object)
        return None


    @classmethod
    def initialize(cls):
        """ ActivityMethod for initialization of animals upon loading records from DB"""
        pass

    @property
    def recordID(self):
        return self.__recordID


    @property
    def mode(self):
        return self._mode

    @mode.setter
    def mode(self, val):
        if val in set(Animal.__animalMode.keys()):
            self._mode = val

    @property
    def countMe(self):
        return self.__countMe

    @countMe.setter
    def countMe(self, val):
        self.__countMe = val if val in set(Animal.__animalMode.values()) else 0


    def getIdentifiers(self):
        return self.__identifiers       # Returns a dict of identifiers {identifier: id_type, }

    def setIdentifiers(self, val):
        if isinstance(val, dict):
            self.__identifiers = val    # Returns a dict of identifiers {identifier: id_type, }

    def updateIdentifiers(self, val):       # Not used for now. But may be needed later...
        if isinstance(val, dict):
            self.__identifiers.update(val)    # Returns a dict of identifiers {identifier: id_type, }

    @property
    def isRegular(self):
        """
        Regular are physical animals defined in the system
        @return: True / False
        """
        return 'regular' in self._mode.lower()               # TODO: AQUI ESTA LA MERMA CON LOS THREADS!!! Cual seria??

    @property
    def isDummy(self):
        """
        Returns ID_Dummy condition. Dummy IDs are ID_Animal with countMe=-1 to support the logic handling Animals with
        missing or no assigned Tags.
        @return: True / False
        """
        return 'dummy' in self._mode.lower()

    @property
    def isSubstitute(self):
        """
        Returns Substitute condition. Substitute IDs are ID_Animal with countMe=0 to support the logic handling Animals
        with missing or no assigned Tags.
        @return: True / False
        """
        return 'substit' in self._mode.lower()


    @property
    def isExternal(self):
        """
        Returns External condition. External means Animal not in control of this system. Animal is externally owned and
        externally managed. Example: Father/Mother of an animal defined in the system that is not part of this system.
        @return: True / False
        """
        return 'extern' in self._mode.lower()

    @property
    def isGeneric(self):
        """
        Returns Generic Animal condition. Generics have countMe=0 to support the logic for multiple-animal notifications
        and Programmed Activities.
        @return: True / False
        """
        return 'generic' in self._mode.lower()

    @property
    def tblDataInventoryName(self):
        return self.__tblDataInventoryName

    @property
    def tblObjectsName(self):
        return self.__tblObjName

    @property
    def tblDataStatusName(self):
        return self.__tblDataStatusName


    # Diccionario Animal.Status - UNICO DICCIONARIO CON VALUES=[list]. Los demas dicts se deberan crear con val unico
    temp = getRecords('tblAnimalesStatus', '', '', None, 'fldID', 'fldStatus', 'fldFlag')
    __animalStatusDict = {}
    for j in range(temp.dataLen):                # {statusName: [statusID, activeYN]}
        __animalStatusDict[str(temp.dataList[j][1])] = [int(temp.dataList[j][0]), int(temp.dataList[j][2])]

    @property
    def statusDict(self):  # Diccionario con definicion de Animal Status
        return self.__animalStatusDict

    @classmethod
    def getStatusDict(cls):
        return cls.__animalStatusDict

    @classmethod
    def processReplicated(cls):        # TODO(cmt): Called by Bovine, Caprine, etc.
        """             ******  Run from an AsyncCursor queue. NO PARAMETERS ACCEPTED FOR NOW. ******
                        ******  IMPORTANT: This code should execute in LESS than 5 msec (switchinterval).      ******
        Used to execute logic for detection and management of INSERTed, UPDATEd and duplicate objects.
        Defined for Animal, Tag, Person, Geo.
        Checks for additions to tblAnimales from external sources (replication from other nodes) for Valid and
        Active objects. Updates _fldID_list, _new_local_fldIDs, _object_fldUPDATE_dict
        @return: True if update operation succeeds, False if reading tblAnimales from db fails.
        """
        temp = getRecords(cls.tblObjName(), '', '', None, '*', fldDateExit=0)
                          # fldMode=('regular', 'substitute', 'external'))
        if not isinstance(temp, DataTable):
            return False
        # 1. INSERT -> Checks for INSERTED new Records verifying the status of the last-stored fldID col and locally
        # added records then, process repeats/duplicates.
        krnl_logger.info(f"------------------- INSIDE {cls.__name__}.processReplicated!! ---------------------------")
        pulled_fldIDCol = temp.getCol('fldID')
        newRecords = set(pulled_fldIDCol).difference(cls._fldID_list)
        if newRecords:
            newRecords = list(newRecords)
            pulledIdentifiersCol = temp.getCol('fldIdentifiers')     # List of lists. Each of the lists contains UIDs.
            fullIdentifiers = []         # List of ALL identifiers from temp table, to check for repeat objects.
            for lst in pulledIdentifiersCol:
                if isinstance(lst, (list, tuple, set)):
                    fullIdentifiers.extend(lst)
            fullIdentifiers = set(fullIdentifiers)
            # TODO(cmt): here runs the logic for duplicates resolution for each of the uids in newRecords.
            for j in newRecords:        # newRecords: ONLY records NOT found in _fldID_list from previous call.
                # TODO: Pick the right cls (Bovine, Caprine, etc).
                animalRec = temp.unpackItem(pulled_fldIDCol.index(j))
                objClassID = animalRec.get('fldFK_ClaseDeAnimal')
                # Gets objClass to instantiate the right type of obj and to get the right __registerDict.
                objClass = next((k for k in cls.getAnimalClasses() if cls.getAnimalClasses()[k] == objClassID), None)
                if objClass:
                    obj = objClass(**animalRec)
                else:
                    continue        # If cls is not found, moves on to next newRecord.

                # If record is repeat (at least one of its identifiers is found in cls._identifiers), updates repeat
                # records for databases that might have created repeats. Changes are propagated by the replicator.
                identifs = obj.getIdentifiers()   # getIdentifiers() returns set of identifiers UIDs.

                """ Checks for duplicate/repeat objects: Must determine which one is the Original and set the record's 
                fldObjectUID field with the UUID of the Original object and fldExitDate to flag it's a duplicate.
                """
                # Note: the search for common identifiers and the logic of using timeStamp assures there is ALWAYS
                # an Original object to fall back to, to ensure integrity of the database operations.
                if fullIdentifiers.intersection(identifs):
                    # TODO(cmt): Here detected duplicates: Assigns Original and duplicates based on myTimeStamp.
                    for o in objClass.getRegisterDict().values():
                        if o.getIdentifiers().intersection(identifs):
                            if o.myTimeStamp <= obj.myTimeStamp:
                                original = o
                                duplicate = obj
                            else:
                                original = obj
                                duplicate = o
                            original.getIdentifiers().update(duplicate.getIdentifiers())  # for data integrity.
                            setRecord(cls.tblObjName(), fldID=duplicate.recordID, fldObjectUID=original.ID,
                                      fldExitDate=time_mt('datetime'))
                            setRecord(cls.tblObjName(), fldID=original.recordID,
                                      fldIdentifiers=original.getIdentifiers())
                            # TODO: execute a baja('duplicate') here to do proper object cleanup ???? Must Confirm this.
                            break
                elif animalRec.get('fldDB_ID') != MAIN_DB_ID:
                    # If record is not duplicate and comes from another node, adds it to __registerDict.
                    obj.register()
                    obj.setMyTags(*[Tag.getRegisterDict()[t] for t in Tag.getRegisterDict() if t in obj.getIdentifiers()])


        # 2. UPDATE - Checks for UPDATED records modified in other nodes and replicated to this database. Checks are
        # performed based on value of fldUPDATE field (a dictionary) in each record.
        # The check for the node generating the UPDATE is done here in order to avoid unnecessary setting values twice.
        # UPDATEDict = {temp.getVal(j, 'fldID'): temp.getVal(j, 'fldUPDATE') for j in range(temp.dataLen) if
        #               temp.getVal(j, 'fldUPDATE')}      # Creates dict only with non-NULL (populated) items.
        UPDATEDict = {}
        for j in range(temp.dataLen):
            if temp.getVal(j, 'fldUPDATE'):
                d = temp.unpackItem(j)
                UPDATEDict[d['fldID']] = d['fldUPDATE']
        changed = {k: UPDATEDict[k] for k in UPDATEDict if k not in cls._object_fldUPDATE_dict or
                   (k in cls._object_fldUPDATE_dict and UPDATEDict[k] != cls._object_fldUPDATE_dict[k])}
        if changed:             # changed = {fldID: fldUPDATE (int count), }
            for k in changed:           # updates all records in local database with records updated by other nodes.
                # Update memory structures here: __registerDict, exitDate, etc, based on passed fldNames, values.
                changedRecord = temp.unpackItem(fldID=k)
                objClassID = changedRecord.get('fldFK_ClaseDeAnimal')
                objClass = next((k for k in cls.getAnimalClasses() if cls.getAnimalClasses()[k] == objClassID), None)
                if objClass:
                    obj = next((o for o in objClass.getRegisterDict().values() if o.recordID == k), None)
                    if obj:
                        # TODO(cmt): UPDATEs obj specific attributes with values from read db record.
                        obj.updateAttributes(**changedRecord)
                        if changedRecord.get('fldObjectUID', 0) != obj.ID:
                            obj.setID(changedRecord['fldObjectUID'])
                        if changedRecord.get('fldDateExit', 0) != obj.exitDate:
                            obj.setExitDate(changedRecord['fldDateExit'])
                            obj.isActive = False
                            # Removes object for now. TODO: baja('Repeat') in principle, not needed here...
                            obj.popMyTags('*')
                            # obj.getRegisterDict().pop(obj, None)
                            obj.unregister()
                # Updates _object_fldUPDATE_dict (of the form {fldID: UUID(str), }) to latest values.
                cls._object_fldUPDATE_dict[k] = changed[k]

        # 3. BAJA / DELETE -> For DELETEd Records and records with fldExitDate !=0, removes object from __registerDict.
        temp1 = getRecords(cls.tblObjName(), '', '', None, '*')
        if not isinstance(temp, DataTable):
            return False
        # Removes from __registerDict Animals with baja() (exit_recs) and DELETE (deleted_recs) executed in other nodes.
        all_recs = temp1.getCol('fldID')
        exit_recs = set(all_recs).difference(pulled_fldIDCol)
        remove_recs = exit_recs.difference(cls._fldID_exit_list)      # Compara con lista de exit Records ya procesados.
        deleted_recs = set(cls._fldID_list).difference(pulled_fldIDCol)
        remove_recs = remove_recs.union(deleted_recs) or []
        for i in remove_recs:
            obj = next((o for o in cls.getRegisterDict().values() if o.recordID == i), None)
            if obj:
                obj.popMyTags('*')          # Clears Tag assignment in Tag objects.
                # TODO: Remove ProgActivities????
                obj.unregister()

        # Updates list of fldID
        cls._fldID_list = pulled_fldIDCol.copy()
        # and list of records with fldExitDate > 0 (Animales con Salida).
        cls._fldID_exit_list = exit_recs.copy()
        return True


    # @classmethod
    # def processReplicated03(cls):  # TODO(cmt): Called by Bovine, Caprine, etc.
    #     """             ******  Run from an AsyncCursor queue. NO PARAMETERS ACCEPTED FOR NOW. ******
    #                     ******  IMPORTANT: This code should execute in LESS than 5 msec (switchinterval).      ******
    #     Used to execute logic for detection and management of INSERTed, UPDATEd and duplicate objects.
    #     Defined for Animal, Tag, Person, Geo.
    #     Checks for additions to tblAnimales from external sources (replication from other nodes) for Valid and
    #     Active objects. Updates _fldID_list, _new_local_fldIDs, _object_fldUPDATE_dict
    #     @return: True if update operation succeeds, False if reading tblAnimales from db fails.
    #     """
    #     temp = getRecords(cls.tblObjName(), '', '', None, '*', fldDateExit=0,
    #                       fldMode=('regular', 'substitute', 'external'))
    #     if not isinstance(temp, DataTable):
    #         return False
    #     # 1. Checks for INSERTED new Records verifying the status of the last-stored fldID col and locally added records
    #     # then, process repeats/duplicates.
    #     pulled_fldIDCol = temp.getCol('fldID')
    #     newRecords = set(pulled_fldIDCol).difference(set(cls._fldID_list).union(cls._new_local_fldIDs))
    #     if newRecords:
    #         newRecords = list(newRecords)
    #         pulledIdentifiersCol = temp.getCol('fldIdentifiers')  # List of dicts: [{identifier: ident_type, }, ]
    #         fullIdentifiers = []  # List of ALL identifiers from temp table, to check for repeat objects.
    #         for d in pulledIdentifiersCol:
    #             fullIdentifiers.extend(list(d.keys()))
    #         fullIdentifiers = set(fullIdentifiers)
    #         # TODO(cmt): here runs the logic for duplicates resolution for each of the uids in newRecords.
    #         for j in newRecords:  # newRecords: ONLY records NOT found in getRegisterDict().
    #             # TODO: Pick the right cls (Bovine, Caprine, etc).
    #             animalRec = temp.unpackItem(pulled_fldIDCol.index(j))
    #             objClassID = animalRec.get('fldFK_ClaseDeAnimal')
    #             # Gets objClass to instantiate the right type of obj and to get the right __registerDict.
    #             objClass = next((k for k in cls.getAnimalClasses() if cls.getAnimalClasses()[k] == objClassID), None)
    #             if objClass:
    #                 obj = objClass(**animalRec)
    #             else:
    #                 continue  # If cls is not found, moves on to next newRecord.
    #
    #             # If record is repeat (at least one of its identifiers is found in cls._identifiers), updates repeat
    #             # records for all databases that might have created repeats. Changes are propagated by the replicator.
    #             identifs = set(obj.getIdentifiers().keys())  # getIdentifiers() retorna dict {identifier: ident_type, }
    #             """ Process duplicate/repeat objects: Must determine which one is the Original and set the record's
    #             fldObjectUID field with the UUID of the Original object and fldExitDate to indicate it's a duplicate."""
    #             # Note: the search for common identifiers and the logic of using timeStamp assures there is ALWAYS an
    #             # Original object to fall back to, to ensure integrity of the database operations.
    #             if fullIdentifiers.intersection(identifs):
    #                 for o in objClass.getRegisterDict().values():
    #                     if o.getIdentifiers().intersection(identifs):
    #                         if o.myTimeStamp <= obj.myTimeStamp:
    #                             original = o
    #                             duplicate = obj
    #                         else:
    #                             original = obj
    #                             duplicate = o
    #                         original.getIdentifiers().update(duplicate.getIdentifiers())  # for data integrity.
    #                         setRecord(cls.tblObjName(), fldID=duplicate.recordID, fldObjectUID=original.ID,
    #                                   fldExitDate=time_mt('datetime'))
    #                         setRecord(cls.tblObjName(), fldID=original.recordID,
    #                                   fldIdentifiers=original.getIdentifiers())
    #                         # TODO: execute a baja('duplicate') here to do proper object cleanup ???? Confirm this.
    #                         break
    #             else:  # If record is not duplicate, adds it to __registerDict.
    #                 obj.register(external=True)
    #     # Updates list of fldID
    #     cls._fldID_list = pulled_fldIDCol.copy()  # [o.recordID for o in cls.getRegisterDict().values()]
    #     cls._new_local_fldIDs *= 0  # And resets local_fldIDs to be able to detect local records in next call.
    #
    #     # 2. Checks for UPDATED records, modified in other nodes, and replicated to this database. Checks are performed
    #     # based on value of fldUPDATE field (a dictionary) in each record.
    #     # The check for the node generating the UPDATE is done here in order to avoid unnecessary setting values twice.
    #     UPDATEDict = {temp.getVal(j, 'fldID'): temp.getVal(j, 'fldUPDATE') for j in range(temp.dataLen) if
    #                   temp.getVal(j, 'fldUPDATE')}  # Creates dict only with non-NULL (populated) items.
    #     changed = {k: UPDATEDict[k] for k in UPDATEDict if k not in cls._object_fldUPDATE_dict or
    #                (k in cls._object_fldUPDATE_dict and UPDATEDict[k] != cls._object_fldUPDATE_dict[k])}
    #     if changed:  # changed = {fldID: fldUPDATE (int count), }
    #         for k in changed:  # updates all records in local database with records updated by other nodes.
    #             """ Update memory structures here: __registerDict, exitDate, etc, based on passed fldNames, values. """
    #             changedRecord = temp.unpackItem(fldID=k)
    #             objClassID = changedRecord.get('fldFK_ClaseDeAnimal')
    #             objClass = next((k for k in cls.getAnimalClasses() if cls.getAnimalClasses()[k] == objClassID), None)
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
    #                         # TODO: Removes object for now. MUST PERFORM baja('Repeat').
    #                         objClass.getRegisterDict().pop(obj, None)
    #             # Updates _fldUPDATE_Dict (of the form {fldID: fldUPDATE_counter, }) to latest values.
    #             cls._object_fldUPDATE_dict[k] = changed[k]
    #
    #         # for k in changed:
    #         #     # Updates _fldUPDATE_Dict (of the form {fldID: fldUPDATE_counter, }) to latest values.
    #         #     cls._object_fldUPDATE_dict[k] = changed.get(k, 0)
    #     return True


    # @classmethod
    # def processReplicated02(cls):  # TODO(cmt): Called by Bovine, Caprine, etc.
    #     """             ******  Run periodically as IntervalTimer func. ******
    #                     ******  IMPORTANT: This code should execute in LESS than 5 msec (switchinterval).      ******
    #     Used to execute logic for detection and management of INSERTed, UPDATEd and duplicate objects.
    #     Defined for Animal, Tag, Person, Geo.
    #     Checks for additions to tblAnimales from external sources (replication from other nodes) for Valid and
    #     Active objects. Updates _fldID_list, _new_local_fldIDs, _object_fldUPDATE_dict
    #     @return: True if update operation succeeds, False if reading tblAnimales from db fails.
    #     """
    #     temp = getRecords(cls.tblObjName(), '', '', None, '*', fldFK_ClaseDeAnimal=cls.animalClassID, fldDateExit=None,
    #                       fldMode=('regular', 'substitute', 'external'))
    #     if not isinstance(temp, DataTable):
    #         return False
    #     if any(cls is j for j in (Animal, Mammal, Birds)):
    #         return None  # Function must not run for Animal, Mammal, Birds. Only for their subclasses.
    #
    #     # 1. Checks for INSERTED new Records verifying the status of the last-stored fldID col and locally added records
    #     # then, process repeats/duplicates.
    #     pulled_fldIDCol = temp.getCol('fldID')
    #     newRecords = set(pulled_fldIDCol).difference(set(cls._fldID_list).union(cls._new_local_fldIDs))
    #     if newRecords:
    #         newRecords = list(newRecords)
    #         pulledIdentifiersCol = temp.getCol('fldIdentifiers')  # List of dicts: [{identifier: ident_type, }, ]
    #         fullIdentifiers = []  # List of ALL identifiers from temp table, to check for repeat objects.
    #         for d in pulledIdentifiersCol:
    #             fullIdentifiers.extend(list(d.keys()))
    #         fullIdentifiers = set(fullIdentifiers)
    #         # TODO(cmt): here runs the logic for duplicates resolution for each of the uids in newRecords.
    #         for j in newRecords:  # newRecords: ONLY records NOT found in getRegisterDict().
    #             obj = cls(**temp.unpackItem(pulled_fldIDCol.index(j)))
    #             # If record is repeat (at least one of its identifiers is found in cls._identifiers), updates repeat
    #             # records for all databases that might have created repeats. Changes are propagated by the replicator.
    #             identifs = set(obj.getIdentifiers().keys())  # getIdentifiers() retorna dict {identifier: ident_type, }
    #
    #             """ Process duplicate/repeat objects: Must determine which one is the Original and set the record's
    #             fldObjectUID field with the UUID of the Original object and fldExitDate to indicate it's a duplicate."""
    #             # Note: the search for common identifiers and the logic of using timeStamp assures there is ALWAYS an
    #             # Original object to fall back to, to ensure integrity of the database operations.
    #             if fullIdentifiers.intersection(identifs):
    #                 for o in cls.getRegisterDict().values():
    #                     if o.getIdentifiers().intersection(identifs):
    #                         if o.myTimeStamp <= obj.myTimeStamp:
    #                             original = o
    #                             duplicate = obj
    #                         else:
    #                             original = obj
    #                             duplicate = o
    #                         original.getIdentifiers().update(duplicate.getIdentifiers())  # for data integrity.
    #                         setRecord(cls.tblObjName(), fldID=duplicate.recordID, fldObjectUID=original.ID,
    #                                   fldExitDate=time_mt('datetime'))
    #                         setRecord(cls.tblObjName(), fldID=original.recordID,
    #                                   fldIdentifiers=original.getIdentifiers())
    #                         # TODO: execute a baja('duplicate') here to do proper object cleanup ???? Confirm this.
    #                         break
    #             else:  # If record is not duplicate, adds it to __registerDict.
    #                 obj.register(external=True)
    #     # Updates list of fldID
    #     cls._fldID_list = pulled_fldIDCol.copy()  # [o.recordID for o in cls.getRegisterDict().values()]
    #     cls._new_local_fldIDs *= 0  # And resets local_fldIDs to be able to detect local records in next call.
    #
    #     # 2. Checks for UPDATED records, modified in other nodes, and replicated to this database. Checks are performed
    #     # based on value of fldUPDATE field in each record.
    #     # The check for the node generating the UPDATE is done here in order to avoid unnecessary setting values twice.
    #     UPDATEDict = {temp.getVal(j, 'fldID'): temp.getVal(j, 'fldUPDATE') for j in range(temp.dataLen) if
    #                   temp.getVal(j, 'fldUPDATE')}  # Creates dict only with non-NULL (populated) items.
    #     changed = {k: UPDATEDict[k] for k in UPDATEDict if k not in cls._object_fldUPDATE_dict or (k in cls._object_fldUPDATE_dict and
    #                                                                                                 UPDATEDict[k] !=
    #                                                                                                 cls._object_fldUPDATE_dict[k])}
    #     if changed:  # changed = {fldID: fldUPDATE(dict), }
    #         for k in changed:  # updates all records in local database with records updated by other nodes.
    #             """ Update memory structures here: __registerDict, exitDate, etc, based on passed fldNames, values. """
    #             changed_fields = temp.unpackItem(fldID=k)
    #             obj = next((o for o in cls.getRegisterDict().values() if o.recordID == k), None)
    #             if obj:
    #                 # TODO(cmt): UPDATEs obj specific attributes with values from read db record.
    #                 obj.updateAttributes(**changed_fields)
    #                 if changed_fields.get('fldObjectUID', 0) != obj.ID:
    #                     obj.setID(changed_fields['fldObjectUID'])
    #                 if changed_fields.get('fldDateExit', 0) != obj.exitDate:
    #                     obj.setExitDate(changed_fields['fldDateExit'])
    #                     obj.isActive = False
    #                     # TODO: Removes object for now. MUST PERFORM baja('Repeat').
    #                     cls.getRegisterDict().pop(obj, None)
    #
    #                 # if changed_fields.get('fldIdentifiers', 0).keys() != obj.getIdentifiers().keys():
    #                 #     obj.setIdentifiers(changed_fields['fldIdentifiers'])        # Updates ALL identifiers for obj.
    #
    #         for k in UPDATEDict:
    #             # Updates _fldUPDATE_Dict (of the form {fldID: fldUPDATE_counter, }) to latest values.
    #             if k in cls._object_fldUPDATE_dict:
    #                 cls._object_fldUPDATE_dict[k] = UPDATEDict.get(k, 0)
    #     return True
    #
    # processReplicated02 = classmethod(processReplicated02)

    # @classmethod
    # def processReplicated01(cls):  # TODO(cmt): Called by Bovine, Caprine, etc.
    #     """             ******  Run periodically as IntervalTimer func. ******
    #                     ******  IMPORTANT: This code should execute in LESS than 5 msec (switchinterval).      ******
    #     Used to execute logic for detection and management of INSERTed, UPDATEd and duplicate objects.
    #     Defined for Animal, Tag, Person, Geo.
    #     Checks for additions to tblAnimales from external sources (replication from other nodes) for Valid and
    #     Active objects. Updates _fldID_list, _new_local_fldIDs, _object_fldUPDATE_dict
    #     @return: True if update operation succeeds, False if reading tblAnimales from db fails.
    #     """
    #     temp = getRecords(cls.tblObjName(), '', '', None, '*', fldFK_ClaseDeAnimal=cls.animalClassID, fldDateExit=None,
    #                       fldMode=('regular', 'substitute', 'external'))
    #     if not isinstance(temp, DataTable):
    #         return False
    #
    #     # 1. Checks for INSERTED new Records verifying the status of the last-stored fldID col and locally added records
    #     # then, process repeats/duplicates.
    #     pulled_fldIDCol = temp.getCol('fldID')
    #     newRecords = set(pulled_fldIDCol).difference(set(cls._fldID_list).union(cls._new_local_fldIDs))
    #     if newRecords:
    #         newRecords = list(newRecords)
    #         pulledIdentifiersCol = temp.getCol('fldIdentifiers')  # List of dicts  {identifier: ident_type,  }
    #         identifiersSet = []
    #         for d in pulledIdentifiersCol:
    #             identifiersSet.extend(list(d.keys()))
    #         identifiersSet = set(identifiersSet)  # List of identifiers to check for repeat objects.
    #         # TODO(cmt): here runs the logic for duplicates resolution for each of the uids in newRecords.
    #         for j in newRecords:  # newRecords: ONLY records NOT found in getRegisterDict().
    #             obj = cls(**temp.unpackItem(pulled_fldIDCol.index(j)))
    #             # If record is repeat (at least one of its identifiers is found in cls._identifiers), updates repeat
    #             # records for all databases that might have created repeats. Changes are propagated by the replicator.
    #             idents = set(obj.getIdentifiers().keys())  # getIdentifiers() retorna dict {identifier: ident_type, }
    #
    #             """ Process duplicate/repeat objects: Must determine which one is the Original and set the record's
    #             fldObjectUID field with the UUID of the Original object and fldExitDate to indicate it's a duplicate."""
    #             if identifiersSet.intersection(idents):
    #                 for o in cls.getRegisterDict().values():
    #                     if o.getIdentifiers().intersection(idents):
    #                         if o.myTimeStamp <= obj.myTimeStamp:
    #                             original = o
    #                             duplicate = obj
    #                         else:
    #                             original = obj
    #                             duplicate = o
    #                         original.getIdentifiers().update(duplicate.getIdentifiers())  # for data integrity.
    #                         setRecord(cls.tblObjName(), fldID=duplicate.recordID, fldObjectUID=original.ID,
    #                                   fldExitDate=time_mt('datetime'))
    #                         setRecord(cls.tblObjName(), fldID=original.recordID,
    #                                   fldIdentifiers=original.getIdentifiers())
    #                         # TODO: execute a baja('duplicate') here to do proper object cleanup ???? Confirm this.
    #                         break
    #             else:  # If record is not duplicate, adds it to __registerDict.
    #                 obj.register(external=True)
    #     # Updates class list of records and
    #     cls._fldID_list = [o.recordID for o in cls.getRegisterDict().values()]
    #     cls._new_local_fldIDs *= 0  # And resets local_fldIDs to be able to detect local records in next call.
    #
    #     # 2. Checks for UPDATED records, modified in other nodes, and replicated to this database. Checks are performed
    #     # based on fldID value of stored and pulled tables.
    #     # The check for the node generating the UPDATE is done here in order to avoid unnecessary setting values twice.
    #     UPDATEDict = {temp.getVal(j, 'fldID'): temp.getVal(j, 'fldUPDATE') for j in range(temp.dataLen) if
    #                   temp.getVal(j, 'fldUPDATE')}  # Creates dict only with non-NULL (populated) items.
    #     changed = {k: UPDATEDict[k] for k in UPDATEDict if k not in cls._object_fldUPDATE_dict or (k in cls._object_fldUPDATE_dict and
    #                                                                                         UPDATEDict[k] !=
    #                                                                                         cls._object_fldUPDATE_dict[k])}
    #     if changed:  # changed = {fldID: fldUPDATE(dict), }
    #         for k in changed:  # updates all records in local database with records updated by other nodes.
    #             # TODO: Update memory structures here: __registerDict, exitDate, etc, based on passed fldNames, values.
    #             changed_fields = changed[k]
    #             obj = next((o for o in cls.getRegisterDict().values() if o.recordID == k), None)
    #             if obj:
    #                 if changed_fields.get('fldDateExit', 0):
    #                     obj.setExitDate(changed_fields['fldDateExit'])
    #                     # TODO: Removes object for now. MUST PERFORM baja('Repeat').
    #                     cls.getRegisterDict().pop(obj, None)
    #                     break
    #                 if 'fldObjectUID' in changed_fields:
    #                     obj.setID(changed_fields['fldObjectUID'])
    #                 if changed_fields.get('fldIdentifiers'):
    #                     obj.setIdentifiers(changed_fields['fldIdentifiers'])  # Updates ALL identifiers for obj.
    #
    #         for k in UPDATEDict:
    #             # Updates _fldUPDATE_Dict (of the form {fldID: fldUPDATE(dict), }) to latest values.
    #             if k in cls._object_fldUPDATE_dict:
    #                 cls._object_fldUPDATE_dict[k] = UPDATEDict.get(k, {})
    #     return True

    # @classmethod
    # # @timerWrapper(iterations=6)
    # def processReplicated00(cls):  # TODO(cmt): Called by Bovine, Caprine, etc.
    #     """             ******  Run periodically as IntervalTimer func. ******
    #                     ******  IMPORTANT: This code should execute in LESS than 5 msec (switchinterval).      ******
    #     Used to execute logic for detection and management of duplicate objects.
    #     Defined for Animal, Tag, Person. (Geo has its own function on TransactionalObject class).
    #     Checks for additions to tblAnimales from external sources (db replication) for Valid and Active objects.
    #     Updates _table_record_count.
    #     @return: True if update operation succeeds is updated, False if reading tblAnimales from db fails.
    #     """
    #     temp = getRecords(cls.tblObjName(), '', '', None, '*', fldFK_ClaseDeAnimal=cls.animalClassID, fldDateExit=None,
    #                       fldMode=('regular', 'substitute', 'external'))
    #     if not isinstance(temp, DataTable):
    #         return False
    #     pulledUIDCol = temp.getCol('fldObjectUID')
    #     newRecords = set(pulledUIDCol).difference(set(cls._full_uid_list).union(cls._new_local_uids))
    #     if newRecords:
    #         newRecords = list(newRecords)
    #         # TODO: here runs the logic for duplicates resolution for each of the uids in newRecords.
    #         for j in newRecords:  # newRecords: ONLY records NOT found in getRegisterDict().
    #             rec = temp.unpackItem(pulledUIDCol.index(j))
    #             obj = cls(**rec)
    #             # If record is repeat (must run the isRepeat() code for the class to determine this), updates repeat records
    #             # for all databases that might have created repeats. The changes are propagated by the replicator.
    #             orig = obj.getOriginalObj(cls._full_uid_list)
    #             if orig:
    #                 if orig.ID in cls._full_uid_list:
    #                     pass
    #                 else:
    #                     obj.register(external=True)
    #             else:  # If record is not repeat, adds it to __registerDict.
    #                 obj.register(external=True)
    #
    #     # Updates class list of records and resets local_uids.
    #     cls._full_uid_list = list(cls.getRegisterDict().keys())
    #     cls._new_local_uids *= 0
    #     return True

    # @classmethod
    # def checksumUID(cls, animal_class_id=None):  # TODO(cmt): Called by Bovine, Caprine, etc.
    #     """
    #     Used to execute logic for detection and management of duplicate objects. Run periodically by AsyncBuffer obj.
    #     Defined for Animal, Tag, Person. (Geo has its own function on TransactionalObject class).
    #     Updates _checksum of all UUIDs from Valid and Active records on cls objects table (tblAnimales, etc.).
    #     Updates _table_record_count.
    #     @return: True if _checksum is updated, False if fails.
    #     """
    #     temp = getRecords(cls.tblObjName(), '', '', None, 'fldObjectUID', fldFK_ClaseDeAnimal=animal_class_id,
    #                       fldDateExit=None, fldMode=('regular', 'substitute', 'external'))
    #     if isinstance(temp, str):
    #         return False
    #     uidCol = temp.getCol('fldObjectUID')
    #     check_sum = sum([int(UUID(j)) for j in uidCol]) or 0
    #     with cls.__lock:
    #         cls._table_record_count = temp.dataLen
    #         cls._checksum = check_sum
    #         return True

    @classmethod
    def generateDOB(cls, animalClass, categoryArg, startDate=None):
        """
        Creates a DOB based on Animal Class and CategoryActivity. Used to provide a DOB when one is not passed via UI.
        Any DOB that comes from UI overrides the system generated DOB.
        @param animalClass: 'Vacuno', 'Caprino', etc.
        @param categoryArg: Animal category (str). Mandatory.
        @param startDate: Initial date to create a dob from. It's a datetime object. Default: None converts to now().
        DOB is counted backward from this startDate, a number of days determined by Category.
        @return: datetime DOB or errorCode (str)
        """
        if animalClass not in Animal.getAnimalKinds():
            raise ValueError(f'ERR_INP_InvalidArgument: {animalClass} - {moduleName()}({lineNum()}) - {callerFunction()}')
        ageLimitsDict = cls.getAgeLimits()    # cls resolves the correct AgeLimits dict to retrieve.
        if categoryArg not in ageLimitsDict:
            raise ValueError(f'ERR_INP_InvalidArgument: {categoryArg} - {moduleName()}({lineNum()}) - {callerFunction()}')
        age = ageLimitsDict[categoryArg]
        if age > 0:
            startDate = startDate if startDate else datetime.fromtimestamp(time_mt())
            # if datetime.fromtimestamp(time_mt()) - datetime.timedelta(days=cls.__MIN_REWIND_DAYS) >= startDate:
            if (datetime.fromtimestamp(time_mt()) - startDate).total_seconds()/SPD > cls.__MIN_REWIND_DAYS:
                raise ValueError(f'ERR_INP_InvalidArgument: {startDate} out of range - '
                                 f'{moduleName()}({lineNum()}) - {callerFunction()}')
            dobSys = startDate - timedelta(days=age)
            dobSys = dobSys.replace(hour=22, minute=22, second=22, microsecond=222222)  # Mask for System-set DOB
            retValue = dobSys
        else:
            raise ValueError(f'ERR_INP_InvalidArgument: {categoryArg} - {moduleName()}({lineNum()}) - {callerFunction()}')
        return retValue

    @property
    def dob(self):
        """
        returns DOB as a datetime object
        @return: datetime object date of birth.
        """
        return self._fldDOB

    @dob.setter
    def dob(self, val):
        """
        sets dob as a datetime object
        @param val: dob (datetime object)
        @return:
        """
        if isinstance(val, datetime):
            self._fldDOB = val

    def _age(self, *, event_date=False, age_delta=False):
        """
        Returns age (if DOB is defined for the object) as days (default). age_timedelta=True -> datetime.timedelta.
        @param age_delta: True: returns datetime.delta object. False (default): Age in days.
        @return: Age: age in number of days since dob (float) or delta object.
        """
        if age_delta:
            return time_mt('datetime') - self.dob        # returns timedelta structure (years, months, days, min, sec)
        if event_date:
            return time_mt('datetime')      # Retorna fecha en que se computa la edad.
        return (time_mt() - self.dob.timestamp())/SPD if USE_DAYS_MULT else (time_mt('datetime') - self.dob).days

    @property
    def mf(self):
        return self._fldMF

    def _setMF(self, val):
        if isinstance(val, str):
            val = val.strip().lower()
            if val == 'm' or val == 'f':
                self._fldMF = val

    @property
    def isCastrated(self):
        return self._fldFlagCastrado

    @isCastrated.setter
    def isCastrated(self, val):
        self._fldFlagCastrado = val if isinstance(val, datetime) else bool(val)

    @property
    def animalClass(self):
        """
        @return:  Returns animalClassID: Vacuno, Ovino, etc. (string). DEPRECATED, left here for compatibility for now.
        """
        return self._kindOfAnimalID

    @property
    def animalClassID(self):
        """
        @return:  Returns animalClassID (int)
        """
        return self._kindOfAnimalID

    @property
    def animalClassName(self):
        """
        @return:  Returns animalClass Name (str)
        """
        return self._animalClassName

    @property
    def animalRace(self):
        return self._fldFK_Raza

    @property
    def exitYN(self):
        return self.__exitDate

    def setExitDate(self, val):
        if isinstance(val, datetime) or val in (0, 1):
            self.__exitDate = val

    @property
    def myTimeStamp(self):   # unusual name to avoid conflicts with the many date, timeStamp, fldDate in the code.
        """
        @return: record creation timestamp as a datetime object.
        """
        return self._timeStamp

    # @fldDate.setter
    # def fldDate(self, val):
    #     """
    #     sets fldDate as a datetime object
    #     @param val: fldDate (datetime object)
    #     @return:
    #     """
    #     if isinstance(val, datetime):
    #         self._timeStamp = val
    #     else:
    #         try:
    #             self._timeStamp = strptime(val, fDateTime)
    #         except (TypeError, ValueError):
    #             raise TypeError(f'ERR_Invalid type for datetime value.')

    @property
    def myProgActivities(self):
        """ Returns dict of ProgActivities assigned to object """
        return self.__myProgActivities          # Dict {paObject: __activityID}

    def registerProgActivity(self, obj: ProgActivity):
        # TODO(cmt): Each node initializes and loads ONLY ProgActivities created by this node in __myProgActivities.
        #  This is to implement a consistent ProgActivity closure across all nodes when all db records are replicated.
        if isinstance(obj, ProgActivity) and obj.isActive > 0 and obj.dbID == MAIN_DB_ID:
            if obj not in self.__myProgActivities:
                self.__myProgActivities[obj] = obj.activityID        # Dict {paObject: __activityID}
                self.__activeProgActivities.append(obj)              # set {paObj, }. ALWAYS appends for this to work.

    def unregisterProgActivity(self, obj: ProgActivity):
        if isinstance(obj, ProgActivity) and obj in self.__myProgActivities:   #  and obj.isActive < 2:
            self.__myProgActivities.pop(obj, None)                   # Dict {paObject: __activityID, }
            try:
                return self.__activeProgActivities.remove(obj)       # List [paObj, ].  Must remove().
            except ValueError:
                return None

    @classmethod
    def paCreateActivity(cls, activity_id=None, *args: DataTable, enable_activity=activityEnableFull, items_dict=None,
                         **kwargs):
        """ Creates a Animal Programmed Activity (PA) and registers the created object in __registerDict.
        Records data in the RAP, Data Programacion, tblLinkPA in database.
        @param enable_activity:
        @param activity_id: (int). Activity ID defined in cls.supportsPA dict.
        @param items_dict: dict {objID: fldProgrammedDate} for objects that the progActivity is to be assigned to.
        @param args: tblLinkPA must be fully populated with items and fldProgrammedDate fields.
        @param kwargs: data fields for progActivity. See _paCreateActivity() definition in abstract_class_prog_activity.py.
        @return: ProgActivity Object. ERR_ (str) if class is not valid or if recordActivity() finished with errors.

        """
        paClass = cls.getPAClass()
        if activity_id not in paClass.progActivitiesDict().values():
            return f'ERR_INP_InvaLid Argument(s) {activity_id}'  # Actividad no definida o no valida para progActivity.

        tblLinkPA = next((j for j in args if isinstance(j, DataTable) and j.tblName == cls.tblLinkPAName()),
                         DataTable(cls.tblLinkPAName()))
        # 1. Crea Actividad. paClass es ProgActivityAnimal.
        paObj = paClass._paCreateActivity(activity_id, *args, enable_activity=enable_activity, **kwargs)
        itemsDict = items_dict if isinstance(items_dict, dict) else {}
        if not tblLinkPA.dataLen:
            # Si no se pasa tblLinkPA con datos, toma obj: fldProgrammedDate de itemsDict y Valida elementos.
            itemsDict = {k: v for (k, v) in itemsDict.items() if k in cls.getRegisterDict() and isinstance(v, datetime)}
        if isinstance(paObj, paClass) and itemsDict:
            # 2. Create records en tblLinkPA. fldProgrammedDate MUST be supplied in args DataTable.
            for j, k in enumerate(itemsDict):
                # Creates execution instance for object k
                tblLinkPA.setVal(j, fldFK_Actividad=paObj.ID, fldFK=k.ID, fldProgrammedDate=itemsDict[k],
                                 fldComment=f'Object: {k} / Activity: {activity_id}.')
            _ = tblLinkPA.setRecords()          # Crea los records en tblLinkPA
            objList = [o for o in cls.getRegisterDict().values() if o.isActive and o.ID in itemsDict.keys()]

            # 3. Registra progActivity creada con cada uno de los objetos (Bovine, etc.) para los que fue definida
            if objList:
                for o in objList:
                    o.registerProgActivity(paObj)    # myProgActivities: {paObj: __activityID}
                return objList
            return None

    def validateActivity(self, activityName: str):
        """
        Checks whether __activityName is defined for the class of Animal of the caller object
        @param activityName: str. Activity Name to check.
        @return: True: Activity defined for Animal - False: Activity is not defined.
        """
        activityName = activityName.strip()
        if activityName in self.__activitiesForMyClass:
            animalClassID = self.__activitiesForMyClass[activityName]
            if animalClassID == 0 or self.animalClassID == animalClassID:
                return True
        return False

    def setMyTags(self, *args: Tag):
        retValue = []
        if args:
            for i in args:
                if isinstance(i, Tag) and i.isAvailable:
                    self.__myTags.add(i)                        # __myTags is set(tagObject, )
                    self.__identifiers.add(i.ID)                # __identifiers = set(tagID, )
                    i.assignTo(self)                # Stores self object in Tag object structure, for ease of access.
                    self.getAllAssignedTags()[i.ID] = i   # Updates dict {tagID: tagObject, } in Bovine, Caprine, etc.
                    retValue.append(i)
        return retValue

    def popMyTags(self, *args: Tag):
        """
        Removes (deassigns) tags in *args from Tag list of Animal
        @param args: Tag object(s) to remove. '*': -> removes all Tags.
        @return: list of tags removed. Empty list if nothing is removed.
        """
        if args:
            retList = []
            if '*' in args:
                for i in self.__myTags:
                    i.assignTo(None)
                    self.getAllAssignedTags().pop(i.ID, None)  # Updates dict {tagNumber: tagObject, } .
                    retList.append(i)
                self.__myTags.clear()
                self.__identifiers.clear()                      # __identifiers = set(tagID, )
                return retList

            for i in args:
                if i in self.__myTags:
                    self.__myTags.remove(i)
                    self.__identifiers.discard(i.ID)           # __identifiers = set(tagID, )
                    i.assignTo(None)
                    self.getAllAssignedTags().pop(i.ID, None)  # Updates dict {tagID: tagObject, } .
                    retList.append(i)
            return retList
        return []

    @property
    def myTags(self):
        return list(self.__myTags)          # Hay que hacer el set subscriptable

    @property
    def myTagNumbers(self):
        if self.__myTags:
            return [j.tagNumber for j in self.__myTags]
        return []

    @property
    def myTagIDs(self):
        if self.__myTags:
            return [j.ID for j in self.__myTags]
        return []

    @classmethod
    def IDFromTagNum(cls, tagList: str = None):
        """
        Returns an ID from passed Tag Numbers. If more than 1 tag is passed, returns a list of all the matching tags.
        @param tagList: List of Tag numbers (tagNumber attribute, str)
        @return: Dictionary {AnimalObj: [tagObj1, tagObj2, ], }  {}: no matches in Tag RegisterDict.
        """
        tagNumbers = tagList if hasattr(tagList, '__iter__') and not isinstance(tagList, str) else [tagList, ]
        matchingDict = {}
        if tagNumbers:
            cleanTagNumbers = [removeAccents(str(j)) for j in tagNumbers]                # map(removeAccents, tagList)
            matchingIDs = []
            for j in cls.getRegisterDict():                                     # List of matching indices
                indices = [cleanTagNumbers.index(tagNum) for tagNum in cls.getRegisterDict()[j].myTagNumbers]
                if indices:
                    myTags = list(cls.getRegisterDict()[j].myTags)
                    for v in range(len(myTags)):
                        if myTags[v].tagNumber in cleanTagNumbers:
                            matchingIDs.append(cls.getRegisterDict()[j].myTags[v].ID)
                    matchingDict[cls.getRegisterDict()[j]] = [Tag.getRegisterDict()[t] for t in matchingIDs]
                break    # Sale al primer match, pero podria seguir. En teoria, en un sistema integro, hay 1 solo match

        return matchingDict

    @classmethod                    # DB Trigger-related method.
    def processRA(cls):
        """ ******** IMPORTANT: Method enqueued by IntervalTimer.checkTriggerTables() into replicateBuffer ***********
        Checks if Activities from external nodes (uploaded from db ONLY when records are NOT generated by the local
        node) is a closing activity for any of the ProgActivities in the myProgActivities of the assigned objects.
        @return: None
        """
        for c in cls.getAnimalClasses().keys():  # TODO: See that outerObject returns the proper class (Bovine,)
            c.processExecutedActivity()  # Checks if executed Activity is closingActivity for node's ProgActivities.
        return None

    @classmethod                 # DB Trigger-related method.
    def processExecutedActivity(cls):               # cls is Bovine, Caprine, etc (see above processRA).
        """
        Uploads records from [Animales Registro De Actividades] ONLY for the Animal class cls, ONLY for nodes different
        from this node (the local node) and checks if the Activities loaded can be the Closure Activity for
        ProgActivities defined in the local node (for whatever objects those ProgActivities are defined).
        @return:
        """
        # 1. Gets all Activities from RA that are generated by other nodes.
        now = time_mt('datetime')
        # 1 year back from today's date. Can work something more sophisticated later on.
        tblRA = getRecords(cls.tblRAName(), now-timedelta(days=MAX_GOBACK_DAYS), now, 'fldTimeStamp', '*')
        tblRALocal = getRecords(cls.tblRAName(), now-timedelta(days=MAX_GOBACK_DAYS), now, 'fldTimeStamp', '*',
                                fldDB_ID=MAIN_DB_ID)
        if any(not isinstance(t, DataTable) for t in (tblRA, tblRALocal)):
            return None

        # Removes all records already processed (listed in _sys_Processed_Activities table).
        processedRecords = []
        try:        # Pulls records generated by this node (DB_ID=MAIN_DB_ID) and for the RA Table for this class.
            back_date = (now - timedelta(days=MAX_GOBACK_DAYS)).strftime(fDateTime)
            fNames, rows = exec_sql(sql=f'SELECT ID_Actividad FROM _sys_Processed_Activities WHERE '
                                        f'DB_ID=="{MAIN_DB_ID}" AND Table_Name=="{cls.tblRAName()}" '
                                        f'AND TimeStamp>="{back_date}"; ')
        except (sqlite3.Error, sqlite3.OperationalError, sqlite3.DatabaseError):
            pass
        else:
            processedRecords = [j[0] for j in rows if j]
        externalRecs = set(tblRA.getCol('fldID')).difference(tblRALocal.getCol('fldID')) or []
        if processedRecords:
            externalRecs = externalRecs.difference(processedRecords)

        for i in externalRecs:
            # 2.Pulls the Activity obj from _activityObjList using activityID: a full-fledged Activity object,thank God.
            # TODO: Does this logic preclude the use of Generic Activities?? See that they can be used!
            activityID = tblRA.getVal(i, 'fldFK_NombreActividad')
            # Pulls Activity object from existing cls._activityObjList.
            activityObj = next((a for a in cls._activityObjList if a._activityID==activityID), None)
            if activityObj is None:
                continue
            # 3. Get the object(s) for which Activity was executed (a list of 1 or more outerObjects).
            tblLink = getRecords(cls.tblLinkName(), '', '', None, '*', fldFK_Actividad=i)

            uidCol = tblLink.getCol('fldFK') or []
            for j, uid in enumerate(uidCol):
                outer_obj = cls.getRegisterDict().get(uid)
                if not outer_obj:
                    continue
                # 4. Get execution date and other Execute DAta from execute_fields. Also excluded_fields for object j.
                execute_fields = tblLink.getVal(j, 'fldExecuteData')            # This is a dict.
                excluded_fields = tblLink.getVal(j, 'fldExcludedFields') or activityObj._excluded_fields  # It's a list.
                execute_date = execute_fields.get('execution_date') or None
                if not execute_date:
                    continue

                # 5. Run _paMatchAndClose() with all the retrieved data. TODO: When closing_status=None, the function
                # must determine the actual closing status based on execute date and ProgActivity windows
                # IMPORTANT: outer_obj is passed here in kwargs / self.outerObject is NOT set in this function call.
                activityObj._paMatchAndClose(i, outer_obj=outer_obj, execute_fields=execute_fields,
                                             excluded_fields=excluded_fields, closing_status=None, force_close=False)
                # Actualiza tabla _sys_Processed_Activities
                setRecord('_sys_Processed_Activities', fldDB_ID=MAIN_DB_ID, fldFK_Actividad=i, fldTimeStamp=now,
                                                       fldTableName=cls.tblRAName())
        return None

    # ---------------------------------------- Animal Activity Objects ---------------------------------------- #

    # TODO: ver como se puede implementar GenericActivityAnimal para poder inicializar con _creatorActivityObjects()
    # __desteteObj = GenericActivityAnimal('Destete', tbl_data_name='tblDataAnimalesActividadDestete',
    #                                      qry_fld_names='fldDate')
    # @property
    # def weaning(self):  # weaning = destete
    #     self.__desteteObj.outerObject = self
    #     return self.__desteteObj

    __altaObj = AltaActivityAnimal()   # Special object to call AltaActivityAnimal object bounded to a class (cls).
    @classmethod
    def alta(cls, *args, **kwargs):
        """ Interface to run class methods passing cls as outerObject parameter, instead of self."""
        cls.__altaObj.outerObject = cls
        return cls.__altaObj(*args, **kwargs)       # alta() ->executes __call__(*args, **kwargs) in AltaActivityAnimal.

    #       TODO lo de abajo reemplazado por creacion dinamica de objetos Activity en _creatorActivityObjects().
    # __inventoryObj = InventoryActivityAnimal()          # Singleton object.
    # @property
    # def inventory(self):    # En cada llamada se setea el valor del Objecto LLamador para acceder a sus atributos
    #     self.__inventoryObj.outerObject = self   # obj.__inventoryObj.__setattr__('obj.__outerAttr', obj)
    #     return self.__inventoryObj  # Retorna objeto __inventoryObj para acceder metodos en InventoryActivityAnimal

    # __myTagsObj = TagActivityAnimal()                   # Singleton object.
    # @property
    # def tags(self):
    #     self.__myTagsObj.outerObject = self  # __setattr__('obj.__outerAttr', obj)  # Pasa obj. Animal para set, get,
    #     return self.__myTagsObj  # Retorna objeto __myTagsObj para poder acceder metodos en TagActivityAnimal
    #
    # __statusObj = StatusActivityAnimal()                # Singleton object.
    # @property
    # def status(self):  # Llamada que hace Animal para asignar su "obj" al objeto "statusObj" que va a llamar a set,get.
    #     self.__statusObj.outerObject = self   # obj.__statusObj.__setattr__('obj.__outerAttr', obj)
    #     return self.__statusObj  # Retorna objeto __statusObj para poder acceder metodos en clase StatusActivityAnimal
    #
    # __castrationObj = CastrationActivityAnimal()  # Singleton object.
    # @property
    # def castration(self):  # Llamada que hace Animal para asignar su "obj" al objeto "statusObj" llama a set,get.
    #     self.__castrationObj.outerObject = self  # obj.__statusObj.__setattr__('obj.__outerAttr', obj)
    #     return self.__castrationObj  # Retorna objeto __statusObj p/ poder acceder metodos en clase StatusActivityAnimal
    #
    # __applicationObject = Application()                 # Singleton object.
    # @property
    # def application(self):
    #     self.__applicationObject.outerObject = self  # obj.__applicationObject.__setattr__('obj.__outerAttr', obj)
    #     return self.__applicationObject  # Retorna objeto __applicationObject para acceder metodos en ApplicationActiv.
    #
    # __TMObj = TMActivityAnimal()
    # @property
    # def tm(self):
    #     self.__TMObj.outerObject = self # __setattr__('obj.__outerAttr', obj)  # OBJETO para ser usado por set,get,etc.
    #     return self.__TMObj
    #
    # __localizationObject = LocalizationActivityAnimal()
    # @property
    # def localization(self):  # Se necesita pasar el outer Object para que metodos de ActivityLocalization() funcionen
    #     self.__localizationObject.outerObject = self  # OBJETO para ser usado por set, get, etc.
    #     return self.__localizationObject
    #
    # __personObj = PersonActivityAnimal()
    # @property
    # def person(self):
    #     self.__personObj.outerObject = self  # OBJETO para ser usado por set, get, etc.
    #     return self.__personObj
    #

    # -------------------- Bkgd Methods. Run by background threads (mainly, but not necessarily) --------------------- #

    @classmethod
    def updateCategories_bkgd(cls, **kwargs):
        """
        Background method to update category for Bovines. Called by krnl_threading module, executed in a Timer thread.
        Daemon Function.
         @param kwargs: multiplier: True, multiplies time values by SPD (86400) in function category.update()
        @return: List of objects with category changed, or empty list if none changed.
        """
        # TODO(cmt) VERY IMPORTANTE. True/False mapping to SQLite: True->1 ; False->NULL.
        #  Also VERY IMPORTANTE: Must manage re-entry and changes in dictionary size (addition/removal of animals)
        counter = 0
        localRegister = cls.getRegisterDict().copy().values()  # Instant-copy of data structure to manage simultaneous access
        mirrorCategories = {obj: obj.category.get() for obj in localRegister}     # Copia local de categorias iniciales
        # mirrorCategories = cls.getRegisterDict().copy()
        for animalObj in localRegister:               # cls finds the proper __registerDict to fetch
            try:      # TODO(cmt): Contempla si cls.getRegisterDict() cambia (desde otro thread) durante la ejecucion.
                if animalObj.isValid and (animalObj.isRegular or animalObj.isSubstitute):
                    # Importante meter los calls de abajo dentro del try -> Si durante la ejecucion falla debido a que
                    # el foreground modifica la estructura accedida por este call, ignora y sigue con el siguente j.
                    # Actualiza solo si dato de animal en cls.getRegisterDict() == al dato del animal en localRegister
                    # Si son distintos es porque el foreground actualizo categoria y tiene prioridad sobre background.
                    if not mirrorCategories.get(animalObj) == animalObj.category.get():
                        retValue = animalObj.category.compute(**kwargs)  # llama c/ objeto de clase correcta (Bovine,etc)
                        bkgd_logger.info(f'Updated category for animal {animalObj.ID} to {retValue}')
                        counter += 1  # TODO: este retorno provisorio, debugging purposes. ELIMINAR.
            except (AttributeError, TypeError, NameError, ValueError):
                continue    # Va al proximo item de localRegister.
        return counter      # return retValue              # counter = numero de animales procesados.
    updateCategories_bkgd.localRegister = {}

    __dbWriteCounter = 0
    @classmethod
    def updateTimeout_bkgd(cls, **kwargs):
        """
        Background method to update Timeout for Bovines. Called by krnl_threading module, executed in a Timer thread.
        Daemon Function.
        @param kwargs: mutiplier: True, multiplies time values by SPD (86400) in function updateTimeout()
        @return:
        """
        localRegister = cls.getRegisterDict().values()  # Instant-copy of data structure to manage simultaneous access
        counter = 0
        for j in localRegister:            # cls value gets the right RegisterDict in each call
            try:      # TODO(cmt): Contempla si cls.getRegisterDict() cambia (desde otro thread) durante la ejecucion.
                if j.isValid and (j.isRegular or j.isSubstitute):
                    # Importante meter el call de de abajo dentro del try -> Si durante la ejecucion falla debido a que
                    # el foreground modifica la estructura accedida por este call, ignora y sigue con el siguente j.
                    a = j.updateTimeout(**kwargs)  # Calls updateTimeout() in entityObject.Same method for all EO.
                    # cls.__dbWriteCounter += 1
                    # if cls.__dbWriteCounter == 1000:  # TODO: Prueba de escritura en DB desde este thread
                    #     cls.__dbWriteCounter = 0
                    #   # TODO(cmt):Estos writes son solo para code testing.Desde bkdg SIEMPRE usar try/except en writes
                    #     j.inventory.set()       # Escribe en DB
                    #     j.inventory.get()
                    #     print(f' &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&     {moduleName()}({lineNum()}), '
                    #           f'{callerFunction(getCallers=False)}: DB read/write - inventory.set()    &&&&&&&&&&&'
                    #           f'&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')

                    if a:
                        cls.getBkgdAnimalTimeouts().append(a)
                        bkgd_logger.info(f'Timeout for animal {j.ID}: {a[2]} days. Last inventory: {a[1]}')
                        counter += 1  # Retorna cantidad de animales procesados, for debugging/timing purposes.
                        # retValue = True  # Indica que se agregaron items en Timeout a la lista
            except (AttributeError, TypeError, NameError, ValueError):
                continue    # Va al proximo item de localRegister.

        # return retValue
        return counter

    updateTimeout_bkgd.localRegister = {}

# =============================================== End class Animal ================================================== #


# =================================================================================================================== #


class Mammal(Animal):
    __objClass = 4
    __objType = 1

    def __init__(self, ID, isValid, isActive, *args, **kwargs):
        super().__init__(ID, isValid, isActive, *args, **kwargs)


class Birds(Animal):
    __objClass = 6
    __objType = 1

    def __init__(self, ID, isValid, isActive, *args, **kwargs):
        super().__init__(ID, isValid, isActive, *args, **kwargs)

