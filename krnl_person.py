from krnl_entityObject import EntityObject
import re
from krnl_config import fDateTime, callerFunction, lineNum, removeAccents
from datetime import datetime
from krnl_custom_types import getRecords, DataTable
from krnl_abstract_class_prog_activity import ProgActivity

SYSTEM_PERSON_ID = 1


class Person(EntityObject):                                # Personas
    # __objClass = 30
    # __objType = 1

    # Variables para logica de manejo de objetos repetidos/duplicados.
    _fldID_list = []  # List of all active records pulled by getRecords() from tblAnimales.
    # _new_local_fldIDs = []  # UID list for records added tblAnimales by local application.
    _fldUPDATE_counter = 0  # Monotonic counter to detect and manage record UPDATEs.

    __activitiesDict = {}         # {NombreActividad: ID_Actividad, }
    __registerDict = {}           # {personID: personObj, } ONLY ACTIVE PERSONS
    __activeProgActivities = []     # List of programmed activities active for Person objects. MUST BE a list.

    _active_uids_dict = {}  # {fldObjectUID: fld_Duplication_Index}
    _active_duplication_index_dict = {}  # {fld_Duplication_Index: set(fldObjectUID, dupl_uid1, dupl_uid2, ), }

    __tblDataStatusName = 'tblDataPersonasStatus'
    __tblObjName = 'tblPersonas'
    __tblRAName = 'tblPersonasRegistroDeActividades'
    __tblLinkName = 'tblLinkPersonasActividades'
    __tblDataLocalizationName = 'tblDataPersonasLocalizacion'
    __tblLinkPAName = 'tblLinkPersonasActividadesProgramadas'       # Usado en object_instantiation.loadItemsFromDB()

    @classmethod
    def getRegisterDict(cls):
        return cls.__registerDict

    @classmethod
    def tblObjName(cls):
        return cls.__tblObjName

    @classmethod
    def tblRAName(cls):
        return cls.__tblRAName

    @classmethod
    def tblLinkName(cls):
        return cls.__tblLinkName

    @property
    def tblDataStatusName(self):
        return self.__tblDataStatusName

    @classmethod
    def tblLinkPAName(cls):
        return cls.__tblLinkPAName

    # __genericPersonID = SYSTEM_PERSON_ID
    #
    # @staticmethod
    # def getGenericObjectID():
    #     return Person.__genericPersonID

    def __init__(self, *args, **kwargs):
        myID = kwargs.get('fldObjectUID')
        if myID is None:
            isValid = isActive = False
        else:

            isValid = True
            isActive = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('active')), 1)
            isActive = 0 if isActive in (0, None, False) else 1

        # LEVEL -> 1: Personas que operan en el Sistema - 2: Instituciones. NO operan en el sistema
        self.__recordID = kwargs.get('fldID', None)
        self.__level = next((int(kwargs[j]) for j in kwargs if str(j).lower().__contains__('personlevel')), 1)
        self.__personType = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('persontype')), 0)    # 0: Fisica - 1: Juridica
        self.__name = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('fldname')), None)
        self.__lastName = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('lastname')), None)
        self.__dob = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('dob')), None)
        self.__comment = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('comment')), '')
        self.__timeStamp = kwargs.get('fldTimeStamp', None)    # Esta fecha se usa para gestionar objetos repetidos.
        # Careful: __personIDNumbers is type str!!
        self.__personIDNumbers = []  # List of all IDs assigned to the person: Passport #, Driver's License #, etc.

        exitDate = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('dateexit') or
                         str(j).lower().__contains__('exitdate')), None)  # DBKeyName: fldDateExit
        if exitDate:
            try:
                self.__exitDate = datetime.strptime(exitDate, fDateTime)
            except(TypeError, AttributeError, NameError):
                self.__exitDate = 1         # Si no es fecha valida -> Tiene salida pero no se sabe la fecha de salida.
            isActive = False  # Tiene salida per no se sabe fecha
        else:
            self.__exitDate = None
        super().__init__(myID, isValid, isActive, *args, **kwargs)

        self.__myProgActivities = {}

    def register(self):  # NO HAY CHEQUEOS. obj debe ser valido
        try:
            self.__registerDict[self.getID] = self
            retValue = self
        except (NameError, KeyError, ValueError, TypeError):
            retValue = False
        return retValue

    def unRegister(self):  # Remueve obj de .__registerDict   NO HAY CHEQUEOS. obj debe ser valido
        try:
            self._fldID_list.remove(self.recordID)      # removes fldID from _fldID_list to keep integrity of structure.
        except ValueError:
            pass
        return self.__registerDict.pop(self.getID, False)  # Retorna False si no encuentra el objeto


    def __repr__(self):                 # self.getCategories() gets the right __categories dictionary.
        return "[{} {} - Level:{}; Person type: {}".format(self.name, self.__lastName, self.level, self.__personType)

    @property
    def getElements(self):  # Retorna los ultimos valores almacenados en cada atributo
        return {'fldID': self.ID, 'fldName': self.__name, 'fldLastName': self.__lastName, 'fldDOB': self.__dob,
                'fldPersonLevel': self.__level, 'fldPersonType': self.__personType, 'fldDateExit': self.__exitDate,
                'fldComment': self.__comment}

    @property
    def name(self):
        return self.name

    @property
    def personID(self):
        return self.__personIDNumbers

    @property
    def recordID(self):
        return self.__recordID


    def updateAttributes(self, **kwargs):
        """ Updates object attributes with values passed in attr_dict. Values not passed leave that attribute unchanged.
        @return: None
        """
        if not kwargs:
            return None
        # TODO: IMPLEMENT THE UPDATE of attributes relevant to Person


    @property
    def myTimeStamp(self):  # impractical name to avoid conflicts with the many date, timeStamp, fldDate in the code.
        """
        returns record creation timestamp as a datetime object
        @return: datetime object Event Date.
        """
        return self.__timeStamp

    @classmethod
    def getObject(cls, name: str):
        """ Returns Person object associated to name.
        Name can be a UUID, one of the person's registered IDs, or the Person's name. In case multiple names match,
        a list of all matches found is returned.
        @return: Person object if any found, None if name not found in getGeoEntities dict. List if more than 1 match.
        """
        try:
            o = cls.getRegisterDict().get(name, None)               # 1. checks if it's UUID.
        except SyntaxError:
            return None
        else:
            if o:
                return o
            else:
                o = next((cls.getRegisterDict()[k] for k in cls.getRegisterDict()
                          if name in cls.getRegisterDict()[k].personID), None)      # 2. Checks if it's person's ID.
                if o:
                    return o

                name = re.sub(r'[-\\|/@#$%^*()=+¿?{}"\'<>,:;_]', ' ', name)         # 3. Checks for person's name.
                name_words = [j for j in removeAccents(name).split(" ") if j]
                retList = []
                for k in cls.getRegisterDict():
                    if all(word in removeAccents(cls.getRegisterDict()[k].name) for word in name_words):
                        retList.append(cls.getRegisterDict()[k])
                return retList
                # return next((cls.getRegisterDict()[k] for k in cls.getRegisterDict() if
                #              all(word in removeAccents(cls.getRegisterDict()[k].name) for word in name_words)), None)

    @property
    def level(self):
        return self.__level

    @level.setter
    def level(self, val):
        if val in (1, 2):
            self.__level = val      # Solo setea si val es valido. Si no, deja el valor existente.

    @property
    def personNames(self):
        return {'names': self.__name, 'lastname': self.__lastName}

        # isValid, isActive, isRegistered=register.get, getID, getPersonData (Name,LastName,dob,etc),
        # getLevel (Usar el metodo getPersonData y filtrar level), status (get/set), localization (get/set),

    @property
    def activities(self):  # @property activities es necesario para las llamadas a obj.outerObject.
        return self.__activitiesDict

    @property
    def exitYN(self):
        try:
            return self.__exitDate
        except AttributeError:
            return None

    @property
    def myProgActivities(self):
        """ Returns dict of ProgActivities assigned to object """
        return self.__myProgActivities  # Dict {paObject: __activityID}

    def registerProgActivity(self, obj: ProgActivity):
        if isinstance(obj, ProgActivity) and obj.isActive > 0:
            if obj not in self.__myProgActivities:
                self.__myProgActivities[obj] = obj.activityID  # Dict {paObject: __activityID}
                self.__activeProgActivities.append(obj)  # set {paObj, }. ALWAYS appends for this to work.

    def unregisterProgActivity(self, obj: ProgActivity):
        if isinstance(obj, ProgActivity) and obj in self.__myProgActivities:  # and obj.isActive < 2:
            self.__myProgActivities.pop(obj, None)  # Dict {paObject: __activityID, }
            try:
                return self.__activeProgActivities.remove(obj)  # List [paObj, ].  Must remove().
            except ValueError:
                return None


    @classmethod
    def getActivitiesDict(cls):
        return cls.__activitiesDict

    @classmethod
    # @timerWrapper(iterations=5)
    def processReplicated(cls):
        """             ******  Run periodically as IntervalTimer func. ******
                        ******  IMPORTANT: This code should execute in LESS than 5 msec (switchinterval).      ******
        Used to execute logic for detection and management of duplicate objects.
        Defined for Animal, Tag, Person. (Geo has its own function on TransactionalObject class).
        Checks for additions to tblAnimales from external sources (db replication) for Valid and Active objects.
        Updates _table_record_count.
        @return: True if update operation succeeds, False if reading tblAnimales from db fails.
        """
        temp0 = getRecords(cls.tblObjName(), '', '', None, '*', fldDateExit=0)
        if not isinstance(temp0, DataTable):
            return False

        del temp0
        return True


    @staticmethod
    def getPersonLevels(activeArg=None, levelArg=None):
        """
        Returns Dictionary of Persons matching active/level passed as args.
        @param levelArg: 1,2: Levels; 0, '', None=ALL
        @return: Dictionary of the form {idPerson: personLevel,}
        """
        tblObjectName = 'tblPersonas'
        level = levelArg if levelArg in [1, 2] else [1, 2]
        active = activeArg if activeArg else None
        temp = getRecords(tblObjectName, '', '', None, '*', fldPersonLevel=level)
        retDict = {}
        for j in range(temp.dataLen):
            tempRecord = temp.unpackItem(j)
            if len(tempRecord) > 0:
                if not active:
                    if tempRecord['fldDateExit'] != '':
                        if not active:
                            retDict[tempRecord['fldID']] = tempRecord['fldPersonLevel']
                    else:
                        if not active or active == 0:
                            retDict[tempRecord['fldID']] = tempRecord['fldPersonLevel']
        return retDict


# Initialize PersonActivityAnimal Objects
# if not Person.getRegisterDict():
temp = getRecords('tblPersonas', '', '', None, '*')
if type(temp) is str:
    pass
elif not temp.dataLen:
    pass
else:
    for i in range(temp.dataLen):
        pDict = temp.unpackItem(i)
        if pDict:
            _ = Person(**pDict)
            if _.ID not in Person.getRegisterDict():
                _.register()   # Esto es inicializacion. Tiene prioridad un person ID ya existente en __registerDict
