import sqlite3
import re
from krnl_config import sessionActiveUser, callerFunction, time_mt, lineNum, removeAccents, os, krnl_logger
from custom_types import getRecords, setRecord, delRecord, DataTable
from uuid import uuid4, UUID
from krnl_exceptions import DBAccessError
from krnl_transactionalObject import TransactionalObject

def moduleName():
    return str(os.path.basename(__file__))

def adapt_geo_to_UID(obj):
    """ 'Serializes' a Geo object to its ID value (integer) for storage as int in the DB (in a DB field of type GEO)."""
    if isinstance(obj, Geo):
        try:
            return obj.ID       # obj.ID es UUID (type str).
        except (AttributeError, NameError, ValueError):
            raise sqlite3.Error(f'Conversion error: {obj} is not a valid Geo object.')
    return obj

def convert_to_geo(val):
    """ Converts UUID from a DB column of name GEOTEXT to a Geo Object, returning the obj found in __registerDict[val]
     GEOTEXT column has TEXT affinity, but it doesn't return a string. Must decode() value received. """
    try:
        ret = Geo.getGeoEntities().get(val.decode('utf-8'), None)   # OJO:sqlite3 pasa val como byte. Must decode()!
    except (TypeError, ValueError, AttributeError):
        # raise sqlite3.Error(f'ERR_Conversion error: {val} Geo Entity not registered or {val} is of invalid type.')
        krnl_logger.warning(f'ERR_Conversion error: {val} Geo Entity not registered or {val} is of invalid type.')
        return val
    else:
        return ret if ret is not None else val    # if key is not found, returns val for processing by the application.


class Geo(TransactionalObject):
    # __objClass = 102
    # __objType = 2
    __tblEntitiesName = 'tblGeoEntidades'
    __tblContainingEntitiesName = 'tblGeoEntidadContainer'
    __tblEntityTypesName = 'tblGeoTiposDeEntidad'
    __tblLocalizationLevelsName = 'tblGeoNivelesDeLocalizacion'
    __tblObjectsName = __tblEntitiesName
    __registerDict = {}         # {fldObjectUID: geoObj, }

    def __call__(self, caller_object=None, *args, **kwargs):        # Not sure this is required here.
        """
        @param caller_object: instance of Bovine, Caprine, etc., that invokes the Activity
        @param args:
        @param kwargs:
        @return: Activity object invoking __call__()
        """
        # item_obj=None above is important to allow to call fget() like that, without having to pass parameters.
        # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {(item_object, *args)}; kwargs: {kwargs}')
        self.outerObject = caller_object  # item_object es instance de Animal, Tag, etc. NO PUEDE SER class por ahora.
        return self


    # Variables para logica de manejo de objetos repetidos/duplicados.
    _fldID_list = []  # List of all active records pulled by getRecords() from tblAnimales.
    # _new_local_fldIDs = []  # UID list for records added tblAnimales by local application.
    _fldUPDATE_counter = 0  # Monotonic counter to detect and manage record UPDATEs.


    @classmethod
    def getGeoEntities(cls):
        return cls.__registerDict       # {fldObjectUID: geoObj, }

    @classmethod
    def getObjectTblName(cls):
        return cls.__tblObjectsName

    temp1 = getRecords(__tblLocalizationLevelsName, '', '', None, 'fldID', 'fldName', 'fldNivelDeLocalizacion',
                         'fldGeoLocalizationActive', 'fldFlag_EntidadEstado', 'fldNivelContainerMandatorio')
    __localizationLevelsDict = {}
    for j in range(temp1.dataLen):
        # {localizLevelName: [localizLevel, localizLevelActive, Entidad Estado YN, Container Mandatorio, tipoEntidad], }
        __localizationLevelsDict[removeAccents(temp1.dataList[j][1])] = [temp1.dataList[j][2], temp1.dataList[j][3],
                                                                         temp1.dataList[j][4], temp1.dataList[j][5],
                                                                         temp1.dataList[j][0]]

    @classmethod
    def tblObjName(cls):
        return cls.__tblObjectsName

    @classmethod
    def getLocalizLevelsDict(cls):
        return cls.__localizationLevelsDict

    def __init__(self, *args, **kwargs):                    # Falta terminar este constructor
        self.__ID = str(kwargs.get('fldObjectUID'))
        try:
            _ = UUID(self.__ID)                         # Validacion de valor en columna fldObjectUID leida de db.
        except(ValueError, TypeError, AttributeError):
            raise TypeError(f'ERR_INP_TypeError: Invalid/malformed UID {self.__ID}. Geo object cannot be created.')
        self.__isValid = True
        self.__recordID = kwargs.get('fldID')       # Useful to access the record directly. NOT expected to change!!
        self.__isActive = kwargs.get('fldFlag', 1)
        self.__name = kwargs.get('fldName')
        self.__containers = kwargs.get('fldContainers', [])  # getting fldContainers from JSON field: Geo objects.

        if not self.__containers or not isinstance(self.__containers, (tuple, list, str)):
            self.__containers = []
        elif isinstance(self.__containers, str):
            self.__containers = [self.__containers, ]  # Si str no es convertible a str -> Exception y a corregir.
        else:
            self.__containers = list(self.__containers)
        self.__container_tree = []    # Lista de objetos Geo asociados a los IDs de __containers.
        self.__localizationLevel = kwargs.get('fldFK_NivelDeLocalizacion')
        self.__abbreviation = kwargs.get('fldAbbreviation', '')
        self.__entityType = kwargs.get('fldFK_TipoDeEntidad')   # int Code for Pais, Provincia, Establecimiento, etc.
        # self.__country = kwargs.get('fldFK_Pais')
        # self.__farmstead = kwargs.get('fldFK_Establecimiento', '')          # farmstead = Establecimiento
        self.__isStateEntity = kwargs.get('fldFlag_EntidadEstado', 0)
        self.__area = kwargs.get('fldArea')
        self.__areaUnits = kwargs.get('fldFK_Unidad', 11)           # 11: Hectarea.
        super().__init__()

    @property
    def ID(self):
        return self.__ID

    @property
    def recordID(self):
        return self.__recordID

    @property
    def name(self):
        return self.__name

    @classmethod
    def getName(cls, name: str):
        n = removeAccents(name)
        namesDict = {k: cls.getGeoEntities()[k].name for k in cls.getGeoEntities() if n in
                     removeAccents(cls.getGeoEntities()[k].name)}
        return namesDict

    @classmethod
    def getUID(cls, name: str):
        name = re.sub(r'[-\\|/@#$%^*()=+¿?{}"\'<>,:;_]', ' ', name)
        name_words = [j for j in removeAccents(name).split(" ") if j]
        return next((k for k in cls.getGeoEntities()
                     if all(word in removeAccents(cls.getGeoEntities()[k].name) for word in name_words)), None)

        # n = removeAccents(name)
        # return next((k for k in cls.getGeoEntities() if n == removeAccents(cls.getGeoEntities()[k].name)), None)

    @classmethod
    def getObject(cls, name: str):
        """ Returns the Geo object associated to name.
        Name can be a UUID or a regular ("human") string name ('El Palmar - Lote 2', '9 de Julio', 'santa fe', etc.).
        @return: Geo object if any found, None if name not found in getGeoEntities dict.
        """
        try:
            o = cls.getGeoEntities().get(name, None)            # Primero busca un UUID y lo retorna si existe.
        except SyntaxError:
            return None
        if not o:
            name = re.sub(r'[-\\|/@#$%^*()=+¿?{}"\'<>,:;_]', ' ', name)
            name_words = [j for j in removeAccents(name).split(" ") if j]
            return next((cls.getGeoEntities()[k] for k in cls.getGeoEntities()
                         if all(word in removeAccents(cls.getGeoEntities()[k].name) for word in name_words)), None)
        return o


    @classmethod
    def getObject00(cls, name: str):
        """ Returns the Geo object associated to name. None if name not found in getGeoEntities dict. """
        n_list = removeAccents(name)
        return next((cls.getGeoEntities()[k] for k in cls.getGeoEntities()
                     if n_list == removeAccents(cls.getGeoEntities()[k].name)), None)

    @classmethod
    def _initialize(cls):
        return cls.loadGeoEntities()
        # return ret

    @classmethod
    def loadGeoEntities(cls):
        """ This method is run in TransactionalObject.__init_subclass__(). So class is not fully initialized yet!! """
        temp1 = getRecords(cls.tblObjName(), '', '', None, '*', fldFlag=1)  # Carga solo GeoEntidades activas.
        if isinstance(temp1, str):
            retValue = f'ERR_DBAccess: Cannot load Geography table. Error: {temp1}'
            krnl_logger.error(retValue)
            raise DBAccessError(retValue)

        for j in range(temp1.dataLen):
            tempDict = temp1.unpackItem(j)
            geoID = tempDict.get('fldObjectUID', '')  # Since fldObjectUID has TEXT affinity, converts fld value to str.
            if geoID:
                cls.getGeoEntities()[geoID] = cls(**tempDict)       # Creates Geo object and adds to __registerDict.

        # Hay que usar loops for separados para inicializar TUTTO el __registerDict
        for entity in cls.getGeoEntities().values():
            container_list = entity.__containers    # __containers aqui es una lista de UID(str).
            if container_list:       # TODO(cmt): Convierte elementos de __containers de UID(str) a Geo.
                entity.__containers = [cls.getGeoEntities()[j] for j in container_list]  # if j in cls.getGeoEntities()
                entity.__container_tree = entity.__containers.copy()  # copy: cannot _generate_container_tree() here!!

            # Initializes the full_uid_list. Used to drive the duplicate detection logic.
        cls._fldID_list = set(g.recordID for g in cls.getGeoEntities().values())
        # Initializes _object_fldUPDATE_dict={fldID: fldUPDATE(dictionary), }. Used to process records UPDATEd by other nodes.
        cls._fldUPDATE_dict = {g.recordID: temp1.getVal('fldUPDATE', fldID=g.recordID) for g in
                               cls.getGeoEntities().values()}

        return True

    def _generate_container_tree(self):
        """Creates a list of GEO objects which are all containers for self. """
        aux_container = self.__containers
        for j in aux_container:
            self.__container_tree.extend(self._iter_containers(j))
            self.__container_tree.append(j)
        self.__container_tree.append(self)   # TODO(cmt): self por def. esta contenido en self -> Agrega al tree.
        self.__container_tree = list(set(self.__container_tree))
        self.__container_tree.sort(key=lambda x: x.localizLevel, reverse=True)  # Ordena por localizLevel decr.
        # print(f'--- Tree for geoObject {self.name}({self.ID}): {[t.name for t in self.__container_tree]}',
        #       dismiss_print=DISMISS_PRINT)
        return self.__container_tree

    @staticmethod
    def _iter_containers(geo_obj):  # geo_obj DEBE ser objeto Geo.
        """ Iterator to build container tree for each Geo object. """
        for item in geo_obj.__containers:
            if isinstance(item, (tuple, list)):         # 1. Procesa lista de objetos Geo.
                for obj in item:
                    if isinstance(obj, Geo):
                        yield obj
                    for loc in geo_obj._iter_containers(obj):       # Procesa elementos (loc) de una lista (obj)
                        if isinstance(loc, Geo):
                            yield loc
            else:
                yield item                              # 2. Procesa objetos Geo individuales
                for obj in geo_obj._iter_containers(item):
                    if isinstance(obj, Geo):
                        # print(f'obj is: {o.name}')
                        yield obj

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
        temp = getRecords(cls.tblObjName(), '', '', None, '*', fldDateExit=0)
        if not isinstance(temp, DataTable):
            return False
        return True


    @property
    def localizLevel(self):
        return self.__localizationLevel

    @property
    def containers(self):
        """ Returns tuple with Geo objects IDs. All containers for self. """
        return self.__containers

    @containers.setter
    def containers(self, item):
        self.__containers = item            # Careful!: Item can by anything...

    @property
    def container_tree(self):
        return self.__container_tree

    @property
    def entityType(self):
        return self.__entityType

    def contains(self, entity=None):
        """ True if self is a container for entity. That is, entity is a part of and is included in self."""
        try:
            return self in entity.container_tree
        except (AttributeError, TypeError, NameError):
            return False

    def contained_in(self, entity=None):
        """ True if entity is a container for self. That is, self is included in entity. """
        try:
            return entity in self.container_tree
        except TypeError:
            return False

    def comp(self, val):            # Method to be used to run comparisons in ProgActivities methods.
        """ Acepta como input Geo object o str (UUID). """
        if isinstance(val, str):
            val = self.getObject(val)
        return self.contained_in(val) if isinstance(val, Geo) else False


    @property
    def isStateEntity(self):
        return self.__isStateEntity

    @property
    def area(self):
        return self.__area, self.__areaUnits

    @property
    def isActive(self):
        return self.__isActive

    def register(self):  # NO HAY CHEQUEOS. obj debe ser valido
        self.__registerDict[self.ID] = self  # TODO: Design criteria: Si se repite key, sobreescribe con nuevo valor
        return self

    def unRegister(self):
        """
        Removes object from __registerDict
        @return: removed object if successful. None if fails.
        """
        return self.__registerDict.pop(self.ID, None)  # Retorna False si no encuentra el objeto


    @classmethod
    def createGeoEntity(cls, *args, name=None, entity_type: str = '',  containers=(), abbrev=None, state_entity=False,
                        **kwargs):
        """   For now, creates anything. TODO: Implement integrity checks for State Entities (country, province, etc.)
        Creates a geo Element (Country, Province, Establecimiento, Potrero, Location, etc). Adds it to the DB.
         @param state_entity: Flag True/False
         @param name: Entity Name. Mandatory.
        @param abbrev: Name abbreviation. Not mandatory.
         @param entity_type: Country, Province/State, Department, Establecimiento, Potrero, etc.
        @param containers: list. Geo Entitities objects that contain the new entity (1 or more).
        At least the MANDATORY container entity(es) must be provided.
        @return: geoObj (Geo object) or errorCode (str)
        """
        tblEntity = next((j for j in args if j.tblName == cls.__tblObjectsName), None)
        tblEntity = tblEntity if tblEntity is not None else DataTable(cls.__tblObjectsName, *args, **kwargs)
        entity_type_id = tblEntity.getVal(0, 'fldFK_TipoDeEntidad', None)
        if entity_type_id is None and removeAccents(entity_type) not in cls.getLocalizLevelsDict():
            retValue = f'ERR_INP_Invalid Argument Entity Type: {entity_type}'
            krnl_logger.info(retValue)
            return retValue
        else:
            entity_type_from_tbl = next((k for k in cls.getLocalizLevelsDict()
                                         if cls.getLocalizLevelsDict()[k] == entity_type_id), None)
            entity_type = entity_type_from_tbl if entity_type_from_tbl is not None else removeAccents(entity_type)
            entity_type_id= entity_type_id if entity_type_id is not None else cls.getLocalizLevelsDict()[entity_type][4]
            entity_type_level = cls.getLocalizLevelsDict()[entity_type][0]
            entity_mandatory_level = cls.getLocalizLevelsDict()[entity_type][3]
            state_entity= bool(state_entity) if state_entity is not None else cls.getLocalizLevelsDict()[entity_type][2]
        containers_from_tbl = tblEntity.getVal(0, 'fldContainers', None)
        if containers_from_tbl is not None:
            containers = containers_from_tbl
        if isinstance(containers, Geo):
            containers = [containers, ]
        elif not isinstance(containers, (list, tuple)):
            retValue = f'ERR_INP_Invalid Arguments. Mandatory container entities not valid or missing.'
            krnl_logger.info(retValue)
            return retValue

        # filtra solo containers que sean Geo y con localizLevel apropiado
        containers = [j for j in containers if isinstance(j, Geo) and j.localizLevel < entity_type_level]
        full_list = []
        for e in containers:
            full_list.extend(e.container_tree)
        full_list_localiz_level = [j.localizLevel for j in list(set(full_list))]  # Lista de localizLevel del Tree.
        if not full_list_localiz_level or all(j > entity_mandatory_level for j in full_list_localiz_level):
            retValue = f'ERR_INP_Invalid Arguments. Mandatory container entities not valid or missing.'
            krnl_logger.info(retValue)
            return retValue          # Sale si no se paso Mandatory Container requerido para ese localizLevel.

        name_from_tbl = tblEntity.getVal(0, 'fldName', None)
        name = name_from_tbl if name_from_tbl is not None else name
        name = removeAccents(name, str_to_lower=False)      # Remueve acentos, mantiene mayusculas.
        abbrev = abbrev if abbrev is not None else tblEntity.getVal(0, 'fldAbbreviation', None)
        abbrev = abbrev.strip().upper() if isinstance(abbrev, str) else None
        timeStamp = tblEntity.getVal(0, 'fldTimeStamp', None) or time_mt('datetime')
        user = tblEntity.getVal(0, 'fldFK_UserID', None) or sessionActiveUser
        containers_ids = [j.ID for j in containers]
        tblEntity.setVal(0, fldTimeStamp=timeStamp, fldFK_NivelDeLocalizacion=entity_type_level, fldName=name,fldFlag=1,
                         fldObjectUID=str(uuid4().hex), fldAbbreviation=abbrev, fldFlag_EntidadEstado=state_entity,
                         fldContainers=containers_ids, fldFK_TipoDeEntidad=entity_type_id, fldFK_UserID=user)
        idRecordGeo = setRecord(tblEntity.tblName, **tblEntity.unpackItem(0))
        if isinstance(idRecordGeo, str):
            retValue = f'ERR_DBAccess: Could not write to table {tblEntity.tblName}. Geo Entity not created.'
            krnl_logger.info(retValue)
            return retValue

        tblEntity.setVal(0, fldID=idRecordGeo, fldContainers=containers)        # Must use Geo Objects for containers.
        entityObj = cls(**tblEntity.unpackItem(0))      # Crea objeto Geo y lo registra.
        entityObj._generate_container_tree()
        entityObj.register()
        return entityObj


    def removeGeoEntity(self):
        """ Removes from __registerDict and sets as Inactive in datbase (for now, to avoid deleting records)
        Sets datetime of 'removal' in db record's fldComment
        """
        self.unRegister()
        return setRecord(self.__tblObjectsName, fldID=self.__recordID, fldFlag=0, fldComment=f'Record set to Inactive '
                                                                                  f'on {time_mt("datetime")}')

# --------------------------------------- Hasta aqui lo util. Revisar que sirve de lo de abajo. --------------------- #


    # @classmethod
    # def geoEntitiesFromDB(cls, *args):
    #     """
    #     Returns dictionary with Geo Entities from DB. Read from DB everytime. DO NOT store this in memory.
    #     @param args: list of idEntities (int) to return info for. if None, returns ALL records in table [Geo Entidades]
    #     @return: {entityID: {fldName: fldValue, }, }
    #     """
    #     argsParsed = [i for i in args if isinstance(i, int) and i > 0]
    #     if argsParsed:
    #         temp = getRecords(cls.__tblEntitiesName, '', '', None, '*', fldID=argsParsed)
    #     else:
    #         temp = getRecords(cls.__tblEntitiesName, '', '', None, '*')      # Si no hay args, retorna toda la tabla
    #     if type(temp) is str:
    #         retValue = f'ERR_INP_InvalidArgument: {temp}. {cls.__tblEntitiesName} - {callerFunction()}'
    #         krnl_logger.info(retValue)
    #         print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
    #     else:
    #         retDict = {}
    #         for j in range(temp.dataLen):
    #             record = temp.unpackItem(j)
    #             idRecord = record.pop('fldID', False)   # Saca fldID del Diccionario "interno"
    #             if idRecord is not False:
    #                 retDict[idRecord] = record
    #         retValue = retDict
    #     return retValue





# ---------------------------------------------- End Class Geo --------------------------------------------------- #
# Complete initialization of Geo objects and data interfacess. Create Container Trees for Geo objects.
# TODO: Got to do this here. Cannot do in loadFromDB()!!.
for o in Geo.getGeoEntities().values():
    o._generate_container_tree()

# These adapter / converter are used for 'ID_Localizacion' fields across the DB. NOT USED WITH THE Geo Tables, though..
sqlite3.register_adapter(Geo, adapt_geo_to_UID)    # Serializes Geo to int, to store in DB.
sqlite3.register_converter('GEOTEXT', convert_to_geo)  # Converts int to a Geo object querying getGeoEntities() by UUID.
krnl_logger.info('Geo structure initialized successfully.\n')
# ---------------------------------------------------------------------------------------------------------------- #
