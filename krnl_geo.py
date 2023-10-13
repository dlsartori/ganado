from krnl_config import fDateTime, strError, sessionActiveUser, callerFunction, time_mt, \
    lineNum, valiDate, removeAccents, os, singleton, krnl_logger, print, DISMISS_PRINT
from custom_types import setupArgs, getRecords, setRecord, delRecord
from krnl_transactionalObject import TransactionalObject
from custom_types import DataTable

def moduleName():
    return str(os.path.basename(__file__))

# @singleton
class Geo(TransactionalObject):
    __objClass = 102
    __objType = 2
    __tblEntitiesName = 'tblGeoEntidades'
    __tblContainingEntitiesName = 'tblGeoEntidadContainer'
    __tblEntityTypesName = 'tblGeoTiposDeEntidad'
    __tblLocalizationLevelsName = 'tblGeoNivelesDeLocalizacion'
    __tblObjectsName = __tblEntitiesName

    @classmethod
    def tblObjName(cls):
        return cls.__tblObjectsName

    _geoEntitiesDict = {}          # {entityID: {}, }

    @classmethod
    def getGeoEntities(cls):
        return cls._geoEntitiesDict

    # TODO: Revisar y actualizar utilizacion de estos 3 dicts de abajo.
    temp = getRecords(__tblEntityTypesName, '', '', None, 'fldID', 'fldName', 'fldRequiredEntityType',
                         'fldMultipleContainersList')
    __entityTypesDict = {}
    for j in range(temp.dataLen):
        __entityTypesDict[temp.dataList[j][1]] = [temp.dataList[j][0], temp.dataList[j][2], temp.dataList[j][3]]
        # diccionario de forma {[NombreTipoDeEntidad: [EntityType_ID, requiredEntityType, fldMultipleContainersList]}

    temp = getRecords(__tblLocalizationLevelsName, '', '', None, 'fldID', 'fldName', 'fldGeoLocalizationOrder',
                         'fldGeoLocalizationActive')
    __localizationLevelsDict = {}
    for j in range(temp.dataLen):
        # {localizLevelName: [fldID, localizLevelOrder, localizLevelActive], }
        __localizationLevelsDict[temp.dataList[j][1]] = [temp.dataList[j][0], temp.dataList[j][2], temp.dataList[j][3]]

    def __init__(self, entity=None, *args, **kwargs):                    # Falta terminar este constructor
        self.__isValid = True
        self._ID = entity
        super().__init__()

    @property
    def ID(self):
        return self._ID

    @classmethod
    def initialize(cls):
        return cls.loadGeoEntities()

    @classmethod
    def loadGeoEntities(cls):
        temp1 = getRecords(cls.tblObjName(), '', '', None, '*', fldFlag=1)  # Carga solo GeoEntidades activas.
        if isinstance(temp1, str):
            retValue = f'ERR_DBAccess: Cannot load Geography table.'
            krnl_logger.error(retValue)
            print(retValue)
            return retValue

        for j in range(temp1.dataLen):
            tempDict = temp1.unpackItem(j)
            # TODO(cmt): Hay que re-convertir los keys de fldContainerTree a int (convertidos a str por el JSON Encoder)
            # Solucionado con hook para json.loads(): intercepta todos los dicts y convierte keys a int cuando se puede
            cls._geoEntitiesDict[tempDict.pop('fldID')] = tempDict
        return True


    @classmethod
    def getEntityTypesDict(cls):
        """
        Returns Dictionary with Entity Types Names and IDs
        @return: {Entity Name: Entity Type ID, }
        """
        return cls.__entityTypesDict

    @classmethod
    def geoEntities(cls, *args):
        """
        Returns dictionary with Geo Entities from DB. Read from DB everytime. DO NOT store this in memory.
        @param args: list of idEntities (int) to return info for. if None, returns ALL records in table [Geo Entidades]
        @return: {entityID: {fldName: fldValue, }, }
        """
        argsParsed = [i for i in args if isinstance(i, int) and i > 0]
        if argsParsed:
            temp = getRecords(cls.__tblEntitiesName, '', '', None, '*', fldID=argsParsed)
        else:
            temp = getRecords(cls.__tblEntitiesName, '', '', None, '*')      # Si no hay args, retorna toda la tabla
        if type(temp) is str:
            retValue = f'ERR_INP_InvalidArgument: {temp}. {cls.__tblEntitiesName} - {callerFunction()}'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
        else:
            retDict = {}
            for j in range(temp.dataLen):
                record = temp.unpackItem(j)
                idRecord = record.pop('fldID', False)   # Saca fldID del Diccionario "interno"
                if idRecord is not False:
                    retDict[idRecord] = record
            retValue = retDict
        return retValue

    __dataFieldMap = {'name': 'fldName', 'abbreviation': 'fldAbbreviation', 'level': 'fldFK_NivelDeLocalizacion',
                      'country': 'fldFK_Pais', 'establecimiento': 'fldFK_Establecimiento',
                      'ranch': 'fldFK_Establecimiento', 'area': 'fldArea', 'containers': 'fldContainerTree',
                      'active': 'fldFlag', 'type': 'fldFK_TipoDeEntidad', 'entityname': 'fldName'}

    @classmethod
    def getEntityData(cls, entityID=None, *, data_field=None):
        """ Fetches Geo Entity data for entityID and data field data_field.
            Returns field value or None if field or geoEntity not found."""
        if data_field and isinstance(data_field, str) and entityID in cls.getGeoEntities():
            fld = next((cls.__dataFieldMap[j] for j in cls.__dataFieldMap if
                        j.__contains__(data_field.strip().lower().replace(' ', ''))), None)
            if fld:
                return cls.getGeoEntities()[entityID].get(fld)
            return cls.getGeoEntities()[entityID].get(data_field)
        return None

    @classmethod
    def compareLocations(cls, *, loc1=None, loc2=None, is_contained=True):
        """ Compares entity1 and entity1 to determine relationship.
        @param loc2: Entity to check if is or belongs to entity1.
        @param loc1: Container entity to check entity1 against.
        @param is_contained: True (default) -> returns valid (True) if entity1 is contained in or is equal to loc2
                             False -> returns valid (True) ONLY if entity1 IS EQUAL to loc2
        """
        if all(j in cls.getGeoEntities() for j in (loc1, loc2)):
            if is_contained is True:
                containers = cls.getEntityData(loc1, data_field='containers')
                # print(f'***{moduleName()}({lineNum()}) Geo compareLocations() containers: {containers}',
                # dismiss_print=DISMISS_PRINT)
                return True if loc2 in containers and \
                               (cls.getEntityData(loc2, data_field='level') <=
                                cls.getEntityData(loc1, data_field='level')) else False
            return loc1 == loc2
        return False

    def contains(self, other):
        try:
            containers = self.getEntityData(self.ID, data_field='containers')
            return True if self.ID in containers and \
                           (self.getEntityData(other.ID, data_field='level') >=
                            self.getEntityData(self.ID, data_field='level')) else False
        except (TypeError, AttributeError, NameError):
            return False

    def equal(self, other):
        try:
            return self.ID == other.ID
        except (TypeError, AttributeError, NameError):
            return False

    def contained_in(self, other):
        try:
            containers = self.getEntityData(other.ID, data_field='containers')
            return True if other.ID in containers and \
                           (self.getEntityData(self.ID, data_field='level') >=
                            self.getEntityData(other.ID, data_field='level')) else False
        except (TypeError, AttributeError, NameError):
            return False


    @classmethod
    def createGeoEntity(cls, *args, **kwargs):     # obj corresponds to the handlerGeo object.
        """
        Creates a geo Element (Country, Province, Establecimiento, Potrero, Location, etc). Adds it to the DB.
        @param args: Not used
        @param kwargs: Mandatory: fldName, fldFK_TipoDeEntidad, fldFK_EntidadContainer(s) depending on entityType.
        fldFK_EntidadContainer contains one or more ID_Entidad of Container Entities for the entity being created.
        At least the MANDATORY fldFK_EntidadContainer must be provided.
        This obj_data is written in table [Geo Entidades Container] for the entity to be created.
        @return: ID_GeoEntidad (int) or errorCode (str)
        """
        if kwargs:
            kwargs.pop('fldID', None)  # This is a "create" method. Removes fldID because a NEW record is to be created
        timeStamp = time_mt('datetime')
        kwargs['fldTimeStamp'] = timeStamp
        kwargs['fldFK_UserID'] = sessionActiveUser
        tblEntity = setupArgs(Geo.__tblEntitiesName, *args, **kwargs)
        if 'fldName' not in tblEntity.fldNames or 'fldFK_TipoDeEntidad' not in tblEntity.fldNames or \
                'fldFK_Pais' not in tblEntity.fldNames or 'fldFK_NivelDeLocalizacion' not in tblEntity.fldNames:
            retValue = 'ERR_InvalidArgument: Required arguments (fldName, fldFK_TipoDeEntidad, fldFK_Pais, ' \
                       'fldFK_NivelDeLocalizacion) missing'
            print(f'GEO.PY({lineNum()} - {retValue})', dismiss_print=DISMISS_PRINT)
        else:
            entityName = removeAccents(kwargs['fldName'])
            entityType = kwargs['fldFK_TipoDeEntidad']
            if entityType == cls.getEntityTypesDict()['Pais'][0]:      # 1) Entidad a crear es Pais.
                temp = getRecords(Geo.__tblEntitiesName, '', '', None, '*', fldFK_TipoDeEntidad=entityType)
                # normalizedNames is the DB Entity Name stripped of all accents, dieresis, etc.
                normalizedNames = []  # if len(normalizedNames) == 0 -> There are no Entities of that type in DB
                if temp.dataLen:
                    namesCol = temp.getCol('fldName')
                    for i in range(temp.dataLen):
                        normalizedNames.append(removeAccents(namesCol[i]))
                if entityName in normalizedNames:       # Checks if Pais name already exists.
                    retValue = f'ERR_Name already exists: Pais(Country) - GEO.PY({lineNum()}'
                else:
                    retValue = setRecord(Geo.__tblEntitiesName, **tblEntity.unpackItem(0))
                    tblEntity.setVal(0, fldID=retValue, fldFK_Pais=retValue)
                    _ = setRecord(Geo.__tblEntitiesName, **tblEntity.unpackItem(0))
            else:
                containerTree = {}
                try:
                    # 2: Entidad a crear NO es Pais.
                    if 'fldFK_EntidadContainer' not in kwargs:
                        retValue = f'ERR_INP_Invalid Argument: Required argument fldFK_EntidadContainer missing'
                        krnl_logger.info(retValue)
                        print(f'GEO.PY({lineNum()}) - {retValue}')
                        return retValue
                    argsContainers = kwargs['fldFK_EntidadContainer']  # kwargs['fldFK_EntidadContainer']: int, [] or None
                    # Aqui, hacer append de los containers producidos por getContainers() y unificar todos los container
                    # Entities en containers,
                    if not argsContainers:
                        argsContainers = []
                    elif type(argsContainers) is int:
                        argsContainers = [argsContainers, ]  # Convierte a list para procesar Entidades Container
                    treeContainers = []
                    for i in argsContainers:
                        treeContainers.extend(cls.getContainerEntities(i))
                    # print(f'GEO.PY({lineNum()}) - Implicit Containers: {treeContainers}')
                    containers = list(set(argsContainers + treeContainers))         # Adds lists and removes duplicates
                    # print(f'GEO.PY({lineNum()}) - Entity Containers: {containers}')
                except (KeyError, ValueError, IndexError):
                    retValue = f'ERR_INP_Invalid Argument: Missing/Invalid mandatory Container Entity(Entidad Container)' \
                               f' - {callerFunction()}'
                    krnl_logger.info(retValue)
                    return retValue

                requiredEntityType = None       # Si no hay requiredEntityType, queda en None. NO deberia ser el caso
                multiplesList = []
                for i in Geo.__entityTypesDict:     # Pulls the Mandatory entityType for the entity being created
                    if cls.getEntityTypesDict()[i][0] == entityType:
                        requiredEntityType = cls.getEntityTypesDict()[i][1]
                        multiplesList = cls.getEntityTypesDict()[i][2] if cls.getEntityTypesDict()[i][2] else []
                        break
                # Crea lista de Tipos de Entidades Container para los elementos de lista container
                entitiesTable = getRecords(Geo.__tblEntitiesName, '', '', None, '*', fldID=containers)
                entitiesCol = entitiesTable.getCol('fldID')
                entitiesTypesCol = entitiesTable.getCol('fldFK_TipoDeEntidad')
                containerTypes = []  # Lista con TipoDeEntidad de los containers pasados por kwargs e implicitos
                implicitCountry = None
                for j, idEntity in enumerate(entitiesCol):
                    if idEntity in containers:
                        if entitiesTypesCol[j] < entityType:        # DEBE ser < entityType para que sea Container
                            containerTypes.append(entitiesTypesCol[j])
                            containerTree[idEntity] = entitiesTypesCol[j]
                        else:
                            # containers.remove(entitiesCol[j])  # Elimina container Entity si el tipo es >= entityType
                            # Sale con error: se paso un container no valido (>= Tipo de Entidad a crear) por kwargs.
                            retValue = f'ERR_INP_Invalid Argument: Parent Entity {entitiesCol[j]} is not valid. ' \
                                       f'Entity {kwargs["fldName"]} not created.'
                            krnl_logger.info(retValue)
                            print(f'GEO.PY({lineNum()}) - {retValue} - Function/Method {callerFunction(getCallers=True)}',
                                  dismiss_print=DISMISS_PRINT)
                            return retValue
                        if entitiesTypesCol[j] == cls.getEntityTypesDict()['Pais'][0]:
                            implicitCountry = idEntity
                # Verifica existencia de Entity Types Mandatorios
                if requiredEntityType not in containerTypes:
                    retValue = 'ERR_INP_Invalid Argument: Required Parent Entity missing'
                    krnl_logger.info(retValue)
                    print(f'GEO.PY({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
                    return retValue         # Sale con error: falta Mandatory Container Entity

                # Multiple Container Entities consist. check: more than 1 Entity of same type allowed only for multiples
                for i in range(len(containerTypes)):
                    count = containerTypes.count(containerTypes[i])
                    if count > 1 and containerTypes[i] not in multiplesList:
                        retValue = f'ERR_InvalidArgument: Multiple Entities {containerTypes[i]}. Only 1 allowed'
                        print(f'GEO.PY({lineNum()}) - {retValue}')
                        return retValue  # Sale con error: falta Mandatory Container Entity
                print(f'GEO.PY({lineNum()}) - Container Entities={containers} / container types: {containerTypes}',
                      dismiss_print=DISMISS_PRINT)

                # Since it's not creating a new country (Pais), pulls ALL records for the country and Entity Type
                # for which the new Entity is being created
                pais = kwargs['fldFK_Pais'] if 'fldFK_Pais' in kwargs else implicitCountry
                temp = getRecords(cls.__tblEntitiesName, '', '', None, '*', fldFK_TipoDeEntidad=entityType,
                                  fldFK_Pais=pais)       # Por este call, fldFK_Pais debe ser campo Mandatorio.
                normalizedNames = []  # if len(normalizedNames) == 0 -> There are no Entities of that type in DB
                if temp.dataLen:
                    namesCol = temp.getCol('fldName')
                    for i in range(temp.dataLen):
                        normalizedNames.append(removeAccents(namesCol[i]))  # normalizedNames del Pais y entityType
                if entityName in normalizedNames:
                    retValue = f'ERR_INP_ Name already exists: {entityName} - Function/Method: createGeoEntity()'
                    print(f'GEO.PY({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
                    return retValue     # Si nombre ya existe para ese entityType, sale con error.

                tblEntity.setVal(fldFK_Pais=pais, fldContainerTree=containerTree)
                retValue = setRecord(cls.__tblEntitiesName, **tblEntity.unpackItem(0))  # Registro en Geo Entidades
                tblEntity.setVal(0, fldID=retValue)

                if Geo.getEntityTypesDict()['Establecimiento'][0] <= entityType < Geo.getEntityTypesDict()['Locacion'][0]:
                    # Se debe llenar campo fldFK_Establecimiento para Establecimiento, Lote, Potrero, Locacion
                    if entityType == cls.getEntityTypesDict()['Establecimiento'][0]:
                        establecimiento = retValue
                    else:
                        establecimiento = kwargs['fldFK_Establecimiento']   # Implementar codigo para buscar Establec.
                    tblEntity.setVal(0, fldFK_Establecimiento=establecimiento)
                    retValue = setRecord(cls.__tblEntitiesName, **tblEntity.unpackItem(0))

                # Todo el codigo de este bloque OBSOLETO (al usar el container Tree en tabla Geo Entidades)
                containersValidated = []
                for j in range(len(argsContainers)):
                    if argsContainers[j] in entitiesCol:
                        containersValidated.append(argsContainers[j])
                print(f'CONTAINERSVALIDATED: {containersValidated}', dismiss_print=DISMISS_PRINT)
                if len(containersValidated) > 0:
                    tblContainerEntities = setupArgs(cls.__tblContainingEntitiesName, *args, **kwargs)
                    for j in range(len(containersValidated)):  # Crea registros en tabla [Geo Entidad Container]
                        tblContainerEntities.setVal(0, fldFK_GeoEntidad=retValue, fldFK_EntidadContainer=containersValidated[j])

                    for k in range(1, 100):       # Codigo de prueba: setRecords() escribe 100 records en 220 msec!!
                        tblContainerEntities.setVal(k, **tblContainerEntities.unpackItem(0))  # Test lines. REMOVE

                    # TODO: con el campo Container Tree en tabla GeoEntidades, ContainerEntities se hace obsoleta
                    idx_list = tblContainerEntities.setRecords()
                    print(f'\nidx_list = {idx_list}\n', dismiss_print=DISMISS_PRINT)

                print(f'Container Tree: {containerTree}', dismiss_print=DISMISS_PRINT)
        return retValue

    @classmethod
    def entitySetState(cls, entityID, *args, **kwargs):
        """
        @param entityID: fldID in [Geo Entidades] for which values are to be set.
        @param args: DataTable with parameters to set on record entityID
        @param kwargs: dictionary with pararms to set on record entityID. Overrides values in args in case of match.
        @return: entityID of successful (int) / errorCode (str) if error.
        """
        argTable = setupArgs(cls.__tblEntitiesName, *args, *kwargs)
        entityWrtTable = setupArgs(cls.__tblEntitiesName, None, argTable.unpackItem(0))  # Toma primer registro(item 0)
        if entityID > 0:
            entityWrtTable.setVal(0, fldID=entityID)
        temp = getRecords(cls.__tblEntitiesName, '', '', None, '*', fldID=entityWrtTable.getVal(0, 'fldID'))
        if type(temp) is str:       # Verifica que record exista en tabla [Geo Entidades]
            retValue = f'ERR_INP_Invalid Argument: ID_Entidad - Function/Method: entitySetState()'
            krnl_logger.info(retValue)
        else:
            retValue = entityWrtTable.setRecords()
        return retValue

    @classmethod
    def entityGetState(cls, entityID, *args):
        """
        Returns fields requested in *args for entityID. if args == '*', returns DataTable with ALL fields for entityID
        @param entityID:
        @param args: field Names to return values for. * or args=None returns all fields in table [Geo Entidades]
        @return: dataTable for GeoEntity with fldID=entityID. 1 record only -> the one matching with entityID.
        """
        if entityID > 0:
            args = [j for j in args if not isinstance(j, str)] + [j.strip() for j in args if isinstance(j, str)]
            if not args or '*' in args:       # returns all fields for Entity.
                temp = getRecords(cls.__tblEntitiesName, '', '', None, '*', fldID=entityID)
            else:
                temp = getRecords(cls.__tblEntitiesName, '', '', None, args, fldID=entityID) #Returns selected fields
            if type(temp) is str:
                retValue = f'ERR_RecordNotFound: ID_Entidad - unction/Method: entityGetState()'
            else:
                retValue = temp
        else:
            retValue = f'ERR_INP_Invalid Argument: ID_Entidad'
        if isinstance(retValue, str):
            krnl_logger.info(retValue)

        return retValue

    @classmethod
    def entityGetID(cls, *args, **kwargs):               # **kwargs Mandatory: Name, TipoDeEntidad
        """
        Returns fldID for GeoEntity identified with Name and TipoDeEntidad
        @param args: Not used. For future developments
        @param kwargs: fldName and fldFK_TipoDeEntidad, Country (Pais) for required Entity. Mandatory: Pais.
        @return: DataTable Object with GeoEntity obj_data if ok / errorCode: str. THERE MAY BE MORE THAN 1 RECORD if the
        trio Name/TipoDeIdentidad/Pais matches more than one record. In that case lists all records with matching Name
        and TipoDeEntidad.
        """
        if kwargs:
            for k in kwargs:
                if isinstance(k, str):
                    kwargs[k.strip()] = kwargs[k]
                    kwargs.pop(k)

            if 'fldFK_Pais' not in kwargs:
                retValue = f'ERR_InvalidArgument: Missing Argument(s) Pais(Country) - Function/Method: entityGetID()'
                print(f'GEO.PY({lineNum()} - {retValue})')
            entityName = kwargs.pop('fldName', '').strip()
            entityNameNormalized = removeAccents(entityName)
            temp = getRecords(cls.__tblEntitiesName, '', '', None, '*', **kwargs)
            namesCol = temp.getCol('fldName')
            for j, name in enumerate(namesCol):
                if removeAccents(name) == entityNameNormalized:
                    kwargs['fldName'] = f'"{entityName}"'
                    temp = getRecords(cls.__tblEntitiesName, '', '', None, '*', **kwargs)
                    break
            retValue = temp
        else:
            retValue = f'ERR_InvalidArgument: Missing Arguments - Function/Method: entityGetID()'
        return retValue

    @classmethod
    def getContainerEntities(cls, idEntity=None, mode=0):
        """
        Produces all containing entities for idEntity. If idEntity is not valid returns error string
        @param idEntity:
        @param mode: 0: returns LIST of Container Entities IDs.
                     1: returns DICT of the form {ID_ContainerID: ID_TipoDeEntidad, }
        @return: List or Dictionary as per mode value. None if no Container Entities are found. errorCode (str) if Error
        """
        if not idEntity or not isinstance(idEntity, int):
            return None

        temp = getRecords(cls.__tblEntitiesName, '', '', None, '*', fldID=idEntity)
        if temp.dataLen == 0:
            return [] if mode == 0 else {}
        treeDict = temp.getVal(0, 'fldContainerTree')
        if mode == 0:
            return tuple([int(j) for j in treeDict]) if treeDict else []
        return {int(j): treeDict[j] for j in treeDict} if treeDict else {}


    # def getContainerEntities00(self, idEntity, mode=0):
    #     """
    #     Produces all containing entities for idEntity. If idEntity is not valid returns error string
    #     @param idEntity:
    #     @param mode: 0: returns LIST of Container Entities IDs.
    #                  1: returns DICT of the form {ID_ContainerID: ID_TipoDeEntidad, }
    #     @return: List or Dictionary as per mode value. None if no Container Entities are found. errorCode (str) if Error
    #     """
    #     level = 0
    #     containers = [[idEntity]]       # List of Lists to manage entities by level
    #     containersTypes = []            # Lista testigo y CONTADOR del numero de veces que aparece cada TipoDeEntidad
    #     containersConsolidated = []     # List to consolidate results from each level into one single list
    #     containersDepartamentos = []    # List of containers to all Departamentos/Counties found in the search
    #     retValue = []
    #     while level >= 0:
    #         # 1. Gets list of container entities
    #         temp1 = getRecords(Geo.__tblContainingEntitiesName, '', '', None, '*', fldFK_GeoEntidad=containers[level])
    #         # print(f'GEO.PY({lineNum()}) - Containing Entities: {temp1.dataList}')
    #         if type(temp1) is str or not temp1.dataLen:
    #             errorMsg = f'ERR_RecordNotFound for ID_Entidad={idEntity} - Function/ActivityMethod: Geo.getContainerEntities()'
    #             print(f'GEO.PY({lineNum()}) - {errorMsg}')
    #             return retValue
    #         # Creates a list of container entities for Departamentos only, to disambiguate multiple entities
    #         temp1EntitiesCol = temp1.getCol('fldFK_GeoEntidad')
    #         temp2 = getRecords(Geo.__tblEntitiesName, '', '', None, '*', fldID=temp1EntitiesCol)
    #         temp2EntitiesTypes = temp2.getCol('fldFK_TipoDeEntidad')
    #         for i in range(len(temp2EntitiesTypes)):
    #             if temp2EntitiesTypes[i] == self.getEntityTypesDict()['Departamento'][0]:
    #                 for j in range(temp1.dataLen):
    #                     if temp1EntitiesCol[i] == temp1.getVal(j, 'fldFK_GeoEntidad'):
    #                         containersDepartamentos.append(temp1.getVal(j, 'fldFK_EntidadContainer'))
    #         # print(f'GEO.PY({lineNum()}) - Departamentos Containers: {containersDepartamentos}')
    #
    #         tblAuxList = temp1.getCol('fldFK_EntidadContainer')     # Checks for end of loop
    #         if temp1.dataLen == 0 or not temp1.dataLen or not tblAuxList[0]:
    #             break       # Sale si no se encuentran mas container Entities en tabla [Geo Entidades Container]
    #
    #         if level == 0:              # Pulls multiples map for entityType corresponding to idEntity
    #             temp = getRecords(Geo.__tblEntitiesName, '', '', None, '*', fldID=idEntity)
    #             multiplesList = []
    #             multiplesString = ''
    #             multiplesStr = ''
    #             idEntityType = temp.getVal(0, 'fldFK_TipoDeEntidad')
    #             for i in self.getEntityTypesDict():
    #                 if self.getEntityTypesDict()[i][0] == idEntityType:
    #                     multiplesString = self.getEntityTypesDict()[i][2]
    #                     break
    #             if multiplesString:
    #                 # Pulls list of Entity Types with multiple values for idEntity
    #                 for j in range(len(multiplesString)):
    #                     multiplesStr += multiplesString[j] if multiplesString[j].isnumeric() or multiplesString[j] == ',' else ''
    #                 multiplesList = multiplesStr.split(',')
    #                 try:
    #                     multiplesList = [int(i) for i in multiplesList]
    #                 except (TypeError, IndexError, ValueError):
    #                     retValue = f'ERR_DBValueNotValid - Function/ActivityMethod Geo.createGeoEntity()'
    #                     print(f'GEO.PY({lineNum()}) - {retValue}')
    #                     return retValue
    #
    #         auxContainerList = []
    #         for j in range(len(tblAuxList)):
    #             if tblAuxList[j] not in auxContainerList:
    #                 auxContainerList.append(tblAuxList[j])
    #
    #         # 2. Gets the Type of the elements in the containerEntititesList
    #         temp2 = getRecords(Geo.__tblEntitiesName, '', '', None, '*', fldID=auxContainerList)
    #         if type(temp2) is str:
    #             errorMsg = f'ERR_RecordNotFound for ID_Entidad={auxContainerList} - Function/ActivityMethod: Geo.getContainerEntities()'
    #             print(f'GEO.PY({lineNum()}) - {errorMsg}')
    #             return retValue
    #         auxContainerTypesList = temp2.getCol('fldFK_TipoDeEntidad')
    #
    #         level += 1
    #         auxList = []
    #         for i in range(len(auxContainerTypesList)):             # Verifica Tipos de Entidad. Valida multiples
    #             if len(containersDepartamentos) > 0 and self.getEntityTypesDict()['Pais'][0] < auxContainerTypesList[i] \
    #                     < self.getEntityTypesDict()['Departamento'][0] and auxContainerList[i] not in containersDepartamentos:
    #                 pass   # Sale si es Entidad Container de Departamento pero no esta en lista de Departamentos
    #             else:
    #                 if auxContainerTypesList[i] in multiplesList:
    #                     auxList.append(auxContainerList[i])
    #                     if auxContainerList[i] not in containersConsolidated:
    #                         containersConsolidated.append(auxContainerList[i])
    #                 else:
    #                     if containersTypes.count(auxContainerTypesList[i]) == 0:
    #                         auxList.append(auxContainerList[i])
    #                         if auxContainerList[i] not in containersConsolidated:
    #                             containersConsolidated.append(auxContainerList[i])
    #                 containersTypes.append(auxContainerTypesList[i])    # Actualiza lista de container types encontrados
    #         containers.append(auxList)                              # Actualiza containers (Lista de Listas)
    #     # ----------------------------------- End while --------------------------------------------
    #
    #     # containersConsolidated = list(set(containersConsolidated))      # Elimina duplicados de lista final
    #     if mode == 0:           # Retorna lista de ID_Entidad de todas las Entidades Container de idEntity
    #         retValue = containersConsolidated
    #     else:       # mode=1 -> retorna diccionario {ID_ContainerID: ID_TipoDeEntidad, }
    #         temp = getRecords(Geo.__tblEntitiesName, '', '', None, '*', fldID=containersConsolidated)
    #         if type(temp) is str:
    #             retValue = f'{temp} - Function/ActivityMethod: Geo.getContainerEntities()'
    #             print(f'GEO.PY({lineNum()}) - {retValue}')
    #             return retValue
    #         # print(temp)
    #         keys = temp.getCol('fldID')
    #         values = temp.getCol('fldFK_TipoDeEntidad')
    #         if len(keys) == 0 or len(values) == 0:
    #             retValue = None
    #         else:
    #             retValue = dict(zip(keys, values))
    #     return retValue


handlerGeo = Geo()
