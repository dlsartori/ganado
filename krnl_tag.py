from krnl_entityObject import *
from krnl_assetItem import AssetItem
from krnl_tag_activity import *
from krnl_config import callerFunction, sessionActiveUser, activityEnableFull
from custom_types import setupArgs, getRecords, setRecord, delRecord
from krnl_abstract_class_prog_activity import ProgActivity
from uuid import UUID, uuid4
from random import randrange


def moduleName():
    return str(os.path.basename(__file__))

class Tag(AssetItem):
    # _objClass = 21
    # __objType = 1

    # Defining _activityObjList attribute will call  _creatorActivityObjects() in EntityObject.__init_subclass__().
    _activityObjList = []  # List of Activity objects created by factory function.
    _myActivityClass = TagActivity
    __tblObjectsName = 'tblCaravanas'

    # Variables para logica de manejo de objetos repetidos/duplicados.
    _fldID_list = []  # List of all active records pulled by getRecords() from tblAnimales.
    _fldID_exit_list = []  # List of all all records with fldExitDate > 0, updated in each call to processReplicated().
    _object_fldUPDATE_dict = {}  # {fldID: fldUPDATE_counter, }        Keeps a dict of uuid values of fldUPDATE fields
    _RA_fldUPDATE_dict = {}  # {fldID: fldUPDATE_counter, }      # TODO: ver si este hace falta.
    _RAP_fldUPDATE_dict = {}  # {fldID: fldUPDATE_counter, }      # TODO: ver si este hace falta.
    temp0 = getRecords(__tblObjectsName, '', '', None, 'fldID', 'fldUPDATE', fldDateExit=0)
    if isinstance(temp0, DataTable) and temp0.dataLen:
        tempList = temp0.getCols('fldID', 'fldUPDATE')  # Initializes _object_fldUPDATE_dict with UPDATEd records.
        _fldID_list = tempList[0]  # Initializes _fldID_list with all fldIDs from active Animals.
        _object_fldUPDATE_dict = dict(zip(tempList[0], tempList[1]))
        _object_fldUPDATE_dict = {k: v for (k, v) in _object_fldUPDATE_dict.items() if v is not None}
        temp1 = getRecords(__tblObjectsName, '', '', None, 'fldID')
        if isinstance(temp1, DataTable) and temp1.dataLen:
            temp1_set = set(temp1.getCol('fldID'))
            _fldID_exit_list = list(temp1_set.difference(_fldID_list))


    # Listas de Tag.Activities, Inventory Activities.
    temp = getRecords('tblCaravanasActividadesNombres', '', '', None, 'fldID', 'fldName', 'fldFlag')
    __activityID = []
    __activityName = []
    __activityIsInv = []
    for j in range(temp.dataLen):
        __activityID.append(temp.dataList[j][0])
        __activityName.append(temp.dataList[j][1])
        __activityIsInv.append(temp.dataList[j][2])
    __activitiesDict = dict(zip(__activityName, __activityID))  # tagActivities = {fldNombreActividad: fldID_Actividad}.
    __activitiesForMyClass = __activitiesDict
    __activeProgActivities = []             # List of all active programmed activities for Tag objects. MUST be a list.

    __tblDataInventoryName = 'tblDataCaravanasInventario'
    __tblDataStatusName = 'tblDataCaravanasStatus'
    __tblObjectsName = 'tblCaravanas'
    _myActivityClass = TagActivity


    @classmethod
    def tblObjName(cls):
        return cls.__tblObjectsName


    @property
    def activities(self):
        return self.__activitiesDict

    @classmethod
    def getActivitiesDict(cls):
        return cls.__activitiesDict

    __isInventoryActivity = dict(zip(__activityName, __activityIsInv))
    @staticmethod
    def getInventoryActivity():
        return Tag.__isInventoryActivity

    @property
    def tblDataInventoryName(self):
        return self.__tblDataInventoryName

    @property
    def tblDataStatusName(self):
        return self.__tblDataStatusName

    @property
    def tblObjectsName(self):
        return self.__tblObjectsName

    # Diccionario de Tag.Status
    temp = getRecords('tblCaravanasStatus', '', '', None, 'fldID', 'fldName', 'fldFlag')
    __tagStatusDict = {}        #TODO(cmt) Estructura: {statusName: [statusID, activeYN]}
    for j in range(temp.dataLen):
        __tagStatusDict[str(temp.dataList[j][1])] = [int(temp.dataList[j][0]), int(temp.dataList[j][2])]

    @property
    def statusDict(self):
        return self.__tagStatusDict

    @classmethod
    def getStatusDict(cls):
        return cls.__tagStatusDict

    tagElementsList = ('fldID', 'fldTagNumber', 'fldFK_TecnologiaDeCaravana', 'fldTagMarkQuantity', 'fldFK_IDItem',
                       'fldFK_Color', 'fldFK_TipoDeCaravana', 'fldFK_FormatoDeCaravana' 'fldImage', 'fldDateExit',
                       'fldFK_UserID', 'fldTimeStamp', 'fldComment')

    __registerDict = {}  # {tagUUID: tagObject} Registro de Tags, para evitar duplicacion.

    @classmethod
    def getRegisterDict(cls):
        return cls.__registerDict

    def register(self):  # NO HAY CHEQUEOS. obj debe ser valido
        try:
            self.__registerDict[self.ID] = self  # TODO: Design criteria: Si se repite key, sobreescribe con nuevo valor
        except (NameError, KeyError, ValueError, TypeError):
            retValue = f'ERR_Sys_KeyError TagRegisterDict'
            krnl_logger.error(retValue, exc_info=True, stack_info=True)
            raise KeyError(retValue)
        else:
            return None

    # obj es Objeto de tipo Tag. NO HAY CHEQUEOS. obj debe ser valido
    def unRegister(self):
        """
        Removes object from __registerDict
        @return: removed object if successful. None if fails.
        """
        try:
            self._fldID_list.remove(self.recordID)      # removes fldID from _fldID_list to keep integrity of structure.
        except ValueError:
            pass
        return self.__registerDict.pop(self.ID, None)  # Retorna False si no encuentra el objeto

    @property
    def isRegistered(self):
        return self.ID in self.__registerDict


    def __init__(self, *args, **kwargs):  # TODO(cmt): lastStatus,lastInventory,lastLocalization NO IMPLEMENTADO p/ Tags
        self.__ID = kwargs.get('fldObjectUID')
        try:
            UUID(self.__ID)
        except(ValueError, TypeError, AttributeError):
            raise TypeError(f'ERR_INP_TypeError: Invalid/malformed UID {self.__ID}. Geo object cannot be created.')

        isValid = True
        self.__recordID = kwargs.get('fldID', None)
        # TODO(cmt): OJO! TAGS case-INSENSITIVE con este setup. Se eliminan acentos, dieresis y se pasa a lowercase
        self.__tagNumber = removeAccents(str(kwargs['fldTagNumber']))
        # self.__tagUID = self.__ID        # UID para identificacion unica del tag. Por ahora igual al numero..
        self.__tagTechnology = kwargs['fldFK_TecnologiaDeCaravana']     # Standard, RFID, LORA, Tatuaje.
        self.__tagMarkQuantity = kwargs['fldTagMarkQuantity']
        self.__idItem = kwargs['fldFK_IDItem'] if 'fldFK_IDItem' in kwargs else None
        self.__tagColor = kwargs['fldFK_Color'] if 'fldFK_Color' in kwargs else None
        self.__tagType = kwargs['fldFK_TipoDeCaravana'] if 'fldFK_TipoDeCaravana' in kwargs else None
        self.__tagFormat = kwargs['fldFK_FormatoDeCaravana'] if 'fldFK_FormatoDeCaravana' in kwargs else None
        self.__tagImage = kwargs['fldImage'] if 'fldImage' in kwargs else None
        self.__myProgActivities = {}        #  {paObj: activityID, }
        self.__timeStamp = kwargs.get('fldTimeStamp', None)  # Esta fecha se usa para gestionar objetos repetidos.
        self.__exitYN = kwargs['fldDateExit'] if 'fldDateExit' in kwargs else None
        if self.__exitYN:
            self.__exitYN = valiDate(self.__exitYN, 1)  # datetime object o 1 (Salida. Sin fecha)
            isActive = False
        else:
            self.__exitYN = 0
            isActive = True

        self.__tagComment = kwargs['fldComment'] if 'fldComment' in kwargs else None
        self.__tagUserID = kwargs['fldFK_UserID'] if 'fldFK_UserID' in kwargs else None
        # Clase de objeto a la que se asigna, para evitar asignacion multiple
        self.__assignedToClass = next((kwargs[j] for j in kwargs if 'assignedtocla' in j.lower()), None)
        self.__assignedTo = None            # Object (of any supported type) to which Tag is assigned.
        # self._isValid = True
        # Tags no soporta getStatus, _getInventory, getLocalization de memoria  (No se pasa kwargs['memdata'])
        super().__init__(self.__ID, isValid, isActive, *args, **kwargs)

    @property
    def tagNumber(self):
        return self.__tagNumber if self.isValid is not False else None

    @tagNumber.setter
    def tagNumber(self, val):
        self.__tagNumber = val


    @property
    def getElements(self):               # Diccionario armado durante inicializacion. Luego, NO SE ACTUALIZA. OJO!!
        return {
                'fldObjectUID': self.ID, 'fldTagNumber': self.__tagNumber, 'fldTagMarkQuantity': self.__tagMarkQuantity,
                'fldFK_IDItem': self.__idItem, 'fldFK_TipoDeCaravana': self.__tagType,  'fldDateExit': self.__exitYN,
                'assignedToClass': self.assignedToClass, 'fldFK_FormatoDeCaravana': self.__tagFormat,
                'fldImage': self.__tagImage, 'fldFK_Color': self.__tagColor, 'fldComment': self.__tagComment,
                'fldFK_UserID': self.__tagUserID, 'fldID': self.__recordID, 'fldTimeStamp': self.__timeStamp,
                }

    @property
    def ID(self):
        return self.__ID

    @property
    def recordID(self):
        return self.__recordID

    @classmethod
    def tagFromNum(cls, tagNum: str):
        """Returns a Tag object given its tagNumber. None if tagNumber is not valid or not found. """
        if isinstance(tagNum, str):
            return next((o for o in cls.getRegisterDict().values() if o.tagNumber == removeAccents(tagNum)), None)
        elif isinstance(tagNum, Tag):
            return tagNum
        else:
            try:
                tagNum = str(tagNum)
            except (TypeError, ValueError, UnicodeError):
                return None
            else:
                return next((o for o in cls.getRegisterDict().values() if o.tagNumber == removeAccents(tagNum)), None)

    def updateAttributes(self, **kwargs):
        """ Updates object attributes with values passed in attr_dict. Values not passed leave that attribute unchanged.
        @return: None
        """
        if not kwargs:
            return None


    @property
    def myTimeStamp(self):  # impractical name to avoid conflicts with the many date, timeStamp, fldDate in the code.
        """
        returns record creation timestamp as a datetime object
        @return: datetime object Event Date.
        """
        return self.__timeStamp

    @property
    def assignedToClass(self):
        return self.__assignedToClass   # Nombre(str) de clase al que fue asignado el tag. Evita duplicaciones.

    @assignedToClass.setter
    def assignedToClass(self, val):
        self.__assignedToClass = val        # NO CHECKS MADE HERE!! val must be a valid string.

    @property
    def tagInfo(self, *args):
        """
        returns Tag Information
        @return: tagElement val or Dictionary: {tagElement(str): elementValue}
        """
        retValue = None
        if self.isValid:
            args = [j for j in args if not isinstance(j, str)] + [j.strip() for j in args if isinstance(j, str)]
            if not args:  #
                retValue = self.__tagNumber  # Sin argumentos: returna tagNumber
            elif '*' in args:
                retValue = self.getElements  # *: Retorna todos los elementos
            elif len(args) == 1:  # Retorna un solo valor: elemento requerido o None si el parametro no existe
                retValue = self.getElements[args[0]] if args[0] in self.getElements else None
            else:
                retDict = {}
                for i in args:
                    if i in self.getElements:
                        retDict[i] = self.getElements[i]
                retValue = retDict  # Si no hay parametros buenos, devuelve {} -> Dict. vacio
        return retValue

    @property
    def isAvailable(self):
        """
        Returns available condition of a Tag from Data Caravanas Status table if status is "AltaActivity" (1) or "Decomisionada" (3)
        @return: True: Available / False: Note Available /None: Error->No se pudo leer status de DB.
        """
        tagsAvailStatus = (0, 1, 3, None)  # 1:AltaActivity; 3:Decomisionada. 2 unicos status que hacen tag "available".
        retValue = self.status.get()             # Devuelve ULTIMO status (Ultimo por fldDate)
        return True if retValue in tagsAvailStatus or self.__assignedToClass is None else False

    @property
    def exitYN(self):
        return self.__exitYN

    @property
    def assignedTo(self):
        """ @return: object to which Tag is assigned. None Tag is not assigned. """
        return self. __assignedTo


    def assignTo(self, val):
        self.__assignedTo = val         # NO CHECKS!! val must be valid.


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
    def loadFromDB(cls):
        """Loads all Active Tags from DB and initializes Tag.__registerDict """
        temp = getRecords(cls.tblObjName(), '', '', None, '*', fldDateExit=0)
        if not isinstance(temp, DataTable):
            raise DBAccessError(f"ERR_DBAccess: {temp}.")

        if temp.dataLen:
            for j in range(temp.dataLen):
                tagObj = Tag(**temp.unpackItem(j))
                tagObj.register()

        if hasattr(cls, '_fldID_list'):
            cls._fldID_list = temp.getCol('fldID')      # Initializes fldID_list for processReplicated() to work..

        return None

    @classmethod
    def generateTag(cls, **kwargs):
        """
        Generates a new tag. *** Used only for testing purposes ***.
        @param kwargs: tagnumber-> Tag Number. if not provided, a unique tag number is generated.
                       technology -> Standard, RFID, etc.
                       marks -> Number of marks (Default = 1)
                       tagType, tagColor, tagFormat
                       tagStatusDict -> Tag Status. Default=1 (Alta)
                       writeDB=True -> writes all required obj_data to DB tables (Caravanas, Caravanas Status, etc)
        @return: Tag Object
        """
        allTagNumbers = [t.tagNumber for t in cls.getRegisterDict().values()]
        tagNumber = '-RND-' + str(next((kwargs[k] for k in kwargs if 'tagnumber' in str(k).lower()),
                                       randrange(10000, 99999)))
        aux = 0
        while tagNumber in allTagNumbers and aux < 9990:
            tagNumber = '-RND-' + str(randrange(10000, 99999))
            aux += 1
        if aux >= 9990:
            retValue = f'ERR_Sys: Cannot assign Tag Number. Tag not created.'
            print(f'{retValue}')
            return retValue

        __tblRAName = 'tblCaravanasRegistroDeActividades'
        __tblObjName = 'tblCaravanas'  # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
        __tblLinkName = 'tblLinkCaravanasActividades'
        __tblStatusName = 'tblDataCaravanasStatus'
        tblRA = DataTable(__tblRAName)  # Tabla Registro De Actividades
        tblObject = DataTable(__tblObjName)  # Tabla "Objeto": tblCaravanas, tblAnimales, etc.
        tblLink = DataTable(__tblLinkName)  # Sin argumentos: se crean TODOS los campos de la tabla; _dataList=[]
        tblStatus = DataTable(__tblStatusName)

        technology = next((j for j in kwargs if str(j).lower().__contains__('techno')), 'Standard')
        marks = next((j for j in kwargs if str(j).lower().__contains__('mark')), 1)
        tagColor = next((j for j in kwargs if str(j).lower().__contains__('color')), 'Blanco')
        tagType = next((j for j in kwargs if str(j).lower().__contains__('tagtype')), 'General')
        tagFormat = next((j for j in kwargs if str(j).lower().__contains__('tagformat')), 'Tarjeta')
        tagStatus = next((j for j in kwargs if str(j).lower().__contains__('status')), 'Alta')
        tagStatus = tagStatus if tagStatus in cls.getStatusDict() else 'Alta'
        new_tag = cls(fldTagNumber=tagNumber, fldFK_TecnologiaDeCaravana=technology, fldFK_Color=tagColor,
                       fldFK_TipoDeCaravana=tagType, fldTagMarkQuantity=marks, fldFK_FormatoDeCaravana=tagFormat,
                       fldFK_UserID=sessionActiveUser, fldObjectUID=str(uuid4().hex), fldTimeStamp=time_mt('datetime'))
        temp_elements = new_tag.getElements.copy()
        temp_elements.pop('fldID')
        idTag = setRecord('tblCaravanas', **temp_elements)
        if isinstance(idTag, int):
            new_tag.__recordID = idTag      # Actualiza Tag ID con valor obtenido de setRecord()
            new_tag.register()   # TODO: SIEMPRE registrar Tag (al igual que Animal). __init__() NO registra los objetos.
            print(f'TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT Caravana Nueva: {type(new_tag)} / {new_tag.getElements} ')
            tblRA.setVal(0, fldFK_NombreActividad=Tag.getActivitiesDict()['Alta'], fldFK_UserID=sessionActiveUser)
            idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))   # Crea registro en tblRA
            tblRA.setVal(0, fldID=idActividadRA, fldComment=f'Generated Tag. ID: {new_tag.ID} / {new_tag.tagNumber}')
            tblLink.setVal(0, fldComment=f'{callerFunction()}. Tag ID: {new_tag.ID} / {new_tag.tagNumber}')  # fldFK_Actividad=idActividadRA,
            # _ = setRecord(tblLink.tblName, **tblLink.unpackItem(0))

            new_tag.status.set(tblRA, tblLink, tblStatus, status=tagStatus)
            new_tag.localization.set(tblRA, tblLink, localization="El Ñandu")
            return new_tag
        else:
            krnl_logger.error(f"ERR_DBAccess: Cannot write to table {cls.__tblObjectsName}. Tag not created.")
            del new_tag
            return None


    def validateActivity(self, activityName: str):
        """
        Checks whether __activityName is defined for the class of Animal of the caller object
        @param activityName: str. Activity Name to check.
        @return: True: Activity defined for Animal - False: Activity is not defined.
        """
        activityName = activityName.strip()
        retValue = True if activityName in self.__activitiesForMyClass else False  # = _activitiesDict
        return retValue


    def isAssigned(self):
        return self.assignedTo


    @classmethod
    def processReplicated(cls):
        """             ******  Run periodically as IntervalTimer func. ******
                        ******  IMPORTANT: This code should execute in LESS than 5 msec (switchinterval).      ******
        Used to execute logic for detection and management of INSERTed, UPDATEd and duplicate objects.
        Defined for Animal, Tag, Person, Geo.
        Checks for additions to tblAnimales from external sources (replication from other nodes) for Valid and
        Active objects. Updates _fldID_list, _object_fldUPDATE_dict
        @return: True if update operation succeeds, False if reading tblAnimales from db fails.
        """
        temp = getRecords(cls.tblObjName(), '', '', None, '*', fldDateExit=0)
        if not isinstance(temp, DataTable):
            return False

        # 1. INSERT -> Checks for INSERTED new Records verifying the status of the last-stored fldID col and locally
        # added records then, process repeats/duplicates.
        krnl_logger.info(f"---------------- INSIDE {cls.__class__.__name__}.processReplicated()!! -------------------")
        pulled_fldIDCol = temp.getCol('fldID')
        newRecords = set(pulled_fldIDCol).difference(cls._fldID_list)
        if newRecords:
            newRecords = list(newRecords)
            pulledIdentifiersCol = temp.getCol('fldTagNumber')  # List of lists. Each of the lists contains UIDs.
            fullIdentifiers = []  # List of ALL identifiers from temp table, to check for repeat objects.
            for lst in pulledIdentifiersCol:
                if isinstance(lst, (list, tuple, set)):
                    fullIdentifiers.extend(lst)
                else:
                    fullIdentifiers.append(lst)
            fullIdentifiers = set(fullIdentifiers)
            # TODO(cmt): here runs the logic for duplicates resolution for each of the uids in newRecords.
            for j in newRecords:  # newRecords: ONLY records NOT found in _fldID_list from previous call.
                # TODO: Pick the right cls (Bovine, Caprine, etc).
                obj_dict = temp.unpackItem(pulled_fldIDCol.index(j))
                objClass = Tag
                obj = objClass(**obj_dict)

                # If record is repeat (at least one of its identifiers is found in cls._identifiers), updates repeat
                # records for databases that might have created repeats. Changes are propagated by the replicator.
                identifs = obj.tagNumber  # getIdentifiers() returns set of identifiers UIDs.

                """ Checks for duplicate/repeat objects: Must determine which one is the Original and set the record's 
                fldObjectUID field with the UUID of the Original object and fldExitDate to flag it's a duplicate.
                """
                # Note: the search for common identifiers and the logic of using timeStamp assures there is ALWAYS
                # an Original object to fall back to, to ensure integrity of the database operations.
                if fullIdentifiers.intersection(identifs):
                    # TODO(cmt): Here detected duplicates: Assigns Original and duplicates based on myTimeStamp.
                    for o in objClass.getRegisterDict().values():
                        if o.tagNumber == identifs:
                            if o.assignedToClass == obj.assignedToClass: # Duplicate only if tags assigned to same class
                                if o.myTimeStamp <= obj.myTimeStamp:
                                    original = o
                                    duplicate = obj
                                else:
                                    original = obj
                                    duplicate = o
                                original.tagNumber = duplicate.tagNumber  # for data integrity.
                                setRecord(cls.tblObjName(), fldID=duplicate.recordID, fldObjectUID=original.ID,
                                          fldExitDate=time_mt('datetime'))
                                setRecord(cls.tblObjName(), fldID=original.recordID, fldTagNumber=original.tagNumber)
                            elif obj_dict.get('fldDB_ID') != MAIN_DB_ID:
                                # If record is not duplicate and comes from another node, adds it to __registerDict.
                                obj.register()
                            break
                elif obj_dict.get('fldDB_ID') != MAIN_DB_ID:
                    # If record is not duplicate and comes from another node, adds it to __registerDict.
                    obj.register()

        # 2. UPDATE - Checks for UPDATED records modified in other nodes and replicated to this database. Checks are
        # performed based on value of fldUPDATE field (a dictionary) in each record.
        # The check for the node generating the UPDATE is done here in order to avoid unnecessary setting values twice.
        # UPDATEDict = {temp.getVal(j, 'fldID'): temp.getVal(j, 'fldUPDATE') for j in range(temp.dataLen) if
        #               temp.getVal(j, 'fldUPDATE')}  # Creates dict only with non-NULL (populated) items.
        UPDATEDict = {}
        for j in range(temp.dataLen):
            if temp.getVal(j, 'fldUPDATE'):
                d1 = temp.unpackItem(j)
                UPDATEDict[d1['fldID']] = d1['fldUPDATE']
        changed = {k: UPDATEDict[k] for k in UPDATEDict if k not in cls._object_fldUPDATE_dict or
                   (k in cls._object_fldUPDATE_dict and UPDATEDict[k] != cls._object_fldUPDATE_dict[k])}
        if changed:             # changed = {fldID: fldUPDATE(int), }
            for k in changed:   # updates all records in local database with records updated by other nodes.
                # Update memory structures here: __registerDict, exitDate, etc, based on passed fldNames, values.
                changedRecord = temp.unpackItem(fldID=k)
                objClass = Tag
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
                            obj.unregister()
                # Updates _object_fldUPDATE_dict (of the form {fldID: UUID(str), }) to latest values.
                cls._object_fldUPDATE_dict[k] = changed[k]

        # 3. BAJA / DELETE -> For DELETEd Records and records with fldExitDate !=0, removes object from __registerDict.
        temp1 = getRecords(cls.tblObjName(), '', '', None, '*')
        if not isinstance(temp, DataTable):
            return False
        # Removes from __registerDict Tag with fldExitDate (exit_recs) and DELETE (deleted_recs) executed in other nodes
        all_recs = temp1.getCol('fldID')
        exit_recs = set(all_recs).difference(pulled_fldIDCol)       # records with fldExitDate != 0.
        remove_recs = exit_recs.difference(cls._fldID_exit_list)  # Compara con lista de exit Records ya procesados.
        deleted_recs = set(cls._fldID_list).difference(pulled_fldIDCol)
        remove_recs = remove_recs.union(deleted_recs) or []
        for i in remove_recs:
            obj = next((o for o in cls.getRegisterDict().values() if o.recordID == i), None)
            if obj:
                obj.unregister()

        # Updates list of fldID and list of records with fldExitDate > 0 (Animales con Salida).
        cls._fldID_list = pulled_fldIDCol.copy()  # [o.recordID for o in cls.getRegisterDict().values()]
        cls._fldID_exit_list = exit_recs.copy()
        return True


Tag.loadFromDB()




   #  # Crea Objetos Actividades: 1 para cada Clase definida krnl_tag_activity.py
   #  __activityName = 'Inventario'
   #  __inventoryObj = InventoryActivityTag(__activityName, __activitiesDict[__activityName],
   #                                        __isInventoryActivity[__activityName], activityEnableFull)
   #
   #  @property             # Lo importante aqui es que se EJECUTA CODIGO (__setattribute()) antes de retornar un objeto
   #  def inventory(self):
   #      self.__inventoryObj.outerObject = self   # obj.__inventoryObj.__setattr__('obj.__outerAttr', obj)
   #      return self.__inventoryObj  # Retorna objeto __inventoryObj para poder acceder metodos en InventoryActivityAnimal
   #
   #  __activityName = 'Status'
   #  __statusObj = StatusActivityTag(__activityName, __activitiesDict[__activityName],
   #                                  __isInventoryActivity[__activityName], activityEnableFull)
   #
   #  @property
   #  def status(self):
   #      self.__statusObj.outerObject = self   # Pasa OBJETO para ser usado por set, get, etc.
   #      return self.__statusObj  # Retorna objeto __statusObj para poder acceder metodos en clase StatusActivityAnimal
   #
   #
   #
   #  __activityName = 'Localizacion'
   #  __localizationObject = LocalizationActivityTag(__activityName, __activitiesDict[__activityName],
   #                                                 __isInventoryActivity[__activityName], activityEnableFull)
   #
   #  @property
   #  def localization(self):  # obj: objeto Tag que se pasa al atributo __outerAttr__ del objeto LocalizationActivity
   #      self.__localizationObject.outerObject = self
   #      return self.__localizationObject  # Retorna objeto __inventoryObj p/ poder acceder metodos en InventoryActivityAnimal
   #
   #  __activityName = 'Comision'
   #  __method_name = 'commission'
   #  __commissionObject = CommissionActivityTag(__activityName, __activitiesDict[__activityName],
   #                                             __isInventoryActivity[__activityName], activityEnableFull)
   #
   #  @property
   #  def commission(self):  # obj es un objeto Tag que se pasa al atributo __outerAttr__ del obj.
   #      self.__commissionObject.outerObject = self
   #      return self.__commissionObject  # Retorna objeto __commisionObj para acceder metodos en CommissionActivityTag
   #
   #