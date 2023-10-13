#################### COPY THE LATEST Class DataTable CODE FROM THIS FILE TO krnl_argumentTable.py ###############
###################    This has to be done to avoid circular import exceptions

from krnl_config import lineNum, strError, getFldName, createDT, obj_dtError, obj_dtBlank, getTblName
import os
ACTIVITY_LOWER_LIMIT = 15
ACTIVITY_UPPER_LIMIT = 30
ACTIVITY_DAYS_TO_ALERT = 15
ACTIVITY_DAYS_TO_EXPIRE = 365
ACTIVITY_DEFAULT_OPERATOR = None
MAX_WRT_ORDER = 100
uidCh = '__'  # Used in Signature and Notifications to create unique field names. Chars MUST be ok with use in DB Names.
oprCh = '__opr'     # Particle added to "fldName" fields in DataTables to store operators strings belonging to "fldName"
nones = (None, 'None', '', 'NULL', 'null', 'Null')

def fileName():
    return str(os.path.basename(__file__))


class DataTable(object):
    """
    Class to store obj_data from DB queries. structures from DB are created initially by function dbRead().
    The objects can be queried to pull records and can be "unpacked" to format the obj_data inside the object to the form
    {fName: fldValue,}
    """

    def __init__(self, tblName, dList=None, keyFieldNames=None, dbFieldNames=None, *args, **kwargs):
        tblName = str(tblName).strip()
        self.__dbTableName = getTblName(tblName)
        if strError in self.__dbTableName:   # _tblName no existe en DB
            self.__isValidFlag = False
            self.__dataList = []
            self.__fldNames = []
            self.__fldUIDs = []
            self.__tblName = self.__dbTableName     # Ambos nombres de tabla contienen la particula ERR_
        else:
            if keyFieldNames in nones or keyFieldNames == []:  # Si no se pasan field names, se crean todos los campos.
                fieldsDict = getFldName(tblName, '*', 1)
                self.__fldNames = tuple(fieldsDict.keys())  # Tuples: No se pueden agregar Campos una vez creada  tabla
                self.__dbFldNames = tuple(fieldsDict.values())
            else:
                self.__fldNames = keyFieldNames  # Aqui, len(keyFieldNames) es > 0
                self.__dbFldNames = dbFieldNames if dbFieldNames is not None else ()  # val tuple if no dbFieldNames
            self.__dataList = dList if dList is not None else []
            self.__tblName = tblName
            self.__fldNamesLen = len(self.__fldNames) if hasattr(self.__fldNames, '__iter__') else 0
            # fldUIDs: {fldName: fldUID, }, con fldUID=self._tblName+uidCh+self.__fldName, y uidCh = '__' al momento..
            self.__fldUIDs = {j: self.__tblName + uidCh + j for j in self.__fldNames}  # Crea UIDs para signatures, etc.
            self.__isValidFlag = True
            self.__undoOnError = False  # True: Undo writes en esta tabla al fallar escritura posterior de otras tablas
            self.__wrtOrder = MAX_WRT_ORDER   # write Order for setDBData(). 1, 2, etc. 1 is highest.
            self.__breakOnError = False  # Exits the writing, leaving all subsequent Tables unwritten. NOT IMPEMENTED.
            self.__associatedTables = []      # List of table names whose writes are linked to this table.
            self.__successfulAssociatedWrite = 0    # > 0: marks that at least 1 associated table has a valid write (so
            # can't undo write of this table anymore. When > 0, OVERRIDES undoOnError val. NOT IMPLEMENTED (22Jun22)

            if len(args) > 0:
                try:
                    for tbl in args:  # Asigna valores de *args (si se pasaron)
                        if str(type(tbl)).__contains__('.DataTable') and tbl.tblName == self.__tblName and tbl.dataLen:
                            for j in range(tbl.dataLen):     # Inicializa registros en dataList (si se pasa mas de 1)
                                self.setVal(j, **tbl.unpackItem(j))  # Escribe obj_data de args en dataList[j] (1 o mas reg)
                except (TypeError, ValueError, IndexError, KeyError, NameError):
                    print(f'ERR_UI_InvalidArgument - DataTable __init__({lineNum()})')

            # Asigna datos de kwargs. Valores de kwargs se escriben en _dataList SOLO si esos campos en *args
            ## Si kwargs[i] es lista o tuple se asigna cada elemento de kwargs[i] a un Index de _dataList
            if kwargs:
                # kwargs = kwargsParseNames(tblName, 1, **kwargs)
                for i in kwargs:
                    if type(kwargs[i]) is dict:
                        pass                 # Ignora: Diccionarios no son un tipo valido para pasar en kwargs
                    else:
                        if type(kwargs[i]) not in (list, tuple, set):
                            kwargs[i] = [kwargs[i], ]
                        for j in range(len(kwargs[i])):  # j es Index de _dataList, i es fldName
                            if self.getVal(j, i) in nones:
                                self.setVal(j, i, kwargs[i][j])  # Escribe solo si kwargs['fldName'] = None, '', NULL

    @property
    def isValid(self):
        return self.__isValidFlag

    @property
    def dataList(self):
        return self.__dataList if self.__isValidFlag is True else []

    @property
    def dataLen(self):
        try:
            return len(self.__dataList) if self.__isValidFlag is True else 0
        except (TypeError, ValueError, IndexError, AttributeError, NameError):
            return 0

    @property
    def fldNames(self):
        return list(self.__fldNames) if self.__isValidFlag is True else []

    @property
    def fldNamesLen(self):
        return self.__fldNamesLen if self.__isValidFlag is True else 0

    @property
    def dbFldNames(self):
        return list(self.__dbFldNames) if self.__isValidFlag is True else []

    def getDBFldName(self, fName: str):  # Retorna False si no encuentra el campo
        # Retorna False si no encuentra el campo, None si objeto no es valido.
        if self.isValid:
            fName = str(fName).strip().lower()
            retValue = next((j for j in self.__fldNames if j.strip().lower() == fName), False)
            # retValue = next((self.__dbFldNames[j] for j, name in enumerate(self.fldNames) if name.lower() == fName), False)
        else:
            retValue = None  # None si objeto no es valido.
        return retValue

    @property
    def getFldUIDs(self):
        """
        @return: Dict. {fldName: fldUID}  fldUID = "tblName__fldName" -> Unique UID for field.
        """
        return self.__fldUIDs if self.__isValidFlag is True else {}

    def getFldUID(self, fName):
        # Retorna False si no encuentra el campo, None si objeto no es valido.
        if self.isValid:
            fName = str(fName).strip().lower()
            retValue = next((self.__fldUIDs[j] for j in self.__fldUIDs if j.strip().lower() == fName), False)
        else:
            retValue = None  # None si objeto no es valido.
        return retValue

    def getFldIndex(self, fldName):
        fldName = str(fldName).strip()
        return next((j for j, name in enumerate(self.fldNames) if name == fldName), None)

    @property
    def tblName(self):
        return self.__tblName if self.__isValidFlag is True else None

    @property
    def dbTblName(self):
        return self.__dbTableName if self.__isValidFlag is True else None

    @property
    def undoOnError(self):  # val: True or False
        return self.__undoOnError if self.__isValidFlag is True else None

    @undoOnError.setter
    def undoOnError(self, val):
        self.__undoOnError = False if val in [*nones, 0, False] else True

    @property
    def wrtOrder(self):  # val: True or False
        return self.__wrtOrder if self.__isValidFlag is True else None

    @wrtOrder.setter
    def wrtOrder(self, val):
        self.__wrtOrder = min(int(val), MAX_WRT_ORDER)

    @property
    def breakOnError(self):  # val: True or False
        return self.__breakOnError if self.__isValidFlag is True else None

    @breakOnError.setter
    def breakOnError(self, val):
        self.__breakOnError = False if val in [*nones, 0, False] else True

    def unpackItem(self, j: int, mode=0):
        """
        Unpacks the j-element of _dataList in dictionary form and returns the dict.
        j < 0 -> Operates list as per Python rules: accesses items starting from last one. -1 represents the last item
        @param mode: 0 -> returns dict with _fldNames as keys. Default mode.
                     1 -> returns dict with dbFieldNames as keys
        @param j: index to _dataList table. Must be in the range of dataLen
        @return: {fldName1: value1, fldName2: value2, }. _fldNames from obj._fldNames list.
        """
        retDict = {}
        if self.__isValidFlag is True:
            if type(j) is not int or self.dataLen == 0:
                return retDict
            else:
                if mode == 0:
                    auxDict = dict(zip(self.__fldNames, self.__dataList[j])) if self.fldNamesLen > 0 else {}
                else:  # Mode != 0
                    auxDict = dict(zip(self.__dbFldNames, self.__dataList[j])) if len(self.__dbFldNames) > 0 else {}
                for i in auxDict:
                    retDict[i] = auxDict[i]
            # print(f'inside unpackItem ({lineNum()}) - retDict =  {retDict}')
        else:
            retDict = None  # None si objeto no es valido.
        return retDict

    def setVal(self, recIndex=0, fName=None, val=None, *args, **kwargs):
        """
        Sets the values for fieldNames passed in kwargs, of the record at recIndex position in _dataList
        Can INSERT (if recIndex > that last record in _dataList) or UPDATE a record (recindex within _dataList range)
        Any keyname not corresponding to a field in DataTable is ignored.
        @param fName: field Name. For backward compatibility
        @param val: fName val. For backward compatibility
        @param recIndex: record number to set in _dataList. recIndex >= dataLen, adds a record at the end of the list.
        @param args: for future developments
        @param kwargs: names and values of fields to update in DataTable.
        @return: Success: Dict {fldName: fldValue, } with values ACTUALLY written to DataTable / None: nothing written
        """
        if self.__isValidFlag:  # and (recIndex in range(obj.dataLen) or recIndex == obj.dataLen):
            if len(kwargs) > 0:
                if abs(recIndex) >= self.dataLen:
                    recIndex = self.dataLen
                    newRec = [None] * self.fldNamesLen
                    self.__dataList.append(newRec)  # Si recIndex >= dataLen, hace append de 1 registro
                if self.dataLen > 0:
                    retValue = {}
                    for i in kwargs:
                        fName = i.strip()
                        if fName in self.__fldNames:  # Si field name no esta en _fldNames, sale con False.
                            for j, name in enumerate(self.fldNames):  # ACTUALIZA valor val en registro existente
                                if name == fName:
                                    self.__dataList[recIndex][j] = kwargs[i]  # Actualiza valor de fName en _dataList.
                                    retValue[name] = kwargs[i]
                                    break
                else:
                    retValue = None
            else:  # Este else es por compatibilidad con versiones anteriores de setVal()
                if abs(recIndex) >= self.dataLen:
                    recIndex = self.dataLen
                    newRec = [None] * self.fldNamesLen
                    self.__dataList.append(newRec)  # Si recIndex llego al final, hace append de 1 registro
                fName = str(fName).strip()
                if self.dataLen > 0:
                    retValue = {}
                    if fName in self.__fldNames:  # Si field name no esta en _fldNames, sale con False.
                        for j, name in enumerate(self.fldNames):  # ACTUALIZA valor val en registro existente
                            if name == fName:
                                self.__dataList[recIndex][j] = val  # Actualiza valor del campo  fName en _dataList.
                                retValue[name] = val
                                break
                else:
                    retValue = None
        else:
            retValue = None
        return retValue

    def getVal(self, recIndex=0, *args):
        """
        Gets the val for field fName, in the recIndex record of table _dataList. if fname == '*' returns the whole
        record, as a dictionary. if fname is not found or recIndex out of range returns None
        @param recIndex: Record index to _dataList. recIndex == -1: Pulls the LAST record of _dataList
        @param args: field Name. Only data_field[0] is used for now.
        @return: fName val on success; if fName = '*': complete record at recIndex, as a dictionary.
        fName not Found: None.
        """
        retValue = None
        if self.isValid and len(args) > 0 and self.dataLen > 0 and len(self.__dataList[0]) > 0 \
                and abs(recIndex) in range(self.dataLen):
            fldName = args[0].strip()
            if fldName == '*':
                retValue = self.unpackItem(recIndex)  # Retorna Diccionario
            else:
                if fldName in self.__fldNames:
                    retValue = next((self.__dataList[recIndex][j] for j, name in enumerate(self.fldNames)
                                     if name == fldName), None)
                    # for j, name in enumerate(obj.fldNames):
                    #     if name == fldName:
                    #         retValue = obj._dataList[recIndex][j]  # Retorna valor del campo unicamente
                    #         break
        return retValue

    def appendRecord(self, *args, **kwargs):
        """
        adds a record after the last record in _dataList with all valid values found in kwargs dict. Values not passed
        are set to None.
        @param kwargs: Dictionary {fName: fldValue, }  values in None are ignored.
        @param args: for future development
        @return: NADA
        """
        newRec = [None] * self.fldNamesLen  # se crea un registro vacio (todos los values en None)
        if self.isValid:
            if kwargs:
                # kwargs = kwargsStrip(**kwargs)        # Esta linea no hace falta.
                for i in kwargs:
                    for j, name in enumerate(self.fldNames):
                        if i == name:
                            newRec[j] = kwargs[i]
                            break
            self.__dataList.append(newRec)
        return

    def getIntervalRecords(self, fldName, sValue='', eValue='', mode=0, **kwargs):
        """
        Gets the records between sValue and eValue. if sValue=eValue='' -> Last Record
        Returns a record based on the logic of Python's max(). So careful with its use: the fields picked must make
        sense for a max() operation.
        @param fldName: Field Name to filter obj_data from. Mandatory
        @param sValue: start Value. 0 or '0': get FIRST record. sValue=eValue='': get LAST record
        @param eValue: end Value. 0 or '0': get FIRST record. sValue=eValue='': get LAST record
        @param mode: 0: int, float, str and anything that is not a date in DB string format
                     1: Date(str) in the DB format 'YYYY-MM-DD HH:MM:SS XXXXXX'
        @param kwargs: fldName=fldValue -> Filters all records previously selected that meet the conditions
                       fldName=fldValue. USES OR LOGIC by default. AND logic to be implemented if ever required.
        @return: DataTable Object with Records meeting search criteria. EMPTY tuple if not found or MAX() not supported.
         DataTable construct.: __init__(obj, tblName, dList=None, keyFieldNames=None, dbFieldNames=None):
        """
        if self.isValid:
            fldName = str(fldName).strip()
            fldIndex = 0
            if fldName in self.__fldNames:
                for j, name in enumerate(self.fldNames):
                    if name == fldName:
                        fldIndex = j
                        break
                indexVector = []  # Array con datetime values. TODOS los records de la tabla
                for i in range(self.dataLen):
                    if len(self.__dataList[i]) > 0:
                        value = self.__dataList[i][fldIndex]
                        if mode == 1:
                            value = createDT(value)     # mode = 1. Fecha. Hay que convertir val a obj. datetime
                            if value in (obj_dtError, obj_dtBlank):
                                break                   # Ignora si fecha no es valida
                        indexVector.append(value)  # array temporal para ejecutar el max()/min(),index().

                if len(indexVector) > 0:
                    if sValue in nones and eValue in nones:     # LAST RECORD
                        searchIndex = indexVector.index(max(indexVector))
                        retList = [self.dataList[searchIndex], ]  # Debe crear lista de listas para que tutto funcione
                        retValue = DataTable(self.tblName, retList, self.fldNames, self.dbFldNames)
                    elif sValue == 0 or sValue == '0' or eValue == 0 or eValue == '0':  # FIRST RECORD
                        searchIndex = indexVector.index(min(indexVector))
                        retList = [self.dataList[searchIndex], ]  # Debe crear lista de listas para que tutto funcione
                        retValue = DataTable(self.tblName, retList, self.fldNames, self.dbFldNames)
                    else:
                        sValue = createDT(sValue) if mode == 1 else sValue
                        eValue = createDT(eValue) if mode == 1 else eValue
                        selectVector = []  # Vector con indices de los registros de dataList a agregar
                        retList = []  # Filtered List of records to return.
                        if sValue > eValue:
                            for j in range(len(indexVector)):  # Ignora endValue si es menor que sValue
                                if indexVector[j] >= sValue:
                                    selectVector.append(j)
                        else:
                            for j in range(len(indexVector)):  # Va entre start Date y End Date
                                if sValue <= indexVector[j] <= eValue:
                                    selectVector.append(j)

                        for i in range(len(selectVector)):
                            retList.append(self.dataList[selectVector[i]])
                        if len(selectVector) <= 1:
                            retList = [retList, ]  # Debe crear lista de listas para que tutto funcione
                        retValue = DataTable(self.tblName, retList, self.fldNames, self.dbFldNames)
                else:
                    retValue = DataTable(self.tblName, [[]], self.fldNames, self.dbFldNames)
            else:
                retValue = DataTable(self.tblName, [[]], self.fldNames, self.dbFldNames)
        else:
            retValue = DataTable(self.tblName, [[]], self.fldNames, self.dbFldNames)

        # Filtra registros resultantes por los valores key=fldValue pasados en kwargs
        if len(kwargs) > 0 and retValue.dataLen > 0: # and any(len(retValue.dataList[j]) > 0 for j in range(retValue.dataLen)):
            result = DataTable(retValue.tblName, [[]], retValue.fldNames, retValue.dbFldNames)
            for key in kwargs:
                k = key.strip()
                if k in retValue.fldNames:
                    for j in range(retValue.dataLen):
                        if retValue.dataList[j][k] == kwargs[key]:
                            result.appendRecord(**retValue.unpackItem(j))
            if result.dataLen > 0:
                retValue = result
        return retValue

    def getCol(self, fldName=None):
        """
        Returns the obj_data "column" corresponding to fName. If fName is not valid retuns None. If no field name is
        provided, returns None.
        @param fldName:
        @return: list with column obj_data corresponding to field name.EMPTY list if nothing is found.
        """
        retValue = []
        if self.isValid:
            fldName = str(fldName).strip()
            if fldName in self.__fldNames:
                fldIndex = next((i for i, name in enumerate(self.fldNames) if name == fldName), None)
                if fldIndex is not None:
                    retValue = [self.__dataList[j][fldIndex] for j in range(self.dataLen) if len(self.__dataList[j]) >0]
        return retValue

    def getCols(self, *args):
        """
        Returns the obj_data "columns" corresponding to fNames. If fName is not valid retuns None. If no field name is
        provided, returns None.
        @param args: Field Names, comma-separated
        @return: Dictionary {fldName: [fldValues, ], } - {} if nothing is found.
        """
        retValue = {}
        if len(args) > 0:
            if self.isValid:
                for fld in args:
                    fldName = str(fld).strip()
                    if fldName in self.__fldNames:
                        fldIndex = next((i for i, name in enumerate(self.fldNames) if name == fldName), None)
                        if fldIndex is not None:
                            retValue[fldName] = [self.__dataList[j][fldIndex] for j in range(self.dataLen)
                                                 if len(self.__dataList[j]) > 0]
        return retValue

        # ============================================================================================ ###












