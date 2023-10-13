import sqlite3

import krnl_sqlite
from krnl_config import lineNum, strError, createDT, obj_dtError, obj_dtBlank, uidCh, nones, callerFunction, \
    isType, krnl_logger
from krnl_sqlite import getTblName, getFldName, SQLiteQuery
from json import loads
import os

ACTIVITY_LOWER_LIMIT = 15
ACTIVITY_UPPER_LIMIT = 30
ACTIVITY_DAYS_TO_ALERT = 15
ACTIVITY_DAYS_TO_EXPIRE = 365
ACTIVITY_DEFAULT_OPERATOR = None
MAX_WRT_ORDER = 100


def moduleName():
    return str(os.path.basename(__file__))

module_logger = krnl_logger

""" 
How logging works:
Internally, messages are turned into LogRecord objects and routed to a Handler object registered for this krnl_logger. 
The handler will then use a Formatter to turn the LogRecord into a string and emit that string.
In general a module should emit log messages as a best practice and should not configure how those messages are handled. 
That is the responsibility of the application.
"""


class DataTable(object):
    """
    Class to store obj_data from DB queries. structures from DB are created initially by function dbRead().
    The objects can be queried to pull records and can be "unpacked" to format the obj_data inside the object
    Also used as the standard parameter-passing structure between functions and methods to access/process DB data.
    """

    def __init__(self, tblName=None, dList=None, keyFieldNames=None, dbFieldNames=None, *args, **kwargs):
        # if kwargs:
        #     pass    # TODO: PROCESAR kwargs como datos en formato json, parsear y asignar a tblName, dList, *args, etc.
                    # Luego seguir con la inicializacion de aqui abajo..

        tblName = str(tblName).strip()
        self.__dbTableName = getTblName(tblName)
        if self.__dbTableName.__contains__(strError):   # _tblName no existe en DB
            self.__isValidFlag = False
            self.__dataList = []              # TODO: Esto pasaria a ser un write-only List cuando se usa row_factory
            self.__fldNames = []
            self.__fldUIDs = {}                    # UIDs se crean durante la inicializacion. Se usan en Signatures.
            self.__tblName = self.__dbTableName     # Ambos nombres de tabla contienen la particula ERR_
        else:
            fieldsDict = getFldName(tblName, '*', 1) # TODO(cmt): esta llamada define si la tabla tiene o no row_factory
            if not keyFieldNames:    # in nones or keyFieldNames == []:  # Si no se pasan field names, se crean todos los campos.
                self.__fldNames = tuple(fieldsDict.keys())  # Tuples: No se pueden agregar Campos una vez creada  tabla
                self.__dbFldNames = tuple(fieldsDict.values())
            else:
                self.__fldNames = tuple(keyFieldNames)  # Aqui, len(keyFieldNames) es > 0
                self.__dbFldNames = tuple(dbFieldNames) if dbFieldNames is not None else ()  # tuple if no dbFieldNames

            self.__tblName = tblName
            self.__dataList = dList if dList is not None else []
            self.__fldNamesLen = len(self.__fldNames) if hasattr(self.__fldNames, '__iter__') else 0

            # row_factory stuff:
            self.__fRows = kwargs.get('fRows')
            self.__fRows = self.__fRows if (self.__fRows and isinstance(self.fRows[0], sqlite3.Row)) else []
            self.__isRowFactory = True if self.__fRows else False
            self.__fRowFldMap = dict(zip(self.__fldNames, self.__dbFldNames))  # TODO: para usar con sqlite3 row_factory
            # Si es row_factory, traspasa TODOS los datos de registros de rows a _dataList, para hacerlos escribibles
            # if self.__isRowFactory:
            #     for j in range(len(self.__fRows)):
            #         self._dataList[j] = [self.__fRows[j][i] for i in range(len(self.__fRows[j]))]
            # NO SE VE POR AHORA LA VENTAJA DE COPIAR todo rows sobre _dataList.

            # fldUIDs: {fldName: fldUID, }, con fldUID=self._tblName+uidCh+self.__fldName, y uidCh = '__' al momento..
            self.__fldUIDs = {j: self.__tblName + uidCh + j for j in self.__fldNames}  # Crea UIDs para signatures, etc.
            self.__uidFldMap = dict(zip(self.__fldNames, self.__fldUIDs))
            self.__isValidFlag = True
            self.__undoOnError = False  # True: Undo writes en esta tabla al fallar escritura posterior de otras tablas
            self.__wrtOrder = MAX_WRT_ORDER   # write Order for setDBData(). 1, 2, etc. 1 is highest.
            self.__breakOnError = False  # Exits the writing, leaving all subsequent Tables unwritten. NOT IMPEMENTED.
            self.__associatedTables = []      # List of table names whose writes are linked to this table.
            self.__successfulAssociatedWrite = 0    # > 0: marks that at least 1 associated table has a valid write (so
            # can't undo write of this table anymore. When > 0, OVERRIDES undoOnError val. NOT IMPLEMENTED (22Jun22)

            if args:
                try:
                    for tbl in args:  # Asigna valores de *args (si se pasaron)
                        if isType(tbl, DataTable) and tbl.tblName == self.__tblName and tbl.dataLen:
                            for j in range(tbl.dataLen):     # Inicializa registros en dataList (si se pasa mas de 1)
                                self.setVal(j, **tbl.unpackItem(j))  # Escribe obj_data de args en dataList[j] (1 o mas reg)
                except (TypeError, ValueError, IndexError, KeyError, NameError):
                    print(f'ERR_UI_InvalidArgument - DataTable __init__({lineNum()})')
                    # krnl_logger.error(f'ERR_UI_InvalidArgument - DataTable __init__({lineNum()})')

            # Asigna datos de kwargs. Valores de kwargs se escriben en _dataList SOLO si esos campos en *args
            # Si kwargs[i] es lista o tuple se asigna cada elemento de kwargs[i] a un Index de _dataList
            if kwargs:
                kwargs = kwargsParseNames(tblName, 1, **kwargs)
                for i in kwargs:
                    if type(kwargs[i]) is dict:
                        pass                 # Ignora: Diccionarios no son un tipo valido para pasar en kwargs
                    else:
                        if type(kwargs[i]) not in (list, tuple, set):
                            kwargs[i] = [kwargs[i], ]
                        for j in range(len(kwargs[i])):  # j es Index de _dataList, i es fldName
                            if self.getVal(j, i) in nones:
                                self.setVal(j, i, kwargs[i][j])  # Escribe solo si kwargs['fldName'] = None, '', NULL
        super().__init__()

    @property
    def isValid(self):
        return self.__isValidFlag

    @property
    def dataList(self):
        return self.__dataList if self.__isValidFlag else [[]]

    @property
    def dataLen(self):
        return len(self.__dataList) if self.__dataList else 0
        # try:
        #     return len(self._dataList) if self.__isValidFlag else 0
        # except (TypeError, ValueError, IndexError, AttributeError, NameError):
        #     return 0

    @property
    def fRows(self):
        return self.__fRows

    @fRows.setter
    def fRows(self, val):
        # testVal = val[0] if hasattr(val, '__iter__') else val
        # if isinstance(testVal, sqlite3.Row):
        self.__fRows = val

    @property
    def fldNames(self):
        return list(self.__fldNames) if self.__isValidFlag else []

    @property
    def fldDBNames(self):
        return list(self.__dbFldNames) if self.__isValidFlag else []

    @property
    def fldNamesLen(self):
        return self.__fldNamesLen if self.__isValidFlag else 0

    @property
    def isRowFactory(self):
        return self.__isRowFactory if self.__isValidFlag else False

    def getDBFldName(self, fName: str):  # Retorna False si no encuentra el campo
        # Retorna None si no encuentra el campo o si objeto DataTable no es valido.
        if self.isValid:
            fName = str(fName).strip()
            # if self.__isRowFactory:
            #     return self.__fldMap[fName]
            # else:
            return self.__fRowFldMap.get(fName)
                # retValue = next((self.__dbFldNames[j] for j, name in enumerate(self.fldNames) if name == fName), None)
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
        # Retorna None si no encuentra el campo o si objeto no es valido.
        if self.isValid:
            fName = str(fName).strip()
            return self.__uidFldMap.get(fName)      # Esta llamada retorna None si fName no se encuentra
            # retValue = next((self.__fldUIDs[j] for j in self.__fldUIDs if j.strip().lower() == fName), None)
        else:
            return None  # None si objeto no es valido.

    def getFldIndex(self, fName):
        # fldName = str(fName).strip()
        try:
            return self.__fldNames.index(fName)
        except(NameError, IndexError):
            return None

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
        if self.__isValidFlag and self.fldNamesLen:
            if type(j) is not int or not self.dataLen:
                return retDict
            else:
                if not mode:
                    auxDict = dict(zip(self.__fldNames, self.__dataList[j])) if abs(j) in range(self.dataLen) else {}
                else:  # Mode != 0
                    auxDict = dict(zip(self.__dbFldNames, self.__dataList[j])) if abs(j) in range(self.dataLen) else {}
                for i in auxDict:
                    retDict[i] = auxDict[i]
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
        @return: Success: True/False {}: nothing written
        """
        if self.__isValidFlag:  # and (recIndex in range(obj.dataLen) or recIndex == obj.dataLen):
            if kwargs:
                if abs(recIndex) >= self.dataLen:   # Si recIndex >= dataLen, hace append de 1 registro
                    recIndex = self.dataLen
                    newRec = [None] * self.fldNamesLen
                    self.__dataList.append(newRec)
                if self.dataLen:
                    for i in kwargs:
                        fName = i.strip()
                        if fName in self.__fldNames:  # Si field name no esta en _fldNames, sale con False.
                            self.__dataList[recIndex][self.getFldIndex(fName)] = kwargs[i]
                    return True

                            # for j, name in enumerate(self.fldNames):  # ACTUALIZA valor val en registro existente
                            #     if name == fName:
                            #         self._dataList[recIndex][j] = kwargs[i]# Actualiza val de fName en _dataList
                            #         retValue = True
                            #         # retValue[name] = kwargs[i]
                            #         break
                else:
                    retValue = None

            else:  # Este else es por compatibilidad con versiones anteriores de setVal()
                if abs(recIndex) >= self.dataLen:
                    recIndex = self.dataLen
                    newRec = [None] * self.fldNamesLen
                    self.__dataList.append(newRec)  # Si recIndex llego al final, hace append de 1 registro
                fName = str(fName).strip()
                if self.dataLen:
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
            krnl_logger.error(f'ERR_Sys_Invalid DataTable. {callerFunction()}({lineNum()})')
        return retValue


    def setVal00(self, recIndex=0, fName=None, val=None, *args, **kwargs):
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
            if kwargs:
                if abs(recIndex) >= self.dataLen:   # Si recIndex >= dataLen, hace append de 1 registro
                    recIndex = self.dataLen
                    newRec = [None] * self.fldNamesLen
                    self.__dataList.append(newRec)
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
            # krnl_logger.error(f'ERR_Sys_Invalid DataTable. {callerFunction()}({lineNum()})')
        return retValue

    def getVal(self, recIndex=0, fName=None, defaultVal=None, *args):
        """
        Gets the val for field fName, in the recIndex record of table _dataList. if fname == '*' returns the whole
        record, as a dictionary. if fname is not found or recIndex out of range returns None
        @param fName: Field Name whose value is to be retrieved. '*': Returns full record at recIndex, as a dictionary
        @param defaultVal: Value to return if Return Value is None (Default=None)
        @param recIndex: Record index to _dataList. recIndex == -1: Pulls the LAST record of _dataList
        @param args: field Name. Only data_field[0] is used for now.
        @return: fName val on success; if fName = '*': complete record at recIndex, as a dictionary.
        Empty (None) value field: defaultVal. None: invalid Parameters
        """
        if self.isValid and fName:
            fldName = str(fName).strip()
            if self.__fRows and fldName in self.__fldNames:
                if self.dataLen and abs(recIndex) in range(self.dataLen) and self.__dataList[recIndex]:
                    retValue = self.__fRows[recIndex][self.__fRowFldMap.get(fldName)]
                else:
                    retValue = defaultVal
            elif fldName in (*self.fldNames, '*'):
                retValue = defaultVal  # abs() porque la funcion acepta recIndex negativos
                if self.dataLen and abs(recIndex) in range(self.dataLen) and self.__dataList[recIndex]:
                    if fldName == '*':
                        retValue = self.unpackItem(recIndex)  # Retorna Diccionario
                    else:
                        if fldName in self.__fldNames:
                            retValue = next((self.__dataList[recIndex][j] for j, name in enumerate(self.fldNames)
                                             if name == fldName), defaultVal)
            else:
                retValue = 'ERR_INP_InvalidArguments'
        else:
            retValue = 'ERR_INP_Table Not Valid'  # None indica error en alguno de los argumentos de getVal()
            if not self.isValid:
                print(f'{moduleName()}({lineNum()}): {retValue} - {callerFunction(getCallers=True)}')
            else:
                retValue = 'ERR_INP_InvalidArguments'
                print(f'{moduleName()}({lineNum()}): {retValue} - {callerFunction(getCallers=True)}')
        return retValue


    def getVal00(self, recIndex=0, fName=None, defaultVal=None, *args):     # TODO: FUNC. ORIGINAL
        """
        Gets the val for field fName, in the recIndex record of table _dataList. if fname == '*' returns the whole
        record, as a dictionary. if fname is not found or recIndex out of range returns None
        @param fName: Field Name whose value is to be retrieved. '*': Returns full record at recIndex, as a dictionary
        @param defaultVal: Value to return if Return Value is None (Default=None)
        @param recIndex: Record index to _dataList. recIndex == -1: Pulls the LAST record of _dataList
        @param args: field Name. Only data_field[0] is used for now.
        @return: fName val on success; if fName = '*': complete record at recIndex, as a dictionary.
        Empty (None) value field: defaultVal. None: invalid Parameters
        """
        fldName = str(fName).strip()
        if self.isValid and fName and fldName in (*self.fldNames, '*'):       #  and fldName
            retValue = defaultVal       # abs() porque la funcion acepta recIndex negativos
            if self.dataLen and abs(recIndex) in range(self.dataLen) and self.__dataList[recIndex]:
                if fldName == '*':
                    retValue = self.unpackItem(recIndex)  # Retorna Diccionario
                else:
                    if fldName in self.__fldNames:
                        retValue = next((self.__dataList[recIndex][j] for j, name in enumerate(self.fldNames)
                                         if name == fldName), None)
                        if retValue is None:
                            retValue = defaultVal
        else:
            retValue = 'ERR_INP_Table Not Valid'     # None indica error en alguno de los argumentos de getVal()
            if not self.isValid:
                print(f'{moduleName()}({lineNum()}): {retValue} - {callerFunction(getCallers=True)}')
            else:
                retValue = 'ERR_INP_InvalidArguments'
                print(f'{moduleName()}({lineNum()}): {retValue} - {callerFunction(getCallers=True)}')
        return retValue




    def appendRecord(self, *args, **kwargs):        # TODO(cmt): _dataList DEBE mantener siempre  el record length.
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
                    for j, name in enumerate(self.fldNames):  # OJO: Aqui fldNames pueden ser ad-hoc (no definidos DB)
                        if i.strip() == name:
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
                        retValue = DataTable(self.tblName, retList, self.fldNames, self.fldDBNames)
                    elif sValue == 0 or sValue == '0' or eValue == 0 or eValue == '0':  # FIRST RECORD
                        searchIndex = indexVector.index(min(indexVector))
                        retList = [self.dataList[searchIndex], ]  # Debe crear lista de listas para que tutto funcione
                        retValue = DataTable(self.tblName, retList, self.fldNames, self.fldDBNames)
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
                        retValue = DataTable(self.tblName, retList, self.fldNames, self.fldDBNames)
                else:
                    retValue = DataTable(self.tblName, [[]], self.fldNames, self.fldDBNames)
            else:
                retValue = DataTable(self.tblName, [[]], self.fldNames, self.fldDBNames)
        else:
            retValue = DataTable(self.tblName, [[]], self.fldNames, self.fldDBNames)

        # Filtra registros resultantes por los valores key=fldValue pasados en kwargs
        if len(kwargs) > 0 and retValue.dataLen > 0: # and any(len(retValue.dataList[j]) > 0 for j in range(retValue.dataLen)):
            result = DataTable(retValue.tblName, [[]], retValue.fldNames, retValue.fldDBNames)
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
                    for j in range(self.dataLen):
                        val = self.__dataList[j][fldIndex] if len(self.__dataList[j]) > 0 else None
                        retValue.append(val)  # Logica p/ que los indices de getCol sean identicos a los de _dataList.
                        # Si hay records de _dataList vacios ([]), ese item se completa con None

                    # retValue=[self._dataList[j][fldIndex] for j in range(self.dataLen) if len(self._dataList[j])>0]
        return retValue

    def getCols(self, *args):
        """
        Returns the obj_data "columns" corresponding to fNames. If fName is not valid retuns None. If no field name is
        provided, returns None.
        @param args: Field Names, comma-separated
        @return: Dictionary {fldName: [fldValues, ], } - {} if nothing is found.
        """
        retValue = {str: []}
        if len(args) > 0:
            if self.isValid:
                for fld in args:
                    fldName = str(fld).strip()
                    if fldName in self.__fldNames:
                        fldIndex = next((i for i, name in enumerate(self.fldNames) if name == fldName), None)
                        if fldIndex is not None:
                            for j in range(self.dataLen):
                                val = self.__dataList[j][fldIndex] if len(self.__dataList[j]) > 0 else None
                                retValue[fldName].append(val)
                            # retValue[fldName] = [self._dataList[j][fldIndex] for j in range(self.dataLen)
                            #                      if len(self._dataList[j]) > 0]
        return retValue

# ================================================End of DataTable ================================================== #







# Las funciones de abajo no pueden salir de este modulo por ahora (circular import errors)

def setupArgs(tblName: str, *args, **kwargs):
    """
    Creates a DataTable object for tblName and writes all valid parameters found in *args and **kwargs.
    Fields values passed in **kwargs are assigned ONLY if corresponding fields in tables passed *args are None,'','NULL'
    @param tblName: valid table Name
    @param args: DataTable objects
    @param kwargs: obj_data to populate fields that are blank (None) in tables passed in *args
    @return: DataTable Object with arguments passed. dataList in DataTable val if no valid arguments found.
             Returns errorCode (str) if tblName is not valid
    """
    tblName = tblName.strip()
    if getTblName(tblName).__contains__(strError):
        retValue = f'ERR_UI_InvalidArgument:  Table Name: {tblName} - {moduleName()}({lineNum()}) - {callerFunction()}'
        return retValue

    tblArgs = DataTable(tblName)
    if args:
        try:
            for table in args:  # Asigna valores de *args (si se pasaron) a tablas tblRA y tblTransact
                if isType(table, DataTable) and table.dataLen and table.tblName == tblArgs.tblName:
                    for j in range(table.dataLen):
                        dicto = table.unpackItem(j)  # Inicializa multiples registros en dataList (si se pasa mas de 1)
                        tblArgs.setVal(j, **dicto)  # Escribe obj_data de args en dataList[j] (1 o mas registros)
        except (TypeError, ValueError, IndexError, KeyError, NameError):
            print(f'ERR_UI_InvalidArgument - {moduleName()}({lineNum()}) - {callerFunction()}. Table: {tblName}')
    # Asigna datos de kwargs a tblArgs. Values de kwargs se escriben en _dataList SOLO si esos campos en *args son None
    if len(kwargs) > 0:
        kwargs = kwargsParseNames(tblName, 1, **kwargs)
        for i in kwargs:
            if type(kwargs[i]) is dict:         # ignora los diccionarios. No son un tipo valido para esta funcion.
                pass
            else:
                if type(kwargs[i]) not in (list, tuple, set):
                    kwargs[i] = [kwargs[i], ]
                for j in range(len(kwargs[i])):                  # j es Index de _dataList, i es fldName
                    if tblArgs.getVal(j, i) in nones:
                        tblArgs.setVal(j, i, kwargs[i][j])      # Escribe solo si kwargs['fldName'] = None, '', NULL
    return tblArgs


def dbRead(tblName, strSQL: str, mode=0):  # _tblName necesario para armar la estructura de retorno  TODO(cmt): NUEVA
    """
    Reads records from DB using argument strSQL. strSQL must be valid, with access to 1 table only.
    mode: 0(Default): returns DataTable Object  -> THIS IS THE MORE EFFICIENT WAY TO PROCESS DATA
          1: returns list of dictionaries [{fldName1:value1, fldName2:value2, }, {fldName1:value3, fldName2: value4, }, ]
    @return: mode 0: Object of class DataTable with all the obj_data queried. (Default)
             mode 1: List of dictionaries [{fld1:val1, fld2:val2, }, {fld3:val2, fld1:val4, fld2:val5,}, ]. Each
             dictionary maps to a record (row) from DB and goes to a record item in DataTable.
    """
    queryObj = SQLiteQuery.getObject()  # Adquiere queryObject del thread desde donde se llama a dbRead()
    dataList = []
    dbFieldNames = []  # DB Names de los campso en keyFieldNames.
    keyFieldNames = []  # keynames de los campos presentes en _dataList
    tblName = tblName.strip()  # Nombre de la tabla
    tblFldNamesDict = getFldName(tblName, '*', 1)  # {fName:fldDBName}. Se necesita para obtener los fldKeyNames.
    cur = queryObj.execute(strSQL)              # TODO(cmt): Acceso a DB
    if not isinstance(cur, str):
        dbFieldNames = [j[0] for j in cur.description]  # Solo campos leidos de DB se incluyen en la DataTable
        for j in range(len(dbFieldNames)):
            for i in tblFldNamesDict:
                if tblFldNamesDict[i] == dbFieldNames[j]:
                    keyFieldNames.append(i)
                    break
        rows = cur.fetchall()               # TODO(cmt): lectura de registros.
        if not rows:        # No hay datos: Retorna tabla vacia, PERO CON keyFieldNames, dbFieldNames inicializados.
            return DataTable(tblName, [dataList, ], keyFieldNames, dbFieldNames)

    else:       # No hay datos: Retorna tabla vacia, keyFieldNames=dbFieldNames=[]
        retValue = DataTable(tblName, [dataList, ], keyFieldNames, dbFieldNames)
        module_logger.error(f'ERR_DB: {cur} - {callerFunction()}')
        return retValue  # Loggea error y retorna tabla vacia

    for j in rows:     # No hay mas opcion que recorrer los campos 1 por 1 para determinar si hay que decodificar json
        rowDict = {}
        rowList = []
        for i in j:
            fldValue = i
            if fldValue and type(fldValue) is str:  # Chequea si es string y si es string, si tiene json encoding.
                if fldValue.__contains__(']') or fldValue.__contains__('}'):  # TODO: Revisar esta verificacion p/ json
                    try:
                        fldValue = loads(fldValue)

                    except ValueError:
                        pass  # Si loads() falla (no es json encoding), deja fldValue en su valor inicial

            if mode == 0:
                rowList.append(fldValue)  # Puebla lista con datos de un record de DB.
            else:
                rowDict[dbFieldNames[i]] = fldValue  # Loop para poblar dict. con datos de 1 record de DB
        dataList.append(rowList) if mode == 0 else dataList.append(rowDict)  # Retorna lista de diccionarios.

    if mode == 0:
        if rows and isinstance(rows[0], sqlite3.Row) or isinstance(rows[0], krnl_sqlite.ERow):   # Retorna objeto de tipo DataTable
            retValue = DataTable(tblName, dataList, keyFieldNames, dbFieldNames, fRows=rows)
        else:
            retValue = DataTable(tblName, dataList, keyFieldNames, dbFieldNames)
    else:
        retValue = dataList
    return retValue




def dbRead00(tblName, strSQL: str, mode=0):  # _tblName necesaria para armar DataTable TODO: Funcion ORIGINAL
    """
    Reads records from DB using argument strSQL. strSQL must be valid, with access to 1 table only.
    mode: 0(Default): returns DataTable Object  -> THIS IS THE MORE EFFICIENT WAY TO PROCESS DATA
          1: returns list of dictionaries [{fldName1:value1, fldName2:value2, }, {fldName1:value3, fldName2: value4, }, ]
    @return: mode 0: Object of class DataTable with all the obj_data queried. (Default)
             mode 1: List of dictionaries [{fld1:val1, fld2:val2, }, {fld3:val2, fld1:val4, fld2:val5,}, ]. Each
             dictionary maps to a record (row) from DB and goes to a record item in DataTable.
    """
    queryObj = SQLiteQuery.getObject()  # Adquiere queryObject del thread desde donde se llama a dbRead()
    dataList = []
    dbFieldNames = []  # DB Names de los campso en keyFieldNames.
    keyFieldNames = []  # keynames de los campos presentes en _dataList
    tblName = tblName.strip()  # Nombre de la tabla
    tblFldNamesDict = getFldName(tblName, '*', 1)  # {fName:fldDBName}. Se necesita para obtener los fldKeyNames.
    cur = queryObj.execute(strSQL)
    if not isinstance(cur, str):
        rows = cur.fetchall()
        dbFieldNames = [j[0] for j in cur.description]  # Solo estos campos (leidos de DB) se incluyen en la DataTable
        for j in range(len(dbFieldNames)):
            for i in tblFldNamesDict:
                if tblFldNamesDict[i] == dbFieldNames[j]:
                    keyFieldNames.append(i)
                    break
    else:
        retValue = DataTable(tblName, [dataList, ], keyFieldNames, dbFieldNames)  # No hay datos: Retorna tabla vacia
        module_logger.error(f'ERR_DB: {cur} - {callerFunction()}')
        return retValue  # Loggea error y retorna tabla vacia

    for j in rows:
        rowDict = {}
        rowList = []
        for i in j:
            fldValue = i
            if fldValue and type(fldValue) is str:  # Chequea si es string y si es string, si tiene json encoding.
                if fldValue.__contains__(']') or fldValue.__contains__('}'):  # TODO: Revisar esta verificacion p/ json
                    try:
                        fldValue = loads(fldValue)
                    except ValueError:
                        pass  # Si loads() falla (no es json encoding), deja fldValue en su valor inicial

            # Qt convierte Nulls en ''. SQLite NULL -> None. La linea abajo iba por compatibilidad,pero ya no hace falta
            # fldValue = fldValue if fldValue is not None else ''

            if mode == 0:
                rowList.append(fldValue)  # Puebla lista con datos de un record de DB.
            else:
                rowDict[dbFieldNames[i]] = fldValue  # Loop para poblar dict. con datos de 1 record de DB
        dataList.append(rowList) if mode == 0 else dataList.append(rowDict)  # Retorna lista de diccionarios.
    if mode == 0:
        if rows and hasattr(rows, '__iter__') and isinstance(rows[0], sqlite3.Row):         # Retorna objeto de tipo DataTable
            retValue = DataTable(tblName, dataList, keyFieldNames, dbFieldNames, fRows=rows)
        else:
            retValue = DataTable(tblName, dataList, keyFieldNames, dbFieldNames)
    else:
        retValue = dataList
    return retValue





def kwargsParseNames(tblName, leMode=0, **kwargs):            # NO FUNCIONA!!!!
    """
    Generates and returns a Dictionary of dictionaries, one dictionary for each tableName passed. Form:
    losDicts[tblName1] = {fName:fldValue,}, losDicts[tblName2]= {fName:fldValue,}, ...
    Intended for use for passing and returning parameters in multiple tables
    @param leMode: 0: Pass all. Checks Table names but not fldNames names. All field names stripped and returned.
                 1: Only DB Fields. Filters only fldNames that are valid DB Field Names.
    @param tblName: a table name.
    @param kwargs: if tblWrite is not provided or not valid: Each dictionary is
                        {tableName : {fieldName1:fieldValue1, fieldName2:fieldValue2,...},} for the corresponding table.
        If tblWrite is provided **kwargs is of the form fieldName1=fieldValue1, fieldName2=fieldValue2,...
    Non-valid names are ignored. If key hdrFields are repeated in a dictionary, the last instance is used.
    If no valid names are found, returns and val dictionary.

    @return: losDicts{} : Dictionary of dictionaries with tblNames as keys and values are dictionaries {fName:fldValue}
    """
    retDict = {}
    if tblName:
        tblName = str(tblName).strip()
        if not getTblName(tblName).__contains__(strError):  # Si no hay error (Nombre de tabla es valido)
            if leMode == 0:  # mode=0 -> Pasa todos los campos. Necesario para parsear campos generados por queries
                retDict = {str(i).strip(): kwargs[i] for i in kwargs}
            else:
                _ = getFldName(tblName, '*', 1)
                for f in _:
                    for i in kwargs:
                        if i.strip() == f:
                            retDict[f] = kwargs[i]      # Retorna SOLO camps en kwargs. Los devuelve validados.
    return retDict






