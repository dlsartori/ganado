from krnl_cfg import *
from custom_types import Activity

ACTIVITY_LOWER_LIMIT = 15
ACTIVITY_UPPER_LIMIT = 30
ACTIVITY_DAYS_TO_ALERT = 15
ACTIVITY_DAYS_TO_EXPIRE = 365
ACTIVITY_DEFAULT_OPERATOR = None
keyCh = '__'  # Used in Signature and Notifications to create unique field names. Chars MUST be ok with use in DB Names.
oprCh = '__opr'     # Particle added to "fldName" fields in DataTables to store operators strings belonging to "fldName"


def moduleName():
    return str(os.path.basename(__file__))



# Funcion usada en ActivitySignature.match() para asignar Actividades Ejecutadas a Activ. Programadas en base al match()
def eval_expression(input_string, allowed_names):
    # print(f'Locals: {locals()}')
    try:
        code = compile(input_string, "<string>", "eval")
    except(SyntaxError, TypeError, AttributeError, EOFError, KeyError, KeyboardInterrupt, SystemError, IndentationError,
           OSError, NameError, ZeroDivisionError, UnboundLocalError, ImportError, FloatingPointError, MemoryError):
        print(f'ERR_Sys: Compile error. krnl_dataTable({lineNum()}) - {callerFunction()}')
        return False
    for name in code.co_names:
        if name not in allowed_names:
            raise NameError(f"Use of {name} not allowed")
    retValue = eval(code, {"__builtins__": {}}, allowed_names)
    return retValue
# ==================== Va en este modulo por seguridad ( para reducir disponibilidad de la funcion) ==================


GENERIC_OBJECT_ID_LIST = [1, 400]    # TODO: TEMPORAL ARREGLAR. Llevarlo a una tabla en Activities seteada correctamente


class ActivitySignature(object):
    """
    Activities Signatures to identify specific activities as a completion activity for Programmed Activities.
    This class implements comparison criteria and checks for Activities in order to assign an executed Activity to a
    pre-existing Programmed Activity
    """
    # Default Allowed Names for eval() function, used in field values matching.
    __allowedNamesDefault = {'set': set, 'issubset': set.issubset, 'lower': str.lower, 'find': str.find,
                             '__contains__': None}
    __tblProgName = Activity.getTblProgName()  # Accede a parametro global de nombre de tblDataProgramacionDeActividades

    @property
    def tblProgName(self):
        return str(self.__tblProgName)

    def __init__(self, signatureKey: str, idActivity, activityName, isProgActivity, *args, **kwargs):
        """
        @param kwargs: 'signatureDataDict': Parameter list to pass to the object signature. {key: (val, operator), }
                       'objIDList': list of Object ID for which the signature is defined.
        @return: NADA
        """
        self.__isValidFlag = True
        self.__signatureKey = signatureKey
        self.__tblRAName = signatureKey[:signatureKey.find('-')]
        self.__idActividadRA = idActivity             # idActividadRA al que este Signature pertenece.
        self.__activityName = activityName
        self.__dataDict = {}           # {fldName: fldValue, } Tabla con TODOS los fldName:fldValues del signature

        # Generado en set() cuando isProgActivity=False. Los keyName deben ser identicos a los keyName de __activityData
        self.__operatorsDict = {}
        self.__objectIDList = []
        self.__isProgActivity = isProgActivity
        if 'progActivityData' in kwargs and len(kwargs['progActivityData']) > 0:     # Crea copias locales
            # k es un key de forma tblName__fldName
            self.__dataDict = {k: kwargs['progActivityData'][k] for k in kwargs['progActivityData']}
        if 'signatureOperators' in kwargs and len(kwargs['signatureOperators']) > 0:
            self.__operatorsDict = {k:  kwargs['signatureOperators'][k] for k in kwargs['signatureOperators']}
        if 'fldObjectIDList' in kwargs and len(kwargs['fldObjectIDList']) > 0:
            self.__objectIDList = [j for j in kwargs['fldObjectIDList']]  # Crea copia local.
        if len(self.__objectIDList) == 0 or len(self.__dataDict) == 0 or type(self.__signatureKey) is not str \
                or self.__tblRAName not in Activity._tblRANames():
            self.__isValidFlag = False
            return

        key = next((k for k in self.__dataDict
                    if str(k).strip().lower().__contains__(self.tblProgName.lower()+keyCh+'clasedeanimal')), None)
        self.__animalClassID = self.__dataDict[key] if key is not None else None
        key = next((k for k in self.__dataDict
                    if str(k).strip().lower().__contains__(self.tblProgName.lower()+keyCh+'flddate')), None)
        try:
            self.__programmedDate = datetime.strptime(self.__dataDict[key], fDateTime)
        except(ValueError, TypeError):
            self.__programmedDate = None        # TODO: ver el manejo de este error, si programmedDate no es valida...
        key = next((k for k in self.__dataDict
                    if str(k).strip().lower().__contains__(self.tblProgName.lower()+keyCh+'fldwindowlo')), None)
        self.__windowLowerLimit = self.__dataDict[key] if key is not None else ACTIVITY_LOWER_LIMIT
        key = next((k for k in self.__dataDict
                    if str(k).strip().lower().__contains__(self.tblProgName.lower()+keyCh+'fldwindowup')), None)
        self.__windowUpperLimit = int(self.__dataDict[key]) if key is not None else ACTIVITY_UPPER_LIMIT
        key = next((k for k in self.__dataDict
                    if str(k).strip().lower().__contains__(self.tblProgName.lower()+keyCh+'flddaystoexp')), None)
        self.__daysToExpire = int(self.__dataDict[key]) if key is not None else ACTIVITY_DAYS_TO_EXPIRE
        key = next((k for k in self.__dataDict
                    if str(k).strip().lower().__contains__(self.tblProgName.lower()+keyCh+'fldflagexpirat')), None)
        self.__flagExpiration = int(self.__dataDict[key]) if key is not None else 1
        return

    @property
    def isValid(self):
        return self.__isValidFlag

    @isValid.setter
    def isValid(self, val):
        self.__isValidFlag = False if val in [*nones, 0, False] else True

    @property
    def signatureKey(self):
        return self.__signatureKey

    @property
    def activitID(self):
        return self.__idActividadRA

    @property
    def dataDict(self):
        return self.__dataDict

    @property
    def activityName(self):
        return self.__activityName

    @property
    def objIDList(self):
        return self.__objectIDList

    @property
    def tblRAName(self):
        return self.__tblRAName

    @property
    def animalClassID(self):
        return self.__animalClassID

    @property
    def operators(self):
        return self.__operatorsDict

    @operators.setter
    def operators(self, opDict: dict):
        self.__operatorsDict = {k: opDict[k] for k in opDict}

    def match(self, signatureDict: dict, filterObjects=True, **kwargs):
        """
        Matches obj to a list of signatures from Programmed Activities. By default, in signatureRegisterItem.
        @param filterObjects: True -> Creates a signature dictionary with filtered objects.
        @param signatureDict: {signatureKey: signatureObject, } Dictionary to match performed Activity conditions to
                                dictionary of programmed activities.
        @param kwargs: 'filterActivityName', 'filterAnimalClass'
        @return: Dictionary {signatureKey: signatureObject, } for matching Activities. {} if none found.
        """
        if self.__isProgActivity or not self.isValid:
            return {}            # Sale si obj es signature de Actividad Programada. Debe ser Actividad Ejecutada.

        # Primero se filtra el diccionario pasado como parametro
        filteredDict1 = signatureDict
        # 1. Filtra por Nombre Actividad y __tblRAName.
        if next((j for j in kwargs if str(j).strip().lower().__contains__('filteractiv')), None) is not None:
            filteredDict1 = {k: signatureDict[k] for k in signatureDict if signatureDict[k].__activityName ==
                             self.__activityName and signatureDict[k].__tblRAName == self.__tblRAName}

        # 2. Filtra por tipo de Animal (si aplica)
        if next((j for j in kwargs if str(j).strip().lower().__contains__('filteranimal')), None) is not None:
            if self.__animalClassID is not None:
                filteredDict1 = {k: filteredDict1[k] for k in filteredDict1 if filteredDict1[k].animalClassID ==
                                 self.__animalClassID}
        filteredDict2 = filteredDict1
        # 3. Matching de idObj: Filtra todos los signatures cuyos signatureObjID contienen a obj.__objectIDList
        if filterObjects is True:
            filteredDict2 = {j: filteredDict1[j] for j in filteredDict1 if
                             set(self.__objectIDList).issubset(set(filteredDict1[j].objIDList))}
        if len(filteredDict2) == 0:
            return {}

        # 4. Se filtran los intervalos de tiempo
        eventDate = None
        if self.__programmedDate in nones:
            if type(self.objIDList) is int and self.objIDList in GENERIC_OBJECT_ID_LIST:
                # TODO: GENERIC_OBJECT_ID_LIST Provisorio. Arreglar!!
                pass            # Es Actividad Permanente: No tiene FechaHora definida de ejecucion.
            else:
                return {}
        else:
            eventDate = self.__programmedDate
        programmedDateDict = {}          # Primero Fecha Programada  programmedDatedict = {signatureKey: date(string), }
        if eventDate is not None and self.operators['fldDate'] in nones:
            # TODO (cmt): Procesa este bloque SOLO si eventDate!=None Y si fldDate NO TIENE operador aisgnado
            for key in filteredDict2:
                programmedDateDict[key] = next((filteredDict2[key].dataDict[j] for j in filteredDict2[key].dataDict
                                                if str(j).strip().lower().__contains__('flddate')), None)
            if len(programmedDateDict) == 0:
                return {}
            else:                               # convierte fechas string a datetime
                programmedDateDict = {key: createDT(programmedDateDict[key]) for key in programmedDateDict}
            windowLowerDict = {}                # Luego Limite inferior del Intervalo
            for key in filteredDict2:
                windowLowerDict[key] = next((filteredDict2[key].dataDict[j] for j in filteredDict2[key].dataDict
                                             if str(j).strip().lower().__contains__('fldwindowlow')), None)
                windowLowerDict[key] = int(windowLowerDict[key]) if windowLowerDict[key] is not None \
                    else ACTIVITY_LOWER_LIMIT
            windowUpperDict = {}                # Luego Limite superior del intervalo
            for key in filteredDict2:
                windowUpperDict[key] = next((filteredDict2[key].dataDict[j] for j in filteredDict2[key].dataDict
                                             if str(j).strip().lower().__contains__('fldwindowupp')), None)
                windowUpperDict[key] = int(windowUpperDict[key]) if windowUpperDict[key] is not None \
                    else ACTIVITY_UPPER_LIMIT
            intervalsDict = {}
            lowr, uppr = 0, 1                   # intervalsDict = {signatureKey: (lowerLimit, upperLimit), }
            for key in filteredDict2:
                intervalsDict[key] = (programmedDateDict[key] - timedelta(windowLowerDict[key]).days,
                                      programmedDateDict[key] + timedelta(windowUpperDict[key]).days)

        # Diccionario {signatureKey: signature, } con eventDate comprendido entre (lowerLimit y upperLimit)
            filteredDict3 = {key: filteredDict2[key] for key in filteredDict2 if intervalsDict[key][lowr] <=
                             eventDate <= intervalsDict[key][uppr]}
            print(f'filteredDict3: {(filteredDict3[j].__dict__() for j in filteredDict3)}')  # A ver que hay hasta aqui
            if len(filteredDict3) == 0:
                return {}
            else:
                processDict = filteredDict3
        else:
            processDict = filteredDict2
        # 5. Aplica Operadores. match() debe recibir el diccionario  obj.__operators2args completo y valido!!
        if len(self.operators) == 0:
            return {}
        resultsDict = {}
        allowedNames = kwargs['allowedNames'] if 'allowedNames' in kwargs else ActivitySignature.__allowedNamesDefault
        # ABAJO:
        # fld es dictkey de forma 'tblName__fldName': dataDict[fld]-> field val; operators[fld]-> operatorString
        # key es un dictkey de tipo signatureKey: filteredDict3[key]-> signatureObj
        for fld in self.dataDict:
            op = str(self.operators[fld]).strip().lower() if fld not in Activity.getOperatorNullFields() else None
            operand1 = f"'{self.dataDict[fld]}'" if type(self.dataDict[fld]) is str else signatureDict[fld]
            for key in processDict:
                operand2 = f"'{processDict[key].dataDict[fld]}'" \
                    if type(processDict[key].dataDict[fld]) is str else processDict[key].dataDict[fld]
                if op.__contains__('issubset'):
                    evalString = f'set({operand1}).issubset(set({operand2}))'
                elif op in allowedNames:
                    evalString = f'{operand1}.{op}' + (f'({operand2})' if operand2 not in nones else '()')
                elif op not in nones:
                    evalString = f'{operand1} {op} {operand2}'
                else:
                    break
                # Genera Dict {signatureKey: signatureObj, } con solo con los signatures que dan match()=True
                evalResult = eval_expression(evalString, allowedNames)       # Call to eval() with evalString code
                if op not in nones and evalResult:
                    resultsDict[key] = processDict[key]
        # resultsDict {signatureKey, signatureObj, } de los signatures que tienen match con obj.signature
        return resultsDict
