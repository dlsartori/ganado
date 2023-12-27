from krnl_cfg import *
# from json import dumps, loads
from threading import Event
from krnl_custom_types import getRecords
from krnl_parsing_functions import setRecord

ACTIVITY_LOWER_LIMIT = 15
ACTIVITY_UPPER_LIMIT = 30
ACTIVITY_DAYS_TO_ALERT = 15
ACTIVITY_DAYS_TO_EXPIRE = 365
ACTIVITY_DEFAULT_OPERATOR = None
keyCh = '__'  # Used in Signature and Notifications to create unique field names. Chars MUST be ok with use in DB Names.
oprCh = '__opr'     # Particle added to "fldName" fields in DataTables to store operators strings belonging to "fldName"


class Notifications(object):
    """
    This class implements the Notifications protocols for all objects in the system
    """
    __registerDict = {}        # {UID: notificationObj, } - Used to process and broadcast notifications.

    @classmethod
    def getRegisterDict(cls):
        return cls.__registerDict

    __notificationsPriorities = [1, 2, 3, 4, 5]     # 1: Highest. Not yet implemented.
    __notificationsActions = {'OK': 1, 'Cancel': 2, 'Run Activity': 3}
    __DEFAULT_PRIORITY = 1
    __DEFAULT_INTERVAL = 7 * INTERVAL_DAY      # Default Broadcast Interval: 7 days.
    __DEFAULT_TIMEOUT = 30 * INTERVAL_DAY       # Default Broadcast time: 30 days.
    __DEFAULT_STREAM = STDOUT_HNDL
    __DEFAULT_MSG = None        # 'Notification: Message not set'
    __DEFAULT_TYPE = None
    __DEFAULT_ACTION = 1
    __tblRNName = 'tblRegistroDeNotificaciones'

    @classmethod
    def getObjectTblName(cls):
        return cls.__tblRNName

    def __init__(self, obj=None, **kwargs):
        """
        Creates a notification object but does NOT register the object in __registerDict. This must be done elsewhere
        @param obj: Object for which Notification is created. Used to derive __UID.
        @param kwargs: 'message': str; 'priority': int(1 - 5); 'interval': float (time in seconds)
                       'timeout': float (time in seconds). __enabled goes to 0 after timeout expires.'stream',
                       'type': int (TBD); 'actions': list, as per __notificationsActions dictionary;
                       'enabled': 0: notification is NOT broadcast, no matter what. 1: Notification broadcasted.
                       'comment': str, for DB record.
        TODO: Este codigo se debe correr cuando todos los demas objetos estan ya creados.
        """
        # Primero, toma el UID de **kwargs, si existe. Esto sucede cuando es Inicializacion (se lee UID de DB)
        self.__UID = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('objuid')), None)
        self.__message = next((str(kwargs[j]) for j in kwargs if str(j).lower().__contains__('message')),
                              Notifications.__DEFAULT_MSG)
        if self.__message in (None, False):       #  or obj in (*nones, 0, False):
            self.__isValid = False  # Sin estos 2, notificacion no valida.
            self.__message = None
            self.__enabled = 0  # Toggle Flag. 0=False: La Notificacion NO se emite (Valores 0/1 para almacenar en DB).
        else:
            if self.__UID is None:
                try:      # Si obj!=None genera UID a partir de DB (UID='tblName__fldID').
                    tblName = obj.tblObjName()
                    self.__UID = str(tblName + keyCh + str(obj.getID))  # __UID de forma 'tblName__fldID' -> viene de DB
                except (NameError, AttributeError, TypeError, RuntimeError):
                    if obj is not None:
                        self.__UID = str(id(obj))  # No tiene ObjectsTblName asociado-> Objeto volatil (no existe en DB)
                    else:
                        self.__UID = str(id(self))  # No se paso objeto -> Crea notificacion"Suelta" (sin owner object)
            self.__priority = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('priority')),
                                   Notifications.__DEFAULT_PRIORITY)
            self.__broadcastInterval = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('interval')),
                                            Notifications.__DEFAULT_INTERVAL)
            self.__broadcastTimeout = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('timeout')),
                                           Notifications.__DEFAULT_TIMEOUT)
            self.__broadcastStream = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('stream')),
                                          [Notifications.__DEFAULT_STREAM, ])  # Stream es list. Facilita procesamiento
            if type(self.__broadcastStream) is not list:
                if type(self.__broadcastTimeout) in (str, int):
                    self.__broadcastStream = [self.__broadcastStream, ]
                else:
                    self.__broadcastStream = [Notifications.__DEFAULT_STREAM, ]  # Stream no valido: setea default
            self.__broadcastNowFlag = False  # True: Indica a background threads que se debe emitir la notificacion
            self.__broadcastNowEvent = Event()   # Forma alternativa de generar broadcast. Events(), por si se necesita.
            self.__notifObjectTblName = str(self.__UID)[:str(self.__UID).find(keyCh)] \
                if str(self.__UID).__contains__(keyCh) else None
            self.__notifObjectID = int(str(self.__UID)[str(self.__UID).find(keyCh) + len(keyCh):]) \
                if str(self.__UID).__contains__(keyCh) else None
            self.__type = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('type')),
                                          Notifications.__DEFAULT_TYPE)
            self.__actions = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('action')),
                                          [Notifications.__DEFAULT_ACTION, ])
            self.__comment = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('comment')), '')
            self.__isValid = True
            # True: Notificacion se emite. TODO: Actualizar en DB con locks (Op. ATOMICA) cada vez que se modifique
            self.__enabled = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('enable')), 1)
        super().__init__()

    @classmethod
    def notificationCreate(cls, obj=None, init=False, *args, **kwargs):
        """
        creates one notification object from passed arguments. Stores it in DB and updates notifications structures.
        Can be used with map() for multiple Notifications creation.
        @param obj: Object for which Notification is created.
        @param init: True -> InitializationActivity process ongoing. Creates objects in memory but DOESN'T write to DB.
        @param args:
        @param kwargs: All notification parameters passed in kwargs
        @return: Notification Object created or None if none created.
        """
        mandatoryArgs = ('message', )
        if not set(mandatoryArgs).issubset(set([str(j).strip().lower() for j in kwargs])): #   or obj is None:
            retValue = f'ERR_INP_InvalidArgument or missing: {mandatoryArgs} - {callerFunction()}'
            print(f'{moduleName()}({lineNum()}) - {retValue}')
        else:
            # Notifications.__registerDict = {}            # Inicializa a {} __notificationsRegisterDict. Por ahora, no.
            notific = Notifications(obj, **kwargs)  # Crea objeto Notifications

            if notific.getID not in Notifications.__registerDict:       # Si ya existe el objeto Notificacion, Ignora
                Notifications.__registerDict[notific.getID] = notific  # Registra {__UID: obj, } en __registerDict
                # TODO(cmt): Notificacion a DB (Registro De Notificaciones), solo si NO es init. Ademas, NO almacena
                #  en DB las notificaciones de objetos NO volatiles (por ahora, al menos)
                if not init and notific.getID.__contains__(keyCh):  # a DB solo Notificaciones de obj NO volatiles
                    tblNotif = DataTable(Notifications.__tblRNName)
                    tblNotif.setVal(0, fldObjectUID=notific.__UID, fldTimeStamp=time_mt(mode='datetime'),
                                    fldMessage=notific.__message, fldType=notific.__type,
                                    fldPriority=notific.__priority, fldFK_UserID=sessionActiveUser,
                                    fldBroadcastInterval=notific.__broadcastInterval, fldEnabled=notific.__enabled,
                                    fldBroadcastStream=notific.__broadcastStream, fldActions=notific.__actions,
                                    fldBroadcastTimeout=notific.__broadcastTimeout, fldComment=notific.__comment)
                    result = setRecord(Notifications.__tblRNName, **tblNotif.unpackItem(0))
                    if type(result) is str:
                        retValue = f'ERR_DB_Write: {result}. {Notifications.__tblRNName} - {callerFunction()}'
                        print(f'{moduleName}({lineNum()}) - {retValue}')
                    else:
                        retValue = notific                                  # Retorna objeto
                else:
                    retValue = notific
            else:   # Si ya existe el UID en __registerDict retorna el obj. Notification existente, previa validacion.
                # Validacion de existencia del objeto en memoria (confirma existencia de objetos volatiles)
                try:  #
                    retValue = Notifications.getRegisterDict()[notific.getID]
                except (NameError, AttributeError, TypeError, RuntimeError, MemoryError, KeyError):
                    # Existe UID pero el objeto ya no existe (obj. volatil que se destruyo): Se elimina de registerDict
                    _ = Notifications.__registerDict.pop(notific.getID)
                    retValue = None         # Retorna None porque no se creo ningun objeto, sino que se elimino uno.
        return retValue

    @classmethod
    def notificationInitialize(cls):
        """
        Intializes Notifications on system start-up by reading obj_data from table [Registro De Notificaciones] and setting
        up __registerDict{}
        @return: Total number of Notification objects created (int). 0 if none created. errorCode (str) if error.
        """
        temp = getRecords(cls.getObjectTblName(), '', '', None, '*', fldActive=1) # Only active notifications from DB
        if type(temp) is str:
            retValue = f'ERR_DB_ReadError: {temp} - {callerFunction()}'
            print(f'{moduleName()}({lineNum()}) - {retValue}')
        else:
            retValue = 0
            if temp.dataLen:
                for i in range(temp.dataLen):
                    if len(temp.dataList[i]) > 0:
                        uid = str(temp.getVal(i, 'fldObjectUID'))
                        msg = temp.getVal(i, 'fldMessage')
                        priority = temp.getVal(i, 'fldPriority')
                        bcastInterval = temp.getVal(i, 'fldBroadcastInterval')
                        bcastTimeout = temp.getVal(i, 'fldBroadcastTimeout')
                        actions = temp.getVal(i, 'fldActions')
                        enable = temp.getVal(i, 'fldEnabled')
                        bcastStream = temp.getVal(i, 'fldBroadcastStream')
                        notifType = temp.getVal(i, 'fldType')
                        cmt = temp.getVal(i, 'fldComment')
                        _ = Notifications.notificationCreate(True, objuid=uid, message=msg, priority=priority,
                                                             comment=cmt, interval=bcastInterval, timeout=bcastTimeout,
                                                             actions=actions, stream=bcastStream, enabled=enable,
                                                             type=notifType)
                        if type(_) is not str:
                            retValue += 1           # Retorna el numero total de obj. Notificaciones creado
        return retValue                         # retValue >= 0: InitializationActivity complete, with __registerDict populated

    @classmethod
    def getObject(cls, uid):
        """
        Returns notification object for a given notification uid
        @param uid: __UID of required Notification object.
        @return: __registerDict[j] (Notification Object)
        """
        return Notifications.__registerDict[uid] if uid in Notifications.__registerDict else None

    @property
    def getID(self):
        return self.__UID

    @property
    def broadcastFlag(self):
        return self.__broadcastNowFlag

    @broadcastFlag.setter
    def broadcastFlag(self, val):
        self.__broadcastNowFlag = True if val not in (*nones, 0, False) else False

    @property
    def broadcastEvent(self):
        return self.__broadcastNowEvent

    @broadcastEvent.setter
    def broadcastEvent(self, val):
        self.__broadcastNowEvent = val if type(val) is Event else False

    def processUpdates(self, *args, **kwargs):
        """
        Processes the passed obj_data structure with updates and sends notifications. The obj_data structure is initialized
        by the background functions.
        @param args:
        @param kwargs:
        @return:
        """
        return

    def broadcast(self, *args, **kwargs):
        """
        Sends a notification to streams specified in obj.__streamList
        @param args:
        @param kwargs:
        @return:
        """
        if self.__broadcastNowFlag or self.__broadcastNowEvent.is_set():
            pass
            # send notification to all streams in streamList. Send the message in obj.__message

        return

