from custom_types import *
from krnl_config import time_mt, DAYS_MULT, USE_DAYS_MULT, tables_and_binding_objects, tables_and_methods

# import pydantic           ### pydantic not used anymore for now (Nov-23)
# from pydantic import (BaseModel, validator, typing, ValidationError, constr, conint, conlist, PositiveInt,
#                       NegativeInt, PositiveFloat)

def moduleName():
    return str(os.path.basename(__file__))

# Initializes set used to cleanup __outerAttr dictionaries in Activity objects.
if '_classes_with_Activity' not in globals():
    globals()['_classes_with_Activity'] = set()

class EntityObject(object):
    __slots__ = ('__ID', '__isValidFlag', '__isActiveFlag', '__supportsMemoryData', '_lastInventory',
                 '_lastStatus', '_lastLocalization', '_lastCategoryID', '__daysToTimeout', '__timeoutFlag')

    # Runs _creatorActivityObjects() method to create Activity Objects and the @property methods to invoke their code.
    # Populates tables_and_methods, tables_and_binding_objects to use in IntervalTimer threads.
    def __init_subclass__(cls, **kwargs):
        super.__init_subclass__()
        if hasattr(cls, '_activityObjList'):  # Si tiene definidos _activityObjList, _myActivityClass ejecuta funcion.
            cls._creatorActivityObjects()     # Crea objectos Activity y sus @property names a ser usados en el codigo.
            if cls._activityObjList:
                # _classes_with_Activity used to clean __outerAttr dictionaries: removes entries for killed threads.
                globals()['_classes_with_Activity'].add(cls)
                # print(f'&&&&&&&& Classes with Activity: {globals()["_classes_with_Activity"]} - {moduleName()}-{lineNum()}')

        # Load ALL elements of the dictionary that have value == cls.
        for k, v in tables_and_binding_objects.items():     # k: Table_Name; v: class name ('Animal', 'Tag', etc.)
            if cls.__name__ == v:
                # 1. Replaces Object_Name with object (ex.: 'Animal' with <class Animal>) in tables_and_binding_objects.
                tables_and_binding_objects[k] = cls
                # 2. Gets method/function from Method_Name via getattr()
                if k in tables_and_methods:
                    tables_and_methods[k] = getattr(cls, tables_and_methods[k]) or None  # A bound method is returned.

        if hasattr(cls, 'registerKindOfAnimal'):
            if cls.getClassesEnabled()[cls.getAnimalClassID()]:
                cls.registerKindOfAnimal()

        return None



    # TODO(cmt): The whole point of this method is to be called with the right _activityObjList, _myActivityClass
    #   attributes based on cls, so that it is defined in one place only (here).
    @classmethod
    def _creatorActivityObjects(cls):
        """ Creates Animal Activity Objects (Instances of ActividadInventarioAnimal, ActividadLocalizacionAnimal, etc)
        by calling cls().
        Also creates class methods using ActivityFactory class and assigns them as @property objects to the classes.
        These are the properties used in the code (inventory, status, category, tags, etc).
        Called from __init_subclass__() method in EntityObject class, WHEN THE CLASS IS BEING CREATED.
        """
        if cls._activityObjList:
            return None  # this function must only run once.

        cls._creator_run = True  # Never run this code again.
        missingList = []
        # Creates InventoryAnimalActivity, StatusAnimalActivity, MoneyActivity singleton objects. Puts them in a list.
        # __class_register is a list that holds all subclasses with a valid __method_name for _myActivityClass.
        for c in cls._myActivityClass.get_class_register():
            # Separate processing for Generic Activity objects as this is class spawning multiple Activity objects.
            # Processing GenericActivityAnimal first in the 'if' enables a Generic Activity being overriden.
            # 'GenericActivityDevice', 'GenericActivityPerson' --> Future development of generic activities.
            if c.__name__ in ('GenericActivityAnimal', 'GenericActivityDevice', 'GenericActivityPerson'):
                genericActivityObjects = c._createGenericActivityObjects()
                if genericActivityObjects:
                    cls._activityObjList.extend(genericActivityObjects)
            else:
                try:
                    cls._activityObjList.append(c())  # Creates Activity object and appends to Activity object List.
                except (AttributeError, TypeError, NameError) as e:
                    krnl_logger.error(f'ERR_SYS_Error in Activity object creation {callerFunction(getCallers=True)}'
                                      f'({lineNum()}): class {c} - error: {e}')
                    missingList.append(c)

        # Loops _activityObjList to create all callable properties (inventory, status, etc.) as attributes in cls.
        for j in cls._activityObjList:  # List of Activty objects (InventoryAnimalActivity, StatusAnimalActivity, etc)
            # j are callable instances (their classes implement __call__())
            if j._decoratorName:  # se salta los None. Solo crea metodos cuando __method_name esta definido en la clase.
                try:
                    # Creates a property (inventory, status, etc., defined by _decoratorName) out of the Activity object
                    # which, by implementing __call__() allows to initialize outerObject when the property is invoked.
                    """ property(j) below IS CRITICAL in order to execute as "inventory.get()": the property calls its
                    # fget() method and this in turns executes __call__() in the object instance assigned to property.
                    # Otherwise it would have to be "inventory().get()" (__call__() being invoked by the instance name 
                    # followed by parenthesis). """
                    setattr(cls, j._decoratorName, property(j))
                    #  setattr(cls, j._decoratorName, property(ActivityMethod(j)))      # Old way of setting property.
                except (AttributeError, TypeError, NameError):
                    pass
        d = {j._decoratorName: j.__class__.__name__ for j in cls._activityObjList}
        print(f'Activity Objects for {cls.__name__} are: {d}', dismiss_print=DISMISS_PRINT)
        return None

        #                              ### Original code for the loop, using ActivityMethod class ###
        # Loops _activityObjList to create all callable properties (inventory, status, etc.) as attributes in cls.
        # for j in cls._activityObjList:  # List of Activty objects (InventoryAnimalActivity, StatusAnimalActivity, etc)
        #     if j._decoratorName:  # se salta los None. Solo crea metodos cuando __method_name esta definido en la clase.
        #         try:
        #             # Creates a callable ActivityMethod object out of Activity object j, converts the callable object to
        #             # property and creates a cls attribute with name (inventory, status, etc) defined by _decoratorName.
        #             setattr(cls, j._decoratorName, property(ActivityMethod(j)))
        #         except (AttributeError, TypeError, NameError):
        #             pass
        # d = {j._decoratorName: j.__class__.__name__ for j in cls._activityObjList}
        # print(f'Activity Objects for {cls.__name__} are: {d}', dismiss_print=DISMISS_PRINT)
        # return None

    tblSysParams = getRecords('tblSysDataParametrosGenerales', '', '', None, '*')
    indexDefaultTO = tblSysParams.getCol('fldName').index('Dias Para Timeout')
    __defaultDaysToTO = 0   # int(tblSysParams.getVal(indexDefaultTO, 'fldParameterValue'))
    __defaultDaysToTO = __defaultDaysToTO if __defaultDaysToTO > 0 else 366

    @staticmethod
    def ActivityCleanup(thread_id):
        """
        Traverses all Activity objects listed in _activityObjList and removes the entry for thread_id in the __outerAttr
        dictionary for all objects present.
        Called by the code that kills thread_id thread.
        This is to prevent the __outerAttr from growing out of control as threads are started and killed.
        @param thread_id:
        @return: None
        """
        if "_classes_with_Activity" in globals():
            for cls in globals()["_classes_with_Activity"]:
                if hasattr(cls, '_activityObjList') and cls._activityObjList:
                    for o in cls._activityObjList:
                        if o._pop_outerAttr_key(thread_id):
                            print(f'...Removing thread_id {thread_id} for {cls.__name__}.{o._decoratorName}.'
                                  f' - {moduleName()}-{lineNum()}.', dismiss_print=DISMISS_PRINT)
        return None


    def __init__(self, ID_Obj, isValid, isActive, *args, **kwargs):
        if isinstance(ID_Obj, (int, str)):
            self.__ID = ID_Obj          # recordID es ID_Animal, ID_Caravana, ID_Persona, ID_Dispositivo, ID_Item
            self.__isValidFlag = isValid or True          # Activo / No Activo de tabla Data Status
            # !=None para Animales, None para el resto de los objetos.
            self.__isActiveFlag = isValid * isActive  # Active es True/False SOLO si __isValidFlag = True
            self.__supportsMemoryData = next((kwargs[j] for j in kwargs if j.lower().startswith('memdata')), None) or False
            self._lastInventory = next((kwargs[j] for j in kwargs if j.lower().startswith('lastinvent')), None) # LISTA!!
            self._lastStatus = next((kwargs[j] for j in kwargs if j.lower().startswith('laststatus')), None)  # LISTA!!
            self._lastLocalization = next((kwargs[j] for j in kwargs if j.lower().startswith('lastlocaliz')), None) # LISTA!!
            self._lastCategoryID = next((kwargs[j] for j in kwargs if j.lower().startswith('lastcategory')), None) # LISTA!!
            self.__daysToTimeout = next((kwargs[j] for j in kwargs if j.lower().startswith('daystotimeout')),
                                        EntityObject.__defaultDaysToTO)  # Periodo en dias para Timeout
            self.__timeoutFlag = False      # True cuando (now()-self__lastInventory).days > obj.__daysToTimeout
        else:
            # Objeto isvalid = False si no se pasan los recordID necesarios.
            self.__isValidFlag = False
            self.__isActiveFlag = False
        super().__init__()

    @property
    def isValid(self):    # Retorna estado valido/no valido de un objeto. Objeto NO valido cuando falla escritura en DB.
        return self.__isValidFlag

    @isValid.setter
    def isValid(self, state):
        """
        Sets __isValidFlag for an object
        @param state: True or False. state MUST BE a valid val, otherwise __isValidFlag remains unchanged.
        @return: NADA, as per Python requirements.
        """
        self.__isValidFlag = bool(state)
        self.__isActiveFlag = self.__isValidFlag * self.__isActiveFlag  # Si isValid es 0, isActive debe hacerse 0.

    @property
    def isActive(self):
        return self.__isActiveFlag

    @isActive.setter
    def isActive(self, state):
        """
        Sets __isActiveFlag for an object. if __isValidFlag == 0, __isActiveFlag will be set to 0
        @param state: True or False. state MUST BE a valid val, otherwise __isActiveFlag remains unchanged.
        @return: NADA, as per Python requirements.
        """
        self.__isActiveFlag = 0 if not state else 1 * self.__isValidFlag

    @property
    def getID(self):            # Se mantiene por compatibilidad. Usar @property ID.
        return self.__ID if self.__isValidFlag is not False else None

    @property
    def ID(self):
        return self.__ID if self.__isValidFlag is not False else None


    def setID(self, val):
        """
        Sets the UUID field for object. Used to update UUID value when resolving repeat records/objects.
        @return: False if val is not a valid UUID.
        """
        try:
            _ = UUID(val)
        except(SyntaxError, TypeError, ValueError):
            return False
        else:
            self.__ID = str(val.hex)
            return True


    @property
    def supportsMemData(self):
        return self.__supportsMemoryData

    @property
    def lastInventory(self):
        if self.__supportsMemoryData and self._lastInventory:         # Retorna Fecha del Ultimo inventario
            return self._lastInventory[0]
        return None

    @lastInventory.setter
    def lastInventory(self, val):
        if self.__supportsMemoryData:
            self._lastInventory = val            # TODO(cmt): val = [varValue(datetime), eventTime(datetime)]
        if val:
            if USE_DAYS_MULT:
                self.__timeoutFlag = (time_mt()-val[0].timestamp()) * DAYS_MULT >= self.__defaultDaysToTO
            else:
                self.__timeoutFlag = (time_mt('datetime')-val[0]).days >= self.__defaultDaysToTO

    @property
    def lastStatus(self):
        if self.__supportsMemoryData and self._lastStatus:
            return self._lastStatus[0]     # Retorna Status actual del Objeto
        return None

    @lastStatus.setter
    def lastStatus(self, val):
        if self.__supportsMemoryData:
            self._lastStatus = val          # val = [varValue(status), eventTime(datetime)]

    @property
    def lastLocalization(self):
        if self.__supportsMemoryData and self._lastLocalization:
            return self._lastLocalization[0]
        return None

    @lastLocalization.setter
    def lastLocalization(self, val):
        if self.__supportsMemoryData:
            self._lastLocalization = val         # val = [varValue(Geo Object), eventTime(datetime)]

    @property
    def lastCategoryID(self):
        if self.__supportsMemoryData and self._lastCategoryID:
            return self._lastCategoryID[0]       # val = [varValue(categoryID), eventTime(datetime)]
        return None

    @lastCategoryID.setter
    def lastCategoryID(self, val):
        if self.__supportsMemoryData:
            self._lastCategoryID = val           # val = [varValue(categoryID), eventTime(datetime)]


    @property
    def timeout(self):
        return self.__timeoutFlag

    @timeout.setter
    def timeout(self, val):
        self.__timeoutFlag = bool(val)

    @property
    def daysToTimeout(self):
        return self.__daysToTimeout

    @daysToTimeout.setter
    def daysToTimeout(self, val):
        try:
            self.__daysToTimeout = val if val > 0 else 365
        except TypeError:
            self.__daysToTimeout = 366


    def updateTimeout(self, **kwargs):
        """
        Timeout arguments may vary as they are assigned INDIVIDUALLY - they are an Instance Property.
        Function called by default daily.
        @return: idObject if in Timeout or False if timeout not reached. None: in Timeout, but self already passed up.
        """
        if self.lastInventory:
            if USE_DAYS_MULT:        # TODO(cmt): if para utilizar formula correcta cuando se usa mutiplicador de dias
                daysSinceLastInv = int((time_mt()-self.lastInventory.timestamp()) * DAYS_MULT)
            else:
                daysSinceLastInv = int((time_mt('datetime') - self.lastInventory).days)  # Esta es la correcta

            krnl_logger.debug(f'=====> Animal: {self.ID}, lastInv: {self.lastInventory} / daysSinceLastInv:{daysSinceLastInv}')

            if daysSinceLastInv > self.__daysToTimeout:
                # if not self.__timeoutFlag:
                self.__timeoutFlag = True
                return [self, self.lastInventory, daysSinceLastInv]   # TODO: ARREGLAR al terminar el debugging (retornar True/False)
                # return True
            else:
                self.__timeoutFlag = False
                krnl_logger.debug(f'=====> NO HAY TIMEOUT!! Animal: {self.ID}, daystoTimeOut: {self.__daysToTimeout} ')
                # return [self, (self.lastInventory, int(daysSinceLastInv))]
        # else:
        #     krnl_logger.debug(f'=====> NO HAY lastInventory!! Animal: {self.ID}, lastInv: {self.lastInventory} ')
        return None
