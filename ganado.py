from krnl_tm import *
from krnl_cfg import *
from krnl_entityObject import *
from krnl_asset import Asset
from krnl_tag import *
# from animal import *
from krnl_bovine import Bovine
from inspect import currentframe, getframeinfo
# Usage
if __name__ == '__main__':                  # vamos a practicar con decorators

    class FirstFactory:
        def __new__(cls, *args, **kwargs):
            instance = super().__new__(args[0])
            return instance


    class Animal(Asset):
        __objClass = 3
        __objType = 1

        __activityClasses = {}      # {animalKind(string): class, } -> Cada class de Activity() inserta aqui sus datos
        activityProperties = {}   # {animalKind(string): propertyName, }
        @staticmethod
        def getActivityClasses():
            return Animal.__activityClasses

        @staticmethod
        def registerActivity(cls, propName):
            Animal.__activityClasses[cls.__class__.__name__] = cls
            Animal.activityProperties[cls.__class__.__name__] = propName

        temp = getRecords('tblAnimalesActividadesNombres', '', '', None, 'fldID', 'fldName', 'fldFlag')
        __activitiesDict = {}
        __isInventoryActivity = {}  # {Nombre Actividad: _isInventoryActivity(1/0), }
        for j in range(temp.dataLen):
            __activitiesDict[temp.dataList[j][1]] = temp.dataList[j][0]
            __isInventoryActivity[temp.dataList[j][1]] = temp.dataList[j][2]

        def __init__(self, ID, isValidFlag, isActiveFlag, *tags, **kwargs):
            self.__myTags = []
            kwargs = kwargsStrip(**kwargs)
            dob = kwargs.get('fldDOB')
            if not isinstance(dob, datetime):       # Si no se pasa obj. datetime asume que es str y trata de convertir
                try:
                    dob = datetime.strptime(dob, fDateTime)
                except(TypeError, ValueError):
                    dob = None

            if type(ID) is not int or ID <= 0 or kwargs.get('fldMF').upper() not in ('M', 'F') or not dob or \
                    not isValidFlag:
                isValid = False
                isActive = False
                myID = 0
                self.__dob = None
                self.__mf = None
                self.__flagCastrado = None
                self.__animalClass = None
                self.__animalRace = None
                self.__countMe = 0
                self.__exitYN = False
                self.__comment = None
            else:
                myID = kwargs['fldID']
                self.__dob = dob  # dob is a datetime object.
                self.__mf = kwargs['fldMF'].u() if 'fldMF' in kwargs else None
                self.__flagCastrado = kwargs['fldFlagCastrado'] if 'fldFlagCastrado' in kwargs else None
                self.__animalClass = kwargs['fldFK_ClaseDeAnimal'] if 'fldFK_ClaseDeAnimal' in kwargs else None
                self.__animalRace = kwargs['fldFK_Raza'] if 'fldFK_Raza' in kwargs else None
                # countMe: Flag 1 / 0 para indicar si objeto se debe contar o no.
                #  1: Regular Animal;
                #  0: Substitute (Animal creado por Reemision de Caravana);
                # -1: Dummy (Creado por perform de un Animal de Sustitucion)
                self.__countMe = kwargs['fldCounter'] if 'fldCounter' in kwargs else 1
                # Fecha Evento de tblAnimales. Se usa en manejo de Dummy Animals en altaMultiple(), perform()
                self.__fldDate = createDT(str(kwargs['fldDate'])) if 'fldDate' in kwargs else None
                isValid = isValidFlag
                isActive = isActiveFlag
                self.__comment = kwargs['fldComment'] if 'fldComment' in kwargs else ''
                exitYN = kwargs['fldDateExit'] if 'fldDateExit' in kwargs else None
                if exitYN not in [*nones, 0, False]:
                    self.__exitYN = exitYN if createDT(
                        exitYN) != obj_dtError else 1  # Tiene salida, pero no se sabe fecha
                    isActive = False
                else:
                    self.__exitYN = ''

            super().__init__(myID, isValid, isActive)  # Llama a Asset.__init__()

        @property
        def activities(self):  # Este @property es necesario para las llamadas a obj.outerObject.
            return Animal.__activitiesDict

        @staticmethod
        def getActivitiesDict():
            return Animal.__activitiesDict


        class InventoryActivity(Activity):
            __tblRAName = 'tblAnimalesRegistroDeActividades'
            __tblObjName = 'tblAnimales'  # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
            __tblLinkName = 'tblLinkAnimalesActividades'
            __tblDataName = 'tblDataAnimalesActividadInventario'
            __tblRA = DataTable(__tblRAName)  # Tabla Registro De Actividades
            __tblObject = DataTable(__tblObjName)  # Tabla "Objeto": tblCaravanas, tblAnimales, etc.
            __tblLink = DataTable(__tblLinkName)  # Sin argumentos: se crean TODOS los campos de la tabla; _dataList=[]
            __tblData = DataTable(__tblDataName)  # Data Inventario, Data AltaActivity, Data Localizacion, etc.
            __classNameFlag = 0
            __className = None

            def __init__(self, activityName, activityID, invActivity):
                if not self.__tblRA.isValid or not self.__tblLink.isValid or not self.__tblData.isValid:
                    self.__isValidFlag = False
                    # Aborta programa si no puede crear objeto
                    raise TypeError(f'ERR_Sys_CannotCreateObject: DECORATIONS.PY - InventoryActivityAnimal({lineNum()})')

                self.__isValidFlag = True
                self.__activityName = activityName  # __activityName, __activityID, invActivity->NO checks. DEBEN SER CORRECTOS
                self.__activityID = activityID
                self.__isInventoryActivity = True if invActivity > 0 else False
                self.__outerAttr = None  # Atributo de Outer Class asignado a esta Inner Class. Va a almacenar OBJETO
                super().__init__(self.__isValidFlag, self.__activityName, self.__activityID, self.__isInventoryActivity)
                if self.__classNameFlag == 0:               # Con el flag, se ejecuta 1 sola vez.
                    self.__className = self.__class__.__name__
            @property
            def className(self):
                return self.__className
            @property
            def isValid(self):
                return self.__isValidFlag
            @property
            def outerObject(self):
                return self.__getattribute__('obj.__outerAttr')

            # --------------------------- FIN CLASS INVENTORYACTIVITY ---------------------------------  ##

        # EStas definiciones, dentro de class Animal()
        __activityName = 'Inventario'
        @property
        def inventory(self):
            self.__activityObj.__setattr__('obj.__outerAttr', self)  # OBJETO para ser usado por set, get, etc.
            return self.__activityObj
        theClass = InventoryActivity
        __activityObj = theClass(__activityName, __activitiesDict[__activityName], __isInventoryActivity[__activityName])
        try:
            __activityClasses[theClass.__name__] = theClass
            activityProperties[theClass.__name__] = inventory
        except (IndexError, KeyError, NameError, TypeError, ValueError):
            raise KeyError(f'Cannot initialize class {theClass.__name__} ')

        print(f'theClass: {theClass} / type(__activityObj): {type(__activityObj)} / theClass.__name__ = {theClass.__name__} ')
        # print(f'theClass.__class__.__dict__: {theClass.__class__.__dict__}')
        print(f'__ACTIVITYCLASSES({lineNum()}): {__activityClasses}')
        print(f'__ACTIVITY@Properties({lineNum()}): {activityProperties}')

        executorObject = activityProperties[theClass.__name__]
        print(f'now, the execution object is: {executorObject}')

    bovine = []
    bovineList = [0, 1, 4, 12, 8, 61, 92, 125, 133, 188, 203, 209, 222, 254, 294, 315, 333, 398]
    animalsVector = getRecords('tblAnimales', '', '', None, '*')
    for j in bovineList:
        tempAnimal = Bovine(**animalsVector.unpackItem(j))
        bovine.append(tempAnimal)
    print(f'TEST.PY Total Bovines created: {len(bovine)}  //  Bovines: {bovine}')
    executorObject = Animal.activityProperties['InventoryActivityAnimal']
    # bovines[4].executorObject.get()
    print(f'EXECUTIONOBJECT({lineNum()}) is {executorObject}')

    lista = [None] * 7
    print(f'listita: {lista}')