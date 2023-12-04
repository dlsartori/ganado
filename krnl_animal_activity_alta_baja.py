from krnl_config import krnl_logger, print, DISMISS_PRINT, time_mt, singleton, sessionActiveUser, callerFunction, \
    lineNum, valiDate, os, activityEnableFull
from custom_types import DataTable, setupArgs, getRecords, setRecord, delRecord
from datetime import datetime
from uuid import uuid4
from krnl_tm import MoneyActivity
from krnl_exceptions import DBAccessError
from krnl_animal_activity import AnimalActivity
from krnl_tag import Tag
from krnl_person import Person
# from krnl_animal_factory import animalCreator

"""  This module implements AnimalActivity.Alta, AnimalActivity.Baja activity classes.  """

def moduleName():
    return str(os.path.basename(__file__))

# =================================== Tablas comunes para perform() y perform() ======================== #

tblObjectsName = 'tblAnimales'
tblRAName = 'tblAnimalesRegistroDeActividades'
tblLinkName = 'tblLinkAnimalesActividades'
tblPersonasName = 'tblDataAnimalesActividadPersonas'
tblDataTMName = 'tblDataAnimalesActividadTM'
tblDataCaravanasName = 'tblDataAnimalesActividadCaravanas'
tblDataDesteteName = 'tblDataAnimalesActividadDestete'
tblDataLocalizacionName = 'tblDataAnimalesActividadLocalizacion'
tblDataCastracionName = 'tblDataAnimalesActividadCastracion'
tblDataStatusName = 'tblDataAnimalesActividadStatus'
tblDataMedicionName = 'tblDataAnimalesActividadMedicion'
tblDataCategoriasName = 'tblDataAnimalesCategorias'
tblDataInventarioName = 'tblDataAnimalesActividadInventario'
tblAnimalesTiposDeAltaBajaName = 'tblAnimalesTiposDeAltaBaja'
tblDataPrenezName = 'tblDataAnimalesActividadPreñez'
tblDataMarcaName = 'tblDataAnimalesActividadMarca'
tblDataEstadoDePosesionName = 'tblDataAnimalesStatusDePosesion'
tblRA_TMName = 'tblTMRegistroDeActividades'  # Tabla MoneyActivity: Argumentos de transaccion monetaria relacionada a Alta/Baja
tblTransactName = 'tblDataTMTransacciones'   # Tabla MoneyActivity
tblMontosName = 'tblDataTMMontos'            # Tabla MoneyActivity
tblCategoriesName = 'tblAnimalesCategorias'
categNames = None
categIDs = None

temp = getRecords(tblAnimalesTiposDeAltaBajaName, '', '', None, 'fldID', 'fldName', fldAltaBaja='Alta')
altaDict = {}
for i in range(temp.dataLen):
    record = temp.unpackItem(i)
    altaDict[record['fldName'].lower()] = record['fldID']       # {NombreAlta: ID_Alta, }

categoriesNamesTbl = getRecords(tblCategoriesName, '', '', None, '*')
if type(categoriesNamesTbl) is str:
    errorVal = f'ERR_DB_ReadError. {categoriesNamesTbl} - {callerFunction()}'
    krnl_logger.warning(errorVal)
    print(f'{moduleName()}({lineNum()}) - {errorVal}', dismiss_print=DISMISS_PRINT)
else:
    categNames = categoriesNamesTbl.getCol('fldName')
    categIDs = categoriesNamesTbl.getCol('fldID')
    categAnimalClass = categoriesNamesTbl.getCol('fldFK_ClaseDeAnimal')
    castradosCol = categoriesNamesTbl.getCol('fldFlagCastrado')

# DataTable with deassigned Tags, to process tag initialization.
dataRADesasignados = getRecords('tblAnimalesRegistroDeActividades', '', '', None, '*',
                                fldFK_NombreActividad=AnimalActivity.getActivitiesDict()['Caravaneo - Desasignar'])
if not isinstance(dataRADesasignados, DataTable):
    dataRADesasignados = DataTable('tblAnimalesRegistroDeActividades')

# ===================================== Tablas comunes para Alta y Baja ======================================= #


@singleton
class AltaActivityAnimal(AnimalActivity):
    """Implements perform() method to create new Animals in the system. The call is made by classes (Bovine, Caprine,
    etc.)
    Callable class: invoking its objects with () automatically executes the code in __call__()
    """
    __tblDataName = 'tblDataAnimalesActividadAlta'
    __activityName = 'Alta'
    __method_name = None  # Used in ActivityMethod to create the callable property. properties don't go with perform()
    __one_time_activity = True      # Don't have a use for this yet.

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False    # Inventory does support PA.
        kwargs['decorator'] = self.__method_name
        kwargs['one_time_activity'] = self.__one_time_activity

        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)

    def __call__(self, *args, **kwargs):
        # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {args}; kwargs: {kwargs}')
        return self.__perform(*args, **kwargs)


    def __perform(self, tipo_de_alta: str, *args: DataTable, tags=None, **kwargs):
        """
        Executes the Alta Operation for Animales. Can perform batch altaMultiple() for multiple idRecord.
        Intended to be used with map(): pass a list of animalClassName, tipoDeAlta, tags, a list of DataTables and
        kwargs enclosed in a list for each Animal to map()
        @param tags: Tag object(s) to assign to new Animal object.
        @param tipo_de_alta: 'Nacimiento', 'Compra', etc.
        @param animalKind: string! Kind of Animal to create Objects ('Vacuno','Caprino','Ovino','Porcino','Equino') or
        cls when called via Animal Class.
        @param animalMode: Tipo de Alta (string Name) from table tblAnimalesTiposDeAltaBaja
        @param
        @param args: DataTable objects with obj_data to insert in DB tables, as part of the Alta operation
                     If multiple owners for 1 Animal, 1 DataTable object must be passed for each owner of the Animal
                     with the structure of table [Data Animales Actividad Personas]
        @param kwargs: additional args for tblAnimales and other uses. Can include fields for other tables if needed.
                Special kwargs (none are mandatory):
                    animalMode='regular' (Default), 'substitute', 'dummy', 'external', 'generic'.
                    recordInventory=1 -> Activity must be counted as Inventory
                    eventDate=Date of Activity. getNow() if not provided.
                    tags = [tagObj1, tagObj1, ]. List of Tag Objects. Tags are MANDATORY for regular and substitute
                    animals
                    tagCommissionType: 'Comision' (Default), 'Comision - Reemplazo', 'Comision - Reemision'
        @return: Animal Object (Bovine, Caprine, Ovine, etc) or errorCode (str)
        """
        global categNames, categIDs
        processedTables = set()
        if type(categoriesNamesTbl) is str:
            retValue = f'ERR_DB_ReadError. {categoriesNamesTbl} - {callerFunction()}'
            raise RuntimeError(f'{moduleName()}({lineNum()}) - {retValue}')

        cls = self.outerObject      # cls is Bovine, Caprine, etc.

        try:
            if cls not in cls.getAnimalClasses():
                raise TypeError(f'ERR_INP_Invalid Arguement: {cls} is not a valid Animal Class.')
        except(AttributeError, NameError, TypeError):
            raise TypeError(f'ERR_INP_Invalid Arguement: {cls} is not a valid Animal Class.')

        try:
            tipo_de_alta = tipo_de_alta.strip().lower()
        except AttributeError:
            tipo_de_alta = None
        tipo_de_alta = tipo_de_alta if tipo_de_alta in altaDict else None
        if tipo_de_alta is None:
            retValue = f'ERR_INP_Invalid Argument Tipo De AltaBaja={tipo_de_alta} - {callerFunction()}'
            krnl_logger.info(record)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue

        retValue = None       # Returns an animal object successfully created
        tblDataCategory = setupArgs(tblDataCategoriasName, *args)
        tblObjects = setupArgs(tblObjectsName, *args, **kwargs)  # FechaEvento(fldDate) should be passed in kwargs
        if any(j in tipo_de_alta for j in ('nacimient', 'ingreso', 'compra')):
            animalMode = 'regular'
        elif any(j in tipo_de_alta for j in ('sustituc', 'substituc')):
            animalMode = 'substitution'
        elif 'dummy' in tipo_de_alta:
            animalMode = 'dummy'
        elif 'extern' in tipo_de_alta:
            animalMode = 'external'
        elif 'generic' in tipo_de_alta:
            animalMode = 'generic'
        else:
            animalMode = str(next((kwargs[j] for j in kwargs if 'mode' in str(j).lower()), None)).strip().lower()
        animalMode = animalMode if animalMode in cls.getAnimalModeDict() else None
        dob = tblObjects.getVal(0, 'fldDOB')
        tags = tags if hasattr(tags, '__iter__') else (tags, )
        tags = [t for t in tags if isinstance(t, Tag)]
        # List of tags used by this Animal Class (there can be repeat tags in different Animal classes)
        tagsInUse = set([t for t in Tag.getRegisterDict().values() if t.assignedToClass == cls.__name__])

        if str(tblObjects.getVal(0, 'fldMF')).upper() not in ('M', 'F'):
            retValue = f'ERR_UI_InvalidArgument: Male/Female missing or not valid'
        elif tipo_de_alta not in altaDict:
            retValue = f'ERR_UI_InvalidArgument: Tipo de Alta {tipo_de_alta} not valid'
        elif not isinstance(dob, datetime):
            retValue = f'ERR_UI_InvalidArgument: DOB {dob} not valid'
        elif animalMode not in cls.getAnimalModeDict():
            retValue = f'ERR_UI_InvalidArgument: Animal Mode {animalMode} not valid'
        elif cls not in cls.getAnimalClasses():
            retValue = f'ERR_UI_InvalidArgument: {cls} is not a valid Animal Class.'
        elif 'fldFK_Categoria' not in tblDataCategory.fldNames:
            retValue = f'ERR_UI_InvalidArgument: Animal Category missing / not valid'
        elif not tags and 'regular' in animalMode or 'substitu' in animalMode:
            retValue = f'ERR_UI_InvalidArgument: Mandatory tags missing - {callerFunction()}'
        elif tagsInUse.intersection(tags):      # checks if any tags in tags are already in use by the Animal class.
            retValue = f'ERR_INP_Invalid Tags: one or more {tags} already assigned to other animals. Pick fresh tags.'
        if type(retValue) is str:
            krnl_logger.info(retValue, exc_info=True, stack_info=True)
            print(f'{moduleName()}({lineNum()}) - {retValue}')
            raise ValueError(retValue)

        animalClassID = cls.getAnimalClassID()  # animalClassID is: 1, 2, 3, etc.
        timeStamp = time_mt('datetime')  # TimeStamp will be written on RA, RA_TM
        eventDate = timeStamp   # eventDate will be written in all tables with field Fecha Evento in non-valid state
        recordInventory = 0
        if kwargs:     # kwargs checks of general arguments (valid for all objects and all records to be written)
            # if Event date passed is not valid or not passed, sets eventDate=timeStamp
            # eventDate de tblObjects tiene precedencia sobre eventDate de kwargs.
            eventDate = tblObjects.getVal(0, 'fldDate') if tblObjects.getVal(0, 'fldDate') is not None else \
                        next((kwargs[j] for j in kwargs if 'eventdate' in str(j).lower()), None)
            eventDate = valiDate(eventDate, timeStamp)
            recordInventory = next((kwargs[j] for j in kwargs if 'recordinvent' in str(j).lower() and
                                    kwargs[j] in (0, 1, True, False)), False)
        tblRA = DataTable(tblRAName)   # Blank table. Nothing for user to write here.  # 1 solo registro en esta tabla.
        tblLink = DataTable(tblLinkName)                # Blank table. Nothing for user to write on this table
        tblData = setupArgs(self.__tblDataName, *args, fldDate=eventDate)    # 1 solo registro en tabla [Data Actividad Alta]
        tblDataPersonas = setupArgs(tblPersonasName, *args, fldDate=eventDate)
        tblDataStatus = setupArgs(tblDataStatusName, *args)
        tblDataLocalization = setupArgs(tblDataLocalizacionName, *args)
        tblDataCastration = setupArgs(tblDataCastracionName, *args)
        tblDataInventory = setupArgs(tblDataInventarioName, *args)
        tblDataPrenez = setupArgs(tblDataPrenezName, *args)
        tblDataMarca = setupArgs(tblDataMarcaName, *args)
        tblTransact = setupArgs(tblTransactName, *args)     # Tabla MoneyActivity. Debe pasarse COMPLETA para crear Transaccion
        tblMontos = setupArgs(tblMontosName, *args)         # Tabla MoneyActivity. Debe pasarse COMPLETA para crear Monto
        tblDataTM = setupArgs(tblDataTMName, *args)         # Animales Actividad MoneyActivity
        tblDataDestete = setupArgs(tblDataDesteteName)
        tblDataMedicion = setupArgs(tblDataMedicionName)
        tblDataEstadoDePosesion = setupArgs(tblDataEstadoDePosesionName, *args)  # Si no se pasa->Propio por Default
        tblRA_TM = DataTable(tblRA_TMName)              # Blank table.  # 1 solo registro en esta tabla.
        # Servicios, Sanidad, Inseminacion, Temperatura, Tacto, Curacion, Activ. Programadas, Alimentacion NO SE HACEN aqui.
        # Status, Localizacion, Inventario se ejecutan con las funciones respectivas, a nivel objeto.
        userID = sessionActiveUser
        tblRA.setVal(fldTimeStamp=timeStamp, fldFK_UserID=sessionActiveUser,
                     fldFK_NombreActividad=self.getActivitiesDict()[self.__activityName])       # AnimalActivity
        idActividadRA = setRecord(tblRA.tblName, **tblRA.unpackItem(0))  # Inserta registro en tabla RA

        if isinstance(idActividadRA, str):
            retValue = idActividadRA + f' {callerFunction()}({lineNum()})'
            krnl_logger.error(retValue)
            print(f'{retValue}', dismiss_print=DISMISS_PRINT)
            return retValue  # Sale c/error

        # 2. Create Animal Object
        countMe = cls.getAnimalModeDict()[animalMode] if animalMode in cls.getAnimalModeDict() else 0
        # Define valid categories for animal kind. Assign category Name.
        categNames = [categNames[j] for j in range(len(categNames)) if int(categAnimalClass[j]) == animalClassID]
        categIDs = [categIDs[j] for j in range(len(categIDs)) if int(categAnimalClass[j]) == animalClassID]
        __validCategories = dict(zip(categIDs, categNames))
        __castrados = [categNames[j] for j in range(len(categNames)) if int(castradosCol[j]) == 1]
        category = int(tblDataCategory.getVal(0, 'fldFK_Categoria'))
        if category not in __validCategories or len(__validCategories) == 0:
            retValue = f'ERR_UI_CategoryNotValid: {category} - {callerFunction()}'
            krnl_logger.warning(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue  # Salta creacion de objeto Animal Categoria no es valida
        categoryName = __validCategories[category]
        print(f'%%%%%%%%%%% {moduleName()}({lineNum()}) animalMode = {animalMode} / categoryName: {categoryName}',
              dismiss_print=DISMISS_PRINT)
        conceptionType = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('concep')), 'Natural')
        eventDate = valiDate(tblObjects.getVal(0, 'fldDate'), timeStamp)
        # dob = tblObjects.getVal(0, 'fldDOB') if isinstance(tblObjects.getVal(0, 'fldDOB'), datetime) else \
        #     eventDate - timedelta(days=365)
        # tblObjects.setVal(0, fldDOB=dob)
        dateExit = valiDate(tblObjects.getVal(0, 'fldDateExit'), 0)
        # Setea estado de Castracion (_fldFlagCastrado y tablas en DB)
        castrated = tblDataCastration.getVal(0, 'fldDate') if tblDataCastration.getVal(0, 'fldDate') else None
        if not castrated:
            castrated = 1 if categoryName in __castrados else 0
        else:
            castrated = valiDate(castrated, 1)
        objID = str(uuid4().hex)        # UID for Animal.
        tblObjects.setVal(0, fldID=None,  fldFK_ClaseDeAnimal=animalClassID, fldMode=animalMode,
                          fldCountMe=countMe,
                          fldFlagCastrado=castrated,
                          fldConceptionType=conceptionType,
                          fldFK_Raza=tblObjects.getVal(0, 'fldFK_Raza'),
                          fldComment=tblObjects.getVal(0, 'fldComment'),
                          fldTimeStamp=eventDate, fldDateExit=dateExit,
                          fldFK_UserID=userID,
                          fldObjectUID=objID
                          )
        idRecord = setRecord(tblObjectsName, **tblObjects.unpackItem(0))

        if not isinstance(idRecord, int):
            retValue = f'ERR_DBAccess: Unable to initialize Animal object in Database.'
            krnl_logger.error(retValue)
            raise DBAccessError(retValue)
        tblObjects.setVal(0, fldID=idRecord)

        # Setea tblRA, tblLink, tblData
        tblRA.undoOnError = True
        tblRA.setVal(0, fldID=idActividadRA, fldComment=f'Alta. ID Animal: {idRecord}')  # Setea fldID en tblRA.
        tblLink.setVal(0, fldFK_Actividad=idActividadRA, fldFK=objID,
                       fldComment=f'{self.__activityName} / ID:{idRecord}')
        tblData.setVal(0, fldFK_Actividad=idActividadRA, fldFK_TipoDeAltaBaja=altaDict[tipo_de_alta],
                       fldDate=eventDate)
        _ = (tblRA.setRecords(), tblLink.setRecords(), tblData.setRecords())
        errResult = [j for j in _ if j == 0]        # 0 records from setRecords() -> Error: Nothing was written.
        if errResult:
            retValue = f'ERR_DB_WriteError: {moduleName()}({lineNum()}): {str(errResult)} - {callerFunction()}'
            krnl_logger.warning(retValue)
            raise DBAccessError(retValue)

        # categoryName NO es parte de tabla Animales. Se necesita para llamar generateDOB() y definir fldDOB.
        # NO SE PASAN Tags en los constructores de Animal, Bovine, etc. Se deben setear via metodo assign()
        animalObj = cls(fldID=idRecord, fldMF=str(tblObjects.getVal(0, 'fldMF')), fldFK_ClaseDeAnimal=animalClassID,
                        fldObjectUID=objID,
                        fldMode=animalMode, categoryName=categoryName,
                        fldFlagCastrado=tblObjects.getVal(0, 'fldFlagCastrado') or (1 if categoryName in __castrados else 0),
                        fldDOB=tblObjects.getVal(0, 'fldDOB'),
                        fldConceptionType=conceptionType,
                        fldFK_Raza=tblObjects.getVal(0, 'fldFK_Raza'),
                        fldComment=tblObjects.getVal(0, 'fldComment'),
                        fldTimeStamp=eventDate, fldDateExit=dateExit,
                        fldFK_UserID=userID,
                        fresh_obj=True  # signals not to read lastInventory, lastStatus, lastCategoryID from DB
                        )
        if not isinstance(animalObj, cls):
            retValue = f'ERR_Sys_ObjectNotCreated: {animalObj} - {callerFunction()}'
            krnl_logger.error(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            raise RuntimeError(retValue)

        animalObj.register()            # Incluye animal nuevo en registerDict.
        tblRA.undoOnError = tblLink.undoOnError = tblData.undoOnError = True
        processedTables.update((tblObjects.tblName, tblRA.tblName, tblLink.tblName, tblData.tblName))
        print(f'\nFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF {moduleName()}({lineNum()}) - '
              f'Animal Object is type: {type(animalObj)}   FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\n',
              dismiss_print=DISMISS_PRINT)

        # Setea Cagetoria
        animalObj.category.set(tblRA, tblLink, tblDataCategory, categoryID=category)
        processedTables.update(tblDataCategory.tblName)

        # TAGS: Asignacion de tag(s) a animalObj y escritura  en tabla [Data Animales Actividad Caravanas]
        if tags:
            kwargs['tags'] = tags
            animalObj.tags.assignTags(tblRA, tblLink, **kwargs)  # kwargs -> tagCommissionType=Comision,Reemplazo,Reemision
            processedTables.update(tblDataCaravanasName)
            print(f'%%%%%% {moduleName()}({lineNum()}) - MYTAGS:{animalObj.myTagIDs} / Tag Numbers:{tags[0].tagNumber}',
                  dismiss_print=DISMISS_PRINT)

        # 3. Object-driven methods: Setup DataTables values and perform writes with methods.
        # tblObjects.setVal(0, fldFlagCastrado=castrated)
        animalObj.castration.set(tblRA, tblLink, tblDataCastration, event_date=castrated)
        processedTables.add(tblDataCastration.tblName)
        print(f'###########{moduleName()}({lineNum()}) fldID: {tblObjects.getVal(0, "fldID")} / dateExit: {dateExit} '
              f'/ eventDate = {eventDate} / castrated = {castrated} / timeStamp: {timeStamp} / mode: {animalMode}',
              dismiss_print=DISMISS_PRINT)

        # Status
        statusID = tblDataStatus.getVal(0, 'fldFK_Status')
        statusID = statusID if statusID is not None else animalObj.statusDict['En Stock'][0]
        statusName = next(j for j in animalObj.statusDict if animalObj.statusDict[j][0] == statusID)
        animalObj.status.set(tblRA, tblLink, tblDataStatus, status=statusName)
        processedTables.add(tblDataStatus.tblName)

        # LocalizationActivityAnimal
        if tblDataLocalization.getVal(0, 'fldFK_Localizacion') is not None:
            animalObj.localization.set(tblRA, tblLink, tblDataLocalization)
            processedTables.update((tblRA.tblName, tblLink.tblName, tblDataLocalization.tblName))

        # Personas: Owner debe venir especificado en tabla tblDataPersonas. Si no hay,se setea a Persona_Orejano
        persons = Person.getRegisterDict()  # Lista de Personas activas (Propietarios validos) para hacer verificaciones
        activePersons = [persons[j] for j in persons if persons[j].isActive]  # lista de ID_Personas Activas en el sistema
        activePersonsID = set([j.getID for j in activePersons])
        noOwnerPerson = 1  # Persona No Owner / TODO(cmt): Orejano. 1 por ahora, puede cambiar.
        tempOwners = DataTable(tblPersonasName)  # DataTable vacio para registrar todos los owners de animalObj
        # Recorre TODAS las tablas de *args y crea una Tabla con todos los propietarios de animalObj.
        if len(activePersonsID):
            for t in args:
                if isinstance(t, DataTable) and t.tblName == tblPersonasName and t.dataLen:
                    for j in range(t.dataLen):
                        ownerID = t.getVal(j, 'fldFK_Persona')
                        if ownerID in activePersonsID:   # Solo personas Activas, con Level=1
                            tempRecord = t.unpackItem(j)
                            tempOwners.appendRecord(fldFK_Actividad=idActividadRA, fldDate=eventDate,
                                                    fldFK_Persona=tempRecord['fldFK_Persona'], fldFlag=1,
                                                    fldPercentageOwnership=tempRecord['fldPercentageOwnership'],
                                                    fldComment=tempRecord['fldComment'])
        if not tempOwners.dataLen:          # Si no hay owners, setea Orejano
            tempOwners.setVal(0, fldFK_Actividad=idActividadRA, fldDate=eventDate, fldFlag=1,
                              fldFK_Persona=noOwnerPerson, fldPercentageOwnership=1, fldComment='Orejano')  # TODO: Aqui da ERROR DE SINTAXIS EN dbRead().
        animalObj.person.set(tblRA, tblLink, tempOwners)
        processedTables.add(tblDataPersonas.tblName)

        # MoneyActivity: Setea Tabla [Data Animales Actividad MoneyActivity] con la(s) idActividadRA_TM pasadas en *args, si existen.
        # TODA la informacion relativa a Registro De Actividades MoneyActivity, Transacciones y Montos debe ser manejada por
        # las funciones de alto nivel y escrita en las tablas respectivas. Aqui solo se actualiza ID_Actividad MoneyActivity y se
        # pasa el resto de la obj_data tal como viene de las funciones de alto nivel.
        if str(type(tblDataTM)).__contains__('MoneyActivity') and tblDataTM.dataLen and len(tblDataTM.dataList[0]) > 0:
            # LLAMADAS A METODOS MoneyActivity para Actualizar Transacciones y Montos
            activityTMList = tblDataTM.getVal(0, 'fldFK_ActividadTM')   # Usa lista, para cubrir el caso general
            if type(activityTMList) not in (tuple, list, set):
                activityTMList = [int(activityTMList), ]             # Convierte a lista para procesar
            for w in range(len(activityTMList)):
                if activityTMList[w] > 0:
                    animalObj.tm.set(fldFK_ActividadTM=activityTMList[w])
                    processedTables.add(tblDataTM.tblName)

        # Si es Actividad Inventario, setea tabla Inventario. Logica: kwargs['recordInventory'] overrides seteo de
        # _isInventoryActivity. Si no se pasa kwargs['recordInventory'] -> usa _isInventoryActivity
        if not recordInventory:
            pass  # NO setear inventario: Overrides _isInventoryActivity
        else:
            if self._isInventoryActivity:
                tblInventory = tblDataInventory
                tblInventory.setVal(0, fldFK_Actividad=idActividadRA)
                if tblInventory.getVal(0, 'fldDate') is None:
                    tblInventory.setVal(0, fldDate=eventDate)
                animalObj.inventory.set(tblRA, tblLink, tblInventory)
                processedTables.add(tblDataInventory.tblName)

        # 4. Write rest of tables (non object-driven). All of them will eventually move to object-driven writes.
        wrtTables = [tbl for tbl in args if isinstance(tbl, DataTable) and tbl.tblName not in processedTables]
        # print(f'&&&&&&&&&&&&&& {moduleName()}({lineNum()})[j.tblName for j in wrtTables if type(j) is not str]}')
        if wrtTables:
            for t in wrtTables:   # Setea campos comunes.Los demas campos DEBEN ESTAR seteados con valores validos
                t.setVal(0, fldFK_Actividad=idActividadRA)       # si el campo no existe en la tabla, ignora
                if not t.getVal(0, 'fldDate'):
                    t.setVal(0, fldDate=eventDate)
                _ = t.setRecords()
        return animalObj


# ------------------------------------------------- End class AltaAnimal -------------------------------------------- #

@singleton
class BajaActivityAnimal(AnimalActivity):
    """Implements __perform() method to remove Animals from the system. The call is made by instances of classes Bovine,
    Caprine, etc.)
       Callable class: invoking its objects automatically executes the code in __call__()
       """
    __tblDataName = 'tblDataAnimalesActividadBaja'
    __activityName = 'Baja'
    # TODO: this value to be read from DB. Already in [Actividades Nombres]
    __method_name = 'baja'          # Used in ActivityMethod to create the callable property.
    __one_time_activity = True      # Don't have a use for this yet. It don't look like this attribute makes sense...

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True    # Inventory does support PA.
        kwargs['decorator'] = self.__method_name
        kwargs['one_time_activity'] = self.__one_time_activity
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)
        temp1 = getRecords('tblAnimalesTiposDeAltaBaja', '', '', None, 'fldID', 'fldName', fldAltaBaja='Baja')
        self.tipoDeBajaDict = dict(zip(temp1.getCol('fldName'), temp1.getCol('fldID')))

    # TODO(cmt): Auto-call class: All instances of this class call this method whenever referenced in the code.
    def __call__(self, caller_object=None):
        self.outerObject = caller_object

        def inner(*args, **kwargs):
            return self.__perform(*args, **kwargs)

        return inner


    def __perform(self, bajaType: str = None, *args: DataTable, **kwargs):    # Called via @property in Class Animal
        """
        @param self: Object for which perform is executed. Object to be removed.
        @param bajaType: Tipo de Baja (Venta, Muerte, Extravio, Consumo Interno, Dummy, Salida-Otra)
        @return: ID_Animal dado de perform (int) o errorCode (str)
        """
        outerObj = self.outerObject
        if not bajaType:
            bajaType = kwargs.get('type', None)
        if not bajaType:
            err_str = f"ERR_INP: Missing or invalid argument 'Tipo de Baja'. Baja for animal {outerObj} not performed."
            krnl_logger.info(err_str)
            return err_str

        tblObjectsName = 'tblAnimales'
        tblDataCategoriasName = 'tblDataAnimalesCategorias'
        tblDataStatusName = 'tblDataAnimalesActividadStatus'
        tblAniGenParametersName = 'tblDataAnimalesParametrosGenerales'
        tblDataLocalizationName = 'tblDataAnimalesActividadLocalizacion'
        animalClassID = outerObj.animalClassID
        dummyIndexMale = 15         # TODO: Leer estos indices de DB.
        dummyIndexFemale = 16

        tblRA = setupArgs(tblRAName, *args)
        tblLink = setupArgs(tblLinkName, *args)
        tblData = setupArgs(self.__tblDataName, *args, **kwargs)
        tblObjects = getRecords(tblObjectsName, '', '', None, '*', fldObjectUID=outerObj.ID)        # tblAnimales
        print(f'||||||||||||||||||||||||||||||||||| perform object type: {type(outerObj)} / objectID: {outerObj.ID} / '
              f'Animal Class ID: {animalClassID} ||||||||||||||||||||||||||||||||||||||||', dismiss_print=DISMISS_PRINT)

        # Checks for Argument errors.
        retValue = None
        if str(bajaType).lower() not in [j.lower() for j in self.tipoDeBajaDict]:
            retValue = f'ERR_UI_InvalidArgument: Tipo de Baja missing or not valid - {callerFunction()})'
        else:
            pass
        if type(retValue) is str:
            krnl_logger.error(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue

        bajaExtravioID = self.tipoDeBajaDict['Extravio']             # 6: Tipo de Baja = Extravio
        paramIndex = dummyIndexFemale if 'F' in str(outerObj.mf).upper() else dummyIndexMale
        tblAniGeneralParams = DataTable(tblAniGenParametersName)

        if outerObj.isSubstitute:           # Logica para crear dummy Animal cuando es perform de un substitute (Ver flowchart)
            eventDate = time_mt('datetime')
            tblDummyAnimal = DataTable(tblObjectsName)
            tblDummyAnimal.setVal(0, fldFK_ClaseDeAnimal=outerObj.animalClassID, fldDOB=outerObj.dob,
                                  fldTimeStamp=eventDate, fldFK_Raza=outerObj.animalRace, fldCountMe=-1,
                                  fldFK_AnimalMadre=outerObj.ID, fldDateExit='', fldMF=outerObj.mf)
            tblDummyCategory = DataTable(tblDataCategoriasName)
            tblDummyCategory.setVal(0, fldFK_Categoria=outerObj.category.get())
            tblDummyStatus = DataTable(tblDataStatusName)
            tblDummyStatus.setVal(0, fldFK_Status=3)  # En Stock - Improductivo
            tblDummyLocaliz = DataTable(tblDataLocalizationName, fldFK_Localizacion=outerObj.localization.get())

            # with lock:   # TODO(cmt): Inicia lectura-escritura. Deberia ser reentrante. Veremos...
            tmp = getRecords(tblAniGenParametersName, '', '', None, 'fldID', 'fldParameterValue',
                             fldFK_ClaseDeAnimal=animalClassID, fldFK_NombreParametro=paramIndex)
            dummyCounter = tmp.getVal(0, 'fldParameterValue')       # Toma counter de Dummies de database.
                # Fin bloque lock
            if dummyCounter <= 0:  # Crea self. Bovine (countMe=-1) y da de Alta. __init__() hace Register de Bovine
                dummyAnimal = outerObj.__class__.perform('dummy', tblDummyAnimal, tblDummyCategory, tblDummyStatus,
                                                         tblDummyLocaliz)
                print(f'BAAAAAAAAAAAAAAAAAAAJA AAAAAAAAANIMAL {moduleName()}({lineNum()}) CON UN DUMMMMMMMYYYY. ---- '
                      f'Dummy Animal: {dummyAnimal.__dict__}', dismiss_print=DISMISS_PRINT)
                if not isinstance(dummyAnimal, outerObj.__class__):
                    retValue = f'ERR_Sys_ObjectCreation. Cannot create Dummy Animal:{dummyAnimal}. {callerFunction()}'
                    krnl_logger.error(retValue)
                else:
                    retValue = dummyAnimal
                    krnl_logger.info(f'Created Dummy Animal {dummyAnimal.ID}')
            else:
                dummyCounter -= 1
                tblAniGeneralParams.setVal(0, fldID=tmp.getVal(0, 'fldID'), fldParameterValue=dummyCounter)
                setRecord(tblAniGeneralParams.tblName, **tblAniGeneralParams.unpackItem(0))

            if isinstance(retValue, str):
                print(f'{moduleName()}({lineNum()}) - {retValue}')
                return retValue
        elif 'regul' in outerObj.mode.lower():  # Animal Regular. Debe procesar salida por Extravio/Timeout
            if self.tipoDeBajaDict[bajaType] == bajaExtravioID:
                dummyList = [j for j in outerObj.getRegisterDict().values() if j.isDummy and j.mf == outerObj.mf]
                dummyOut = None
                if dummyList:      # Selecciona Best Fit Dummy: min(fldDate) with fldDate > self.lastInventory
                    dummiesToRemove = {j: j.fldDate for j in dummyList if j.fldDate > self.lastInventory}
                    for dummyOut, fldDate in dummiesToRemove.items():
                        if fldDate == min(dummiesToRemove.values()):  # ELIMINA DUMMY ANIMAL. Busca el mas antiguo
                            idActivRA_bajaDummyOut = dummyOut.perform('Dummy', fldFK_AnimalAsociado=outerObj.getID)
                            # TODO: Error Handling de la Baja de dummyOut
                            break
                if dummyOut is None:        # Si no se encuentra Dummy valido para sacar, dummyOut queda =None.
                    # No hay Dummy Objects validos p/ dar de baja. Va a "pedir credito" incrementando dummyCounter
                    # with lock:      # TODO(cmt): Inicia lectura-escritura. Deberia ser reentrante. Veremos
                    tmp = getRecords(tblAniGenParametersName, '', '', None, 'fldID', 'fldParameterValue',
                                     fldFK_ClaseDeAnimal=animalClassID, fldFK_NombreParametro=paramIndex)
                    dummyCounter = tmp.getVal(0, 'fldParameterValue')
                        # Fin bloque no reentrante
                    tblAniGeneralParams.setVal(0, fldID=tmp.getVal(0, 'fldID'), fldParameterValue=dummyCounter+1)
                    setRecord(tblAniGeneralParams.tblName, **tblAniGeneralParams.unpackItem(0))
                else:
                    kwargs['fldFK_AnimalAsociado'] = dummyOut.getID

        retValue = self.__performBaja(self.tipoDeBajaDict[bajaType], tblRA, tblLink, tblData, tblObjects, *args, **kwargs)
        if type(retValue) is str:
            retValue = f'ERR_Sys_Function perform(): {retValue}'
            krnl_logger.error(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)

        return retValue

    def __performBaja(self, bajaTypeID, *args: DataTable, **kwargs):       # Not to be called by itself.
        """
        Function that completes the Baja Operation. Called from perform()
        @param animalObj: animal Object to remove/retire from the system.
        @param bajaTypeID: Tipo de Baja
        @param args: DataTable objects with obj_data to insert in DB tables, as part of the Baja operation
        @param kwargs:
                'tagStatusDict' = Baja (Default) or Desasignada
        @return: idActividadRA: Record ID in [Registro De Actividades] for Baja operation; errorCode (str)
        """
        animalObj = self.outerObject
        print(f'*** performBaja({lineNum()}) - AnimalClasses: {tuple(animalObj.getAnimalClasses().keys())}')
        if type(animalObj) not in animalObj.getAnimalClasses():
            retValue = f'ERR_INP_InvalidArgument: {animalObj.__class.__name__} type is not valid.'
            krnl_logger.warning(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue         # Sale si se llama desde objeto clase Animal.

        tblRA = setupArgs(tblRAName, *args)
        tblLink = DataTable(tblLinkName)
        # fldDate, fldFK_AnimalAsociado passed in fldDate in tblData. if fldDate not supplied, timeStamp is used
        tblData = setupArgs(self.__tblDataName, *args, **kwargs)
        tblDataTM = DataTable(tblDataTMName)
        tblMontos = DataTable(tblMontosName)
        tblCaravanas = DataTable(tblDataCaravanasName)
        tblDataCategory = setupArgs(tblDataCategoriasName, *args)
        tblObjects = setupArgs(tblObjectsName, *args)
        timeStamp = time_mt('datetime')
        userID = sessionActiveUser
        eventDate = valiDate(tblData.getVal(0, 'fldDate'), timeStamp)  # Si no hay fldDate en tblData usa timeStamp
        activityID = AnimalActivity.getActivitiesDict()[self.activityName]
        # Sobreescribe campo fldExitDate, Identificadores en tblAnimales(fldID=idAnimal)->Indica NO cargar Animal
        # durante inicializacion
        # _ = tblObjects.getVal(0, 'fldDateExit')
        tblObjects.setVal(0, fldID=animalObj.recordID, fldDateExit=eventDate, fldIdentifiers=None)
        _ = tblObjects.setRecords()   # Escribe fldDateExit en tblAnimales.

        tblRA.setVal(0, fldTimeStamp=timeStamp, fldFK_UserID=userID)
        idActividadRA = tblRA.getVal(0, 'fldID')
        tblLink.setVal(0, fldFK_Actividad=idActividadRA, fldFK=animalObj.ID,
                       fldComment=f'{self.activityName}. ID Animal: {animalObj.recordID}')
        tblData.setVal(0, fldFK_TipoDeAltaBaja=bajaTypeID, fldDate=eventDate,
                       fldComment=f'{self.activityName}. ID Animal: {animalObj.recordID}')
        activityID = activityID if tblRA.getVal(0, 'fldFK_NombreActividad') is None \
            else tblRA.getVal(0, 'fldFK_NombreActividad')
        tblRA.setVal(0, fldFK_NombreActividad=activityID, fldComment=f'{self.activityName}.')
        # Inserta actividad de Baja en tblRA, tblLink, tblData

        idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, *args, tbl_data_name=tblData.tblName)
        if type(idActividadRA) is str:
            retValue = f'ERR_DB_WriteError: {idActividadRA} - Function/Method: perform()'
            krnl_logger.error(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
        else:
            tblRA.setVal(0, fldID=idActividadRA)
            animalObj.status.set(tblRA, tblLink, status='Baja')     # Status del Animal: Baja (4)
            # TAGS: DesAsignacion de tag(s) a animalObj y escritura  en tabla [Data Animales Actividad Caravanas]
            if animalObj.myTags:
                decomissionPermittedStatus = ('perform', 'decomisionada', 'comisionada', 'extraviada', 'reemplazada')
                tagStatus = next((str(j) for j in kwargs if 'status' in str(j).lower()), 'Baja')
                tagStatus = tagStatus if tagStatus.lower() in decomissionPermittedStatus else 'Baja'
                animalObj.tags.deassignTags(tblRA, tblLink, tblCaravanas, tags=animalObj.myTags, tagStatus=tagStatus)

            """ --- Cierra TODAS las ProgActivities en tblLinkPA, tblRAP (si aplica) y tblDataPAStatus --- """
            self._paMatchAndClose(idActividadRA, execute_date=tblData.getVal(0, 'fldDate'))

            _ = animalObj.unRegister()  # pop de Bovine.__registerDict. Llama al metodo unregister() correcto.


            # MoneyActivity: Setea Tabla [Data Animales Actividad MoneyActivity] con la(s) idActividadRA_TM pasadas en *args, si existen.
            # TODA la informacion relativa a Registro De Actividades MoneyActivity, Transacciones y Montos debe ser manejada por
            # las funciones de alto nivel y escrita en las tablas respectivas. Aqui solo se actualiza ID_Actividad MoneyActivity.
            if isinstance(tblDataTM, DataTable) and tblDataTM.dataLen and len(tblDataTM.dataList[0]) > 0:
                # LLAMADAS A METODOS MoneyActivity para Actualizar Transacciones y Montos
                activityTMList = tblDataTM.getVal(0, 'fldFK_ActividadTM')  # Usa lista, para cubrir el caso general
                if type(activityTMList) not in (tuple, list, set):
                    activityTMList = [int(activityTMList), ]  # Convierte a lista para procesar
                for j in range(len(activityTMList)):
                    if activityTMList[j] is int and activityTMList[j] > 0:
                        animalObj.tm.set(tfldFK_ActividadTM=activityTMList[j])

            # Si es Actividad Inventario, setea tabla Inventario. Logica: kwargs['recordInventory'] overrides seteo
            # de _isInventoryActivity. Si no se pasa kwargs['recordInventory'] -> usa _isInventoryActivity
            recordInventory = kwargs.get('recordInventory', False) if not animalObj.isDummy else False
            if not recordInventory:
                pass  # NO setear inventario: Overrides _isInventoryActivity
            else:
                if animalObj.isInventoryActivity:
                    tblInventory = setupArgs(animalObj.tblDataInventoryName, *args)
                    tblInventory.setVal(0, fldFK_Actividad=idActividadRA)
                    tblInventory.setVal(0, fldDate=valiDate(tblInventory.getVal(0, 'fldDate'), eventDate))
                    animalObj.inventory.set(tblRA, tblLink, tblInventory)
            retValue = idActividadRA
            krnl_logger.info(f'Removal of animal {animalObj.ID} completed.')
        return retValue

# --------------------------------------------------- FIN __performBaja() ----------------------------------------- #
