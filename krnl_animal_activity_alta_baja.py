import collections
import sqlite3

from krnl_config import krnl_logger, print, DISMISS_PRINT, time_mt, singleton, sessionActiveUser, callerFunction, \
    lineNum, valiDate, os, activityEnableFull, TERMINAL_ID, json, fDateTime
from krnl_custom_types import DataTable, setupArgs, setRecord, getTblName, getFldName, getrecords, getColorID
import pandas as pd
from krnl_db_query import DBAccessSemaphore, AccessSerializer     # Query object for MAIN DB.
from krnl_db_access import setrecords
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

# =================================== Tablas comunes para perform() functions ======================== #

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

# temp = getRecords(tblAnimalesTiposDeAltaBajaName, '', '', None, 'fldID', 'fldName', fldAltaBaja='Alta')
temp = getrecords(tblAnimalesTiposDeAltaBajaName, 'fldID', 'fldName',
                     where_str=f'WHERE "{getFldName(tblAnimalesTiposDeAltaBajaName, "fldAltaBaja")}" == "Alta"')
# altaDict = dict(zip(temp['fldName'].str.lower(), temp[temp.db.col('fldid')]))
del temp

df_categories_names = getrecords(tblCategoriesName, '*')
if df_categories_names.empty:
    errorVal = f'ERR_DB_ReadError. {df_categories_names} - {callerFunction()}'
    krnl_logger.warning(errorVal)
    print(f'{moduleName()}({lineNum()}) - {errorVal}', dismiss_print=DISMISS_PRINT)
else:
    categNames = list(df_categories_names['fldName'])  # df_categories_names.getCol('fldName')
    categIDs = list(df_categories_names['fldID'])  # df_categories_names.getCol('fldID')
    categAnimalClass = list(df_categories_names['fldFK_ClaseDeAnimal'])  # df_categories_names.getCol('fldFK_ClaseDeAnimal')
    castradosCol = list(df_categories_names['fldFlagCastrado'])  # df_categories_names.getCol('fldFlagCastrado')
    categMF = dict(zip(df_categories_names['fldName'], df_categories_names['fldMF']))
# DataTable with deassigned Tags, to process tag initialization.
# dataRADesasignados = getRecords('tblAnimalesRegistroDeActividades', '', '', None, '*',
#                                 fldFK_NombreActividad=AnimalActivity.getActivitiesDict()['Caravaneo - Desasignar'])
# if not isinstance(dataRADesasignados, DataTable):
#     dataRADesasignados = DataTable('tblAnimalesRegistroDeActividades')

# ===================================== Fin Tablas comunes para Alta y Baja ======================================= #

def json_parser(data):
    if data:
        return json.loads(data)     # For now, throw JSON Exception to address malformed json during debugging.
    return data
    # try:
    #     return json.loads(data)
    # except json.JSONDecodeError:
    #     return data

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
    temp = getrecords('tblAnimalesTiposDeAltaBaja', '*')
    _tipos_de_alta_baja = dict(zip(temp['fldName'].str.lower(), temp['fldID']))     # {NombreTipoDeAltaBaja: fldID }
    del temp


    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False
        kwargs['decorator'] = self.__method_name
        kwargs['one_time_activity'] = self.__one_time_activity

        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)


    def __call__(self, *args, csv=None, **kwargs):
        """ Returns list of Animal objects (1 object or list of objects). """
        # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {args}; kwargs: {kwargs}')
        objects = []
        if csv is not None:
            dfAlta = pd.read_csv(csv, header=[0, 1], skipinitialspace=True, parse_dates=None,
                                 converters={('tblDataAnimalesActividadPersonas', 'Owner'): json_parser,
                                             ('tblCaravanas', 'fldFK_TagTechnology'): json_parser,
                                             ('tblCaravanas', 'fldTagNumber'): json_parser,
                                             ('tblCaravanas', 'fldFK_Color'): json_parser,
                                             ('tblCaravanas', 'fldFK_TagType'): json_parser})

            tables = dfAlta.columns.levels[0].to_list()  # Gets full list of tables present in csv file.
            for j, row in dfAlta.iterrows():
                tags = []
                dataframes = []  # List of resulting dataframes to pass to __alta__().
                tipo_de_alta = row.tblDataAnimalesActividadAlta.fldFK_TipoDeAltaBaja
                for name in tables:
                    if 'tblcaravanas' in name.lower():       # 2 tbls with special treatment: Caravanas, Owners.
                        for i in range(len(row[name].fldTagNumber)):
                            tag = Tag.getObject(str(row[name].fldTagNumber[i]))   # must convert any int to str.
                            if not tag:
                                tech = row.tblCaravanas.fldFK_TagTechnology[i].lower()
                                ttype = Tag.tagTypeDict[row[name].fldFK_TagType[i]] \
                                    if isinstance(row[name].fldFK_TagType, (list, tuple)) else None
                                fmt = Tag.tagFormatDict[row[name].fldFK_TagFormat[i]] if \
                                    isinstance(row[name].fldFK_TagFormat, (list, tuple)) else None
                                tagObjClass = self.outerObject._myTagClass()._tagObjectsClasses.get(tech, None)
                                if tagObjClass is not None:
                                    tag = tagObjClass.create_tag(fldTagNumber=str(row[name].fldTagNumber[i]),
                                                                 fldFK_Color=row[name].fldFK_Color[i],
                                                                 fldFK_TagTechnology=tech, fldFK_TagType=ttype,
                                                                 fldFK_TagFormat=fmt)
                            if tag:
                                tags.append(tag)

                    elif 'actividadpersonas' in name.lower():
                        # Process owners for animal object.
                        owners_dict = row[name].Owner
                        owners_data = collections.defaultdict(dict)
                        # for n, (k, v) in enumerate(owners_dict.items()):
                        #     owners_data.update({'fldFK_Persona': {n: k}, 'fldPercentageOwnership': {n: v}})
                        for i, (k, v) in enumerate(owners_dict.items()):
                            dfperson = Person.person_by_name(last_name=k, enforce_order=False)
                            if not dfperson.empty:
                                # IMPORTANT: Assumes only 1 valid name per key. Uses 1st row from dataframe only.
                                owners_data['fldFK_Persona'].update({i: dfperson.loc[0, 'fldObjectUID']})
                                owners_data['fldPercentageOwnership'].update({i: v})
                        ownership_sum = sum(owners_data['fldPercentageOwnership'].values())
                        if ownership_sum != 1.0:
                            raise ValueError(f'ERR_ValueError: PercentOwnership {ownership_sum} is wrong. Must be 1.')
                        dfOwners = pd.DataFrame.db.create(name, data=owners_data)
                        dataframes.append(dfOwners)
                    else:
                        dicto = row[name].to_dict()
                        df = pd.DataFrame.db.create(name, data=[dicto.values()], columns=dicto.keys())
                        dataframes.append(df)

                obj = self.__perform(tipo_de_alta, *dataframes, tags=tags, **kwargs)
                if obj:
                    objects.append(obj)
        else:
            objects = self.__perform(*args, **kwargs)

        # Reloads Animales dataframes.
        self.outerObject._init_uid_dicts()
        return objects


    def _process_csv(self, csv_file):
        """ Implements parsing of csv file. Converts file into multiple dataframes to be used for the Alta Activity.
        @return: list of dataframes used in the Alta.__perform() method.
        """
        pass

    def __perform(self, tipo_de_alta: str = None, *args: pd.DataFrame, tags=None, **kwargs):
        """
        Executes the Alta Operation for Animales. Can perform batch altaMultiple() for multiple idRecord.
        Runs a for loop for each row of Objects dataframe.
        @param tags: List of tuples of Tag object(s) to assign to new Animal objects.
        @param tipo_de_alta: (string) from table tblAnimalesTiposDeAltaBaja.
        @param args: pandas DataFrames formatted with 1 row for each Animal object to be created.
        tags = [(tagObj1,) (tagObj2,) ]. List of Tag Objects. Tags are mandatory for regular and substitute
                    animals
        @param kwargs: table data to create DataFrames, in the form:
                        {tblName1:{data_dict 1}, tblName2: {data_dict 2}, } data_dict must be in the DataFrame format.
                Special kwargs (none are mandatory):
                    animalMode='regular' (Default), 'substitute', 'dummy', 'external', 'generic'.
                    recordInventory=1 -> Activity must be counted as Inventory
                    eventDate=Date of Activity. getNow() if not provided.
                    tagCommissionType: 'Comision' (Default), 'Comision - Reemplazo', 'Comision - Reemision'
        @return: List of Animal Object (Bovine, Caprine, Ovine, etc) or errorCode (str)
        """
        global categNames, categIDs
        processedTables = set()

        if df_categories_names.empty:
            retValue = f'ERR_DBAccess: cannot read Categories table from database. - {callerFunction()}'
            raise sqlite3.DatabaseError(f'{moduleName()}({lineNum()}) - {retValue}')

        cls = self.outerObject  # cls is Bovine, Caprine, etc.
        try:
            if cls not in cls.getAnimalClasses():
                raise TypeError(f'ERR_INP_Invalid argument: {cls} is not a valid Animal Class.')
        except(AttributeError, NameError, TypeError):
            raise TypeError(f'ERR_INP_Invalid argument: {cls} is not a valid Animal Class.')

        try:
            tipo_de_alta = tipo_de_alta.strip().lower()
        except AttributeError:
            tipo_de_alta = None
        tipo_de_alta = tipo_de_alta if tipo_de_alta in self._tipos_de_alta_baja else None
        if tipo_de_alta is None:
            retValue = f'ERR_INP_Invalid Argument Tipo De AltaBaja={tipo_de_alta} - {callerFunction()}'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue

        retValue = None  # Returns an animal object successfully created.
        df_data_category = next((j for j in args if j.db.tbl_name == tblDataCategoriasName),
                                pd.DataFrame.db.create(tblDataCategoriasName))
        category = df_data_category.loc[0, 'fldFK_Categoria'] if not df_data_category.empty else None
        dfObjects = next((j for j in args if j.db.tbl_name == tblObjectsName), pd.DataFrame.db.create(tblObjectsName))
        frames_len = len(dfObjects.index)
        if not frames_len or any(len(j.index) != frames_len for j in args if isinstance(j, pd.DataFrame)
                                                            and 'actividadpersonas' not in j.db.tbl_name.lower()):
            retValue = f'ERR_INP_ValueError: Invalid DataFrames - {callerFunction()}'
            krnl_logger.info(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            raise ValueError(retValue)


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
        dob = dfObjects.loc[0, 'fldDOB'] if not dfObjects.empty else None
        tags = tags if hasattr(tags, '__iter__') else (tags,)
        tags = [t for t in tags if isinstance(t, Tag) and t.isAvailable]  # Filters only available tags. Ignores rest.
        # # List of tags used by this Animal Class (there can be repeat tags in different Animal classes)

        try:        # Tries to obtain a valid mf.
            mf = dfObjects.loc[0, 'fldMF'].lower()
        except (KeyError, IndexError):
            mf = None
        else:
            if mf not in ('m', 'f'):
                mf = None
        if mf is None:
            mf = self.outerObject.get_mf_from_cat(category)

        if isinstance(dob, pd.Timestamp):                # Tries to obtain a valid dob.
            dob = pd.Timestamp.to_pydatetime(dob)
        elif not isinstance(dob, datetime):
            try:
                dob = datetime.strptime(dob, fDateTime)
            except (TypeError, ValueError):
                dob = None
        if dob is None:     # if DOB not passed in dataframe, computes it based on animal category.
            dob = cls.generateDOB(category)

        if mf is None:
            retValue = f'ERR_UI_InvalidArgument: Male/Female missing or not valid'
        elif tipo_de_alta not in self._tipos_de_alta_baja:
            retValue = f'ERR_UI_InvalidArgument: Tipo de Alta {tipo_de_alta} not valid'
        elif not isinstance(dob, (datetime, pd.Timestamp)):
            retValue = f'ERR_UI_InvalidArgument: DOB {dob} not valid'
        elif animalMode not in cls.getAnimalModeDict():
            retValue = f'ERR_UI_InvalidArgument: Animal Mode {animalMode} not valid'
        elif cls not in cls.getAnimalClasses():
            retValue = f'ERR_UI_InvalidArgument: {cls} is not a valid Animal Class.'
        elif df_data_category.db.col('fldFK_Categoria') not in df_data_category.columns:
            retValue = f'ERR_UI_InvalidArgument: Animal Category missing / not valid'
        elif not tags and 'regular' in animalMode or 'substitu' in animalMode:
            retValue = f'ERR_UI_InvalidArgument: Mandatory tags missing - {callerFunction()}'
        elif not tags:  # checks if any tags in tags are already in use by the Animal class.
            retValue = f'ERR_INP_Invalid Tags: Tags assigned are not available. Please pick fresh tags.'
        if type(retValue) is str:
            krnl_logger.info(retValue, exc_info=True, stack_info=True)
            print(f'{moduleName()}({lineNum()}) - {retValue}')
            raise ValueError(retValue)

        animalClassID = cls.getAnimalClassID()  # animalClassID is: 1, 2, 3, etc.
        timeStamp = time_mt('datetime')  # TimeStamp will be written on RA, RA_TM
        eventDate = timeStamp  # eventDate will be written in all tables with field Fecha Evento in non-valid state
        recordInventory = 0
        dfRA = pd.DataFrame.db.create(tblRAName)  # Blank dataframe.
        dfRA.loc[0, 'fldFK_ClaseDeAnimal'] = animalClassID
        dfLink = pd.DataFrame.db.create(tblLinkName)  # Blank table. Nothing for user to write on this table
        dfData = next((j for j in args if j.db.tbl_name == self.__tblDataName),
                      pd.DataFrame.db.create(self.__tblDataName))  # 'Data Animales Actividad Alta'
        if dfData.empty or pd.isnull(dfData.loc[0, 'fldDate']):
            dfData['fldDate'] = eventDate

        df_data_caravanas = next((j for j in args if j.db.tbl_name == tblDataCaravanasName),
                                 pd.DataFrame.db.create(tblDataCaravanasName))
        df_data_personas = next((j for j in args if j.db.tbl_name == tblPersonasName),
                                pd.DataFrame.db.create(tblPersonasName))
        df_data_personas.loc[0, 'fldDate'] = eventDate
        df_data_status = next((j for j in args if j.db.tbl_name == tblDataStatusName),
                              pd.DataFrame.db.create(tblDataStatusName))
        df_data_localiz = next((j for j in args if j.db.tbl_name == tblDataLocalizacionName),
                               pd.DataFrame.db.create(tblDataLocalizacionName))
        df_data_castration = next((j for j in args if j.db.tbl_name == tblDataCastracionName),
                                  pd.DataFrame.db.create(tblDataCastracionName))
        df_data_inventory = next((j for j in args if j.db.tbl_name == tblDataInventarioName),
                                 pd.DataFrame.db.create(tblDataInventarioName))
        df_data_pregnancy = next((j for j in args if j.db.tbl_name == tblDataPrenezName),
                                 pd.DataFrame.db.create(tblDataPrenezName))
        df_data_branding = next((j for j in args if j.db.tbl_name == tblDataMarcaName),
                                pd.DataFrame.db.create(tblDataMarcaName))
        df_tm_transaction = next((j for j in args if j.db.tbl_name == tblTransactName),
                                 pd.DataFrame.db.create(tblTransactName))  # Tabla MoneyActivity. Pasar COMPLETA!
        df_data_tm = next((j for j in args if j.db.tbl_name == tblDataTMName),
                          pd.DataFrame.db.create(tblDataTMName))  # Animales Actividad MoneyActivity
        df_data_destete = next((j for j in args if j.db.tbl_name == tblDataDesteteName),
                               pd.DataFrame.db.create(tblDataDesteteName))
        df_data_medicion = next((j for j in args if j.db.tbl_name == tblDataMedicionName),
                                pd.DataFrame.db.create(tblDataMedicionName))
        df_data_possession = next((j for j in args if j.db.tbl_name == tblDataEstadoDePosesionName),
                                  pd.DataFrame.db.create(
                                      tblDataEstadoDePosesionName))  # Si no se pasa->Propio por Default
        dfRA_TM = pd.DataFrame.db.create(tblRA_TMName)  # Blank table.  # 1 solo registro en esta tabla.

        # Servicios, Sanidad, Inseminacion, Temperatura, Tacto, Curacion, Activ. Progr., Alimentacion NO SE HACEN aqui.

        userID = sessionActiveUser
        dfRA.loc[0, ('fldTimeStamp', 'fldFK_UserID', 'fldFK_NombreActividad')] = \
                    (timeStamp, sessionActiveUser, self.getActivitiesDict()[self._activityName])
        idActividadRA = setRecord(tblRAName, **dfRA.loc[0].to_dict())  # Inserta registro en tabla RA
        if isinstance(idActividadRA, str):
            retValue = idActividadRA + f' {callerFunction()}({lineNum()})'
            krnl_logger.error(retValue)
            print(f'{retValue}', dismiss_print=DISMISS_PRINT)
            return retValue  # Sale c/error
        else:
            dfRA.loc[0, 'fldID'] = idActividadRA

        # 2. Create Animal Object
        countMe = cls.getAnimalModeDict()[animalMode] if animalMode in cls.getAnimalModeDict() else 0
        # Define valid categories for animal kind. Assign category Name.
        categNames = [categNames[j] for j in range(len(categNames)) if int(categAnimalClass[j]) == animalClassID]
        categIDs = [categIDs[j] for j in range(len(categIDs)) if int(categAnimalClass[j]) == animalClassID]
        __validCategories = dict(zip(categIDs, categNames))
        castrados = [categNames[j] for j in range(len(categNames)) if int(castradosCol[j]) == 1]
        category = int(df_data_category.loc[0, 'fldFK_Categoria'])
        if category not in __validCategories or len(__validCategories) == 0:
            retValue = f'ERR_UI_CategoryNotValid: {category} - {callerFunction()}'
            krnl_logger.warning(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            return retValue  # Salta creacion de objeto Animal Categoria no es valida
        categoryName = __validCategories[category]
        print(f'%%%%%%%%%%% {moduleName()}({lineNum()}) animalMode = {animalMode} / categoryName: {categoryName}',
              dismiss_print=DISMISS_PRINT)
        conceptionType = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('concep')), 'Natural')
        eventDate = valiDate(dfData.loc[0, 'fldDate'], timeStamp)
        dateExit = valiDate(dfObjects.loc[0, 'fldDateExit'] if 'fldDateExit' in dfObjects.columns else 0, 0)
        # Setea estado de Castracion (_fldFlagCastrado y tablas en DB)
        castrated = df_data_castration.loc[0, 'fldDate'] if not df_data_castration.empty else None
        if not castrated:
            castrated = 1 if categoryName in castrados else 0
        else:
            castrated = valiDate(castrated, 1)
        objID = str(uuid4().hex)  # UID for Animal.

        dfObjects.loc[0, ('fldID', 'fldFK_ClaseDeAnimal', 'fldMode', 'fldDOB', 'fldMF',
                          'fldCountMe',
                          'fldFlagCastrado',
                          'fldConceptionType',
                          'fldFK_Raza',
                          'fldComment',
                          'fldTimeStamp', 'fldDateExit',
                          'fldFK_UserID',
                          'fldObjectUID',
                          'fldDaysToTimeout')] = \
            (None, animalClassID, animalMode, dob, mf,
             countMe,
             castrated,
             conceptionType,
             dfObjects.loc[0, 'fldFK_Raza'] if 'fldFK_Raza' in dfObjects.columns else 5,
             dfObjects.loc[0, 'fldComment'] if 'fldComment' in dfObjects.columns else '',
             eventDate, dateExit,
             userID,
             objID,
             dfObjects.loc[0, 'fldDaysToTimeout'] if 'fldDaysToTimeout' in dfObjects.columns else
                                                      cls._defaultDaysToTimeout)

        idRecord = setRecord(tblObjectsName, **dfObjects.loc[0].to_dict())

        if not isinstance(idRecord, int):
            retValue = f'ERR_DBAccess: Unable to initialize Animal object in Database.'
            krnl_logger.error(retValue)
            raise DBAccessError(retValue)
        dfObjects.loc[0, 'fldID'] = idRecord

        # Setea tblRA, tblLink, tblData
        dfRA.loc[0, ('fldID', 'fldTerminal_ID', 'fldComment')] = \
            (idActividadRA, TERMINAL_ID, f'Alta. ID Animal: {idRecord}')  # Setea fldID en tblRA.
        dfLink.loc[0, ('fldFK_Actividad', 'fldFK', 'fldComment')] = \
            (idActividadRA, objID, f'{self.__activityName} / ID:{idRecord}')
        dfData.loc[0, ('fldFK_Actividad', 'fldFK_TipoDeAltaBaja', 'fldDate')] = \
            (idActividadRA, self._tipos_de_alta_baja[tipo_de_alta], eventDate)
        _ = (setrecords(dfRA), setrecords(dfLink), setrecords(dfData))  # List of tuples of sqlit3.Cursor objects.
        # setrecords(dfRA)
        # setrecords(dfLink)
        # setrecords(dfData)
        try:
            errResult = [j for item in _ for j in item if j.rowcount <= 0]  # <=0 -> Error: Nothing was written.
        except (sqlite3.DatabaseError, sqlite3.OperationalError, sqlite3.DataError):
            errResult = True
        if errResult:
            retValue = f'ERR_DB_WriteError: {moduleName()}({lineNum()}): {str(errResult)} - {callerFunction()}'
            krnl_logger.warning(retValue)
            raise DBAccessError(retValue)

        # categoryName NO es parte de tabla Animales. Se necesita para llamar generateDOB() y definir fldDOB.
        # NO SE PASAN Tags en los constructores de Animal, Bovine, etc. Se deben setear via metodo assign()
        animalObj = cls(fldID=idRecord, fldMF=mf, fldFK_ClaseDeAnimal=animalClassID,
                        fldDOB=dfObjects.loc[0, 'fldDOB'],
                        fldObjectUID=objID,
                        fldMode=animalMode,
                        categoryName=categoryName,
                        fldFlagCastrado=dfObjects.loc[0, 'fldFlagCastrado'] or (
                            1 if categoryName in castrados else 0),
                        fldConceptionType=conceptionType,
                        fldFK_Raza=dfObjects.loc[0, 'fldFK_Raza'],
                        fldComment=dfObjects.loc[0, 'fldComment'],
                        fldTimeStamp=eventDate, fldDateExit=dateExit,
                        fldFK_UserID=userID,
                        fldDaysToTimeout=dfObjects.loc[0, 'fldDaysToTimeout']
                        )
        if not isinstance(animalObj, cls):
            retValue = f'ERR_Sys_ObjectNotCreated: {animalObj} - {callerFunction()}'
            krnl_logger.error(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            raise RuntimeError(retValue)

        # animalObj.register()            # Incluye animal nuevo en registerDict.

        # processedTables.update((tblObjects.tblName, tblRA.tblName, tblLink.tblName, tblData.tblName))
        print(f'\nFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF {moduleName()}({lineNum()}) - '
              f'Animal Object is type: {type(animalObj)}   FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\n',
              dismiss_print=DISMISS_PRINT)

        # Computa y Setea Cagetoria
        # Must set initial category assuming passed category data as valid.
        _ = animalObj.category.set(category=df_data_category.loc[0, 'fldFK_Categoria'])
        # processedTables.update(df_data_category)

        # TAGS: Asignacion de tag(s) a animalObj y escritura  en tabla [Data Animales Actividad Caravanas]
        if tags:
            animalObj.tags.assignTags(dfRA, dfLink, tag_list=tags,
                                      **kwargs)  # kwargs ->tagCommissionType=Comision,Reemplazo,Reemision
            # processedTables.update(df_data_caravanas)
            print(f'%%%%%% {moduleName()}({lineNum()}) - MYTAGS:{animalObj.myTagIDs} / Tag Numbers:{tags[0].tagNumber}',
                  dismiss_print=DISMISS_PRINT)

        # 3. Object-driven methods: Setup DataTables values and perform writes with methods.
        # tblObjects.setVal(0, fldFlagCastrado=castrated)
        animalObj.castration.set(dfRA, dfLink, df_data_castration, date=castrated)
        print(f'###########{moduleName()}({lineNum()}) fldID: {dfObjects.loc[0, "fldID"]} / dateExit: {dateExit} '
              f'/ eventDate = {eventDate} / castrated = {castrated} / timeStamp: {timeStamp} / mode: {animalMode}',
              dismiss_print=DISMISS_PRINT)

        # Status
        statusID = df_data_status.loc[0, 'fldFK_Status'] if not df_data_status.empty else None
        statusID = statusID if statusID is not None else animalObj.statusDict['En Stock'][0]
        statusName = next(j for j in animalObj.statusDict if animalObj.statusDict[j][0] == statusID)
        animalObj.status.set(dfRA, dfLink, df_data_status, status=statusName)

        # LocalizationActivityAnimal
        if not df_data_localiz.empty and pd.notnull(df_data_localiz.loc[0, 'fldFK_Localizacion']):
            animalObj.localization.set(dfRA, dfLink, df_data_localiz)

        # Personas: Owner debe venir especificado en tabla tblDataPersonas. Si no hay,se setea a Persona_Orejano
        activePersonsUID = set()
        noOwner = None
        for frame in Person.obj_dataframe():
            activePersonsUID.update(frame['fldObjectUID'].tolist())
            if noOwner is None:
                try:  # Pulls uid for Orejano (noOwner).
                    noOwner = frame.loc[frame['fldLastName'].str.lower().isin(['orejano']), 'fldObjectUID'].tolist()[0]
                except IndexError:
                    continue

        tempOwners = pd.DataFrame.db.create(tblPersonasName)  # df vacio para registrar todos los owners de animalObj
        # Recorre TODAS las tablas de *args y crea una Tabla con todos los propietarios de animalObj.
        if activePersonsUID:
            for j, row in df_data_personas.iterrows():
                if row.fldFK_Persona in activePersonsUID:  # Solo personas Activas, con Level=1
                    tempOwners.loc[j, ('fldFK_Actividad', 'fldDate', 'fldFK_Persona', 'fldFlag',
                                       'fldPercentageOwnership')] = \
                        (idActividadRA, eventDate, row.fldFK_Persona, 1, row.fldPercentageOwnership
                         if row.fldPercentageOwnership <= 1 else row.fldPercentageOwnership / 100)
        # if tempOwners['fldPercentageOwnership'].sum() > 1:
        #     raise ValueError(f'ERR_ValueError Alta Animales() - Invalid data: Total % of ownership exceeds 1.')
        # Si no hay owners, setea Orejano
        if tempOwners.empty:
            tempOwners.loc[0, ('fldFK_Actividad', 'fldDate', 'fldFlag', 'fldFK_Persona', 'fldPercentageOwnership',
                               'fldComment')] = (idActividadRA, eventDate, 1, noOwner, 1, 'Orejano')
        animalObj.person.set(dfRA, dfLink, tempOwners)
        # processedTables.add(df_data_personas)

        # MoneyActivity: Setea Tabla [Data Animales Actividad MoneyActivity] con la(s) idActividadRA_TM pasadas en *args
        # (si existen).
        # TODA la informacion relativa a Registro De Actividades MoneyActivity, Transacciones y Montos debe ser manejada
        # por las funciones de alto nivel y escrita en las tablas respectivas. Aqui solo se actualiza ID_Actividad
        # MoneyActivity y se pasa el resto de la obj_data tal como viene de las funciones de alto nivel.
        if not df_data_tm.empty:
            # Llamadas a metodos MoneyActivity para Actualizar Transacciones y Montos
            activityTM = df_data_tm.loc[0, 'fldFK_ActividadTM']
            if pd.notnull(activityTM):
                animalObj.tm.set(fldFK_ActividadTM=activityTM)  # IMPORTANT: 1 df_data_tm row per Animal object.

        # Si es Actividad Inventario, setea tabla Inventario. Logica: kwargs['recordInventory'] overrides seteo de
        # _isInventoryActivity. Si no se pasa kwargs['recordInventory'] -> usa _isInventoryActivity
        # if not recordInventory:
        #     pass  # NO setear inventario: Overrides _isInventoryActivity
        # else:
        #     if self._isInventoryActivity:
        #         tblInventory = df_data_inventory
        #         df_data_inventory.loc[0, 'fldFK_Actividad'] = idActividadRA
        #         if pd.isnull(df_data_inventory.loc[0, 'fldDate']):
        #             df_data_inventory.loc[0, 'fldDate'] = eventDate
        #         # TODO(cmt): The call to inventory.set() will set the inventory date in __memory_data dictionary.
        #         animalObj.inventory.set(dfRA, dfLink, tblInventory)

        # Leave this out for now to avoid recording repeat parturitions on a mother.
        # if pd.notnull(dfObjects.loc[0, 'fldFK_AnimalMadre']) and 'nacimiento' in tipo_de_alta.lower():
        #     mother_obj = cls.getObject(dfObjects.loc[0, 'fldFK_AnimalMadre'])
        #     if mother_obj:
        #         mother_obj.parturition.set(*args, even_date=dob, **kwargs)  # Sets the parturition event for mother.

        return animalObj
    # ------------------------------------------ End Alta.__perform() ----------------------------------------- #

    def __perform_multi(self, *args: pd.DataFrame, tags=None, **kwargs):
        """
        Executes the Alta Operation for Animales. Can perform batch altaMultiple() for multiple idRecord.
        Runs a for loop for each row of Objects dataframe.
        KEY REQUIREMENT: dfObjects DataFrame index is used a reference index. All dataframes indices must conform to it.
        @param tags: List of tuples of Tag object(s) to assign to new Animal objects.
        @param args: pandas DataFrames formatted with 1 row for each Animal object to be created.
        tags = [(tagObj1,) (tagObj2,) ]. List of Tag Objects. Tags are mandatory for regular and substitute
                    animals
        @param kwargs: table data to create DataFrames, in the form:
                        {tblName1:{data_dict 1}, tblName2: {data_dict 2}, } data_dict must be in the DataFrame format.
                Special kwargs (none are mandatory):
                    animalMode='regular' (Default), 'substitute', 'dummy', 'external', 'generic'.
                    recordInventory=1 -> Activity must be counted as Inventory
                    eventDate=Date of Activity. getNow() if not provided.
                    tagCommissionType: 'Comision' (Default), 'Comision - Reemplazo', 'Comision - Reemision'
        @return: List of Animal Object (Bovine, Caprine, Ovine, etc) or errorCode (str)
        """
        global categNames, categIDs
        processedTables = set()
        if df_categories_names.empty:
            retValue = f'ERR_DBAccess: cannot read Categories table from database. - {callerFunction()}'
            raise sqlite3.DatabaseError(f'{moduleName()}({lineNum()}) - {retValue}')

        cls = self.outerObject      # cls is Bovine, Caprine, etc.
        try:
            if cls not in cls.getAnimalClasses():
                raise TypeError(f'ERR_INP_Invalid argument: {cls} is not a valid Animal Class.')
        except(AttributeError, NameError, TypeError):
            raise TypeError(f'ERR_INP_Invalid argument: {cls} is not a valid Animal Class.')
        animalClassID = cls.getAnimalClassID()  # animalClassID is: 1, 2, 3, etc.
        timeStamp = time_mt('datetime')  # TimeStamp will be written on RA, RA_TM
        eventDate = timeStamp  # eventDate will be written in all tables with field Fecha Evento in non-valid state
        recordInventory = 0
        userID = sessionActiveUser
        dfObjects = next((j for j in args if j.db.tbl_name == tblObjectsName), pd.DataFrame.db.create(tblObjectsName))
        # dfObjects.reset_index()
        frames_len = len(dfObjects.index)
        dfRA = pd.DataFrame.db.create(tblRAName)  # Blank dataframe.

        dfLink = pd.DataFrame.db.create(tblLinkName)  # Blank table. Nothing for user to write on this table
        dfData = next((j for j in args if j.db.tbl_name == self.__tblDataName),
                      pd.DataFrame.db.create(self.__tblDataName))  # 'Data Animales Actividad Alta'
        # dfData.reset_index()

        retValue = None       # Returns an animal object successfully created.
        df_data_category = next((j for j in args if j.db.tbl_name == tblDataCategoriasName),
                                pd.DataFrame.db.create(tblDataCategoriasName))
        # df_data_category.reset_index()
        if pd.isnull(dfData.loc[0, 'fldDate']):
            dfData['fldDate'] = eventDate           # Sets eventDate in dfData if it's missing.
        # # Resets index of all tables passed in args.
        # for j in args:
        #     if isinstance(j, pd.DataFrame):
        #         j.reset_index()

        df_data_caravanas = next((j for j in args if j.db.tbl_name == tblDataCaravanasName),
                                 pd.DataFrame.db.create(tblDataCaravanasName))
        df_data_personas = next((j for j in args if j.db.tbl_name == tblPersonasName),
                                pd.DataFrame.db.create(tblPersonasName))
        df_data_personas.loc[0, 'fldDate'] = eventDate
        df_data_status = next((j for j in args if j.db.tbl_name == tblDataStatusName),
                              pd.DataFrame.db.create(tblDataStatusName))
        df_data_localiz = next((j for j in args if j.db.tbl_name == tblDataLocalizacionName),
                               pd.DataFrame.db.create(tblDataLocalizacionName))
        df_data_castration = next((j for j in args if j.db.tbl_name == tblDataCastracionName),
                                  pd.DataFrame.db.create(tblDataCastracionName))
        df_data_inventory = next((j for j in args if j.db.tbl_name == tblDataInventarioName),
                                 pd.DataFrame.db.create(tblDataInventarioName))
        df_data_pregnancy = next((j for j in args if j.db.tbl_name == tblDataPrenezName),
                                 pd.DataFrame.db.create(tblDataPrenezName))
        df_data_branding = next((j for j in args if j.db.tbl_name == tblDataMarcaName),
                                pd.DataFrame.db.create(tblDataMarcaName))
        df_tm_transaction = next((j for j in args if j.db.tbl_name == tblTransactName),
                                 pd.DataFrame.db.create(tblTransactName))  # Tabla MoneyActivity. Pasar COMPLETA!
        df_data_tm = next((j for j in args if j.db.tbl_name == tblDataTMName),
                          pd.DataFrame.db.create(tblDataTMName))  # Animales Actividad MoneyActivity
        df_data_destete = next((j for j in args if j.db.tbl_name == tblDataDesteteName),
                               pd.DataFrame.db.create(tblDataDesteteName))
        df_data_medicion = next((j for j in args if j.db.tbl_name == tblDataMedicionName),
                                pd.DataFrame.db.create(tblDataMedicionName))
        df_data_possession = next((j for j in args if j.db.tbl_name == tblDataEstadoDePosesionName),
                                  pd.DataFrame.db.create(
                                      tblDataEstadoDePosesionName))  # Si no se pasa->Propio por Default
        dfRA_TM = pd.DataFrame.db.create(tblRA_TMName)  # Blank table.  # 1 solo registro en esta tabla.
        # Servicios, Sanidad, Inseminacion, Temperatura, Tacto, Curacion, Activ. Progr., Alimentacion NO SE HACEN aqui.
        if not frames_len or len(dfData.index) != frames_len or len(df_data_category.index) != frames_len:
            # or any(len(j.index) != frames_len for j in args if isinstance(j, pd.DataFrame)) -> Must remove tblDataPersonas from this check
            retValue = f'ERR_ValueError: Invalid DataFrame format and/or length - {callerFunction()}.'

        animal_list = []        # function return value (list of animal objects).
        for i in range(frames_len):
            dfRA.loc[i, 'fldFK_ClaseDeAnimal'] = animalClassID
            tipo_de_alta = next(k for k, v in self._tipos_de_alta_baja.items() if
                                dfData.loc[i, 'fldFK_TipoDeAltaBaja'] == v)
            if any(j in tipo_de_alta.lower() for j in ('nacimient', 'ingreso', 'compra')):
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
                animalMode = None
            dob = dfObjects.loc[i, 'fldDOB'] if not dfObjects.empty else None

            if dfObjects.loc[i, dfObjects.db.col('fldMF')].upper() not in ('M', 'F'):
                retValue = f'ERR_ValueError: Male/Female missing or not valid.'
            elif tipo_de_alta not in self._tipos_de_alta_baja.keys():
                retValue = f'ERR_ValueError: Tipo de Alta {tipo_de_alta} not valid.'
            elif not isinstance(dob, datetime):
                retValue = f'ERR_ValueError: DOB {dob} not valid'
            elif animalMode not in cls.getAnimalModeDict():
                retValue = f'ERR_ValueError: Animal Mode {animalMode} not valid.'
            elif cls not in cls.getAnimalClasses():
                retValue = f'ERR_ValueError: {cls} is not a valid Animal Class.'
            elif df_data_category.db.col('fldFK_Categoria') not in df_data_category.columns:
                retValue = f'ERR_ValueError: Animal Category missing / not valid.'
            elif i >= len(tags):
                retValue = f'ERR_ValueError: Invalid number of tags - {callerFunction()}.'
            if type(retValue) is str:
                krnl_logger.info(retValue, exc_info=True, stack_info=True)
                print(f'{moduleName()}({lineNum()}) - {retValue}')
                raise ValueError(retValue)

            dfRA.loc[i, dfRA.db.cols('fldTimeStamp', 'fldFK_UserID', 'fldFK_NombreActividad')] = \
                (timeStamp, sessionActiveUser, self.getActivitiesDict()[self._activityName])
            idActividadRA = setRecord(tblRAName, **dfRA.loc[0].to_dict())  # Inserta registro en tabla RA
            if isinstance(idActividadRA, str):
                retValue = idActividadRA + f' {callerFunction()}({lineNum()})'
                krnl_logger.error(retValue)
                print(f'{retValue}', dismiss_print=DISMISS_PRINT)
                return retValue  # Sale c/error
            else:
                dfRA.loc[i, 'fldID'] = idActividadRA

            # 2. Create Animal Object
            countMe = cls.getAnimalModeDict()[animalMode] if animalMode in cls.getAnimalModeDict() else 0
            # Define valid categories for animal kind. Assign category Name.
            categNames = [categNames[j] for j in range(len(categNames)) if int(categAnimalClass[j]) == animalClassID]
            categIDs = [categIDs[j] for j in range(len(categIDs)) if int(categAnimalClass[j]) == animalClassID]
            __validCategories = dict(zip(categIDs, categNames))
            __castrados = [categNames[j] for j in range(len(categNames)) if int(castradosCol[j]) == 1]
            category = int(df_data_category.loc[i, 'fldFK_Categoria'])
            if category not in __validCategories or len(__validCategories) == 0:
                retValue = f'ERR_UI_CategoryNotValid: {category} - {callerFunction()}'
                krnl_logger.warning(retValue)
                print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
                return retValue  # Salta creacion de objeto Animal Categoria no es valida
            categoryName = __validCategories[category]
            print(f'%%%%%%%%%%% {moduleName()}({lineNum()}) animalMode = {animalMode} / categoryName: {categoryName}',
                  dismiss_print=DISMISS_PRINT)
            # conceptionType = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('concep')), 'Natural')
            conceptionType = dfObjects.loc[i, 'fldConceptionType'] if pd.notnull(dfObjects.loc[i, 'fldConceptionType'])\
                else 'natural'
            eventDate = valiDate(dfData.loc[i, 'fldDate'], timeStamp)
            dateExit = valiDate(dfObjects.loc[i, 'fldDateExit'], 0)
            # Setea estado de Castracion (_fldFlagCastrado y tablas en DB)
            castrated = df_data_castration.loc[i, 'fldDate'] if not df_data_castration.empty else None
            if not castrated:
                castrated = 1 if categoryName in __castrados else 0
            else:
                castrated = valiDate(castrated, 1)
            objID = str(uuid4().hex)        # UID for Animal.
            dob = dfObjects.loc[i, 'fldDOB']
            if not isinstance(dob, datetime):
                try:
                    cls.generateDOB(category)   # system-generated DOB based on category, if DOB not provided.
                except AttributeError:
                    retValue = f'{callerFunction()}({moduleName()}) ERR_UI_Invalid or missing DOB.'
                    return retValue
                else:
                    dfObjects.loc[i, 'fldDOB'] = dob

            dfObjects.loc[i, ('fldID', 'fldFK_ClaseDeAnimal', 'fldMode',
                               'fldCountMe',
                               'fldFlagCastrado',
                               'fldConceptionType',
                               'fldFK_Raza',
                               'fldComment',
                               'fldTimeStamp', 'fldDateExit',
                               'fldFK_UserID',
                               'fldObjectUID',
                               ' fldDaysToTimeout')] = \
                (None, animalClassID, animalMode,
                    countMe,
                    castrated,
                    conceptionType,
                    dfObjects.loc[i, 'fldFK_Raza'],
                    dfObjects.loc[i, 'fldComment'],
                    eventDate, dateExit,
                    userID,
                    objID,
                    dfObjects.loc[i, 'fldDaysToTimeout'] or cls._defaultDaysToTimeout)

            idRecord = setRecord(tblObjectsName, **dfObjects.loc[i].to_dict())

            if not isinstance(idRecord, int):
                retValue = f'ERR_DBAccess: Unable to initialize Animal object in Database.'
                krnl_logger.error(retValue)
                raise sqlite3.DatabaseError(retValue)
            dfObjects.loc[i, 'fldID'] = idRecord        # Updates dfObjects row.

            # Setea tblRA, tblLink, tblData
            dfRA.loc[i, ('fldID', 'fldTerminal_ID', 'fldComment')] = \
                        (idActividadRA, TERMINAL_ID, f'Alta. ID Animal: {idRecord}')  # Setea fldID en tblRA.
            dfLink.loc[i, ('fldFK_Actividad', 'fldFK', 'fldComment')] = \
                (idActividadRA, objID, f'{self.__activityName} / ID:{idRecord}')
            dfData.loc[i, ('fldFK_Actividad', 'fldFK_TipoDeAltaBaja', 'fldDate')] = \
                (idActividadRA, self._tipos_de_alta_baja[tipo_de_alta], eventDate)
            # _ = (setrecords(dfRA), setrecords(dfLink), setrecords(dfData))  # List of tuples of sqlit3.Cursor objects.
            _ = (setRecord(dfRA.db.tbl_name, **dfRA.loc[i].to_dict()),
                 setRecord(dfLink.db.tbl_name, **dfLink.loc[i].to_dict()),
                 setRecord(dfData.db.tbl_name, **dfData.loc[i].to_dict()))  # List of tuples of sqlit3.Cursor objects.
            try:
                errResult = [j for item in _ for j in item if j.rowcount <= 0]     # <=0 -> Error: Nothing was written.
            except (sqlite3.DatabaseError, sqlite3.OperationalError, sqlite3.DataError):
                errResult = True
            if errResult:
                retValue = f'ERR_DB_WriteError: {moduleName()}({lineNum()}): {str(errResult)} - {callerFunction()}'
                krnl_logger.warning(retValue)
                raise DBAccessError(retValue)

            # categoryName NO es parte de tabla Animales. Se necesita para llamar generateDOB() y definir fldDOB.
            # NO SE PASAN Tags en los constructores de Animal, Bovine, etc. Se deben setear via metodo assign()
            animalObj = cls(fldID=idRecord, fldMF=dfObjects.loc[i, 'fldMF'].lower(), fldFK_ClaseDeAnimal=animalClassID,
                            fldObjectUID=objID,
                            fldMode=animalMode,
                            categoryName=categoryName,
                            fldFlagCastrado=dfObjects.loc[i, 'fldFlagCastrado'] or (1 if categoryName in __castrados
                                                                                    else 0),
                            fldDOB=dfObjects.loc[i, 'fldDOB'],
                            fldConceptionType=conceptionType,
                            fldFK_Raza=dfObjects.loc[i, 'fldFK_Raza'],
                            fldComment=dfObjects.loc[i, 'fldComment'],
                            fldTimeStamp=eventDate, fldDateExit=dateExit,
                            fldFK_UserID=userID,
                            fldDaysToTimeout=dfObjects.loc[i, 'fldDaysToTimeout']
                            )
            # if not isinstance(animalObj, cls):
            #     retValue = f'ERR_Sys_ObjectNotCreated: {animalObj} - {callerFunction()}'
            #     krnl_logger.error(retValue)
            #     print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
            #     raise RuntimeError(retValue)
            print(f'\nFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF {moduleName()}({lineNum()}) - '
                  f'Animal Object is type: {type(animalObj)}   FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\n',
                  dismiss_print=DISMISS_PRINT)

            # Computa y Setea Cagetoria
            # Must set initial category assuming passed category data as valid.
            _ = animalObj.category.set(category=df_data_category.loc[i, 'fldFK_Categoria'])
            processedTables.update(df_data_category)

            # TAGS: Asignacion de tag(s) a animalObj y escritura  en tabla [Data Animales Actividad Caravanas]
            animal_tags = tags[i]
            animal_tags = animal_tags if hasattr(animal_tags, '__iter__') else (animal_tags,)
            if animal_tags:
                animalObj.tags.assignTags(dfRA[i], dfLink[i], tag_list=animal_tags, **kwargs)
                # processedTables.update(df_data_caravanas)
                print(f'==={moduleName()}({lineNum()}) - Tags:{animalObj.myTagIDs} / Tag Numbers:{tags[0].tagNumber}',
                      dismiss_print=DISMISS_PRINT)

            # TODO: FIX TABLES.
            # 3. Object-driven methods: Setup DataTables values and perform writes with methods.
            animalObj.castration.set(dfRA.loc[i], dfLink.loc[i], df_data_castration.loc[i], date=castrated)
            print(f'###########{moduleName()}({lineNum()}) fldID: {dfObjects.loc[i, "fldID"]} / dateExit: {dateExit} '
                  f'/ eventDate = {eventDate} / castrated = {castrated} / timeStamp: {timeStamp} / mode: {animalMode}',
                  dismiss_print=DISMISS_PRINT)

            # Status
            statusID = df_data_status.loc[i, 'fldFK_Status']
            statusID = statusID if statusID is not None else animalObj.statusDict['En Stock'][0]
            statusName = next(j for j in animalObj.statusDict if animalObj.statusDict[j][0] == statusID)
            animalObj.status.set(dfRA.loc[i], dfLink.loc[i], df_data_status.loc[i], status=statusName)

            # LocalizationActivityAnimal
            if not df_data_localiz.empty and pd.notnull(df_data_localiz.loc[i, 'fldFK_Localizacion']):
                animalObj.localization.set(dfRA.loc[i], dfLink.loc[i], df_data_localiz.loc[i])

            # Personas: Owner debe venir especificado en tabla tblDataPersonas. Si no hay,se setea a PersonaOrejano
            activePersonsUID = set()
            noOwner = None
            for frame in Person.obj_dataframe():
                activePersonsUID.update(frame['fldObjectUID'].tolist())
                if noOwner is None:
                    try:            # Pulls uid for Orejano (noOwner).
                        noOwner = frame.loc[frame['fldLastName'].str.lower().isin(['orejano']),
                                            'fldObjectUID'].tolist()[0]
                    except IndexError:
                        continue

            tempOwners = pd.DataFrame.db.create(tblPersonasName)  # df vacio p/ registrar todos los owners de animalObj
            # Recorre las tablas de *args y crea una Tabla con todos los propietarios de animalObj.
            if activePersonsUID:
                for j, row in df_data_personas.iterrows():
                    # i (index) is the identifier field to group owners of animal obj defined by row i in all tables.
                    if row.index[0] == i and row.fldFK_Persona in activePersonsUID:   # Only active Persons, Level=1
                        tempOwners.loc[j, ('fldFK_Actividad', 'fldDate', 'fldFK_Persona', 'fldFlag',
                                           'fldPercentageOwnership', 'fldComment')] = \
                            (idActividadRA, eventDate, row.fldFK_Persona, 1, row.fldPercentageOwnership
                             if row.fldPercentageOwnership <= 1 else row.fldPercentageOwnership/100, row.fldComment)
            if tempOwners['fldPercentageOwnership'].sum() > 1:
                raise ValueError(f'ERR_ValueError Alta Animales() - Invalid data: Total % of ownership exceeds 1.')
            # Si no hay owners, setea Orejano
            if tempOwners.empty:
                tempOwners.loc[i, ('fldFK_Actividad', 'fldDate', 'fldFlag', 'fldFK_Persona', 'fldPercentageOwnership',
                                   'fldComment')] = (idActividadRA, eventDate, 1, noOwner, 1, 'Orejano')
            animalObj.person.set(dfRA.loc[i], dfLink.loc[i], tempOwners)
            # processedTables.add(df_data_personas)

            # MoneyActivity: Setea Tabla [Data Animales Actividad MoneyActivity] con la(s) idActividadRA_TM pasadas en
            # *args (si existen).
            # TODA la informacion de Registro De Actividades MoneyActivity, Transacciones y Montos debe ser manejada
            # por las funciones de alto nivel y escrita en las tablas respectivas. Aqui solo se actualiza ID_Actividad
            # MoneyActivity y se pasa el resto de la obj_data tal como viene de las funciones de alto nivel.
            if not df_data_tm.empty:
                # Llamadas a metodos MoneyActivity para Actualizar Transacciones y Montos
                activityTM = df_data_tm.loc[i, 'fldFK_ActividadTM']
                if pd.notnull(activityTM):
                    animalObj.tm.set(fldFK_ActividadTM=activityTM)      # IMPORTANT: 1 df_data_tm row per Animal object.

            # Si es Actividad Inventario, setea tabla Inventario. Logica: kwargs['recordInventory'] overrides seteo de
            # _isInventoryActivity. Si no se pasa kwargs['recordInventory'] -> usa _isInventoryActivity
            # if not recordInventory:
            #     pass  # NO setear inventario: Overrides _isInventoryActivity
            # else:
            #     if self._isInventoryActivity:
            #         tblInventory = df_data_inventory
            #         df_data_inventory.loc[0, 'fldFK_Actividad'] = idActividadRA
            #         if pd.isnull(df_data_inventory.loc[0, 'fldDate']):
            #             df_data_inventory.loc[0, 'fldDate'] = eventDate
            #         # TODO(cmt): The call to inventory.set() will set the inventory date in __memory_data dictionary.
            #         animalObj.inventory.set(dfRA, dfLink, tblInventory)

            # Leave this out for now to avoid recording repeat parturitions on a mother.
            # if pd.notnull(dfObjects.loc[0, 'fldFK_AnimalMadre']) and 'nacimiento' in tipo_de_alta.lower():
            #     mother_obj = cls.getObject(dfObjects.loc[0, 'fldFK_AnimalMadre'])
            #     if mother_obj:
            #         mother_obj.parturition.set(*args, even_date=dob, **kwargs)  Sets the parturition event for mother.

            animal_list.append(animalObj)

        return animal_list





    # def __perform01(self, tipo_de_alta: str = None, *args: pd.DataFrame, tags=None, **kwargs):
    #     """
    #     Executes the Alta Operation for Animales. Can perform batch altaMultiple() for multiple idRecord.
    #     Runs a for loop for each row of Objects dataframe.
    #     @param tags: List of tuples of Tag object(s) to assign to new Animal objects.
    #     @param tipo_de_alta: 'Nacimiento', 'Compra', etc. If != None, overrides settings in AltaBaja DataFrame.
    #     @param animalKind: string! Kind of Animal to create Objects ('Vacuno','Caprino','Ovino','Porcino','Equino') or
    #     cls when called via Animal Class.
    #     @param tipo_de_alta: (string) from table tblAnimalesTiposDeAltaBaja.
    #     @param args: DEPRECATED. Not used for passing table data anymore.
    #     @param kwargs: table data to create DataFrames, in the form:
    #                     {tblName1:{data_dict 1}, tblName2: {data_dict 2}, } data_dict must be in the DataFrame format.
    #             Special kwargs (none are mandatory):
    #                 animalMode='regular' (Default), 'substitute', 'dummy', 'external', 'generic'.
    #                 recordInventory=1 -> Activity must be counted as Inventory
    #                 eventDate=Date of Activity. getNow() if not provided.
    #                 tags = [tagObj1, tagObj1, ]. List of Tag Objects. Tags are MANDATORY for regular and substitute
    #                 animals
    #                 tagCommissionType: 'Comision' (Default), 'Comision - Reemplazo', 'Comision - Reemision'
    #     @return: Animal Object (Bovine, Caprine, Ovine, etc) or errorCode (str)
    #     """
    #     global categNames, categIDs
    #     processedTables = set()
    #
    #     if df_categories_names.empty:
    #         retValue = f'ERR_DBAccess: cannot read Categories table from database. - {callerFunction()}'
    #         raise sqlite3.DatabaseError(f'{moduleName()}({lineNum()}) - {retValue}')
    #
    #     cls = self.outerObject      # cls is Bovine, Caprine, etc.
    #     try:
    #         if cls not in cls.getAnimalClasses():
    #             raise TypeError(f'ERR_INP_Invalid argument: {cls} is not a valid Animal Class.')
    #     except(AttributeError, NameError, TypeError):
    #         raise TypeError(f'ERR_INP_Invalid argument: {cls} is not a valid Animal Class.')
    #
    #     try:
    #         tipo_de_alta = tipo_de_alta.strip().lower()
    #     except AttributeError:
    #         tipo_de_alta = None
    #     tipo_de_alta = tipo_de_alta if tipo_de_alta in altaDict else None
    #     if tipo_de_alta is None:
    #         retValue = f'ERR_INP_Invalid Argument Tipo De AltaBaja={tipo_de_alta} - {callerFunction()}'
    #         krnl_logger.info(retValue)
    #         print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
    #         return retValue
    #
    #     retValue = None       # Returns an animal object successfully created
    #     # df_data_category = setupArgs(tblDataCategoriasName, *args)
    #     # tblObjects = setupArgs(tblObjectsName, *args, **kwargs)  # FechaEvento(fldDate) should be passed in kwargs
    #     df_data_category = next((j for j in args if j.db.tbl_name == tblDataCategoriasName),
    #                             pd.DataFrame.db.create(tblDataCategoriasName))
    #     dfObjects = next((j for j in args if j.db.tbl_name == tblObjectsName), pd.DataFrame.db.create(tblObjectsName))
    #
    #     if any(j in tipo_de_alta for j in ('nacimient', 'ingreso', 'compra')):
    #         animalMode = 'regular'
    #     elif any(j in tipo_de_alta for j in ('sustituc', 'substituc')):
    #         animalMode = 'substitution'
    #     elif 'dummy' in tipo_de_alta:
    #         animalMode = 'dummy'
    #     elif 'extern' in tipo_de_alta:
    #         animalMode = 'external'
    #     elif 'generic' in tipo_de_alta:
    #         animalMode = 'generic'
    #     else:
    #         animalMode = str(next((kwargs[j] for j in kwargs if 'mode' in str(j).lower()), None)).strip().lower()
    #     animalMode = animalMode if animalMode in cls.getAnimalModeDict() else None
    #     dob = dfObjects.loc[0, 'fldDOB'] if not dfObjects.empty else None
    #     tags = tags if hasattr(tags, '__iter__') else (tags, )
    #     tags = [t for t in tags if isinstance(t, Tag) and t.isAvailable]  # Filters only available tags. Ignores rest.
    #     # # List of tags used by this Animal Class (there can be repeat tags in different Animal classes)
    #
    #     if dfObjects.loc[0, dfObjects.db.col('fldMF')].upper() not in ('M', 'F'):
    #         retValue = f'ERR_UI_InvalidArgument: Male/Female missing or not valid'
    #     elif tipo_de_alta not in altaDict:
    #         retValue = f'ERR_UI_InvalidArgument: Tipo de Alta {tipo_de_alta} not valid'
    #     elif not isinstance(dob, datetime):
    #         retValue = f'ERR_UI_InvalidArgument: DOB {dob} not valid'
    #     elif animalMode not in cls.getAnimalModeDict():
    #         retValue = f'ERR_UI_InvalidArgument: Animal Mode {animalMode} not valid'
    #     elif cls not in cls.getAnimalClasses():
    #         retValue = f'ERR_UI_InvalidArgument: {cls} is not a valid Animal Class.'
    #     elif df_data_category.db.col('fldFK_Categoria') not in df_data_category.columns:
    #         retValue = f'ERR_UI_InvalidArgument: Animal Category missing / not valid'
    #     elif not tags and 'regular' in animalMode or 'substitu' in animalMode:
    #         retValue = f'ERR_UI_InvalidArgument: Mandatory tags missing - {callerFunction()}'
    #     elif not tags:      # checks if any tags in tags are already in use by the Animal class.
    #         retValue = f'ERR_INP_Invalid Tags: Tags assigned are not available. Please pick fresh tags.'
    #     if type(retValue) is str:
    #         krnl_logger.info(retValue, exc_info=True, stack_info=True)
    #         print(f'{moduleName()}({lineNum()}) - {retValue}')
    #         raise ValueError(retValue)
    #
    #     animalClassID = cls.getAnimalClassID()  # animalClassID is: 1, 2, 3, etc.
    #     timeStamp = time_mt('datetime')  # TimeStamp will be written on RA, RA_TM
    #     eventDate = timeStamp   # eventDate will be written in all tables with field Fecha Evento in non-valid state
    #     recordInventory = 0
    #     if kwargs:     # kwargs checks of general arguments (valid for all objects and all records to be written)
    #         # if Event date passed is not valid or not passed, sets eventDate=timeStamp
    #         # eventDate de tblObjects tiene precedencia sobre eventDate de kwargs.
    #         eventDate = dfObjects.loc[0, dfObjects.db.col('fldDate')] if not dfObjects.empty else \
    #                     next((v for k, v in kwargs.items() if 'eventdate' in k.lower()), None)
    #         eventDate = valiDate(eventDate, timeStamp)
    #         recordInventory = next((v for k, v in kwargs.items() if 'recordinvent' in k.lower() and
    #                                 v in (0, 1, True, False)), False)
    #
    #     dfRA = pd.DataFrame.db.create(tblRAName)  # Blank dataframe.
    #     dfRA.loc[0, 'fldFK_ClaseDeAnimal'] = animalClassID
    #     dfLink = pd.DataFrame.db.create(tblLinkName)  # Blank table. Nothing for user to write on this table
    #     dfData = next((j for j in args if j.db.tbl_name == self.__tblDataName),
    #                   pd.DataFrame.db.create(self.__tblDataName))
    #     if dfData.empty or pd.isnull(dfData.loc[0, 'fldDate']):
    #         dfData.loc[0, 'fldDate'] = eventDate
    #     df_data_caravanas = next((j for j in args if j.db.tbl_name == tblDataCaravanasName),
    #                               pd.DataFrame.db.create(tblDataCaravanasName))
    #     df_data_personas = next((j for j in args if j.db.tbl_name == tblPersonasName),
    #                             pd.DataFrame.db.create(tblPersonasName))
    #     df_data_personas.loc[0, 'fldDate'] = eventDate
    #     df_data_status = next((j for j in args if j.db.tbl_name == tblDataStatusName),
    #                           pd.DataFrame.db.create(tblDataStatusName))
    #     df_data_localiz = next((j for j in args if j.db.tbl_name == tblDataLocalizacionName),
    #                            pd.DataFrame.db.create(tblDataLocalizacionName))
    #     df_data_castration = next((j for j in args if j.db.tbl_name == tblDataCastracionName),
    #                               pd.DataFrame.db.create(tblDataCastracionName))
    #     df_data_inventory = next((j for j in args if j.db.tbl_name == tblDataInventarioName),
    #                              pd.DataFrame.db.create(tblDataInventarioName))
    #     df_data_pregnancy = next((j for j in args if j.db.tbl_name == tblDataPrenezName),
    #                              pd.DataFrame.db.create(tblDataPrenezName))
    #     df_data_branding = next((j for j in args if j.db.tbl_name == tblDataMarcaName),
    #                             pd.DataFrame.db.create(tblDataMarcaName))
    #     df_tm_transaction = next((j for j in args if j.db.tbl_name == tblTransactName),
    #                              pd.DataFrame.db.create(tblTransactName))  # Tabla MoneyActivity. Pasar COMPLETA!
    #     df_data_tm = next((j for j in args if j.db.tbl_name == tblDataTMName),
    #                       pd.DataFrame.db.create(tblDataTMName))        # Animales Actividad MoneyActivity
    #     df_data_destete = next((j for j in args if j.db.tbl_name == tblDataDesteteName),
    #                            pd.DataFrame.db.create(tblDataDesteteName))
    #     df_data_medicion = next((j for j in args if j.db.tbl_name == tblDataMedicionName),
    #                             pd.DataFrame.db.create(tblDataMedicionName))
    #     df_data_possession = next((j for j in args if j.db.tbl_name == tblDataEstadoDePosesionName),
    #                          pd.DataFrame.db.create(tblDataEstadoDePosesionName))  # Si no se pasa->Propio por Default
    #     dfRA_TM = pd.DataFrame.db.create(tblRA_TMName)  # Blank table.  # 1 solo registro en esta tabla.
    #
    #     # Servicios, Sanidad, Inseminacion, Temperatura, Tacto, Curacion, Activ. Progr., Alimentacion NO SE HACEN aqui.
    #
    #     userID = sessionActiveUser
    #     dfRA.loc[0, dfRA.db.cols('fldTimeStamp', 'fldFK_UserID', 'fldFK_NombreActividad')] = \
    #         (timeStamp, sessionActiveUser, self.getActivitiesDict()[self._activityName])
    #     idActividadRA = setRecord(tblRAName, **dfRA.loc[0].to_dict())  # Inserta registro en tabla RA
    #     if isinstance(idActividadRA, str):
    #         retValue = idActividadRA + f' {callerFunction()}({lineNum()})'
    #         krnl_logger.error(retValue)
    #         print(f'{retValue}', dismiss_print=DISMISS_PRINT)
    #         return retValue  # Sale c/error
    #     else:
    #         dfRA.loc[0, 'fldID'] = idActividadRA
    #
    #     # 2. Create Animal Object
    #     countMe = cls.getAnimalModeDict()[animalMode] if animalMode in cls.getAnimalModeDict() else 0
    #     # Define valid categories for animal kind. Assign category Name.
    #     categNames = [categNames[j] for j in range(len(categNames)) if int(categAnimalClass[j]) == animalClassID]
    #     categIDs = [categIDs[j] for j in range(len(categIDs)) if int(categAnimalClass[j]) == animalClassID]
    #     __validCategories = dict(zip(categIDs, categNames))
    #     __castrados = [categNames[j] for j in range(len(categNames)) if int(castradosCol[j]) == 1]
    #     category = int(df_data_category.loc[0, 'fldFK_Categoria'])
    #     if category not in __validCategories or len(__validCategories) == 0:
    #         retValue = f'ERR_UI_CategoryNotValid: {category} - {callerFunction()}'
    #         krnl_logger.warning(retValue)
    #         print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
    #         return retValue  # Salta creacion de objeto Animal Categoria no es valida
    #     categoryName = __validCategories[category]
    #     print(f'%%%%%%%%%%% {moduleName()}({lineNum()}) animalMode = {animalMode} / categoryName: {categoryName}',
    #           dismiss_print=DISMISS_PRINT)
    #     conceptionType = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('concep')), 'Natural')
    #     eventDate = valiDate(dfData.loc[0, 'fldDate'], timeStamp)
    #     dateExit = valiDate(dfObjects.loc[0, 'fldDateExit'], 0)
    #     # Setea estado de Castracion (_fldFlagCastrado y tablas en DB)
    #     castrated = df_data_castration.loc[0, 'fldDate'] if not df_data_castration.empty else None
    #     if not castrated:
    #         castrated = 1 if categoryName in __castrados else 0
    #     else:
    #         castrated = valiDate(castrated, 1)
    #     objID = str(uuid4().hex)        # UID for Animal.
    #     dob = dfObjects.loc[0, 'fldDOB']
    #     if not isinstance(dob, datetime):
    #         try:
    #             cls.generateDOB(category)   # system-generated DOB based on category, if DOB not provided.
    #         except AttributeError:
    #             retValue = f'{callerFunction()}({moduleName()}) ERR_UI_Invalid or missing DOB.'
    #             return retValue
    #         else:
    #             dfObjects.loc[0, 'fldDOB'] = dob
    #
    #     dfObjects.loc[0, ('fldID', 'fldFK_ClaseDeAnimal', 'fldMode',
    #                        'fldCountMe',
    #                        'fldFlagCastrado',
    #                        'fldConceptionType',
    #                        'fldFK_Raza',
    #                        'fldComment',
    #                        'fldTimeStamp', 'fldDateExit',
    #                        'fldFK_UserID',
    #                        'fldObjectUID',
    #                        ' fldDaysToTimeout')] = \
    #         (None, animalClassID, animalMode,
    #             countMe,
    #             castrated,
    #             conceptionType,
    #             dfObjects.loc[0, 'fldFK_Raza'],
    #             dfObjects.loc[0, 'fldComment'],
    #             eventDate, dateExit,
    #             userID,
    #             objID,
    #             dfObjects.loc[0, 'fldDaysToTimeout'] or cls._defaultDaysToTimeout)
    #
    #     idRecord = setRecord(tblObjectsName, **dfObjects.loc[0].to_dict())
    #
    #     if not isinstance(idRecord, int):
    #         retValue = f'ERR_DBAccess: Unable to initialize Animal object in Database.'
    #         krnl_logger.error(retValue)
    #         raise DBAccessError(retValue)
    #     dfObjects.loc[0, 'fldID'] = idRecord
    #
    #     # Setea tblRA, tblLink, tblData
    #
    #     dfRA.loc[0, ('fldID', 'fldTerminal_ID', 'fldComment')] = \
    #                 (idActividadRA, TERMINAL_ID, f'Alta. ID Animal: {idRecord}')  # Setea fldID en tblRA.
    #     dfLink.loc[0, ('fldFK_Actividad', 'fldFK', 'fldComment')] = \
    #         (idActividadRA, objID, f'{self.__activityName} / ID:{idRecord}')
    #     dfData.loc[0, ('fldFK_Actividad', 'fldFK_TipoDeAltaBaja', 'fldDate')] = \
    #         (idActividadRA, altaDict[tipo_de_alta], eventDate)
    #     # _ = (tblRA.setRecords(), tblLink.setRecords(), tblData.setRecords())
    #     _ = (setrecords(dfRA), setrecords(dfLink), setrecords(dfData))     # List of tuples of sqlit3.Cursor objects.
    #     try:
    #         errResult = [j for item in _ for j in item if j.rowcount <= 0]     # <=0 -> Error: Nothing was written.
    #     except (sqlite3.DatabaseError, sqlite3.OperationalError, sqlite3.DataError):
    #         errResult = True
    #     if errResult:
    #         retValue = f'ERR_DB_WriteError: {moduleName()}({lineNum()}): {str(errResult)} - {callerFunction()}'
    #         krnl_logger.warning(retValue)
    #         raise DBAccessError(retValue)
    #
    #     # categoryName NO es parte de tabla Animales. Se necesita para llamar generateDOB() y definir fldDOB.
    #     # NO SE PASAN Tags en los constructores de Animal, Bovine, etc. Se deben setear via metodo assign()
    #     animalObj = cls(fldID=idRecord, fldMF=dfObjects.loc[0, 'fldMF'].lower(), fldFK_ClaseDeAnimal=animalClassID,
    #                     fldObjectUID=objID,
    #                     fldMode=animalMode, categoryName=categoryName,
    #                     fldFlagCastrado=dfObjects.loc[0, 'fldFlagCastrado'] or (1 if categoryName in __castrados else 0),
    #                     fldDOB=dfObjects.loc[0, 'fldDOB'],
    #                     fldConceptionType=conceptionType,
    #                     fldFK_Raza=dfObjects.loc[0, 'fldFK_Raza'],
    #                     fldComment=dfObjects.loc[0, 'fldComment'],
    #                     fldTimeStamp=eventDate, fldDateExit=dateExit,
    #                     fldFK_UserID=userID,
    #                     fldDaysToTimeout=dfObjects.loc[0, 'fldDaysToTimeout']
    #                     )
    #     if not isinstance(animalObj, cls):
    #         retValue = f'ERR_Sys_ObjectNotCreated: {animalObj} - {callerFunction()}'
    #         krnl_logger.error(retValue)
    #         print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
    #         raise RuntimeError(retValue)
    #
    #     # animalObj.register()            # Incluye animal nuevo en registerDict.
    #
    #     # processedTables.update((tblObjects.tblName, tblRA.tblName, tblLink.tblName, tblData.tblName))
    #     print(f'\nFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF {moduleName()}({lineNum()}) - '
    #           f'Animal Object is type: {type(animalObj)}   FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\n',
    #           dismiss_print=DISMISS_PRINT)
    #
    #     # Computa y Setea Cagetoria
    #     # TODO(cmt): This call will set the animal category in memory_data dataframe.
    #     _ = animalObj.category.get(set_category=True)
    #     processedTables.update(df_data_category)
    #
    #
    #     # TAGS: Asignacion de tag(s) a animalObj y escritura  en tabla [Data Animales Actividad Caravanas]
    #     if tags:
    #         kwargs['tags'] = tags
    #         animalObj.tags.assignTags(dfRA, dfLink, **kwargs)  # kwargs ->tagCommissionType=Comision,Reemplazo,Reemision
    #         # processedTables.update(df_data_caravanas)
    #         print(f'%%%%%% {moduleName()}({lineNum()}) - MYTAGS:{animalObj.myTagIDs} / Tag Numbers:{tags[0].tagNumber}',
    #               dismiss_print=DISMISS_PRINT)
    #
    #     # 3. Object-driven methods: Setup DataTables values and perform writes with methods.
    #     # tblObjects.setVal(0, fldFlagCastrado=castrated)
    #     animalObj.castration.set(dfRA, dfLink, df_data_castration, event_date=castrated)
    #     print(f'###########{moduleName()}({lineNum()}) fldID: {dfObjects.loc[0, "fldID"]} / dateExit: {dateExit} '
    #           f'/ eventDate = {eventDate} / castrated = {castrated} / timeStamp: {timeStamp} / mode: {animalMode}',
    #           dismiss_print=DISMISS_PRINT)
    #
    #     # Status
    #     statusID = df_data_status.loc[0, 'fldFK_Status']
    #     statusID = statusID if statusID is not None else animalObj.statusDict['En Stock'][0]
    #     statusName = next(j for j in animalObj.statusDict if animalObj.statusDict[j][0] == statusID)
    #     animalObj.status.set(dfRA, dfLink, df_data_status, status=statusName)
    #
    #     # LocalizationActivityAnimal
    #     if not df_data_localiz.empty and pd.notnull(df_data_localiz.loc[0, 'fldFK_Localizacion']):
    #         animalObj.localization.set(dfRA, dfLink, df_data_localiz)
    #
    #     # Personas: Owner debe venir especificado en tabla tblDataPersonas. Si no hay,se setea a Persona_Orejano
    #     persons = Person.getRegisterDict()  # Lista de Personas activas (Propietarios validos) para hacer verificaciones
    #     activePersons = [persons[j] for j in persons if persons[j].isActive]  # lista de ID_Personas Activas en el sistema
    #     activePersonsID = set([j.getID for j in activePersons])
    #     noOwnerPerson = 1  # Persona No Owner / TODO(cmt): Orejano. 1 por ahora, puede cambiar.
    #     tempOwners = pd.DataFrame.db.create(tblPersonasName)  # df vacio para registrar todos los owners de animalObj
    #     # Recorre TODAS las tablas de *args y crea una Tabla con todos los propietarios de animalObj.
    #     if activePersonsID:
    #         for j, row in df_data_personas.iterrows():
    #             if row.fldFK_Persona in activePersonsID:   # Solo personas Activas, con Level=1
    #                 tempOwners.loc[j, ('fldFK_Actividad', 'fldDate', 'fldFK_Persona', 'fldFlag',
    #                                    'fldPercentageOwnership', 'fldComment')] = \
    #                     (idActividadRA, eventDate, row.fldFK_Persona, 1, row.fldPercentageOwnership
    #                      if row.fldPercentageOwnership <= 1 else row.fldPercentageOwnership/100, row.fldComment)
    #     if tempOwners['fldPercentageOwnership'].sum() > 1:
    #         raise ValueError(f'ERR_ValueError: Alta Animales(). Total % of ownership exceeds 1. '
    #                          f'Data provided is invalid. ')
    #     # Si no hay owners, setea Orejano
    #     if tempOwners.empty:
    #         tempOwners.loc[0, ('fldFK_Actividad', 'fldDate', 'fldFlag', 'fldFK_Persona', 'fldPercentageOwnership',
    #                            'fldComment')] = (idActividadRA, eventDate, 1, noOwnerPerson, 1, 'Orejano')
    #     animalObj.person.set(dfRA, dfLink, tempOwners)
    #     processedTables.add(df_data_personas)
    #
    #     # MoneyActivity: Setea Tabla [Data Animales Actividad MoneyActivity] con la(s) idActividadRA_TM pasadas en *args
    #     # (si existen).
    #     # TODA la informacion relativa a Registro De Actividades MoneyActivity, Transacciones y Montos debe ser manejada por
    #     # las funciones de alto nivel y escrita en las tablas respectivas. Aqui solo se actualiza ID_Actividad MoneyActivity y se
    #     # pasa el resto de la obj_data tal como viene de las funciones de alto nivel.
    #     if not df_data_tm.empty:
    #         # LLAMADAS A METODOS MoneyActivity para Actualizar Transacciones y Montos
    #         activityTMList = df_data_tm['fldFK_ActividadTM'].tolist()   # Usa lista, para cubrir el caso general
    #         # if type(activityTMList) not in (tuple, list, set):
    #         #     activityTMList = [int(activityTMList), ]             # Convierte a lista para procesar
    #         for j in activityTMList:
    #             if pd.notnull(j):
    #                 animalObj.tm.set(fldFK_ActividadTM=j)
    #
    #     # Si es Actividad Inventario, setea tabla Inventario. Logica: kwargs['recordInventory'] overrides seteo de
    #     # _isInventoryActivity. Si no se pasa kwargs['recordInventory'] -> usa _isInventoryActivity
    #     if not recordInventory:
    #         pass  # NO setear inventario: Overrides _isInventoryActivity
    #     else:
    #         if self._isInventoryActivity:
    #             tblInventory = df_data_inventory
    #             df_data_inventory.loc[0, 'fldFK_Actividad'] = idActividadRA
    #             if pd.isnull(df_data_inventory.loc[0, 'fldDate']):
    #                 df_data_inventory.loc[0, 'fldDate'] = eventDate
    #             # TODO(cmt): The call to inventory.set() will set the inventory date in __memory_data dictionary.
    #             animalObj.inventory.set(dfRA, dfLink, tblInventory)
    #
    #     if pd.notnull(dfObjects.loc[0, 'fldFK_AnimalMadre']) and 'nacimiento' in tipo_de_alta.lower():
    #         mother_obj = cls.getObject(dfObjects.loc[0, 'fldFK_AnimalMadre'])
    #         if mother_obj:
    #             mother_obj.parturition.set(*args, even_date=dob, **kwargs)  # Sets the parturition event for the mother.
    #
    #     return animalObj





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
    __method_name = 'baja'          # Used in activityCreator() to create the callable property.
    __one_time_activity = True      # Don't have a use for this yet. It don't look like this attribute makes sense...


    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True    # Inventory does support PA.
        kwargs['decorator'] = self.__method_name
        kwargs['one_time_activity'] = self.__one_time_activity
        # Registers activity_name to prevent re-instantiation in GenericActivity.
        self.definedActivities()[self.__activityName] = self
        super().__init__(self.__activityName, *args, activity_enable=activityEnableFull,
                         tbl_data_name=self.__tblDataName, **kwargs)
        dftemp = getrecords('tblAnimalesTiposDeAltaBaja', 'fldID', 'fldName',
                            where_str=f'WHERE "{getFldName("tblAnimalesTiposDeAltaBaja", "fldAltaBaja")}" == "Baja"')
        self.tipoDeBajaDict = dict(zip(dftemp['fldName'], dftemp['fldID']))

    # TODO(cmt): Auto-call class: All instances of this class call this method whenever invoked with ().
    def __call__(self, caller_object=None, *args, **kwargs):     # caller_obj here is Bovine, Caprine, Ovine, etc.
        """...If a method func has a parameter 'caller_object' bound to it, and gets called with *args, it will add
        'caller_object' to the beginning of *args and then pass it to the function. The beginning is important here...
        """
        self.outerObject = caller_object   # caller_obj can be a class (Bovine) or an Object instance (animal_object).
        return self.__perform(*args, **kwargs)

        # def inner(*args, **kwargs):
        #     # inner() can only be implemented for mono-method classes, where __call__ invokes 1 and ONLY 1 method.
        #     return self.__perform(*args, **kwargs)
        # return inner


    def __perform(self, bajaType: str = None, *args, **kwargs):    # Called via @property in Class Animal
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
        # Serializer to manage access to dummy counter values read from DB.
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=tblRAName, dfLink_name=tblLinkName,
                                               dfData_name=self.__tblDataName)

        wherestr = f'WHERE "{getFldName(tblObjectsName, "fldObjectUID")}" == "{outerObj.ID}"'
        dfObjects = getrecords(tblObjectsName, where_str=wherestr)        # tblAnimales
        print(f'|||||||||||||||||||||||||||||||| perform object type: {type(outerObj)} / objectID: {outerObj.ID} / '
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
        dfAniGeneralParams = pd.DataFrame.db.create(tblAniGenParametersName)

        if outerObj.isSubstitute:   # Logica para crear dummy Animal cuando es perform de un substitute (Ver flowchart)
            eventDate = time_mt('datetime')
            # tblDummyAnimal = DataTable(tblObjectsName)
            dfDummyAnimal = pd.DataFrame.db.create(tblObjectsName)
            dfDummyAnimal.loc[0, ('fldFK_ClaseDeAnimal', 'fldDOB', 'fldTimeStamp', 'fldFK_Raza', 'fldCountMe',
                                   'fldFK_AnimalMadre', 'fldDateExit', 'fldMF')] = \
              (outerObj.animalClassID, outerObj.dob, eventDate, outerObj.animalRace, -1, outerObj.ID, None, outerObj.mf)

            dfDummyCategory = pd.DataFrame.db.create(tblDataCategoriasName)
            dfDummyCategory.loc[0, 'fldFK_Categoria'] = outerObj.category.get()
            dfDummyStatus = pd.DataFrame.db.create(tblDataStatusName)
            dfDummyStatus.loc[0, 'fldFK_Status'] = 3           # En Stock - Improductivo
            dfDummyLocaliz = pd.DataFrame.db.create(tblDataLocalizationName)
            dfDummyLocaliz.loc[0, 'fldFK_Localizacion'] = outerObj.localization.get()

            # with lock:
            # tmp = getRecords(tblAniGenParametersName, '', '', None, 'fldID', 'fldParameterValue',
            #                  fldFK_ClaseDeAnimal=animalClassID, fldFK_NombreParametro=paramIndex)
            wherestr = f'WHERE "{getFldName(tblObjectsName, "fldFK_ClaseDeAnimal")}" == "{animalClassID.ID}" AND' \
                       f'"{getFldName(tblObjectsName, "fldFK_NombreParametro")}" == "{paramIndex}"'

            # Acquires lock to prevent access to dummyCounter value in DB while it's being retrieved.
            # It only locks access to tblAniGenParametersName tbl. The rest of the db operates normally, whithout locks.
            # A particular use for DBAccessSemaphore: Semaphore that when > 1 puts other threads to wait until this
            # thread notifies a waiting thread toresume execution, and so on with other waiting threads.
            # This use does not involve any code sections protected by the global lock.
            with DBAccessSemaphore(tblAniGenParametersName):
                tmpdf = getrecords(tblAniGenParametersName, 'fldID', 'fldParameterValue', where_str=wherestr)
                dummyCounter = tmpdf.loc[0, 'fldParameterValue']  # Toma counter de Dummies de database.
            # End of "semaphored" block.

            if dummyCounter <= 0:  # Crea self. Bovine (countMe=-1) y da de Alta. __init__() hace Register de Bovine
                dummyAnimal = outerObj.__class__.perform('dummy', dfDummyAnimal, dfDummyCategory, dfDummyStatus,
                                                         dfDummyLocaliz)
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
                dfAniGeneralParams.loc[0, ('fldID', 'fldParameterValue')] = (tmpdf.loc[0, 'fldID'], dummyCounter)
                setRecord(dfAniGeneralParams.db.tbl_name, **dfAniGeneralParams.loc[0].to_dict())

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
                    wherestr = f'WHERE  "{getFldName(tblAniGenParametersName, "fldFK_ClaseDeAnimal")}" == ' \
                               f'"{animalClassID}" AND' \
                               f'"{getFldName(tblAniGenParametersName, "fldFK_NombreParametro")}" == "{paramIndex}"'
                    dftmp = getrecords(tblAniGenParametersName, 'fldID', 'fldParameterValue', where_str=wherestr)
                    dummyCounter = dftmp.loc[0, 'fldParameterValue']
                        # Fin bloque no reentrante
                    dfAniGeneralParams.loc[0, ('fldID', 'fldParameterValue')] = (dftmp.loc[0, 'fldID'], dummyCounter+1)

                    setRecord(dfAniGeneralParams.db.tbl_name, **dfAniGeneralParams.loc[0].to_dict())
                else:
                    kwargs['fldFK_AnimalAsociado'] = dummyOut.getID

        retValue = self.__performBaja(self.tipoDeBajaDict[bajaType], dfRA, dfLink, dfData, dfObjects, *args, **kwargs)
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

        # tblRA = setupArgs(tblRAName, *args)
        # tblLink = DataTable(tblLinkName)
        # tblData = setupArgs(self.__tblDataName, *args, **kwargs)
        # fldDate, fldFK_AnimalAsociado passed in fldDate in tblData. if fldDate not supplied, timeStamp is used
        dfRA, dfLink, dfData = self.set3frames(*args, dfRA_name=tblRAName, dfLink_name=tblLinkName,
                                               dfData_name=self.__tblDataName)
        dfDataTM = pd.DataFrame.db.create(tblDataTMName)
        dfMontos = pd.DataFrame.db.create(tblMontosName)
        dfCaravanas = pd.DataFrame.db.create(tblDataCaravanasName)
        dfDataCategory = pd.DataFrame.db.create(tblDataCategoriasName)
        dfObjects = pd.DataFrame.db.create(tblObjectsName)


        timeStamp = time_mt('datetime')
        userID = sessionActiveUser
        eventDate = dfData.loc[0, 'fldDate']. to_pydatetime() if pd.notnull(dfData.loc[0, 'fldDate']) else timeStamp
        activityID = AnimalActivity.getActivitiesDict()[self.activityName]
        # Sobreescribe campo fldExitDate, Identificadores en tblAnimales(fldID=idAnimal)->Indica NO cargar Animal
        # durante inicializacion
        # _ = tblObjects.getVal(0, 'fldDateExit')
        dfObjects.loc[0, ('fldID', 'fldDateExit', 'fldIdentificadores')] = (animalObj.recordID, eventDate, None)
        _ = setrecords(dfObjects)  # Escribe fldDateExit en tblAnimales.

        dfRA.loc[0, ('fldTimeStamp', 'fldFK_UserID')] = (timeStamp, userID)
        idActividadRA = dfRA.loc[0, 'fldID']
        dfLink.loc[0, ('fldFK_Actividad', 'fldFK', 'fldComment')] = \
            (idActividadRA, animalObj.ID, f'{self.activityName}. ID Animal: {animalObj.recordID}')
        dfData.loc[0, ('fldFK_TipoDeAltaBaja', 'fldDate', 'fldComment')] = \
            (bajaTypeID, eventDate, f'{self.activityName}. ID Animal: {animalObj.recordID}')
        activityID = activityID if pd.isnull(dfRA.loc[0, 'fldFK_NombreActividad']) else \
                                            dfRA.loc[0, 'fldFK_NombreActividad']
        dfRA.loc[0, ('fldFK_NombreActividad', 'fldComment')] = (activityID, f'{self.activityName}.')
        # Inserta actividad de Baja en tblRA, tblLink, tblData

        idActividadRA = self._createActivityRA(dfRA, dfLink, dfData, *args) #   tbl_data_name=tblData.tblName)
        if type(idActividadRA) is str:
            retValue = f'ERR_DB_WriteError: {idActividadRA} - Function/Method: perform()'
            krnl_logger.error(retValue)
            print(f'{moduleName()}({lineNum()}) - {retValue}', dismiss_print=DISMISS_PRINT)
        else:
            dfRA.loc[0, 'fldID'] = idActividadRA
            animalObj.status.set(dfRA, dfLink, status='Baja')     # Status del Animal: Baja (4)
            # TAGS: DesAsignacion de tag(s) a animalObj y escritura  en tabla [Data Animales Actividad Caravanas]
            if animalObj.myTags:
                decomissionPermittedStatus = ('perform', 'decomisionada', 'comisionada', 'extraviada', 'reemplazada')
                tagStatus = next((str(j) for j in kwargs if 'status' in str(j).lower()), 'Baja')
                tagStatus = tagStatus if tagStatus.lower() in decomissionPermittedStatus else 'Baja'
                animalObj.tags.deassignTags(dfRA, dfLink, dfCaravanas, tags=animalObj.myTags, tagStatus=tagStatus)

            """ --- Cierra TODAS las ProgActivities en tblLinkPA, tblRAP (si aplica) y tblDataPAStatus --- """
            self._paMatchAndClose(idActividadRA, execute_date=dfData.loc[0, 'fldDate'])

            _ = animalObj.unRegister()  # pops key corresponding to this object from __memory_data dictionary


            # MoneyActivity: Setea Tabla [Data Animales Actividad MoneyActivity] con la(s) idActividadRA_TM pasadas en *args, si existen.
            # TODA la informacion relativa a Registro De Actividades MoneyActivity, Transacciones y Montos debe ser manejada por
            # las funciones de alto nivel y escrita en las tablas respectivas. Aqui solo se actualiza ID_Actividad MoneyActivity.
            if not dfDataTM.empty:
                # LLAMADAS A METODOS MoneyActivity para Actualizar Transacciones y Montos
                activityTMList = dfDataTM.loc[0, 'fldFK_ActividadTM'].tolist()  # Usa lista, para cubrir el caso general
                for j in activityTMList:
                    if pd.notnull(j):
                        animalObj.tm.set(tfldFK_ActividadTM=j)

            # Si es Actividad Inventario, setea tabla Inventario. Logica: kwargs['recordInventory'] overrides seteo
            # de _isInventoryActivity. Si no se pasa kwargs['recordInventory'] -> usa _isInventoryActivity
            # recordInventory = kwargs.get('recordInventory', False) if not animalObj.isDummy else False
            # if not recordInventory:
            #     pass  # NO setear inventario: Overrides _isInventoryActivity
            # else:
            #     if animalObj.isInventoryActivity:
            #         tblInventory = setupArgs(animalObj.tblDataInventoryName, *args)
            #         tblInventory.setVal(0, fldFK_Actividad=idActividadRA)
            #         tblInventory.setVal(0, fldDate=valiDate(tblInventory.getVal(0, 'fldDate'), eventDate))
            #         animalObj.inventory.set(tblRA, tblLink, tblInventory)
            retValue = idActividadRA
            krnl_logger.info(f'Removal of animal {animalObj.ID} completed.')
        return retValue

# --------------------------------------------------- FIN __performBaja() ----------------------------------------- #
