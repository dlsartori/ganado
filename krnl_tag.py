from __future__ import annotations
# from krnl_entityObject import *
import pandas as pd
import numpy as np
import itertools as it
import hashlib
from krnl_assetItem import AssetItem
from krnl_tag_activity import *
from krnl_config import callerFunction, sessionActiveUser, activityEnableFull
from krnl_db_query import AccessSerializer, AccessSerializer
from krnl_custom_types import getRecords, setRecord, getTblName, getFldName
from krnl_abstract_class_prog_activity import ProgActivity
from uuid import UUID, uuid4
# from random import randrange
from threading import Lock


def moduleName():
    return str(os.path.basename(__file__))

class Tag(AssetItem):
    # _objClass = 21
    # __objType = 1

    # Defining _activityObjList attribute will call  _creatorActivityObjects() in EntityObject.__init_subclass__().
    _activityObjList = []  # List of Activity objects created by factory function. TODO: This should be a set.
    _myActivityClass = TagActivity      # TODO: This is to be replaced by _activityClasses dict.
    _activityClasses = {}  # {tagTechnology(string): <TagTechnologyActivity>}     # Activity classes by tag technology.
    __tblObjectsName = 'tblCaravanas'
    __tblRAName = 'tblCaravanasRegistroDeActividades'
    __tblLinkName = 'tblLinkCaravanasActividades'
    _tagIdentifierChar = '-'  # Used to generate identifier = tagName + '-' + ID_TagTechnology in SQLITE Caravanas tbl.
    __subclass_register = {}       # Stores Tag subclasses -> {Tag subclass: <class Bovine>, etc | None, }
    _tech_class_register = {}      # {tagTech(str): TagTech class, }
    # __init_subclass() is used to register dynamically added Tag subclasses, in particular when new tag technology
    # modules are added to the system.
    # This code executes after the subclasses complete their creation code, WITHOUT any object instantiation. Beleza.
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if getattr(cls, '_active_uids_df', None):
            cls.register_class()

        tagTech = getattr(cls, '_' + cls.__name__ + '__tech', None)
        if tagTech:
            cls._tech_class_register[tagTech] = cls # Dict used in _create_subclass() to manufacture Tag object classes.

    @classmethod
    def register_class(cls):
        cls.__subclass_register[cls] = getattr(cls, '_objectClass', None)  # {Tag subclass: <class Bovine>, etc | None,}

    @classmethod
    def __unregister_class(cls):    # Hidden method(). unregistering a class only managed with proper privileges.
        cls.__subclass_register.pop(cls)

    @classmethod
    def getTagClasses(cls):
        return cls.__subclass_register    # Stores Tag subclasses -> {Tag subclass: <class Bovine>, etc | None, }

    @classmethod
    def getTagTechClasses(cls):
        return cls._tech_class_register


    # Listas de Tag.Activities, Inventory Activities.
    dftemp = getrecords('tblCaravanasActividadesNombres', 'fldID', 'fldName', 'fldFlag')
    # __activityID = []
    # __activityName = []
    # __activityIsInv = []
    # for j in range(temp.dataLen):
    #     __activityID.append(temp.dataList[j][0])
    #     __activityName.append(temp.dataList[j][1])
    #     __activityIsInv.append(temp.dataList[j][2])
    # __activitiesDict = dict(zip(__activityName, __activityID))  # tagActivities = {fldNombreActividad: fldID_Actividad}.
    __activitiesDict = dict(zip(dftemp['fldName'], dftemp['fldID']))
    __activitiesForMyClass = __activitiesDict
    __activeProgActivities = []             # List of all active programmed activities for Tag objects. MUST be a list.
    # __isInventoryActivity = dict(zip(__activityName, __activityIsInv))
    __isInventoryActivity = dict(zip(dftemp['fldName'], dftemp['fldFlag']))

    del dftemp

    temp1 = getrecords('tblCaravanasTecnologia', 'fldID', 'fldTagTechnology')
    tagTechDict = dict(zip(temp1['fldTagTechnology'], temp1['fldID']))           # {techName: techID(int), }
    del temp1

    __tblDataInventoryName = 'tblDataCaravanasInventario'
    __tblDataStatusName = 'tblDataCaravanasStatus'
    __tblObjDBName = getTblName(__tblObjectsName)

    temp2 = getrecords('tblCaravanasTipos', 'fldID', 'fldTagType')
    tagTypeDict = dict(zip(temp2['fldTagType'], temp2['fldID']))  # {techName: techID(int), }
    del temp2

    temp2 = getrecords('tblCaravanasFormato', 'fldID', 'fldTagFormat')
    tagFormatDict = dict(zip(temp2['fldTagFormat'], temp2['fldID']))  # {techName: techID(int), }
    del temp2


    @classmethod
    def tblObjDBName(cls):
        return cls.__tblObjDBName

    @classmethod
    def obj_mem_fields(cls):
        return cls._object_mem_fields       # defined in classes created by TagAnimal, TagDevice factory functions


    @classmethod
    def obj_dataframe(cls):
        # Produces 2 iterators: 1 to replenish the exhausted _active_uids_df; 1 to return to caller.
        with cls._sem_obj_dataframe:                # BoundedSemaphore specific to each class _active_uids_df iterator.
            ittr = it.tee(cls._active_uids_df, 2)   # tee() is not thread safe.
            cls._active_uids_df = ittr[0]
            return ittr[1]                                # OJO: _active_uids_df is an iterator.


    @classmethod
    def obj_dupl_series(cls):
        return cls._dupl_series             # defined in classes created by TagAnimal, TagDevice factory functions


    @classmethod
    def _sql_uids(cls):
        """ @return: str. SQLite sql string to execute inside _init_uid_dicts(). """
        if '*' in cls._object_mem_fields:
            mem_flds = '*'
        else:
            mem_flds = ", ".join(tuple([f'"{getFldName(cls.tblObjName(), j)}"' for j in
                                        cls._object_mem_fields])).replace('(', '').replace(')', '')
        return f'SELECT {mem_flds} FROM "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" '\
               f'IS NULL OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ; '

    @classmethod
    def _init_uid_dicts(cls):  # Reads dataframe in chunks to handle large dataframes. Pandas does ALL the memory mgmt.
        """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
        Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
        This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
        _processDuplicates() run by background functions.
         This function is called on data changes, so it will force an update of the object structures (_active_uids_df)
        @return: None
        Instead of locking out the whole code block uses a AccessSerializer obj with a wait to free up the Python lock.
        An access_count counter is used internally to identify the last caller so that only the last caller will update
        the dataframe data. Handles concurrency more efficiently than other methods my minimizing the use of the
        global mutex lock.
        _access_serializer is freed just after reading the database so that others can run this same code concurrently
        and eventually, in the case of concurrency, only one write to memory is done. The write is done by the function
        instance that made the last (latest) db read, and all others are ignored. The logic is to write to memory
        (eventually to db, when writes are done to db with this same system) ONLY ONCE when concurrent access happens.
        """

        # TODO(cmt): all the code below down to the end is protected from concurrenty re-entry by other threads.
        #  All subsequent calls to _init_uid_dicts() will go to wait() if the Semaphore in _access_serializer blocks
        #  them. The whole point of this approach is to minimize the code protected under the global lock.
        with cls._access_serializer:  # This 'with' creates a thread-safe value for access_count to be pulled below.
            access_count = cls._access_serializer.access_count
            # Starts serializing access here, by detecting all concurrent calls to _init_uid_dicts() and icrementing
            # access_count for each instance of _init_uid_dicts that runs concurrently. The entry order is needed to
            # later identify the last one to access database. This will be the instance that updates _active_uids_df.
            # The 'if' below:
            #   access_count == cls._access_serializer.total_count -> Enters 'if' for LAST execution instance of func.
            #   access_count == 1 -> Enters 'if' for the FIRST execution instance of func. To be used where needed.
            with cls._sem_obj_dataframe:  # Acquires Semaphore(n=1). Blocks further accesses to _active_uids_df.
                if access_count == cls._access_serializer.total_count:
                    # TODO(cmt): This 'if' guarantees that the LAST thread to get here concurrently will do the db read.
                    #  All the block below, protected by a semaphore.
                    read = pd.read_sql_query(cls._sql_uids(), SQLiteQuery().conn, chunksize=cls._chunk_size)

                    # tee() is NOT thread-safe but read and ittr are local vars. They won't be accessed by other threads
                    ittr = it.tee(read, 3)  # 2 iterators used for processing; last one is assigned to _active_uids_df.
                    for df in ittr[0]:      # Detects empty frames.
                        if not df.empty:
                            break
                        # Exits with error if 1st dataframe in iterator is empty.
                        val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts " \
                              f"not initialized. System cannot operate."
                        krnl_logger.warning(val)
                        raise sqlite3.DatabaseError(val)

                    dupseries = None
                    duplic_list = []
                    for df in ittr[1]:
                        # Sets up/updates a Series with duplicates for ease of access to duplicates.
                        if any(df['fld_Duplication_Index'].notnull()):
                            not_nulls = df.loc[df['fld_Duplication_Index'].notnull()]   # Picks all notnull values.
                            temp_dupl = not_nulls.groupby('fld_Duplication_Index')['fldObjectUID'].agg(set)  # Series.
                            duplic_list.append(temp_dupl)
                    if duplic_list:
                        dupseries = pd.concat(duplic_list, copy=False)

                    cls._active_uids_df = ittr[2]               # SHARED resource. Accessed from background threads.
                    if dupseries is not None:
                        try:
                            pd.testing.assert_series_equal(dupseries, cls._dupl_series)
                        except (AssertionError, TypeError, ValueError, KeyError, IndexError):
                            cls._dupl_series = dupseries.copy()    # SHARED resource. Accessed from background threads.
            # End of semaphore-protected section: releases lock to next the thread that may be waiting for semaphore.
        # End of outer 'with' block: __exit__() is called and internal access_counter is decremented.

    __trig_name_replication = 'Trigger_Caravanas Registro De Actividades_INSERT'  # Not used for now.
    __trig_name_Terminal_ID = 'Trigger_Caravanas_INSERT_Terminal_ID'
    __trig_name_NO_PHATT = 'Trigger_Caravanas_INSERT_NO_PHATT'
    __trig_name_PHATT = 'Trigger_Caravanas_INSERT_PHATT'
    __trig_name_Inventory = 'Trigger_Data Caravanas Inventario_INSERT'

    @classmethod
    def _processDuplicates(cls):  # Run by  Caravanas, Geo, Personas in other classes.
        """             ******  Run from an AsyncCursor queue. NO PARAMETERS ACCEPTED FOR NOW. ******
                        ******  Run periodically as an IntervalTimer func. ******
                        ****** This code should (hopefully) execute in less than 5 msec (switchinterval).   ******
        Re-loads duplication management dicts for class cls.
        @return: True if dicts are updated, False if reading tblAnimales from db fails or dicts not updated.
        """
        # Reads 2 records from _sys_Trigger_Tables: __trig_name_PHATT and __trig_name_NO_PHATT triggers.
        sql_duplication = f'SELECT * FROM _sys_Trigger_Tables WHERE Trigger_Name IN ' \
                          f'{str(tuple((cls.__trig_name_PHATT, cls.__trig_name_NO_PHATT)))} AND ROWID == Flag_ROWID; '

        # temp = dbRead('tbl_sys_Trigger_Tables', sql_duplication)  # Only 1 record (for Terminal_ID) is pulled.
        tempdf = pd.read_sql_query(sql_duplication, SQLiteQuery().conn)
        # if isinstance(temp, DataTable) and temp:
        # time_stamp = temp.getVal(0, 'fldTimeStamp', None)  # time of the latest update to the table.
        dates_cols = (tempdf['fldTimeStamp'].tolist(), tempdf['fldLast_Processing'].tolist())
        process_data = []
        if dates_cols[0][0] > dates_cols[1][0]:
            process_data.append(1)          # TimeStamp __trig_name_PHATT > Last_Processing
        if dates_cols[0][1] > dates_cols[1][1]:
            process_data.append(2)         # TimeStamp __trig_name_NO_PHATT > Last_Processing
        if process_data:
            try:
                cls._init_uid_dicts()  # Reloads uid_dicts for class TagBovine, TagCaprine, etc.
            except(TypeError, AttributeError, ValueError):
                return

        print(f'hhhhooooooooolaa!! Estamos en "{cls.tblObjDBName()}".processDuplicates. Just updated the dicts!!')
        # TODO(cmt): VERY IMPORTANT. _sys_Trigger_Tables.Last_Processing MUST BE UPDATED here before exiting.
        # Updates Trigger rows selectively based on which one triggered the processing. Last_Processing is set to
        # the TimeStamp read from DB, to account for the case when a new update (new TimeStamp) is inserted in the
        # Trigger row while this update is ongoing.
        if 1 in process_data:
            tempdf.loc[0, 'fldLast_Processing'] = dates_cols[0][0]
            _ = setRecord('tbl_sys_Trigger_Tables', **tempdf.loc[0].to_dict())
            # _ =setRecord('tbl_sys_Trigger_Tables', fldID=temp.getVal(0, 'fldID'), fldLast_Processing=dates_cols[0][0])
        if 2 in process_data:
            tempdf.loc[1, 'fldLast_Processing'] = dates_cols[0][1]
            _ = setRecord('tbl_sys_Trigger_Tables', **tempdf.loc[1].to_dict())
            # _ =setRecord('tbl_sys_Trigger_Tables', fldID=temp.getVal(1, 'fldID'), fldLast_Processing=dates_cols[0][1])
        return


    @classmethod
    def _processReplicated(cls):
        """ Tags do not implement ProgActivities (for now), hence replication management of Caravanas RA table is not
        needed.
        """
        return

    # List here ALL triggers defined for Animales table. Triggers can be functions (callables) or str.
    # TODO: Careful here. _processDuplicates is stored as an UNBOUND method. Must be called as _processDuplicates(cls)
    __db_triggers_list = [(__trig_name_replication, _processReplicated), (__trig_name_Terminal_ID, None),
                          (__trig_name_NO_PHATT, _processDuplicates), (__trig_name_PHATT, _processDuplicates),
                          (__trig_name_Inventory, None)]

    @staticmethod
    def create_identifier(*, elements=None):
        """ Creates a tag identifier of the form "number-assignedToClass". Example: 2845-Bovine, A98x-Caprine.
        @param elements:  (tagNumber, tagSeparator, animalClass). A tuple or list. Example: ('2845', '-', 'Bovine').
        @return: identifer (str), as Tag identifiers are all strings, for any kind of tagged object.
        """
        if not isinstance(elements, (tuple, list, set)):
            raise ValueError(f'Al carajo!!. Bad elements argument.')
        elements = list(elements)
        for i, j in enumerate(elements):
            if pd.isnull(j):
                elements[i] = ""
        return "".join(elements)            # OJITO: NO CHECKS MADE HERE!!!

    @classmethod
    def identifier_get_user_confirmation(cls, identifier, **kwargs):
        """
        Requests user confirmation of the validity of a tag identifier, in order to create a new tag from it.
        This is a UI function that must request input and validate all the parameters required for tag creation (in
        particular tag type, tech, color, localization, format, etc).
        @return: dict if positive confirmation. None if user rejects the identifier as valid (Not confirmed)
        """
        # TODO: Define here all the UID code to request user confirmation.
        tag_number, assigned_to_class = identifier.split(cls._tagIdentifierChar)
        if assigned_to_class in [v.__name__ for v in cls.getTagClasses().values() if v is not None]:
            dicto = {'fldFK_TagTechnology': 'standard', 'fldFK_Color': 1, 'fldTagMarkQuantity': 0,
                     'fldTagNumber': tag_number, 'fldFK_IDItem': None, 'fldFK_TagType': 1, 'fldFK_TagFormat': 1,
                     'fldImage': None, 'fldDateExit': None, 'fldTimeStamp': time_mt('dt'), 'fldObjectUID': uuid4().hex,
                     'fldFK_UserID': sessionActiveUser, 'fldAssignedToClass': assigned_to_class,
                     }
            for k, v in kwargs.items():
                if k in dicto:
                    dicto[k] = v
            dicto.pop('fldIdentificadores', None)   # fldIdentificadores is a generated column. Pops it just in case.
            return dicto

        return {}

    @classmethod
    def get_input(cls, input_type='keyboard'):
        """ Prompts for or fetches a Tag Number value from a Terminal or an input device and returns it.
            Must support the following input types:
            - Keyboard: User-entered using Terminal keyboard.
            - Terminal scan (using terminal's own camera / scanning device).
            - External device (input via bluetooth, NFC).
            - csv file (values read from file).
        IMPORTANT: After validating the input, the function that calls get_input() must record and Inventory for the
        resulting Tag object.
        """
        if 'keyb' in input_type.lower():
            return input('Enter Tag Number: ')
        elif 'scan' in input_type.lower():
            pass  # Code to support input via Terminal's own camera.
        elif 'external' in input_type.lower():
            pass  # Code to support input via external scanning device (linked to Terminal via bluetooth, NFC, Wifi).
        elif 'csv' in input_type.lower():
            pass  # Code to support input from formatted csv file.
        else:
            raise ValueError(f'ERR_ValueError: Invalid input type. Exiting Tag Input.')


    def input_error_resolution(self):
        """
        Logic to resolve potential input errors in Tags, accounting for tag duplication and assignment errors that arise
        from human error during tag entry
        @return: Tag uid defined as best-match by the error-resolution logic.
        TODO: This is to be deprecated now.
        """
        dupl_index = self.get_active_uids_iter().get(self.ID, None)
        # if dupl_index and self.__tagTechnology in (1, 4):      # 1: Standard tag, 4: Tatuaje tag.
        #     # duplicates_list is a list of Tag fldObjectUID that refer to the same physical tag.
        #     duplicates_list = self.get_duplication_index_dict().get(dupl_index, {})
        #     if len(duplicates_list) > 1:        # > 1 --> There's duplicates
        #         # Get Inventories for all uids in duplicates_list.
        #         tblRA = DataTable(self.inventory.getTblRAName())
        #         tblLink = DataTable(self.inventory.getTblLinkName())
        #         tblData = DataTable(self.inventory.getTblDataName())
        #         invent_activities_names = [k for k, v in self.getInventoryActivityDict().items() if v]
        #         invent_activities_ids = [v for k, v in self.getActivitiesDict().items() if k in invent_activities_names]
        #         tbl = self.inventory._getRecordLinkTables(tblRA, tblLink, tblData,
        #                                                   acttivity_list=str(tuple(invent_activities_ids)),
        #                                                   outer_obj_id=str(tuple(duplicates_list)))
        #         col_uid_date = tbl.getCols('fldObjectUID', 'fldDate')
        #         col_combined = list(zip(col_uid_date[0], col_uid_date[1]))      # [(uid, fldDate), ]
        #         inventories_dict = {}           # {uid: (Inventory Count, Latest Inventory/max(fldID)), }
        #         for uid in duplicates_list:
        #             inventories_dict[uid] = (col_uid_date[0].count(uid), max([item[1] for item in col_combined if
        #                                                                       item[0] == uid]))
        #         if isinstance(tbl, str):
        #             krnl_logger.error(f'ERR_DBAccess: Database read error. {tbl}')
        #             return self.ID

                # for each uid in duplicates_list must pull the list of inventory records associated to the uid.

        return self.ID


    @classmethod
    def tblObjName(cls):
        return cls.__tblObjectsName

    @classmethod
    def tblRAName(cls):
        return cls.__tblRAName

    @classmethod
    def tblLinkName(cls):
        return cls.__tblLinkName

    @property
    def activities(self):
        return self.__activitiesDict

    @classmethod
    def getActivitiesDict(cls):
        return cls.__activitiesDict

    @staticmethod
    def getInventoryActivity():
        return Tag.__isInventoryActivity

    @property
    def tblDataInventoryName(self):
        return self.__tblDataInventoryName

    @classmethod
    def getTblInventoryName(cls):
        return cls.__tblDataInventoryName


    @property
    def tblDataStatusName(self):
        return self.__tblDataStatusName

    @property
    def tblObjectsName(self):
        return self.__tblObjectsName

    # Diccionario de Tag.Status
    tempdf = getrecords('tblCaravanasStatus', 'fldID', 'fldName', 'fldFlag')
    __tagStatusDict = {}        # {statusName: [statusID, activeYN]}
    for i in tempdf.index:
        __tagStatusDict[tempdf.loc[i, 'fldName']] = (tempdf.loc[i, 'fldID'], tempdf.loc[i, 'fldFlag'])
    # for j in range(temp.dataLen):
    #     __tagStatusDict[str(temp.dataList[j][1])] = [int(temp.dataList[j][0]), int(temp.dataList[j][2])]

    @property
    def statusDict(self):
        return self.__tagStatusDict

    @classmethod
    def getStatusDict(cls):
        return cls.__tagStatusDict

    tagElementsList = ('fldID', 'fldTagNumber', 'fldFK_TagTechnology', 'fldTagMarkQuantity', 'fldFK_IDItem',
                       'fldFK_Color', 'fldFK_TagType', 'fldFK_TagFormat' 'fldImage', 'fldDateExit',
                       'fldFK_UserID', 'fldTimeStamp', 'fldComment')


    def __init__(self, *args, **kwargs):
        myID = kwargs.get('fldObjectUID')           # Reads UUID value formatted as str.
        if not isinstance(myID, UUID):
            try:
                myID = UUID(myID)
            except(ValueError, TypeError, AttributeError):
                # self.isValid = False
                # self.isActive = False
                raise TypeError(f'ERR_INP_Invalid / malformed UID {kwargs.get("fldObjectUID")}. Object not created!')

        myID = myID.hex      # All uids are processed as strings. Converted to UUID only when needed.
        n = removeAccents(kwargs.get('fldTagNumber', None))
        if not n or not isinstance(n, str):
            # self.isValid = False
            # self.isActive = False
            raise TypeError(f'ERR_INP_Invalid / malformed tag numb`er {n}. Object not created!')

        self.__tagNumber = n
        isValid = True
        self.__recordID = kwargs.get('fldID', None)
        # TODO(cmt): OJO! TAGS case-INSENSITIVE con este setup. Se eliminan acentos, dieresis y se pasa a lowercase

        self.__identifiers = kwargs.get('fldIdentificadores')           # str: Tags have only 1 identifier.
        self.__tagTechnology = kwargs.get('fldFK_TagTechnology')    # Standard, rfid, lora, tatoo, bluetooth, etc.
        self.__tagMarkQuantity = kwargs.get('fldTagMarkQuantity')
        self.__idItem = kwargs.get('fldFK_IDItem')
        self.__tagColor = kwargs.get('fldFK_Color')
        self.__tagType = kwargs.get('fldFK_TagType')
        self.__tagFormat = kwargs.get('fldFK_TagFormat')
        self.__tagImage = kwargs.get('fldImage')
        self.__myProgActivities = {}        #  {paObj: activityID, }
        self.__timeStamp = kwargs.get('fldTimeStamp', time_mt('dt'))  # Fecha usada para gestionar objetos repetidos.
        self.__exitYN = kwargs.get('fldDateExit', None)
        if self.__exitYN:
            self.__exitYN = valiDate(self.__exitYN, 1)  # datetime object o 1 (Salida sin fecha)
            isActive = False
        else:
            self.__exitYN = 0
            isActive = True

        self.__tagComment = kwargs.get('fldComment')   # This line for completeness only.
        self.__tagUserID = kwargs.get('fldFK_UserID')
        # Clase de objeto a la que se asigna, para evitar asignacion multiple
        self.__assignedToClass = next((kwargs[j] for j in kwargs if 'assignedtocla' in j.lower()), None)
        super().__init__(myID, isValid, isActive, *args, **kwargs)

    @property
    def tagNumber(self):
        return self.__tagNumber if self.isValid else None

    @tagNumber.setter
    def tagNumber(self, val):
        self.__tagNumber = val

    @property
    def tagTech(self):
        return self.__tagTechnology


    def getIdentifiers(self):
        """Returns Tag identifier (str). Made as tagNumber-tagTechnologyAnimalClass. ex: 8330-1Bovine"""
        return self.__identifiers       # Returns a list of identifiers

    def setIdentifiers(self, val):
        """Sets Tag identifier (str). Made as tagNumber-tagTechnologyAnimalClass. ex: 8330-1Bovine"""
        self.__identifiers = val      # Returns a list of identifiers


    @property
    def getElements(self):               # Diccionario armado durante inicializacion. Luego, NO SE ACTUALIZA. OJO!!
        return {
                'fldObjectUID': self.ID, 'fldTagNumber': self.__tagNumber, 'fldTagMarkQuantity': self.__tagMarkQuantity,
                'fldFK_IDItem': self.__idItem, 'fldFK_TagType': self.__tagType,  'fldDateExit': self.__exitYN,
                'fldIdentificadores': self.__identifiers, 'fldID': self.__recordID,  'fldFK_Color': self.__tagColor,
                'fldAssignedToClass': self.assignedToClass, 'fldFK_TagFormat': self.__tagFormat,
                'fldTimeStamp': self.__timeStamp, 'fldAssignedToUID': self.ID, 'fldImage': self.__tagImage,
                'fldComment': self.__tagComment, 'fldFK_UserID': self.__tagUserID,
                }

    @property
    def recordID(self):
        return self.__recordID

    @recordID.setter
    def recordID(self, val):
        self.__recordID = val

    def updateAttributes(self, **kwargs):
        """ Updates object attributes with values passed in attr_dict. Values not passed leave that attribute unchanged.
        @return: None
        """
        if not kwargs:
            return None

    @classmethod
    def getObject(cls, obj_id: str = None, *, assigned_to_class=None, fetch_from_db=False, **kwargs):
        """ Returns the Tag object associated to obj_id.

        @param fetch_from_db: Ignores memory data (_active_uids_df) and reads data from db.
        @param obj_id: can be a UUID or a regular human-readable string (Tag Number for Animals).
        @param assigned_to_class: (str). Class name of class to which tag is assigned (Bovine, Caprine, etc).
        @param kwargs: Uses fldObjectUID when dict is passed.
        Tag numbers are normalized (removal of accents, dieresis, special characters, lower()) before processing.
        @return: cls Object or None if no object of class cls is found for obj_id passed.
        """
        # First picks for values must be from kwargs to prioritize args passed via dict. unpacking.
        if obj_id:
            try:
                obj_id = UUID(obj_id.strip()).hex
            except SyntaxError:     # SyntaxError: obj_id non compliant with UUID format. May be a string.
                if isinstance(obj_id, str):
                    name = re.sub(r'[\\|/@#$%^*()=+¿?{}"\'<>,:;_-]', ' ', obj_id)  # OJO:'-' is NOT a tag name separator
                    name_words = [j for j in removeAccents(name).split(" ") if j]
                    name_words = [j.replace(" ", "") for j in name_words]
                    name = "".join(name_words)
                    # tagTech = kwargs.get('fldFK_TagTechnology', 1) or 1         # Not used for identifier for now.
                    assignedToClass = assigned_to_class or ""
                    identifier = name + cls._tagIdentifierChar + assignedToClass  # identifier: "number-assignedToClass"

                    """ Looks up uid via its identifier in identifiers_dict. 
                        IMPORTANT: Returns 1st match, which may not be the original object uid if duplicates exist. """
                    for df in cls.obj_dataframe():
                        try:
                            obj_id = df.loc[df['fldIdentificadores'].isin([identifier]), 'fldObjectUID'].iloc[0]
                        except IndexError:
                            continue
                        else:
                            if obj_id:
                                break
                else:
                    return None
            except (ValueError, TypeError, AttributeError):
                return None

            # Down here, there's a uid. Now MUST CHECK for duplication of the object's record (using _Duplication_Index)
            # Gets a list with just 1 item: the uid for the Earliest Duplicate Record.
            # The func. below with no other arguments gets the Original uid if duplicates exist, None if no duplicates.
            dupl_uids = cls._get_duplicate_uids(obj_id, all_duplicates=True) or (obj_id,)
            uid_nor = cls._get_duplicate_uids(obj_id) or obj_id  # NOR: Node Original Record (same as EDR).
            if not fetch_from_db:    # Fetches data from memory (obj_dataframe() iterator)
                # TODO IMPORTANT: df must contain all duplicate records for uid_nor in order to run checksum.
                df = None
                for frame in cls.obj_dataframe():
                    aux = frame[frame['fldObjectUID'].isin(dupl_uids)]
                    if not aux.empty:
                        if df is None:  # All this coding to retain the db Accessor values for resulting df.
                            df = pd.DataFrame.db.create(frame.db.tbl_name, data=aux.to_dict())
                        else:
                            df.append(aux, ignore_index=True)  # uses append to retain db Accessor settings in df.
                if df is None:
                    fetch_from_db = True  # No data in memory, forces retrieval from db.

            if fetch_from_db:
                # Fetches data from db.
                if dupl_uids != (obj_id,):
                    # There are duplicates: pulls data using fld_Duplication_Index.
                    sql = f'SELECT * FROM "{cls.__tblObjDBName}" WHERE ' \
                          f'"{getFldName(cls.tblObjName(), "fld_Duplication_Index")}" IN ' \
                          f'{str(dupl_uids) if len(dupl_uids) > 1 else str(dupl_uids).replace(",", "")}; '
                else:
                    # No duplicates: fld_Duplication_Index may be empty. Pulls data using fldObjectUID.
                    sql = f'SELECT * FROM "{cls.__tblObjDBName}" WHERE ' \
                          f'"{getFldName(cls.tblObjName(), "fldObjectUID")}" IN ' \
                          f'{str(dupl_uids) if len(dupl_uids) > 1 else str(dupl_uids).replace(",", "")}; '
                df = pd.read_sql_query(sql, SQLiteQuery().conn)  # df contains all duplicate records (if any) or just 1.

                if not df.empty:
                    # Get checksum from Node Original Record (also called EDR) and compare to data just read into df.
                    # All this logic to update the memory dataframe with any new data appearing on tblOjbect.
                    # TODO(cmt): NOR is the only record in the table with fldObjectUID = _Duplication_Index (by design)
                    #  NOR: Node Original Record
                    # nor_idx = df[df['fld_Duplication_Index'] == uid_nor].index[df['fld_Duplication_Index'] ==
                    #                                                             df['fldObjectUID']].tolist()[0]
                    nor_idx = df[df['fldObjectUID'] == uid_nor].index[0]  # Works independent of fld_Duplication_Index
                    nor_checksum = df.loc[nor_idx, 'fld_Update_Checksum']
                    # Removes the checksum column from the checksum computation so that checksum is valid. Also sets all
                    # null values to None BEFORE computating checksum so that it's consistent with value read from db.
                    # fld_Update_Checksum val may be outdated when loaded from db. Run this code to update if needed.
                    df.fillna(np.nan).replace([np.nan], [None], inplace=True)
                    new_checksum = hashlib.sha256(df.drop('fld_Update_Checksum', axis=1).to_json().encode()).hexdigest()
                    if nor_checksum != new_checksum:
                        # Values have changed for the duplicate records associated to animal_uid. Must reload from db.
                        # Updates row check sum and writes nor record to db (the only record modified here).
                        df.loc[nor_idx, 'fld_Update_Checksum'] = new_checksum
                        # Should update the correct db record regardless of nor_idx, as fldID remains unchanged .
                        _ = setRecord(cls.tblObjName(), **df.loc[nor_idx].to_dict())  # Writes to db if NOR row changed
                        # After setRecord(), must reload full data from db since _active_uids_df is an iterator and
                        # cannot be updated on a single-record basis.
                        cls._init_uid_dicts()

                    tag_data_dict = df.loc[nor_idx].to_dict()
                    tagObjClass = cls._tagObjectsClasses.get(tag_data_dict['fldFK_TagTechnology'], None)
                    if tagObjClass is not None:
                        return tagObjClass(**tag_data_dict)  # Original Object uid found.

        # Returning None means no duplicates for the uid (99.9% of the cases for each object table, hopefully)
        return None



    # @classmethod
    # def getObject00(cls, obj_id: str = None, **kwargs):
    #     """ Returns the Tag object associated to obj_id.
    #     @param obj_id: can be a UUID or a regular human-readable string (Tag Number for Animals).
    #     @param kwargs: Uses fldObjectUID when dict is passed.
    #     Tag numbers are normalized (removal of accents, dieresis, special characters, lower()) before processing.
    #     @return: cls Object or None if no object of class cls is found for obj_id passed.
    #     """
    #     # First picks for values must be from kwargs to prioritize args passed via dict. unpacking.
    #     obj_id = kwargs.get('fldObjectUID', obj_id) or obj_id
    #     if obj_id:
    #         try:
    #             obj_id = UUID(obj_id.strip()).hex
    #         except SyntaxError:  # SyntaxError: obj_id non compliant with UUID format. May be a string.
    #             if isinstance(obj_id, str):
    #                 name = re.sub(r'[\\|/@#$%^*()=+¿?{}"\'<>,:;_-]', ' ', obj_id)  # OJO:'-' is NOT a tag name separator
    #                 name_words = [j for j in removeAccents(name).split(" ") if j]
    #                 name_words = [j.replace(" ", "") for j in name_words]
    #                 name = "".join(name_words)
    #                 # tagTech = kwargs.get('fldFK_TagTechnology', 1) or 1         # Not used for identifier for now.
    #                 assignedToClass = kwargs.get('fldAssignedToClass', "") or ""
    #                 identifier = name + cls._tagIdentifierChar + assignedToClass  # identifier: "number-assignedToClass"
    #
    #                 """ Looks up uid via its identifier in identifiers_dict.
    #                     IMPORTANT: Returns 1st match, which may not be the original object uid if duplicates exist. """
    #                 for df in cls.obj_dataframe():
    #                     try:
    #                         obj_id = df.loc[df['fldIdentificadores'].isin([identifier]), 'fldObjectUID'][0]
    #                     except IndexError:
    #                         continue
    #             else:
    #                 return None
    #         except (ValueError, TypeError, AttributeError):
    #             return None
    #
    #         # Down here, there's a uid. Now MUST CHECK for duplication of the object's record (using _Duplication_Index)
    #         # Gets a list with just 1 item: the uid for the Earliest Duplicate Record.
    #         # The func. below with no other arguments gets the Original uid if duplicates exist, None if no duplicates.
    #         uid_orig = cls._get_duplicate_uids(obj_id) or obj_id  # Defined in EntityObject.
    #         # Now, pull the record index corresponding to uid from tbl. This step is crucial: Returns the original uid.
    #         # sql to look up record(s) by UID.
    #         sql = f'SELECT * from "{cls.__tblObjDBName}" WHERE {getFldName(cls.tblObjName(), "fldObjectUID")} == ' \
    #               f'"{str(uid_orig)}"; '
    #         auxdf = pd.read_sql_query(sql, SQLiteQuery().conn)
    #         tag_data_dict = auxdf.iloc[0].to_dict()
    #         # Pulls the Tag Class belonging to the object's tag technology, stored in cls._tagObjectsClasses dictionary.
    #         # (cls._tagObjectsClasses is populated during initialization by TagAnimal._create_subclass() factory func.)
    #         tagObjClass = cls._tagObjectsClasses.get(tag_data_dict['fldFK_TagTechnology'], None)
    #         if tagObjClass is not None:
    #             return tagObjClass(**tag_data_dict)  # Original Object uid found. cls object returned.
    #
    #     return None


    @classmethod
    def tag_by_number(cls, tag_num: str = None, *, assigned_to_class=None, use_identifier=False):
        """ Returns the database row for
        @param use_identifier: creates a Tag identifer (number-AnimalClass) and searches by fldIdentificadores field.
        @param tag_num: can be a UUID or a regular human-readable string (Tag Number for Animals).
        @param assigned_to_class: (str). Class name of class to which tag is assigned (Bovine, Caprine, etc).
        Tag numbers are normalized (removal of accents, dieresis, special characters, lower()) before processing.
        @return: cls Object or None if no object of class cls is found in database.
        """
        # First picks for values must be from kwargs to prioritize args passed via dict. unpacking.
        if tag_num:
            try:
                tag_num = UUID(tag_num.strip()).hex
            except (SyntaxError, ValueError):  # SyntaxError: obj_id non compliant with UUID format. May be a string.
                if isinstance(tag_num, str):
                    name = re.sub(r'[\\|/@#$%^*()=+¿?{}"\'<>,:;_-]', ' ', tag_num)  # OJO:'-' is NOT tag name separator
                    name_words = [j for j in removeAccents(name).split(" ") if j]
                    name_words = [j.replace(" ", "") for j in name_words]
                    name = "".join(name_words)
                    assignedToClass = assigned_to_class or ""
                    if use_identifier:
                        identifier = name + cls._tagIdentifierChar + assignedToClass  # identifier: "number-assignedToClass"
                        sql = f'SELECT * FROM "{cls.__tblObjDBName}" WHERE ' \
                              f'"{getFldName(cls.tblObjName(), "fldIdentificadores")}" == "{identifier}"; '

                    else:
                        sql = f'SELECT * FROM "{cls.__tblObjDBName}" WHERE ' \
                              f'"{getFldName(cls.tblObjName(), "fldTagNumber")}" == "{name}"; '
                else:
                    return None
            else:
                sql = f'SELECT * FROM "{cls.__tblObjDBName}" WHERE ' \
                      f'LOWER("{getFldName(cls.tblObjName(), "fldObjectUID")}") == "{tag_num}"; '

            return pd.read_sql_query(sql, SQLiteQuery().conn)  # df contains


    # Mimics data in "Data Caravanas Inventario" table. Used to test code in _get_duplicate_uids()
    # testdict = {'fldFK_Actividad': {0: 4, 1: 4, 2: 3, 3: 5, 4: 12},
    #             'fldObjectUID': {0: ('uid2',), 1: ('uid1', 'uid2'), 2: ('uid1', 'uid3'),
    #                              3: ('uid1', 'uid2', 'uid3'), 4: ('uid1', 'uid2', 'uid3')},
    #             'fldDate': {0: "2018-05-20 00:00:00", 1: "2018-09-20 00:00:00", 2: "2022-09-01 00:00:00",
    #                         3: "2020-10-10 00:00:00", 4: "2018-09-01 00:00:00"}}

    @classmethod  # Overrides func. in EntityObject to resolve the particulars of Tag duplication and tag input errors.
    def _get_duplicate_uids(cls, uid: UUID | str = None, *, all_duplicates=False) -> UUID | tuple | None:
        """For a given uid, returns the Earliest Duplicate Record (the Original) if only 1 record exists or a tuple of
        duplicate records associated to the uid.
        For reference, the dictionaries are defined as follows:
                _active_uids_df = {uid: _Duplication_Index, }   --> _Duplication_Index IS a uid.
                _active_duplication_index_dict = {_Duplication_Index: [fldObjectUID, dupl_uid1, dupl_uid2, ], }
        @param all_duplicates: True: returns all uids linked by a Duplication_Index value.
                False: returns Earliest Duplicate uid. (Default).
        @return: Original uid (UUID) or list of duplicate uids (tuple). None no duplicates exist for uid.

         _Duplication_Index is set by SQLite. Flags db records created by different nodes that refer
         to the same physical object. Duplicates are resolved between SQLite (triggers) and this function, by picking
         min(fldTimeStamp) (record 1st created) in all cases.
        """
        if uid:
            try:
                uid = uid.hex if isinstance(uid, UUID) else UUID(uid.strip()).hex  # converts to str.
            except (SyntaxError, AttributeError, TypeError):
                return None
            # Pulls the right _active_uids_df for cls.
            duplication_index = None
            for df in cls.obj_dataframe():  # Careful, this is an iterator! Exhausted after 1st use.
                aux_series = duplication_index = df.loc[df['fldObjectUID'] == uid, 'fld_Duplication_Index']
                if len(aux_series.index) > 0:
                    duplication_index = aux_series.iloc[0]
                    break

            if pd.notnull(duplication_index):
                if all_duplicates:
                    return tuple(cls.obj_dupl_series()[duplication_index])
                # return duplication_index  # Returns single value (a UUID or None)

                duplicates_list = tuple(cls.obj_dupl_series()[duplication_index])
                duplicates_list_str = str(duplicates_list) if len(duplicates_list) > 1 else \
                    str(duplicates_list).replace(',', '')
                if duplicates_list:  # Most probably this if is not needed but...
                    # 1. Checks if PHATT is set for any of the uids associated to _Duplication_Index
                    # This path of the code is expected to execute the most often as the usual sequence for tag entry is
                    # to commission it when it's attached to an Animal, and from that point on PHATT field is set to 1.
                    sqlPHATT = f'SELECT "fldPhysicallyAttached" from {cls.tblObjDBName()} WHERE "UID_Objeto" ' \
                               f'IN {str(duplicates_list_str)}; '
                    con = SQLiteQuery().conn
                    dfPHATT = pd.read_sql_query(sqlPHATT, con)
                    if len(dfPHATT.fldPhysicallyAttached) > 0:
                        return duplication_index

                    # 2.PHATT field is not set for _Duplication_Index -> gets Inv. data from tblDataCaravanasInventario.
                    tblInvName = cls.getTblInventoryName()
                    sql = f'SELECT * FROM "{getTblName(tblInvName)}";'  # Avoids using tblInventario."ObjectUID" for now
                    dfInventory = pd.read_sql_query(sql, con)
                    if not len(dfInventory.index):
                        krnl_logger.warning(f'ERR_DBAccess: cannot read from {tblInvName}.')
                        return duplication_index

                    #        TODO(cmt): Code below ested and re-tested with testdict. Works fine (03Aug24).
                    """All the yara-yara below is to avoid loading up Link tables into memory, which will be large. """
                    # Tag.inventory.set() MUST update fldObjectUID in tblDataCaravanasInventario with the uid from the
                    # inventory operation. fldObjectUID is a JSON list. -> Implemented via INSERT TRIGGERS, hehehe!
                    #  The code below traverses each JSON list searching for uids that are present in duplicates_list
                    # and counts the number of  records for each uid. Picks the uid with the highest number of inventory
                    # operations. In case more than 1 uid have an equal max number, picks the uid with the earliest
                    # fldDate operation.
                    df_exploded = dfInventory.explode('fldObjectUID').reset_index(drop=True)
                    duplicates_df = df_exploded[df_exploded['fldObjectUID'].isin(duplicates_list)]

                    # reset_index() or as_index=False allows for grouping column to remain a column in the created df.
                    inventory_counts = duplicates_df.groupby(['fldObjectUID'], as_index=False)['fldDate'].agg(set)
                    max_len = inventory_counts.fldDate.apply(len).max()
                    max_count_uids = inventory_counts.loc[inventory_counts.fldDate.apply(len) == max_len]
                    if len(max_count_uids.index) > 1:
                        # Several duplicate uids with same max: picks the earliest fldDate (original UID by definition)
                        # Goes through all this hassle only for 2 or more duplicate uids with equal Inventory count.
                        max_count_uids = inventory_counts.loc[inventory_counts.fldDate.str.len() == max_len].\
                            explode('fldDate', ignore_index=True)       # Another way to explode() and ignore index.
                        duplication_index = max_count_uids.loc[max_count_uids.fldDate ==
                                                               max_count_uids.fldDate.min(), 'fldObjectUID'].iloc[0]
                    else:
                        # Normal (usual) case: just 1 uid with the highest count of Inventory records.
                        duplication_index = max_count_uids.iloc[0, max_count_uids.columns.get_loc('fldObjectUID')]
                return duplication_index  # Returns single value (a UUID or None)

        # In the 99.9% chance there are no duplicates, the function just exists and the original uid is to be used
        return None



    # @classmethod  # Overrides func. in EntityObject to resolve the particulars of Tag duplication and tag input errors.
    # def _get_duplicate_uids01(cls, uid: UUID | str = None, *, all_duplicates=False) -> UUID | tuple | None:
    #     """For a given uid, returns the Earliest Duplicate Record (the Original) if only 1 record exists or the record
    #     with the highest count of Tag Inventory Activities. When 2 or more records share the same max. count, returns
    #     the Earliest Duplicate Record among them.
    #     When fldPhysicallyAttached is set in Caravanas record, skips testing for Inventory counts and returns
    #     _Duplication_Index associated to that record.
    #     For reference, the dictionaries are defined as follows:
    #             _active_uids_df = {uid: _Duplication_Index, }   --> _Duplication_Index IS a uid.
    #             _active_duplication_index_dict = {_Duplication_Index: [fldObjectUID, dupl_uid1, dupl_uid2, ], }
    #     @param all_duplicates: True: returns all uids linked by a Duplication_Index value.
    #             False: returns Earliest Duplicate uid. (Default).
    #     @return: Original uid (UUID) or list of duplicate uids (tuple). None no duplicates exist for uid.
    #
    #      _Duplication_Index is set by SQLite. Flags db records created by different nodes that refer
    #      to the same physical object. Duplicates are resolved between SQLite (triggers) and this function, by picking
    #      min(fldTimeStamp) (record 1st created) in all cases.
    #     """
    #     if uid:
    #         try:
    #             uid = uid.hex if isinstance(uid, UUID) else UUID(uid.strip()).hex  # converts to str.
    #         except (SyntaxError, AttributeError, TypeError):
    #             return None
    #         # Pulls the right _active_uids_df for cls.
    #         duplication_index = getattr(cls, '_active_uids_df', {}).get(uid, None)
    #         if duplication_index is not None:
    #             dupl_index_dict = getattr(cls, '_active_duplication_index_dict', {})
    #             if all_duplicates:
    #                 return tuple(dupl_index_dict.get(duplication_index, []))  # returns full list of duplicate uids.
    #
    #             duplicates_list = tuple(dupl_index_dict.get(duplication_index, []))
    #             if duplicates_list:  # Most probably this if is not needed but...
    #                 # 1. Checks if PHATT is set for any of the uids associated to _Duplication_Index
    #                 # This path of the code is expected to execute the most often as the usual sequence for tag entry is
    #                 # to commission it when it's attached to an Animal, and from that point on PHATT field is set to 1.
    #                 sqlPHATT = f'SELECT "fldPhysicallyAttached" from {cls.tblObjDBName()} WHERE "UID_Objeto" ' \
    #                            f'IN {str(duplicates_list)}; '
    #                 tbl = dbRead(cls.tblObjName(), sqlPHATT)
    #                 if isinstance(tbl, DataTable) and tbl:
    #                     if any(j > 0 for j in tbl.getCol("fldPhysicallyAttached")):
    #                         return duplication_index
    #
    #                 # 2.PHATT field is not set for _Duplication_Index -> gets Inv. data from tblDataCaravanasInventario.
    #                 tblInvName = cls.getTblInventoryName()
    #                 # sqlO = f'SELECT * FROM "{getTblName(tblInvName)}" WHERE "{getFldName(tblInvName, "ObjectUID")}" '\
    #                 #       f'IN {str(duplicates_list)}; '
    #
    #                 # Tag.inventory.set() MUST update fldObjectUID in tblDataCaravanasInventario with the uid from the
    #                 # inventory operation. fldObjectUID is a JSON list. -> Implemented via INSERT TRIGGERS, hehehe!
    #                 #  The code below traverses each JSON list searching for uids that are present in duplicates_list.
    #                 sql = f'SELECT * FROM "{getTblName(tblInvName)}";'  # Avoids using tblInventario."ObjectUID" for now
    #                 tblInventory = dbRead(tblInvName, sql)
    #                 if isinstance(tblInventory, str):
    #                     krnl_logger.warning(f'ERR_DBAccess: cannot read from {tblInvName}. Error: {tblInventory}.')
    #                     return duplication_index
    #
    #                 #                TODO: **** TEST THIS LOGIC WITH DATA, MAKE SURE IT WORKS.  ****
    #                 # Resolves uid with the highest count of Inventory activities performed.
    #                 idx_uids = tblInventory.getFldIndex('fldObjectUID')
    #                 # Dict with DataTable records carrying Inventory data (a list of Inventory records) for each uid.
    #                 records_dict = {uid: [r for r in tblInventory.dataList if uid in r[idx_uids]]
    #                                 for uid in duplicates_list}
    #                 inventory_counts_dict = {k: len(v) for k, v in records_dict.items()}  # len(v) = inventory count.
    #                 max_inventory_count = max(inventory_counts_dict.values())
    #                 # Now find Earliest Duplicate Record for the max_count_uids using min(fldDate).
    #                 max_count_uids = [k for k, v in inventory_counts_dict.items() if
    #                                   v == max_inventory_count]  # 1 or more uid with same count.
    #                 if len(max_count_uids) > 1:
    #                     # Goes through all this hassle only for 2 or more duplicate uids with equal Inventory count.
    #                     idx_date = tblInventory.getFldIndex('fldDate')
    #                     # value in mins_dict is fldDate value, to pick its minimum.
    #                     mins_dict = {uid: min(records_dict[uid], key=lambda r: r[idx_date])[idx_date]
    #                                  for uid in max_count_uids}
    #                     duplication_index = next((k for k, v in mins_dict if v == min(mins_dict.values())), None)
    #                 else:
    #                     # Normal (usual) case: just 1 uid with the highest count of Inventory records.
    #                     duplication_index = max_count_uids[0]
    #             return duplication_index  # Returns single value (a UUID or None)
    #
    #         # In the 99.9% chance there are no duplicates, the function just exists and the original uid is to be used
    #     return None

    # @classmethod  # Overrides func. in EntityObject to resolve the particulars of Tag duplicates.
    # def _get_duplicate_uids00(cls, uid: UUID | str = None, *, all_duplicates=False) -> UUID | tuple | None:
    #     """For a given uid, returns the Earliest Duplicate Record (the Original) if only 1 record exists or the record
    #     with the highest count of Tag Inventory Activities. When 2 or more records share the same max. count, returns
    #     the Earliest Duplicate Record among them.
    #     For reference, the dictionaries are defined as follows:
    #             _active_uids_df = {uid: _Duplication_Index, }   --> _Duplication_Index IS a uid.
    #             _active_duplication_index_dict = {_Duplication_Index: [fldObjectUID, dupl_uid1, dupl_uid2, ], }
    #     @param all_duplicates: True: returns all uids linked by a Duplication_Index value.
    #             False: returns Earliest Duplicate uid. (Default).
    #     @return: Original uid (UUID) or list of duplicate uids (tuple). None no duplicates exist for uid.
    #
    #      _Duplication_Index is set by SQLite. Flags db records created by different nodes that refer
    #      to the same physical object. Duplicates are resolved between SQLite (triggers) and this function, by picking
    #      min(fldTimeStamp) (record 1st created) in all cases.
    #     """
    #     if uid:
    #         try:
    #             uid = uid.hex if isinstance(uid, UUID) else UUID(uid.strip()).hex  # converts to str.
    #         except (SyntaxError, AttributeError, TypeError):
    #             return None
    #         # Pulls the right _active_uids_df for cls.
    #         duplication_index = getattr(cls, '_active_uids_df', {}).get(uid, None)
    #         if duplication_index is not None:
    #             dupl_idx_dict = getattr(cls, '_' + cls.__name__ + '_active_duplication_index_dict', {})
    #             if all_duplicates:
    #                 return tuple(dupl_idx_dict.get(duplication_index, []))  # returns full list of duplicate uids.
    #
    #             duplicates_list = tuple(dupl_idx_dict.get(duplication_index, []))
    #             if duplicates_list:  # Most probably this if is not needed but...
    #                 # Reads Inventory data from tblDataCaravanasInventario
    #                 tblInvName = cls.getTblInventoryName()
    #                 # sqlO = f'SELECT * FROM "{getTblName(tblInvName)}" WHERE "{getFldName(tblInvName, "fldFK_ObjectUID")}" '\
    #                 #       f'IN {str(duplicates_list)}; '
    #
    #                 sql = f'SELECT * FROM "{getTblName(tblInvName)}"; '
    #                 tblInventory = dbRead(tblInvName, sql)
    #                 if isinstance(tblInventory, str):
    #                     krnl_logger.warning(f'ERR_DBAccess: cannot read from {tblInvName}. Error: {tblInventory}.')
    #                     return duplication_index
    #
    #                 # Pulls uid column (JSON) with lists of uids for each record.
    #                 col_uids = tblInventory.getCol('fldObjectUID')  # Each item is a list of 1 or more uids.
    #                 # Resolves uid with the highest count of Inventory activities performed.
    #                 idx_uids = tblInventory.getFldIndex('fldObjectUID')
    #                 # Dict with DataTable records with Inventory data (a list of Inventory records) for each uid.
    #                 records_dict = {uid: [r for r in tblInventory.dataList if uid in r[idx_uids]]
    #                                 for uid in duplicates_list}
    #                 inventory_counts_dict = {k: len(v) for k, v in records_dict.items()}  # len(v) = inventory count.
    #                 max_inventory_count = max(inventory_counts_dict.values())
    #
    #                 # Now find Earliest Duplicate Record for the max_count_uids using min(fldDate).
    #                 max_count_uids = [k for k, v in inventory_counts_dict.items() if
    #                                   v == max_inventory_count]  # 1 or more uid with same count.
    #                 if len(max_count_uids) > 1:
    #                     # Goes through all this hassle only for 2 or more duplicate uids with equal Inventory count.
    #                     #                TODO: **** TEST THIS LOGIC WITH DATA, MAKE SURE IT WORKS.  ****
    #                     idx_date = tblInventory.getFldIndex('fldDate')
    #                     # value in mins_dict is fldDate value, to pick its minimum.
    #                     mins_dict = {uid: min(records_dict[uid], key=lambda r: r[idx_date])[idx_date]
    #                                  for uid in max_count_uids}
    #                     duplication_index = next((k for k, v in mins_dict if v == min(mins_dict.values())), None)
    #                     # cols_uid_date = tblInventory.getCols('fldObjectUID', 'fldDate')     # TODO: FIX THIS PART!
    #                     # cols_itemized = list(zip(cols_uid_date[0], cols_uid_date[1]))  # [(uid,fldDate), (uid,fldDate),]
    #                     # uids_min_dates = {dupl_uid: min([j[1] for j in cols_itemized if j[0] == dupl_uid])
    #                     #                   for dupl_uid in max_count_uids}
    #                     # duplication_index = next((k for k, v in uids_min_dates.items()
    #                     #                          if v == min(uids_min_dates.values())), duplication_index)
    #                 else:
    #                     duplication_index = max_count_uids[0]  # just 1 uid with highest count of Inventory records.
    #             return duplication_index  # Returns single value (a UUID or None)
    #
    #         # In the 99.9% chance there are no duplicates, the function just exists and the original uid is to be used.
    #     return None


    @classmethod
    def alta(cls, *args, **kwargs):         # cls is TagBovine, TagCaprine, etc.
        """ Interface to run class methods passing cls as outerObject parameter, instead of self.
           Executes the Alta Operation for Tags. Can perform batch altaMultiple() for multiple idRecord.
           kwargs enclosed in a list for each Tag to map()
           @param args: DataTable objects with obj_data to insert in DB tables, as part of the Alta operation
           @param kwargs: dictionary with all attributes to set for new tag.
           @return: Tag Object (TagBovine, TagCaprine, TagOvine, etc) or errorCode (str)
           """
        if cls is Tag:
            raise TypeError(f'ERR_SYS: Cannot create instance for Tag abstract class. Use the proper Tag sublass.')
        # alta_classmethod is an attribute (classmethod) of the singleton instance of AltaActivityTag (callable class).
        return cls.alta_classmethod(*args, **kwargs)  # alta() ->executes __call__(*args, **kwargs) in AltaActivityTag.


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

    # 1:AltaActivity; 3:Decomisionada. 2 unicos status que hacen tag "available".
    __availableStatus = (0, 1, 3, None)
    @property
    def isAvailable(self):
        """
        Returns available condition of a Tag from Data Caravanas Status table if status is "AltaActivity" (1) or
        "Decomisionada" (3)
        @return: True: Available / False: Note Available /None: Error->No se pudo leer status de DB.
        """
        retValue = self.status.get()             # Devuelve ULTIMO status (Ultimo por fldDate)
        return retValue in self.__availableStatus or self.__assignedToClass is None

    @classmethod
    def get_tags_in_use(cls):
        """ Returns tuple of tags not available for use (In use by some object of classes Person, Device, Bovine, etc.).
        @return: list of uids of tags Not Available. Empty list if no tags taken found.
        """
        dicto = cls.status_classmethod().get_mem_dataframes()
        tags_taken = ()
        if isinstance(dicto, dict):
            tags_taken_df = None
            for frame in dicto.values():
                df = frame[~(frame['fldFK_Status'].isin(cls.__availableStatus) | frame['fldFK_Status'].isnull())]
                if tags_taken_df is None:
                    tags_taken_df = df
                else:
                    if not df.empty:
                        tags_taken_df = pd.concat((tags_taken_df, df), ignore_index=True, copy=False, sort=True)
            if tags_taken_df is not None and not tags_taken_df.empty:
                tags_taken = tags_taken_df.fldFK.to_list()       # index in df holds the uid values.
        return tuple(tags_taken)

    @property
    def exitYN(self):
        return self.__exitYN

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


    def validateActivity(self, activityName: str):
        """
        Checks whether __activityName is defined for the class of Animal of the caller object
        @param activityName: str. Activity Name to check.
        @return: True: Activity defined for Animal - False: Activity is not defined.
        """
        activityName = activityName.strip()
        retValue = True if activityName in self.__activitiesForMyClass else False  # = _activitiesDict
        return retValue


    @classmethod
    def generateTag(cls, *, tag_number: str = None, localization=None, **kwargs):
        """
        Generates a new tag.
        @param localization: str. Localization assigned to the tag.
        @param tag_number: str. Human-readable number stamped on the tag.
        @param kwargs: tagnumber-> Tag Number. if not provided, a unique tag number is generated.
                       technology -> Standard, RFID, etc.
                       marks -> Number of marks (Default = 1)
                       tagType, tagColor, tagFormat
                       tagStatusDict -> Tag Status. Default=1 (Alta)
                       writeDB=True -> writes all required obj_data to DB tables (Caravanas, Caravanas Status, etc)
        @return: Tag Object
        """
        tag_uid = uuid4().hex
        tagNumber = tag_number if isinstance(tag_number, str) else None
        if tagNumber is None:
            tagNumber = next((v for k, v in kwargs.items() if 'tagnum' in k.lower()), 'RND-' + tag_uid)

        technology = next((v for k, v in kwargs.items() if 'tech' in k.lower()), 'standard')
        marks = next((v for k, v in kwargs.items() if 'mark' in k.lower()), 1)
        tagColor = next((v for k, v in kwargs.items() if 'color' in k.lower()), 'blanco')
        tagType = next((v for k, v in kwargs.items() if 'tagtype' in k.lower()), 'general')
        tagFormat = next((v for k, v in kwargs.items() if 'tagformat' in k.lower()), 'tarjeta')
        tagStatus = next((v for k, v in kwargs.items() if 'status' in k.lower()), 'Alta')
        tagStatus = tagStatus if tagStatus in cls.getStatusDict() else 'Alta'

        new_tag = cls.alta(fldID=0, fldTagNumber=str(tagNumber), fldFK_TagTechnology=technology, fldObjectUID=tag_uid,
                           fldFK_Color=tagColor, fldFK_TagType=tagType, fldTagMarkQuantity=marks,
                           fldFK_TagFormat=tagFormat, fldFK_UserID=sessionActiveUser, fldAssignedToClass='Bovine',
                           fldAssignedToUID=tag_uid)

        print(f'TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT Caravana Nueva: {type(new_tag)} / {new_tag.getElements} ')
        if localization:
            new_tag.localization.set(localization=localization)
        new_tag.status.set(status=tagStatus)
        return new_tag

    create_tag = generateTag