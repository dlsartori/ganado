from krnl_entityObject import EntityObject
import re
import pandas as pd
import itertools as it
import numpy as np
import hashlib
import sqlite3
import threading
from krnl_config import fDateTime, callerFunction, lineNum, removeAccents, krnl_logger
from datetime import datetime
from krnl_custom_types import getRecords, DataTable, setRecord, getrecords
from krnl_abstract_class_prog_activity import ProgActivity
from krnl_db_query import getFldName, getTblName, AccessSerializer, SQLiteQuery
from uuid import UUID
from krnl_tag_person import TagPerson

SYSTEM_PERSON_ID = 1


class Person(EntityObject):                                # Personas
    # __objClass = 30
    # __objType = 1

    __activitiesDict = {}         # {NombreActividad: ID_Actividad, }
    __activeProgActivities = []     # List of programmed activities active for Person objects. MUST BE a list.

    # _active_uids_dict = {}  # {fldObjectUID: fld_Duplication_Index}  --> fld_Duplication_Index IS an object UID.
    # _duplic_index_checksum = 0  # sum of _active_uids_df.values() to detect changes and update _active_uids_df.
    # _active_duplication_index_dict = {}  # {fld_Duplication_Index: (fldObjectUID, dupl_uid1, dupl_uid2, ), }

    __tblDataStatusName = 'tblDataPersonasStatus'
    __tblObjName = 'tblPersonas'
    __tblObjDBName = getTblName(__tblObjName)
    __tblRAName = 'tblPersonasRegistroDeActividades'
    __tblLinkName = 'tblLinkPersonasActividades'
    __tblDataLocalizationName = 'tblDataPersonasLocalizacion'
    __tblLinkPAName = 'tblLinkPersonasActividadesProgramadas'       # Usado en object_instantiation.loadItemsFromDB()

    @classmethod
    def tblObjDBName(cls):
        return cls.__tblObjDBName

    # TODO(cmt): dataframes and Series to manage Bovine object queries, handling Duplicates management behind the scenes
    #  and storing frequently accessed data in memory.
    _active_uids_df = {}  # DataFrame.  {fldObjectUID: fld_Duplication_Index}
    _object_mem_fields = ('*',)  # ('fldID', 'fldObjectUID', 'fldDOB', 'fld_Duplication_Index', 'fldDaysToTimeout',
    # 'fldMode','fldIdentificadores', 'fldMF')
    _access_serializer = AccessSerializer()  # Keeps track of # of accesses to the code in _init_uid_dicts().
    _sem_obj_dataframe = threading.BoundedSemaphore()  # Semaphore specific to _active_uids_df for each Object class.
    """ Important concurrency concept: If more than 1 thread is blocked by the wait on the AccessSerializer object
     (Impossible in this code), the execution order of the waiting threads is NOT guaranteed. It is handled by the OS
     scheduler and the threads will not in general be executed in the order they requested access to the shared
     resource. THIS ORDER IS RELEVANT because each call to _init_uid_dicts() may load different versions of the
     database based on the order they access the database.
     So the trick here is to ensure that in a situation of concurrency the last-read version of the database prevails.
     To achieve this, the read-from-db and write operations must be sequential and uninterrupted by other threads.
     This is achieved by the use of the AccessSerializer object in the function code. """
    # Duplication dataframe, for ease of access to the (hopefully very few) duplicates.
    _dupl_series = pd.Series([], dtype=object)  # Array of the form (_Duplication_Index, (fldObjUID1, fldObjUID2,))
    # _dupl_series.index.set_names('fld_Duplication_Index', inplace=True)
    _chunk_size = 300  # PANDAS_READ_CHUNK_SIZE   #todo: reinstate PANDAS_READ_CHUNK_SIZE. value specific to each class.

    @classmethod
    def obj_mem_fields(cls):
        return cls._object_mem_fields

    @classmethod
    def obj_dataframe(cls):
        """ @return: iterator with dataframes for active objects (Tags, Bovines, Caprines, etc.) """
        # tee() ain't thread safe and _active_uids_df may be accessed by mutiple threads, so run under a lock.
        with cls._sem_obj_dataframe:
            ittr = it.tee(cls._active_uids_df, 2)
            cls._active_uids_df = ittr[0]
            return ittr[1]  # OJO: _active_uids_df is an iterator.

    @classmethod
    def obj_dupl_series(cls):
        return cls._dupl_series

    @classmethod
    def _sql_uids(cls):
        """ This sql loads a fresh snapshot of all active objects from database.
            @return: str. SQLite sql string to execute inside _init_uid_dicts(). """
        if '*' in cls._object_mem_fields:
            mem_flds = '*'
        else:
            mem_flds = ", ".join(tuple([f'"{getFldName(cls.tblObjName(), j)}"' for j in
                                        cls._object_mem_fields])).replace('(', '').replace(')', '')
        return f'SELECT {mem_flds} from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" '\
               f'IS NULL OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0); '

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
        #  All subsequent calls to _init_uid_dicts() will go to wait() if the Semaphore in _access_serializer blocks them.
        #  The whole point of this approach is to minimize the code protected under the global lock.
        with cls._access_serializer:
            access_count = cls._access_serializer.access_count  # This call uses an internal stack. It's thread-safe.
            # Starts serializing access here, by detecting all concurrent calls to _init_uid_dicts() and icrementing
            # access_count for each instance of _init_uid_dicts that runs concurrently. The entry order is needed to
            # later identify the last one to access database. This will be the instance that updates _active_uids_df.
            # The 'if' below:
            #   access_count == cls._access_serializer.total_count -> Enters 'if' for LAST execution instance of func.
            #   access_count == 1 -> Enters 'if' for the FIRST execution instance of func. To be used where needed.
            with cls._sem_obj_dataframe:  # Acquires semaphore(n=1).
                if access_count == cls._access_serializer.total_count:
                    # TODO(cmt): This 'if' guarantees that the LAST thread to get here concurrently will do the db read.
                    #  All the block below, protected by a semaphore.
                    read_itr = pd.read_sql_query(cls._sql_uids(), SQLiteQuery().conn, chunksize=cls._chunk_size)

                    # IMPORTANT: tee() is NOT thread-safe but read_itr and ittr are local vars. They are local to each
                    # thread. With them being local, we can use blocking=False in _access_serializer above.
                    ittr = it.tee(read_itr, 3)  # 2 iterators for processing; ittr[2] is assigned to _active_uids_df.
                    for df in ittr[0]:  # Detects empty frames.
                        if not df.empty:
                            break
                        # Exits with error if 1st dataframe in iterator is empty.
                        val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts " \
                              f"not initialized. System cannot operate."
                        krnl_logger.warning(val)
                        raise sqlite3.DatabaseError(val)

                    # Sets up/updates a Series obj with duplicates for ease of access to duplicates.
                    dupseries = None
                    duplic_list = []
                    for df in ittr[1]:
                        if any(df['fld_Duplication_Index'].notnull()):
                            not_nulls = df.loc[df['fld_Duplication_Index'].notnull()]
                            temp_dupl = not_nulls.groupby('fld_Duplication_Index')['fldObjectUID'].agg(set)  # Series
                            duplic_list.append(temp_dupl)
                    if duplic_list:
                        dupseries = pd.concat(duplic_list, copy=False)

                    cls._active_uids_df = ittr[2]  # SHARED resource. Accessed from background threads.
                    if dupseries is not None:
                        try:
                            pd.testing.assert_series_equal(dupseries, cls._dupl_series)
                        except (AssertionError, TypeError, ValueError, KeyError, IndexError):
                            cls._dupl_series = dupseries.copy()  # SHARED resource. Accessed from background threads.
            # End of semaphore-protected section: releases lock to next the thread that may be waiting for semaphore
        # End of outer 'with' block: Here, __exit__() is called and internal access_counter is decremented.
        return



    @classmethod
    def tblObjName(cls):
        return cls.__tblObjName

    @classmethod
    def tblRAName(cls):
        return cls.__tblRAName

    @classmethod
    def tblLinkName(cls):
        return cls.__tblLinkName

    @property
    def tblDataStatusName(self):
        return self.__tblDataStatusName

    @classmethod
    def tblLinkPAName(cls):
        return cls.__tblLinkPAName


    def __init__(self, *args, **kwargs):
        myID = kwargs.get('fldObjectUID')
        if myID is None:
            isValid = isActive = False
        else:

            isValid = True
            isActive = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('active')), 1)
            isActive = 0 if isActive in (0, None, False) else 1

        # LEVEL -> 1: Personas que operan en el Sistema - 2: Instituciones. NO operan en el sistema
        self.__recordID = kwargs.get('fldID', None)
        self.__level = next((int(kwargs[j]) for j in kwargs if 'personlevel' in str(j).lower()), 1)
        self.__personType = next((kwargs[j] for j in kwargs if 'persontype' in str(j).lower()), 0)    # 0: Fisica - 1: Juridica
        self.__name = next((kwargs[j] for j in kwargs if 'fldname' in str(j).lower()), '')
        self.__lastName = next((kwargs[j] for j in kwargs if 'lastname' in str(j).lower()), '')
        self.__dob = next((kwargs[j] for j in kwargs if 'dob' in str(j).lower()), None)
        self.__comment = next((kwargs[j] for j in kwargs if 'comment' in str(j).lower()), '')
        self.__timeStamp = kwargs.get('fldTimeStamp', None)    # Esta fecha se usa para gestionar objetos repetidos.
        # __identifiers: list of UUID strings for each ID assigned to the person.
        self.__identifiers = []  # List of all IDs assigned to the person: Passport #, Driver's License #, etc.
        exitDate = next((kwargs[j] for j in kwargs if 'flddateexit' in str(j).lower()), None)
        if exitDate:
            try:
                self.__exitDate = datetime.strptime(exitDate, fDateTime)
            except(TypeError, AttributeError, NameError):
                self.__exitDate = 1         # Si no es fecha valida -> Tiene salida pero no se sabe la fecha de salida.
            isActive = False  # Tiene salida per no se sabe fecha
        else:
            self.__exitDate = None
        super().__init__(myID, isValid, isActive, *args, **kwargs)

        self.__myProgActivities = {}


    def __repr__(self):                 # self.getCategories() gets the right __categories dictionary.
        return "[{} {} - Level:{}; Person type: {}".format(self.__name, self.__lastName, self.level, self.__personType)

    @property
    def getElements(self):  # Retorna los ultimos valores almacenados en cada atributo
        return {'fldID': self.ID, 'fldName': self.__name, 'fldLastName': self.__lastName, 'fldDOB': self.__dob,
                'fldPersonLevel': self.__level, 'fldPersonType': self.__personType, 'fldDateExit': self.__exitDate,
                'fldComment': self.__comment}

    @property
    def name(self):
        return self.__name

    @property
    def personID(self):
        return self.__identifiers

    @property
    def recordID(self):
        return self.__recordID


    def updateAttributes(self, **kwargs):
        """ Updates object attributes with values passed in attr_dict. Values not passed leave that attribute unchanged.
        @return: None
        """
        if not kwargs:
            return None
        # TODO: IMPLEMENT THE UPDATE of attributes relevant to Person


    @property
    def myTimeStamp(self):  # impractical name to avoid conflicts with the many date, timeStamp, fldDate in the code.
        """
        returns record creation timestamp as a datetime object
        @return: datetime object Event Date.
        """
        return self.__timeStamp


    @classmethod
    def person_by_name(cls, *, name: str = None, last_name: str = None, enforce_order=True):
        """
        Returns dataframe with records from tbl Personas that match name / last_name.

        @param name: str
        @param last_name: str
        @param enforce_order: Verifies the order of name/last_name particles. Different order of items is a no-match.
        @return: DataFrame with all rows that match name / last_name combination.
        """
        # Processes name / last_name.
        if name:
            name = re.sub(r'[\\|@#$%^*()=+¿?{}"\'<>,:;_]', '', name)
            name = removeAccents(name)
            name = re.split(r'[-/ ]', name)
            name = [j for j in name if j]
        if last_name:
            last_name = re.sub(r'[\\|@#$%^*()=+¿?{}"\'<>,:;_]', '', last_name)
            last_name = removeAccents(last_name)
            last_name = re.split(r'[-/ ]', last_name)     # '/' and '-' are replaced by ' ': they separate names.
            last_name = [j for j in last_name if j]

        df = None
        for frame in cls.obj_dataframe():
            # if last_name:    # Logic to pull last name in order: only matches ALL items in last name in same order.
            #     # Here, split fldLastName by ' ' only.
            if last_name:    # Logic to find matches with any items that are present in last_name.
                aux1 = frame.copy()
                aux1.fldLastName = aux1['fldLastName'].apply(removeAccents).str.split()
                aux2 = aux1.copy()                  # aux2 required for enforce_order logic below.
                aux1 = aux1.explode('fldLastName', ignore_index=True)
                aux_last_name = aux1[aux1.fldLastName.isin(last_name)]
                if len(aux_last_name) != len(last_name):
                    # Not all items in last_name were found in frame -> There's no match.
                    aux_last_name = aux_last_name.iloc[0:0]
                else:
                    if enforce_order:       # Only applied to last names.
                        # Enforces order of items, that is: ['gomez', 'chavez'] != ['chavez', 'gomez']
                        last_name_lists = aux2.loc[aux2.fldID.isin(aux_last_name.fldID), ('fldID', 'fldLastName')]
                        aux_df = aux2[aux2.fldID.isin(last_name_lists.fldID.tolist())]
                        aux_last_name = aux_df[aux_df.fldLastName.isin([last_name])]
            else:
                aux_last_name = frame

            if name:      # name does NOT use enforce_order: 'Roque Ramon' is equal to 'Ramon Roque' for all purposes.
                # Split fldName by ' ' only.
                aux1 = frame.copy()
                aux1.fldName = aux1.fldName.apply(removeAccents).str.split()
                aux1 = aux1.explode('fldName', ignore_index=True)
                aux_name = aux1[aux1.fldName.isin(name)]
                if len(aux_name) != len(name):
                    # Not all items in name were found in frame -> There's no match.
                    aux_name = aux_name.iloc[0:0]
            else:
                aux_name = frame

            aux = aux_name[aux_name.fldID.isin(set(aux_last_name.fldID.tolist()))]
            aux.drop_duplicates(subset=['fldID', ], inplace=True)
            # Restores original name and last name.
            aux.fldName = frame.loc[frame['fldID'].isin(aux.fldID.tolist()), 'fldName']
            if df is None:
                df = aux.reset_index()
            else:
                if not aux.empty:
                    df.append(aux, ignore_index=True)

        return df


    # @classmethod
    # def person_by_name00(cls, *, name: str = None, last_name: str = None):
    #     """
    #     @param name: str
    #     @param last_name: str
    #     @return: DataFrame with all rows that match name / last_name combination.
    #     """
    #     # Processes name / last_name.
    #     if name:
    #         name = re.sub(r'[\\|@#$%^*()=+¿?{}"\'<>,:;_]', '', name)
    #         name = removeAccents(name)
    #         name = re.split(r'[-/]', name)
    #
    #     if last_name:
    #         last_name = re.sub(r'[\\|@#$%^*()=+¿?{}"\'<>,:;_]', '', last_name)
    #         last_name = removeAccents(last_name)
    #         last_name = re.split(r'[-/]', last_name)  # '/' and '-' are replaced by ' ': they separate names.
    #
    #     df = None
    #     for frame in cls.obj_dataframe():
    #         aux = None
    #         if not name:
    #             for lname in last_name:
    #                 inner = frame[frame['fldLastName'].apply(removeAccents).str.contains(lname)]
    #                 if not inner.empty:
    #                     if aux is None:
    #                         aux = inner
    #                     else:
    #                         aux.append(inner, ignore_index=True)
    #         elif not last_name:
    #             for lname in last_name:
    #                 inner = frame[frame['fldName'].apply(removeAccents).str.contains(lname)]
    #                 if not inner.empty:
    #                     if aux is None:
    #                         aux = inner
    #                     else:
    #                         aux.append(inner, ignore_index=True)
    #         else:
    #             aux = frame[frame['fldLastName'].apply(removeAccents).str.split('-').isin([last_name]) &
    #                         frame['fldName'].apply(removeAccents).str.split('-').isin([last_name])]
    #         if df is None:
    #             df = aux.reset_index()
    #         else:
    #             df.append(aux, ignore_index=True)
    #
    #     return df

    @classmethod
    def getObject(cls, obj_id: str = None, *, name=None, last_name=None, fetch_from_db=False, **kwargs):
        """ Returns list of Person objects that match obj_id. In the case of uid, list will contain only 1 item. Does
        NOT take person names. Person names must be sorted elsewhere.
        @param last_name:
        @param name:
        @param obj_id: can be a person UUID or a regular human-readable Person-ID string (DNI, Passport, CUIT, etc.) or
                       a name / lastname combination.
        @param fetch_from_db: Ignores memory data (_active_uids_df) and reads data from db.
        @param kwargs: Uses fldObjectUID when dict is passed.
        Tag numbers are normalized (removal of accents, dieresis, special characters, lower()) before processing.
        @return: cls Object or None if no object of class cls is found for obj_id passed.
        """
        if obj_id:
            if isinstance(obj_id, UUID):
                obj_id = obj_id.hex
            else:
                try:
                    obj_id = UUID(obj_id).hex
                except (SyntaxError, ValueError):  # SyntaxError: obj_id non compliant with UUID format. May be str.
                    if isinstance(obj_id, str):
                        names = re.sub(r'[-\\|/@#$%^*()=+¿?{}"\'<>,:;_]', '', obj_id)  # Clean Identifier name string.
                        # names = re.sub(r'-', ' ', names)
                        identifier = names.lower().strip().split(" ")[0] if names else None  # Pulls 1st identifier.

                        # Looks up uid via its identifier in TagPerson dataframes.
                        # IMPORTANT: Returns 1st match, which may not be the original object uid if duplicates exist.
                        if identifier:
                            uid_found = False
                            for df in TagPerson.obj_dataframe():   # Searches identifier in "Caravanas Personas" table.
                                try:
                                    identif = df.loc[df['fldIdentificadores'].isin([identifier]),'fldObjectUID'].iloc[0]
                                except IndexError:
                                    continue
                                else:
                                    if identif:
                                        # Pulls Person object uid from tblPersonas with identifier provided.
                                        # For this to work, TagPerson data must be already initialized (TagPerson
                                        # obj_dataframe above).
                                        for frame in cls.obj_dataframe():
                                            auxdf = frame[['fldObjectUID', 'fldIdentificadores']].explode(
                                                'fldIdentificadores')
                                            if not auxdf.empty:
                                                mask = auxdf[auxdf.isin([identif])['fldIdentificadores']]
                                                try:
                                                    obj_id = frame.loc[frame['fldObjectUID'].isin(
                                                                       mask['fldObjectUID']), 'fldObjectUID'].iloc[0]
                                                except (SyntaxError, ValueError, TypeError, IndexError):
                                                    continue
                                                else:
                                                    if obj_id is not None:
                                                        uid_found = True
                                                        break
                                        break
                            if not uid_found:
                                fetch_from_db = True      # uid not found in Person memory structure. Pulls it from db.
                        else:
                            return None
                    else:
                        return None
                except (TypeError, AttributeError, NameError):
                    return None     # Quits on any other type for obj_id.

            # Now MUST CHECK for duplication of the object's record (using _Duplication_Index)
            # Gets a list with 1 item: the uid for the Node Original Record (NOR, formerly Earliest Duplicate Record).
            dupl_uids = cls._get_duplicate_uids(obj_id, all_duplicates=True) or (obj_id,)
            uid_nor = cls._get_duplicate_uids(obj_id) or obj_id  # NOR: Node Original Record (same as EDR).
            if not fetch_from_db:
                # TODO IMPORTANT: df must contain all duplicate records for uid_nor in order to run checksum
                df = None
                for frame in cls.obj_dataframe():
                    aux = frame.loc[frame['fldObjectUID'].isin(dupl_uids)]
                    if not aux.empty:
                        if df is None:  # All this coding to retain the db Accessor values for resulting df.
                            aux.reset_index()
                            df = pd.DataFrame.db.create(frame.db.tbl_name, data=aux.to_dict())
                        else:
                            df.append(aux, ignore_index=True)  # uses append to retain db Accessor settings.
                if df is None:
                    fetch_from_db = True  # No data in memory, forces retrieval from db.

            if fetch_from_db:
                # Fetches data from db.
                if dupl_uids != (obj_id,):
                    # There are duplicates: pulls data using fld_Duplication_Index.
                    sql = f'SELECT * FROM "{cls.__tblObjDBName}" WHERE ' \
                          f'"{getFldName(cls.tblObjName(), "fld_Duplication_Index")}" ' \
                          f'IN {str(dupl_uids) if len(dupl_uids) > 1 else str(dupl_uids).replace(",", "")}; '
                else:
                    # No duplicates: fld_Duplication_Index may be empty. Pulls data using fldObjectUID.
                    sql = f'SELECT * FROM "{cls.__tblObjDBName}" WHERE ' \
                          f'"{getFldName(cls.tblObjName(), "fldObjectUID")}" ' \
                          f'IN {str(dupl_uids) if len(dupl_uids) > 1 else str(dupl_uids).replace(",", "")}; '
                df = pd.read_sql_query(sql, SQLiteQuery().conn)  # df contains all duplicate records (if any) or just 1.

            if not df.empty:
                # Get checksum from Node Original Record (also called EDR) and compare to data just read into df.
                # All this logic to update the memory dataframe with any new data appearing on tblOjbect.
                # TODO(cmt): NOR is the only record in the table with fldObjectUID = _Duplication_Index (by design)
                #  NOR: Node Original Record
                # nor_idx = df[df['fld_Duplication_Index'] == uid_nor].index[df['fld_Duplication_Index'] ==
                #                                                            df['fldObjectUID']].tolist()[0]
                nor_idx = df[df['fldObjectUID'] == uid_nor].index[0]
                nor_checksum = df.loc[nor_idx, 'fld_Update_Checksum']
                # Removes the checksum column from the checksum computation so that checksum is valid. Also sets all
                # null values to None BEFORE computating checksum so that it's consistent with value read from db.
                # fld_Update_Checksum val may be outdated when loaded from db. Always run this code to update if needed.
                df.fillna(np.nan).replace([np.nan], [None], inplace=True)
                new_checksum = hashlib.sha256(df.drop('fld_Update_Checksum', axis=1).to_json().encode()).hexdigest()
                if nor_checksum != new_checksum:
                    # Values have changed for the duplicate records associated to animal_uid. Must reload from db.
                    # Updates row check sum and writes nor record to db (the only record modified here).
                    df.loc[nor_idx, 'fld_Update_Checksum'] = new_checksum
                    # Update the correct db record regardless of nor_idx, as fldID remains unchanged.
                    _ = setRecord(cls.tblObjName(), **df.loc[nor_idx].to_dict())  # Writes to db if NOR row changed
                    # After setRecord(), must reload full data from db since _active_uids_df is an iterator and cannot
                    # be updated on a single-record basis.
                    cls._init_uid_dicts()

                obj_data_dict = df.loc[nor_idx].to_dict()
                return Person(**obj_data_dict)  # Original Object uid found.
        return None



    # @classmethod
    # def getObject01(cls, obj_id: str = None, fetch_from_db=False, **kwargs):        # Doesn't process names/lastnames.
    #     """ Returns list of Person objects that match obj_id. In the case of uid, list will contain only 1 item. Does
    #     NOT take person names. Person names must be sorted elsewhere.
    #     @param obj_id: can be a person UUID or a regular human-readable Person-ID string (DNI, Passport, CUIT, etc.).
    #     @param fetch_from_db: Ignores memory data (_active_uids_df) and reads data from db.
    #     @param kwargs: Uses fldObjectUID when dict is passed.
    #     Tag numbers are normalized (removal of accents, dieresis, special characters, lower()) before processing.
    #     @return: cls Object or None if no object of class cls is found for obj_id passed.
    #     """
    #     if obj_id:
    #         if isinstance(obj_id, UUID):
    #             obj_id = obj_id.hex
    #         else:
    #             try:
    #                 obj_id = UUID(obj_id).hex
    #             except (SyntaxError, ValueError):  # SyntaxError: obj_id non compliant with UUID format. May be str.
    #                 if isinstance(obj_id, str):
    #                     names = re.sub(r'[-\\|/@#$%^*()=+¿?{}"\'<>,:;_]', '', obj_id)  # Clean Identifier name string.
    #                     identifier = names.lower().strip().split(" ")[0] if names else None  # Pulls 1st identifier.
    #
    #                     # Looks up uid via its identifier in TagPerson dataframes.
    #                     # IMPORTANT: Returns 1st match, which may not be the original object uid if duplicates exist.
    #                     if identifier:
    #                         uid_found = False
    #                         for df in TagPerson.obj_dataframe():  # Searches identifier in "Caravanas Personas" table.
    #                             try:
    #                                 identif = df.loc[df['fldIdentificadores'].isin([identifier]), 'fldObjectUID'].iloc[
    #                                     0]
    #                             except IndexError:
    #                                 continue
    #                             else:
    #                                 if identif:
    #                                     # Pulls Person object uid from tblPersonas with identifier provided.
    #                                     # For this to work, TagPerson data must be already initialized (TagPerson
    #                                     # obj_dataframe above).
    #                                     for frame in cls.obj_dataframe():
    #                                         auxdf = frame[['fldObjectUID', 'fldIdentificadores']].explode(
    #                                             'fldIdentificadores')
    #                                         if not auxdf.empty:
    #                                             mask = auxdf[auxdf.isin([identif])['fldIdentificadores']]
    #                                             try:
    #                                                 obj_id = frame.loc[frame['fldObjectUID'].isin(
    #                                                     mask['fldObjectUID']), 'fldObjectUID'].iloc[0]
    #                                             except (SyntaxError, ValueError, TypeError, IndexError):
    #                                                 continue
    #                                             else:
    #                                                 if obj_id is not None:
    #                                                     uid_found = True
    #                                                     break
    #                                     break
    #                         if not uid_found:
    #                             fetch_from_db = True  # uid not found in Person memory structure. Pulls it from db.
    #                     else:
    #                         return None
    #                 else:
    #                     return None
    #             except (TypeError, AttributeError, NameError):
    #                 return None  # Quits on any other type for obj_id.
    #
    #         # Now MUST CHECK for duplication of the object's record (using _Duplication_Index)
    #         # Gets a list with 1 item: the uid for the Node Original Record (NOR, formerly Earliest Duplicate Record).
    #         dupl_uids = cls._get_duplicate_uids(obj_id, all_duplicates=True) or (obj_id,)
    #         uid_nor = cls._get_duplicate_uids(obj_id) or obj_id  # NOR: Node Original Record (same as EDR).
    #         if not fetch_from_db:
    #             # TODO IMPORTANT: df must contain all duplicate records for uid_nor in order to run checksum
    #             df = None
    #             for frame in cls.obj_dataframe():
    #                 aux = frame.loc[frame['fldObjectUID'].isin(dupl_uids)]
    #                 if not aux.empty:
    #                     if df is None:  # All this coding to retain the db Accessor values for resulting df.
    #                         aux.reset_index()
    #                         df = pd.DataFrame.db.create(frame.db.tbl_name, data=aux.to_dict())
    #                     else:
    #                         df.append(aux, ignore_index=True)  # uses append to retain db Accessor settings.
    #             if df is None:
    #                 fetch_from_db = True  # No data in memory, forces retrieval from db.
    #
    #         if fetch_from_db:
    #             # Fetches data from db.
    #             if dupl_uids != (obj_id,):
    #                 # There are duplicates: pulls data using fld_Duplication_Index.
    #                 sql = f'SELECT * FROM "{cls.__tblObjDBName}" WHERE ' \
    #                       f'"{getFldName(cls.tblObjName(), "fld_Duplication_Index")}" ' \
    #                       f'IN {str(dupl_uids) if len(dupl_uids) > 1 else str(dupl_uids).replace(",", "")}; '
    #             else:
    #                 # No duplicates: fld_Duplication_Index may be empty. Pulls data using fldObjectUID.
    #                 sql = f'SELECT * FROM "{cls.__tblObjDBName}" WHERE ' \
    #                       f'"{getFldName(cls.tblObjName(), "fldObjectUID")}" ' \
    #                       f'IN {str(dupl_uids) if len(dupl_uids) > 1 else str(dupl_uids).replace(",", "")}; '
    #             df = pd.read_sql_query(sql, SQLiteQuery().conn)  # df contains all duplicate records (if any) or  1.
    #
    #         if not df.empty:
    #             # Get checksum from Node Original Record (also called EDR) and compare to data just read into df.
    #             # All this logic to update the memory dataframe with any new data appearing on tblOjbect.
    #             # TODO(cmt): NOR is the only record in the table with fldObjectUID = _Duplication_Index (by design)
    #             #  NOR: Node Original Record
    #             # nor_idx = df[df['fld_Duplication_Index'] == uid_nor].index[df['fld_Duplication_Index'] ==
    #             #                                                            df['fldObjectUID']].tolist()[0]
    #             nor_idx = df[df['fldObjectUID'] == uid_nor].index[0]
    #             nor_checksum = df.loc[nor_idx, 'fld_Update_Checksum']
    #             # Removes the checksum column from the checksum computation so that checksum is valid. Also sets all
    #             # null values to None BEFORE computating checksum so that it's consistent with value read from db.
    #             # fld_Update_Checksum val may be outdated when loaded from db. Run this code to update if needed.
    #             df.fillna(np.nan).replace([np.nan], [None], inplace=True)
    #             new_checksum = hashlib.sha256(df.drop('fld_Update_Checksum', axis=1).to_json().encode()).hexdigest()
    #             if nor_checksum != new_checksum:
    #                 # Values have changed for the duplicate records associated to animal_uid. Must reload from db.
    #                 # Updates row check sum and writes nor record to db (the only record modified here).
    #                 df.loc[nor_idx, 'fld_Update_Checksum'] = new_checksum
    #                 # Update the correct db record regardless of nor_idx, as fldID remains unchanged.
    #                 _ = setRecord(cls.tblObjName(), **df.loc[nor_idx].to_dict())  # Writes to db if NOR row changed
    #                 # After setRecord(), must reload full data from db since _active_uids_df is an iterator and cannot
    #                 # be updated on a single-record basis.
    #                 cls._init_uid_dicts()
    #
    #             obj_data_dict = df.loc[nor_idx].to_dict()
    #             return Person(**obj_data_dict)  # Original Object uid found.
    #     return None


    @property
    def level(self):
        return self.__level

    @level.setter
    def level(self, val):
        if val in (1, 2):
            self.__level = val      # Solo setea si val es valido. Si no, deja el valor existente.

    @property
    def personNames(self):
        return {'names': self.__name, 'lastname': self.__lastName}

        # isValid, isActive, isRegistered=register.get, getID, getPersonData (Name,LastName,dob,etc),
        # getLevel (Usar el metodo getPersonData y filtrar level), status (get/set), localization (get/set),

    @property
    def activities(self):  # @property activities es necesario para las llamadas a obj.outerObject.
        return self.__activitiesDict

    @property
    def exitYN(self):
        try:
            return self.__exitDate
        except AttributeError:
            return None

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
    def getActivitiesDict(cls):
        return cls.__activitiesDict

    @classmethod
    # @timerWrapper(iterations=5)
    def processReplicated(cls):
        """             ******  Run periodically as IntervalTimer func. ******
                        ******  IMPORTANT: This code should execute in LESS than 5 msec (switchinterval).      ******
        Used to execute logic for detection and management of duplicate objects.
        Defined for Animal, Tag, Person. (Geo has its own function on TransactionalObject class).
        Checks for additions to tblAnimales from external sources (db replication) for Valid and Active objects.
        Updates _table_record_count.
        @return: True if update operation succeeds, False if reading tblAnimales from db fails.
        """
        dftemp0 = getrecords(cls.tblObjName(), '*', where_str=f'WHERE "{getFldName(cls.tblObjName(), "fldDateExit")}" '
                                                              f'== 0; ')
        if dftemp0.empty:
            return False

        return True


    @staticmethod
    def getPersonLevels(activeArg=None, levelArg=None):
        """
        Returns Dictionary of Persons matching active/level passed as args.
        @param levelArg: 1,2: Levels; 0, '', None=ALL
        @return: Dictionary of the form {idPerson: personLevel,}
        """
        tblObjectName = 'tblPersonas'
        level = levelArg if levelArg in [1, 2] else [1, 2]
        active = activeArg if activeArg else None
        temp = getRecords(tblObjectName, '', '', None, '*', fldPersonLevel=level)
        retDict = {}
        for j in range(temp.dataLen):
            tempRecord = temp.unpackItem(j)
            if len(tempRecord) > 0:
                if not active:
                    if tempRecord['fldDateExit'] != '':
                        if not active:
                            retDict[tempRecord['fldID']] = tempRecord['fldPersonLevel']
                    else:
                        if not active or active == 0:
                            retDict[tempRecord['fldID']] = tempRecord['fldPersonLevel']
        return retDict

