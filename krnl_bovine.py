import faulthandler
import pandas as pd
import itertools as it
import threading
from krnl_abstract_class_animal import *
# from collections import defaultdict
from krnl_config import PANDAS_READ_CHUNK_SIZE
from krnl_bovine_activity import CategoryActivity, BovineActivity, InventoryActivityAnimal
from krnl_db_query import AccessSerializer, AccessSerializer

                                          # Vacunos #
def moduleName():
    return str(os.path.basename(__file__))

class Config:
    arbitrary_types_allowed = True
# @pydantic.dataclasses.dataclass(config=Config)


class Bovine(Mammal):
    # __objClass = 5
    # __objType = 1
    # __genericAnimalID = None
    # __moduleRunning = 1   # Flag que indica si el modulo de la clase de Animal se esta ejecutando. Ver como setear
    _animalClassName = None     # 'Bovine'
    _kindOfAnimalID = None      # 1             # 1:'Bovine', 2:'Caprine', 3:'Ovine', 4:'Porcine', 5:'Equine'.
    __cls_name = __qualname__
    for k, v in Animal.getAnimalKinds().items():
        if k.lower() == __cls_name.lower():
            _animalClassName, _kindOfAnimalID = (k, v)
    _defaultDaysToTimeout = 366      # Default days to Timeout for Bovines.
    __lock = threading.Lock()

    faulthandler.enable()       # TODO: Disable this later on...
    # Defining _activityObjList will call  _creatorActivityObjects() in EntityObject.__init_subclass__().
    _activityObjList = []  # List of Activity objects created by factory function.
    _myActivityClass = BovineActivity  # Will look for the property-creating classes starting in this class.
    __uses_tags = True          # Flag to implement Tag subclass for this class.

    @classmethod
    def animalClassID(cls):
        return cls._kindOfAnimalID      # TODO: cambiar a cls.__name__ (some day...)

    # TODO(cmt): dataframes and Series to manage Bovine object queries, handling Duplicates management behind the scenes
    #  and storing frequently accessed data in memory.
    _active_uids_df = {}              # DataFrame.  {fldObjectUID: fld_Duplication_Index}
    _object_mem_fields = ('*', )  # ('fldID', 'fldObjectUID', 'fldDOB', 'fld_Duplication_Index', 'fldDaysToTimeout',
                                  # 'fldMode','fldIdentificadores', 'fldMF')
    _access_serializer = AccessSerializer()     # Keeps track of # of accesses to the code in _init_uid_dicts().
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
    _dupl_series = pd.Series([], dtype=object)     # Array of the form (_Duplication_Index, (fldObjUID1, fldObjUID2,))
    # _dupl_series.index.set_names('fld_Duplication_Index', inplace=True)
    _chunk_size = 300  # PANDAS_READ_CHUNK_SIZE   #todo: reinstate PANDAS_READ_CHUNK_SIZE  # This value is specific to each class.

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
              f'IS NULL OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ' \
              f'AND "{getFldName(cls.tblObjName(), "fldFK_ClaseDeAnimal")}" == {cls.animalClassID()}; '


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
            access_count = cls._access_serializer.access_count     # This call uses an internal stack. It's thread-safe.
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
                    for df in ittr[0]:              # Detects empty frames.
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
                            temp_dupl = not_nulls.groupby('fld_Duplication_Index')['fldObjectUID'].agg(set)  #Series
                            duplic_list.append(temp_dupl)
                    if duplic_list:
                        dupseries = pd.concat(duplic_list, copy=False)

                    cls._active_uids_df = ittr[2]               # SHARED resource. Accessed from background threads.
                    if dupseries is not None:
                        try:
                            pd.testing.assert_series_equal(dupseries, cls._dupl_series)
                        except (AssertionError, TypeError, ValueError, KeyError, IndexError):
                            cls._dupl_series = dupseries.copy()    # SHARED resource. Accessed from background threads.
            # End of semaphore-protected section: releases lock to next the thread that may be waiting for semaphore
        # End of outer 'with' block: Here, __exit__() is called and internal access_counter is decremented.
        return


    # @classmethod
    # def _init_uid_dicts07(cls):             # reads from db full db table. DOES NOT use chunks
    #     """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
    #     Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
    #     This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
    #     _processDuplicates() run by background functions.
    #      This function is called on data changes, so it will force an update of the object structures (_active_uids_df)
    #     @return: None
    #     Instead of locking out the whole code block uses a AccessSerializer obj with a wait to free up the Python lock.
    #     An access_count counter is used internally to identify the last caller so that only the last caller will update
    #     the dataframe data. Handles concurrency more efficiently than other methods my minimizing the use of the
    #     global mutex lock.
    #     _access_serializer is freed just after reading the database so that others can run this same code concurrently
    #     and eventually, in the case of concurrency, only one write to memory is done. The write is done by the function
    #     instance that made the last (latest) db read, and all others are ignored. The logic is to write to memory
    #     (eventually to db, when writes are done to db with this same system) ONLY ONCE when concurrent access happens.
    #     """
    #
    #     # TODO(cmt): all the code below down to the end is protected from concurrenty re-entry by other threads.
    #     #  All subsequent calls to _init_uid_dicts() will go to wait() until release() runs its notify() line.
    #     with cls._access_serializer(wait=False):      #  as access_count:
    #         access_count = cls._access_serializer.access_count
    #         # access_count = cls._access_serializer.acquire(wait=False)
    #         # Starts _serializer here, without waiting, to detect all concurrent calls to _init_uid_dicts and initialize
    #         # access_count for each instance of _init_uid_dicts that accesses the database. It is needed in order to
    #         # identify later on the last one to access database. This will be the instance that updates _active_uids_df.
    #         df = pd.read_sql_query(cls._sql_uids(), SQLiteQuery().conn)
    #         if df.empty:
    #             val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts not initialized." \
    #                   f" System cannot operate."
    #             krnl_logger.warning(val)
    #             # cls._access_serializer.release()        # TODO(cmt): Line not required when using context manager.
    #             raise sqlite3.DatabaseError(val)
    #
    #         if any(df['fld_Duplication_Index'].notnull()):
    #             # Series with duplicates sets for ease of access to duplicates. Uses set to do item comparisons.
    #             not_nulls = df.loc[df['fld_Duplication_Index'].notnull()]
    #             dupseries = not_nulls.groupby('fld_Duplication_Index')['fldObjectUID'].agg(set)
    #         else:
    #             dupseries = None  # Duplication Series, for ease of access to the (hopefully very few) duplicates.
    #
    #         """ In the lines below the thread accesses shared resources, enters wait state only on critical sections """
    #         # The 'if' below:
    #         #   access_count == cls._access_serializer.total_count -> Enters 'if' for LAST execution instance of func.
    #         #   access_count == 1 ->Enters 'if' for FIRST execution instance of func. Use this flexibility where needed.
    #         if access_count == cls._access_serializer.total_count:
    #             cls._access_serializer._wait()  # Enters wait, until notified. To serialize access to _active_uids_df.
    #             cls._active_uids_df = df.copy()  # SHARED resource. Once notified above proceeds with the writes to mem.
    #             try:
    #                 pd.testing.assert_series_equal(dupseries, cls._dupl_series)
    #             except (TypeError, AssertionError, ValueError):
    #                 cls._dupl_series = dupseries.copy()  # SHARED resource. Accessed from background threads.
    #
    #         # End of with block: release() is called, which issues notify() and awakens 1 thread at a time for every
    #         # concurrent call to _init_uids_dict().
    #         # MUST be in the same tab line as acquire(), works in pairs, same as acquire()/release() of a lock.
    #         # TODO(cmt): Using notify() (and NOT notify_all()) enforces serialization of access to the protected code.
    #     # cls._access_serializer.release()      # release() must be always paired with acquire()/begin()
    #     return


    # @classmethod
    # def _init_uid_dicts06(cls):  # Full implementation of AccessSerializer. Using context manager, NOT using _wait() func.
    #     """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
    #     Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
    #     This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
    #     _processDuplicates() run by background functions.
    #      This function is called on data changes, so it will force an update of the object structures (_active_uids_df)
    #     @return: None
    #     Instead of locking out the whole code block uses a Condition obj with a wait to free up the lock to other tasks.
    #     A calls_count counter is used internally to identify the last caller so that only the last caller will update
    #     the dataframe data. Handles concurrency more efficiently than other methods my minimizing the used of the
    #     global lock.
    #     _access_serializer is freed just after reading the database so that other can run this same code concurrently
    #     and eventually, in the case of concurrency, only one write to memory is done. The write is done by the function
    #     instance that made the last (latest) db read, and all others are ignored. The idea is to write to memory
    #     (eventually to db, when writes are done to db with this same system) ONLY ONCE when concurrent access happens.
    #     """
    #     # This sql will bring a fresh snapshot of all active objects in the database.
    #     mem_flds = ", ".join(tuple([f'"{getFldName(cls.tblObjName(), j)}"' for j in
    #                                 cls._object_mem_fields])).replace('(', '').replace(')', '')
    #     sql = f'SELECT {mem_flds} from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" ' \
    #           f'IS NULL OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ' \
    #           f'AND "{getFldName(cls.tblObjName(), "fldFK_ClaseDeAnimal")}" == {cls.animalClassID()}; '
    #
    #     # TODO(cmt): all the code below down to the end is protected from concurrenty re-entry by other threads.
    #     #  All subsequent calls to _init_uid_dicts() will go to wait() until release() runs its notify() line.
    #
    #     with cls._access_serializer as access_count:
    #         # access_count = cls._access_serializer.acquire(wait=False)
    #         # TODO: 'if' below will enter the protected code ONLY ONCE: on the last concurrent call. See how it works...
    #         # Between the 'with' above and the 'if' below there could be a call to _init_uids_dict from another thread.
    #         # Then, cls._access_serializer.total_count will yield a value different from call_count, and the logic works
    #         # by running the code below only when the thread that has call_count == total_count is invoked.
    #         # The 'if' below:
    #         #   access_count == cls._access_serializer.total_count -> Enters 'if' for LAST execution instance of func.
    #         #   access_count == 1 ->Enters 'if' for FIRST execution instance of func. Use this flexibility where needed.
    #         if access_count == cls._access_serializer.total_count:
    #             df = pd.read_sql_query(sql, SQLiteQuery().conn)
    #             if df.empty:
    #                 val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts not " \
    #                       f"initialized. System cannot operate."
    #                 krnl_logger.warning(val)
    #                 cls._access_serializer.release()
    #                 raise sqlite3.DatabaseError(val)
    #
    #             if any(df['fld_Duplication_Index'].notnull()):
    #                 # Series with duplicates sets for ease of access to duplicates. Uses set to do item comparisons.
    #                 not_nulls = df.loc[df['fld_Duplication_Index'].notnull()]
    #                 dupseries = not_nulls.groupby('fld_Duplication_Index')['fldObjectUID'].agg(set)
    #             else:
    #                 dupseries = None  # Duplication dataframe for ease of access to the (hopefully very few) duplicates.
    #
    #             # Thread enters wait state only on critical sections.
    #             cls._active_uids_df = df.copy()  # SHARED resource. Accessed from background threads.
    #             try:
    #                 pd.testing.assert_series_equal(dupseries, cls._dupl_series)
    #             except (TypeError, AssertionError, ValueError):
    #                 cls._dupl_series = dupseries.copy()  # SHARED resource. Accessed from background threads.
    #
    #     # End of with block: release() is called and release() calls notify() and awakens 1 thread at a time for
    #     # every call to _init_uids_dict().
    #     # MUST be in the same tab line as acquire(), works in pairs, same as acquire()/release() of a lock.
    #     # TODO(cmt): Using notify() (and NOT notify_all()) enforces serialization of access to the protected code.
    #     # cls._access_serializer.release()
    #     return
    #
    # @classmethod
    # def _init_uid_dicts05(cls):  # TODO: This version runs on Condition. MUST BE TESTED on concurrent access.
    #     """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
    #     Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
    #     This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
    #     _processDuplicates() run by background functions.
    #      This function is called on data changes: it will perform updates of the object structures (_active_uids_df)
    #     @return: None
    #     Instead of using a lock, uses an Event object with a wait to free up the lock to other tasks.
    #     A calls_count counter is used to identify the last caller. Only the last caller will update the
    #     dataframe data. Handles concurrency more efficiently than other methods my minimizing the blocked time of
    #     threads put to wait.
    #     _access_serializer Event is freed just after reading the database so that other can run this same code concu
    #     rrently and eventually, in the case of concurrency, only one write to memory is done. The write is done by the
    #     function instance that made the last (latest) db read, and all others are ignored. The idea is to write to mem
    #     (eventually to db, when writes are done to db with this same system) ONLY ONCE when concurrent access happens.
    #     """
    #     # This sql will bring a fresh snapshot of all active objects in the database.
    #     mem_flds = ", ".join(tuple([f'"{getFldName(cls.tblObjName(), j)}"' for j in
    #                                 cls._object_mem_fields])).replace('(', '').replace(')', '')
    #     sql = f'SELECT {mem_flds} from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" ' \
    #           f'IS NULL OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ' \
    #           f'AND "{getFldName(cls.tblObjName(), "fldFK_ClaseDeAnimal")}" == {cls.animalClassID()}; '
    #
    #     # TODO(cmt): all the code below down to the end is protected from concurrenty re-entry by other threads.
    #     #  All subsequent calls to _init_uid_dicts() will go to the wait() line until they get notify_all() from below.
    #     with cls._access_serializer:
    #         if not cls._calls_count:  # This line acquires the condition to be used further on.
    #             cls._calls_count += 1  # 1st call doesn't wait.
    #         else:
    #             cls._calls_count += 1  # Must keep track of # of threads accesing _init_uid_dicts at the same time.
    #             cls._access_serializer.wait(5)  # All subsequent calls are put to wait. Here, releases the lock.
    #
    #     df = pd.read_sql_query(sql, SQLiteQuery().conn)
    #     if df.empty:
    #         val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts no initialized. " \
    #               f"System cannot operate."
    #         krnl_logger.warning(val)
    #         with cls._access_serializer:
    #             cls._calls_count -= 1
    #             cls._access_serializer.notify()          # notify next waiting thread to resume execution.
    #         raise sqlite3.DatabaseError(val)
    #
    #     if any(df['fld_Duplication_Index'].notnull()):
    #         # Creates Series with duplicates sets, for ease of access to duplicates. Uses set to do item comparisions.
    #         dupseries = df.loc[df['fld_Duplication_Index'].notnull()].groupby('fld_Duplication_Index')['fldObjectUID'].\
    #             agg(set)
    #     else:
    #         dupseries = None  # Duplication dataframe, for ease of access to the (hopefully very few) duplicates.
    #     cls._active_uids_df = df.copy()  # SHARED resource. Accessed from background threads.
    #     try:
    #         pd.testing.assert_series_equal(dupseries, cls._dupl_series)
    #     except (TypeError, AssertionError, ValueError):
    #         cls._dupl_series = dupseries.copy()  # If not equal, updates of _dupl_series.
    #
    #     with cls._access_serializer:
    #         cls._calls_count -= 1
    #         cls._access_serializer.notify()      # notify next waiting thread to resume execution.
    #     return


    # @classmethod                 # TODO: This version is very simple and runs well with EventLock object.
    # def _init_uid_dicts04(cls):  # Simpler version with blocking of full db-read to mem-write block. WORKING VERSION
    #     """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
    #     Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
    #     This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
    #     _processDuplicates() run by background functions.
    #     This function is called on data changes, so it will force an update of the object structures (_active_uids_df)
    #     @return: None
    #     Instead of using a lock, uses an Event object with a wait to free up the lock to other tasks.
    #     A calls_count counter is used to identify the last caller. Only the last caller will update the
    #     dataframe data. Handles concurrency more efficiently than other methods my minimizing the blocked time of
    #     threads put to wait.
    #     _access_serializer Event is freed just after reading the database so that other can run this same code concu
    #     rrently and eventually, in the case of concurrency, only one write to memory is done. The write is done by the
    #     function instance that made the last (latest) db read, and all others are ignored. The idea is to write to mem
    #     (eventually to db, when writes are done to db with this same system) ONLY ONCE when concurrent access happens.
    #     """
    #     # This sql will bring a fresh snapshot of all active objects in the database.
    #     mem_flds = ", ".join(tuple([f'"{getFldName(cls.tblObjName(), j)}"' for j in
    #                                 cls._object_mem_fields])).replace('(', '').replace(')', '')
    #     sql = f'SELECT {mem_flds} from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" ' \
    #           f'IS NULL OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ' \
    #           f'AND "{getFldName(cls.tblObjName(), "fldFK_ClaseDeAnimal")}" == {cls.animalClassID()}; '
    #
    #     """ All other threads calling this code will sit to wait until this code releases _access_serializer. """
    #     # TODO(cmt): all the code below down to the end is protected from concurrenty re-entry by other threads.
    #     cls._access_serializer.start()
    #     df = pd.read_sql_query(sql, SQLiteQuery().conn)
    #
    #     if df.empty:
    #         val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts no initialized. " \
    #               f"System cannot operate."
    #         krnl_logger.warning(val)
    #         cls._access_serializer.release()
    #         raise sqlite3.DatabaseError(val)
    #
    #     if any(df['fld_Duplication_Index'].notnull()):
    #         # Creates Series with duplicates sets, for ease of access to duplicates. Uses set to do item comparisions.
    #         dupseries = df.loc[df['fld_Duplication_Index'].notnull()].groupby('fld_Duplication_Index')['fldObjectUID'].agg(set)
    #     else:
    #         dupseries = None  # Duplication dataframe, for ease of access to the (hopefully very few) duplicates.
    #     cls._active_uids_df = df.copy()  # SHARED resource. Accessed from background threads.
    #     try:
    #         pd.testing.assert_series_equal(dupseries, cls._dupl_series)
    #     except (TypeError, AssertionError, ValueError):
    #         cls._dupl_series = dupseries.copy()  # If not equal, updates of _dupl_series.
    #     cls._access_serializer.release()
    #     return



    # @classmethod
    # def _init_uid_dicts03(cls):         # Full version, with dedicated blocking in specific parts.
    #     """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
    #     Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
    #     This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
    #     _processDuplicates() run by background functions.
    #      This function is called on data changes, so it will force an update of the object structures (_active_uids_df)
    #     @return: None
    #     Instead of using a lock, uses an Event object with a wait to free up the lock to other tasks.
    #     A calls_count counter is used to identify the last caller. Only the last caller will update the
    #     dataframe data. Handles concurrency more efficiently than other methods my minimizing the blocked time of
    #     threads put to wait.
    #     _access_serializer Event is freed just after reading the database so that other can run this same code concu
    #     rrently and eventually, in the case of concurrency, only one write to memory is done. The write is done by the
    #     function instance that made the last (latest) db read, and all others are ignored. The idea is to write to memory
    #     (eventually to db, when writes are done to db with this same system) ONLY ONCE when concurrent access happens.
    #     """
    #     # This sql will bring a fresh snapshot of all active objects in the database.
    #     mem_flds = ", ".join(tuple([f'"{getFldName(cls.tblObjName(), j)}"' for j in
    #                                 cls._object_mem_fields])).replace('(', '').replace(')', '')
    #     sql = f'SELECT {mem_flds} from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" ' \
    #           f'IS NULL OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ' \
    #           f'AND "{getFldName(cls.tblObjName(), "fldFK_ClaseDeAnimal")}" == {cls.animalClassID()}; '
    #
    #     cls._access_serializer.wait(3)  # System starts with event set so this line won't wait in normal circumstances.
    #     """ All other threads calling this code will sit to wait until this code sets _access_serializer again. """
    #     with cls.__lock:                                                                                # TODO: start()
    #         cls._access_serializer.clear()  # Signals update ongoing: this will put all other threads to wait.
    #     cls._acquired_count += 1  # Inside lock because += is not atomic. lock is not strictly needed, but still..
    #     counter = cls._acquired_count  # defines function instance variable.
    #     df = pd.read_sql_query(sql, SQLiteQuery().conn)
    #     cls._access_serializer.set()  # Frees up function. Blocked concurrent call(s) resume execution.# TODO: release()
    #
    #     if df.empty:
    #         val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts no initialized. " \
    #               f"System cannot operate."
    #         krnl_logger.warning(val)
    #         cls._acquired_count -= 1  # Since this one aborts without attempting a mem write, must decrease _acquired_count.  # TODO: abort()
    #         raise sqlite3.DatabaseError(val)
    #
    #     if any(df['fld_Duplication_Index'].notnull()):
    #         # Creates Series with duplicates sets, for ease of access to duplicates. Uses set to do item comparisions.
    #         dupseries = df.loc[df['fld_Duplication_Index'].notnull()].
    #         groupby('fld_Duplication_Index')['fldObjectUID'].agg(set)
    #     else:
    #         dupseries = None  # Duplication dataframe, for ease of access to the (hopefully very few) duplicates.
    #
    #     # Memory update done by the function instance that did the LAST db read (counter == cls._acquired_count).
    #     # Makes logic independent of thread execution sequence.
    #     with cls.__lock:
    #         if counter == cls._acquired_count:                                          # TODO: check_write_condition()
    #             cls._access_serializer.clear()  # Blocks other concurring threads in order to write to memory.
    #         else:
    #             return  # If not the instance that made the last db read, exits without writing to mem.
    #
    #     # All the code below protected from concurrency with another instance of _init_uid_dicts() by event clear().
    #     cls._active_uids_df = df.copy()  # SHARED resource. Accessed from background threads.
    #     try:
    #         pd.testing.assert_series_equal(dupseries, cls._dupl_series)
    #     except (TypeError, AssertionError, ValueError):
    #         cls._dupl_series = dupseries.copy()  # If not equal, updates of _dupl_series.
    #     cls._acquired_count = 0  # After writing to mem, restes _acquired_count.                 # TODO: reset_and_release()
    #     cls._access_serializer.set()  # sets Event so that next call can execute.
    #     return
    #
    # @classmethod
    # def _init_uid_dicts02(cls):                     # Working version. Uses an event to handle concurrenc
    #     """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
    #     Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
    #     This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
    #     _processDuplicates() run by background functions.
    #     This function is called on data changes, so it will force an update of the object structures (_active_uids_df)
    #     @return: None
    #
    #     This approach works ok and shows a nice use of events to handle access to a shared resource by mutiple threads
    #     However, it is not the most efficient as a waiting thread must wait for the whole function body to execute.
    #     An alternative method, more efficient in this terms, is implemented with locks and a sequence counter.
    #     This one is left here for reference."""
    #     # todo(cmt): CAUTION. The variables used (event) are removed.
    # # This sql will bring a fresh snapshot of all active objects in the database.
    #     mem_flds = ", ".join(tuple([f'"{getFldName(cls.tblObjName(), j)}"' for j in
    #                                 cls._object_mem_fields])).replace('(', '').replace(')', '')
    #     sql = f'SELECT {mem_flds} from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" ' \
    #           f'IS NULL OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ' \
    #           f'AND "{getFldName(cls.tblObjName(), "fldFK_ClaseDeAnimal")}" == {cls.animalClassID()}; '
    #
    #     cls._access_serializer.wait(5)  # System starts with event set so this line won't wait in normal circumstances.
    #     # TODO(cmt): All other threads calling this code will sit to wait until this code sets _access_serializer again.
    #     cls._access_serializer.clear()  # Signals update busy: this will put all other threads to wait.
    #
    #     df = pd.read_sql_query(sql, SQLiteQuery().conn)
    #     if df.empty:
    #         val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts no initialized. " \
    #               f"System cannot operate."
    #         krnl_logger.warning(val)
    #         cls._access_serializer.set()
    #         raise sqlite3.DatabaseError(val)
    #
    #     col_duplication_index = df['fld_Duplication_Index'].fillna(np.nan).replace([np.nan], [None]).tolist()
    #     col_uid = df['fldObjectUID'].tolist()
    #     # dicto1 = dict(zip(col_uid, col_duplication_index))  # Reference dict
    #     temp_dict = {}
    #     df2 = df.loc[df['fld_Duplication_Index'].notnull(), ['fldObjectUID', 'fld_Duplication_Index']]
    #     dicto2 = dict(zip(df2['fldObjectUID'], df2['fld_Duplication_Index']))
    #     if any(df['fld_Duplication_Index'].notnull()):
    #         df_dupl = df.loc[df['fld_Duplication_Index'].notnull()].groupby('fld_Duplication_Index')['fldObjectUID']. \
    #             agg(set)
    #         if not isinstance(cls._dupl_series, pd.DataFrame) or any(df_dupl != cls._dupl_series):
    #             cls._dupl_series = df_dupl.copy()
    #     # dicto2_old = {k: v for k, v in dicto1.items() if pd.notnull(v)}  # dict with None values stripped off.
    #     # if dicto2:
    #     #     duplic_values = list(dicto2.values())
    #     #     for item in set(duplic_values):     # item is a _Duplication_Index value.
    #     #         if duplic_values.count(item) > 1:
    #     #             # If item appears more than once in col_duplication_index, it's a duplicate item.
    #     #             # Gets all the uids associated to _Duplication_Index for item.
    #     #             uid_list = [col_uid[j] for j, val in enumerate(col_duplication_index) if val == item]
    #     #             # ONLY DUPLICATE ITEMS HERE (_Duplication_Index count > 1), to make search more efficient.
    #     #             temp_dict[item] = tuple(uid_list)
    #
    #     # With _access_serializer clear all other threads running this code will sit at the wait() line until the
    #     # copy action to cls._active_uids_df and the update of cls._active_duplication_index_dict are complete.
    #     cls._active_uids_df = df.copy()  # SHARED resource. Accessed from background threads.
    #     # if temp_dict:
    #     #     # Empty _active_duplication_index_dict -> there's NO duplicates for that uid in the db table. (99% of cases)
    #     #     cls._active_duplication_index_dict = temp_dict
    #     # with cls.__lock:
    #     cls._access_serializer.set()  # This will end the wait of other threads. set is the normal state of _event.
    #
    #     # identifs_dict = dict(zip(col_uid, df["fldIdentificadores"].fillna(np.nan).replace([np.nan], [None]).tolist()))
    #     # with cls.__lock:
    #     #     cls._active_uids_df = df.copy()  # THIS IS A SHARED DICT!!
    #     #     # {fldObjectUID: fldIdentfiers}. Placed here for any uses required.
    #     #     setattr(cls, '_identifiers_dict', identifs_dict)
    #     # print(f"IDENTIFIERS Bovine: {getattr(cls, '_identifiers_dict', identifs_dict)}")
    #
    #     return
    #
    # @classmethod
    # def _init_uid_dicts01(cls):     # TODO(cmt): Intermediate version: uses df but with dicts as memory structures.
    #     """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
    #     Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
    #     This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
    #     _processDuplicates() run by background functions.
    #     Uses a checksum of _active_uids_df.values() to determine if there are changes to the dict.
    #     @return: None
    #     """
    #     # This sql will bring a fresh snapshot of all active objects in the database.
    #     sql = f'SELECT * from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" IS NULL ' \
    #           f'OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ' \
    #           f'AND "{getFldName(cls.tblObjName(), "fldFK_ClaseDeAnimal")}" == {cls.animalClassID()}; '
    #     # temp = dbRead(cls.tblObjName(), sql)
    #     df = pd.read_sql_query(sql, SQLiteQuery().conn)
    #     if df.empty:
    #         val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()} internal uid_dicts no initialized. " \
    #               f"System cannot operate."
    #         krnl_logger.warning(val)
    #         raise sqlite3.DatabaseError(val)
    #
    #     col_uid = df['fldObjectUID'].tolist()
    #     col_duplication_index = df['fld_Duplication_Index'].replace(
    #         {np.nan: None, float('nan'): None, pd.NA: None}).tolist()
    #     dicto1 = dict(zip(col_uid, col_duplication_index))  # Reference dic
    #
    #     # Check for differences in dict values. Must update _active_uids_df,  _active_duplication_index_dict.
    #     current_checksum = getattr(cls, '_duplic_index_checksum', 0)
    #     try:
    #         """ There is a long-shot chance that the checksum below fails when 2 or more items change and the
    #             resulting sum remains unaltered. Then, there's an efficiency-driven bet that such a scenario will
    #             NEVER materialize. IF it ever does, it will correct itself next time the checksum changes, and
    #             all db records will be properly updated and re-assigned. """
    #         checksum = sum([UUID(j).int for j in col_duplication_index if isinstance(j, str)])
    #     except(TypeError, ValueError):
    #         checksum = current_checksum  # On checksum failure, must exit with checksums unchanged.
    #
    #     temp_dict = {}
    #     if checksum != cls._duplic_index_checksum:
    #         # Initializes __active_Duplication_Index_dict ONLY FOR DUPLICATE uids.
    #         # An EMPTY _active_duplication_index_dict means there's NO duplicates for that uid in the db table.
    #         setattr(cls, '_duplic_index_checksum', checksum)  # Updates checksum.
    #         dicto2 = {k: v for k, v in dicto1.items() if v is not None}  # dict with None values stripped off.
    #         duplic_values = list(dicto2.values())
    #         for item in set(duplic_values):  # item is a _Duplication_Index value.
    #             if duplic_values.count(item) > 1:
    #                 # If item appears more than once in col_duplication_index, it's a duplicate item.
    #                 # Gets all the uids associated to _Duplication_Index for item.
    #                 uid_list = [col_uid[j] for j, val in enumerate(col_duplication_index) if val == item]
    #                 # ONLY DUPLICATE ITEMS HERE (_Duplication_Index count > 1), to make search more efficient.
    #                 temp_dict[item] = tuple(uid_list)
    #
    #     if temp_dict:  # There are changes in _Duplication_Index values. Must update both dicts.
    #         with cls.__lock:  # Both must be updated in the lock block.
    #             cls._active_uids_df = dicto1
    #             cls._active_duplication_index_dict = temp_dict
    #     else:
    #         if set(dicto1) != set(getattr(cls, '_active_uids_df').keys()):
    #             # Changes in keys only.Updates _active_uids_df, _identfiers_dict.
    #             identifs_dict = dict(zip(col_uid, df["fldIdentificadores"].replace(
    #                 {np.nan: None, float('nan'): None, pd.NA: None}).tolist()))
    #             with cls.__lock:
    #                 setattr(cls, '_active_uids_df', dicto1)  # THIS IS A SHARED DICT!!
    #                 # {fldObjectUID: fldIdentfiers}. Placed here for any uses required.
    #                 setattr(cls, '_identifiers_dict', identifs_dict)
    #             print(f"IDENTIFIERS Bovine: {getattr(cls, '_identifiers_dict', identifs_dict)}")
    #     return None

    # @classmethod
    # def _init_uid_dicts00(cls):             # Uses DataTable. Deprecated.
    #     """   ***** This method MUST support multiple access. In particular, WILL NOT remove items from dicts *****
    #     Initializes uid dicts upon system start up and when the dict_update flag is set for the class by SQLite.
    #     This method is run in the __init_subclass__() routine in EntityObject class during system start, and by
    #     _processDuplicates() run by background functions.
    #     Uses a checksum of _active_uids_df.values() to determine if there are changes to the dict.
    #     @return: None
    #     """
    #     # This sql will bring a fresh snapshot of all active objects in the database.
    #     sql = f'SELECT * from "{cls.tblObjDBName()}" WHERE ("{getFldName(cls.tblObjName(), "fldDateExit")}" IS NULL ' \
    #           f'OR "{getFldName(cls.tblObjName(), "fldDateExit")}" == 0) ' \
    #           f'AND "{getFldName(cls.tblObjName(), "fldFK_ClaseDeAnimal")}" == {cls.animalClassID()}; '
    #     temp = dbRead(cls.tblObjName(), sql)
    #     if not isinstance(temp, DataTable):
    #         val = f"ERR_DBAccess cannot read from table {cls.tblObjDBName()}. Internal uid_dicts no initialized. " \
    #               f"System cannot operate."
    #         krnl_logger.warning(val)
    #         raise DBAccessError(val)
    #     if temp:
    #         idx_uid = temp.getFldIndex("fldObjectUID")
    #         col_uid = [j[idx_uid] for j in temp.dataList]  # temp.getCol("fldObjectUID")
    #         idx_dupl = temp.getFldIndex("fld_Duplication_Index")
    #         col_duplication_index = [j[idx_dupl] for j in temp.dataList]  # temp.getCol("fld_Duplication_Index")
    #         dicto1 = dict(zip(col_uid, col_duplication_index))  # Reference dict with all values
    #
    #         # Check for differences in dict values. Must update _active_uids_df,  _active_duplication_index_dict.
    #         try:
    #             """ There is a long-shot chance that the checksum below fails when 2 or more items change and the
    #             # resulting sum remains unaltered. Then, there's an efficiency-driven bet that such a scenario will
    #             # NEVER materialize. IF it ever does, it will correct itself next time the checksum changes, and
    #             # all db records will be properly updated and re-assigned. """
    #             checksum = sum([UUID(j).int for j in col_duplication_index if isinstance(j, str)])
    #         except(TypeError, ValueError):
    #             checksum = cls._duplic_index_checksum  # On checksum failure, must exit with checksums unchanged.
    #         temp_dict = {}
    #         if checksum != cls._duplic_index_checksum:
    #             # Initializes __active_Duplication_Index_dict ONLY FOR DUPLICATE uids.
    #             # An EMPTY _active_duplication_index_dict means there's NO duplicates for that uid in the db table.
    #             cls._duplic_index_checksum = checksum
    #             dicto2 = {k: v for k, v in dicto1.items() if v is not None}  # dict with None values stripped off.
    #             # for item in col_duplication_index:              # item is a _Duplication_Index value.
    #             duplic_values = list(dicto2.values())
    #             for item in duplic_values:
    #                 if item is not None and duplic_values.count(item) > 1:
    #                     # If item appears more than once in col_duplication_index, it's a duplicate item.
    #                     # Gets all the uids associated to _Duplication_Index for item.
    #                     uid_list = [col_uid[j] for j, val in enumerate(col_duplication_index) if val == item]
    #                     # ONLY DUPLICATE ITEMS HERE (_Duplication_Index count > 1), to make search more efficient.
    #                     temp_dict[item] = tuple(uid_list)
    #
    #         if temp_dict:  # There are changes in _Duplication_Index values. Must update both dicts.
    #             with cls.__lock:  # Both must be updated in the lock block.
    #                 cls._active_uids_df = dicto1
    #                 cls._active_duplication_index_dict = temp_dict
    #         else:
    #             # If there's differences in dict keys only -> Initializes / updates _active_uids_df only.
    #             if set(dicto1) != set(getattr(cls, '_active_uids_df').keys()):
    #                 # Changes in keys only.Updates _active_uids_df, _identifiers_dict.
    #                 idx_identifs = temp.getFldIndex("fldIdentificadores")
    #                 col_identifs = [j[idx_identifs] for j in temp.dataList]
    #                 identifs_dict = dict(zip(col_uid, col_identifs))  # Tags: identifiers is a string (1 identifier)
    #                 with cls.__lock:
    #                     setattr(cls, '_active_uids_df', dicto1)  # THIS IS A SHARED DICT!!
    #                     # {fldObjectUID: fldIdentfiers}. Placed here for any uses required.
    #                     setattr(cls, '_identifiers_dict', identifs_dict)
    #                 print(f"IDENTIFIERS Bovine: {getattr(cls, '_identifiers_dict', identifs_dict)}")
    #     return None

    @classmethod
    def get_active_uids_iter(cls):
        """ OJO!: Returns dataframe iterator. Thread-safe implementation ."""
        return cls.obj_dataframe()


    @classmethod
    def get_active_uids(cls):
        return cls.obj_dataframe()

    __timeoutEvent = threading.Event()  # Events for class Bovine to communicate with bkgd functions.
    __categoryEvent = threading.Event()
    __allAssignedTags = {}  # dict {tagID: tagObject, }. All tags assigned to animals of a given class.

    @classmethod
    def registerKindOfAnimal(cls):
        """ Registers class Bovine in __registeredClasses dict. Method is run by EntityObject.__init_subclass__()"""
        Animal.getAnimalClasses()[cls] = cls._kindOfAnimalID

    __bkgdAnimalTimeouts = []   # stores objects that had timeout changes made by background threads.
    __bkgdCategoryChanges = {}  # stores objects that had category changes made by bckgd threads: {obj: newCategory,}

    @classmethod
    def getBkgdAnimalTimeouts(cls):
        return cls.__bkgdAnimalTimeouts

    @classmethod
    def getBkgdCategoryChanges(cls):
        return cls.__bkgdCategoryChanges

    @classmethod
    def timeoutEvent(cls):
        return cls.__timeoutEvent

    @classmethod
    def categoryEvent(cls):
        return cls.__categoryEvent

    # temp = getRecords('tblAnimalesActividadesNombres', '', '', None, 'fldID', 'fldName', 'fldFlag',
    #                      'fldFK_ClaseDeAnimal')
    temp0 = getrecords('tblAnimalesActividadesNombres', 'fldID', 'fldName', 'fldFlag', 'fldFK_ClaseDeAnimal')
    temp0 = temp0[temp0['fldFK_ClaseDeAnimal'].isin([0, _kindOfAnimalID])]
    # __bovineActivitiesDict = {}  # {fldNombreActividad: fldID_Actividad, }
    # __isInventoryActivity = {}          # _isInventoryActivity = {fldNombreActividad: _isInventoryActivity (1/0),}

    __bovineActivitiesDict = dict(zip(temp0['fldName'], temp0['fldFK_ClaseDeAnimal']))
    __isInventoryActivity = dict(zip(temp0['fldName'], temp0['fldFlag']))

    # for j in range(temp.dataLen):
    #     if temp.dataList[j][3] == 0 or temp.dataList[j][3] == _kindOfAnimalID:
    #         # 0: Define en DB una Actividad que aplica a TODAS las clases de Animales
    #         __bovineActivitiesDict[temp.dataList[j][1]] = temp.dataList[j][0]       # {activityName: activityID, }
    #         __isInventoryActivity[temp.dataList[j][1]] = temp.dataList[j][2]
    del temp0


    @classmethod
    def getActivities(cls):
        """ class method to return activities dictionary"""
        return cls.__bovineActivitiesDict   # {fldNombreActividad: fldID_Actividad,}

    _myActivityClass._activities_dict = __bovineActivitiesDict      # Initializes activities dict in Activity class.

    @property
    def activities(self):           # @property activities es necesario para las llamadas a obj.outerObject.
        return self.__bovineActivitiesDict

    # def register(self):
    #     """
    #     Creates entry for object in __memory_data dictionary.
    #     Adds uid to _active_uids_df. Updates _active_duplication_index_dict if required.
    #     @return: Inserted object. None if one tries to register a Generic or External animal.
    #     """
    #     if self.mode.lower() in ('regular', 'substitute', 'dummy'):  # POR AHORA, generic y external no van
    #         pass
    #
    #     return None
    #
    # def unRegister(self):  # Remueve obj .__registerDict.
    #     """
    #     Removes object entry from __memory_data dictionary.
    #     @return: removed object ID if successful. None if object not found in dict.
    #     """
    #     duplicat_index = self._active_uids_df.pop(self.ID)
    #     if duplicat_index:
    #         self._active_duplication_index_dict.pop(duplicat_index)
    #     return self.get_memory_data().pop[self.ID]              # TODO: INCOMPLETE. Fix this method.


    @classmethod
    def getActivitiesDict(cls):
        return cls.__bovineActivitiesDict

    @classmethod
    def getInventoryActivitiesDict(cls):
        return cls.__isInventoryActivity

    # temp1 = getRecords('tblAnimalesCategorias', '', '', None, '*', fldFK_ClaseDeAnimal=_kindOfAnimalID)
    wherestr = f'WHERE "{getFldName("tblAnimalesCategorias", "fldFK_ClaseDeAnimal")}" == "{_kindOfAnimalID}"'
    dftemp1 = getrecords('tblAnimalesCategorias', '*', where_str=wherestr)
    # {Category Name: ID_Categoria, } List of ALL Categories for animalClassID
    __categories = d2 = dict(zip(dftemp1['fldName'], dftemp1['fldID']))
    __categoryID_mf_dict = dict(zip(dftemp1['fldID'], dftemp1['fldMF']))
    __categoryName_mf_dict = dict(zip(dftemp1['fldName'], dftemp1['fldMF']))

    @property
    def categories(self):
        return self.__categories             # {Category Name: ID_Categoria, }

    @classmethod
    def getCategories(cls):
        """ Class access for Animal categories. """
        return cls.__categories              # {Category Name: ID_Categoria, }


    @classmethod
    def get_mf_from_cat(cls, category):
        """Returns male/female from a category value.
       @param: category: str or int: Animal category.
       return: str: 'm' or 'f'. If category is invalid for cls, returns None.
       """
        if isinstance(category, str):
            return cls.__categoryName_mf_dict.get(category.strip().lower(), None)
        return cls.__categoryID_mf_dict.get(category, None)     # not string, assumes it's int or numpy.int64


    __edadLimiteTernera = fetchAnimalParameterValue('Edad Limite Ternera')
    __edadLimiteVaquillona = fetchAnimalParameterValue('Edad Limite Vaquillona')
    __edadLimiteVaca = fetchAnimalParameterValue('Edad Limite Vaca')
    __edadLimiteTernero = fetchAnimalParameterValue('Edad Limite Ternero')
    __edadLimiteTorito = fetchAnimalParameterValue('Edad Limite Torito')
    __edadLimiteNovillito = fetchAnimalParameterValue('Edad Limite Novillito')
    __edadLimiteNovillo = fetchAnimalParameterValue('Edad Limite Novillo')
    __edadLimiteToro = fetchAnimalParameterValue('Edad Limite Toro')
    __setNovilloByAge = 0  # TODO: leer este parametro de DB.
    __AGE_LIMIT_BOVINE = 20 * 365  # TODO: leer este parametro de DB.


    @classmethod
    def novilloByAge(cls, val=None):
        """ getter/setter for this attribute"""
        if val is not None:
            cls.__setNovilloByAge = bool(val)
        return cls.__setNovilloByAge


    def __init__(self, *args, **kwargs):
        try:
            kwargs = self.validate_arguments(kwargs)
        except (TypeError, ValueError) as e:
            krnl_logger.info(f'{e} - Object not created!.')
            # del self  # Removes invalid/incomplete Bovine object
            return

        mode = next((str(kwargs[j]).lower().strip() for j in kwargs if 'mode' in str(j).lower()), None)
        if 'generic' in mode.lower():
            retValue = 'ERR_Inp_InvalidObject: Generic Animals can only have one instance. Object not created'
            krnl_logger.error(retValue)
            raise ValueError(retValue)          # Solo 1 objeto Generic se puede crear.

        self.__flagCastrado = kwargs.get('fldFlagCastrado')
        self.__comment = kwargs.get('fldComment', '')
        super().__init__(*args, **kwargs)


    @staticmethod
    def validate_arguments(argsDict):
        # Animal Category is not required here. It will be derived from M/F, Castration status and age for each object.
        # DOB required for Bovines. Other Animals may not require dob.
        dob = next((argsDict[k] for k in argsDict if 'flddob' in str(k).lower()), '')
        if not isinstance(dob, datetime):
            try:
                dob = datetime.strptime(dob, fDateTime)
            except(TypeError, ValueError):
                err_str = f'ERR_INP_Invalid or missing argument DOB: {dob}'
                print(err_str, dismiss_print=DISMISS_PRINT)
                raise ValueError(f'{err_str} - {moduleName()}({lineNum()})')

        key, castr = next(((k, argsDict[k]) for k in argsDict if 'castrad' in k.lower()), (None, 0))
        if castr in (0, 1, True, False, None):          # 1: Castrated, but castration date not known.
            argsDict[key] = bool(castr) * 1  # Si fldFlagCastrado no esta en kwargs, se crea y setea a 0
        else:
            argsDict[key] = valiDate(castr, 0)  # verifica si es fecha. Si fecha no es valida, asume NO Castrado (0).
        key, mode = next(((k.lower(), str(argsDict[k]).lower()) for k in argsDict if 'mode' in str(k).lower()),
                         (None, 'regular'))         # if mode not passed defaults to 'regular' Animal.
        if mode not in Animal.getAnimalModeDict():
            raise ValueError(f'ERR_UI_InvalidArgument: Animal Mode {mode} not valid. - '
                             f'{moduleName()}({lineNum()})')
        else:
            argsDict[key] = mode
            return argsDict


