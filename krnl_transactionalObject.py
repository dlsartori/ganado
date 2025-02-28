from __future__ import annotations          # Used to define _get_duplicate_uids()
from os import path
from uuid import UUID
from krnl_config import krnl_logger
from krnl_custom_types import DataTable, getRecords, DBTrigger


def moduleName():
    return str(path.basename(__file__))

class TransactionalObject(object):
    __objClass = 100
    __objType = 2

    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, '_initialize'):
            cls._initialize()               # Inicializa clases que definan metodo _initialize()
        super().__init_subclass__()

        # TODO(cmt): THESE ARE THE NEW TRIGGERS. Initializes all db triggers defined for class cls, if any.
        # Name mangling required here to prevent inheritance from resolving to wrong data.
        triggers_list = getattr(cls, '_' + cls.__name__ + '__db_triggers_list', None)
        if triggers_list:
            for j in triggers_list:
                if isinstance(j, (list, tuple)) and len(j) == 2:  # j=(trigger_name, trigger_processing_func (callable))
                    _ = DBTrigger(trig_name=j[0], process_func=j[1], calling_obj=cls)
            print(f'CREATED Triggers for {cls}: {str(tuple([tr.name for tr in DBTrigger.get_trigger_register()]))}.')
        # Initializes _active_uid_dict in the respective subclasses. Used to manage duplication in Animales,Caravanas,
        # Geo, Device, etc. Name mangling needed to access ONLY classes that define _active_uids_df dictionaries.
        if getattr(cls, '_active_uids_df', None) is not None:
            # Calls ONLY for classes with _active_uids_df defined (Bovine, Caprine, etc) and NOT the parent classes.
            # Also initializes dictionary {class: Duplication_index, } for all classes that implement duplication
            # classes_with_duplication[cls] = cls._processDuplicates
            # try:
            cls._init_uid_dicts()  # executes only for classes that define _init_uid_dicts.

        """IMPORTANT! this one MUST GO AFTER _init_uid_dicts(). The call below is deprecated. Use NEW ONE """
        mem_data_classes = cls._myActivityClass._memory_data_classes if hasattr(cls, '_myActivityClass') else None
        if mem_data_classes:
            # with ThreadPoolExecutor(max_workers=len(mem_data_classes) + 1) as executor:
            for c in mem_data_classes:
                # Initializes local uids dicts for c and parent classes. Appends dict ONLY to classes
                # that support_mem_data()
                # TODO(cmt): Activities DON'T need to wait for these threads to complete. They operate normallly
                # Activity._futures[c].append(executor.submit(c._memory_data_init_last_rec, cls,
                #                                             cls.get_active_uids_iter()))
                c._memory_data_init_last_rec(cls, cls.get_active_uids_dict())    # Non-threaded call.
            f = [c for c in mem_data_classes if not c.is_ready()]
            if f:           # Warns if any mem_data initializers are not yet finished.
                krnl_logger.info(f'Activity {tuple(f)}  NOT READY while running thread launcher loop.')
            # print(f'\n============= Futures dict Transactional Objects ({cls.__name__}): {Activity._futures}.')
            # print(f"MEMORY DATA UID DICTS: {getattr(c, '_' + c.__name__ + '__local_active_uids_dict', None)}")

    @classmethod
    def get_active_uids_dict(cls):
        """ OJO!: Returns the full dict for compatibility and out of coding needs. This dict MUST NEVER be modified. """
        # return getattr(cls, '_active_uids_df', None)  # {animal_uid: duplication_index }
        return cls.obj_dataframe()['fldOjbectUID'].tolist()



    @classmethod
    def get_dupl_index_checksum(cls):
        """ _duplic_index_checksum getter """
        return getattr(cls, '_duplic_index_checksum', None)

    # @classmethod
    # def set_dupl_index_checksum(cls, val):
    #     """ _duplic_index_checksum setter """
    #     setattr(cls, '_duplic_index_checksum', val)

    @classmethod
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
                uid = uid if isinstance(uid, UUID) else UUID(uid.strip())
            except (SyntaxError, AttributeError, TypeError):
                return None
            uid = uid.hex  # converts to str for proper search in dictionaries.
            # Pulls the right _active_uids_df for cls.
            duplication_index = getattr(cls, '_active_uids_df', 0).get(uid)
            if duplication_index is not None:
                if all_duplicates:
                    dupl_idx_dict = getattr(cls, '_active_duplication_index_dict', {})
                    # if duplication_index in dupl_idx_dict:
                    return tuple(dupl_idx_dict.get(duplication_index, []))
                return duplication_index  # Returns single value (a UUID.hex or None)
        return None

    def __init__(self):
        super().__init__()


    temp = getRecords('tblObjetosTiposDeTransferenciaDePropiedad', '', '', None, 'fldID', 'fldName')
    __ownershipXferDict = {}
    if isinstance(temp, DataTable) and temp.dataLen > 0 and len(temp.dataList[0]) > 0:
        for j in range(temp.dataLen):
            __ownershipXferDict[temp.dataList[j][1]] = str(temp.dataList[j][0])  # {NombreTransfProp: ID_TransfProp, }

    @property
    def ownershipXferDict(self):
        return self.__ownershipXferDict

        # Diccionario Geo.Entidades. Diccionario de Diccionarios con definiciones de las Localizaciones en Sistema
    temp = getRecords('tblGeoEntidades', '', '', None, '*')
    if isinstance(temp, DataTable) and temp.dataLen and temp.dataList[0]:
        __localizationsDict = {}  # {statusName: [statusID, activeYN]}
        for j in range(temp.dataLen):
            rowDict = temp.unpackItem(j)
            key = rowDict['fldID']
            rowDict.pop('fldID')
            __localizationsDict[key] = rowDict
    else:
        raise ImportError(f' ERR_Sys_DBCannotRead table tblGeoEntidades. Exiting...')


    @property
    def localizationsDict(self):
        return TransactionalObject.__localizationsDict
