import threading
from krnl_tag import Tag
from krnl_config import os
from krnl_db_query import AccessSerializer, AccessSerializer, getTblName
from krnl_config import PANDAS_READ_CHUNK_SIZE
import pandas as pd
def moduleName():
    return str(os.path.basename(__file__))


class TagPerson(Tag):
    """ Implements Abstract Class TagPerson to provide support
    Tags, in Persons, are defined as any type of ID that identifies the person. As such, Passports, DNI, CUIT/CUIL
    and any other form of Person id or Tax id are defined and handled as a tag objects in the system.
    """
    __tblObjectsName = 'tblCaravanasPersonas'
    __tblObjDBName = getTblName(__tblObjectsName)
    @classmethod
    def tblObjName(cls):
        return cls.__tblObjectsName

    @classmethod
    def tblObjDBName(cls):
        return cls.__tblObjDBName


    _active_uids_df = {}
    _dupl_series = pd.Series([], dtype=object)
    _chunk_size = 300  # PANDAS_READ_CHUNK_SIZE,       # This value is specific to each class.
    _object_mem_fields = ('*', )        # All fields in tblCaravanasPersonas, to be able to create object from memory.
    # _object_mem_fields = ('fldID', 'fldObjectUID', 'fld_Duplication_Index', 'fldTagNumber',  'fldFK_TagType',
    #                       'fldIdentificadores', 'fldAssignedToClass', 'fldPhysicallyAttached', 'fldTimeStamp')
    _access_serializer = AccessSerializer()     # keeps access_count to init_uid_dicts() code.
    _sem_obj_dataframe = threading.BoundedSemaphore()  # Semaphore specific to _active_uids_df for each class.
    _tagObjectsClasses = {}  # {tagTech(str): <Child (object) class>, } ex: {TagStandard: <TagStandardBovine>}

    def __init__(self, *args, **kwargs):
        super().__init__(* args, **kwargs)
