from krnl_assetItem import AssetItem
from custom_types import getRecords, DataTable
GENERIC_DEVICE_ID = 1


class Device(AssetItem):                                # Dispositivos
    # objClass = 40
    # objType = 1
    #

    __registerDict = {}

    @classmethod
    def getRegisterDict(cls):
        return cls.__registerDict

    __tblObjectsName = 'tblDispositivos'

    # @classmethod
    # def getTblObjectsName(cls):
    #     return cls.__tblObjectsName

    @classmethod
    def tblObjName(cls):
        return cls.__tblObjectsName


    def __init__(self, ID_Device, isValid, isActive):
        AssetItem.__init__(self, ID_Device, isValid, isActive)


    def processReplicated(cls):        # TODO(cmt): Called by Bovine, Caprine, etc.
        """             ******  Run periodically as IntervalTimer func. ******
                        ******  IMPORTANT: This code should execute in LESS than 5 msec (switchinterval).      ******
        Used to execute logic for detection and management of INSERTed, UPDATEd and duplicate objects.
        Defined for Animal, Tag, Person, Geo.
        Checks for additions to tblAnimales from external sources (replication from other nodes) for Valid and
        Active objects. Updates _fldID_list, _object_fldUPDATE_dict
        @return: True if update operation succeeds, False if reading tblAnimales from db fails.
        """
        temp = getRecords(cls.tblObjName(), '', '', None, '*', )
        if not isinstance(temp, DataTable):
            return False
        # TODO: COMPLETE THIS METHOD!!!

    processReplicated = classmethod(processReplicated)