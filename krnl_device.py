from krnl_assetItem import AssetItem
from krnl_custom_types import getRecords, DataTable, setRecord
from uuid import UUID

GENERIC_DEVICE_ID = 1


class Device(AssetItem):                                # Dispositivos
    # objClass = 40
    # objType = 1
    #

    __tblObjectsName = 'tblDispositivos'
    __subclass_register = set()  # Stores Device subclasses -> {class_obj, }

    # Initialized in EntityObject.__init_subclass__(). Used for memory_data support.
    __dictofdicts = {}  # {Device: uids_device, SubTagClass: uids_sub_device, }


    # __init_subclass() is used to register dynamically added Device subclasses, in particular when new device types
    # modules are added to the system.
    # This code executes after the subclasses complete their creation code, WITHOUT any object instantiation. Beleza.
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if getattr(cls, '_' + cls.__name__ + '__add_to_register', None):
            cls.register_class()

    @classmethod
    def register_class(cls):
        cls.__subclass_register.add(cls)

    @classmethod
    def __unregister_class(cls):  # Hidden method(). unregistering a class only managed with proper privileges.
        cls.__subclass_register.discard(cls)

    @classmethod
    def get_sub_classes(cls):
        return cls.__subclass_register.copy()

    @classmethod
    def tblObjName(cls):
        return cls.__tblObjectsName


    def __init__(self, ID_Device, isValid, isActive):

        self.__identifiers = set()          # set of Tag identifiers for device.
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

    # def setIdentifier(self, identifier):
    #     """ Animal identifier. Adds an identifier to the __identifiers set, and updates Animales db table.
    #     Function defined only for dynamic identifiers. NOT defined for Tags, Geo that have single, unmutable identifiers
    #     Three data structures to update:
    #     1. Object (self.__identifiers)
    #     2. Database record (fldIdentificadores in tblObject()).
    #     3. __identifiers dict (if defined for the class).
    #     In the case of Animals as this one, Identifier must be a valid UUID or a string that converts to UUID.
    #     @return: New identifier for object (set)."""
    #     ident = None
    #     if isinstance(identifier, UUID):
    #         ident = identifier.hex
    #     elif isinstance(identifier, str):
    #         try:
    #             ident = UUID(identifier).hex
    #         except (SyntaxError, TypeError, ValueError):
    #             return None
    #     if ident:
    #         self.__identifiers.add(ident)   # Each Device identifiers is str. __identifiers is a set of those strings.
    #         _ = setRecord(self.tblObjName(), fldID=self.recordID, fldIdentificadores=self.__identifiers)
    #         # Updates _identifiers_dict (if they exist) for self.
    #         try:
    #             self.update_identfiers_dict(self.__identifiers)
    #         except AttributeError:
    #             pass
    #     return self.__identifiers


    # def removeIdentifier(self, ident=None):
    #     """ Three data structures to update:
    #     1. Object (self.__identifiers)
    #     2. Database record (fldIdentificadores in tblObject()).
    #      3. __identifiers dict (if defined for the class).
    #     @param ident: Identifier to remove from set.
    #     @return: True if removed. None if not found/not removed.
    #     """
    #     if ident in self.__identifiers and hasattr(self.__identifiers, "__iter__"):
    #         self.__identifiers.discard(ident)
    #         _ = setRecord(self.tblObjName(), fldID=self.recordID, fldIdentificadores=self.__identifiers)
    #         try:
    #             self.update_identfiers_dict()[self.ID] = self.__identifiers
    #         except AttributeError:
    #             pass
    #         return ident
    #     return None
