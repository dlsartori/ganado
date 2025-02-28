from krnl_tag import Tag
from krnl_tag_activity_by_tech import TagStandardActivity, TagRFIDActivity, TagLoRaActivity, TagBluetoothActivity, \
    TagSatelliteActivity, TagTatooActivity

TAGS_TECH_VERSION = 1.0
TAGS_TECH_CREATED_DATE = "2024-06-26 14:30:30"         # Data for module import purposes.


class TagStandard(Tag):
    """ Implements Abstract Class TagStandard to link tag technology type (Standard, RFID, etc) with the attributes and
    methods specific to the technology
    """
    __tech = 'standard'  # String defining Tag technology. Must be equal to value stored in table [Caravanas Tecnologia]
    # __add_to_register = True  # Signals class must be initialized by __init_subclass__() method in Tag class.
    __activityClass = TagStandardActivity    # Activity objects created in EO.__init_subclass__() MUST be of this type.

    @classmethod
    def myActivityClass(cls):
        return cls.__activityClass

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TagRFID(Tag):
    """ Implements Abstract Class TagRFID to link tag technology type (Standard, RFID, etc) with the attributes and
    methods specific to the technology
    """
    __tech = 'rfid'  # String defining Tag technology. Must be equal to value stored in table [Caravanas Tecnologia]
    # __add_to_register = True  # Signals class must be initialized by __init_subclass__() method in Tag class.
    __activityClass = TagRFIDActivity   # Activity objects created in EO.__init_subclass__() MUST be of this type.

    @classmethod
    def myActivityClass(cls):
        return cls.__activityClass

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TagLORA(Tag):
    """ Implements Abstract Class TagLORA to link tag technology type (Standard, RFID, etc) with the attributes and
    methods specific to the technology
    """
    __tech = 'lora'  # String defining Tag technology. Must be equal to value stored in table [Caravanas Tecnologia]
    # __add_to_register = True  # Signals class must be initialized by __init_subclass__() method in Tag class.
    __activityClass = TagLoRaActivity   # Activity objects created in EO.__init_subclass__() MUST be of this type.

    @classmethod
    def myActivityClass(cls):
        return cls.__activityClass

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TagBluetooth(Tag):
    """ Implements Abstract Class TagBluetooth to link tag technology type (Standard, RFID, etc) with the attributes and
    methods specific to the technology
    """
    __tech = 'bluetooth'  # String defining Tag technology.Must be equal to value stored in table [Caravanas Tecnologia]
    # __add_to_register = True  # Signals class must be initialized by __init_subclass__() method in Tag class.
    __activityClass = TagBluetoothActivity  # Activity objects created in EO.__init_subclass__() MUST be of this type.

    @classmethod
    def myActivityClass(cls):
        return cls.__activityClass


    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TagSatellite(Tag):
    """ Implements Abstract Class TagSatellite to link tag technology type (Standard, RFID, etc) with the attributes and
    methods specific to the technology
    """
    __tech = 'satellite'  # String defining Tag technology.Must be equal to value stored in table [Caravanas Tecnologia]
    # __add_to_register = True  # Signals class must be initialized by __init_subclass__() method in Tag class.
    __activityClass = TagSatelliteActivity  # Activity objects created in EO.__init_subclass__() MUST be of this type.

    @classmethod
    def myActivityClass(cls):
        return cls.__activityClass


    def __init__(self, **kwargs):
        super().__init__(**kwargs)

class TagTatoo(Tag):
    """ Implements Abstract Class TagTatoo to link tag technology type (Standard, RFID, etc) with the attributes and
    methods specific to the technology
    """
    __tech = 'tatoo'  # String defining Tag technology.Must be equal to value stored in table [Caravanas Tecnologia]
    _add_to_register = True  # Signals class must be initialized by __init_subclass__() method in Tag class.
    __activityClass = TagTatooActivity

    @classmethod
    def myActivityClass(cls):
        return cls.__activityClass


    def __init__(self, **kwargs):
        super().__init__(**kwargs)

