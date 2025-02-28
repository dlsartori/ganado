from krnl_tag_activity import TagActivity
from krnl_config import singleton

# General definitions for methods and attributes specific to tag technologies.

class TagStandardActivity(TagActivity):
    """ Implements methods and attributes specific to TagStandard. Inherits all common data from parent. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
# ----------------------------------- End TagStandardActivity subclasses ------------------------------------------ #

class TagRFIDActivity(TagActivity):
    """ Implements methods and attributes specific to TagRFID. Inherits all common data from parent. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

@singleton              # ********** This is an example *************************
class ChipReadoutTagRFIDActivity(TagActivity):
    # Class Attributes: Tablas que son usadas por todas las instancias de CommissionActivityTag
    __activityName = 'Readout'
    __method_name = 'readout'     # __method_name enables the creation of a singleton Activity object for the class.

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # Activities definition continues here...
    # TODO: to create Activity objects, EO routine will scan the _activity_class_register and create objects for all
    #  those classes that are an instance of TagRFIDActivity.
# ----------------------------------- End TagRFIDActivity subclasses ------------------------------------------ #



class TagLoRaActivity(TagActivity):
    """ Implements methods and attributes specific to TagLoRa. Inherits all common data from parent. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)




class TagBluetoothActivity(TagActivity):
    """ Implements methods and attributes specific to TagBluetooth. Inherits all common data from parent. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)



class TagSatelliteActivity(TagActivity):
    """ Implements methods and attributes specific to TagBSatellite. Inherits all common data from parent. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)




class TagTatooActivity(TagActivity):
    """ Implements methods and attributes specific to TagTatoo. Inherits all common data from parent. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


