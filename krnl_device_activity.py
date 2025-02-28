from krnl_abstract_class_activity import *
from krnl_geo import Geo
from krnl_tm import MoneyActivity, handlerTM
from krnl_device import Device


class DeviceActivity(Activity):
    __tblRAName = 'tblDispositivosRegistroDeActividades'
    __tblObjName = 'tblDispositivos'  # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
    __tblLinkName = 'tblLinkDispositivosActividades'
    __supportsPA = {}

    # Lists all Activity classes that support memory data, for access and initialization.
    _memory_data_classes = set()  # Initialized on creation of Activity classes. Defined here to include all Activities.


    def __init__(self, isValid, activityName=None, activityID=None, invActivity=None, enableActivity=activityEnableFull,
                 *args, **kwargs):

        if kwargs.get('supportsPA') is None:
            # Si no hay override desde abajo, setea al valor de __supportsPA{} para ese __activityName.
            kwargs['supportsPA'] = self.__supportsPA.get(activityName)
        super().__init__(isValid, activityName, activityID, invActivity, enableActivity,
                         self.__tblRAName, *args, **kwargs)

