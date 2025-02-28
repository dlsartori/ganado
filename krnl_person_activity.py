from krnl_abstract_class_activity import *
from krnl_custom_types import setRecord
from krnl_geo_old import Geo
from krnl_tm import MoneyActivity
from krnl_person import Person


@singleton
class PersonActivity(Activity):
    __tblRAName = 'tblPersonasRegistroDeActividades'
    __tblObjName = 'tblPersonas'  # Si hay que cambiar estos nombres usar InventoryActivityAnimal.__setattr__()
    __tblLinkName = 'tblLinkPersonasActividades'
    __supportsPA = {}

    # Lists all Activity classes that support memory data, for access and initialization.
    _memory_data_classes = set()  # Initialized on creation of Activity classes. Defined here to include all Activities.

    @classmethod
    def getTblObjectsName(cls):
        return cls.__tblObjectsName

    @classmethod
    def getTblRAName(cls):
        return cls._tblRAName

    @classmethod
    def getTblLinkName(cls):
        return cls._tblLinkName


    def __init__(self, isValid, activityName=None, activityID=None, invActivity=None,
                 enableActivity=activityEnableFull, *args, **kwargs):
        # Agrega tablas especificas de Animales para pasar a Activity.

        if kwargs.get('supportsPA') is None:
            # Si no hay override desde abajo, setea al valor de __supportsPA{} para ese __activityName.
            kwargs['supportsPA'] = self.__supportsPA.get(activityName)
        super().__init__(isValid, activityName, activityID, invActivity, enableActivity,
                         self.__tblRAName, *args, **kwargs)


    def __call__(self, caller_object=None, *args, **kwargs):
        """
        @param caller_object: instance of Bovine, Caprine, etc., that invokes the Activity
        @param args:
        @param kwargs:
        @return: Activity object invoking __call__()
        """
        # item_obj=None above is important to allow to call fget() like that, without having to pass parameters.
        # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {(item_object, *args)}; kwargs: {kwargs}')
        self.outerObject = caller_object  # item_object es instance de Animal, Tag, etc. NO PUEDE SER class por ahora.
        return self
