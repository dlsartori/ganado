import threading
from krnl_tag import Tag
from krnl_config import os
from krnl_db_query import AccessSerializer, AccessSerializer
from krnl_config import PANDAS_READ_CHUNK_SIZE
import pandas as pd
def moduleName():
    return str(os.path.basename(__file__))


class TagAnimal(Tag):
    """ Implements Abstract Class TagAnimal to provide support to lower classes (TagBovine, TagBird, TagPorcine,etc)
    Implements a class factory method (_create_subclass) to create all applicable Tag subclasses for TagAnimal. """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def _create_subclass(cls, cls_name, *, obj_class=None):
        """ Class Factory function. Returns a Tag class that will be used as a repository of subclasses. These
        subclasses will in turn be used to create the actual Tag objects used in the system.
        Creates Tag subclasses for every Animal type that defines the __uses_tag attribute.
        Classes created implement the dictionaries and data structures below and use all the methods from class Tag.
        @param cls_name: str. Class Name given to the class being created ('TagBovine', 'TagCaprine', etc).
        @param obj_class: Animal, Device subclass that will use this Tag class (class Bovine, class Caprine, etc).
        """
        def init_parent(self, *args, **kwargs):  # Can't remove self from here. It must always be 1st arg in __init__().
            super().__init__(*args, **kwargs)

        # Returns Tag sub_class template. Base class is cls. cls_name=TagBovine, TagCaprine, TagChicken, TagDove, etc.
        parent_class = type(cls_name, (cls, ), {
            '_objectClass': obj_class,  # <class Bovine>, <class Caprine>, etc.
            '_active_uids_df': {},
            '_dupl_series':  pd.Series([], dtype=object),
            '_chunk_size': 300,    # PANDAS_READ_CHUNK_SIZE,       # This value is specific to each class.
            '_object_mem_fields': ('*', ),          # All fields in tblCaravanas, to create objects from memory.
            '_access_serializer': AccessSerializer(),       # keeps access_count to init_uid_dicts code.
            '_sem_obj_dataframe': threading.BoundedSemaphore(),  # Semaphore specific to _active_uids_df for each class.
            '_tagObjectsClasses': {},  # {tagTech(str): <Child (object) class>, } ex: {TagStandard: <TagStandardBovine>}
            '__init__': init_parent,               # __init__() to pass up the chain.
        })

        def init_child(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        # Creation of subclasses that will instantiate actual Tag objects.
        # name is 'standard', 'rfid', 'bluetooth', etc. All tag technology names defined in the database.
        # c is TagStandard, TagRFID, TagBluetooth, etc.(classes themselves, NOT class names).
        for tech_name, c in cls.getTagTechClasses().items():
            # child_name is TagStandardBovine, TagRFIDMachine, TagBluetoothChicken, etc.
            child_name = c.__name__ + parent_class._objectClass.__name__
            child_class = type(child_name, (parent_class, c), {
                # '_active_uids_df': {},
                # '_dupl_series': pd.Series([], dtype=object),
                # '_chunk_size': 300,  # PANDAS_READ_CHUNK_SIZE,       # This value is specific to each class.
                # '_object_mem_fields': ('*',),  # All fields in tblCaravanas, to create objects from memory.
                # '_access_serializer': AccessSerializer(),  # keeps access_count to init_uid_dicts code.
                # '_sem_obj_dataframe': threading.BoundedSemaphore(),  # Semaphore specific to _active_uids_df for each class.
                '_parent': parent_class,
                '__init__': init_child,            # __init__() to pass up the chain
              })
            # This dict is meant to remain unchanged for now: the Tag Technologies should remain constant in the system.
            # The dict is used to create actual Tag objects, based on object they are assigned to and the Tag technology
            parent_class._tagObjectsClasses[tech_name] = child_class  # {tagTech: TagObjectClass (TagStdBovine, etc.) }

        # print(f'################# I"M GETTING THROUGH!!!! Parent: {parent_class.__name__} - '
        #       f'Children: {parent_class._tagObjectsClasses}#####################')

        return parent_class



class TagMammal(TagAnimal):
    """ Implements Abstract Class TagMammal (abstract class) to provide support to lower classes
    (TagBovine, TagBird, TagPorcine,etc) """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class TagBird(TagAnimal):
    """ Implements Abstract Class TagMammal (abstract class) to provide support to lower classes
    (TagBovine, TagBird, TagPorcine,etc) """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
