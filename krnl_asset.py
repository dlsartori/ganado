from krnl_entityObject import EntityObject
import os

@property
def moduleName():
    return str(os.path.basename(__file__))


class Asset(EntityObject):
    objClass = 2
    objType = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)



