from krnl_entityObject import *


@property
def moduleName():
    return str(os.path.basename(__file__))


class Asset(EntityObject):
    objClass = 2
    objType = 1

    def __init__(self, ID_Obj, isValid, isActive, *args, **kwargs):
        EntityObject.__init__(self, ID_Obj, isValid, isActive, *args, **kwargs)



