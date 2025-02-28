from krnl_entityObject import *
from krnl_asset import *



@property
def moduleName():
    return str(os.path.basename(__file__))


class AssetItem(Asset):
    __objClass = 20
    __objType = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Asset.__init__(self, *args, **kwargs)
