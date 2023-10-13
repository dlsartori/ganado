from krnl_entityObject import EntityObject
from krnl_cfg import os, lineNum, callerFunction


def moduleName():
    return str(os.path.basename(__file__))


class Brand(EntityObject):
    """
    Implements brands (Marcas). Mainly aimed to animals, for animal branding.
    """
    pass
