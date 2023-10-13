from krnl_tag import Tag
from krnl_config import os, lineNum, callerFunction


def moduleName():
    return str(os.path.basename(__file__))


class TagAnimal(Tag):
    """ Implements Abstract Class TagAnimal to provide support to lower classes (TagBovine, TagBird, TagPorcine,etc) """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

