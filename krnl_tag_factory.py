from krnl_abstract_base_classes import AbstractFactoryBaseClass
from krnl_config import lineNum, callerFunction, valiDate, obj_dtError, obj_dtBlank, os, datetime, \
    fDateTime, singleton

from custom_types import setupArgs
from krnl_bovine_activity import BovineActivity, AnimalActivity
from krnl_tag import Tag           # Se tiene que importar esta clase porque se invoca el constructor
from krnl_caprine import Caprine                # Se tiene que importar esta clase porque se invoca el constructor
# from krnl_tag_caprine import TagCaprine         # Se tiene que importar esta clase porque se invoca el constructor


def moduleName():
    return str(os.path.basename(__file__))


class TagFactory(AbstractFactoryBaseClass):    # Concrete Factory #1: Concrete Factory inicial.
                                               # Contiene diccionario de objetos con las subclases de tags a crear

    __creatorObjectsDict = {}       # TODO(cmt): en este Dict se registran OBJETOS bovine_factory, caprine_factory, etc.

    def __init__(self, tagType='Tag'):
        self.__tagType = tagType.strip()
        super().__init__()

    def register_object(self):                 # TODO(cmt):La funcion para registrar los OBJETOS en __creatorObjectsDict
        self.__creatorObjectsDict[self.__tagType] = self
        # self es un objeto TagBovineFactory, TagCaprineFactory, etc que  crea tags

    @classmethod
    def get_creator_obj(cls, tagType):          # TODO: usar por ahora 'Tag' como argumento tagType
        creator_obj = cls.__creatorObjectsDict.get(tagType)
        if not creator_obj:
            raise ValueError(tagType)
        return creator_obj

    @classmethod
    def getObjectsDict(cls):        # For debugging purposes only
        return cls.__creatorObjectsDict

    @staticmethod
    def __createTag(**kwargs):
        """
       Creates Animal Tag Object to assign to Animal, based in its class. object for main Class in this module.
       Used in Alta/Baja. Calls BovineTag, PorcineTag, etc constructor based on Animal Clas and returns object.
       Writes tblAnimales only. No other table is modified.
       @param kwargs: Dict of parameters for object construction
                    'generateTag' = True -> Uses generateTag method to create a random tag for testing purposes.
       @return: Tag Object or errorCode (str) if Tag not created
       """
        generate = str(next((kwargs[j] for j in kwargs if str(j).lower().__contains__('generate')), None)).lower()
        if not generate:
            newTag = Tag(**kwargs)      # TODO(cmt): Aqui se define clase de Tag a crear. Todo lo demas queda igual
        else:
            newTag = Tag.generateTag(**kwargs)
        retValue = newTag if newTag.isValid else f'TagFactory: ERR_Sys_ObjectInstantiationFailure'
        return retValue

tag_factory = TagFactory()  # Objeto clase AnimalFactory.Se llama desde animalCreator para obtener los creator_obj
tag_factory.register_object()


@singleton
class TagBovineFactory(TagFactory):       # Concrete Factory #2. TODO: Se va a instanciar 1 solo objeto de esta clase,
    def __init__(self):
        super().__init__('Vacuno')

    @classmethod
    def create_tag(cls, **kwargs):
        return cls.__createTag(**kwargs)  # Retorna OBJETO BovineTag, o error.

    @staticmethod
    def __createTag(**kwargs):
        """
        Creates Animal Tag Object to assign to Animal, based in its class. object for main Class in this module.
        Used in Alta/Baja. Calls BovineTag, PorcineTag, etc constructor based on Animal Clas and returns object.
        Writes tblAnimales only. No other table is modified.
        @param kwargs: Dict of parameters for object construction
                     'generateTag' = True -> Uses generateTag method to create a random tag for testing purposes.
        @return: Tag Object or errorCode (str) if Tag not created
        """
        generate = str(next((kwargs[j] for j in kwargs if str(j).lower().__contains__('generate')), None)).lower()
        if not generate:
            newTag = Tag(**kwargs)      # TODO(cmt): Aqui se define clase de Tag a crear. Todo lo demas queda igual
        else:
            newTag = Tag.generateTag(**kwargs)
        retValue = newTag if newTag.isValid else f'TagBovine: ERR_Sys_ObjectInstantiationFailure'
        return retValue


__tagBovine_factory = TagBovineFactory()
__tagBovine_factory.register_object()

@singleton
class TagCreator(object):
    def __init__(self):
        super().__init__()

    @staticmethod
    def object_creator(tagType, **obj_data):
        creator = tag_factory.get_creator_obj(tagType)
        print(f'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFACTORY Running - {moduleName()}({lineNum()}). Object Data: {obj_data}')
        return creator.create_object(**obj_data)      # Retorna metodo de creacion (de Objectos o de Actividades)

    # def activity_creator(self, animalKind, **obj_data):      # __createActivity: NO IMPLEMENTADO POR AHORA.
    #     creator = __animalFactory.get_creator_obj(animalKind)
    #     return creator.create_activity(**obj_data)      # Retorna metodo de creacion de Actividades

    @staticmethod
    def tag_creator(tagType, **obj_data):
        creator = tag_factory.get_creator_obj(tagType)
        print(f'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFACTORY TAG CREATOR - {moduleName()}({lineNum()}). '
              f'creator_object type: {type(creator)} / Object Data(kwargos): {obj_data}  FFFFFFFFFFFFFFFFFFFFFFFFFFFF')
        return creator.create_tag(**obj_data)  # Retorna metodo de creacion (de Objectos o de Actividades)

tagCreator = TagCreator()  # INSTANCIA UNICA. TODO(cmt): Este objeto es llamado desde afuera para crear objetos.


