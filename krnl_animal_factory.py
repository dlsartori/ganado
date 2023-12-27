from krnl_config import lineNum, callerFunction, valiDate, os, datetime, fDateTime, singleton, krnl_logger
from krnl_abstract_base_classes import AbstractFactoryBaseClass
from krnl_custom_types import setupArgs, setRecord, delRecord
from krnl_tag import Tag                        # Se tiene que importar esta clase porque se invoca el constructor
from krnl_bovine import Bovine                  # Se tiene que importar esta clase porque se invoca el constructor
from krnl_caprine import Caprine                # Se tiene que importar esta clase porque se invoca el constructor

def moduleName():
    return str(os.path.basename(__file__))

# Animal/Tag pairs: Bovine/TagBovine,etc. Activities por ahora no se crea aqui.

class AnimalFactory(AbstractFactoryBaseClass):          # Concrete Factory #1: Concrete Factory inicial,la primera.
    tblObjName = 'tblAnimales'

    __creatorObjectsDict = {}       # TODO(cmt): en este Dict se registran OBJETOS bovine_factory, caprine_factory, etc.

    def __init__(self, animalClassName=None):       # animalClassName is a string: 'Bovine', 'Caprine', 'Ovine', etc.
        self.__animalClassName = animalClassName
        super().__init__()

    def register_object(self):               # TODO(cmt): Funcion para registrar los OBJETOS en __creatorObjectsDict
        # if self._animalClassName in Animal.getAnimalKinds():
        self.__creatorObjectsDict[self.__animalClassName] = self
        # self es un objeto BovineFactory, CaprineFactory, etc que  crea Objetos y Actividades.

    @classmethod
    def get_creator_obj(cls, animalClassName):      # animalClassName is a string: 'Bovine', 'Caprine', 'Ovine', etc.
        creator_obj = cls.__creatorObjectsDict.get(animalClassName)
        if not creator_obj:
            raise ValueError(animalClassName)
        return creator_obj

    @classmethod
    def getObjectsDict(cls):        # For debugging purposes only
        return cls.__creatorObjectsDict


animal_factory = AnimalFactory()  # Objeto clase AnimalFactory.Se llama desde animalCreator para obtener los creator_obj


@singleton
class BovineFactory(AnimalFactory):     # Concrete Factory #2.TODO(cmt): Se va a instanciar 1 solo objeto de esta clase
    def __init__(self):
        super().__init__(Bovine.__name__)     # Aqui se pasa nombre de la Class (string)

    @classmethod
    def create_object(cls, **kwargs):
        return cls.__createObject(**kwargs)  # Retorna OBJETO Bovine, o error.

    @classmethod
    def create_activity(cls, **kwargs):  # NO IMPLEMENTADO POR AHORA..
        return cls.__createActivity(**kwargs)  # Retorna OBJETO Activity, o error.

    @classmethod
    def create_tag(cls, **kwargs):
        return cls.__createTag(**kwargs)  # Retorna OBJETO BovineTag, o error.

    @staticmethod
    def __createObject(**kwargs):
        """
        Creates object for main Class in this module (Bovine in this case). Used in Alta/Baja. Calls Bovine constructor
        and returns object.
        Writes tblAnimales only. No other table is modified.
        NO CHECKS PERFORMED. DO THIS WHERE IT BELONGS, PLEASE.
        @param kwargs: Dict of parameters for object construction
        @return: Object or errorCode if error
        """
        # TODO: Validate kwargs:
        #  (fldID=None, fldMF=str(tblObjects.getVal(0, 'fldMF')),
        #  fldFK_ClaseDeAnimal=animalClassID, fldFlagCastrado=(1 if categoryName in __castrados else 0),
        #  fldCountMe=countMe, fldDOB=tblObjects.getVal(0, 'fldDOB'),
        #  fldConceptionType=conceptionType,
        #  fldFK_Raza=tblObjects.getVal(0, 'fldFK_Raza'),
        #  fldComment=tblObjects.getVal(0, 'fldComment'),
        #  fldMode=animalMode, fldDate=eventDate, fldDateExit=tblObjects.getVal(0, 'fldDateExit'),
        #  categoryName=categoryName)  # categoryName se necesita para llamar generateDOB()
        __validationKey = '$Ra567(Gghhhh@L&&;;;'  # Validacion para determinar desde donde se llama a __init__()

        dob = next((kwargs[j] for j in kwargs if str(j).lower().__contains__('flddob')), None)  # Busca en kwargs.

        if len(kwargs) == 0 or (not dob and 'categoryName' not in kwargs):
            retValue = f'ERR_INP_InvalidArgument: Object arguments not valid or missing. ' \
                       f'{moduleName()}({lineNum()}) - {callerFunction()}'
            print(retValue)
        else:
            # CREA OBJETOS en base a animalKind:
            if valiDate(dob, None) is None:
                dob = Bovine.generateDOB('Vacuno', kwargs['categoryName'])  # Genera DOB si no se pasa en kwargs.
                dob = datetime.strftime(dob, fDateTime)
            kwargs['fldDOB'] = dob
            if not next((j for j in kwargs if j.lower().__contains__('category')), None):
                kwargs['categoryName'] = Bovine.categories  # TODO: falta setear categoria, partiendo de dob (Edad)
            tblObjects = setupArgs(AnimalFactory.tblObjName, **kwargs)

            # Genera ID_Animal y guarda en DB. Aqui, kwargs tiene que estar totalmente validado.
            # Limpieza de Argumentos: Solo pasa a setRecord campos que esten en fldNames de tblObjects.
            # Ignora categoryName y demas que no esten ahi
            wrtDict = {j: kwargs[j] for j in kwargs if j in tblObjects.fldNames}
            idAnimal = setRecord(tblObjects.tblName, **wrtDict)  # si fldID==None crea registro nuevo
            if type(idAnimal) is str:
                retValue = f'ERR_DB_WriteError: {tblObjects.tblName}, {idAnimal}. Object not created.'
                print(f'{moduleName()}({lineNum()}) - {retValue}')
            else:
                tblObjects.setVal(0, fldID=idAnimal)  # No es necesario setear tblObjects, pero para mantener la alerta
                kwargs['fldID'] = idAnimal

                # Crea objeto Bovine (Vacuno)

                kwargs['factoryKey'] = __validationKey
                retValue = Bovine(**kwargs)
                if retValue:
                    if retValue.isActive:
                        _ = retValue.register()  # Registra objeto en __registerDict SOLO si el objeto esta Activo.
                else:
                    retValue = f'ERR_Sys_ObjectCreationFailure: {tblObjects.tblName}, {idAnimal}. Object not created.'
                    raise RuntimeError(f'{moduleName()}({lineNum()}) - {retValue}')
        return retValue

    @staticmethod
    def __createActivity(**kwargs):
        return kwargs

    @staticmethod
    def __createTag(*, generate=False, **kwargs):
        """
        Creates Animal Tag Object to assign to Animal, based in its class. object for main Class in this module.
        Used in Alta/Baja. Calls BovineTag, PorcineTag, etc constructor based on Animal Clas and returns object.
        Writes tblAnimales only. No other table is modified.
        @param kwargs: Dict of parameters for object construction
                     'generateTag' = True -> Uses generateTag method to create a random tag for testing purposes.
        @return: Tag Object or errorCode (str) if Tag not created
        """
        # genTags = str(next((kwargs[j] for j in kwargs if str(j).lower().__contains__('generate')), None)).lower()
        if not generate:
            newTag = Tag(**kwargs)          # TODO: Aqui se define clase de Tag a crear. Todo lo demas queda igual
        else:
            newTag = Tag.generateTag(**kwargs)
        retValue = newTag if newTag.isValid else f'ERR_Sys_ObjectInstantiationFailure'
        krnl_logger.error(retValue)
        return retValue

__bovineFactory = BovineFactory()
__bovineFactory.register_object()
# TODO: SIEMPRE registrar objetos derivados de AnimalFactory (BovineFactory, CaprineFactory, etc) en
#  AnimalFactory.__creatorObjectsDict

# ============================================== End BovineFactory ============================================ #

@singleton
class CaprineFactory(AnimalFactory):                # Concrete Factory #3

    def __init__(self):
        super().__init__(Caprine.__name__)

    @classmethod
    def create_object(cls, **kwargs):  # TODO: Se va a instanciar 1 solo objeto de esta clase,
        return cls.__createObject(**kwargs)  # Retorna OBJETO Bovine, o error.

    # @classmethod
    # def create_activity(cls, **kwargs):  # NO IMPLEMENTADO POR AHORA..
    #     return cls.__createActivity(**kwargs)  # Retorna OBJETO Activity, o error.

    @staticmethod
    def __createObject(**kwargs):
        """
            Creates object for Caprines.  Used in Alta / Baja. Calls Bovine onstructor and
            returns object.
            NO CHECKS PERFORMED. DO THIS WHERE IT BELONGS, PLEASE.
            @param kwargs: Dict of parameters for object construction
            @return: Object or errorCode if error
            """
        return kwargs

__caprineFactory = CaprineFactory()
__caprineFactory.register_object()
# __ovineFactory = OvineFactory()
# __ovineFactory.register()

# ============================================== End Caprine Factory ============================================

# TODO(cmt): class necesaria para crear distintas subclases (Bovine,Caprine,etc). Las Factories de clases que no tengan
#  subclases (ej: Person) no necesitan crean un __registerDict ni una clase como esta. Pueden instanciar directamente
#  desde sus objetos Factory. Tag, Device en particular TIENEN subclases por lo que deberan usar este mismo sistema.

@singleton
class AnimalCreator(object):
    def __init__(self):         #
        super().__init__()

    @staticmethod
    def object_creator(animalClassName, **obj_data):  # animalClassName is a string: 'Bovine', 'Caprine', 'Ovine', etc.
        creator = animal_factory.get_creator_obj(animalClassName)
        print(f'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFACTORY Running - {moduleName()}({lineNum()}). Object Data: {obj_data}')
        return creator.create_object(**obj_data)      # Retorna metodo de creacion (de Objectos o de Actividades)

    @staticmethod
    def tag_creator(animalClassName, **obj_data):
        creator = animal_factory.get_creator_obj(animalClassName)
        print(f'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFACTORY TAG CREATOR - {moduleName()}({lineNum()}). '
              f'creator_object type: {type(creator)} / Object Data(kwargos): {obj_data}  FFFFFFFFFFFFFFFFFFFFFFFFFFFF')
        return creator.create_tag(**obj_data)  # Retorna metodo de creacion (de Objectos o de Actividades)

animalCreator = AnimalCreator()  # INSTANCIA UNICA. TODO(cmt): Este objeto es llamado desde afuera para crear objetos.

# ---------------------------------------------------------------------------------------------------------------#
