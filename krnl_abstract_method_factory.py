from krnl_abstract_base_classes import AbstractFactoryBaseClass

class MethodFactory(AbstractFactoryBaseClass):        # Abstract class
    def __init__(self):
        super().__init__()


class ActivityMethod(MethodFactory):
    """ Specific implementation of a methods generator for Activity Classes that use the outerObject feature """
    def __init__(self, activity_obj):  # activity_obj: instance of InventoryAnimalActivity, StatusAnimalActivity, etc.
        self.__activityObj = activity_obj
        super().__init__()

    #  Implementacion especifica de __call__(). Se invoca cada vez que se ejecuta un property
    def __call__(self, item_object=None, *args, **kwargs):
        # item_obj=None above is important to allow to call fget() like that, without having to pass parameters.
        # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {(item_object, *args)}; kwargs: {kwargs}')
        self.__activityObj.outerObject = item_object  # item_object es instance de Animal, Tag, etc. NO PUEDE SER class
        return self.__activityObj             # porque el 'self' de __call__ es un @property y no hace bind to classes
