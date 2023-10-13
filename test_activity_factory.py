from krnl_config import callerFunction, lineNum
from krnl_abstract_class_animal import Animal
from krnl_bovine import Bovine
from krnl_animal_activity import AnimalActivity
from krnl_object_instantiation import loadItemsFromDB
from krnl_abstract_method_factory import ActivityMethod  # Factory of Methods (inventory, etc.) for Activity classes


if __name__ == "__main__":
    print(f'class_register: {AnimalActivity.get_class_register()}')
    activityObjList = []
    missingList = []
    # # Creates InventoryAnimalActivity, StatusAnimalActivity singleton objects. Puts them in a list.
    # for j in AnimalActivity.get_class_register():
    #     try:
    #         activityObjList.append(j())     # Creates Activity object.
    #     except (AttributeError, TypeError, Exception) as e:
    #         print(f'Error in Activity object creation {callerFunction(getCallers=True)}({lineNum()}): class: {j} - error: {e}')
    #         missingList.append(j)
    #
    # print(f'Activity Objects are: {activityObjList}')
    # print(f'decoratorNames(Initial): {[j._decoratorName for j in activityObjList]}\nMissingList: {missingList}')
    #
    # for j in activityObjList:
    #     try:
    #         if j._decoratorName:        # se salta los None
    #             setattr(j, j._decoratorName, ActivityMethod(j))  # creates callable inventory in InventoryAnimalActivity,
    #             method = getattr(j, j._decoratorName)           #  status in StatusAnimalActivity, etc.
    #             # Aplica decorator @property. Necesario porque convierte method a class property. Si no, no puede
    #             # method = property(method)           # acceder a los metodos de InventoryAnimalActivity, etc.
    #             setattr(Animal, j._decoratorName, property(method))   # Crea atributo inventory, status, etc. en class Animal.
    #     except (AttributeError, TypeError, Exception):
    #         pass

    supershort = (363, 368, 372)
    bovines = loadItemsFromDB(Bovine, items=supershort, init_tags=True)  # Abstract Factory funca lindo aqui...
    bicho = bovines[0]

    # bicho.inventory
    bicho.inventory.get()
    print(f'Status: {[{j.ID: j.status.get()} for j in bovines]} ')



    methodNames = []
    for j in activityObjList:
        try:
            methodNames.append(j.__method_name)
        except AttributeError:
            pass

    print(f'Activity methods: {methodNames}')
