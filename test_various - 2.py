import sys
from datetime import datetime
import inspect

import functools
from krnl_custom_types import DataTable
from krnl_config import singleton
from operator import attrgetter
from json import dump, dumps, loads, load
import time

if __name__ == '__main__':












# ============================================================================================================ #
                                                # Pruebita de slicing
    # loschars = '_______'
    # alguito = 'aaaaaaaaabbbbbbvvvvvvvvm2'+loschars+'@345345345345345'
    # queCosa = alguito[alguito.find(loschars) + len(loschars):]
    # print(f'que cosa che: {queCosa}')
# ============================================================================================================ #

          # Storing lists as strings in DB. Usar json dumps()/loads() por simplicidad.
    # maxNum = 8
    # storeList = [('a' + str(j)) for j in range(maxNum)]
    # storeListstr = str(storeList)
    # storeDict = {1: 1, 2: 2, 3: 3, 4: 'Hola'}
    # print(f'\nstring of storeList: {storeListstr}')
    # print(f'Now back to List: {list(storeListstr)}')
    # print(f'\nNow going with json...')
    # jsonList = dumps(storeList)
    # jsonDict = dumps(storeDict)
    # print(f'jsonList dumps: {jsonList} / type(jsonList): {type(jsonList)}')
    # print(f'jsonDict: {jsonDict} / type(jsonDict): {type(jsonDict)}')
    # retrievedList = loads(jsonList)
    # retrievedDict = loads(jsonDict)
    # print(f'jsonList retrieved list (loads): {retrievedList} /  type(retrievedList): {type(retrievedList)}')
    # print(f'jsonList retrieved Dict (loads): {retrievedDict} /  type(retrievedList): {type(retrievedDict)}')
# ============================================================================================================ #

    # print(f'\n                            Practicando con subsets...')
    # myList1 = [2, 3.8, 'Hola', (4, 5, 6), 4, 'A', 'B', 'C', ('A', 'B', 'C'), 'JuvDT']
    # subList1 = [2, 3.8, 'Hola', (4, 5, 6), 4, 'A', 'B', 'C', ('A', 'B', 'C'), 'JuvDT']
    # subList2 = [(4, 5, 6), 3.8, 4]
    # print(f'Lista: {myList1}')
    # print(f'{subList1} is subset: {set(subList1).issubset(set(myList1))}')
    # print(f'{subList2} is subset: {set(subList2).issubset(set(myList1))}')
    # print(f'Intersection: {set(myList1).intersection(subList2)}')
    # dict1 = {'a': 'a', 'b': 4, 'c': "HH"}
    # dict2 = {'hola': 'HOLA!', 'M': 55, 'rat': [2, 3, 'mm'], 'b': 'lopario', 'c': 95}
    #
    # common = set(dict1).intersection(dict2)
    # print(f'common: {common}')
# =================================================================================================================

    # tblAnimal = DataTable('tblAnimales', fldDOB='2022-01-24 22:22:22:000000', fldFK_AnimalPadre='El Cornudo')
    # print(f'{tblAnimal.dataList} / dataLen: {tblAnimal.dataLen} / fields: {tblAnimal.fldNames}')
    # setRecord('tblAnimales', fldDOB='2022-01-24 22:22:22:000000', fldFK_AnimalPadre='El Cornudo')
    # # tblAnimals.setVal(0, fldDOB='2022-01-24 22:22:22:000000', fldFK_AnimalPadre='El Cornudo')
    # print(f'@@@ tablita DataList: {tblAnimal.dataList}')
    #
    # tablita1 = getRecords('tblAnimales', '', '', None, '*', fldID=[3, 4, 5, 6])
    # print(f'Lista1{tablita1.dataList} / dataLen1: {tablita1.dataLen} / fields1: {tablita1.fldNames}')
    # tablita1.setVal(0, fldMode='Dummy')
    # print(f'Columna Fecha: {tablita1.getCols("fldDate", "fldID")} / VALOR 0: {tablita1.getVal(0, "fldMode")}')

    # val = 4.5   # 'a String'        # OJO: Strings are iterable!!!
    # print(f'{val} es iterable?: {hasattr(val, "__iter__")}')
    #
    # print(f"Is '' True or False??: {True if '' else False} / type(''): {type('')}")
    # print(f'Is "" True or False??: {True if "" else False}')
    #
    # print(f'\n CHECK of inTypes() function: checks 1 object against several types')
    # types = 'asdf', DataTable, int, float
    # print(f'Is {val} (type: {type(val)}) in the Types [{types}]: {inTypes(val, types)}')
    #
    # tableta = DataTable('tlbuasdf')

# =====================================Pruebas con @decorators===================================================

    def count_calls(func):
        # @functools.wraps(func)
        def wrapper_count_calls(*args, **kwargs):
            wrapper_count_calls.num_calls += 1
            print(f"Call {wrapper_count_calls.num_calls} of {func.__name__!r}")
            return func(*args, **kwargs)
        wrapper_count_calls.num_calls = 0   # Se ejecuta una unica vez, durante inicializacion. Jamas durante ejecucion
        return wrapper_count_calls

    def repeat(_func=None, *, num_times=2):
        def decorator_repeat(func):
            # @functools.wraps(func)
            def wrapper_repeat(*args, **kwargs):
                for _ in range(num_times):
                    value = func(*args, **kwargs)
                return value
            return wrapper_repeat
        if _func is None:         # Este if se ejecuta SOLO UNA VEZ. Durante la definicion/import de la funcion say_whee
            return decorator_repeat
        else:
            return decorator_repeat(_func)

    @repeat(num_times=3)
    def say_whee():
        print("Whee!")

    say_whee()

    @singleton    # Al inicializar se llama la funcion singleton() y se corre su codigo (en el cuerpo de singleton() por
    # unica vez. NO se ejecuta en este momento el codigo de wrapper.Este se ejecutara luego con cada llamada a TestClass
    class TestClass(object):  # Las llamadas posteriores a funcion/class decorada van directamente a ejecutar el wrapper
        def __init__(self):
            super().__init__()
        @staticmethod
        def estatico():
            return 4

    obj1 = TestClass()  # Pruebitas nomas...
    obj2 = TestClass()
    print(f'Objetos singleton: obj1={obj1}; obj2={obj2} / id(obj1)={id(obj1)}; id(obj2)={id(obj2)} '
          f'/ obj1==obj2:{obj1==obj2}')
    mac = obj1.estatico()
    mac1 = TestClass().estatico()  # Forma de llamada p/ que @singleton(TestClass) acceda a class/static methods

    kwargs = {'Animal': 'gato', 'caleNDula': 'flor', 'ID': 488, 'BestMatch': 11.11, 'genList': [1, 2, 3, 'aa']}

    key = 'calendula'
    lowerSetting = 1
    defaultVal = 8888
    exactMatch = True
    # a = getArg(key, lower=lowerSetting, defaultVal=defaultVal, exact_match=exactMatch, **kwargs)
    # print(f'Values found: {key}: {a}')


# ===================================================== SETS ==================================================


