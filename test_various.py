import sys
from datetime import datetime
import inspect
import operator
import time
from time import sleep
from krnl_cfg import *
import functools
from krnl_custom_types import DataTable, setupArgs
from krnl_cfg import moduleName, callerFunction
from operator import attrgetter
from json import dump, dumps, loads, load
import time

ax = 'JuvDT'


class Denada(object):

    def __init__(self):
        self.__var = 1

    # Pruebas para determinar desde dentro de una funcion/metodo si me llama una instancia de un objeto
    def metodo(self, val=0):

        self.__var = val
        print(f'@@ metodo: {self.__var}. Me llamo: {callerFunction()}')
        frameInfo = inspect.stack()[0]
        otherInfo = inspect.currentframe()  # TODO: Avoid using currentframe() to prevent compatibility issues
        try:
            info = frameInfo.frame.f_locals['obj']
            print(f'@@INFO [obj] - Denada.metodo() llamado desde Objeto {info}')
        except KeyError:
            print(f'@@INFO [obj] - Denada.metodo(): No es llamada desde un objeto...')
        print(f'@@INFO [obj] - Denada.metodo() llamado desde Objeto {frameInfo.frame.f_locals}')
        return self.__var

    @classmethod
    def estatico(cls):
        print(f'@@ estatico. Me Llaman: {callerFunction()}')
        # curframe = inspect.currentframe()
        # calframe = inspect.getouterframes(curframe, 2)
        # print('caller name:', calframe[1][3])
        frameInfo = inspect.stack()[1]
        if any(str(j).__contains__('Denada object at') for j in frameInfo.frame.f_locals.values()):
            print(f'@@INFO - estatico(): &&&&Llamado desde Objeto...&&&&')
            print(f'&&& type(objetito): {type(frameInfo.frame.f_locals["objetito"])}')
            print(f'&&& dict(objetito): {frameInfo.frame.f_locals["objetito"].__class__.__name__}')
            # clases = [type(frameInfo.frame.f_locals[j]) for j in frameInfo.frame.f_locals]
            # print(f'&&& Clases en locals: {clases} ')
        print(f'@@INFO - estatico(): NO hay obj...{frameInfo.frame.f_locals}')
        print(f'@@@CLASS: {cls.__name__}')


    # ======================================== Test de Operators para funcion Signature.match() =========================
def operations(op: str):

    __operatorsDict = {'in': operator.contains, '==': operator.eq, '>': operator.gt, '>=': operator.ge,
                       '<': operator.lt, '<=': operator.le, 'not': operator.not_, 'and': operator.and_,
                       'or': operator.or_, '!=': operator.ne, '<>': operator.ne, 'between': 'between', 'bet': 'between'}

    myList = [2, 3.8, 'Hola', (4, 5, 6), 'A', 'B', 'C', ['A', 'B', 'C'], 'JuvDT']
    ax = 9  # myList
    if op.strip().lower() in __operatorsDict:
        result = __operatorsDict[op](ax, myList) if op != 'in' else __operatorsDict[op](myList, ax)
        print(f'Dict result {ax} {op} myList: {result}')

        evaluated = eval_expression(f'"{ax}" {op} {myList}')            # Toma el valor de x!!!! No hace falta {}
        print(f'eval( {ax} {op} {myList}): {evaluated} ')

    r = range(1, 10)
    print(f' 5 in {r}: {5 in r}')
    print(f' 0.77 in {r}: {0.77 in r}')
    print(f' 2.77 in {r}: {2.77 in r}')     # TODO-IMPORTANTE: esta comparacion da False. 2.77 NO esta en rango (1, 10)


# ================================  IMPLEMENTACION DE eval() ====================================

def eval_expression(input_string, allowed_names):
    # print(f'allowed_names: {allowed_names}')
    print(f'Locals: {locals()}')
    try:
        code = compile(input_string, "<string>", "eval")
    except SyntaxError:
        return 'ERR_: SyntaxError'
    except NameError:
        return 'ERR_: NameError'
    except TypeError:
        return 'ERR_: TypeError'
    except KeyError:
        return 'ERR_: KeyError'
    except AttributeError:
        'ERR_: AttributeError'
    except EOFError:
        'ERR_: EOFError'
    print(f'co_names: {[name for name in code.co_names]}')
    for name in code.co_names:
        if name not in allowed_names:
            raise NameError(f"Use of {name} not allowed")
    # print(callerFunction(getCallers=4))
    try:
        retValue = eval(code, {"__builtins__": {}}, allowed_names)
    except SyntaxError('Error de sintaxis'):
        pass
    except NameError('NameError'):
        pass
    except TypeError('TypeError'):
        pass
    except KeyError('KeyError'):
        pass
    except AttributeError('AttributeError'):
        pass

    return retValue
# ==================================================================================================================


if __name__ == '__main__':

    myList = [2, 3.8, 'Hola', (4, 5, 6), 'A', 'B', 'C', ('A', 'B', 'C'), 'JuvDT']
    myList.__getitem__(5)
    startDate = datetime(2000, 1, 1)
    endDate = datetime(2022, 6, 30, 12, 0, 0, 1)
    someDate = datetime(2022, 5, 30, 12, 0, 0, 1)
    startDateStr = datetime.strftime(startDate, fDateTime)
    endDateStr = datetime.strftime(endDate, fDateTime)
    someDateStr = datetime.strftime(someDate, fDateTime)
    allowedNames = {'set': set, 'issubset': set.issubset, 'lower': str.lower, '__contains__': ''} # ESTO PUEDE SER UNA LISTA NOMAS...

    #TODO: La comparacion de fechas funciona como strings! -> Ver de formatear TODAS las fechas con fDateTime y pasarlas
    # directamente como string, a ver que pasa. - OJO: No conviene hacer +, -, * con strings. Para estas operaciones, convertir a datetime.

    activityOperands1 = {'fldName1': 1, 'fldName2': 'Hola', 'fldDate': someDateStr, 'fldName4': (9, 6, 7), 'fldName5': [12, 13]}         # This one comes from Performed Activity
    activityOperators = {'fldName1': '==s', 'fldName2': 'lower', 'fldDate': 'between', 'fldName4': 'issubset', 'fldName5': '__contains__'}  # This one comes from Performed Activity
    activityOperands2 = {'fldName1': 1, 'fldName2': None, 'fldDate': (startDateStr, endDateStr), 'fldName4': [6, 7, 9], 'fldName5': 12}   # This one comes from list of Programmed Activities
    # TODO: Operando 1 (Actividad Ejecutada) se debera pasar como argumento a eval_expression. Es un diccionario de
    #  la forma {fldName: fldValue, } - Los operadores se definen dentro de la funcion _paCreateActivity, como esta aqui
    #  arriba. - Los Operandos 2 tienen la misma forma {fldName: fldValue, } y vienen de Signature. Ver de definirlos en
    #  _paCreateActivity() directamente (como esta myList[] aqui abajo)

    resultsDict = {}
    for key in activityOperands1:
        try:
            if type(activityOperands1[key]) is str:
                activityOperands1[key] = f'"{activityOperands1[key]}"'  # Formato de string para argumentos string
            if type(activityOperands2[key]) is str:
                activityOperands2[key] = f'"{activityOperands2[key]}"'  # Formato de string para argumentos string
        except (AttributeError, KeyError, NameError, TypeError):
            resultsDict[key] = False
            break
        if activityOperators[key].strip().lower() in ['between', 'bet']:
            if type(activityOperands2[key]) in (list, set, tuple):
                startDate, endDate = (min(activityOperands2[key]), max(activityOperands2[key])) \
                    if len(activityOperands2[key]) == 2 else (0, activityOperands2[key][0])
            else:
                startDate, endDate = 0, activityOperands2[key]

            startDate = startDate if type(startDate) is not str else f'"{startDate}"'
            endDate = endDate if type(endDate) is not str else f'"{endDate}"'
            evalString = f'{startDate} <= {activityOperands1[key]} <= {endDate}'
        elif activityOperators[key].strip().lower().__contains__('issubset'):
            evalString = f'set({activityOperands1[key]}).issubset(set({activityOperands2[key]}))'
        elif activityOperators[key].strip().lower() in allowedNames:
            evalString = f'{activityOperands1[key]}.{activityOperators[key].strip().lower()}' + \
                         (f'({activityOperands2[key]})' if activityOperands2[key] is not None else '()')
        else:
            evalString = f'{activityOperands1[key]} {activityOperators[key]} {activityOperands2[key]}'

        resultsDict[key] = eval_expression(evalString, allowedNames)
        print(f'evalString: {evalString}: {resultsDict[key]}')
    print(f'Resultados: {resultsDict}')
    dumpedDict = dumps(activityOperands1)
    # print(f'{activityOperators.items()}')
    print(f'Dumped Dict (dumps): {dumpedDict}')
    retrievedDict = loads(dumpedDict)
    toprint = [f'{retrievedDict[j]}:{str(type(retrievedDict[j])).replace("<class ", "").replace(">","")}' for j in retrievedDict]
    print(f'Retrieved Dict (loads): {toprint}')
    # for i in retrievedDict:
    #     print(f'{retrievedDict[i]}: {type(retrievedDict[i])}')

    # operations('==')

    # ==== Enlightening sorting exercises: Sorts first by writeOrder (ascending), then by undoOnError (True, then False)
    def organicemonos(*args: DataTable):
        print(f'ahora en organicemonos() con todos sus callers: {callerFunction(includeMain="siiii")}  '
              f'/ StackDepth: {len(inspect.stack(0))}')
        argsNames = [i.tblName for i in args]
        wrtOrders = [i.wrtOrder for i in args]
        undoOnErrorInicial = [i.undoOnError for i in args]
        print(f'args Original: {argsNames}')
        print(f'write Orders: {wrtOrders}')
        print(f'undoOnError Inicial: {undoOnErrorInicial}')

        ordered_args = sorted(args, key=lambda x: (x.wrtOrder, -x.undoOnError))
        ordered_wrtOrder = [i.wrtOrder for i in ordered_args]
        ordered_argsNames = [i.tblName for i in ordered_args]
        ordered_undoOnError = [i.undoOnError for i in ordered_args]
        print(f'args Ordered : {ordered_argsNames}')
        print(f'ordered wrtOrder: {ordered_wrtOrder}')
        print(f'ordered undoOnError: {ordered_undoOnError}')

    def test_sorted(val, *args, **kwargs):
        print(f'ahora en test_sorted : {callerFunction(includeMain=True)}')
        __tblRAName = 'tblAnimalesRegistroDeActividades'
        __tblLinkName = 'tblLinkAnimalesActividades'
        __tblDataName = 'tblDataAnimalesActividadBaja'
        __tblTransactName = 'tblDataTMTransacciones'  # Tabla MoneyActivity. Debe pasarse COMPLETA para crear Transaccion
        __tblMontosName = 'tblDataTMMontos'  # Tabla MoneyActivity. Debe pasarse COMPLETA para crear Monto
        __tblPersonasName = 'tblDataAnimalesActividadPersonas'
        __tblDataTMName = 'tblDataAnimalesActividadTM'
        __tblDataCaravanasName = 'tblDataAnimalesActividadCaravanas'
        __tblDataDesteteName = 'tblDataAnimalesActividadDestete'
        __tblDataLocalizacionName = 'tblDataAnimalesActividadLocalizacion'
        __tblDataCastracionName = 'tblDataAnimalesActividadCastracion'
        __tblDataStatusName = 'tblDataAnimalesActividadStatus'
        __tblDataMedicionesName = 'tblDataAnimalesActividadMedicion'
        __tblDataCategoriasName = 'tblDataAnimalesCategorias'
        __tblDataInventarioName = 'tblDataAnimalesActividadInventario'
        __tblDataServiciosName = 'tblDataAnimalesServicios'
        __tbleDataPrenezName = 'tblDataAnimalesActividadPreñez'
        __tblDataMarcaName = 'tblDataAnimalesActividadMarca'

        tblObjectsName = 'tblAnimales'
        tblRAName = 'tblAnimalesRegistroDeActividades'
        tblLinkName = 'tblLinkAnimalesActividades'
        tblDataName = 'tblDataAnimalesActividadBaja'
        tblPersonasName = 'tblDataAnimalesActividadPersonas'
        tblDataTMName = 'tblDataAnimalesActividadTM'
        tblDataCaravanasName = 'tblDataAnimalesActividadCaravanas'
        tblDataDesteteName = 'tblDataAnimalesActividadDestete'
        tblDataLocalizacionName = 'tblDataAnimalesActividadLocalizacion'
        tblDataCastracionName = 'tblDataAnimalesActividadCastracion'
        tblDataStatusName = 'tblDataAnimalesActividadStatus'
        tblDataMedicionesName = 'tblDataAnimalesActividadMedicion'
        tblDataCategoriasName = 'tblDataAnimalesCategorias'
        tblDataInventarioName = 'tblDataAnimalesActividadInventario'
        tblAnimalesTiposDeAltaBajaName = 'tblAnimalesTiposDeAltaBaja'
        tblDataPrenezName = 'tblDataAnimalesActividadPreñez'
        tblDataMarcaName = 'tblDataAnimalesActividadMarca'
        tblDataEstadoDePosesionName = 'tblDataAnimalesStatusDePosesion'
        tblRA_TMName = 'tblTMRegistroDeActividades'  # Tabla MoneyActivity: setea params. de transac. monetaria relacionada
        tblTransactName = 'tblDataTMTransacciones'  # Tabla MoneyActivity
        tblMontosName = 'tblDataTMMontos'  # Tabla MoneyActivity

        tblRA = setupArgs(tblRAName)
        tblLink = DataTable(tblLinkName)
        tblData = DataTable(tblDataName)  # FechaEvento should be passed in fldDate in this Table, or timeStamp is used
        tblTMTrasactions = DataTable(tblTransactName)
        tblMontos = DataTable(tblMontosName)
        tblDataCategory = setupArgs(tblDataCategoriasName)
        tblObjects = DataTable(tblObjectsName)
        tblPersonas = DataTable(tblPersonasName)
        tblDataTMName = DataTable(tblDataTMName)
        tblDataCaravanas = DataTable(tblDataCaravanasName)
        tblDataDestete = DataTable(tblDataDesteteName)
        tblDataLocalizacion = DataTable(tblDataLocalizacionName)
        tblDataCastracion = DataTable(tblDataCastracionName)
        tblDataStatus = DataTable(tblDataStatusName)
        tblDataMediciones = DataTable(__tblDataMedicionesName)
        tblRA.wrtOrder = 1
        tblRA.undoOnError = True
        tblLink.wrtOrder = 2
        tblLink.undoOnError = True
        tblDataCategory.undoOnError = True
        tblPersonas.undoOnError = True
        tblDataCaravanas.wrtOrder = 3
        tblObjects.undoOnError = True
        tblDataTMName.undoOnError = True
        organicemonos(tblTMTrasactions, tblPersonas, tblRA, tblMontos, tblDataLocalizacion, tblDataCastracion, tblLink,
                      tblDataStatus, tblTMTrasactions, tblRA, tblObjects, tblDataCaravanas, tblDataCategory)
        # TEST DE kwargs...
        if kwargs:
            print(f'test_sorted({val}): AQUI ESTAN: {kwargs}')
        else:
            print(f'test_sorted({val}): NO SE PASARON kwargs')

        print(f'args: {args} / test_sorted type(args): {type(args)}')

    test_sorted(1, 'hola', 'que tal')
    test_sorted(2, 'este es el', 2,  **{})
    test_sorted(3, algo='mas')

    print(f'\n\n** Pruebas para determinar desde dentro de una funcion/metodo si me llama una instancia de un objeto **')
    objetito = Denada()
    objetito.metodo(1000)
    objetito.estatico()
    print(f'\n^^^Ahora llamando desde class Denada...^^^')
    Denada.estatico()

    print(f'Y el modulito: {moduleName()} // {callerFunction()}')
# ============================================================================================================ #
                                                # Pruebita de slicing
    loschars = '_______'
    alguito = 'aaaaaaaaabbbbbbvvvvvvvvm2'+loschars+'@345345345345345'
    queCosa = alguito[alguito.find(loschars) + len(loschars):]
    print(f'que cosa che: {queCosa}')
# ============================================================================================================ #

          # Storing lists as strings in DB. Usar json dumps()/loads() por simplicidad.
    maxNum = 8
    storeList = [('a' + str(j)) for j in range(maxNum)]
    storeListstr = str(storeList)
    storeDict = {1: 1, 2: 2, 3: 3, 4: 'Hola'}
    print(f'\nstring of storeList: {storeListstr}')
    print(f'Now back to List: {list(storeListstr)}')
    print(f'\nNow going with json...')
    jsonList = dumps(storeList)
    jsonDict = dumps(storeDict)
    print(f'jsonList dumps: {jsonList} / type(jsonList): {type(jsonList)}')
    print(f'jsonDict: {jsonDict} / type(jsonDict): {type(jsonDict)}')
    retrievedList = loads(jsonList)
    retrievedDict = loads(jsonDict)
    print(f'jsonList retrieved list (loads): {retrievedList} /  type(retrievedList): {type(retrievedList)}')
    print(f'jsonList retrieved Dict (loads): {retrievedDict} /  type(retrievedList): {type(retrievedDict)}')
# ============================================================================================================ #

    # print(f'\n                            Practicando con subsets...')
    # myList1 = [2, 3.8, 'Hola', (4, 5, 6), 4, 'A', 'B', 'C', ('A', 'B', 'C'), 'JuvDT']
    # subList1 = [2, 3.8, 'Hola', (4, 5, 6), 4, 'A', 'B', 'C', ('A', 'B', 'C'), 'JuvDT']
    # subList2 = [(4, 5, 6), 3.8, 4]
    # print(f'Lista: {myList1}')
    # print(f'{subList1} is subset: {set(subList1).issubset(set(myList1))}')
    # print(f'{subList2} is subset: {set(subList2).issubset(set(myList1))}')
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

    val = 4.5   #  'a String'        # OJO: Strings are iterable!!!
    print(f'{val} es iterable?: {hasattr(val, "__iter__")}')

    print(f"Is '' True or False??: {True if '' else False} / type(''): {type('')}")
    print(f'Is "" True or False??: {True if "" else False}')

    print(f'\n CHECK of inTypes() function: checks 1 object against several types')
    types = 'asdf', DataTable, int, float
    print(f'Is {val} (type: {type(val)}) in the Types [{types}]: {inTypes(val, types)}')

    tableta = DataTable('tlbuasdf')


    def count_calls(func):
        # @functools.wraps(func)
        def wrapper_count_calls(*args, **kwargs):
            wrapper_count_calls.num_calls += 1
            print(f"Call {wrapper_count_calls.num_calls} of {func.__name__!r}")
            return func(*args, **kwargs)
        wrapper_count_calls.num_calls = 0   # Se ejecuta una unica vez, durante inicializacion. Jamas durante ejecucion
        return wrapper_count_calls


    # def repeat(num_times=2):
    #     def decorator_repeat(func):
    #         # @functools.wraps(func)
    #         def wrapper_repeat(*args, **kwargs):
    #             for _ in range(num_times):
    #                 value = func(*args, **kwargs)
    #             return value
    #         return wrapper_repeat
    #
    #     return decorator_repeat


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
    # say_whee()
    # say_whee()
    # print(f'{say_whee.num_calls}')

    # t1 = time.perf_counter()
    # t2 = time.perf_counter()
    # print(f'Elapsed time: {(t2-t1)}')

