from fe_config import *
from krnl_config import time_mt, fe_logger, getEventDate, moduleName, ShutdownException
from krnl_object_instantiation import loadItemsFromDB
from krnl_abstract_class_animal import Animal
from krnl_bovine import Bovine
from krnl_custom_types import DataTable, getRecords, setRecord
from krnl_db_query import SQLiteQuery     # Usar un objeto de esta clase para los DB queries. Implementa execute()
from krnl_db_access import writeObj     # DB object enabling sequential writes via a write spooler. USE ONLY THIS ONE!!
from ui_test import bovine_test

"""                                FrontEndHandler - DIRECTIONS FOR USE (from __main__):
- To launch a frontend module:
        frontend_obj1 = FrontEndHandler(func1, args, kwargs)        # func is bovine_frontend, for instance.
        frontend_obj2 = FrontEndHandler(func2, args, kwargs)   # note: func WITHOUT PARENTHESIS para el FrontEndHandler!
        frontend_obj3 = FrontEndHandler(func3, args, kwargs) 
        FrontEndHandler.main_loop_launch()
- To stop a module:
        front_end_obj.stop()
- To re-start a module:
        front_end_obj.start(*args, **kwargs)  # Include *args, **kwargs if need to start function with different args.
- To quit all front end modules and shutdown front-end main loop thread:
        FrontEndHandler.main_loop_quit()                                                                    """

""" Use fe_logger to log status messages, warning/error conditions to file. Example:
        fe_logger.warning('Esto es un warning. Algo paso...')  // fe_logger.info('Hoy es un lindo dia')
        5 niveles de mensajes:  debug, info, warning, error, critical.                                      """


def bovine_frontend(*args, **kwargs):  # TODO: template p/ modulos de front end (bovine,caprine,etc.)
    shortList = (1, 4, 5, 11, 18, 27, 32, 130, 172, 210, 244, 280, 363, 368, 372, 92, 120)  # ID=0 no existe.
    module_class = Bovine
    qryObj = SQLiteQuery()   # qryObj para todos los accesos (de query) a la database. Conexion ya abierta.
    bovines = loadItemsFromDB(module_class, items=shortList, init_tags=True)
    # TODO: DO NOT do registerKindOfAnimal here. Registrations are done in EntityObject.__init_subclass__(), and never
    #  unregistered because even if this node is not running a module, it may be running in other nodes.

    bovine_test()


    # aa = 'Some'
    # print('\nHOLA! Entre a bovine_frontend. Y YA ME ESTOY DIENDO!!!')
    # if aa == 'Some':
    #     print(f"************** ...ME VOY PA'L ShutdownException() - {module_class.__name__} frontend() ***************")
    #     # Removes itself from __registeredClasses dict so that background processing functions (processRA) stop
    #     # executing for the class.
    #     # module_class.removeAnimalClass()      # TODO: NOT TO BE removed here.
    #     raise ShutdownException()           # La funcion FrontEnd puede salir con ShutdownException...

    return False  # ... O retornar un resultado. Este resultado se testea en FrontEndHandler.executor() para terminar.
    # Si el resultado es !=VOID (que SIEMPRE debiera ser), la funcion se termina y se mata el thread que lo corre.

