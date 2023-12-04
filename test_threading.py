import sys
from krnl_db_access import writeObj, init_db_replication_triggers, SqliteQueueDatabase
from custom_types import close_db_writes
from krnl_threading import REPORT_PERIOD, IntervalFunctions, dailyFunctions, FrontEndHandler
from krnl_object_instantiation import loadItemsFromDB
from krnl_async_buffer import AsyncBuffer       # Used to issue stop() to all buffers writers.
from krnl_bovine import Bovine, Animal
# from fe_bovine import bovine_frontend           # Front end function.
from krnl_person import Person
from krnl_config import *
from datetime import datetime
from time import sleep, perf_counter
import threading
from krnl_sqlite import __fldNameCounter
try:
    import psutil           # These 2 used to query total memory used by application.
    import os
except ImportError:
    pass

def moduleName():
    return str(os.path.basename(__file__))

if __name__ == '__main__':
    shortList = (0, 1, 4, 8, 11, 18, 27, 32, 130, 172, 210, 244, 280, 398, 41, 61, 92, 363, 368, 372)  # ID=0 no existe.
    bovines = loadItemsFromDB(Bovine, items=shortList, init_tags=True)  # Abstract Factory funca lindo aqui...
    reportPeriod = REPORT_PERIOD   # En dias. Tiempo para presentar una nueva linea en pantalla

    krnl_logger.info(f'******* switchinterval is set to {sys.getswitchinterval()} seconds.')
    # print(f'ANIMAL REGISTER DICT: {Bovine.getRegisterDict()}')
    people = loadItemsFromDB(Person)
    print(f'About Classes. person is: {people[0].__class__.__name__} / Animal: {bovines[0].__class__.__name__} / '
          f'isinstance(bovine[0], Animal): {isinstance(bovines[0], Animal)}')
    krnl_logger.info(f'======================================= End loadItemsFromDB =================================='
                     f'=========')

    initialCat = {}
    t1 = t2 = 0  # medidores de tiempo del lazo for.
    days = 15  # Numero de dias a setear Inventario, Categoria, contados desde t0.

    # bovine_fe_obj = FrontEndHandler(bovine_frontend)  # Can pass *args, **kwargs for bovine_frontend here.
    # FrontEndHandler.main_loop_launch()              # Lanza thread con interfaz de usuario (front end).

    t0 = time_mt()                          # tiempo referencia para DOB, Inventario, _setCategory(), EN SEGUNDOS..

    # TODO (cmt). IMPORTANTE: A partir de t0, TODOS los tiempos se deben contar en segundos porque daysSinceLastInv y


    inv_categ_date = datetime.fromtimestamp(t0 + days/DAYS_MULT if DAYS_MULT else 0)
    for j in bovines:
        j.dob = datetime.fromtimestamp(t0)    # Todos los animales nacen ahora. time_mt()->numero de secs desde epoch,
        t1s = perf_counter()
        j.inventory.set(date=inv_categ_date)  # Inventory, category: n dias despues de dob
        t1 = perf_counter() - t1s
        # print(f'{j.ID} lastInventory: {j.lastInventory} / j.inventory.get(): {j.inventory.get()} - t0={t0}')
        # print(f'lastInv[{j.ID}]: {j.lastInventory} / dob: {j.dob}')
        t2s = perf_counter()
        if 'f' in j.mf:
            j.category.set(category='Ternera', enforce=True, date=inv_categ_date)
        else:
            j.category.set(category='Ternero', enforce=True, date=inv_categ_date) # Registra inventario solo para machos
            j.isCastrated = True if j.recordID % 2 == 0 else False        # Machos con ID par castrados, ID impar no.
        t2e = perf_counter()
        t2 += t2e-t2s
        print(f'.', end='')
        initialCat[j.ID] = j.category.get()

    print(f'\nMAIN THREAD>   inventory.set() avg time (msec): {t1/len(bovines)*1000}')
    print(f'MAIN THREAD>  category.set() avg time (msec): {t2 / len(bovines)*1000}')
    print(f'Initial Categories: {initialCat}')
    krnl_logger.info(f'\n================================= End Category, DOB Setup ==================================')
    print(f'\n')

    # writeObj.start()  # TODO(cmt) TESTEADO!: Varios objetos CREAN errores de acceso (database is locked). USAR SOLO 1!

    print(f'                        ..................THREADING AWAY. MAIN THREAD Starts...')
    print(f'...INTERVALTIMER THREADS:')
    # intervalFunctionsList = IntervalFunctions.getFuncs()  # Funcion retorna los IntervalTimer threads activos
    for f in IntervalFunctions.getFuncs():  # TODO(cmt): Launching all IntervalTimer threads...
        f.startThread()
        print(f'MAIN THREAD> - {f.thread.name} Launching {f.thread.function.__name__}() / '
              f'Daemon: {f.thread.daemon} / Interval: {f.thread.interval}')

    try:
        process = psutil.Process(os.getpid())
        print(f'\n+++++++++++++++++++++  Total memory used by process (MB): {process.memory_info().rss/(1024 * 1024)}.')
    except (AttributeError, NameError):
        pass

    print(f'\n...INTERVALTIMER THREADS...')
    print(f'MAIN THREAD> Years simulated: {sleepAmount * DAYS_MULT / 365} / 1 second = {DAYS_MULT} days / '
          f'MAIN THREAD> New screen line = {reportPeriod} days.')
    print(f'MAIN THREAD> sleepAmount: {sleepAmount} seconds')
    print(f'MAIN THREAD> DB Writes from frontend: inventory.set(): ')

    startTime = time_mt()
    print(f'\nMAIN THREAD> ******* Now oficially starting Timer Threads... ')

    print('Running Threads are: ')
    for thread in threading.enumerate():
        print(f'{thread.name}: {thread.ident}', end='; ')
    print(f'\n\n')

    n = 6
    lele = len(bovines)
    for i in range(n):
        idx = min(lele, i)
        bovines[idx].inventory.set()      # Escribe inventario de animal ID=4 desde Main Thread
        print(f'\n*************************** FRONTEND: inventory #{i+1:2} recorded for AnimalID={bovines[idx]} **********************')
        print(f'**************************************************************************************************\n')
        sleep(sleepAmount/n)
    elapsed = time_mt() - startTime
    print(f'MAIN THREAD> Termino sleep de main thread...///')
    print(f'\n~~~~~~~~~~~~~~~~~~~ Total number of threads running: {threading.active_count()} ~~~~~~~~~~~~~~~~~~~~~ ')

    for f in IntervalFunctions.getFuncs():          # Exit all background threads.
        f.killThread()
        print(f"+++++++++++ Thread: {f.thread.name} +++++++++++++ Thread Counter: {f.thread.threadCounter}")

    print(f'\nMAIN THREAD>  Total Seconds main(): {elapsed:.2f} / "Time" elapsed (days): {int(elapsed * DAYS_MULT)}')
    print(f'MAIN THREAD> timerThread1.cancel()...ANTES de cancel(), Finished: {dailyFunctions.thread.finished.is_set()}'
          f'// counter value (taskExecutorDaily): {dailyFunctions.thread.counter}')
    print(f'MAIN THREAD> timerThread2 counter value (screen output thread): {dailyFunctions.thread.counter}')
    print(f'getFldName() Access Counter: {__fldNameCounter}')
    # sleep(1)
    # SqliteQueueDatabase.stop_all_writers()        # Funca lindo. Aqui da aviso de buffers activos y sale.
    # FrontEndHandler.main_loop_quit()
    close_db_writes()

    # AsyncBuffer.flush_all()                     # Flushes all AsyncBuffer queues, processing all objects in them.
    # SqliteQueueDatabase.stop_all_writers()      # Processes all pending database cursor objects (mostly db writes).
    # writeObj.stop()

    print(f'MAIN THREAD> ULTIMA linea de codigo............adios.')

