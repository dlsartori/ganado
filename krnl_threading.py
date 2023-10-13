# from krnl_config import *
import sqlite3

from krnl_config import timerWrapper, bkgd_logger, connCreate, time_mt, print, DISMISS_PRINT, callerFunction, lineNum, \
    USE_DAYS_MULT, MAIN_DB_NAME, os, VOID, SPD, DAYS_MULT, krnl_logger, fe_logger, ShutdownException, MAIN_DB_ID, \
    tables_and_methods, tables_and_binding_objects, db_time_stamps
from datetime import datetime, timedelta
import threading
from krnl_db_access import SqliteQueueDatabase
# from krnl_entityObject import EntityObject                      # Needed to pull processReplicated() deque
# from krnl_transactionalObject import TransactionalObject        # Needed to pull processReplicated() deque
from krnl_abstract_base_classes import AbstractFactoryBaseClass
from time import sleep
# import functools
# import dis                            # Modulo pseudo-desensamblador de codigo
from threading import Thread, Timer, Event, Lock, RLock
from krnl_async_buffer import AsyncBuffer, BufferAsyncCursor
from krnl_abstract_class_animal import Animal
from krnl_sqlite import SQLiteQuery

RUNNING = object()
NOT_RUNNING = 0
REENTRY = -10           # Permite hasta 10 niveles de anidamiento. Interesting solution...
MIN_INTERVAL = 1        # Min interval in seconds for IntervalTimer threads

def moduleName():
    return str(os.path.basename(__file__))

class IntervalTimer(Timer):
    """
    Class to execute background routines at specified time intervals. Routines defined:
    1. defineCategory() for Animals (All classes). Interval: daily (default)
    2. updateTimeout_bkgd() for Animals (All classes). Interval:daily (default). Can be set for each animal individually
    3. updateProgrammedActivities() for Animals, Persons, Devices. Interval: 6 hours. Also updates Programmed Activities
       Status when this function executes.
    """
    __THREAD_COUNT_1HR = 6          # 1 hour < interval <=MAX_INTERVAL
    __1HR = 3600               # Group 4
    __THREAD_COUNT_1MIN = 3
    __1MIN = 60                # Group 3
    __THREAD_COUNT_10SEC = 4
    __10SEC = 10               # Group 2
    __THREAD_COUNT_2SECS = 1
    __2SEC = 2                 # Group 1
    __MIN_INTERVAL = 2                 # Minimum Interval (in seconds)
    __MAX_INTERVAL = 30 * 24 * 3600     # Max Interval = 30 days (in seconds)

    # Dict with total number of running threads per interval range. Maximum values for each range just above.
    __activeThreadCounter = {__2SEC: 0, __10SEC: 0, __1MIN: 0, __1HR: 0}  # {GroupName:# of running threads in interval}

    # Seconds from start of hour or start of minute when threads will run.
    __startSeconds = {__2SEC: (1.5, 2.7), __10SEC: (15, 45),  __1MIN: (10, 30, 55),
                        __1HR: (0, 20, 35, 40, 50)}

    __allowedThreads = {__2SEC: len(__startSeconds[__2SEC]), __10SEC: len(__startSeconds[__10SEC]),
                        __1MIN: len(__startSeconds[__1MIN]), __1HR: len(__startSeconds[__1HR])}

    @property
    def threadCounter(self):
        return self.__activeThreadCounter

    @property
    def allowedThreads(self):
        return self.__allowedThreads

    def __init__(self, interval, func_name, *args, schedule=False, start_hour=None, **kwargs):
        self.__interval = 0
        self.__isValid = True
        self.__threadKey = None
        if not isinstance(interval, (int, float)) or interval <= 0:   # Procesa interval a un valor permitido
            bkgd_logger.warning(f'ERR_INP_InvalidArgument interval={interval}. Thread creation aborted.'
                                f'Thread not created.')
            self.__isValid = False
            return

        self.__startSecs = 0.0  # Este tiempo en segundos lo setea el launcher del thread, para evitar reentradas.
        self.__startHour = start_hour
        if schedule:  # Setea interval a valores predeterminados.
            interval = interval if interval < self.__MAX_INTERVAL else self.__MAX_INTERVAL
            loopList = sorted(list(self.allowedThreads.keys()), reverse=True)
            # print(f'+++++++++++++ loopllist: {loopList}')
            for i in loopList:
                if interval >= i:
                    if self.__activeThreadCounter[i] < self.allowedThreads[i]:
                        self.__interval = interval - (interval % i)      # (interval // i) * i
                        self.__startSecs = self.__startSeconds[i][self.__activeThreadCounter[i]]
                        self.__activeThreadCounter[i] += 1
                        self.__threadKey = i
                        self.__isValid = True
                    else:
                        self.__isValid = False
                    print(f'++++++++++++ init(): interval={self.__interval} / startSecs={self.__startSecs} '
                          f'key={self.__threadKey}', dismiss_print=DISMISS_PRINT)
                    break
                if self.__interval <= self.__MIN_INTERVAL:
                    self.__interval = interval if USE_DAYS_MULT else self.__MIN_INTERVAL
        else:
            self.__interval = interval

        if self.isValid is False:
            errorMsg = f'ERR_SYS_Number of IntervalTimer threads exceeded for threads with interval = ' \
                       f'{interval}. Consider adding new functions to existing threads.'
            bkgd_logger.error(errorMsg)

        super().__init__(self.__interval, func_name, *args, **kwargs)      # LLama a constructor de Timer()

        self.__counter = 0
        self.__lastExecutionTime = None
        self.__interval0 = 0  # tiempo en segungos en que run() entra al lazo while y corre func. por primera vez.
        self.__exitEvent = Event()
        # self.__condition = condition
        self.__queryObj = None  # query del thread. Inicializar en el cuerpo de run() para tener valor correcto
        # TODOS los threads que se ejecuten TIENEN que generar un queryObj para que lo usen las funciones de query a DB.
        self.__wrtObj = None
        self.__threadName = None    # Nombre del thread. Inicializar en el cuerpo de run() para tener valor correcto
        self.__threadID = None      # ID del thread. Inicializar en el cuerpo de run() para tener valor correcto
        self.__args = args
        self.__kwargs = kwargs

    def run(self):
        """
        Overloads original run() method in Timer Class. It is executed (only once) when start() is invoked for any
        Timer object. finished (a Event obj), interval, function, args, kwargs are built-in properties of Timer objects.
        @return: Nada
        """
        # TODO(cmt): Aqui se entra EFECTIVAMENTE al thread de ejecucion (IntervalTimer thread). A run() se ingresa 1
        #  sola vez. Ella termina por si misma o se mata el thread. Si __isValid=False -> sale sin ejecutar el loop.
        if self.__isValid is False:
            bkgd_logger.error(f'ERR_SYS_Number of IntervalTimer threads exceeded for threads with interval = '
                              f'{self.__interval}. Consider adding new functions to existing threads.')
            return False                # sale sin ejecutar por haberse excedido numero de threads..

        # TODO Thread creado DEBERA definir un queryObj para acceder a la database usando SQLiteQuery.__init__()
        if self.__queryObj is None:
            self.__queryObj = SQLiteQuery()  # Obj. de LECTURA: crea queryObj del Thread,lo registra en queryObjectsPool
        if self.__wrtObj is None:
            self.__wrtObj = SqliteQueueDatabase(MAIN_DB_NAME, autostart=True)   # Objeto de ESCRITURA en  DB.
        self.__threadName = threading.current_thread().name
        self.__threadID = threading.current_thread().ident  # Hay que inicializar aqui los parametros thread-specific
        self.__counter = 0
        self.function(self, *self.args, **self.kwargs)      # TODO(cmt): Corre func por primera vez. ESTO ES IMPORTANTE.

        # print(f'=== run() ===> function:{self.function.__name__} /self.args: {self.args} /self.kwargs: {self.kwargs}')

        # Waits (BLOCKS the thread) until _flag inside obj.finished Event is set to True by another thread, or until
        # self.interval times out. If self.finished is not set via a cancel() call obj.function executes when
        # self.interval expires. When the timeout occurs, obj.finished.wait returns False (hence the "while not")
        # self.finished is set to True when cancel() is called on the thread ->the while loop is then exited right there
        # function is internal to Timer Class, it is set to the target function to be run once the interval elapses,
        # with the arguments passed.
        # TODO(cmt): la asignacion abajo TIENE que estar pegada al lazo while: el minimo tiempo entre ambas es CRITICO.
        # TODO(cmt): sets interval0 for 1st-time execution of while loop.
        self.__interval0 = self.scheduler()
        print(f'=========> IntervalTimer.run()-> {self.function.__name__}() Interval: {self.interval} // '
              f'__interval0 = {self.__interval0}', dismiss_print=DISMISS_PRINT)
        while not self.finished.wait(self.interval if self.__interval0 is None else self.__interval0):
            self.__interval0 = None    # es mas eficiente una asignacion que hacer un if self.__interval0 is not False.
            self.__counter += 1     # Counter temporario. Debugging purposes.
            self.function(self, *self.args, **self.kwargs)  # args, kwargs viene de estruc. interna
            self.__lastExecutionTime = time_mt('datetime')           # No se usa esto por ahora...
            if isinstance(self.__exitEvent, Event) and self.__exitEvent.is_set():
                print(f'===>>>Leaving now {self.name}, via exitEvent: {self.__exitEvent} '
                      f'/ self.finished: {self.finished.is_set()}\n', dismiss_print=DISMISS_PRINT)
                break

        print(f'\n&&&&&&&&&&&&>> {self.name}, OUTSIDE run() while Loop. self.finished: {self.finished.is_set()} - '
              f'Loop Counter: {self.__counter}', dismiss_print=DISMISS_PRINT)
        if self.__threadKey:
            self.threadCounter[self.__threadKey] = max(self.threadCounter[self.__threadKey]-1, 0)  # Decrementa threadCounter
        self.queryObject.__del__()      # checks for same thread inside __del__() code.
        return     # return NO setea obj.finished a True. Solo cancel() setea este flag en True.

    @property
    def queryObject(self):
        return self.__queryObj

    @property
    def isValid(self):
        return self.__isValid

    @property
    def lastExecutionTime(self):
        return self.__lastExecutionTime

    @property
    def exitEvent(self):
        return self.__exitEvent

    @exitEvent.setter
    def exitEvent(self, event):
        if type(event) is Event:
            self.__exitEvent = event

    @property
    def threadID(self):
        return self.__threadID

    @property
    def counter(self):
        return self.__counter

    @property
    def my_interval(self):  # TODO(cmt): DO NOT USE "interval" name. It overrides Timer.interval and screws things up.
        return self.interval

    def scheduler(self):  # TODO: Codigo para espaciar IntervalTimer threads en el tiempo y que no se pisen entre ellos.
        """
        Defines and sets variable __interval0 in class IntervalTimer: the time in seconds (down to msec) when the while
        loop in function IntervalTimer.run() will execute for the 1st time. After that, the while loop will be excuted
        every __interval seconds, until cancel() is called on the thread.
        This is used to spread out threads and minimize threads stepping onto each other.
        @return:
        """
        if USE_DAYS_MULT:
            return None  # Va a ejecutar interval pasado como argumento si esta en modo simulacion de dias..
        microsecs = int(self.__startSecs % int(self.__startSecs) * 1000000) if self.__startSecs > 0 else 0
        print(f'\nThread: {self.__threadName}, seconds: {int(self.__startSecs)}, microsecs: {microsecs}',
              dismiss_print=DISMISS_PRINT)
        try:  # Valida y setea hora de ejecucion.
            startHour = int(self.__startHour % 24)
        except (TypeError, ValueError, AttributeError):
            # __startHour=None (default) genera TypeError exception -> se asigna hora actual.
            startHour = time_mt('datetime').hour
        time0 = time_mt('datetime')  # tiempo en segundos al que se le va a sumar delta para arrancar el thread.
        executionDatetime = datetime(time0.year, time0.month, time0.day, startHour, time0.minute
                                      if self.__interval < self.__1HR else 0, int(self.__startSecs), microsecs)
        # executionDatetime += timedelta(hours=startHour)  # Setea hora de inicio.

        # if self.__interval >= self.__1HR:
        #     if executionDatetime <= time_mt('datetime') + timedelta(microseconds=50000):   # Changui de 50 mseg.
        #         # si dict1 time se paso de start time, setea comienzo al cumplirse "interval" seconds.
        #         executionDatetime += timedelta(days=1) if isinstance(self.__startHour, int) else timedelta(hours=1)
        # # Recortar minutos y segundos de time0
        # else:
        # if self.__interval < self.__1HR:
        # executionDatetime += timedelta(minutes=time0.minute)
        # executionDatetime += timedelta(microseconds=microsecs)
        if executionDatetime <= time_mt('datetime') + timedelta(microseconds=50000):             # Changui de 50 mseg.
            # si time_mt() excede start time, setea ejecucion al comienzo del proximo periodo (suma "interval" seconds).
            executionDatetime += timedelta(seconds=self.__interval)  # suma 'interval' seconds si se paso start time

        retValue = executionDatetime.timestamp() - time_mt()
        print(f'=========> {moduleName()}({lineNum()}) Thread:{self.__threadName}-executionDateTime:{executionDatetime}'
              f' - seconds to start (from now): {retValue}', dismiss_print=DISMISS_PRINT)
        return retValue


# ========================================== Fin IntervalTimer =================================================== #

class IntervalFuncsFactory(AbstractFactoryBaseClass):     # Concrete Factory #1: Concrete Factory inicial,la primera.

    def __init__(self):       # object_name: string, user provided.
        super().__init__()

    @classmethod
    def create_object(cls, *, interval=None, func_list=(), start_hour=None, start_thread=False, **kwargs):
        """
        Creates one IntervalFunctions object. @return: IntervalFunctions Obj.
        """
        return IntervalFunctions(interval=interval, func_list=func_list, start_hour=start_hour,
                                 start_thread=start_thread, **kwargs)  # Crea y Retorna obj IntervalFunctions
# ========================================= End IntervalFuncsFactory ============================================= #

class IntervalFunctions(object):
    __rlock = RLock()
    __objectsIF = set()     # Registro de objetos IntervalFunctions (set). Ver donde y como usar...

    @classmethod
    def getFuncs(cls):                  # Retorna tuple de objectos IntervalFunctions validos, para operar 'as needed'
        return tuple(cls.__objectsIF)   # Retorna un immutable:Tuple no se puede modificar desde afuera

    def __init__(self, *, interval=None, func_list=(), start_hour=None, start_thread=False,  **kwargs):
        """
        @param func_list: List of functions to be executed by Interval Timer calls.
        @param interval: required interval in seconds. IntervalTimer will modify that value if required.
        @param func_list: List of functions to be run by func_executor(). Executed in order passed in func_list
        @param start_hour: start hour for intervals >= 1 hour (int). Ignored if interval < 1 hour.
        @param kwargs:
        """
        # removes non-callables and repeat values, KEEPING function order in list ('cause set(list) doesn't keep order)
        self.__funcList = list(dict.fromkeys([f for f in func_list if callable(f)]))
        self.__suspendedList = []           # list to store suspended functions.
        self.__exitEvent = Event()
        self.__busyFlag = dict.fromkeys(self.func_list, NOT_RUNNING)      # __busyFlag = {function: busy_flag, }
        super().__init__()

        # TODO(cmt): Creates and starts IntervalTimer thread to run functions in __func_list.
        self.__ITThread = IntervalTimer(interval, self.func_executor, self.__funcList, start_hour=start_hour,
                                        schedule=True, **kwargs)
        self.__ITThread.daemon = False  # Todas estos threads que corren un lazo while pueden ser daemon, pero por ahora se fuerza a terminarlos antes de salir del main()
        self.__isValid = self.thread.isValid
        self.__interval = self.thread.my_interval  # Interval in seconds at which IntervalFunctions are executed.
        if self.isValid:
            self.__objectsIF.add(self)
            if start_thread:
                self.thread.start()

    @property
    def func_list(self):                            # Returns list of functions to be executed.
        return self.__funcList

    @property
    def interval(self):
        return self.__interval

    @property
    def isValid(self):
        return self.__isValid

    @property
    def thread(self):
        return self.__ITThread

    def startThread(self):
        try:
            # if not self.thread.is_alive():
            self.thread.start()
        except (AttributeError, NameError, TypeError):
            pass

    def killThread(self):
        try:
            if self.thread.is_alive():
                self.thread.cancel()     # setea flag Timer.finished=True para salir del lazo while en func. run()
                if not self.thread.daemon:  # join() de los IntervalTimer Threads que se definan como non-daemon
                    self.thread.join(timeout=2)
        except (AttributeError, NameError, TypeError):
            pass

    def __del__(self):
        self.__objectsIF.discard(self)              # discard() no tira exception error si falla.
        self.killThread()

    def func_executor(self, *args, **kwargs):
        """
        Runs all functions in __funcList, in the order listed there.
        @param args:
        @param kwargs:
        @return: Dict {func: retValue, }. {} if func_list is empty (nothing executed). If reentry happens and is not
        supported, function returns RUNNING object.
        """
        # TODO(cmt): ejecuta las funciones listadas en __funcList, con verificacion de reentry.
        # RLock is used in order to allow use of locks by different threads, in particular the IntervalTimer threads.
        # Within 1 particular thread the __busyFlag dictionary handles reentry (nested execution) of the individual
        # functions in funcList.
        results = {}
        for f in self.__funcList:               # TODO(cmt): Runs functions in the order listed in __funcList.
            with self.__rlock:
                self.__busyFlag[f] += 1         # Incrementa flag para indicar "funcion f ejecutandose"

            if self.__busyFlag[f] > 1:          # 0: NOT_RUNNING; 1: Funcion corriendo; >1: Reentrada en funcion y como
                results[f] = RUNNING            # funcion f esta corriendo y no es reentrante -> No se ejecuta.
            else:
                results[f] = f(*args, **kwargs)     # Si f no esta corriendo, o es reentrante -> Se ejecuta.
                if self.__busyFlag[f] <= 0:         # __busyFlag[f] <=0, funcion definida como re-entrante.
                    bkgd_logger.warning(f'OOPPPAAAAAA - ACA HAY UN REENTRY: {f.__name__}...')

            with self.__rlock:
                self.__busyFlag[f] -= 1                 # Decrementa flag para indicar "ejecucion termino"
        return results

    def add_func(self, func, *, flag=NOT_RUNNING):
        """Adds function to __funcList. Returns True if added, False if nothing added (func is not valid)
        flag: NOT_RUNNING: function not running.
              RUNNING:  function is running.
              REENTRY: admits re-entry, can run nested.
        """
        if callable(func) and func not in self.__funcList:
            self.__funcList.append(func)
            self.__busyFlag[func] = flag if flag == REENTRY else NOT_RUNNING
            return True
        return False

    def remove_func(self, func):
        """ Removes a function from __funcList.
        @return: True if success or False if nothing removed, RUNNING if function is running (can't be removed).
        """
        if self.__busyFlag.get(func, 0) > NOT_RUNNING:
            return RUNNING              # Si se esta ejecutando, retorna RUNNING para indicar que no se pudo remover.
        elif func not in self.__funcList:
            return False                # func no es valida.
        else:
            with self.__rlock:
                self.__funcList.remove(func)
                self.__busyFlag.pop(func)                       # Elimina entrada func del diccionario __busyFlag.
            if func in self.__suspendedList:
                self.__suspendedList.remove(func)           # Elimina func de __suspendedList si estuviera ahi..
            return True

    def suspend(self, func):
        """ Suspends the execution of a function by removing it from __funcList"""
        # Con este codigo se conserva el valor de __busyFlag de la funcion para cuando se haga un resume(func)
        try:
            self.__funcList.remove(func)
            self.__suspendedList.append(func)
            return True
        except (ValueError, KeyError, TypeError):
            return False

    def resume(self, func):
        """ Adds func to __funcList to resume execution """
        # Con este codigo se recupera el valor de __busyFlag de la funcion
        if func in self.__suspendedList:
            flag = self.__busyFlag.get(func, NOT_RUNNING)
            self.__suspendedList.remove(func)       # Quita funcion de suspended list.
            return self.add_func(func, flag=flag)
        return False

    def suspend_all(self):
        """
        Halts execution of all functions present in __funcList
        @return: Nada
        """
        for f in self.__funcList:
            self.suspend(f)

    def resume_all(self):
        """
        Resumes execution of all functions present in __suspendedList
        @return: Nada
        """
        for f in self.__suspendedList:
            self.resume(f)

    def is_func_valid(self, f):
        """ Checks whether function is defined in func_list. Returns False if function is not defined """
        return f in (self.__funcList + self.__suspendedList)

    def is_func_running(self, f):
        """ Returns True if function is running, False if it's suspended """
        return f in self.__funcList

    def set_busy_flag(self, func, val=None):
        """ Sets reentry flag for func to REENTRY or NOT_RUNNING.
            All functions are initialized as non-reentrant (flag=NOT_RUNNING). Reentrant functions must be individually
            set calling this method for func.
        @return: True/False depending on whether accessing the flag succeeds or fails.
        """
        if func in self.__busyFlag:
            self.__busyFlag[func] = val if val == REENTRY else NOT_RUNNING
            return True
        return False

    def info(self):
        """ Returns IntervalFunction object information"""
        return {'interval': self.__interval, 'func_list': self.func_list, 'suspended functions': self.__suspendedList,
                'thread': self.thread, 'thread is running': self.thread.is_alive()}
# ========================================== End IntervalFunctions ================================================ #


class DatabaseReplicationCursor(BufferAsyncCursor):
    """ Implements the execution of methods that update memory data structures resulting from database replication, in
    a separate thread.
    Decoupling all these db-intensive tasks from the front-end thread is the right way to go to free-up the front-end.
    """
    _writes_to_db = MAIN_DB_NAME    # Flag to signal that the class uses db-write functions setRecord(), setRecords()

    def __init__(self, *args, event=None, the_object=None, the_callable=None, **kwargs):
        self._args = args       # Data for the object to be operated on (stored, bufferized, etc).
        self._kwargs = kwargs
        super().__init__(event=event, the_object=the_object, the_callable=the_callable)

    @classmethod
    def format_item(cls, *args, event=None, the_object=None, the_callable=None, **kwargs):
        """ Item-specific method to put item on the AsyncBuffer queue. Called from AsyncBuffer.enqueue()
        Standard interface: type(cursor)=cls -> valid cursor. None: Invalid object. Do not put in queue.
        @param event: Event object created for the cursor/item to signal its results are ready for fetching.
        @param args: All data to be appended (put) to the queue as a BufferAsyncCursor object.
        @param kwargs: More data to be appended to the cursor.
        @param the_object: the object for which the callable is called. Optional.
        @param the_callable: callable to execute operations on the object. Optional.
        """
        if not the_callable:
            return None
        return cls(*args, event=event, the_object=the_object, the_callable=the_callable, **kwargs)   # returns cursor.

    def execute(self):
        # self._callable -> processReplicated() (for now...)
        if callable(self._callable):
            # print(f'lalalalalalala execute {self.__class__.__qualname__}({lineNum()}): \n{self._callable}, '
            #       f'object: {self._object}, args: {self._args}')
            if hasattr(self._callable, "__self__"):
                return self._callable(*self._args, **self._kwargs)  # self._callable comes already bound to self._object
            if self._object:
                return self._callable(self._object, *self._args, **self._kwargs)  # self._callable NOT bound to _object.


    def reset(self):
        self._args = []
        self._kwargs = {}

# TODO: Final setting for thread_priority: > 15
replicateBuffer = AsyncBuffer(DatabaseReplicationCursor, autostart=True, thread_priority=10, qsize_threshold=2,
                              precedence=0)     # precedence=0 -> On exit, this queue stops LAST.

# --------------------------------------------- End DatabaseReplicationCursor ------------------------------------- #


# Funciones que se lanzan en background, usando clase IntervalFunctions.
# class IntervalFunctions EVITA RACE CONDITIONS entre funciones para que no se interrumpan entre ellas...

@timerWrapper(iterations=150, verbose=True)          # 200 iteraciones aqui..
def animalUpdates(*args, **kwargs):
    """ Updates Timeout and Category for Animals """
    retValue = 0
    # 1. Update Categories and Timeout status for all Animals in the system
    for cls in Animal.getAnimalClasses():
        count1 = cls.updateTimeout_bkgd()  # retorna int:cantidad de obj. procesados por updtTmt_bkgd
        # count2 = cls.updateCategories_bkgd()
        if count1:
            retValue += count1   # count1, count2 son cantidades de objetos (animales) procesados por la llamada.
            cls.timeoutEvent().set()        # TODO(cmt): Signal para otros threads, notificando Timeout.
        # if count2:
        # retValue += count2  # count1, count2 son cantidades de objetos (animales) procesados por la llamada.
        #     cls.categoryEvent().set()       # TODO(cmt): Signal para otros threads, notificando category change.
        # Prueba de acceso a DB desde bkgd thread
        # test_lectura = getRecords('tblCaravanas', '','', None, '*', fldFK_Color=6)
    return retValue

def twiceADay(obj, *args, **kwargs):   # obj es tipo IntervalTimer, para acceder a atributos de ahi (counter)
    """ Functions that run twice a day. """
    pass
    return

def FourADay(obj, *args, **kwargs):   # obj es tipo IntervalTimer, para acceder a atributos de ahi (counter)
    """ Functions that run every 6 hours. """
    db_optimize()           # Optimizes MAIN_DB connection.
    return None

def db_optimize(*, analysis_lim=None):
    writer = SqliteQueueDatabase(MAIN_DB_NAME)
    if not writer.is_stopped():
        analysis_limit = analysis_lim if isinstance(analysis_lim, (int, float)) and 0.0 <= analysis_lim <= 5000 else 500
        writer.execute_sql(f"PRAGMA ANALYSIS_LIMIT={int(analysis_limit)}; ")
        writer.execute_sql("PRAGMA OPTIMIZE; ")


REPORT_PERIOD = 60          # Report period in days to display screen messages.
# TODO(cmt): funciones a ejecutar en horas (1 < intervalo < 24 horas) PROVISORIO: -> Se usa para output a pantalla
def hourlyTasks1(obj, *args, **kwargs):   # obj es tipo IntervalTimer, para acceder a atributos de ahi (counter)
    """ For now: periodic screen output routine. """

    for cls in Animal.getAnimalClasses():                # cls is Bovine, Ovine, etc. Animal Object Class.
        print(f"Day # {obj.counter * REPORT_PERIOD}:", end='')
        if cls.timeoutEvent().is_set():
            cls.timeoutEvent().clear()
            dicto1 = {j[0]: (f'age:{int(j[0].age.get() * (SPD*DAYS_MULT if USE_DAYS_MULT else 1))}', j[1], j[2])
                      for j in cls.getBkgdAnimalTimeouts()}
            # if dicto1:
            print(f'TimeOutAnimals: {dicto1}')
        else:
            print('', end='')
        # if cls.categoryEvent().is_set():
        #     cls.categoryEvent().clear()
        #     dicto2 = {j.ID: cls.getBkgdCategoryChanges()[j] for j in cls.getBkgdCategoryChanges()}
        #     print(f'  ----- CatChange: {dicto2}-{callerFunction()} ThreadID:{threading.current_thread().ident}',
        #           dismiss_print=DISMISS_PRINT)
        #     bkgd_logger.info(f' ------ CatChange: {dicto2} - {callerFunction()} '
        #                          f'ThreadID: {threading.current_thread().ident}')

    return

def hourlyTasks2(obj, *args, **kwargs):
    print(f'$$$$ HOURS $$$$ Inside {callerFunction(namesOnly=True).replace("Function/ActivityMethod: ", "")} '
          f'- RUN #:{obj.counter}....Done.')
    return

def minuteTasks1(obj, *args, **kwargs):
    # 1. Update Categories and Timeout status for all Animals in the system
    # print(f'$$$$ MINUTES $$$$ - RUN #:{obj.counter}... Done.')
    return

@timerWrapper(iterations=10)             # Temporizar pa' ver como anda...
def checkTriggerTables(*args, **kwargs):
    """ Checks whether TimeStamp has changed for the rows in _sys_Trigger_Tables. For any changes, enqueues a cursor in
    replicateBuffer with all the information needed to perform checks and updates of the memory data structures
    associated to each of the tables.
    """
    try:
        con = connCreate(MAIN_DB_NAME)
    except (sqlite3.Error, sqlite3.DatabaseError):
        return None
    else:
        if not isinstance(con, sqlite3.Connection):
            return None
    cur = con.execute("SELECT DB_Table_Name, TimeStamp, Last_Updated_By FROM _sys_Trigger_Tables; ")
    with con:
        if not isinstance(cur, sqlite3.Cursor):
            return None
        cur_data = cur.fetchall()
    con.close()      # NEED to close() here. Exiting with con: will call commit(), but NOT con.close()
    if cur_data:
        tbl_tstamps = {cur_data[i][0]: cur_data[i][1] for i in range(len(cur_data))}  # {Table_Name: TimeStamp, }
        last_updated_by = {cur_data[i][0]: cur_data[i][2] for i in range(len(cur_data))}  # {Table_Name:Last_Updated_,}
    else:
        tbl_tstamps = {}
        last_updated_by = {}
    if not tbl_tstamps:
        return None

    for k in tbl_tstamps:                          # k is DB_Table_Name
        if tbl_tstamps[k] != checkTriggerTables.tstamps_last_values[k]:  #  and last_updated_by[k] != MAIN_DB_ID:
            if k in tables_and_methods:
                the_method = tables_and_methods[k]
                checkTriggerTables.tstamps_last_values[k] = tbl_tstamps[k]  # updates TimeStamp in last_values dict.
                if callable(the_method):
                    the_object = tables_and_binding_objects.get(k, None)
                    if the_object:
                        print(f' hhhhhhheeeeeeeeeey Trigger Tables: AQUI ESTOY con {k}: ({tbl_tstamps[k]}, '
                              f'{last_updated_by[k]}) / '
                              f'{the_object.__name__}.{the_method} - self?: {the_method.__self__}!!!--------------')
                        replicateBuffer.enqueue(the_object=the_object, the_callable=the_method)
                    else:
                        replicateBuffer.enqueue(the_callable=the_method)
                else:
                    return None
    return True

checkTriggerTables.tstamps_last_values = db_time_stamps     # {Table_Name: TimeStamp,}


dailyFunctions = IntervalFuncsFactory.create_object(interval=1.5 if USE_DAYS_MULT else 24*3600,  # start_hour=1,
                                                    func_list=(animalUpdates,), start_hour=0, start_thread=False)

fourADayFunctions = IntervalFuncsFactory.create_object(interval=1 if USE_DAYS_MULT else 6*3600,  # start_hour=2,
                                                         func_list=(FourADay,), start_thread=False)

hourlyFunctions = IntervalFuncsFactory.create_object(interval=REPORT_PERIOD/DAYS_MULT if USE_DAYS_MULT else 3600,
                                                     func_list=(hourlyTasks1, hourlyTasks2), start_thread=False)

minuteFunctions = IntervalFuncsFactory.create_object(interval=0.5 if USE_DAYS_MULT else 60,
                                                     func_list=(minuteTasks1, checkTriggerTables), start_thread=False)

# minuteFunctions = IntervalFunctions(interval=0.1243 if USE_DAYS_MULT else 60, func_list=(minuteTasks1, ),
#                                     start_thread=False)  # interval=60

# secondsFunctions = IntervalFuncsFactory.create_object(interval=1, func_list=(), start_thread=False)


# ========================================= End Interval Functions ================================================ #


# TODO(cmt): Executor classes para lanzar threads con funciones de foreground (fe_bovine, fe_caprine, etc).
#  Cada una de estas funciones va en su modulo y corre en un thread independiente.

class FuncRunner(object):
    def __init__(self, fn=None, *args, **kwargs):
        if not callable(fn):
            return
        self.__func = fn
        self.__args = args
        self.__kwargs = kwargs

    def run(self):
        running = VOID  # TODO: Las funciones TIENEN que salir con un valor != VOID para terminar su ejecucion.
        try:
            running = self.__func(*self.__args, **self.__kwargs)
        except (ShutdownException, Exception) as e:  # Exception captures any kind of Exception while executing func.
            running = e
        finally:
            if isinstance(running, str) and "ShutdownException" in running:
                krnl_logger.info(f'{self.__func.__name__} requested shutdown via ShutdownException. Exiting.')
            return running
    # IMPORTANT: threading.is_alive() will return False just after the last line of run() ends execution.


class FrontEndHandler(object):
    __lock = Lock()
    _obj_set = set()                # Object list with front end functions to be run by main_loop_launch()
    __main_running = False
    __main_loop_thread = None       # "Mother" thread to run all the individual front-end threads.
    _main_run_result = None
    _front_end_quit = Event()       # Exit event for Mother thread.

    def __init__(self, fn, *args, event=None, autostart=True, **kwargs):
        self.__func = fn
        self.__args = args
        self.__kwargs = kwargs
        self.__thread = None
        self.__running = False
        self.__run_result = VOID
        self.__stop_event = event or Event()

        self.register_obj()

    def start(self, *args, **kwargs):
        def executor():
            # Crea el objeto runner para correr la funcion target.
            func_runner = FuncRunner(self.__func, *self.__args, **self.__kwargs)
            fe_logger.info(f'Starting thread for module {self.__func.__name__}. About to run()...')
            self.__run_result = func_runner.run()       # thread just sits here until run() ends.
            if self.__run_result is not VOID:
                print(f'Thread: {self.__thread.name}; Alive: {self.__thread.is_alive()}; run_result: {self.__run_result}')
                self.__stop_event.set()  # event necesario porque join() de este thread se debe hacer desde OTRO thread.

        # Crea thread. Lanza thread si esta en autostart.
        if not self.__running:
            if args:                    # Updates args, kwargs if passed.
                self.__args = args
            if kwargs:
                self.__kwargs = self.__kwargs
            self.__thread = Thread(target=executor, name=self.__func.__name__, args=self.__args, kwargs=self.__kwargs)
            self.__thread.daemon = False  # will join() at the end and have main wait until target func. orderly closes.
            if self not in self._obj_set:
                self.register_obj()
            self.__thread.start()  # TODO(cmt): Aqui arranca el front end thread con la funcion target.
            self.__running = True
            return True       # retorna thread para hacer start() mas tarde, si no se hizo autostart.
        return False

    def stop(self):  # DO NOT CALL THIS ONE FROM INSIDE THIS CLASS. Will fail with "cannot join current thread" error.
        with self.__lock:
            if not self.__running:
                return False
            self.__running = False
            self.unregister_obj()
        print(f'ABOUT TO JOIN THREAD {self.__thread}.')
        self.__thread.join()

    def is_running(self):
        return self.__running

    def exit_event(self):
        return self.__stop_event.is_set()

    def register_obj(self):
        self._obj_set.add(self)

    def unregister_obj(self):
        self._obj_set.discard(self)

    def get_result(self):
        return self.__run_result

    @classmethod
    def __main_loop_runner(cls):
        while True:
            if cls._front_end_quit.is_set():  # If event for main loop is set, quits main looping function.
                break
            local_copy = list(cls._obj_set.copy())      # copy() es necesario porque stop() modifica _obj_set.
            for j in local_copy:
                if j.exit_event():
                    print(f'///MAIN_LOOP_RUNNER - STOPPING FUNCTION: {j.__func.__name__}')
                    j.stop()
                elif not j.is_running():            # si lo pusieron en cls._obj_set, hay que correrlo.
                    print(f'///MAIN_LOOP_RUNNER - STARTING FUNCTION: {j.__func.__name__}')
                    j.start()
            sleep(2)  # Checks every few seconds if a front-end func has to be stopped, to avoid impact on performance.
        return None

    @classmethod
    def main_loop_launch(cls):
        # Crea thread del main loop para correr todos los modulos de front end en threads independientes.
        if not cls.__main_running:
            cls.__main_loop_thread = Thread(target=cls.__main_loop_runner)
            cls.__main_loop_thread.daemon = False  # join() at the end and have main wait until target orderly closes.
            cls.__main_running = True
            cls.__main_loop_thread.start()  # TODO(cmt): Aqui arranca el 'Mother' thread con la func __main_loop_runner.
            return True       # retorna thread para hacer start() mas tarde, si no se hizo autostart.
        return False

    @classmethod
    def main_loop_quit(cls):
        with cls.__lock:
            cls.__main_running = False
            cls._main_run_result = True             # Algo aqui...Luego setear a lo que corresponda.
            local_list = cls._obj_set.copy()        # copy() porque stop() modifica cls._obj_set.
        for j in local_list:
            j.stop()            # Aqui se llama a los join() para cada front end module.

        print(f'///AND NOW, MAIN_LOOP_QUIT IS ABOUT TO JOIN {cls.__main_loop_thread.name}...\n\n\n')
        cls._front_end_quit.set()       # Le indica a __main_loop_runner() salir...
        cls.__main_loop_thread.join()   # Luego hace join() y espera que la funcion termine al procesar _front_end_quit.


"""                                FrontEndHandler - DIRECTIONS FOR USE:
- To launch a frontend module:
        frontend_obj1 = FrontEndHandler(func1, args, kwargs)        # func is bovine_frontend, etc.
        frontend_obj2 = FrontEndHandler(func2, args, kwargs)        # note: func WITHOUT PARENTHESIS!!
        frontend_obj3 = FrontEndHandler(func3, args, kwargs) 
        FrontEndHandler.main_loop_launch()

- To stop a module:
        front_end_obj.stop()
        
- To re-start a module:
        front_end_obj.start(*args, **kwargs)  # Include *args, **kwargs if need to start function with different args.
        
- To quit all front end modules and shutdown front-end looping thread:
        FrontEndHandler.main_loop_quit()
"""





