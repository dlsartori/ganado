import queue
import sqlite3
import sys
from threading import Event
from time import sleep, perf_counter_ns
from krnl_abstract_base_classes import AbstractFactoryBaseClass, AbstractAsyncBaseClass
from krnl_config import krnl_logger, lineNum, print, DISMISS_PRINT, callerFunction, time_mt
from threading import Lock
from krnl_db_access import ResultTimeout, ThreadHelper, GreenletHelper, WriterPaused, PAUSE, UNPAUSE, SHUTDOWN, \
    ShutdownException


class BufferAsyncCursor(AbstractAsyncBaseClass):
    """ Creates instances of cursor objects with async_buffer data. This class is abstract and will not be instantiated.
        1. The cursors are stored on a queue for execution.
        2. A Writer class object pulls the objects from the queue and executes the SQL statements via sqlite3 module.
            The results are stored back in the cursor object via the set_result method and an Event is set to flag the
            retrieval methods that the results are available to the caller.
        3. On request, the retrieval methods provide the results if available, or wait() until available and return them.
    """
    __slots__ = ('timeout', '_event', '_cursor', '_exc', '_ready', '_object', '_callable')

    __class_register = set()  # Stores all subclasses of BufferAsyncCursor (db-writers and non db-writers)

    # This code executes after the subclasses complete the creation code, WITHOUT any object instantiation. Beleza.
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.register_class()

    def __new__(cls, *args, **kwargs):          # __new__() override prevents instantiation of BufferAsyncCursor class.
        if cls is BufferAsyncCursor:
            krnl_logger.error(f"ERR_SYS_Invalid type: {cls.__name__} cannot be instantiated, please use a subclass")
            raise TypeError(f"{cls.__name__} cannot be instantiated, please use a subclass")
        return super().__new__(cls)

    def __init__(self, *, event=None, timeout=None, the_object=None, the_callable=None):
        self._event = event
        self.timeout = timeout
        self._cursor = self._exc = None
        self._ready = False
        self._object = the_object
        self._callable = the_callable
        super().__init__()

    @classmethod
    def writes_to_db(cls):
        try:
            return cls._writes_to_db  # Pulls the right instance of _writes_to_db from subclasses (DataUploadCursor,etc)
        except AttributeError:
            return False

    def set_result(self, result, exc=None):
        self._cursor = result
        self._exc = exc
        self._event.set()
        return self

    def _wait(self, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        print(f'                                 \n###################  Waiting in _wait  ########################')
        if not self._event.wait(timeout=timeout) and timeout:
            raise ResultTimeout('Buffer results not ready, timed out.')
        if self._exc is not None:
            raise self._exc
        self._ready = True

    @property
    def result(self):
        if not self._ready:
            self._wait()
        return self._cursor

    def execute(self):      # Must be implemented by all subclasses.
        pass

    @classmethod
    def format_item(cls, *args, **kwargs):
        """ Item-specific method to put item on the AsyncBuffer queue
        Standard interface: type(cursor)=cls -> valid cursor. None: Invalid object. Do not put in queue.
        """
        pass                                # Must be implemented by all subclasses.

    @classmethod
    def register_class(cls):
        cls.__class_register.add(cls)

    @classmethod
    def __unregister_class(cls):                # Hidden method(). unregistering a class will cause data corruption.
        cls.__class_register.discard(cls)

    @classmethod
    def get_class_register(cls):
        return cls.__class_register.copy()


class AsyncBuffer(AbstractAsyncBaseClass):  # Class to manage memory asynchronous buffers.
    """ Implements memory buffers objects to perform actions or process data asynchronously.
        Each instance creates a async_buffer for a different object type (DataUploadCursor, ClosePACursor, etc)
        Each instance launches a writer thread that runs in a loop and processes the data as defined by the Cursor methods.
    """
    __lock = Lock()

    # List of AsyncBuffer instances that are active. Used by flush_all() method and by stop() method...
    __active_buffers = []  # ...to signal ok to SHUTDOWN to db writer via buffer_writers_stop_events Event.
    __qsize_threshold_def = 200     # default number of items in queue to trigger changes on thread_priority.

    def __init__(self, cursor_type=None, *, queue_max_size=None, use_gevent=False, autostart=False, precedence=1,
                 thread_priority=0, qsize_threshold=0):
        """
        @param cursor_type: Class. Mandatory. Cursor Class for cursor objects to bufferize. The cursor class should be
        defined in the same module file and implement the execute() method.
        @param thread_priority: float 0 - 10. 0: highest priority., Sets execution priority for async_buffer-processing thread
        (the thread that gets objects asynchronously from the queue and processes them).
        @param queue_max_size: amount of objects in queue before thread_priority is raised.
        @param precedence:  Closing order for thread. 0: closes last after all other buffers have been closed.
        @param autostart:
        """
        if cursor_type not in BufferAsyncCursor.get_class_register():
            raise TypeError(f'Invalid cursor class type {cursor_type}. Available types: '
                            f'{BufferAsyncCursor.get_class_register()}')
        self._cursor_type = cursor_type  # Object class for which the async_buffer is implemented.
        self._cursor_precedence = precedence if isinstance(precedence, (int, float)) and 0 <= precedence <= 3.0 else 1
        self._thread_helper = self.get_thread_implementation(use_gevent)(queue_max_size)
        self._is_stopped = True
        self._writer_thread = None
        self.__buffer_writer = None            # Para poder acceder a BufferWriter y cambiar thread_priority.
        self._result = None
        self._autostart = autostart
        self.__thread_priority = thread_priority        # if !=0, sets thread execution priority using sleep() feature.
        self.__qsize_threshold = qsize_threshold if isinstance(qsize_threshold, int) and qsize_threshold > 0 else \
            self.__qsize_threshold_def
        super().__init__()

        self._create_write_queue()  # Create the write queue, start the writer thread for the object AsyncBuffer.
        if self._autostart:
            self.start()

    def enqueue(self, *args, the_object=None, the_callable=None, **kwargs):
        """Puts a cursor object in the _write_queue defined for the class.
        *args, **kwargs are generic arguments to pass all the arguments required for the object.
        Two special arguments keyword arguments (Both optional. The names are reserved. DO NOT USE for other purposes):
            the_object: object that executes the callable.
            the_callable: function/method to be executed by the_object.
        format_item() MUST be implemented with the arguments passed to enqueue in order to create a valid buffer item.
        format_item() must pass an event (defined via thread_helper.event()) if the object is required to return results.
        execute() MUST be implemented to perform the actions required on the buffer object.
        """
        if not self._is_stopped:
            cursor = self._cursor_type.format_item(*args, event=self._thread_helper.event(), the_object=the_object,
                                                   the_callable=the_callable, **kwargs)
            if isinstance(cursor, self._cursor_type):
                self._write_queue.put(cursor)
                return cursor  # TODO(cmt):this is the only point of access to the cursor object from the outside world.
        return None     # returning None -> error in creation of cursor. TODO: Caller needs to handle the error.


    def _execute(self, obj):
        """ Object-specifc method to process the object (store in async_buffer, record to DB, etc).
        This method is called by the loop() method in BufferWriter class. There, all exceptions are handled and results
        are posted via set_result() back into the object's structure.
        @return: Whatever the specific execute method in the object class returns.
        """
        # obj es objeto del queue (DataUploadCursor, ProgActivityCursor, etc). Invoca execute() en esas clases.
        return obj.execute()

    def get_thread_implementation(self, use_gevent):
        return GreenletHelper if use_gevent else ThreadHelper

    def _create_write_queue(self):
        self._write_queue = self._thread_helper.queue()
        # krnl_logger.info(f'queue object for {self._cursor_type}: {self._write_queue}')

    def queue_size(self):
        return self._write_queue.qsize()

    def start(self):
        with self.__lock:
            if not self._is_stopped:
                return False  # Si el writer esta andando (_is_stopped=False), la llamada a start() retorna False.

        def buffer_writer():
            # Crea el objeto writer. Pasa asyncBuffer object y queue
            self.__buffer_writer = BufferWriter(self, self._write_queue, priority=self.__thread_priority,
                                                qsize_action_threshold=self.__qsize_threshold)
            krnl_logger.info(f'Starting writer object for {self._cursor_type}.')
            self.__buffer_writer.run()  # Con esta llamada entra al loop de extraccion de datos del queue

        # Crea thread. write_spooler es el target.
        self._writer_thread = self._thread_helper.thread(buffer_writer)

        with self.__lock:
            self._is_stopped = False
            self._writer_thread.start()                    # TODO(cmt): Aqui arranca el writer thread.
            db_name = self._cursor_type.writes_to_db()
            if db_name:
                # if not any(j.writes_to_db() == self._cursor_type.writes_to_db() for j in self.__active_buffers):
                # Si no existe Event en dict buffer_writers_stop_events, lo crea.
                if db_name not in self.buffer_writers_stop_events:
                    self.buffer_writers_stop_events[db_name] = Event()
                else:       # Si ya existe, resetea Event para el db-writer retornado por self.writes_to_db().
                    self.buffer_writers_stop_events[db_name].clear()
            self.__active_buffers.append(self)  # Internal count of all active buffers. []-> it's ok to shutdown
        return True

        # with self.__lock:
        #     self._is_stopped = False
        #     self._writer_thread.start()  # TODO(cmt): Aqui arranca el writer thread.
        #     if self._cursor_type.writes_to_db():
        #         if not self.__active_buffers:  # Si es primer async_buffer en llegar a __active_buffers, resetea Event.
        #             self.buffer_writers_stop_events[].clear()
        #     self.__active_buffers.append(self)  # Internal count of all active db-writers. []-> it's ok to shutdown
        # return True

    def stop(self):
        """DO NOT USE this func from different threads to avoid deadlocks. Use only for orderly SHUTDOWN of buffers. """
        krnl_logger.info(f'{self._writer_thread} stop() requested. Queue size:{self._write_queue.qsize()}. '
                         f'About to join()...')
        with self.__lock:
            if self._is_stopped:
                return False
            self._set_thread_priority(0)        # TODO(cmt): Sets thread_priority to 0 to quit sleep() loop.
            self._write_queue.put(SHUTDOWN)
            self._is_stopped = True
            if self in self.__active_buffers:
                self.__active_buffers.remove(self)
            db_name = self._cursor_type.writes_to_db()      # db_name = database name | False.
        t1 = perf_counter_ns()
        # TODO(cmt): join() effectively sits here until the _writer_thread.run() ends, processing all the pending items
        #  in the _writer_thread buffer and before resuming from this point on.
        self._writer_thread.join()     # join fuerza al thread que lanzo a writer a esperar que writer procese SHUTDOWN.
        t2 = perf_counter_ns()
        if db_name:
            if not any(j._cursor_type.writes_to_db() == db_name for j in self.__active_buffers):
                self.buffer_writers_stop_events[db_name].set()      # This line ALWAYS after the above join()!

        print(f'EEEHHHHHHHYYY stop(236) Just removed {self._writer_thread.name}. Remaining AsyncBuffers: '
              f'{self.__active_buffers}\nTime to execute join(): {(t2-t1)/1000} usec.')
        return True


    @classmethod
    def flush_all(cls):
        """ Flushes ALL buffer objects by forcing the processing of all enqueued items and setting them to stop(). """
        # Needs local copy of list because stop() modifies __active_buffers.
        # This call works when done from main thread: Then, each join() executed by j.stop() will halt main until all
        # items in the buffer have been processed and only then will it move to the next (higher priority) buffer.
        # Uses precedence to ensure DataUploadCursor is shutdown LAST so that no records are dropped in the DB upload.
        buffers_list = cls.__active_buffers.copy()
        buffers_list.sort(key=lambda x:x._cursor_precedence, reverse=True)
        for j in buffers_list:
            if not j._is_stopped:
                j.stop()  # This line BLOCKS HERE until ALL queued items for buffer in j are processed, then continues.

    def is_stopped(self):
        # krnl_logger(f'{self._cursor_type}.is_stopped() CALLED IN THREAD {threading.current_thread().ident}. '
        #             f'Callers: {callerFunction(getCallers=True)}')
        """ locks HERE can be troublesome due to many cursors calling asynchronously is_stopped(). Deadlocks can be
            can be frequent because setRecord/setRecords must check is_stopped() in the processing of every queue item.
            Solution: 1. Access _is_stopped attribute directly to avoid going into locks; 
                      2. Keep the join() call OUTSIDE the lock block in stop() """
        with self.__lock:
            return self._is_stopped

    def pause(self):
        with self.__lock:
            self._write_queue.put(PAUSE)

    def unpause(self):
        with self.__lock:
            self._write_queue.put(UNPAUSE)

    def reset(self):
        pass
        # self._cursor_type.reset()

    def _set_thread_priority(self, val=0):
        if self.__buffer_writer:
            self.__thread_priority = self.__buffer_writer.set_thread_priority(val)

    def _get_thread_priority(self):
        return self.__thread_priority

# ------------------------------------------------ End Class AsyncBuffer ------------------------------------- #


class BufferWriter(object):         # BufferWriter instances are created inside AsyncCursor class.
    """ Implements a Queue where database records to be uploaded to server are placed.
        Implements a dedicated thread that runs a function in a continuous loop that scans the queue for objects of
        specific type and sends the object data for writing to db. The write is performed calling the object's own
        _execute() method.
        Also implements PAUSE, UNPAUSE, SHUTDOWN conditions to administer the writing thread.
        Implements thread thread_priority: puts the BufferWriter thread to sleep using the fact that sleep() releases the
        thread while sleeping. Then, the longer the sleep, the less often the thread executes.
    """
    __slots__ = ('buffer_obj', 'queue', 'thread_priority', 'sleep_time', 'qsize_action_threshold', '__sleep_chunk',
                 '__restore_priority')
    __lock = Lock()
    # TODO(cmt): must fine-tune this value once the system is fully running. 5 - 10 should be ok.Some criteria could be:
    #  skip 1 round of all threads running before checking the queue again for a middle-priority thread (priority=5).
    #  With 7-8 threads running this will yield __BASE_SLEEP_TIME MULTIPLIER = 0.5.
    #  Thread Sleep Time logic: thread_priority <= 15 is cubed,
    #  between 15 and 20: self.thread_priority ** (self.thread_priority/(5-(self.thread_priority-15)*0.2))
    #  thread_priority = 15 -> 15**3 = 3375 switchintervals (16.8 secs with switchinterval = 0.005 sec).
    #  thread_priority=15.1, sleep time = 15.1**3.03 = 3756 switchintervals.
    #  thread_priority=16, sleep time = 16**3.33 = 10321 switchintervals (51 secs with switchinterval = 0.005 sec).
    #  thread_priority = 18, sleep time = 18**4.0909 = 136,523 switchintervals.
    #  thread_priority=20 -> sleep time = 20**5 = 3,200,000 switchintervals (4.44 hrs).
    __BASE_SLEEP_TIME = sys.getswitchinterval()  # sleeps for 1 switchinterval when priority=1. For design convenience.
    __MAX_SLEEP_TIME = 20 ** 5 + 1    # Max. possible sleeping time for a thread: 4.44 hours.

    def __init__(self, buffer_obj, queue_obj, *, priority=0, qsize_action_threshold=200):
        self.buffer_obj = buffer_obj  # buffer_obj es un objeto de clase AsyncBuffer. Accede a metodo _execute()
        self.queue = queue_obj

        # Implementation of thread_priority using sleep()
        self.sleep_time = 0
        self.__sleep_chunk = 2  # Value in seconds that the while loop below sleeps before checking for SHUTDOWN.
        self.thread_priority = 0
        self.qsize_action_threshold = qsize_action_threshold  # Number of objects in queue to adjust thread_priority.

        self.thread_priority = self.set_thread_priority(priority)
        self.__restore_priority = self.thread_priority     # stores original thread_priority, to restore after changes.


    def run(self):
        looping = None
        while True:
            # Adjusts thread priority based on present value of qsize()
            if self.thread_priority:
                queue_size = self.queue.qsize()
                priority = min(self.qsize_action_threshold / queue_size, self.thread_priority) if queue_size \
                    else self.thread_priority
                if queue_size > int(self.qsize_action_threshold / 4) and priority != self.thread_priority:  # starts moving thread_priority from 4 downward.
                    self.set_thread_priority(priority)
                    krnl_logger.info(f'Setting thread priority for {self.buffer_obj._cursor_type.__name__} to %f. '
                                     f'Queue size: %d.', priority, self.queue.qsize())
                elif queue_size <= min(5, self.qsize_action_threshold) and priority != self.__restore_priority:
                    self.set_thread_priority(self.__restore_priority)
                    krnl_logger.info(f'Restoring thread priority for {self.buffer_obj._cursor_type.__name__} to %d.'
                                     f' Queue size: %d.', self.thread_priority, self.queue.qsize())

            # Runs a locking or a non-locking queue.get() depending on thread_priority.
            try:
                looping = self.loop_func()
                if self.sleep_time:
                    # TODO(cmt): sleep() activates thread priority. Shorter sleep times -> thread runs more often. Must
                    #  run a while loop in order not to block the thread with a long sleep (must check for SHUTDOWN).
                    #  With this, checks thread_priority every second and if it changed to 0 (due to SHUTDOWN request),
                    #  breaks out from the for loop.
                    if self.sleep_time > self.__sleep_chunk:
                        sleep(self.sleep_time % self.__sleep_chunk)
                        for i in range(int(self.sleep_time/self.__sleep_chunk)):
                            if self.thread_priority == 0:   # thread_priority=0 set by AsyncBuffer.stop() to quit sleep.
                                break
                            sleep(self.__sleep_chunk)
                    else:
                        sleep(self.sleep_time)
            except ShutdownException:
                krnl_logger.info(f'{self.__class__.__name__} received shutdown request. Exiting.')
                if looping is not True:
                    self.buffer_obj.reset()
                return

    def loop_func(self):
        if self.thread_priority == 0:
            obj = self.queue.get()  # """ locking get(). This line waits here until an item is available in queue. """
        else:
            try:                    # """ Non-locking get(). """
                obj = self.queue.get_nowait()   # Cuando hay thread_priority, no bloquea, porque se bloquea en sleep().
            except queue.Empty:
                return True         # Returns to while loop in run()
        try:
            self.execute(obj)
            # print(f'\n         ^^^^^^^^^^^^ loop(): Just processed object {obj} from queue {self.queue} ^^^^^^^^^^^')
            return True          # Returns to while loop in run()
        except(AttributeError, NameError, TypeError, ValueError):
            pass

        if obj is PAUSE:
            krnl_logger.info('Buffer writer paused')
            return False
        elif obj is UNPAUSE:
            krnl_logger.error('Buffer writer received unpause, but is already running.')
        elif obj is SHUTDOWN:
            print(f'^^^^^^^^^^^^^^^^^ object SHUTDOWN just gotten from {self.queue}. ADIOS!! ^^^^^^^^^^^^^^^^^^')
            raise ShutdownException()
        else:
            krnl_logger.error('Buffer writer received unsupported object: %s', type(obj))
        return True


    def execute(self, obj):     # obj comes from queue.
        try:
            # print(f' EEEEEeeeeeeeeeee  about to execute():{obj} / args=')
            cursor = self.buffer_obj._execute(obj)   # obj contiene la fn execute() especifica y la data (args, kwargs).
            exc = None
        except (sqlite3.Error, AttributeError, NameError, TypeError, Exception) as execute_err:
            cursor = None
            exc = execute_err
        return obj.set_result(cursor, exc)

    def wait_unpause(self):
        obj = self.queue.get()  # While on a wait_unpause() thread_priority effectively goes to 0 (queue locking get()).
        if obj is UNPAUSE:
            krnl_logger.info('writer unpaused - reconnecting to database.')
            return True
        elif obj is SHUTDOWN:
            raise ShutdownException()
        elif obj is PAUSE:
            krnl_logger.error('Buffer writer received pause, but is already paused.')
        else:
            obj.set_result(None, WriterPaused())
            krnl_logger.warning('Buffer writer paused, not handling %s', obj)

    def set_thread_priority(self, val=0):
        if isinstance(val, (int, float)):
            if val < 1:
                self.thread_priority = 0
            elif val > 20:
                self.thread_priority = 20       # 20 will allow for 20**3 = 8000/2 thread switches between runs.
            else:
                self.thread_priority = val
            with self.__lock:
                self.sleep_time = min(self.thread_priority ** (3 if self.thread_priority <= 15 else
                                      self.thread_priority ** (self.thread_priority/(5-(self.thread_priority-15)*0.2)))
                                      * self.__BASE_SLEEP_TIME, self.__MAX_SLEEP_TIME) if self.thread_priority else 0
        return self.thread_priority         # Retorna thread_priority porque se necesita el valor en AsyncBuffer.

