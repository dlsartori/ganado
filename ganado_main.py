from time import sleep
from krnl_threading import FrontEndHandler, IntervalFunctions
from krnl_async_buffer import AsyncBuffer
from krnl_db_access import SqliteQueueDatabase
from fe_bovine import bovine_frontend


if __name__ == '__main__':

    # Do some stuff here if needed.

    bovine_fe_obj = FrontEndHandler(bovine_frontend)  # Can pass *args, **kwargs for bovine_frontend here.
    FrontEndHandler.main_loop_launch()

    # Launch all IntervalTimer threads defined for the system operation.
    print(f'MAIN THREAD > Launching all IntervalTimer threads...')
    for f in IntervalFunctions.getFuncs():
        f.startThread()
        print(f'MAIN THREAD > {f.thread.name} Launching {f.thread.function.__name__}() / Interval: {f.thread.interval}')

    while True:
        loop = None
        try:
            # do the main loop here: Launch front end modules, check on their status, stop, re-start  and exit...
            sleep(1)
            loop = bovine_fe_obj.get_result()
        except(KeyboardInterrupt, Exception) as e:
            print(f'Received exception: {e}')
            loop = False
        finally:
            if loop:
                continue
            else:
                break

    """ Orderly system shutdown:                                                                     """
    FrontEndHandler.main_loop_quit()        # Closes ALL front end modules and quits main front-end loop.
    for f in IntervalFunctions.getFuncs():
        f.killThread()                      # Exit all background threads.
    AsyncBuffer.flush_all()  # Flushes all AsyncBuffer queues, processing all objects in them.
    SqliteQueueDatabase.stop_all_writers()  # Processes all pending database cursor objects (mostly db writes).
    # writeObj.stop()                         # Shuts down the db writing queue. Closes DB connection.

    print('bye, bye...')

