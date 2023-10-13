import sys
from datetime import datetime
from time import *
from krnl_config import time_mt
import inspect
from krnl_sqlite import *
import time
# from time import clock_gettime, clock_settime, get_clock_info


from krnl_config import fDateTime
from custom_types import DataTable, dbRead
from krnl_parsing_functions import setRecord


if __name__ == '__main__':

    # conn = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    # cur = conn.cursor()
    # t = datetime(2045, 1, 1, 12, 1, 1)
    # fecha = '1900-01-01'
    # cur.execute('CREATE TABLE foo(bar TIMESTAMP, fecha DATE)')
    # cur.execute("INSERT INTO foo(bar, fecha) VALUES (?,?)", (t, fecha))
    # # cur.execute("INSERT INTO foo(bar) VALUES (?)", ('2022-09-11 22:58:58',))
    # cur.execute("SELECT * FROM foo")            # AQUI INVOCA LAS FUNCIONES DE CONVERSION!!
    # data = cur.fetchall()
    # data = list(zip(*data))
    # print(f'Result INSERT: {data}')
    #
    # # Ciclo de update.
    # tbl = 'foo'
    # values = (datetime.now(), fecha)
    # strSQL = f' UPDATE {tbl} SET bar=? WHERE fecha=? '
    # print(f'strSQL *** UPDATE ***: {strSQL}')
    # cur.execute(strSQL, values)
    # cur.execute("SELECT * FROM foo")
    # data1 = cur.fetchall()
    # data1 = list(zip(*data1))
    # print(f'Result UPDATE: {data1}')
    # print(f'==========================================Fin pruebas Memory Database ==============================\n')

    """ Warning
    Because naive datetime objects are treated by MANY datetime methods as local times, it is preferred to use aware 
    datetimes to represent times in UTC. As such, the recommended way to create an object representing a specific 
    timestamp in UTC is by calling datetime.fromtimestamp(timestamp, tz=timezone.utc). 
    Also: CLOCK_MONOTONIC, CLOCK_MONOTONIC_RAW don't count suspended time. USE CLOCK_BOOTTIME.
    CLOCK_REALTIME           = 0 # Identifier for system-wide realtime clock.
    CLOCK_MONOTONIC	         = 1 # Monotonic system-wide clock.
    CLOCK_PROCESS_CPUTIME_ID = 2 # High-resolution timer from the CPU
    CLOCK_THREAD_CPUTIME_ID	 = 3 # Thread-specific CPU-time clock. 
    CLOCK_MONOTONIC_RAW      = 4 # Monotonic system-wide clock, not adjusted for frequency scaling. 
    CLOCK_REALTIME_COARSE    = 5 # Identifier for system-wide realtime clock, updated only on ticks. 
    CLOCK_MONOTONIC_COARSE   = 6 # Monotonic system-wide clock, updated only on ticks. 
    CLOCK_BOOTTIME	         = 7 # Monotonic system-wide clock that includes time spent in suspension. 
    CLOCK_REALTIME_ALARM     = 8 # Like CLOCK_REALTIME but also wakes suspended system.
    CLOCK_BOOTTIME_ALARM     = 9 # Like CLOCK_BOOTTIME but also wakes suspended system.

    python-monotonic-time only uses GetTickCount() or GetTickCount64().

    It is important to decide which clock is used for the Python time.monotonic() because it may change the design of 
    the PEP 418. If we use GetTickCount() for time.monotonic(), we should use QueryPerformanceCounter() for 
    time.highres(). But in this case, it means that time.highres() is not a simple "try monotonic or falls back to 
    system time", but may use a different clock with an higher resolution. So we might add a third function for the 
    "try monotonic or falls back to system time" requirement.
    Python implements time.clock() using QueryPerformanceCounter() on Windows.
    https://groups.google.com/g/dev-python/c/q9E_9W445h4
    """
    print(f'========================================= Some Time misc. tests =========================================')
    naive = datetime.now()
    localNow = localtime()
    print(f'UTC time now  : {gmtime()} ')
    print(f'datetime.now(): {naive}, localtime(): {localNow}, ')
    print(f'timezone is: {localNow.tm_zone}, hours offset:{localNow.tm_gmtoff / 3600}')
    a = mktime(localNow)
    print(f'mktime(localNow): {a}')
    print(f'datetime(localNow) is: {datetime.fromtimestamp(a)}')

    print(f'\n  -----------------------------------------------------------------------------------------------------')
    t = time.time()
    a = time.strftime('%Y-%m-%d %H:%M:%S %Z %z', time.localtime(t))   # localtime(), gmtime() don't take fract. seconds
    a1 = datetime.fromtimestamp(t)
    print(f't={t} seconds since epoch.')
    print(f'&&&&& localtime(t): {a}')
    b = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(t))
    b1 = datetime.fromtimestamp(t)
    print(f'&&&&&    gmtime(t): {b}')
    t_local = a1.timestamp()
    t_utc = b1.timestamp()
    print(f'\nSeconds from timestamp t={t}. tlocal={t_local}, t_utc={t_utc}\n')

    sleep_time = 0.5
    before = monotonic()        # TODO: USAR perf_counter aqui para determinar SYS_BASE_SEC
    base_time = time.time()
    after = monotonic()

    print(f'base_time (Reference Time in seconds)={base_time}')
    print(f'monotonic(before): {before}; monotonic(after): {after}  => monotonic(average)={(before+after)/2}')

    sleep(sleep_time)
    pastSleep = monotonic()
    t_ = time.time()
    print(f'time after sleep({sleep_time}): {t_} / monotonic after sleep({sleep_time}): {pastSleep}')

    print(f'--------------------------------------------------------------------------------------------------')
    dates = ['2025-09-09', '2025-09-09 12:12', '2025-09-09 12:12:12', '2025-09-09 12:12:12:00',  '2025-09-09 12:12:12:00']
    for j in dates:
        try:
            print(f'Date:{j} / Conversion strptime(): {datetime.strptime(j, fDateTime)}')
        except TypeError:
            print(f'Conversion {j} failed...')

