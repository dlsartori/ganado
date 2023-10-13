from krnl_db_access import *
from time import sleep
import threading
# from krnl_db_functions import setRecord
MAIN_DB_NAME = "GanadoSQLite.db"

if __name__ == '__main__':
    writeObj = SqliteQueueDatabase(MAIN_DB_NAME, autostart=True)
    writeObj.start()
    writeObj.pause()
    print(f'Threads: {threading.enumerate()}')
    sleep(1)
    writeObj.unpause()
    strSQL = 'INSERT INTO "Data Animales Actividad Inventario" ("ID_Data Inventario",  "ID_Actividad") VALUES  (NULL, 22000);'
    strSQL2 = 'SELECT LAST_INSERT_ROWID()'
    data = writeObj.execute_sql(strSQL).lastrowid         # Esta llamada se pasa al writer y se ejecuta desde bckd Thread
    lastRow = writeObj.execute_sql(strSQL2).lastrowid     # Esta llamada (por no ser INSERT) se ejecuta desde Main Thread
    # Esto puede generar errores de acceso: en este caso, lastRow leido es (0, ) porque el execute para lastRow se hace
    # con un cursor distinto del general en el primer call para data.
    # TODO:Hasta encontrar solucion SOLAMENTE utilizar el writer para INSERT, UPDATE, REPLACE, DELETE. El resto de las
    #  los queries de lectura/consulta, hacerlos desde foreground un una conexion propia.

    print(f'{data} / lastRow = {lastRow}')

