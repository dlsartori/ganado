import krnl_custom_types
from krnl_custom_types import getRecords
from krnl_abstract_class_animal import Animal
import datetime
from krnl_geo_new import Geo
from krnl_device import Device
from krnl_bovine import Bovine
from krnl_config import DB_REPLICATE, tables_and_binding_objects, tables_and_methods, register_terminal, TERMINAL_ID, time_mt
from krnl_db_access import init_db_replication_triggers
from krnl_sqlite import getFldCompare
from krnl_threading import checkTriggerTables

maindbID = 'c1434644fb73439e8006acf03ca25df4'
get_trigger_list = 'select * from sqlite_master where type = "trigger" ; '
drop_trigger = 'drop trigger "Trigger_UPDATE_Animales"; '
update_str = 'UPDATE "Animales" SET "ID_Animal Madre" = "MOMMA" WHERE "ID_Animal"=1'
insert_str = 'INSERT INTO "Animales" (UID_Objeto,"Fecha De Nacimiento","FechaHora Registro","MachoHembra","ID_Clase De'\
             ' Animal") VALUES ("cdfea5ca723441b6afbfc87527bebf06","2022-02-11 00:00:00","2018-05-01 01:01:01", "f",1 )'
sys_db_id_trigg = 'UPDATE "_sys_terminal_id" SET "Terminal_ID" = "MOMMA" WHERE ID_ROW=3214041033; '

'CREATE TRIGGER ro_cols BEFORE UPDATE OF "Nombre Referencia", "FechaHora Registro", "ID_Clase De Animal", ' \
 'Equivalencias ON "Animales Sanidad Equivalencias" FOR EACH ROW BEGIN SELECT RAISE (FAIL, "read only data"); END'


if __name__ == '__main__':
    print(f'\n ----------------------------------  Database UID is : "{TERMINAL_ID}" ----------------------------------\n')
    if DB_REPLICATE:
        trigger_tables = init_db_replication_triggers()
        print(f'INSERT/UPDATE Triggers created for: {trigger_tables}')
        print(f'tables_and_binding_objs: {tables_and_binding_objects}\ntables_and_methods:{tables_and_methods}.')


    # checkTriggerTables()
    # Animal._processDuplicates()
    # val = getFldCompare('tblAnimales.fldID', 'tblLinkAnimalesActividades.fldFK')
    # now = time_mt('datetime')
    # lower_date = now - datetime.timedelta(days=580)
    # tblRA = getRecords('tblAnimalesRegistroDeActividades', lower_date, now, 'fldTimeStamp', '*')



# 'cdfea5ca723441b6afbfc87527bebf06'
# '78f1d77d3b174c9f9e57e2dacf12ee8f'
# 'cded19c1044247808dd1fb72bf3d299a'
# '4114bf3891db43ad95b5ecb3b049f170'
# 'a5b8ac644b54441e85e73a202b2249f6'
# '01ce0949acdb42618900411d3c1fad5d'
# '78f1d77d3b174c9f9e57e2dacf23eedf'
# '78f1d77a3b174c9f9e57e2dacf12ee68'
# '78f1d7783b174c9f9e57e2dacf12eea9'
# '78f1d7703b174c9f9e57e2dacf12eeba'
# '78f1d77b3b174c9f9e57e2dacf12eecb'
# '78f1d77d3b174c9f9e57e2dacf12aadf'
# '78f1d77d3b1743f59e57e2dacf12aadf'

