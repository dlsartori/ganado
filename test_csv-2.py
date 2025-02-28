from krnl_config import *
import pandas as pd
from pandas.testing import assert_frame_equal     # allows comparison of 2 frames, giving correct result for nan values.
from krnl_custom_types import getTblName, getFldName, getrecords
from krnl_db_query import SQLiteQuery
from krnl_db_access import setrecords
from krnl_bovine import Bovine
from krnl_person import Person


# def json_parser(data):
#     if data:
#         return json.loads(data)  # For now, throw JSON Exception to address malformed json during debugging.
#     return data

# def json_parser(data):
#     if data:
#         a = json.loads(data)
#         print(f'data: {a} / {type(a)}')
#         return a
#     return data


if __name__ == '__main__':
    count0 = Bovine.getCount()
    print(f'\n========================================= Total count before: {count0} ==============================\n')

    dfperson = Person.person_by_name(name=' alejandro   armando', last_name=' gonzalez Buyatti  ', enforce_order=False)
    exit(0)
    objects = Bovine.alta(csv='alta_2024-01.csv')

    count = Bovine.getCount()
    print(f'\n========================================= Total count before: {count0} ==============================\n')
    print(f'\n========================================= Total count after: {count} ==============================')

    # p = Person.getObject('20241696n')
    # animal_obj = Bovine.getObject('fc1703bc1f174c8099635dc97e4dc74c', load_identifiers=True)
    # inv = animal_obj.inventory.get()
    # animal_obj.inventory.set(df, date=time_mt('dt'))




