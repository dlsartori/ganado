from krnl_config import *
import pandas as pd
from pandas.testing import assert_frame_equal     # allows comparison of 2 frames, giving correct result for nan values.
from krnl_custom_types import getTblName, getFldName, getrecords
from krnl_db_query import SQLiteQuery
from krnl_db_access import setrecords
from krnl_bovine import Bovine
from krnl_person import Person

if __name__ == '__main__':
    dfAlta = pd.read_csv('alta_2024.csv')           # 1 linea para leer un csv.

    fldExit = getFldName("tblAnimales", "fldDateExit")
    sql = f'SELECT * FROM "{getTblName("tblAnimales")}" WHERE "{fldExit}" == 0 OR "{fldExit}" IS NULL; '
    # dfAnimals = pd.read_sql_query(sql, SQLiteQuery().conn, chunksize=PANDAS_READ_CHUNK_SIZE)
    # setrecords(dfAnimals)
    count = Bovine.getCount()
    dfAnimals = pd.read_sql_query(sql, SQLiteQuery().conn, chunksize=PANDAS_READ_CHUNK_SIZE)
    animal_uid = 'fc1703bc1f174c8099635dc97e4dc74c'
    p = Person.getObject('20241696n')
    df = next(dfAnimals)
    # setrecords(df)
    animal_obj = Bovine.getObject('fc1703bc1f174c8099635dc97e4dc74c', load_identifiers=True)
    inv = animal_obj.inventory.get()
    # animal_obj.inventory.set(df, date=time_mt('dt'))
    if isinstance(dfAnimals, pd.DataFrame):
        dfAnimals = (dfAnimals, )
    for chunk in dfAnimals:
        print(f'dfAnimales columns: {chunk.columns}.')
        print(f'Added attributes for DataFrame: tbl_name: {chunk.db.tbl_name} / fields: {chunk.db.field_names}.\n')

        # lee 3 columnas, pasando nombres case-oblivious y un indice de la lista de campos.
        b = chunk.loc[:, chunk.db.col(' fld_duPlicaTION_INDEx', '\n fecha DE \t nacimiento ', 3, ' FLDTIMESTAMP\t\n ')]
        c = chunk.loc[4, chunk.db.col('fldtimestamp')]
        print(f'b-columns: {b.columns}\n')

    df1 = getrecords('tblAnimalesTiposDeAltaBaja', 'fldID', 'fldName',
                     where_str=f'WHERE "{getFldName("tblAnimalesTiposDeAltaBaja", "fldAltaBaja")}" == "Alta" ')
    print(f'df1:\n{df1}')
    # print(f'----------------- df1.db.reverse_names(): {df1.db.reverse_names()}')

    tbl = pd.DataFrame.db.create('Link Animales   Actividades ')
    tbl.loc[0, tbl.db.col('fldid', 8, ' id_animal', 'fldexecuteData ')] = [4, 5, 6, 12]
    print(f'tbl: {tbl}')
    print(f'tbl index 0:\n{tbl.loc[0]}')




