import collections
import sqlite3
from krnl_exceptions import *
# from collections.abc import Iterable
import functools
import itertools
import pandas as pd
import numpy as np
import re
from datetime import datetime
from pandas.testing import assert_frame_equal  # allows comparison of 2 frames, giving correct result for nan values.
import threading
from threading import Lock
from krnl_config import strError, callerFunction, lineNum, moduleName, uidCh, db_logger, MAIN_DB_NAME, NR_DB_NAME, \
     DATASTREAM_DB_NAME, json, print, DISMISS_PRINT, fDateTime, krnl_logger
import collections
import collections.abc

SQLiteDBError = sqlite3.DatabaseError
SQLiteError = sqlite3.Error
SYS_TABLES_NAME = '_sys_Tables'
SYS_FIELDS_NAME = '_sys_Fields'
nulls_with_None = {np.nan: None, float('nan'): None, pd.NA: None}      # Replacements for pandas/numpy NaN

# ----------------------- Accessors and decorators to mirror-image DataFrames to database tables ----------------
@pd.api.extensions.register_dataframe_accessor("db")  # Accessor to pd.DataFrame mirroring db records. Low level stuff.
# Executed as df.db.method() or DataFrame.db.method(). Example: DataFrame.db.create(tbl_name='tblAnimales').
class DatabaseTableAccessor:
    """
    Implements an association of database tables with pd.Dataframes.
    Dataframes read from database may eventually include columns that are not part of the database table associated to
    it.
    The results from the accessor will only include valid db table field names (for now, still in review).
    """
    __slots__ = ('_obj', '__tbl_name', '__db_tbl_name', '__field_names', '__db_name', '__tbl_names_list',
                 '_initialized')
    def __init__(self, pandas_obj):     # pandas_obj: inserted by caller object when calling as df_obj.df.method()
        self._validate(pandas_obj)   # True if pandas object with db Accessor already initialized.
        self._obj = pandas_obj  # passed from DataFrame obj. as 1st arg after self. Used for fetching dataframe data.
        self.__tbl_name = None
        self.__db_tbl_name = None
        self.__field_names = None       # dict {tbl_name: {fldName: dbFldName}, }
        self.__db_name = None
        self.__tbl_names_list = ()      # List of db_table_names when more than 1 table name passed.


    @staticmethod
    def _validate(obj):   # MUST be static as it's called on the pandas_obj passed BEFORE setting the self attributes.
        # Must raise AttributeError if validation doesn't pass.
        pass
        # return getattr(obj, 'db.tbl_name', False)

    @property
    def tbl_name(self):
        return self.__tbl_name

    @tbl_name.setter
    def tbl_name(self, val):
        self.__tbl_name = val

    @property
    def db_tbl_name(self):
        return self.__db_tbl_name

    @db_tbl_name.setter
    def db_tbl_name(self, val):
        self.__db_tbl_name = val

    @property
    def field_names(self):
        # For compatibility returns: 1 tbl -> dict / Multiple tables: dict of dicts.
        return self.__field_names.get(self.__tbl_name, self.__field_names)

    @property
    def tbl_names_list(self):
        return self.__tbl_names_list

    @property
    def tbl_names(self):
        return self.__tbl_names_list

    def initialize(self, tbl_names_list, *, con=None, db_name=None, cols_multi_index=False, multi_select=False,
                   rename_cols=None):
        """ Executed by create() method and by read_sql_query() @decorator. Initializes all attributes of the Acessor.
            Renames all column names to key names (starting with fld).
            IMPORTANT: If there are repeat column names and col_multi_index=True, creates MultiIndex columns that must
            be accessed with a (tblName, fldName) tuple. This can be the case of dataframes created from JOIN queries.
            @param rename_cols: Renames columns of created dataframe to key names (starting with 'fld'). Default: True.
                                Column names with no match in db table structure are left unchanged.
            @param multi_select: True -> More than 1 table in SELECT statement. Cannot assign tbl_name to Accessor. A
                                 warning is issued and __tbl_name, __db_tbl_name are left in None.
            @param con: database connection. None -> Default: MAIN_DB_NAME.
            @param db_name: db name the tables are to be associated to. None -> falls back to con argument.
            @param tbl_names_list: (str). set of table names found in the sql string passed to read_sql_query().
            @param cols_multi_index: True: MultiIndex columns if repeat col names.
                                     False: raises ValueError if repeat col names.
            @return: None
        """
        if db_name:
            self.__db_name = db_name if db_name in _DataBase.active_databases() else MAIN_DB_NAME
        elif isinstance(con, sqlite3.Connection):
            # Pulls db name from connection. To be used in getFldName(), getTblName() funcs.
            # TODO(cmt). IMPORTANT: The Accessor (and the system, in general) work with only 1 database per connection!
            db_path = con.execute('PRAGMA DATABASE_LIST; ').fetchone()[2]
            self.__db_name = next((name for name in _DataBase.active_databases() if name.lower() in db_path.lower()),
                                   MAIN_DB_NAME)
        else:
            self.__db_name = MAIN_DB_NAME  # Defaults to MAIN_DB_NAME if db connection is not valid.

        self.__tbl_names_list = [getTblName(db_table_name=j, db_name=self.__db_name) if not j.lower().startswith('tbl')
                                 else j for j in tbl_names_list]
        if any(strError in j for j in self.__tbl_names_list):
            self.__tbl_names_list = ()   # Error in table names. Leaves  all attributes as None, (). Exit.
            return

        # Sets valid __tbl_name, __db_tbl_name ONLY if 1 name in tbl_list.
        if not multi_select:
            # Sets attributes only if SELECT statement includes only 1 table.
            self.__tbl_name = self.__tbl_names_list[0]
            self.__db_tbl_name = getTblName(self.__tbl_name, db_name=self.__db_name)

        else:
            krnl_logger.warning(f'DataFrame "db" Accessor: Multiple tables present in SELECT statement. Table name '
                                f'cannot be assigned to DataFrame.\n'
                                f'Use db.tbl_names_list to access dataframe table names.')

        # __field_names = {tblName: {fldName, dbFldName,} ,} contains full field list for all tables in _tbl_names_list.
        self.__field_names={tbl: getFldName(tbl, '*', mode=1, db_name=self.__db_name) for tbl in self.__tbl_names_list}

        # Rename all columns to fldNames. When multiple tables, there may be repeat names.
        # Column names with no match for field key (a name starting with 'fld') are left unchanged.
        rename_cols = rename_cols if rename_cols is not None else True
        if self.__field_names and rename_cols:
            self._obj.rename(columns={v: k for fdict in self.__field_names.values() for k, v in fdict.items() if v in
                                      self._obj.columns}, inplace=True)

            # Sets MultiIndex columns if required.
            if cols_multi_index:
                # Implements MultiIndex Columns here, accessed via tuple (tblName, fldName).
                cols_MultiIndex = [(tbl, f) for tbl in self.__field_names for f in self.__field_names[tbl].keys()]
                self._obj.columns = pd.MultiIndex.from_tuples(cols_MultiIndex)

            # Check if any renamed col names are equal. If so and cols_multi_index=False warns the user.
            if len(set(self._obj.columns)) != len(list(self._obj.columns)) and not cols_multi_index:
                krnl_logger.warning(f'DataFrame "db" Accessor: Repeat column names are present in dataframe. "db" '
                                    f'Accessor NOT initialized for database access.')

        self._obj = self._obj.convert_dtypes()    # Forces proper dtype on all columns.



    def col(self, *args, db_names=False, full_names=False):
        """
        Returns dataframe column name (str, key name, starting with 'fld') or tuple if multiple args passed.
        Returns column names in dictionary form {fldName: dbFldName, } for tbl_name for all names passed in args when
        full_names=True.
        @param full_names: True -> returns {fldName, dbFldName, } dictionary.
        @param db_names: returns column database name. Default: False.
        @param args: field names (key or db field names).
        @return: {fldName: dbFldName, } with proper key and db field names for tbl_name (tuple) or empty tuple if no
                 field names found for table tbl_name.
        """
        if self.tbl_name is None:
            return ()
        valid_names = {}
        for fname in args:
            if isinstance(fname, str):
                fname = (" ".join(fname.split())).lower()  # Removes all excess leading, trailing and inner whitespace.
                if fname.startswith('fld'):
                    fld_name = next((j for j in self._obj.columns if j.lower() == fname), None)
                    pair = {fld_name: d[fld_name] for d in self.__field_names.values()} if fld_name else {}
                else:
                    db_fld_name = next((v for d in self.__field_names.values() for k, v in d.items() if
                                        fname == v.lower() and k in self._obj.columns), None)
                    pair = {k: v for d in self.__field_names.values() for k, v in d.items() if v == db_fld_name}

                if pair:
                    valid_names.update(pair)
            elif isinstance(fname, int):
                key = self._obj.columns[fname]
                valid_names.update({key: next((v for d in self.__field_names.values() for k, v in d.items()
                                               if k == key), None)})
            else:
                continue    # ignores invalid types.
                # raise TypeError(f'Pandas DatabaseTableAccessor error: invalid field name type {type(fname)}.')
        if len(valid_names) == 1:
            return list(valid_names.keys())[0] if not db_names else list(valid_names.values())[0]
        elif len(valid_names) == 0:
            return {}
        else:
            if full_names:
                return valid_names    # Returns only valid names for tbl_name.
            return tuple(valid_names.keys()) if not db_names else tuple(valid_names.values())
    cols = col

    def dbcolname(self, *args):
        """Returns 1 or more column names for the columns defined in the dataframe.
        A proper database column name (str), NOT double-quoted. Caution!!
        @return: str if 1 col name; tuple for multiple column names. """
        return self.col(*args, db_names=True)


    @staticmethod
    def create(tbl_name: str, *, data=None, index=None, columns=None, dtype=None, copy=None, db_name=None) \
            -> pd.DataFrame:
        """ Creates an empty DataFrame setting columns with ALL columns from datables tbl_name. The dataframe is
        composed of:
            - Row index: pandas-assigned index.
            - Column names: field key names belonging to table tbl_name (start with 'fld'). If none passed via
                            columns argument assigns all the fields in the table as column names.
            - data: data argument. if None: creates empty dataframe. Equivalent to creating an empty DataTable.
            data MUST BE IN THE FORMAT FOR DataFrame constructor (dict, list or other iterables).
        @param db_name: Database name to which tbl_name is to be associated.
        @param tbl_name: Database table key name (starts with "tbl") or actual table name (Animales, Caravanas, etc.).
         @param data: data to populate the dataframe, in pandas-accepted format (list, dict).
        @param columns: (list, tuple, set) of str with database field names or key field names (starting with "fld").
        If None -> creates dataframe with ALL fields existing in tbl_name.
        columns items (str) must match exactly the key field names or db field names in table tbl_name.
        All other params passed for compatibility with pd.DataFrame usage.
        Meant to be called by pd.DataFrame class, not by instances (although it should work anyway).
        """
        db_name = db_name if db_name in _DataBase.active_databases() else MAIN_DB_NAME
        if isinstance(tbl_name, str):
            tbl_name = " ".join(tbl_name.split())    # Removes all leading, trailing, middle whitespace characters.
            if not tbl_name.lower().startswith('tbl'):
                tbl_name = getTblName(db_table_name=tbl_name, db_name=db_name)
                if strError in tbl_name:
                    raise ValueError(f"Pandas DatabaseTableAccessor Error: Invalid database table name {tbl_name}.")
            fld_names = getFldName(tbl_name, '*', mode=1, db_name=db_name)  # {fldName: DBFldName, }
            if not isinstance(fld_names, dict):
                raise ValueError(f"Pandas DatabaseAccessor Error: Invalid database table name {tbl_name}.")

            if data:
                df = pd.DataFrame(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
            else:
                if columns and isinstance(columns, (tuple, list, set)):
                    columns_lower = [j.lower() for j in columns]
                    flds = {k: v for k, v in fld_names.items() if (k.lower() in columns_lower or
                                                                   v.lower() in columns_lower)}
                    if flds:
                        # fld_names passed via columns arg. Creates dataframe ONLY with field names valid for tbl_name.
                        fld_names = flds
                # df = pd.DataFrame(data=data, index=index, columns=list(fld_names.keys()), dtype=dtype, copy=copy)
                df = pd.DataFrame({c: pd.Series(dtype=object) for c in fld_names.keys()}, index=index, copy=copy)

            df.db.initialize((tbl_name, ), db_name=db_name, rename_cols=False)     # This line accesses db.__init__()

            # Converts all pd.Timestamp columns to datetime data  ///Deprecated. Code left here only for reference.///
            # for col in df.columns:  # Converts all pd.TimeStamp objects to string.
            #     if 'datetime' in df.dtypes[col].__class__.__name__.lower():
            #         # df[col] = df[col].dt.strftime(fDateTime)  # uses TimeStamp Series dt Accessor.
            #         df[col] = pd.Series(df[col].dt.to_pydatetime(), dtype=object)
            return df
        raise TypeError(f"Pandas DatabaseTableAccessor Error: Invalid database table name {tbl_name}.")


# ---------------------------------- END class DatabaseTableAccessor ---------------------------------------------- #

def wrapper_pandas_read_sql(f):
    """ Decorator for pd.read_sql_query and pd.read_sql_table to setup tbl_name, field_names attributes in DataFrame.
    Uses a generator that wraps the df iterator returned by pd.read_sql_query() to set parameters in db accessor obj.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        f""" @param args: arg[0]: sql statement. Used to pull table name.
             @param params: 'rename_cols': 'true' | 'false' (dict). Forces renaming of table columns on dataframe. 
                             Default: 'True'
             @param kwargs:
                        'convert_dates': False -> skips conversion of date columns to pd.Timestamp. Default: True.        
             @return: pd.DataFrame or iterator object (if chunksize argument != None)
        """
        if f.__name__ in 'read_sql_query':          # Covers both -> ('read_sql', 'read_sql_query'):
            tbl_names_list = tables_in_query(args[0])
            # multiple_tables: More than 1 table selected in query. Cannot assign a table name to db accessor.
            multiple_tables = len(tables_in_query(args[0], inspect_select_tables=True)) > 1
        elif f.__name__ == 'read_sql_table':
            tblname: str = args[0]
            if tblname.startswith('tbl'):
                # Converts name to proper table db name to pass on to read_sql_table()
                args = list(args)
                args[0] = getTblName(tblname)
            else:
                dbname = getTblName(db_table_name=tblname)
                tblname = dbname if strError not in dbname else None
            tbl_names_list = (tblname, )
            multiple_tables = False     # This func. works with 1 table only.
        else:
            return f(*args, **kwargs)

        # Uses read_sql_query 'params' argument to pass 'rename_cols' propietary argument down to initialize() func.
        params = kwargs.get('params', None)
        if isinstance(params, dict):
            rename_cols = params.pop('rename_cols', None)  # Pops 'rename_cols' from dict to pass on to initialize()
        else:
            rename_cols = None
        if rename_cols is not None:
            # rename_cols is passed as str in params dict. Must be converted to bool.
            rename_cols = False if any(j == rename_cols.lower().strip() for j in
                                       ('0', 'false', 'none', 'na', 'no', 'nan')) else True

        coerce_float = kwargs.get('coerce_float', None)
        kwargs['coerce_float'] = coerce_float if coerce_float is not None else False  # Avoid automatic cast to float.
        if kwargs.get('convert_dates', True):
            # Forces importing date columns as pd.Timestamp, to avoid multiple conversions in the kernel code.
            # Usage:
            #  pd.read_sql_query(sql_str, convert_dates=True/False) -> A warning will appear, but the arg. is passed on.
            cur = args[1].cursor()   # Gets a cursor. db connection is arg[1] in all 3 pandas db read functions.
            tbl_info = cur.execute(f'PRAGMA TABLE_INFO("{tbl_names_list[0]}"); ').fetchall()
            type_idx = next((i for i, val in enumerate(cur.description) if 'type' in val), None)
            name_idx = next((i for i, val in enumerate(cur.description) if 'name' in val), None)
            if type_idx is None or name_idx is None:
                raise ValueError(f'ERR_DBAccess: Cannot read from db. sql=PRAGMA TABLE_INFO("{tbl_names_list[0]}")')
            date_cols = [f'"{j[name_idx]}"' for j in tbl_info if 'timestamp' in j[type_idx].lower()]
            if date_cols:
                # Sets parse_dates so that all date columns (type=TIMESTAMP in SQLite) are imported as pd.Timestamp.
                kwargs['parse_dates'] = dict.fromkeys(date_cols, fDateTime)

        df = f(*args, **kwargs)  # Calls sql_read func. df is DataFrame or iterator object when chunksize > 0.

        if isinstance(df, pd.DataFrame):
            df.db.initialize(tbl_names_list, con=args[1], multi_select=multiple_tables, rename_cols=rename_cols)
            return df
        else:               # df is a generator.
            """ This was it!: wrapping the df iterator inside a custom generator that sets up tbl_name, field names.
            Defined here to use the closure and pass tbl_name, db_tbl_name, fld_names values for o.db Accessor.
            No need to use hash_values or other tricks. 
            """
            # return DBIterator(df, tbl_names_list, con=args[1], multi_select=multiple_tables)

            def generator(iterator):
                for o in iterator:
                    # Sets up DatabaseTableAccessor attributes for object o. dataframe cols are renamed in initialize().
                    o.db.initialize(tbl_names_list, con=args[1], multi_select=multiple_tables, rename_cols=rename_cols)
                    yield o  # Sends o back to caller. Sits here until next call to next().That's what generators do.
            return generator(df)

    return wrapper

# Decorates 3 main pandas read sql functions.
pd.read_sql_query = wrapper_pandas_read_sql(pd.read_sql_query)
pd.read_sql_table = wrapper_pandas_read_sql(pd.read_sql_table)
pd.read_sql = wrapper_pandas_read_sql(pd.read_sql)


class DBIterator(object):
    """ Implements an iterator class that initializes each iterator object (dataframes) with db Accessor attributes
    before yielding it out.
    Runs like a charm, but not needed for now...
    """
    def __init__(self, iterator, tbl_names_list, con=None, multi_select=None):
        self.__iterator = iterator
        self.__tbl_name_list = tbl_names_list
        self.__con = con or SQLiteQuery().conn
        self.__multi_select = multi_select

    def __iter__(self):
        ittr = itertools.tee(self.__iterator, 2)
        self.__iterator = ittr[0]
        for df in ittr[1]:
            # Sets up DatabaseTableAccessor table names. The rest is already setup in the df itself.
            df.db.initialize(self.__tbl_name_list, con=self.__con, multi_select=self.__multi_select)
            yield df  # Sends o back to caller. Sits here until next call to next().That's what generators do
        # return self.generator()

    def __next__(self):
        df = next(self.__iterator)
        df.db.initialize(self.__tbl_name_list, con=self.__con, multi_select=self.__multi_select)
        return df

    # def generator(self):
    #     ittr = itertools.tee(self.__iterator, 2)
    #     self.__iterator = ittr[0]
    #     for df in ittr[1]:
    #         # Sets up DatabaseTableAccessor table names. The rest is already setup in the df itself.
    #         df.db.initialize(self.__tbl_name_list, con=self.__con, multi_select=self.__multi_select)
    #         yield df  # Sends o back to caller. Sits here until next call to next().That's what generators do




# def wrapper_read_sql_query02(f):                # Working version
#     """ Decorator for pd.read_sql_query and pd.read_sql_table to setup tbl_name, field_names attributes in DataFrame.
#     Uses a generator that wraps the df iterator returned by pd.read_sql_query() to set parameters in db accessor obj.
#     """
#     @functools.wraps(f)
#     def wrapper(*args, **kwargs):
#         """ @param args: arg[0]: sql statement. Used to pull table name.
#             @return: pd.DataFrame or iterator object (if chunksize argument != None)
#         """
#         if f.__name__ == 'read_sql_query':
#             tbl_names_list = tables_in_query(args[0])
#             db_tbl_name = tbl_names_list[0]
#         elif f.__name__ == 'read_sql_table':
#             db_tbl_name = args[0]
#             tbl_names_list = (db_tbl_name, ) if db_tbl_name else ()
#         else:
#             return f(*args, **kwargs)
#         coerce_float = kwargs.get('coerce_float', None)
#         kwargs['coerce_float'] = coerce_float if coerce_float is not None else False  # Avoid automatic cast to float.
#
#         df = f(*args, **kwargs)  # Calls sql_read func. df is DataFrame or iterator object when chunksize > 0.
#
#         tbl_name = getTblName(db_table_name=db_tbl_name) if db_tbl_name else ''
#         if tbl_name and strError not in tbl_name:        # Checks for valid tbl_names only.
#             fld_names = getFldName(tbl_name, '*', 1)            # {fldName: dbFldName, }
#             if isinstance(fld_names, dict):
#                 if isinstance(df, pd.DataFrame):
#                     # Passes arg to initialize db Accessor. Renames column names to fldNames.
#                     # df._temp_arg = tbl_names_list
#                     df.db.initialize(tbl_names_list)
#                     # Converts all pd.Timestamp columns to datetime.datetime data.
#                     # for col in df.columns:  # Converts all pd.TimeStamp objects to string.
#                     #     if 'datetime' in df.dtypes[col].__class__.__name__.lower():
#                     #         df[col] = pd.Series(df[col].dt.to_pydatetime(), dtype=object)
#                 else:               # df is a generator.
#                     """ This was it!: wrapping the df iterator inside a custom generator that sets up tbl_name,
#                     field_names. Defined here to use the closure and pass tbl_name, db_tbl_name, fld_names values for
#                     o.db Accessor. No need to use hash_values or other tricks. """
#                     def generator(iterator):
#                         o = next(iterator)          # This line creates the actual DataFrame object.
#                         # o._temp_arg = tbl_names_list
#                         # Sets up DatabaseTableAccessor attributes for object o. Renames df columns in initializer.
#                         o.db.initialize(tbl_names_list)
#                         # for col in o.columns:  # Converts all pd.TimeStamp objects to string.
#                         #     if 'datetime' in o.dtypes[col].__class__.__name__.lower():
#                         #         o[col] = pd.Series(o[col].dt.to_pydatetime(), dtype=object)
#                         yield o  # Sends o back to caller. Sits here until next call to next().That's what generators do
#
#                     return generator(df)
#         return df
#     return wrapper


# def wrapper_read_sql_query01(f):  # Tries to implement list of tbl_names, dict of fields_names. Too complicated. Drop!
#     """ Decorator for pd.read_sql_query and pd.read_sql_table to setup tbl_name, field_names attributes in DataFrame.
#     Uses a generator that wraps the df iterator returned by pd.read_sql_query() to set parameters in db accessor obj.
#     """
#     @functools.wraps(f)
#     def wrapper(*args, **kwargs):
#         """ @param args: arg[0]: sql statement. Used to pull table name.
#             @return: pd.DataFrame or iterator object (if chunksize argument != None)
#         """
#         if f.__name__ == 'read_sql_query':
#             # db_tbl_name = tables_in_query(args[0])[0] or None
#             db_tbl_name = tables_in_query(args[0]) or ()
#         elif f.__name__ == 'read_sql_table':
#             db_tbl_name = tuple(args[0]) or ()
#         else:
#             return f(*args, **kwargs)
#
#         tbl_names = [getTblName(db_table_name=tn) for tn in db_tbl_name] if db_tbl_name else ()
#
#         coerce_float = kwargs.get('coerce_float', None)
#         kwargs['coerce_float'] = coerce_float if coerce_float is not None else False  # Avoid automatic cast to float.
#
#         df = f(*args, **kwargs)  # Calls sql_read func. df is DataFrame or iterator object if chunksize != None.
#
#         fld_names_dict = {}
#         tbl_names = []
#         for tbl_name in tbl_names:
#             if tbl_name and strError not in tbl_name:        # Checks for valid tbl_names only.
#                 fld_names = getFldName(tbl_name, '*', 1)            # {fldName: dbFldName, }
#                 if isinstance(fld_names, dict):
#                     tbl_names.append(tbl_name)
#                     fld_names_dict.update({tbl_name: fld_names})        # {tbl_name: {fldName: dbFldName, }, }
#
#         if tbl_names and fld_names_dict:
#             if isinstance(df, pd.DataFrame):
#                 df = df.convert_dtypes()         # Forces dtype on all columns.
#                 # Converts column names to fldNames.
#                 df.rename(columns={v: k for k, v in [j.items() for j in list(fld_names_dict.values())] if v in
#                                    df.columns}, inplace=True)
#                 df.db._tbl_name = tbl_names   # This line invokes DatabaseTableAccessor.__init__() when executed.
#                 df.db._db_tbl_name = [getTblName(j) for j in tbl_names]
#                 df.db._field_names = fld_names_dict      # TODO(cmt): Dict contains full field list by design.
#
#                 # Converts all pd.Timestamp columns to datetime.datetime data.
#                 for col in df.columns:  # Converts all pd.TimeStamp objects to string.
#                     if 'datetime' in df.dtypes[col].__class__.__name__.lower():
#                         df[col] = pd.Series(df[col].dt.to_pydatetime(), dtype=object)
#
#             else:               # df is a generator.
#                 """ This was it!: wrapping the df iterator inside a custom generator that sets up tbl_name,
#                 field_names. Defined here to use the closure and pass tbl_name, db_tbl_name, fld_names values
#                 for o.db Accessor. No need to use hash_values or other tricks. """
#                 def generator(iterator):
#                     o = next(iterator)          # This line creates the actual DataFrame object.
#                     o = o.convert_dtypes()      # Forces dtype on all columns.
#                     # Sets up DatabaseTableAccessor attributes for object o.
#                     # Converts column names to fldNames.
#                     o.rename(columns={v: k for k, v in [j.items() for j in list(fld_names_dict.values())] if v in
#                                        o.columns}, inplace=True)
#                     o.db._tbl_name = tbl_names  # This line invokes DatabaseTableAccessor.__init__() when executed.
#                     o.db._db_tbl_name = [getTblName(j) for j in tbl_names]
#                     o.db._field_names = fld_names_dict  # TODO(cmt): Dict contains full field list by design.
#
#                     for col in o.columns:  # Converts all pd.TimeStamp objects to string.
#                         if 'datetime' in o.dtypes[col].__class__.__name__.lower():
#                             o[col] = pd.Series(o[col].dt.to_pydatetime(), dtype=object)
#
#                     yield o
#                 return generator(df)
#         return df
#     return wrapper


# def wrapper_read_sql_query00(f):  # TODO: May have found a way to use this decorator, using enqueue.
#     """ Decorator for pd.read_sql_query to setup tbl_name, field_names attributes in DataFrame when tbl_name is
#     passed.
#     """
#     @functools.wraps(f)
#     def wrapper(*args, **kwargs):
#         """ @param args: arg[0]: sql statement. Used to pull table name.
#             @return: pd.DataFrame or iterator object (if chunksize argument != None)
#         """
#         db_tbl_name = tables_in_query(args[0])[0] or None
#         tbl_name = getTblName(db_table_name=db_tbl_name) if db_tbl_name else None
#
#         df = f(*args, **kwargs)    # Calls sql_read func. df is DataFrame or iterator object (if chunksize != None)
#
#         if tbl_name and strError not in getTblName(tbl_name):
#             fld_names = getFldName(tbl_name, '*', 1)        # Skips generated and hidden fields.
#             if isinstance(fld_names, dict):
#                 if isinstance(df, pd.DataFrame):
#                     df.db.tbl_name = tbl_name
#                     df.db.field_names = fld_names
#                     return df
#                 else:               # df is a generator.
#                     # All this yara-yara to avoid loading all items in the iterator into memory (like in a list)
#                     df1, df2 = itertools.tee(df, 2)    # Makes copies of iterator. df IS exhausted in this operation!!.
#                     for frame in df1:       # Loads frames in memory 1 at a time.
#                         # Same hash_val for frames with identical data sets. That's why _accessor_params must be list.
#                         # (It's a remote, remote possibility but...)
#                         hash_val = hashlib.sha256(frame.to_json().encode()).hexdigest()
#
#                         # No good: df2.__hash__() cannot be passed from iterator to object when obj is created.
#                         # hash_val = df2.__hash__()
#                         pd.DataFrame.db.enqueue(hash_val, tbl_name, fld_names)
#                     return df2
#         return df
#     return wrapper

# ----------------------- END Accessors and decorators to mirror-image DataFrames to database tables ----------------



""" Initial version of Accessor, using db names as column names. That changes in version 2 of the Accessor."""
# @pd.api.extensions.register_dataframe_accessor("db")  # Accessor to pd.DataFrame mirroring db records. Low level stuff
# # Executed as df.db.method() or DataFrame.db.method(). Example: DataFrame.db.create(tbl_name='tblAnimales').
# class DatabaseTableAccessor01:
#     """
#     Implements an association of database tables with pd.Dataframes.
#     Dataframes read from database may eventually include columns that are not part of the database table associated to
#     it.
#     The results from the accessor will only include valid db table field names (for now, still in review).
#     """
#
#     #  Used to pass parameter to DataBaseAccessor instances. TODO(cmt): No longer needed with generator() below.
#     _accessor_params = []       # [(hash_val, table_name, field_names), }  hash_val is pandas_obj checksum value.
#
#     def __init__(self, pandas_obj):     # pandas_obj: inserted by caller object when calling as df_obj.df.method()
#         self._validate(pandas_obj)
#         self._obj = pandas_obj     # Stored here just in case, for eventual uses. Only arg passed from DataFrame obj.
#         # hash value MUST be encoded with object data.
#         # self.__hash_val = hashlib.sha256(self._obj.to_json().encode()).hexdigest()
#         self.tbl_name = None             # initialized by initialize(), if used.
#         self.field_names = None          # ALL field names in the table, by design.
#         self._reverse_names = None      # {dbName: fldName (key name), }
#         # self.initialize()
#
#     @staticmethod
#     def _validate(obj):
#         # Validation code: Columns for objects that have field_names attribute set must be of type Index.
#         pass
#
#     def initialize(self):   # TODO(cmt): No longer needed, with the use of generator() in read_sql @decorator func.
#         """ Initialize using list _accessor_params. """
#         item = next((j for j in self._accessor_params if j[0] == self.__hash_val), None)
#         self.tbl_name, self.field_names = (item[1], item[2]) if item is not None else (None, None)
#         if item:
#             self._accessor_params.remove(item)  # Removes processed element from list.
#
#     # def initialize00(self):                               # Must pop to keep dict size in line
#     #     """ Initialize using dictionary _accessor_params. """
#     #     self.tbl_name, self.field_names = self._accessor_params.pop(self.__hash_val, (None, None))
#
#     def col(self, *args):
#         """
#         Returns valid column names (DB names) for tbl_name for all names passed in args, to access DataFrame columns.
#         @param args: field names (key or db field names).
#         @return: db field names for tbl_name (tuple) or empty tuple if no field names found for table tbl_name.
#         """
#         if any(j is None for j in (self.tbl_name, self.field_names)):
#             return ()
#
#         valid_names = []
#         for fname in args:
#             if isinstance(fname, str):
#                 fname = (" ".join(fname.split())).lower()  # Removes all excess leading, trailing and inner whitespace.
#                 if fname.startswith('fld'):
#                     # Pulls field db name from field key name.
#                     fname = next((v for k, v in self.field_names.items() if fname == k.lower()), '').lower()
#                 name = next((j for j in self._obj.columns if j.lower() == fname), None)
#                 if name:
#                     valid_names.append(name)
#             elif isinstance(fname, int):
#                 valid_names.append(self._obj.columns[fname])
#             else:
#                 continue    # ignores invalid types.
#                 # raise TypeError(f'Pandas DatabaseTableAccessor error: invalid field name type {type(fname)}.')
#         return valid_names[0] if len(valid_names) == 1 else tuple(valid_names)  # Returns only valid names for tbl_name.
#
#     cols = col
#
#     def reverse_names(self):
#         """
#         Returns dictionary of the form {fld_db_name: fldKeyName | None, }, ONLY for col names present in the DataFrame.
#         If fld_db_name is not a valid field name for the database table, fldKeyName is None.
#         @return: dict {fld_db_name: fldKeyName | None, }
#         """
#         return {k: self._reverse_names.get(k, None) for k in list(self._obj.columns)}
#
#
#     @staticmethod
#     def create(tbl_name: str, *, data=None, index=None, columns=None, dtype=None, copy=None, **kwargs) -> pd.DataFrame:
#         """ Creates an empty DataFrame setting columns with ALL columns from datables tbl_name. The dataframe is
#         composed of:
#             - Row index: pandas-assigned index.
#             - Column names: Proper database field names belonging to table tbl_name. If none passed via columns argument
#                             assigns all the fields in the table as column names.
#             - Data: data argument. if None: creates empty dataframe. Equivalent to creating an empty DataTable.
#             - Attributes of the DatabaseTableAccessor object:
#                     tbl_name: tbl_name (table key name)
#                     field_names: dictionary with field name data -> {fldName: dbFldName, }
#                     _reverse_names: dictionary with reverse field name data {dbFldName: fldName , }
#
#         @param tbl_name: Database table key name (starts with "tbl") or actual table name (Animales, Caravanas, etc.).
#          @param data: data to populate the dataframe, in pandas-accepted format (list, dict).
#         @param columns: (list, tuple, set) of str with database field names or key field names (starting with "fld").
#         If None -> creates dataframe with ALL fields existing in tbl_name.
#         columns items (str) must match exactly the key field names or db field names in table tbl_name.
#         @param kwargs: {fldName: val, } Dict with values to set record 0 in the dataframe.
#         All other params passed for compatibility with pd.DataFrame usage.
#         Meant to be called by pd.DataFrame class, not by instances (although it should work anyway).
#         """
#         if isinstance(tbl_name, str):
#             tbl_name = " ".join(tbl_name.split())    # Removes all leading, trailing, middle whitespace characters.
#             if not tbl_name.lower().startswith('tbl'):
#                 tbl_name = getTblName(db_table_name=tbl_name)
#                 if strError in tbl_name:
#                     raise ValueError(f"Pandas DatabaseAccessor Error: Invalid database table name {tbl_name}.")
#             fld_names = getFldName(tbl_name, '*', mode=1)  # {fldName: DBFldName, }  All fields when columns=None.
#             if not isinstance(fld_names, dict):
#                 raise ValueError(f"Pandas DatabaseAccessor Error: Invalid database table name {tbl_name}.")
#
#             if data:
#                 df = pd.DataFrame(data=data, index=index, columns=columns, dtype=dtype, copy=copy)
#             else:
#                 if columns and isinstance(columns, (tuple, list, set)):
#                     # Uses db field names as columns to pass to DataFrame constructor.
#                     flds = {k: v for k, v in fld_names.items() if (k in columns or v in columns)}
#                     if flds:
#                         # fld_names passed via columns arg. Creates dataframe ONLY with field names valid for tbl_name.
#                         fld_names = flds
#                 df = pd.DataFrame(data=data, index=index, columns=list(fld_names.values()), dtype=dtype, copy=copy)
#
#             df.db.tbl_name = tbl_name              # This line, when reached, accesses db.__init__()
#             df.db.field_names = fld_names
#             # df.db._reverse_names = {v: k for k, v in fld_names.items()}
#
#             if df.empty and kwargs:      # No data (empty dataframe created) and fields to be set passed in kwargs.
#                 # Filters out any invalid col names for tbl_name and passes only the valid ones for setting the df.
#                 valid_dict = {k: v for k, v in kwargs.items() if k in df.db.field_names}
#                 df.loc[0, list(valid_dict.keys())] = list(valid_dict.values())      # sets row 0 with values passed.
#             # Replaces all NaN values with None for compatibility with existing code.
#             df.replace({np.nan: None}, inplace=True)
#
#             return df
#         raise TypeError(f"Pandas DatabaseAccessor Error: Invalid database table name {tbl_name}.")
#
#     @classmethod
#     def enqueue(cls, hash_val, tbl_name, field_names):       # TODO(cmt): no longer needed with generator() below.
#         # Uses a hash_value based on dataframe data, the only way found (so far) to identify elements of an iterator.
#         # Appends to a list, as there is a (remote) possibility that 2 or more identical frames are stacked in the list.
#         cls._accessor_params.append((hash_val, tbl_name, field_names))  #
#         # cls._accessor_params[hash_val] = (tbl_name, field_names)        # dict line

# ---------------------------------- END class DatabaseTableAccessor ---------------------------------------------- #


class SQLiteQuery(object):      # Database Query objects created for different threads and different databases.
    """
    *** This class is to be used to create QUERY-ONLY objects. DO NOT WRITE to DB with these objects. Use
    SqliteQueueDatabase for writing to avoid database blocking. ***
    By the way, this class opens all connections in 'ro' (READ-ONLY) mode.
    DB Access Model:
       - SQLiteQuery objects are a pool of DB connections to perform db queries concurrently from different threads:
            The constructor allows 1 SQLiteQuery object for each database used by the system, for each thread running.
       - SSqliteQueueDatabase objects are unique objects (singleton class) used to WRITE to DB, using a queue logic.
    Returns an SQLiteQuery object to access DB. If a _conn is already available returns it or use, otherwise
    creates a new _conn. Returns the first _conn available (in_transaction=False) found in list of connections
    for the calling thread
    @param: force_new -> True: forces creation of a new DB _conn and returns the new _conn. Connections created with
    'force_new' are not managed by this class. Must be closed by the creator.
    """
    __slots__ = ('__conn', '__threadID', '__dict__', '__db_name')
    __MAX_CONNS_PER_THREAD = 100
    __queryObjectsPool = collections.defaultdict(list)  # {threadID1: [conn1, conn2,], }
    __newInstance = {}  # Dict para avisar a __init__ que es new instance. {threadID: True/False,  }

    def __new__(cls, **kwargs):  # Override de new para crear objeto nuevo solo si el thread no tiene un definido
        callingThread = threading.current_thread().ident
        # TODO(cmt): Si es force_new -> NO SE REGISTRA el objeto en el pool, es exclusivo del que lo crea
        if kwargs.get('force_new') is True:
            instance = super().__new__(cls)
            if len(cls.__queryObjectsPool.get(callingThread, [])) > SQLiteQuery.__MAX_CONNS_PER_THREAD:
                db_logger.warning(f'{moduleName()}({lineNum()}) - {callerFunction()}: 'f'ERR_SYS_DBAccess: '
                                   f'Connections for {callingThread} exceeded {SQLiteQuery.__MAX_CONNS_PER_THREAD}.')
            cls.__newInstance[callingThread] = True  # Esta lista permite re-entry al codigo de __new__() e __init__()
            return instance

        # Here, an available _conn is procured.
        objList = cls.__queryObjectsPool.get(callingThread, [])
        name = kwargs.get('db_name') or MAIN_DB_NAME
        obj = next((j for j in objList if name == j.dbName and not j.conn.in_transaction), None)
        if obj:
            return obj

        # En este punto, no se encontraron queryObj libres. Crea uno mas para este thread. __new__()lo agrega a Pool
        instance = super().__new__(cls)   # Crea objeto si el numero este dentro del limite de objs per thread
        cls.__newInstance[callingThread] = True         # Signal __init__() that the instance must be initialized.
        return instance

    def __init__(self, *, db_name=None, check_same_thread=True, timeout=4.0, detect_types=0, **kwargs):
        """
        Opens a SQLiteQuery _conn and returns a valid cursor. This object becomes the DB handler (for queries ONLY).
        # The line of code assigning sqlite3.Row to the row_factory of _conn creates what some people call a
        # 'dictionary cursor', - instead of tuples it starts returning 'dictionary' rows after fetchall or fetchone.
        @param kwargs:
        """
        super().__init__()
        db_name = db_name or MAIN_DB_NAME
        threadID = threading.current_thread().ident
        # __newInstance dict signals whether instance needs to be initialized or not.
        if self.__newInstance.get(threadID, None):
            self.__threadID = threadID
            self.__newInstance.pop(threadID)   # Elimina callingThread de dict __newInstance.
            try:
                # detect_types para tratar de convertir datetime automaticamente. Por default a TODAS las conexiones.
                self.__conn = self.connCreate(db_name=db_name, check_same_thread=check_same_thread, timeout=timeout,
                                              detect_types=detect_types|sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
                                              **kwargs)
                self.__db_name = db_name
                self.__queryObjectsPool[self.__threadID].append(self)   # appends SQLiteQuery instance to objects pool.
                db_logger.info(f'=====> SQLiteQuery Constructor: Database: {self.__db_name} - Connection thread is '
                                 f'{threading.current_thread().ident} '
                                 f'- ThreadRegister: {self.__queryObjectsPool} - {callerFunction()}')
            except(sqlite3.Error, sqlite3.DatabaseError, sqlite3.OperationalError) as e:
                val = f'ERR_DB_Cannot create connection: {e} - {callerFunction(getCallers=True)}.'
                self.__conn = self.__db_name = None
                db_logger.error(val)
                raise DBAccessError(f'{val}')


    @staticmethod
    def connCreate(db_name='', *, check_same_thread=True, detect_types=0, isolation_level=None, timeout=0.0,
                    cach_stmt=0, uri=None, **kwargs):  # kwargs a todas por ahora, para flexibilidad
        """Creates a DB connection in READ-0NLY mode. """
        # dbName = db_name.strip() if db_name else MAIN_DB_NAME. mode=rw to allow for PRAGMA OPTIMIZE to work.
        return sqlite3.connect(f'file:{db_name}?mode=rw', check_same_thread=check_same_thread,
                               detect_types=detect_types, isolation_level=isolation_level, timeout=timeout,
                               cached_statements=cach_stmt, uri=True)

    @classmethod
    def getObjectsPool(cls):
        return cls.__queryObjectsPool

    @classmethod
    def fetchObject(cls, *, db_name=''):
        """ Returns a SQLite query object associated to the thread and database that requests a DB query.
        """
        # Toma el thread id desde donde se inicio la llamada a setRecord() y busca lista de conexiones de ese thread y
        # para la db especificada por db_name
        name = db_name or MAIN_DB_NAME
        objList = SQLiteQuery.getObjectsPool().get(threading.current_thread().ident, [])
        obj = next((j for j in objList if name == j.dbName and not j.conn.in_transaction), None)
        return obj if obj else SQLiteQuery(db_name=name)  # Si no hay queryObj libres crea uno para este thread y db.

    @classmethod
    def checkThreadSafety(cls):
        """ Returns SQLite build Threading Safety level: 0, 1, 2 (int). None if the call fails """
        con = sqlite3.connect(":memory:")
        val = con.execute("select * from pragma_compile_options where compile_options like 'THREADSAFE=%' ").fetchall()
        if val and len(val) > 0:
            safeStr = val[0][0].lower().strip()
            _ = safeStr.replace('threadsafe=', '')
            try:
                return int(_)
            except TypeError:
                return None
        return None

    @property
    def dbName(self):
        return self.__db_name

    def getCursor(self):
        if self.__conn:
            return self.__conn.cursor()

    @property
    def conn(self):
        return self.__conn

    @property
    def callingThread(self):
        return threading.current_thread().ident

    @property
    def threadID(self):
        return self.__threadID

    def __del__(self):  # Cerrar conexion db, remover objeto del pool de conexiones.
        if self.__conn and self.__threadID == threading.current_thread().ident:
            try:
                self.execute('PRAGMA OPTIMIZE; ')
                self.connClose()
                self.__conn = None
                # Busca conexiones del calling thread
                if next((j for j in self.getObjectsPool().get(threading.current_thread().ident) if j == self), None):
                    self.__queryObjectsPool[self.__threadID].remove(self)
                    val = f'========> {self.__class__.__name__}.__del__(): Deleted obj={id(self)} / Thread: {self.__threadID}'
                    db_logger.info(val)
                    print(val, dismiss_print=DISMISS_PRINT)
            except (StopIteration, ValueError, sqlite3.Error) as e:
                retValue = f'ERR_DBClose. Cannot close connection or object not found. Error: {e}'
                db_logger.error(retValue)
                raise DBAccessError(f'{retValue} - {callerFunction(getCallers=True)}')


    # @timerWrapper(iterations=50)   # range: 300 - 2300 usec (50 iterations) - New version 23-Dec-23: 300-600 usec!
    def execute(self, strSQL='', params='', *, tbl=None, fldID_idx=None):
        """ Executes strSQL. Returns a cursor object or errorCode (str). This function for QUERY/DB Reads ONLY!!
            DO NOT attempt to write to DB with this function. Use SQLiteQueueDatabase class for writes.
        """
        if strSQL:
            with self.__conn:
                db_logger.debug(f'SQLiteQuery received query: {strSQL}')
                try:
                    cur = self.__conn.execute(strSQL, params)
                except (json.JSONDecodeError, ValueError) as e:
                    cur = f"ERR_DB_Data type converter error (JSON, TIMESTAMP or GEO). Error: {e}; sql: {strSQL}"
                except (sqlite3.Error, DBAccessError, Exception) as e:
                    cur = f'ERR_SQLiteQuery {callerFunction()} - error: {e}'
                    self.__conn.rollback()
                    db_logger.error(f'{cur}; strSQL: {strSQL}')
                    self.__conn.execute('PRAGMA OPTIMIZE; ')
                    raise DBAccessError(f'DatabaseError Exception.{cur}; strSQL: {strSQL}')
            return cur
        return None


    def connClose(self, *, optimize=False):
        # Verify what type of cleanup and closures must be done (incl. backup dumps) before closing.
        try:
            if optimize:
                self.execute('PRAGMA OPTIMIZE; ')
            self.__conn.close()
        except sqlite3.Error as e:
            retValue = f'ERR_DBClose: {e} - Cannot close connection.'
            raise DBAccessError(f'{retValue} - {callerFunction(getCallers=True)}')


    def initialize(self):
        cur = self.execute('PRAGMA JOURNAL_MODE = WAL; ')  # Setear JOURNAL_MODE=WAL al iniciar el sistema (performance)
        _ = self.execute('PRAGMA OPTIMIZE; ')
        if isinstance(cur, str) or not str(cur.fetchone()[0]).lower().__contains__('wal'):  # JOURNAL_MODE queda en disco
            db_logger.info('PRAGMA setting failed. WAL journal_mode not enabled')
            cur = f'ERR_DBWrite: PRAGMA setting failed. WAL journal_mode not enabled'
        return cur

# ============================================ End SQLiteQuery class ============================================= #

# Main Thread (FrontEnd) database access object:       # TODO: Pasar luego esta conexion al main().
def createDBConn(*, db_name=''):
    try:
        obj = SQLiteQuery(db_name=db_name, check_same_thread=True)  # DB query object.
    except (sqlite3.DatabaseError, sqlite3.OperationalError, DBAccessError, Exception):
        exit(-1)
    else:
        return obj

queryObj = createDBConn()
queryObj_nr = createDBConn(db_name=NR_DB_NAME)
# ---------------------------------------------------------------------------------------------------------------- #

class AccessSerializer(object):
    """
       Implements a soft lock using and BoundedSemaphore obj and its functions and avoiding the use of the general
       Python mutex to block large swates of code.
       Designed to serialize access to shared data resources (in memory and db) when they are accessed concurrenty by
       foreground and bkgd threads. Operates by allowing a chosen call to the protected code to proceed and putting
       any subsequent calls from concurrent threads to wait while the protected code is run. Once on wait(), the mutex
       lock is released for the rest of the system to continue running.
       If another thread sharing the AccessSerializer object attempts to run, it will find the internal semaphore
       exhausted and will be put to wait until the semaphore is released.
       Usage:
       1)  my_lock = AccessSerializer(resource)     or, alternatively    my_lock = AccessSerializer(resource)
           ...                                                               ...
           ...                                                          with my_lock:
                                                                            access count = some serializing condition
           my_lock.acquire()                                                with my_lock.semaphore:
           ...Protected access to resource ...                                  ...Protected access to resource ...
           my_loc.release()
        Important: acquire()/release() work in pairs (as a normal lock). Failure to conform may result in freezes.

       2) Can use the value returned by acquire() to implement a selective read/write of shared resources, as shown in
       _init_uid_dicts() method.

       3) This class implements a sacha-singleton system based on the resource argument passed to the Constructor:
            - If resource is None, it behaves exactly the same as the previous AccessSerializer: creates 1 new object
              everytime.
            - If resource is != None, it creates only 1 object per resource value, stores it in a dictionary and any
              subsequent calls with the same resource value will yield the same object.
            - If resource is str and starts with 'tbl', assumes it's a database table and creates a unique name as
              "db_name.resource", so that multiple databases with equal table names can be handled in a dictionary.
       """
    __slots__ = ('_lock_obj',  '_acquired_count', '_timeout', '_total_count', '__acq_count_stack', '__reset', '__n')
    __lock = threading.Lock()

    def __init__(self, *, timeout: float = None):
        """
        @param timeout: (int, float): seconds to timeout a wait().
        blocking is NOT an instance attribute, but a thread attribute. Should NOT be initialized in __init__().
        """
        self.__n = 1
        self._lock_obj = threading.BoundedSemaphore(self.__n)       # BoundedSemaphore defined with n=1
        self._timeout = timeout if isinstance(timeout, (int, float)) and (0 <= timeout <= 1000) else 5
        self._acquired_count = 0        # Keeps track of concurrent acquisitions of the lock by different threads.
        self._total_count = 0           # The total number of times the lock has been concurrently acquired.
        self.__acq_count_stack = collections.defaultdict(list)  # Stack to pass _acquired_count to the outside world.
        self.__reset = False

    def __call__(self):     # NOT USED FOR NOW...
        """ Executed when an instance object is invoked with arguments (or just with parenthesis).
            Passes blocking setting to __enter__() function.
            Used to implement the passing of blocking setting when using context manager.
            Usage: lock_obj(blocking=True/False).
        """
        # self.__blocking_status[threading.current_thread().ident].append(bool(allow_reentry))
        return self


    def enter(self) -> int:
        """ This func. executed only from Context Mgr __enter__() method. """
        thread_id = threading.current_thread().ident
        with self.__lock:   # Does NOT acquire the Semaphore.
            self._acquired_count += 1  # Must keep track of # of threads accesing the lock concurrently.
            # Thread code operates in a nested fashion, so the stack below will work to pass _acquired_count out.
            self.__acq_count_stack[thread_id].append(self._acquired_count)  # Adds val to stack to make it accessible
            self._total_count = self._acquired_count
            return self._acquired_count


    def exit(self) -> None:
        """ This func. executed only from Context Mgr __exit__() method. """
        thread_id = threading.current_thread().ident
        with self.__lock:           # Acquire lock in Condition object.
            self._acquired_count -= 1
            if self._acquired_count < 0:
                self._acquired_count += 1       # Return to previous state before throwing exception.
                raise RuntimeError(f'ERR_RuntimeError AccessSerializer: attempt to release an unacquired lock.')
            self.__acq_count_stack[thread_id].pop()     # If this line throws exception -> Logic is wrong!
            if self._acquired_count == 0:  # Exhausted all concurrent calls to the protected code block
                self._total_count = 0  # Resets control variable for next usage of the lock.


    def acquire(self, *, blocking=True, timeout=None) -> bool:
        """Passes the acquire request to internal semaphore object. """
        return self._lock_obj.acquire(blocking=blocking, timeout=timeout)


    def release(self):
        """Passes the release request to internal semaphore object. """
        with self.__lock:
            if self._lock_obj._value == self.__n-1 and self.__reset:
                self._acquired_count = 0
                self._total_count = 0
                self.__acq_count_stack.clear()
                self.__reset = False
        self._lock_obj.release()

    end = release


    @property
    def semaphore(self) -> threading.BoundedSemaphore:
        """ Returns internal semaphore for use in blocking access to critical code sections. """
        return self._lock_obj

    @property
    def access_count(self) -> int:         # This stack is thread-safe. Can return values without using a lock.
        try:
            return self.__acq_count_stack[threading.current_thread().ident][-1]  # Returns last item in stack for thread
        except (KeyError, IndexError, AttributeError):
            return 0          # Stack empty or thread_id not in thread_stack dict -> Access count is 0.

    @property
    def total_count(self):
        with self.__lock:
            return self._total_count


    def __del__(self):
        self.reset()


    def reset(self) -> bool:
        """ Return AccessSerializer object to known state. In case it's needed."""
        with self.__lock:
            if self._lock_obj._value == self.__n:
                self._acquired_count = 0
                self._total_count = 0
                self.__acq_count_stack.clear()
                self.__reset = False
                return True
            else:
                self.__reset = True     # Instructs to perform reset when the semaphore is released.
        return False


    # The 2 methods below implement the context mgr. for AccessSerializer class.
    def __enter__(self) -> int:
        """ Returns _acquired_count value (int). Not returning an object to avoid its use from a context manager. """
        return self.enter()


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit()
        if exc_type is not None:
            krnl_logger.error(f'AccessSerializer Exception raised: {exc_type}, {exc_val}. Traceback: {exc_tb}.')
            return False     # False signals that the exception needs to be handled by the outside code.
    """                                  A few words on context managers
    # When the with statement executes, it calls .__enter__() on the context manager object to signal that you’re
    # entering into a new runtime context. If you provide a target variable with the as specifier, then the return
    # value of .__enter__() is assigned to that variable.
    #
    # When the flow of execution leaves the context, .__exit__() is called. If no exception occurs in the with code
    # block, then the three last arguments to .__exit__() are set to None. Otherwise, they hold the type, value, and
    # traceback associated with the exception at hand.
    #
    # If the .__exit__() method returns True, then any exception that occurs in the with block is swallowed and
    # the execution continues at the next statement after with. If .__exit__() returns False, then exceptions are
    # propagated out of the context. This is also the default behavior when the method doesn’t return anything
    # explicitly. You can take advantage of this feature to encapsulate exception handling inside the context manager.
    # See: https://realpython.com/python-with-statement/  
    """

# ------------------------------------------ End class AccessSerializer ----------------------------------------#

class DBAccessSemaphore(threading.BoundedSemaphore):
    """ Implements a locking object to serialize access to db based on table names: will block access on a
    table-by-table basis, enabling operations with tables not listed in the internal blocking list to proceed.
    1 instance (and 1 instance only) for each active database is created within the SQLiteQueueDatabase class.
    Subclasses BoundedSemaphore for ease of use.
    """
    __slots__ = ('_resource_name', '_timeout', '__reset', '__n')
    __resources_dict = {}    # {resource_name: DBAccessSemaphore obj, } Dict of db tables with access locking ongoing.
    __join_str = "##~#"
    __lock = threading.Lock()

    def __new__(cls, tbl_name=None, *args, db_name=None, **kwargs):
        db_name = db_name if db_name in _DataBase.active_databases() else MAIN_DB_NAME
        if not isinstance(tbl_name, str) or not tbl_name.startswith('tbl'):
            raise ValueError(f'ERR_ValueError. DBAccessSerializer: {tbl_name} is not a valid table name for {db_name}.')
        resource_name = db_name + cls.__join_str + tbl_name
        if resource_name in cls.__resources_dict:
            # Implements a sacha-singleton on a per-table basis to be shared by all threads.
            return cls.__resources_dict[resource_name]
        return super().__new__(cls)

    def __init__(self, tbl_name: str = None, *, semaphore_n: int = 1, db_name: str = None):
        """
        Implements a 'named' semaphore with n=1. This object serializes access to specific tables in a database,
        allowing only 1 thread to access the table at a time. If another thread attempts to access the table while the
        semaphore is acquired, it's put to wait until semaphore is freed or until it times out.
        @param tbl_name: (str). Database table key name (starts with 'tbl').
        @param db_name: Database for which the Serializer Lock is being created. Only 1 instance per active database.
        @param semaphore_n: int. Semaphore n value. Must be 1 for the semaphore to work properly.
        """
        self._db_name = db_name if db_name in _DataBase.active_databases() else MAIN_DB_NAME
        resource_name = self._db_name + self.__join_str + tbl_name
        if resource_name not in self.__resources_dict:  # Initializes object only once.
            n = semaphore_n if isinstance(semaphore_n, int) and semaphore_n >= 0 else 1
            super().__init__(n)     # IMPORTANT: semaphore_n must be int=1 for serialization of access to work properly.
            self.__n = n
            self._resource_name = resource_name
            self.__resources_dict[self._resource_name] = self
            self._timeout = 1           # Short timeout for database operations.
            self.__reset = False

    # This class will use most methods (including __enter__(), __exit__(), acquire()) from parent class BoundedSemaphore
    # release()/reset() are under testing. The idea is to do without them.

    # def acquire(self, blocking: bool = ..., timeout: float = ...) -> bool:
    #     return super().acquire(blocking=blocking, timeout=timeout)

    def release(self, n: int = 1) -> None:
        with self.__lock:
            if self._value == self.__n - n:
                self.__resources_dict.pop(self._resource_name, None)
                self.__reset = False
        super().release(n=n)


    def reset(self):
        """ Removes self._resource_name entry from __resources_dict."""
        with self.__lock:
            if self._value == self.__n:     # Accesses private attribute in threading.Semapore.
                self.__resources_dict.pop(self._resource_name, None)
            else:
                self.__reset = True

    pop_key = reset


    @classmethod
    def _get_resources_dict(cls):               # TODO: debugging purposes. Remove after testing.
        return cls.__resources_dict

    @property
    def timeout(self):
        return self._timeout
# -------------------------------------------- End class DBAccessSemaphore -----------------------------------------#




# class DBAccessSerializerOriginal(AccessSerializer):     # Uses AccessSerializer. DEPRECATED.
#     """ Implements a locking object to serialize access to db based on table names: will block access on a
#     table-by-table basis, enabling operations with tables not listed in the internal blocking list to proceed.
#     1 instance (and 1 instance only) for each active database is created within the SQLiteQueueDatabase class.
#     Does NOT support Context Manager (until the issue of passing tbl_name to __exit__() is resolved)
#     """
#     # _Serializer = collections.namedtuple("AccessSerializer", ('lock', 'resource_name'))       # Not used.
#     __resources_dict = {}       # {resource_name: AccessSerializer obj, }   Dict of db tables with access locking ongoing.
#     __join_str = "##/#"
#
#     def __new__(cls, tbl_name=None, *args, db_name=None, **kwargs):
#         db_name = db_name if db_name in _DataBase.active_databases() else MAIN_DB_NAME
#         if not isinstance(tbl_name, str) or not tbl_name.startswith('tbl'):
#             raise ValueError(f'ERR_ValueError. DBAccessSemaphore: {tbl_name} is not a valid table name for {db_name}.')
#         resource_name = db_name + cls.__join_str + tbl_name
#         if resource_name in cls.__resources_dict:
#             return cls.__resources_dict[resource_name]  # Implements a singleton on a per-database basis.
#         return super().__new__(cls)
#
#     def __init__(self, tbl_name: str = None, *, timeout=None, db_name: str = None):
#         """
#         @param tbl_name: (str). Database table key name (starts with 'tbl').
#         @param db_name: Database for which the Serializer Lock is being created. Only 1 instance per active database.
#         """
#         self._db_name = db_name if db_name in _DataBase.active_databases() else MAIN_DB_NAME
#         resource_name = self._db_name + self.__join_str + tbl_name
#         if resource_name not in self.__resources_dict:
#             # Initializes object only once.
#             self._resource_name = resource_name
#             self.__resources_dict[self._resource_name] = self
#             super().__init__(timeout=timeout)
#
#     # Uses all methods and attributes from parent class (AccessSerializer), except for the overrides below.
#
#     def reset(self, *, pop_key=True):    # pop_key=True -> Remove self._resource_name entry from __resources_dict.
#         if pop_key:
#             with self._lock_obj:  # Accesses underlying condition directly: bypasses AccessSerializer.acquire/release
#                 self.__resources_dict.pop(self._resource_name, None)
#                 super().reset()     # Nested acquisition of Condition. Should work, as Condition uses RLock.
#         else:
#             super().reset()   # Resets AccessSerializer object, leaves entry in __resources_dict.
#
#     @classmethod
#     def _get_resources_dict(cls):               # TODO: debugging purposes. Remove after testing.
#         # with cls._lock_obj:     # Acquires lock defined in AccessSerializer class.
#         return cls.__resources_dict
#
#     @classmethod
#     def _clear_resources_dict(cls):
#         for v in cls.__resources_dict.copy().values():
#             v.reset()

# -------------------------------------------- End class DBAccessSemaphore -----------------------------------------#


class _DataBase(object):
    """ Implements the internal working of the Ganado databases (use of _sys_tables, _sys_fields and data management
    using tbl and fld parameterized table/field names.
    Enables the use of DataTable class with multiple databases simultaneously open in the system.
    _DataBase objects are associated to specific databases (1 object per database) and and thread-independent.
    Each database may have multiple connections open: 1 connection (and 1 only) for writing, it being an object of class
    SqliteQueueDatabase, and multiple read connections (1 per thread that requests to read), managed by the SQLiteQuery
    class. These connections are not associated to the _DataBase objects (although they should. Perhaps in the future).
    Implementation for internal use only.
    """
    __db_cache = {}         # cache to allow 1 instance only per db name {__db_name: db_obj, }

    @classmethod
    def __new__(cls, *args, **kwargs):
        """ Overrides __new__() to instantiate each database object only once (One _DataBase object per database name,
        irrespective of number of  threads. the _DataBase objects are thread-independent).
        """
        obj = cls.__db_cache.get(kwargs.get('name'), None)
        if obj:
            return obj
        # En este punto, no se encontro obj _DataBase con el nombre especificado. Crea uno mas para este thread.
        instance = super().__new__(cls)  # Crea objeto si el numero este dentro del limite de objs per thread
        return instance


    def __init__(self, *, name=''):
        name = name or MAIN_DB_NAME
        if not self.__db_cache.get(name):
            super().__init__()
            self.__db_name = name
            self.__db_cache[self.__db_name] = self  # Registers Database object in cache dict with db_name as key.
            self.__sysTables = {}
            self.__sysTablesCopy = {}
            self.__sysFields = {}
            self.__sysFieldsCopy = {}
            self.__serializer_flds = AccessSerializer()  # One set of locks per database to access tables independently.
            self.__serializer_tbls = AccessSerializer()

            # Initial loading of _sys_Tables, _sysFields tables
            self.__reloadTables = True  # Flag to signal reload is required.
            self.__reloadFields = True  # Flag to signal reload is required.
            self.reloadTables()
            self.reloadFields()

    @classmethod
    def getObject(cls, *, db_name=None):
        """ Returns _DataBase object or None if database associated to db_name doesn't exist
        @return: _DataBase object | None
        """
        return cls.__db_cache.get(db_name or MAIN_DB_NAME)

    @classmethod
    def active_databases(cls):
        return tuple(cls.__db_cache.keys())

    @property
    def dbName(self):
        return self.__db_name

    @property
    def reload_tables(self):
        return self.__reloadTables

    @property
    def reload_fields(self):
        return self.__reloadFields

    @property
    def sys_tables(self):
        return self.__sysTables

    @property
    def sys_fields(self):
        return self.__sysFields

    def __del__(self):
        self.__db_cache.pop(self.__db_name)


    def reloadFields(self):
        """ True: Success / str: errorCode to return back to callers """
        # print(f'Hola, reloadFields()...')
        qryObj = SQLiteQuery.fetchObject(db_name=self.dbName)  # Conexion para DB y para thread desde donde se llama
        strSQL = f"SELECT ID_Table, Field_Key_Name, Field_Name, ID_Field, Table_Key_Name, Excluded_Field, " \
                 f"Compare_Index, Hidden_Generated FROM '{SYS_FIELDS_NAME}' "
        with self.__serializer_flds:
            access_count = self.__serializer_flds.access_count    # Getting access_count value like this is thread-safe.
            with self.__serializer_flds.semaphore:       # Aquires Semaphore(n=1) with its context mgr.
                if access_count == self.__serializer_flds.total_count:
                    try:                           # (ID_Table,Field_Key_Name,Field_Name,ID_Field,Table_Key_Name)
                        rows = qryObj.execute(strSQL).fetchall()
                        if not rows:
                            retValue = f'ERR_INP_TableNotFound {SYS_FIELDS_NAME} - {callerFunction()}'
                            db_logger.warning(f'{retValue}')
                    except (sqlite3.Error, sqlite3.DatabaseError, sqlite3.OperationalError, Exception) as e:
                        retValue = f'ERR_DBRead: {e} - {callerFunction()}'
                        db_logger.error(retValue)
                        raise sqlite3.DatabaseError(retValue)
                    tblNameList = {j[4] for j in rows}  # set con tableNames de todas las tablas del sistema

                    for name in tblNameList:
                        tempFldList = [j for j in rows if j[4] == name]
                        # __sysFields = {tblName1:{fldName1:(dbFldName, fldIndex, Compare_Index, Hidden_Generated),},}
                        self.__sysFieldsCopy[name] = {tempFldList[k][1]: (tempFldList[k][2], tempFldList[k][3],
                                                                          tempFldList[k][6], tempFldList[k][7])
                                                        for k in range(len(tempFldList))}
                    self.__sysFields = self.__sysFieldsCopy
                    self.__reloadFields = False  # Resets action flag ONLY if this is the last caller in the chain.
                    retValue = True
                else:
                    retValue = False
            # Releases global lock at the end of the with block.
        # End of outer 'with' block: Here, __exit__() is called and internal access_counter is decremented.
        if retValue:
            db_logger.info(f'Hola, reloadFields() just run!...')
        return retValue                         # True: Update completed. False: No update of _sys_Tables


    def reloadTables(self):
        qryObj = SQLiteQuery.fetchObject(db_name=self.dbName)  # Gets query object for calling Thread and db used.
        strSQL = f"SELECT Table_Key_Name, Table_Name, ID_Table, Bitmask_Table, Methods FROM '{SYS_TABLES_NAME}'"
        with self.__serializer_tbls:     # This 'with' creates a thread-safe value for access_count to be pulled below.
            access_count = self.__serializer_tbls.access_count
            with self.__serializer_tbls.semaphore:  # Acquires Semaphore(n=1) to update _sysTables.
                if access_count == self.__serializer_tbls.total_count:
                    try:
                        # fetchall() -> (tblName, tblIndex, isAutoIncrement, isWITHOUTROWID, table_bitmask, Methods)
                        rows = qryObj.execute(strSQL).fetchall()
                        if rows:
                            retValue = False
                        else:
                            retValue = f'ERR_DB_Access: cannot read from {SYS_TABLES_NAME}-' \
                                       f'{callerFunction(2, getCallers=True)}'
                            db_logger.error(f'{retValue}')
                    except (sqlite3.Error, sqlite3.DatabaseError, sqlite3.OperationalError, Exception) as e:
                        retValue = f'ERR_DBRead: {e} - {callerFunction()}'  # DEBE estar ERR_ en los strings de error
                        db_logger.error(retValue)
                        raise sqlite3.DatabaseError(retValue)

                    # mode = 0 -> Retorna dbTblName
                    # mode!= 0 -> Retorna tupla (dbTblName, tblIndex, pkAutoIncrement(1/0), isROWID(1/0))
                    self.__sysTablesCopy = {rows[j][0]: [rows[j][1], rows[j][2], None, None, rows[j][3], rows[j][4]]
                                                  for j in range(len(rows))}
                    # Agrega campos de identificacion de AutoIncrement y WITHOUT ROWID para cada Tabla.
                    # AutoIncrement, WITHOUT ROWID longer needed. Left here only for reference on how to access them.
                    for t in self.__sysTablesCopy:
                        try:
                            strPragma = f'PRAGMA TABLE_INFO("{self.__sysTablesCopy[t][0]}")'
                            cursor = qryObj.execute(strPragma)
                            tblData = cursor.fetchall()
                            colNames = [j[0] for j in cursor.description]
                            pkIndex, typeIndex = colNames.index('pk'), colNames.index('type')
                            pkFieldsIndices = [tblData.index(j) for j in tblData if
                                               j[pkIndex]]  # Lista de index de PK de cada tbl: 1 index per PK column
                            if len(pkFieldsIndices) == 1 and str(tblData[pkFieldsIndices[0]][typeIndex]).upper() == \
                                    'INTEGER':
                                self.__sysTablesCopy[t][2] = 1  # 3rd Field: 1 ->AutoIncrement (DO NOT REUSE Indices)
                            else:
                                self.__sysTablesCopy[t][2] = 0  # 3rd Field: 0 -> Not AutoIncrement (REUSE Indices)

                            # Define si es WITHOUT ROWID para actualizar AutoIncrement y WITHOUT_ROWID
                            strPragma = f'PRAGMA INDEX_INFO("{self.__sysTablesCopy[t][0]}")'
                            tblData = qryObj.execute(strPragma).fetchall()
                            if len(tblData):
                                self.__sysTablesCopy[t][2] = 0  # Cuando tabla es WITHOUT ROWID
                                self.__sysTablesCopy[t][3] = 1  # WITHOUT ROWID -> __sysTables[t][3]=1 -> None NO autoincrementa
                            else:
                                self.__sysTablesCopy[t][3] = 0  # No es WITHOUT ROWID -> __sysTables[t][3]=0 (Autoincr.)
                        except (sqlite3.Error, sqlite3.DatabaseError, sqlite3.OperationalError, Exception) as e:
                            self.__sysTablesCopy[t][2] = self.__sysTablesCopy[t][3] = None
                            db_logger.error(f'Cannot load data from {SYS_TABLES_NAME}. Error: {e}')
                            raise sqlite3.DatabaseError(f'Cannot load data from {SYS_TABLES_NAME}. Error: {e}')
                        # print(f'tblData({t})/{colNames}: {pkFieldsIndices}/Auto Incr:{getTblName.__sysTables[t][2]}')

                    # Updates __sysTables with fresh data read from DB.
                    self.__sysTables = self.__sysTablesCopy
                    self.__reloadTables = False
                    retValue = True
                else:
                    retValue = False
            # Releases global lock at the end of the inner 'with' block.
        # End of outer 'with' block: Here, __exit__() is called and internal access_counter is decremented.
        if retValue:
            db_logger.info(f'Hola, reloadTables() just run!...')
        return retValue                                 # True: Update completed. False: No update of _sys_Tables


    def set_reloadFields(self):
        """ Signals to reload table _sysFields into memory """
        self.__reloadFields = True

    def set_reloadTables(self):
        """ Signals to reload table _sysTables into memory """
        self.__reloadTables = True



    # __fld_nesting_levels = 0
    # def reloadFields00(self):
    #     """ True: Success / str: errorCode to return back to callers """
    #     # print(f'Hola, reloadFields()...')
    #     with self.__lock:  # TODO(cmt): This lock MUST execute BEFORE any computation in the func.
    #         self.__fld_nesting_levels += 1
    #         my_nesting_level = self.__fld_nesting_levels  # local var to handle nested execution by different threads.
    #
    #     qryObj = SQLiteQuery.fetchObject(db_name=self.dbName)  # Conexion para DB y para thread desde donde se llama.
    #     strSQL = f"SELECT ID_Table, Field_Key_Name, Field_Name, ID_Field, Table_Key_Name, Excluded_Field, " \
    #              f"Compare_Index, Hidden_Generated FROM '{SYS_FIELDS_NAME}' "
    #     try:
    #         rows = qryObj.execute(strSQL).fetchall() #(ID_Table, Field_Key_Name, Field_Name, ID_Field, Table_Key_Name)
    #         if not rows:
    #             retValue = f'ERR_INP_TableNotFound {SYS_FIELDS_NAME} - {callerFunction()}'
    #             db_logger.warning(f'{retValue}')
    #     except (sqlite3.Error, sqlite3.DatabaseError) as e:
    #         retValue = f'ERR_DBRead: {e} - {callerFunction()}'
    #         db_logger.error(retValue)
    #         raise DBAccessError(retValue)
    #
    #     retValue = False
    #     tblNameList = {j[4] for j in rows}  # set con tableNames de todas las tablas del sistema
    #     for name in tblNameList:
    #         tempFldList = [j for j in rows if j[4] == name]
    #         # Arma dict __sysFields = {tblName1:{fldName1:(dbFldName, fldIndex, Compare_Index, Hidden_Generated), },}
    #         self.__sysFieldsCopy[name] = {tempFldList[k][1]: (tempFldList[k][2], tempFldList[k][3], tempFldList[k][6],
    #                                                           tempFldList[k][7])
    #                                       for k in range(len(tempFldList))}
    #     if my_nesting_level == self.__fld_nesting_levels:  # Last nested caller reached: must update __sysFields dict
    #         with self.__lock:
    #             if my_nesting_level == self.__fld_nesting_levels:  # Repeats just in case there was a change above.
    #                 self.__sysFields = self.__sysFieldsCopy
    #                 self.__reloadFields = False  # Resets action flag ONLY if this is the last caller in the chain.
    #                 retValue = True
    #             if my_nesting_level == 1:  # Back to the 1st caller of the nesting chain:
    #                 self.__fld_nesting_levels = 0  # ... resets __tbl_nesting_levels.
    #     if retValue:
    #         db_logger.info(f'Hola, reloadFields()...')
    #     return retValue  # True: Update completed. False: No update of _sys_Tables


# ------------------------------------------------ END Class _DataBase ----------------------------------------------- #



# class SerializerLockOriginal01(object):
#     """
#        Implements a soft lock using and Condition object and its wait functions and avoiding the use of the general
#        Python mutex (except for the uses of lock in the Condition class code).
#        Designed to serialize access to shared data resources (in memory and db) when they are accessed concurrenty by
#        foreground and bkgd threads. Operates by allowing the 1st call to the protected code to proceed and putting
#        any subsequent calls from concurrent threads to wait while the protected code is run. Once on wait(), the mutex
#        lock is released for the rest of the system to continue running.
#        If another thread sharing the AccessSerializer object attempts to run, the wait() method in acquire() will block it
#        until the executing thread notifies() 1 of the waiting threads to resume. They are notified one by one to enforce
#        serial access to the shared resource.
#        Usage:
#        1)  my_lock = AccessSerializer(resource)     or, alternatively    my_lock = AccessSerializer(resource)
#            ...                                                               ...
#            ...                                                               ...
#            my_lock.acquire()                                            with my_loc:
#            ...Protected access to resource ...                              ...Protected access to resource ...
#            my_loc.release()
#         Important: acquire()/release() work in pairs (as a normal lock). Failure to conform may result in freezes.
#
#        2) Can use the value returned by acquire() to implement a selective read/write of shared resources, as shown in
#        _init_uid_dicts() method.
#
#        3) This class implements a sacha-singleton system based on the resource argument passed to the Constructor:
#             - If resource is None, it behaves exactly the same as the previous AccessSerializer: creates 1 new object
#               everytime.
#             - If resource is != None, it creates only 1 object per resource value, stores it in a dictionary and any
#               subsequent calls with the same resource value will yield the same object.
#             - If resource is str and starts with 'tbl', assumes it's a database table and creates a unique name as
#               "db_name.resource", so that multiple databases with equal table names can be handled in a dictionary.
#        """
#     __slots__ = ('_resource', '_lock_obj', '_timeout', '_acquired_count', '_total_count', '_blocking_status', '__reset',
#                  '__acq_count_stack')
#     __lock = threading.Lock()
#
#     def __init__(self, *, timeout=None):
#         """
#         @param timeout: (int, float): seconds to timeout a wait().
#         blocking is NOT an instance attribute, but a thread attribute. Should NOT be initialized in __init__().
#         """
#         self._lock_obj = threading.Condition()
#         self._timeout = timeout if isinstance(timeout, (int, float)) and (0 <= timeout <= 1000) else 5
#         self._acquired_count = 0        # Keeps track of concurrent acquisitions of the lock by different threads.
#         self._total_count = 0           # The total number of times the lock has been concurrently acquired.
#
#         # _blocking_status Used by __call__ to pass wait/nowait setting to acquire(). DYNAMIC ATTRIBUTE.
#         # 1 value per thread. If needed, use a stack (analogous to __acq_count_stack).
#         self._blocking_status = {}      # { thread_id: block(True/False), }
#         self.__acq_count_stack = collections.defaultdict(list)  # Stack to pass _acquired_count to the outside world.
#         self.__reset = False        # Signals object reset ongoing.
#
#     def __call__(self, *, blocking=None):
#         """ Executed when an instance object is invoked with arguments (or just with parenthesis).
#             Passes blocking setting to __enter__() function.
#             @param blocking: Bool. Defines whether AccessSerializer obj will enter the wait loop for thread. Default:True.
#             Used to implement the passing of blocking setting when using context manager.
#             Usage: lock_obj(blocking=True/False).
#         """
#         if blocking is not None:
#             self._blocking_status[threading.current_thread().ident] = bool(blocking)
#         return self
#
#     def acquire(self, *, blocking=None, timeout=None) -> int:
#         """     *** This is actually and acquire_and_wait() method. Puts thread to wait if concurrent access occurs. ***
#         Any subsequenquent call to protected code after the 1st one is put to wait until an assigned instance in the
#         list of waiting callers notifies the other threads to proceed.
#         @param blocking: (bool): Implements a blocking or non-blocking lock. Default: True.
#         @param timeout: (int, float). Time in seconds to run out the Condition obj wait loop.
#         @return: Sequence order of the concurrent call (int). Return value can be used to manage execution of the
#         protected code with an 'if' statement and the following 3 options:
#             - Ignore return value: all the concurrent threads execute the code in random order as they get notified.
#             - Compare with 1: FIRST thread to enter the code executes the protected block. The others skip the code.
#             - Compare with _total_count: LAST thread to enter the code executes the code. The others skip the code.
#         """
#         thread_id = threading.current_thread().ident
#         timeout = timeout if isinstance(timeout, (int, float)) and (0 <= timeout <= 1000) else self._timeout
#         with self._lock_obj:                    # _lock_obj is a Condition object.
#             self._acquired_count += 1           # Must keep track of # of threads accesing the lock concurrently.
#             # Thread code operates in a nested fashion, so the stack below will work to pass _acquired_count out.
#             self.__acq_count_stack[thread_id].append(self._acquired_count)  # Adds count to stack to make it retrievable
#             self._total_count = self._acquired_count
#             my_count = self._acquired_count  # local variable keeps number of this call instance.
#
#             if self._acquired_count > 1:       # 1st call: doesn't wait. Subsequent calls: they go to wait().
#                 # Initializes _blocking_status and waits only for concurrent threads coming after the 1st one.
#                 if blocking is None:
#                     if thread_id not in self._blocking_status:
#                         # When blocking arg not passed, intializes ONLY if thread_id still not in _blocking_status.
#                         # This ensures that _blocking_status[thread_id] is initialized only once when None is passed.
#                         self._blocking_status[thread_id] = True
#                 else:
#                     self._blocking_status[thread_id] = bool(blocking)  # Forces blocking value if passed in acquire()
#                 if self._blocking_status.get(thread_id):        # Throw KeyError if thread_id not in dict.
#                     _ = self._lock_obj.wait(timeout)   # All subsequent calls are put to wait. Lock is released here.
#                     # Then lock is re-acquired here by Condition after wait ends.It is then released by the context mgr.
#         return my_count   # Returns local var my_count, which is used to implement selective access to shared resources.
#
#     begin = acquire
#
#     def _wait00(self, *, timeout=None):
#         """ Enters a wait state and waits for notify() from other parts of the code to resume execution.
#         Executed only when there are concurrent threads active (_acquired_count > 1).
#         @return: Wait operation result. True if no timeout; False if wait timed-out.
#         IMPORTANT: Upon exit of this func, the global lock IS ALREADY RELEASED.
#         If any code must be run protected by the global lock AFTER a Condition wait() call, it must be manually coded
#         using the obj attribute within this class.
#         """
#         timeout = timeout if isinstance(timeout, (int, float)) and (0 <= timeout <= 1000) else self._timeout
#         with self._lock_obj:  # MUST acquire global lock whitin Condition in order to be able to execute wait().
#             if self._acquired_count > 1:
#                 return self._lock_obj.wait(timeout)  # Releases global lock and thread sits here until is notified.
#                 # Re-acquires the global lock when _wait() returns.
#         # TODO(cmt) - CAUTION: Upon exit of this block, the global lock acquired at the end of _wait() is released.
#     # _block = _thread_block = _wait  # Various names for blocking. See what sticks.
#
#
#     def _wait(self, *, timeout=None, relase_lock_on_exit=True):     # Old version
#         """ Enters a wait state and waits for notify() from other parts of the code to resume execution.
#         Executed only when there are concurrent threads active (_acquired_count > 1).
#         @return: Wait operation result. True if no timeout; False if wait timed-out. """
#
#         timeout = timeout if isinstance(timeout, (int, float)) and (0 <= timeout <= 1000) else self._timeout
#         res = True
#         self._lock_obj.acquire()             # MUST acquire to be able to execute wait().
#         if self._acquired_count > 1:  # WRONG place for testing on self._acquired_count. Must be inside lock.
#             res = self._lock_obj.wait(timeout)  # thread sits here until is notified by a release() from another thread.
#         if relase_lock_on_exit:
#             self._lock_obj.release()
#         return res                              # Returns result of wait (timeout or no timeout).
#     _block = _thread_block = _wait              # Various names for blocking. See what sticks.
#
#
#     def wait_and_lock(self, *, timeout=None):     # Old version
#         """ Enters a wait state and waits for notify() from other parts of the code to resume execution.
#         Executed only when there are concurrent threads active (_acquired_count > 1).
#         @return: Wait operation result. True if no timeout; False if wait timed-out. """
#
#         timeout = timeout if isinstance(timeout, (int, float)) and (0 <= timeout <= 1000) else self._timeout
#         res = True
#         self._lock_obj.acquire()             # MUST acquire to be able to execute wait().
#         if self._acquired_count > 1:  # WRONG place for testing on self._acquired_count. Must be inside lock.
#             res = self._lock_obj.wait(timeout)  # thread sits here until is notified by a release() from another thread.
#         return res                              # Returns result of wait (timeout or no timeout).
#
#     def lock_release(self):
#         if self.__lock.locked():
#             self._lock_obj.release()
#
#
#     def release(self) -> int:
#         with self._lock_obj:           # Acquire lock in Condition object.
#             self._acquired_count -= 1
#             if self._acquired_count < 0:
#                 self._acquired_count += 1       # Return to previous state before throwing exception.
#                 raise RuntimeError(f'ERR_RuntimeError AccessSerializer: attempt to release an unacquired lock.')
#             self.__acq_count_stack[threading.current_thread().ident].pop()  # If this line throws except. -> Logic is wrong.
#
#             if self._acquired_count == 0:           # Exhausted all concurrent calls to the protected code block
#                 self._total_count = 0               # Resets control variable for next usage of the lock.
#                 if self.__reset:            # Checks for reset request only when reaching 0 (no threads left waiting).
#                     self._blocking_status = {}      # Clears _blocking_status dict only when object reset.
#                     self.__reset = False
#             # VERY IMPORTANT: Must notify() only 1 thread to enforce serialized access to the shared resource (DO NOT
#             # use notify_all()). This is at the heart of the serialization of access provided by AccessSerializer class.
#             elif self._acquired_count > 1:    # Notifies only if there are concurrent (more than 1) threads waiting.
#                 self._lock_obj.notify()  # Notifies 1 waiting thread at a time to resume execution of blocked section.
#
#             return self._acquired_count                    # return value not used for now...
#     end = release
#
#
#     def release00(self) -> int:     # Old version. Needs context mgr. to handle internal exception raise.
#         self._lock_obj.acquire()            # Acquire lock in Condition object.
#         self._acquired_count -= 1
#         if self._acquired_count < 0:
#             self._acquired_count += 1       # Return to previous state before throwing exception.
#             raise RuntimeError(f'ERR_RuntimeError AccessSerializer: attempt to release an unacquired lock.')
#
#         if self._acquired_count == 0:           # Exhausted all concurrent calls to the protected code block
#             self._total_count = 0               # Resets control variables for next usage of the lock.
#             if self.__reset:
#                 self._blocking_status = {}      # Clears _blocking_status dict only when object reset.
#                 self.__reset = False
#         # VERY IMPORTANT: Must notify() only 1 thread to enforce serialized access to the shared resource (DO NOT use
#         #     notify_all()). This is at the heart of the serialization of access provided by the AccessSerializer class.
#         elif self._acquired_count > 1:    # Notifies only if there are concurrent (more than 1) threads waiting.
#             self._lock_obj.notify()  # Notifies 1 waiting thread at a time to resume execution of the blocked section.
#
#         self.__acq_count_stack[threading.current_thread().ident].pop()
#         count = self._acquired_count
#         self._lock_obj.release()
#         return count                    # return value not used for now...
#     # end = release
#
#
#     def set_blocking(self, blocking):
#         thread_id = threading.current_thread().ident
#         # if thread_id in self._blocking_status:
#         if blocking is not None:        # if None, ignores. Exits without updating.
#             self._blocking_status[thread_id] = bool(blocking)
#
#
#     @property
#     def access_count(self):         # This stack is thread-safe. It's ok to return a value like it's done here.
#         try:
#             return self.__acq_count_stack[threading.current_thread().ident][-1]  # Returns last item in stack for thread
#         except (KeyError, IndexError, AttributeError):
#             return 0          # Stack empty or thread_id not in thread_stack dict -> Access count is 0.
#
#     @property
#     def total_count(self):
#         with self.__lock:
#             return self._total_count
#
#     @property
#     def condition(self):            # Access to underlying Condition object for whatever is needed.
#         return self._lock_obj       # Used to execute code sections under the global lock.
#
#     # @property
#     # def lock(self):
#     #     return self.__lock
#
#
#     def __del__(self):
#         # Releases any waiting threads before obj deletion occurs.TODO: HOWEVER, DO NOT DELETE AccessSerializer objects!
#         self.reset()
#
#
#     def reset(self):
#         """ Return AccessSerializer object to known state. In case it's needed."""
#         with self._lock_obj:
#             # self._acquired_count = 0
#             # self._total_count = 0
#             # self._blocking_status = {}
#             # self.__acq_count_stack.clear()
#             if not self.__reset:            # runs only once per reset cycle.
#                 self.__reset = True
#                 # Releases all waiting threads. Leaves self object in a state that can continue to operate.
#                 self._lock_obj.notify_all()
#
#
#     # The 2 methods below implement the context mgr. for AccessSerializer class.
#     def __enter__(self):
#         # TODO(cmt): acquire() return value is the simple way to safely return the concurrent access order to the caller
#         #  Here, it was implemented via via __acq_count_stack to define a fully compatible context manager.
#         _ = self.acquire()
#         return self
#         # return self.acquire()  # returns int: the call order of the concurrent call (1, 2, etc.) for use by the caller
#
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         _ = self.release()
#         if exc_type is not None:
#             krnl_logger.error(f'AccessSerializer Exception raised: {exc_type}, {exc_val}. Traceback: {exc_tb}.')
#             return False     # False signals that the exception needs to be handled by the outside code.
#     """                                  A few words on context managers
#     # When the with statement executes, it calls .__enter__() on the context manager object to signal that you’re
#     # entering into a new runtime context. If you provide a target variable with the as specifier, then the return
#     # value of .__enter__() is assigned to that variable.
#     #
#     # When the flow of execution leaves the context, .__exit__() is called. If no exception occurs in the with code
#     # block, then the three last arguments to .__exit__() are set to None. Otherwise, they hold the type, value, and
#     # traceback associated with the exception at hand.
#     #
#     # If the .__exit__() method returns True, then any exception that occurs in the with block is swallowed and
#     # the execution continues at the next statement after with. If .__exit__() returns False, then exceptions are
#     # propagated out of the context. This is also the default behavior when the method doesn’t return anything
#     # explicitly. You can take advantage of this feature to encapsulate exception handling inside the context manager.
#     # See: https://realpython.com/python-with-statement/
#     """
#
# # ------------------------------------------ End class AccessSerializer ----------------------------------------#
#
#
#
# class SerializerLockOriginal(object):  # Does not implement access to _acquired_count attribute. DEPRECATED!!
#     """
#        Implements a soft lock using and Condition object and its wait functions and avoiding the use of the general
#        Python mutex (except for the uses of lock in the Condition class code).
#        Designed to serialize access to shared data resources (in memory and db) when they are accessed concurrenty by
#        foreground and bkgd threads. Operates by allowing the 1st call to the protected code to proceed and putting
#        any subsequent calls from concurrent threads to wait while the protected code is run. Once on wait(), the mutex
#        lock is released for the rest of the system to continue running.
#        If another thread sharing the AccessSerializer object attempts to run, the wait() method in acquire() will block
#        it until the executing thread notifies() 1 of the waiting threads to resume. They are notified one by one to
#        enforce serial access to the shared resource.
#        Usage:
#        1)      my_lock = AccessSerializer()           or, alternatively       my_lock = AccessSerializer()
#                ...                                                               ...
#                ...                                                               ...
#                my_lock.acquire()                                            with my_loc:
#                ....Protected block ...                                          Protected block
#                my_loc.release()
#         Important: acquire()/release() work in pairs (as a normal lock). Failure to conform may result in freezes.
#
#        2) Can use the value returned by acquire() to implement a selective read/write of shared resources, as shown in
#        _init_uid_dicts() method.
#        """
#     def __init__(self, *, timeout=None):
#         """
#         @param timeout: (int, float): seconds to timeout a wait().
#         blocking is NOT an instance attribute, but a thread attribute. Should NOT be initialized in __init__().
#         """
#         self._lock_obj = threading.Condition()
#         self._timeout = timeout if isinstance(timeout, (int, float)) and (0 <= timeout <= 1000) else 5
#         self._acquired_count = 0        # Keeps track of concurrent acquisitions of the lock by different threads.
#         self._total_count = 0           # The total number of times the lock has been concurrently acquired.
#
#         # _blocking_status Used by __call__ to pass wait setting to __enter__(). DYNAMIC ATTRIBUTE.
#         # 1 value per thread since the lock works in tandem acquire()/release() actions.
#         self._blocking_status = {}      # { thread_id: block(True/False), }
#
#
#     def __call__(self, *, blocking=None, **kwargs):
#         """ Executed when an instance object is invoked with arguments (or just with parenthesis).
#             Passes blocking setting to __enter__() function.
#             @param blocking: Bool. Defines whether or not AccessSerializer obj will enter the wait loop. Default: True.
#             Used to implement the passing of blocking setting when using context manager.
#             Usage: lock_obj(blocking=True/False).
#         """
#         if blocking is not None:
#             self._blocking_status[threading.current_thread().ident] = bool(blocking)
#         return self
#
#     def acquire(self, *, blocking=None, timeout=None):
#         """
#         Any subsequenquent call to protected code after the 1st one is put to wait until an assigned instance in the
#         list of waiting callers notifies the other threads to proceed.
#         @param blocking: (bool): Implements a blocking or non-blocking lock. Default: True.
#         @param timeout: (int, float). Time in seconds to run out the Condition obj wait loop.
#         @return: Sequence order of the concurrent call (int). Return value can be used to manage execution of the
#         protected code with an 'if' statement and the following 3 options:
#             - Ignore return value: all the concurrent threads execute the code in random order as they get notified.
#             - Compare with 1: FIRST thread to enter the code executes the code. The others skip the code.
#             - Compare with total_count: LAST thread to enter the code executes the code. The others skip the code.
#         """
#         thread_id = threading.current_thread().ident
#         timeout = timeout if isinstance(timeout, (int, float)) and (0 <= timeout <= 1000) else self._timeout
#         with self._lock_obj:                    # _lock_obj is a Condition object.
#             self._acquired_count += 1           # Must keep track of # of threads accesing the lock concurrently.
#             self._total_count = self._acquired_count
#             my_count = self._acquired_count  # local variable keeps number of this call instance.
#
#             if self._acquired_count > 1:       # 1st call: doesn't wait. Subsequent calls: they go to wait().
#                 # Initializes _blocking_status and waits only for concurrent threads coming after the 1st one.
#                 if blocking is None:
#                     if thread_id not in self._blocking_status:
#                         # When blocking arg not passed, intializes ONLY if thread_id still not in _blocking_status.
#                         # This ensures that _blocking_status[thread_id] is initialized only once when None is passed.
#                         self._blocking_status[thread_id] = True
#                 else:
#                     self._blocking_status[thread_id] = bool(blocking)  # Forces blocking value if passed in acquire()
#                 if self._blocking_status.get(thread_id):        # Throw KeyError if thread_id not in dict.
#                     _ = self._lock_obj.wait(timeout)   # All subsequent calls are put to wait. Lock is released here.
#         return my_count   # Returns local var my_count, which is used to implement selective access to shared resources.
#
#     begin = acquire
#
#
#     def _wait(self, *, timeout=None):
#         """ Enters a wait state and waits for notify() from other parts of the code to resume execution.
#         Executed only when there are concurrent threads active (_acquired_count > 1).
#         @return: Wait operation result. True if no timeout; False if wait timed-out. """
#         if self._acquired_count > 1:
#             timeout = timeout if isinstance(timeout, (int, float)) and (0 <= timeout <= 1000) else self._timeout
#             self._lock_obj.acquire()             # MUST acquire to be able to execute wait().
#             res = self._lock_obj.wait(timeout)   # thread sits here until is notified by release() from another thread.
#             self._lock_obj.release()
#             return res
#     _block = _thread_block = _wait              # Various names for blocking. See what sticks.
#
#
#     def release(self) -> int:
#         self._lock_obj.acquire()            # Acquire lock in Condition object.
#         self._acquired_count -= 1
#         if self._acquired_count < 0:
#             self._acquired_count += 1       # Return to previous state before throwing exception.
#             raise RuntimeError(f'ERR_RuntimeError AccessSerializer: attempt to release an unacquired lock.')
#
#         if self._acquired_count == 0:           # Exhausted all concurrent calls to the protected code block
#             self._total_count = 0               # Resets control variables for next usage of the lock.
#             self._blocking_status.clear()       # Clears _blocking_status dict in the final release() call.
#         # VERY IMPORTANT: Must notify() only 1 thread to enforce serialized access to the shared resource (DO NOT use
#         #     notify_all()). This is at the heart of the serialization of access provided by the AccessSerializer class.
#         elif self._acquired_count > 1:    # Notifies only if there are concurrent (more than 1) threads waiting.
#             self._lock_obj.notify()  # Notifies 1 waiting thread at a time to resume execution of the blocked section.
#
#         count = self._acquired_count
#         self._lock_obj.release()
#         return count
#     end = release
#
#     @property
#     def total_count(self):
#         return self._total_count        # Should be run in a lock!!
#
#     @property
#     def obj(self):             # Access to underlying Condition object for whatever is needed. Remove if possible.
#         return self._lock_obj
#
#     def reset(self):
#         """ Return AccessSerializer object to known state. In case it's needed."""
#         self._lock_obj.acquire()
#         self._acquired_count = 0
#         self._total_count = 0
#         self._blocking_status = {}
#         self._lock_obj.notify_all()         # Releases any hung-up threads.
#         self._lock_obj.release()
#
#     # The 2 methods below implement the context mgr. for AccessSerializer class.
#     def __enter__(self):
#         # TODO: DO NOT return self here! This is the only way to safely (and in a simple manner) return _acquired_count
#         #  to the outside caller. self can be accessed in a host of other ways.
#         return self.acquire()  # returns int: the call order of the concurrent call (1, 2, etc.) for use by the caller.
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.release()
#         if exc_type is not None:
#             krnl_logger.error(f'AccessSerializer Exception raised: {exc_type}, {exc_val}. Traceback: {exc_tb}.')
#             return False     # False signals that the exception needs to be handled by the outside code.
#     """                                  A few words on context managers
#     # When the with statement executes, it calls .__enter__() on the context manager object to signal that you’re
#     # entering into a new runtime context. If you provide a target variable with the as specifier, then the return
#     # value of .__enter__() is assigned to that variable.
#     #
#     # When the flow of execution leaves the context, .__exit__() is called. If no exception occurs in the with code
#     # block, then the three last arguments to .__exit__() are set to None. Otherwise, they hold the type, value, and
#     # traceback associated with the exception at hand.
#     #
#     # If the .__exit__() method returns True, then any exception that occurs in the with block is swallowed and
#     # the execution continues at the next statement after with. If .__exit__() returns False, then exceptions are
#     # propagated out of the context. This is also the default behavior when the method doesn’t return anything
#     # explicitly. You can take advantage of this feature to encapsulate exception handling inside the context manager.
#     # See: https://realpython.com/python-with-statement/
#     """

# ---------------------------------------------- End class AccessSerializer ------------------------------------------#


# Creates database objects for databases in use by the system.
db_main = _DataBase(name=MAIN_DB_NAME)           # GanadoSQLite.db
db_nr = _DataBase(name=NR_DB_NAME)               # Non-replicable database.
# db_data_stream = _DataBase(name=DATASTREAM_DB_NAME)

def tables_in_query01(sql_str, inspect_select_tables=False):
    """ Parses sql_str and returns a set of db tables pulled from the string
    @param inspect_select_tables: Returns set of tables found between 1st SELECT and 1st FROM in sql string.
    @param sql_str: (str) SQL statement (usually a SELECT).
     @param inspect_multi_tables: Returns a set with table names present in the first SELECT / FROM statement of sql_str
            Used by read_sql_query() decorator to setup DataFrame db Accessor.
    @return: list (set) of table names, each table_name is (str).
    Code from https://grisha.org/blog/2016/11/14/table-names-from-sql/
    """
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])

    # if '"' in q:
    #     tokens = q.split('"')  # splits quotes (", ') to account for compound table names ("Animales Actividades")
    # elif "'" in q:
    #     tokens = q.split("'")
    # else:
    #     # split on blanks, parenthesis, semicolons
    #     tokens = re.split(r"[\s)(;]+", q)


    # Pulls table names between the 1st SELECT and FROM to flag if there are multiple tables in the SELECT statement
    str0 = sql_str.lower().split('from')[0]
    select_tables = set()
    if "." in str0:
        for i, tok in enumerate(tokens):
            if tok == '.':                          # The item previous to '.' is a table name.
                select_tables.add(tokens[i-1])      # appends all table names found BEFORE 1st 'FROM'.
            elif 'from' in tok.lower():             # Finds 1st 'FROM' in tokens and quits.
                break


    if inspect_select_tables:
        return select_tables            # Returns list of tables present in first SELECT / FROM statement of sql_str.

    # Original code: Pulls all table names.
    result = [select_tables.pop(), ] if len(select_tables) == 1 else []  # if 1 item in select_tables, it'll be tbl_name
    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).
    # split on blanks, parenthesis, semicolons
    tokens = re.split(r"[\s)(;]+", q)
    get_next = False
    for i, tok in enumerate(tokens):
        if get_next:
            if tok.lower() not in ("", "select"):
                tok = re.sub('["\']', "", tok)         # Remove all '' and "". Not needed now (removed in if above).
                result.append(tok)
            get_next = False
        else:
            get_next = any(j in tok.lower() for j in ("from", "join"))
    return tuple(dict.fromkeys(result))  # Must tupleize to index elements. dict.fromkeys() keeps list order.



def tables_in_query(sql_str, inspect_select_tables=False):
    """ Parses sql_str and returns a set of db tables pulled from the string
    @param inspect_select_tables: Returns set of tables found between 1st SELECT and 1st FROM in sql string.
    @param sql_str: (str) SQL statement (usually a SELECT).
     @param inspect_multi_tables: Returns a set with table names present in the first SELECT / FROM statement of sql_str
            Used by read_sql_query() decorator to setup DataFrame db Accessor.
    @return: list (set) of table names, each table_name is (str).
    Code from https://grisha.org/blog/2016/11/14/table-names-from-sql/
    """
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])

    if '"' in q:
        tokens = q.split('"')  # splits quotes (", ') to account for compound table names ("Animales Actividades")
    elif "'" in q:
        tokens = q.split("'")
    else:
        # split on blanks, parenthesis, semicolons
        tokens = re.split(r"[\s)(;]+", q)


    # Pulls table names between the 1st SELECT and FROM to flag if there are multiple tables in the SELECT statement
    select_tables = set()
    for i, tok in enumerate(tokens):
        if tok == '.':                          # The item previous to '.' is a table name.
            select_tables.add(tokens[i-1])      # appends all table names found BEFORE 1st 'FROM'.
        elif 'from' in tok.lower():             # Finds 1st 'FROM' in tokens and quits.
            break
    if inspect_select_tables:
        return select_tables            # Returns list of tables present in first SELECT / FROM statement of sql_str.

    # Original code: Pulls all table names.
    result = [select_tables.pop(), ] if len(select_tables) == 1 else []  # if 1 item in select_tables, it'll be tbl_name
    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).
    get_next = False
    for i, tok in enumerate(tokens):
        if get_next:
            if tok.lower() not in ("", "select"):
                tok = re.sub('["\']', "", tok)         # Remove all '' and "". Not needed now (removed in if above).
                result.append(tok)
            get_next = False
        else:
            get_next = any(j in tok.lower() for j in ("from", "join"))
    return tuple(dict.fromkeys(result))  # Must tupleize to index elements. dict.fromkeys() keeps list order.


def getTblName(tbl: str = '', mode=0, *, db_name=None, reload=False, db_table_name=None):
    """
    Gets the name of a table by its keyname
            mode: O -> dbTblName (str)
                  1 -> (dbTblName, tblIndex, isAutoIncrement, isWithoutROWID, tbl_bitmask, Methods) (tuple)
                  db_table_name: returns tblName for the db_table_name provided. Ignores the other settings.
                  db_name: Database name.
    @return: tableName (str), tuple (dbTblName, tblIndex, AutoIncrement, isWITHOUTROWID, tbl_bitmask) or errorCode (strError)
    """
    db_obj = _DataBase.getObject(db_name=(db_name or MAIN_DB_NAME))
    if not isinstance(db_obj, _DataBase):
        return 'ERR_DBAccess: database not found.'
    if reload:
        db_obj.set_reloadTables()

    if db_obj.reload_tables > 0:            # @property to differentiate it from reloadTables() func.
        _ = db_obj.reloadTables()
        if isinstance(_, str):
            db_logger.error(f'reloadTables() failed!.')
            return _ if mode != 1 else _, None, None, None, None, None
        db_logger.info(f'reloadTables() function called. return = {_}')

    if db_table_name:
        # Reverse lookup: Busca tblName a partir de dbTblName
        return next((k for k in db_obj.sys_tables if db_obj.sys_tables[k][0].lower() == db_table_name.lower()),
                        f"ERR_INP_Invalid arguments: table {db_table_name} not found.")

    if tbl and isinstance(tbl, str):
        tbl = (" ".join(tbl.split()))  # Removes all excess leading, trailing and inner whitespace.
        if tbl[:3].lower() != 'tbl' and tbl != '*':      # Va '*' por las dudas (futuras extensiones a tablas)
            retValue = f'ERR_INP_InvalidArgument: {tbl}.'
            return retValue if mode != 1 else retValue, None, None, None, None, None

        if tbl in db_obj.sys_tables:
            if mode == 1:
                retValue = tuple(db_obj.sys_tables[tbl])        # Returns tuple
            else:
                retValue = db_obj.sys_tables[tbl][0]            # Returns db_table_name.
            return retValue

        else:   # tblName not found: Primero re-carga tabla en Memoria y busca la tabla no encontrada en __sysTables.
            _ = db_obj.reloadTables()
            if isinstance(_, str):
                db_logger.error(f'reloadTables() failed!. error: {_}')
                return _ if mode != 1 else _, None, None, None, None, None
            db_logger.info(f'reloadFields() function called.')
            if tbl in db_obj.sys_tables:
                retValue = db_obj.sys_tables[tbl][0] if not mode else tuple(db_obj.sys_tables[tbl])
            else:
                retValue = f'ERR_INP_TableNotFound {tbl} - {callerFunction(2, getCallers=True, namesOnly=True)}'
                db_logger.info(retValue)
            return retValue
    else:
        retValue = f'ERR_INP_InvalidArgument: {tbl}.'
        db_logger.info(retValue)
    return retValue if mode != 1 else retValue, None, None, None, None, None


def getFldName(tbl=None, fld: str = '*', mode=0, *,  db_name=None, reload=False, db_table_name=None,
               exclude_hidden_generated=False):
    """
    Gets the name of a field by the keynames of a table and a field
    tbl : table keyname (str)
    @param: db_table_name: if passed, ignores tbl. Only works with mode 1.
    fld : field keyname(str)
    mode:    0: Field Name (str: 1 field name - list: multiple field names)
             1: {Field Keyname: Field DBName,} (dict)
             2: {Field Keyname: [Field DBName, Field_Index, Compare_Index],} (dict)
             3: {Field Keyname: ID_Field,} (dict)
    @param exclude_hidden_generated: Excludes hidden and generated fields from field list (Default=False).
    Returns: string: 1 Field; string: ERR_ for Error; list []: Multiple fields
    """
    global __fldNameCounter
    __fldNameCounter += 1        # Contador de ejecucion de esta funcion. Debugging purposes only.

    db_obj = _DataBase.getObject(db_name=(db_name or MAIN_DB_NAME))
    if reload:
        db_obj.set_reloadFields()

    if db_obj.reload_fields:       # reload_fields is a @property to differentiate it from reloadFields() func.
        retValue = db_obj.reloadFields()
        if isinstance(retValue, (str, type(None))):
            db_logger.info('reloadFields() call failed!.')
            return retValue
        else:
            db_logger.info('reloadFields() function called.')

    if db_table_name:
        # Reverse lookup: Busca tblName a partir de dbTblName
        tbl = next((k for k in db_obj.sys_tables if db_obj.sys_tables[k][0].lower() == db_table_name.lower()), '')
        mode = 1
        fld = '*'         # This works only with mode 1 and fld='*'. Ignores any fldNames passed.
    fld = " ".join(fld.split()) if isinstance(fld, str) else '' # Removes excess leading, trailing and inner whitespace.

    if fld.lower().startswith('fld') or fld.startswith('*'):  # fldKeyNames son 'fld' o '*'
        tblName = " ".join(tbl.split())  # Removes all excess leading, trailing and inner whitespace.
        if tblName not in db_obj.sys_fields:
            _ = db_obj.reloadTables()
            db_logger.info(f'reloadTables() function called. return = {_}')
            _ = db_obj.reloadFields()
            db_logger.info(f'reloadFields() function called. return = {_}')

        if tblName not in db_obj.sys_fields:
            retValue = f'ERR_INP_TableNotFound: {tbl}'  # Sale si tblName no esta en diccionario __sysFields.
            db_logger.info(retValue)
            return retValue
        else:
            tblDict = db_obj.sys_fields[tblName]   # {fldName1:(dbFldName, fldIndex, Compare_Index, Hidden_Generated), }
            if exclude_hidden_generated:
                tblDict = {k: v for k, v in tblDict.items() if (v[3] is None or v[3] == 0)}

            if fld == '*':
                if not mode:
                    retValue = [tblDict[j][0] for j in tblDict]         # List [dbFldName, ]
                elif mode == 1:
                    retValue = {k: tblDict[k][0] for k in tblDict}      # Dict {fldName: dbFldName, }
                elif mode == 2:
                    retValue = tblDict                      # Dict {fldName: (dbFldName, fldIndex, Compare_Index), }
                else:
                    retValue = {k: tblDict[k][1] for k in tblDict}      # Dict {fldName: fldIndex, }
                return retValue
            else:  # Se selecciono 1 solo campo. Lo busca; si no lo encuentra va a recargar tabla de Fields
                if fld in tblDict:
                    # tblDict={fldName: (dbFldName, fldIndex, Compare_Index)}
                    retValue = tblDict[fld][0] if not mode else tblDict[fld]  # (dbFldName, fldIndex, Compare_Index)
                    return retValue
                else:           # No se encontro fieldName, va a regargar tabla de Fields
                    retValue = db_obj.reloadFields()
                    db_logger.info(f'reloadFields() function called. return = {retValue}')
                    if isinstance(retValue, (str, type(None))):
                        return retValue

                    tblDict = db_obj.sys_fields[tblName]
                    if fld in tblDict:
                        retValue = tblDict[fld][0] if not mode else tblDict[fld]
                    else:
                        retValue = f'ERR_INP_FieldNotFound: {fld} - {callerFunction(2, getcallers=True,namesOnly=True)}'
                        # db_logger.warning(f'NEW getFldName retValue:{retValue}.')
    else:
        retValue = f'ERR_INP_InvalidArgument: {fld}'
        db_logger.info(retValue)
    return retValue

__fldNameCounter = 0     # Con el atributo getFldName. -> no se puede importar/exportar. Esta se usa en test_threading

# ------------------------------------------------------------------------------------------------------------------ #

def getFldCompare(name1: str, name2: str):
    """ Returns Compare_Index if fld1 is comparable to fld2 (they share the same Compare_Index value).
    @param name1: tblName1.fldName1 (str)
    @param name2: tblName2.fldName2 (str)
    @return Compare_Index from [_sys_Fields] if fld1 and fld2 share Compare_Index values. Otherwise False.
    """
    if any(not isinstance(j, str) for j in (name1, name2)):
        return False

    split_name1 = name1.split(".")
    split_name2 = name2.split(".")
    tbl1, fld1 = (split_name1[0], split_name1[1]) if len(split_name1) >= 2 else (None, None)
    tbl2, fld2 = (split_name2[0], split_name2[1]) if len(split_name2) >= 2 else (None, None)
    if any(not i for i in (tbl1, fld1, tbl2, fld2)):        # not i computes to True for None, 0, (), '', {}, [].
        return False
    fldName1 = getFldName(tbl1, fld1, 1)    # retrieves tuple (dbFldName, fldIndex, Compare_Index) for tbl1.fldName1
    fldName2 = getFldName(tbl2, fld2, 1)    # retrieves tuple (dbFldName, fldIndex, Compare_Index) for tbl2.fldName2
    fld1CompIndex = fldName1[2] if isinstance(fldName1, (list, tuple)) else None
    fld2CompIndex = fldName2[2] if isinstance(fldName2, (list, tuple)) else None
    if fld1CompIndex is not None and fld2CompIndex is not None:
        if isinstance(fld1CompIndex, int) and isinstance(fld2CompIndex, int):
            return fld1CompIndex if fld1CompIndex == fld2CompIndex else False
        # elif any(isinstance(j, (tuple, list, set)) for j in (fld1CompIndex, fld2CompIndex)):
        else:
            compVals = [set(j) if isinstance(j, (list,tuple,set,dict)) else {j} for j in (fld1CompIndex, fld2CompIndex)]
            return compVals[0].issubset(compVals[1]) or compVals[1].issubset(compVals[0])
    return False



def getTableInfo(tbl=None, *, db_table_name=None):              # DEPRECATED.
    """ Returns column info for tbl in dictionary form
        @param tbl: table name
        @param db_table_name: if passed, ignores tbl and pulls data for db_table_name.
        @return: {'name': 'fldName', 'type': 'fldType', 'notnull': 0/1, 'dflt_value': defValue, 'pk': 0/1}
        @return: get_fld_names=True -> {fldName: dbFldName, }
        """
    # cols = ['cid', 'name', 'type', 'notnull', 'dflt value', 'pk']. cid is Index of the field in that table.
    if db_table_name:
        dbTblName = db_table_name
    elif tbl:
        dbTblName = getTblName(tbl)
    else:
        return {}
    if dbTblName.startswith(strError):
        return f'ERR_INP_Invalid Argument: {tbl}'

    cur = queryObj.execute(f' PRAGMA TABLE_INFO("{dbTblName}")')
    if isinstance(cur, str):
        retValue = f'ERR_SYS_DBAccess. Cannot read from table {dbTblName}. Error: {cur}.'
        db_logger.error(retValue)
        return retValue

    cols = [j[0].lower() for j in cur.description]
    rows = cur.fetchall()
    name_idx = cols.index('name')
    retValue = {rows[j][name_idx]: {cols[i]: rows[j][i] for i in range(1, len(cols))} for j in range(len(rows))} \
                if rows else {}
    return retValue
