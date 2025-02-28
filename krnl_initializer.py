from krnl_config import MAIN_DB_NAME, NR_DB_NAME, krnl_logger, set_bit, clear_bit, test_bit
from krnl_custom_types import dbRead, DataTable, setRecord
import importlib
import inspect

MODULES_DICT = {}           # Dict of all modules loaded by module_loader(): {module_name: module_obj, }
ATTRIBUTES_DICT = {}        # General variable to store attributes from all runs of module_loader().

def tbl_writer(tbl: DataTable = None):     # quick testing of database writes, specifying the database.
    if isinstance(tbl, DataTable):
        print(f'Now test-writing to {tbl.dbName}.{tbl.tblName} using setRecords() and setRecord()...')
        for j in range(tbl.dataLen):
            tbl.setVal(j, fldComment=f'Comentario #{j+3}.')
        tbl.setRecords()
        setRecord(tbl.tblName, db_name=NR_DB_NAME, fldID=1, fldComment='Hola, I am setRecord(). Previous comment is: '
                                                                   + tbl.getVal(0, 'fldComment', fldID=1))
    return tbl

def module_loader():
    """ Loads modules defined in table _sys_Modules (_nr database) and creates a dictionary with all attributes defined
    in those modules, by scanning all rows in the table and importing/reloading records with import or reload bits set.
    Each time a module is loaded MODULES_LIST, ATTRIBUTES_DICT are updated with the new modules and the attribute
    objects defined therein.
    If the RELOAD bit is set in Bitmask field, reload is forced on the module.
    @return: Attributes dict {attr_name: attr_object, }; ERR_ string on database access error.
    """
    import_bit = 0      # bit0 from Bitmask: import module (if not in the system) or use module loaded in [sys.modules].
    reload_bit = 1      # bit1 from Bitmask: force module reload.
    temp = dbRead('tbl_sys_Modules', 'SELECT * FROM _sys_Modules; ', db_name=NR_DB_NAME)
    if isinstance(temp, str):
        return f'ERR_DBAccess: cannot read from table tbl_sys_Modules. Error: {temp}.'
    if temp:        # goes to work only if temp table holds 1 or more records (temp._dataList is not empty).
        cols = temp.getCols('fldModule_Name', 'fldAttributes', 'fldBitmask')
        mod_attr_dict = dict(zip(cols[0], cols[1]))     # mod_attr_dict = {module_name: [attr_name1, attr_name2, ], }
        bitmask_dict = dict(zip(cols[0], cols[2]))      # bitmask_dict = {module_name: bitmask(int), }
        module = None                                   # marker to signal that a module has been loaded.
        for idx, k in enumerate(mod_attr_dict):         # Must enumerate to access fldID to reset reload bit in Bitmask.
            if test_bit(bitmask_dict[k], reload_bit):
                try:        # 1st tests for reload bit.
                    if inspect.ismodule(MODULES_DICT.get(k)):
                        module = importlib.reload(MODULES_DICT.get(k))
                        krnl_logger.info(f'Module {k} reloaded.')
                    else:
                        module = importlib.import_module(k)     # If module was not loaded beforehand, imports it.
                except (ModuleNotFoundError, ImportError):
                    continue  # On failure, skips to the next module name (keys in mod_attr_dict).
            elif test_bit(bitmask_dict[k], import_bit):
                try:        # 2nd tests for import bit.
                    module = importlib.import_module(k)
                except (ModuleNotFoundError, ImportError):
                    continue  # On failure, skips to the next module name (keys in mod_attr_dict).
            else:
                continue    # If neither bit is set, moves on to the next module listed in mod_attr_dict.

            if module is not None:
                MODULES_DICT.update({k: module})   # appends the module object to the master dict for imported modules.
                # j is each attribute_name defined in the JSON Attribute field in _sys_Modules, in _nr database.
                ATTRIBUTES_DICT.update({j: getattr(module, j) for j in mod_attr_dict[k]})  # updates master attribs dict
                # Resets import and reload bits after import is complete. Saves in _sys_Modules table.
                setRecord(temp.tblName, fldID=temp.getVal(idx, 'fldID'),
                          fldBitmask=bitmask_dict[k] & ~(1 << reload_bit | 1 << import_bit), db_name=temp.dbName)
        print(f'Imported MODULES: {MODULES_DICT}')

    return ATTRIBUTES_DICT

    # tbl_writer(temp)      # tests some write operations in non-replicable database (_nr database).










