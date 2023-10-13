from krnl_config import *
from krnl_db_access import SqliteQueueDatabase
from krnl_sqlite import *           # getTblName, getFldName estan aqui!!


TIMEOUT_READ = 2
TIMEOUT_WRT = 2    # En segundos. Tiempo que el caller espera a que se complete la operacion antes de abortar.
writeObj = SqliteQueueDatabase(MAIN_DB_NAME)  # Asynchronous write object for setRecord() function. This is a singleton.

def moduleName():
    return str(os.path.basename(__file__))


def kwargsParseNames(tblName, leMode=0, **kwargs):
    """
    Generates and returns a Dictionary of dictionaries, one dictionary for each tableName passed. Form:
    losDicts[tblName1] = {fName:fldValue,}, losDicts[tblName2]= {fName:fldValue,}, ...
    Intended for use for passing and returning parameters in multiple tables
    @param leMode: 0: Pass all. Checks Table names but not fldNames names. All field names stripped and returned.
                 1: Only DB Fields. Filters only fldNames that are valid DB Field Names.
    @param tblName: a table name.
    @param kwargs: if tblWrite is not provided or not valid: Each dictionary is
                        {tableName : {fieldName1:fieldValue1, fieldName2:fieldValue2,...},} for the corresponding table.
        If tblWrite is provided **kwargs is of the form fieldName1=fieldValue1, fieldName2=fieldValue2,...
    Non-valid names are ignored. If key hdrFields are repeated in a dictionary, the last instance is used.
    If no valid names are found, returns and val dictionary.

    @return: losDicts{} : Dictionary of dictionaries with tblNames as keys and values are dictionaries {fName:fldValue}
    """
    retDict = {}
    tblName = str(tblName).strip()
    if strError not in getTblName(tblName):  # Si no hay error (Nombre de tabla es valido)
        if leMode == 0:  # mode=0 -> Pasa todos los campos. Necesario para parsear campos generados por queries
            return kwargs                                   # {i: kwargs[i] for i in kwargs}
        _ = getFldName(tblName, '*', 1)
        commonKeys = set(kwargs).intersection(_)            # set(kwargs) arma un set con todos los keys de kwargs.
        retDict = {k: kwargs[k] for k in commonKeys}        # retorna solo campos presentes en tabla tblName
    return retDict

def strSQLConditions1Table(tblName: str, **kwargs):  # 1 table only. Any extra table is ignored
    """
    Parses multiple values in lists and returns result packed in IN clause. Ex. recordID = [4,5,6]-> recordID IN (4,5,6)
    @param tblName: Table for the hdrFields in kwargs. Single table.
    @param kwargs: {fldName1:fldValue1, fldName2:fldValue2,}
    @return: {"Field_Name":fldValue,}. Field Name is a proper DB field name, already in quotes ("ID_Caravana", etc)
    """
    tblName = tblName.strip()
    kwargsDict = kwargsParseNames(tblName, 1, **kwargs)  # Formatea kwargs para pasar a kwargsParseNames().
    if not kwargsDict:
        return {}  # Sale con diccionario vacio si hay error en parametros.
    fldNamesDict = getFldName(tblName, '*', 1)
    commonKeys = set(fldNamesDict).intersection(kwargsDict)  # Genera set con interseccion de los keys de ambos dicts.
    if not commonKeys:
        return {}

    for i in commonKeys:  # Identifica valores multiples y formatea para clausula "IN"
        if isinstance(kwargsDict[i], (list, tuple, set)):
            if isinstance(kwargsDict[i], set):
                kwargsDict[i] = list(kwargsDict[i])
            # kwargsDict[i] = kwargsDict[i][0] if len(kwargsDict[i]) == 1 else str(tuple(kwargsDict[i]))
            temp_list = tuple(['"'+j+'"' if isinstance(j, str) else j for j in kwargsDict[i]])
            kwargsDict[i] = temp_list[0] if len(temp_list) == 1 else str(temp_list)
        elif isinstance(kwargsDict[i], str):
            kwargsDict[i] = f'"{kwargsDict[i]}"'    # Pone los strings entre comillas porque pueden tener blank spaces
    fieldsAndValues = {f'"{fldNamesDict[i]}"': kwargsDict[i] for i in commonKeys}
    # print(f'fieldAndValues: {fieldsAndValues}', dismiss_print=DISMISS_PRINT)
    return fieldsAndValues


def strSQLSelect1Table(tblName: str, groupOp, groupFldName=None, useFrom=1, *args):  # Funcion para parametrizar
    """string SELECT de 1 SOLA TABLA            # TODO(cmt): Se usa en getRecords()!!
    Use: strSQLSelect1Table(tblName, 'MAX','Fecha Evento','Data Animales Actividad Inventario','ID_Data Inventario',
    'ID_Actividad','Fecha Evento')  Esta funcion NO es Mandrake: groupOp (group Operation), groupFldName, tblWrite,
    fieldNames DEBEN ser validos.
    """
    # args = argsStrip(*args)
    tblName = tblName.strip()
    groupOp = str(groupOp).strip() if groupOp else None
    groupFldName = str(groupFldName).strip() if groupFldName else None
    tableName = f'"{getTblName(tblName)}"'
    if strError in tableName:  # Table Not Found: sale.
        db_logger.info(tableName)
        print(f'strSQLSelect: {tableName}', dismiss_print=DISMISS_PRINT)
        return tableName
    if not args and groupFldName is None:
        return f'ERR_INP Invalid Parameters {args}'

    dbFieldNames = []
    strSelect = ' SELECT '
    if not args or '*' in args:
        strSelect += '* '
    else:
        fldNamesDict = getFldName(tblName, '*', 1)    # Diccionario {fldName: dbFldName, }
        dbFieldNames = [fldNamesDict[j] for j in args if j in fldNamesDict]
    if groupOp and groupFldName:
        fldName = getFldName(tblName, groupFldName)
        if strError not in fldName:
            strSelect += f'{groupOp}("{fldName}")' + ', ' if dbFieldNames else ' '

    if dbFieldNames:
        strSelect += str(dbFieldNames)[1:-1].replace("'", '"') + ' '

    strSelect += f' FROM {tableName}' if useFrom else ''
    # print(strSelect)
    return strSelect


