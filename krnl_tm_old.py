
from krnl_config import time_mt, singleton, sessionActiveUser, callerFunction, lineNum, valiDate, os, activityEnableFull
from krnl_transactionalObject import TransactionalObject
from krnl_custom_types import DataTable, setupArgs, getRecords, setRecord, delRecord
from krnl_abstract_class_activity import Activity
from krnl_parsing_functions import kwargsParseNames
systemDefaultCurrency = 1       # 1=ARS
systemReferenceCurrency = 2     # 2=USD


def moduleName():
    return str(os.path.basename(__file__))


@singleton
class TM(TransactionalObject, Activity):
    __objClass = 101
    __objType = 2

    temp = getRecords('tblTMActividadesNombres', '', '', None, 'fldID', 'fldName')
    __activitiesDict = {}
    for j in range(temp.dataLen):
        __activitiesDict[temp.dataList[j][1]] = str(temp.dataList[j][0])  #  {NombreActividad: ID_Actividad,}

    @property
    def activities(self):                       # NO RENOMBARAR - Usada en getRecordLinkTables()
        return self.__activitiesDict

    __classExcludedFieldsClose = {"fldProgrammedDate", "fldWindowLowerLimit", "fldWindowUpperLimit", "fldFK_Secuencia",
                                  "fldDaysToAlert", '"fldDaysToExpire"'}
    __classExcludedFieldsCreate = {"fldPADataCreacion"}

    __activityExcludedFieldsClose = {}  # {activityID: (excluded_fields, ) }
    __activityExcludedFieldsCreate = {}  # {activityID: (excluded_fields, ) }

    @classmethod  # TODO(cmt): Main two methods to access excluded_fields
    def getActivityExcludedFieldsClose(cls, activityName=None):
        return cls.__activityExcludedFieldsClose.get(activityName, set())

    @classmethod
    def getActivityExcludedFieldsCreate(cls, activityName=None):
        return cls.__activityExcludedFieldsCreate.get(activityName, set())

    __tblRAName = 'tblTMRegistroDeActividades'
    __tblMontosName = 'tblDataTMMontos'
    __tblDataName = 'tblDataTMTransacciones'
    __tblObjectsName = __tblRAName
    __tblRA = DataTable(__tblRAName)
    __tblMontos = DataTable(__tblMontosName)
    __tblData = DataTable(__tblDataName)

    @classmethod
    def getObjectTblName(cls):
        return cls.__tblObjectsName

    def __init__(self, enableActivity=activityEnableFull, *args, **kwargs):
        TransactionalObject.__init__(self)
        if not self.__tblRA.isValid or not self.__tblMontos.isValid or not self.__tblData.isValid:
            isValid = False
            print(f'ERR_Sys_CannotCreateObject: MoneyActivity:activities()')
            return
        else:
            isValid = True
            self.__activityName = 'MoneyActivity General Activity'  # __activityName, __activityID, invActivity->NO checks.
        self.__activityName = 'MoneyActivity General Activity'
        self.__activityID = 201
        isInventoryActivity = False
        super().__init__()
        Activity.__init__(self, isValid, self.__activityName, self.__activityID, isInventoryActivity, enableActivity,
                          self.__tblRAName, *args, **kwargs)

    def createAmount(self, *args: DataTable, **kwargs):
        """
        @param args: DataTable tables with obj_data to write. Enables writing of full tables (all fields). Write Tables:
        tblTMRegistroDeActividades, tblDataTMMontos. Any other tables are ignored.
        @param kwargs: {fName=fldValue, }-> Field Names in table [Data MoneyActivity Montos] ONLY. No other tables
        supported. kwargs provide a short version of params to operate the method without passing full parameters.
        MANDATORY: fldQuantity, fldUnitAmount.
        If fldUnitAmount is not passed, returns ERR_Sys_InvalidParameter
        @return: [MoneyActivity Registro De Actividades].ID_Actividad (int) or errorCode (string)
        """
        tblRA = setupArgs(self._tblRAName, *args)                      # arma tblRA con argumentos en dataList[0]
        tblMontos = setupArgs(self.__tblMontosName, *args, **kwargs)    # arma tblMontos con argumentos en dataList[0]

        # Seteo de Valores de tabla Data Montos
        qty = tblMontos.getVal(0, 'fldQuantity')             # qty=None si fldQuantity no esta entre los argumentos
        unitAmt = tblMontos.getVal(0, 'fldUnitAmount')       # unitAmt=None si fldUnitAmount no esta entre los argumentos
        if qty is None or unitAmt is None:
            retValue = f'ERR_UI_InvalidArgument: Quantity,Unit Amount - MoneyActivity.PY createAmount({lineNum()})'
            print(f'{retValue} / {callerFunction()}')
            return retValue     # Faltan Unit Amount o Quantity: Sale con error

        totalAmt = float(tblMontos.getVal(0, 'fldUnitAmount') * tblMontos.getVal(0, 'fldQuantity'))
        tblMontos.setVal(0, 'fldAmountTotal', totalAmt)
        timeStamp = time_mt('datetime')
        userID = sessionActiveUser
        eventDate = valiDate(tblMontos.getVal(0, 'fldDate'), timeStamp)
        tblMontos.setVal(0, 'fldDate', eventDate)
        currency = int(tblMontos.getVal(0, 'fldFK_Moneda')) if tblMontos.getVal(0, 'fldFK_Moneda') is not None \
                                                            else systemDefaultCurrency
        tblMontos.setVal(0, 'fldFK_Moneda', currency)

        # Seteo de valores de TMRegistroDeActividades (SOLO SI SE DEBE insertar record en MoneyActivity Registro De Actividades)
        if tblRA.getVal(0, 'fldID') is not None:
            idActividad = int(tblRA.getVal(0, 'fldID'))     # Hace idActividad=fldID si existe
        else:
            tblRA.setVal(0, 'fldFK', self.activities['Asignacion De Monto'])        # Si no, es Asignacion de Monto
            tblRA.setVal(0, 'fldTimeStamp', timeStamp)  # El Sistema SIEMPRE sobreescribe fldTimeStamp y fldFK_UserID
            tblRA.setVal(0, 'fldFK_UserID', userID)
            idActividad = setRecord(tblRA.tblName, **tblRA.unpackItem(0))  # No se paso idActividad -> Insertar registro en RA
        if type(idActividad) is str:
            retValue = f'ERR_DB_WriteError - MoneyActivity.PY Function/Method: createAmount({lineNum()})'
            print(f'{retValue} / {callerFunction()}')
            return retValue                     # Sale con Error si no pudo escribir DB

        else:
            # Completa valores de tblMontos y escribe.
            tblRA.setUndoOnError(True)
            tblMontos.setVal(0, 'fldFK_Actividad', idActividad)
            idActividadMonto = setRecord(tblMontos.tblName, **tblMontos.unpackItem(0))
            if type(idActividadMonto) is not int:   # idActividadMonto tiene ErrorMsg si no es int
                if tblRA.getVal(0, 'fldID') is not None:     # Delete record  de [MoneyActivity RA] solo si se escribio antes
                    _ = delRecord(tblRA.tblName, idActividad)
                    # queryObj = QSqlQuery(self.db)
                    # strDelete = strSQLDeleteRecord(tblRA.tblName, idActividad)
                    # queryObj.exec_(strDelete)
                retValue = idActividadMonto + f'Function/Method: createAmount({lineNum()})'
                print(retValue)
            else:
                retValue = idActividad
        return retValue

    def createTransaction(self, *args: DataTable, **kwargs):
        """
        Inserts obj_data in tables MoneyActivity Registro De Actividades, Data MoneyActivity Transacciones Monetarias.
        @param args: Data Tables MoneyActivity Registro De Actividades, Data MoneyActivity Transacciones Monetarias.
        @param kwargs: {fName=fldValue}, fName -> Field Names in table [Data MoneyActivity Transacciones Monetarias].
        Mandatory arguments (in *args or **kwargs): fldAmount, fldFKOwnershipTransferType, fldFK(Activity Name)
        @return:[MoneyActivity Registro De Actividades].ID_Actividad (int) or errorCode (string)
        """
        tblRA = setupArgs(self._tblRAName, *args)  # Arma tblRA con argumentos en dataList[0]
        tblTransact = setupArgs(self.__tblDataName, *args, **kwargs)  #  tblTransact con argumentos en dataList[0]
        if type(tblRA) is str or type(tblTransact) is str:
            retValue = f'ERR_UI_InvalidArgument: Invalid table Names. MoneyActivity.PY creatTransaction({lineNum()})'
            print(f'{retValue} / {callerFunction()}')
            return retValue

        # Seteo de Valores MANDATORIOS de tabla Data Transacciones
        if tblRA.getVal(0, 'fldFK_NombreActividad') is not None:
            tblTransact.setVal(0, 'fldFK_NombreActividad', tblRA.getVal(0, 'fldFK_NombreActividad'))
        elif tblTransact.getVal(0, 'fldFK_NombreActividad') is not None:
            tblRA.setVal(0, 'fldFK_NombreActividad', tblTransact.getVal(0, 'fldFK'))
        else:
            tblRA.setVal(0, 'fldFK_NombreActividad', self.activities['Transaccion Monetaria'])
            tblTransact.setVal(0, 'fldFK_NombreActividad', self.activities['Transaccion Monetaria'])

        if tblTransact.getVal(0, 'fldFK_OwnershipTransferType') is None:
            retValue = f'ERR_Sys_ParameterInvalid: Tipo Transferencia De Propiedad - MoneyActivity.PY: creatAmount({lineNum()})'
            print(f'{retValue} - {callerFunction()}')
            return retValue  # Sale con error si no se indica el Tipo de Transferencia de Propiedad

        timeStamp = time_mt('datetime')
        userID = sessionActiveUser
        eventDate = valiDate(tblTransact.getVal(0, 'fldDate'), timeStamp)
        tblTransact.setVal(0, fldDate=eventDate)
        if tblTransact.getVal(0, 'fldFK_Moneda') is None:
            tblTransact.setVal(0, fldFK_Moneda=systemDefaultCurrency)

        idActividad = tblRA.getVal(0, 'fldID')
        if idActividad is None:
            tblRA.setVal(0, 'fldTimeStamp', timeStamp)      # Insertar registro en TMRegistroDeActividades
            tblRA.setVal(0, 'fldFK_UserID', userID)
            idActividad = setRecord(tblRA.tblName, **tblRA.unpackItem(0))
            if type(idActividad) is str:
                retValue = f'ERR_DB_WriteError - MoneyActivity.PY Function/Method: creatAmount({lineNum()})'
                print(f'{retValue} - {callerFunction()}')
                return retValue

        # Completa valores de Tabla MoneyActivity Transancciones Monetarias y escribe.
        tblRA.setUndoOnError(True)
        tblTransact.setVal(0, fldFK_Actividad=idActividad)
        idActividadTransact = setRecord(tblTransact.tblName, **tblTransact.unpackItem(0))
        if type(idActividadTransact) is not int:  # idActividadMonto tiene ErrorMsg si no es int
            if tblRA.getVal(0, 'fldID') is None:  # Delete record  de [MoneyActivity RA] solo si se inserto arriba. Si no, lo deja
                _ = delRecord(tblRA.tblName, idActividad)
                # queryObj = QSqlQuery(self.db)
                # strDelete = strSQLDeleteRecord(tblRA.tblName, idActividad)
                # queryObj.exec_(strDelete)
            retValue = idActividadTransact + f'Function/Method: createAmount({lineNum()}) '
            print(retValue)
        else:
            retValue = idActividad

        return retValue

    def getTransactions(self, sDate: str, eDate: str, **kwargsConditions):
        """
       Returns DataTable with records from [Data MoneyActivity Transacciones]
       @param kwargs: condition fields for which Transactions are pulled
       @param sDate: start Date. Format YYYY-MM-DD
       @param eDate: end Date. Format YYYY-MM-DD
       @return: DataTable Object or errorCode (string)
       """
        sDate = str(sDate).strip()
        eDate = str(eDate).strip()
        kwargs = kwargsParseNames(self.__tblDataName, 1, **kwargsConditions)
        result = getRecords(self.__tblDataName, sDate, eDate, None, None, **kwargs)
        return result

    def getTransactionTotals(self, idTransaction: int):
        """
        Returns tuple: Total Amount for idTransaction (addition of all Montos with that idTransaction) and ID_Moneda
        @param idTransaction: TransactionActivity in [Data MoneyActivity Transacciones] for which total amount is pulled.
        @return: [TotalAmount (float), Currency (int)] / errorCode (str) if invalid idTransacion is passed
        """
        if not isinstance(idTransaction, int) or idTransaction <= 0:
            retValue = f'ERR_UI_InvalidArgument: ID_Transaccion Monetaria - MoneyActivity.PY getTransactionTotals({lineNum()})'
            print(f'{retValue}')
            return retValue

        retTable = getRecords(self.__tblMontosName, '', '', None, 'fldID', 'fldAmountTotal', 'fldFK_Moneda',
                              fldFK_Transaccion=idTransaction)
        currency = retTable.getVal(0, 'fldFK_Moneda')   # currency es unico para todos los montos de una Transasccion
        currency = int(currency) if currency is not None else systemDefaultCurrency
        amount = 0
        if retTable.dataLen:
            for i in range(retTable.dataLen):
                try:
                    amount += retTable.getVal(i, 'fldAmountTotal') if retTable.getVal(i, 'fldAmountTotal') is not None else 0
                except (TypeError, ValueError, IndexError, NameError):
                    pass            # Ignora registros/campos con datos no validos
        amount = float(amount)
        retValue = [amount, currency]
        return retValue

    def getTransactionMontos(self, idTransaction: int):
        """
        Returns tuple: List of Montos Records (all fields in each record) belonging to 1 TransactionActivity (idTransascion)
        @param idTransaction: TransactionActivity in [Data MoneyActivity Transacciones] for obj_data is pulled.
        @return: [[MontosRecord1], [MontosRecord2], ] / errorCode (str) if invalid idTransacion is passed
        """
        if not isinstance(idTransaction, int) or idTransaction <= 0:
            retValue = f'ERR_UI_InvalidArgument: ID_Transaccion Monetaria - MoneyActivity.PY getTransactionTotals({lineNum()})'
            print(f'{retValue}')
            return retValue
        retTable = getRecords(self.__tblMontosName, '', '', None, '*', fldFK_TransaccionMonetaria=idTransaction)
        retArray = []
        if retTable.dataLen:
            for i in range(retTable.dataLen):
                retArray.append(retTable.dataList[i])
            retValue = retArray
        else:
            retValue = [retArray, ]
        return retValue


handlerTM = TM()

# TODO: Logica para Cuentas bancarias: ALIAS puede no tener puntos. identificar CBU como un string de 22 digitos.
#  Si no es string de 22 digitos decimales, considerar ALIAS.
# accountUID = str(uid).strip()
# if accountUID.find("."):
#     fldAlias = accountUID        # Si contiene al menos 1 punto, es Alias. Lo almacena en campo Alias
# else:
#     fldUID = accountUID         # Si no, es CBU. Almacena el dato en fldUID