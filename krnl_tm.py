from krnl_config import time_mt, singleton, sessionActiveUser, callerFunction, lineNum, valiDate, os, krnl_logger, \
    activityEnableFull, print, DISMISS_PRINT, datetime_from_kwargs
from krnl_entityObject import EntityObject
from krnl_custom_types import DataTable, setupArgs, getRecords, setRecord, delRecord, Amount, Transaction,defaultCurrencyName
from krnl_exceptions import DBAccessError
from krnl_abstract_class_activity import Activity
from krnl_person import Person
from krnl_exch_rates import RealTimeExchangeRate
# from money.exceptions import CurrencyMismatch, ExchangeRateNotFound, InvalidOperandType


def moduleName():
    return str(os.path.basename(__file__))

#       TODO: Implement getDefaultParam(param) to obtain default parameters.

class MoneyActivity(Activity):          # Abstract class

    _activity_class_register = set()  # Used to create instance objects for each MoneyActivity subclass.
    _objClass = Amount

    def __call__(self, caller_object=None, *args, **kwargs):
        """
        @param caller_object: instance of Bovine, Caprine, etc., that invokes the Activity
        @param args:
        @param kwargs:
        @return: Activity object invoking __call__()
        """
        # item_obj=None above is important to allow to call fget() like that, without having to pass parameters.
        # print(f'>>>>>>>>>>>>>>>>>>>> {self} params - args: {(item_object, *args)}; kwargs: {kwargs}')
        self.outerObject = caller_object  # item_object es instance de Animal, Tag, etc. NO PUEDE SER class por ahora.
        return self


    temp = getRecords('tblTMActividadesNombres', '', '', None, 'fldID', 'fldName', 'fldNivelRequerido', 'fldFlag',
                      'fldFlagTransactionActivity')
    __activitiesDict = {}
    __accessLevelDict = {}  # dict de accessLevel TIENE que estar aqui (MoneyActivity, AnimalActivity, TagActivity,etc)
    __ledgerActivityDict = {}
    __transactionActivityDict = {}
    for j in range(temp.dataLen):
        __activitiesDict[temp.dataList[j][1]] = temp.dataList[j][0]      # {activityName: ID_Actividad,}
        __accessLevelDict[temp.dataList[j][1]] = temp.dataList[j][2] or 0
        __ledgerActivityDict[temp.dataList[j][1]] = temp.dataList[j][3] or 0  # {activityName: isLedgerFlag,}
        __transactionActivityDict[temp.dataList[j][1]] = temp.dataList[j][4] or 0  # {activityName: fldFlagTransactionActivity,}

    __tblRAName = 'tblTMRegistroDeActividades'
    __tblMontosName = 'tblDataTMMontos'
    __tblCurrencyName = 'tblMonedasNombres'
    __tblLinkName = 'tblLinkMontosActividades'
    __tblTransactionsName = 'tblDataTMTransacciones'
    __tblObjectsName = __tblRAName
    __tblRA = DataTable(__tblRAName)
    __tblMontos = DataTable(__tblMontosName)
    __tblCurrencies = DataTable(__tblCurrencyName)

    _moneyPhases = {}                # {Phase Name: PhaseID, }, where Phases are Unbilled, Billed/Invoiced, paid, etc.
    temp = getRecords('tblTMFases', '', '', None, 'fldID', 'fldName')
    if isinstance(temp, DataTable):
        _moneyPhases = {j[1]: j[0] for j in temp.dataList}      # {Phase Name: PhaseID, }

    _moneyInstruments = {}
    temp = getRecords('tblTMInstrumentosMonetarios', '', '', None, 'fldID', 'fldName')
    if isinstance(temp, DataTable):
        _moneyInstruments = {j[1]: j[0] for j in temp.dataList}  # {Phase Name: PhaseID, }

    @property
    def tblRAName(self):
        return self.__tblRAName

    @property
    def tblMontosName(self):
        return self.__tblMontosName

    @classmethod
    def getObjectTblName(cls):
        return cls.__tblObjectsName


    @property
    def activities(self):                       # TODO: NO RENOMBARAR! - Usada en getRecordLinkTables()
        return self.__activitiesDict            #  {activityName: activityID,}

    @classmethod
    def ledgerActivityDict(cls):
        return cls.__ledgerActivityDict    # pulls data from the right object using self.

    @property
    def isLedgerActivity(self):
        return bool(self.__ledgerActivityDict.get(self._activityName, None))

    @property
    def isTransactionActivity(self):
        return bool(self.__transactionActivityDict.get(self._activityName, None))

    @classmethod
    def setDefaultCurrency(cls, cur_name):
        """
        Sets the default currency for the system. Updates parameters y Sys tables.
        @param cur_name: Currency code (str) ('USD', 'ARS', 'EUR', etc).
        @return:
        """
        pass

    @classmethod
    def setBaseCurrency(cls, cur_name):
        """
        Sets the base currency for the system. Updates parameters y Sys tables.
        Base currency is the "reference" currency used to convert other currencies to.
        @param cur_name: Currency code (str) ('USD', 'ARS', 'EUR', etc).
        @return:
        """
    pass

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


    def __init__(self, activityName, *args, enableActivity=activityEnableFull, tbl_data_name=None, **kwargs):
        if not self.__tblRA.isValid or not self.__tblMontos.isValid:
            krnl_logger.error(f'ERR_Sys_Cannot create object: MoneyActivity.')
            raise TypeError(f'ERR_Sys_Cannot create object: MoneyActivity.')
        else:
            isValid = True
        self.__user = next((kwargs[k] for k in kwargs if 'user' in k.lower()), sessionActiveUser)
        self.__requiredLevel = self.__accessLevelDict.get(activityName, 100)  # Si no encuentra activityName bloquea la ejecucion
        activityID = self.activities.get(activityName, None)  # None: __activityName is not defined in db.
        isInventoryActivity = False
        super().__init__(isValid, activityName, activityID, isInventoryActivity, enableActivity, self.__tblRAName,
                         *args, tblDataName=tbl_data_name, tblLinkName=self.__tblLinkName, **kwargs)

    @property
    def user(self):
        return self.__user

    def setUser(self, val):
        if isinstance(val, Person):
            self.__user = val

    @property
    def requiredLevel(self):
        return self.__requiredLevel

    @property
    def tblDataName(self):
        return self._tblDataName        # tblDataName from subclasses.


    def createAmountActivity(self, *args: DataTable, amount=None, currency=None, id_transaction=None, **kwargs):
        """  Writes data to db for activities that carry an Amount value (Transaction, Price, etc).
        Inserts data in tables TM Registro De Actividades, Data TM Transacciones Monetarias.
        tblData for now is: tblDataTMTransacciones, tblDataTMPrecios
        @param id_transaction:  fldID for the Transaccion assigned to the Amount(s) created here.
        @param amount: int, float or str or Amount. amount value to create Amount obj, or Amount object directly.
        @param currency: str. Currency code to create Amount obj. Ignored if amount is type Amount.
        @param args: Data Tables TM Registro De Actividades, Data TM Transacciones Monetarias, Link Montos TM, Montos.
            If multiple records are passed in Montos, then multiple Amounts will be created for the transaction.
            Not very common, but the feature is implemented in any case.
        @param kwargs: {fName=fldValue}, fName -> Field Names in table [Data TM Transacciones Monetarias].
        Mandatory arguments (in *args or **kwargs): fldAmount, fldFKOwnershipTransferType, fldFK(Activity Name)
        @return:[TM Registro De Actividades].ID_Actividad (int) or errorCode (string)
        """
        # if self.__user.accessLevel < self.requiredLevel:
        #     return None         # sale si el usuario no tiene privilegios suficientes.
        tblRA = setupArgs(self._tblRAName, *args)  # Arma tblRA con argumentos en dataList[0]
        tblData = setupArgs(self.tblDataName, *args, **kwargs)  # tblData: tblDataTMTransacciones or tblDataTMPrecios.
        if any(isinstance(j, str) for j in (tblRA, tblData)):
            retValue = f'ERR_INP_InvalidArgument: Invalid table Names. {moduleName()}({lineNum()})'
            krnl_logger.error(f'{retValue} / {callerFunction()}')
            return retValue

        val = None
        # Sets up tblMontos based either on tbl passed in args, or creating Amount from argument in kwargs.
        # Here, multiple amounts may be created for 1 Transaction. Those multiple Amounts must be passed in tblMontos.
        tblMontos = next((t for t in args if isinstance(t, DataTable) and t.tblName == self.__tblMontosName), None)
        if tblMontos and tblMontos.dataLen:
            for j in range(tblMontos.dataLen):      # In case there's more than 1 Amount in this Transaction.
                tblMontos.setVal(j, fldFK_Transaccion=tblMontos.getVal(0, 'fldFK_Transaccion') or id_transaction)
        else:
            if isinstance(amount, Amount):
                amt = amount                # NO tblMontos, and only 1 Amount passed.
                if id_transaction:
                    amt.setTransaction(id_transaction)
            else:
                # Creates Amount obj from kwargs and tblMontos table when they haven't been passed in args.
                if not isinstance(amount, (int, float, str)) or not isinstance(currency, str):
                    val = f'ERR_INP_Invalid Arguments. Amount value and/or currency are not valid.'
                else:
                    try:
                        amt = Amount.create_obj(amount, currency)     # Creates Amount obj and stores it in database.
                    except(ValueError, TypeError, SyntaxError):
                        val = f'ERR_INP_Invalid Arguments. Amount value and/or currency are not valid.'
                    else:
                        amt.setTransaction(id_transaction)
        if isinstance(val, str):
            krnl_logger.error(val)
            return val          # type(val)==str -> Some error in parameters passed. Function exits.

        tblRA.setVal(0, fldFK_NombreActividad=self._activityID, fldFK_UserID=sessionActiveUser)
        if self.isLedgerActivity:
            tblRA.setVal(0, fldFlag=1)
        # Seteo de Valores MANDATORIOS en tblRA, tblLink, tblData.
        tblLink = next((j for j in args if isinstance(j, DataTable) and j.tblName == self.__tblLinkName),
                       DataTable(self.__tblLinkName))
        tblLink.setVal(0, fldFK=amt.ID)    # Can also use amt alone, as Amount converter is working ok.

        idActividadRA = self._createActivityRA(tblRA, tblLink, tblData, tbl_data_name=tblData.tblName)
        if isinstance(idActividadRA, str):
            retVal = f'ERR_DBAccess: Cannot write to tables {tblRA.tblName}, {tblLink.tblName} or {tblData.tblName}.'
            krnl_logger.error(retVal)
        else:
            if tblMontos:
                _ = tblMontos.setRecords()  # Creates all the Amounts db records as passed in tblMontos (1 or more...)
            else:
                _ = setRecord(self.tblMontosName, fldAmount=amt.amount, fldCurrency=amt.currency,
                                    fldFK_Transaccion=amt.transaction)
                # amt.recordID = idMonto        # This is not really needed
            retVal = idActividadRA

        return retVal


    @classmethod
    def getAmountsFromActivity(cls, id_activity=None):
        """ Gets all the Amounts associated to id_Activity. Returns list of Amount objects. [] if nothing is found.
        Used for all Activities other than Transaction: Price plus any other that uses Amount. """
        temp = getRecords(cls.__tblLinkName, '', '', None, '*', fldFK_Actividad=id_activity)
        if not isinstance(temp, DataTable) or not temp.dataLen:
            return []          # Nothing found, or error reading table.
        return [Amount.fetch_obj(temp.getVal(j, 'fldFK')) for j in temp.dataList]


# ----------------------------------------------- END CLASS MoneyActivity -------------------------------------------  #


class TransactionActivity(MoneyActivity):         # Abstract class. TODO(cmt): Most subclasses SHOULD BE ledgerActivity.
    __tblDataName = 'tblDataTMTransacciones'
    _objClass = Transaction

    def __init__(self, activityName, *args, **kwargs):
        super().__init__(activityName, *args, activity_enable=activityEnableFull, tbl_data_name=self.__tblDataName,
                         **kwargs)

    # def billing(self):          # bill credits or be-billed for debits.
    #     print('billing')
    # invoice = billing          # identical to billing.
    # def unbilled(self):
    #     print('unbilled')
    #
    # def billed_in(self):        # Bill received for an outstanding debt.
    #     print('billed_in')
    #
    # def collection(self):       # For IncomeActivity / credits: Use for cash, check transfers_in,
    #     print('collection')

    def accrual(self):
        print('accrual')

    def provision(self):
        print('provision')

    # def payment(self):
    #     pass
    # def expense(self):          # for OutflowActivity / debts: Use for cash, check transfers_out.
    #     pass


    @property
    def tblDataName(self):
        return self._tblDataName

    def createTransaction(self, **kwargs):
        """ Creates a transaction from kwargs passed, writes data to db and returns the Transaction object.
        @return: Transaction Object. ERR_ string if object cannot be created.
        """
        if 'fldFK_Actividad' in kwargs.keys():
            tblTransact = DataTable(self.__tblDataName, **kwargs)
            ret = tblTransact.setRecords()  # TODO(cmt): setRecords() deja fldID seteado en DataTable. Pasa a self.__ID.
            if ret == 0:
                krnl_logger.error(f'ERR_DBAccess: Cannot write to table {tblTransact.tblName}.')
                return ret
            transact_obj = self._objClass(**tblTransact.unpackItem(0))
            if transact_obj.isValid:
                return transact_obj
            else:
                retVal = 'ERR_SYS: cannot create Transaction object. Invalid object.'
        else:
            retVal = 'ERR_INP_Invalid arguments: cannot create Transaction object. Mandatory arg. ID_Actividad missing.'
        krnl_logger.error(retVal)
        return retVal


    def getTransaction(self, *args, create=False, **kwargs):
        """Pulls a transaction ID from tables passed, or creates a new transaction with data passed and returns the new
        ID.
        @return: ID_Transaction (int). ERR_ string if error.
        """
        tblTransact = next((j for j in args if isinstance(j, DataTable) and j.tblName == self.__tblDataName), None)
        if tblTransact:
            idTransact = tblTransact.getVal(0, 'fldID')
            if idTransact:
                return idTransact
            if create:
                idTransact = self.createTransaction(**tblTransact.unpackItem(0))
                if isinstance(idTransact, self._objClass):
                    return idTransact
                else:
                    return 'ERR_INP_Invalid Arguments: Cannot create object Transaction.'
            return None
        else:
            if create:
                return self.createTransaction(**kwargs)
            else:
                return None

    def fullTransact4TargetObjects(self, target_obj_dict=None, **kwargs):
        """ When a dict of target_objects is passed, creates and records to database:
            1 idActivityRA in [TM Registro De Actividades]
            n Transactions (1 for each target object in dict passed).
            m Amounts for each target_object, with m defined by a list passed as the values of target_obj_dict.
            @param target_obj_dict: Dictionary {object: (amt1, amt2, amtn), }       amt: Amount objects.
            @return: idActivityRA (int) if successful. None if fails.
        """
        if target_obj_dict and isinstance(target_obj_dict, dict):
            for k in target_obj_dict:
                if not hasattr(target_obj_dict[k], '__iter__'):
                    target_obj_dict[k] = (target_obj_dict[k],)  # if only 1 amount passed, converts to iterable
            if any(isinstance(target_obj_dict[k][j], Amount) for k in target_obj_dict
                   for j in range(len(target_obj_dict[k]))):
                pass
        else:
            return None

        # 1. Create 1 TM Activity
        eventDate = datetime_from_kwargs(time_mt('datetime'), **kwargs)
        idActivity = setRecord(self.tblRAName, fldFK_NombreActividad=self._activityID, fldFlag=self.isLedgerActivity,
                               fldTimeStamp=eventDate)
        if isinstance(idActivity, str):
            val = f'ERR_DBAccess: Cannot create record for table {self.tblRAName}. Error: {idActivity}.'
            krnl_logger.error(val)
            raise DBAccessError(val)
        # 2. Create many Transactions tied to that Activity, each of them with their respective Amounts.
        retVal = None
        kwargs['fldQuantity'] = 1  # target_objects passed in dict:  quantity must be 1.
        kwargs['fldFK_Unidad'] = 0  # No units.
        kwargs['fldFK_Actividad'] = idActivity
        kwargs['fldDescription'] = f'Sale of {str(tuple(target_obj_dict.keys()))[:-2].replace("(", "")}'
        for k in target_obj_dict:
            transaction = self.createTransaction(**kwargs)
            try:  # Will process only if target_obj_dict has tm.set() defined as attribute.
                k.tm.set(id_transaction=transaction.ID or None)
            except(AttributeError, ValueError, TypeError):
                pass                    # Ignore attempt to set target_obj TM Actividad table.
            finally:
                if isinstance(transaction, self._objClass):
                    for amt in target_obj_dict[k]:
                        if isinstance(amt, Amount):
                            amt.setTransaction(transaction)
                            amt.record()
                            retVal = True
        if retVal is None:
            delRecord(self.tblRAName, idActivity)
            return None
        return idActivity


class SaleActivity(TransactionActivity):          # Sale. Callable class.
    __activityName = 'Venta'
    __method_name = 'sale'
    _short_name = 'sale'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True  # IncomeActivity supports PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        self.perform(*args, **kwargs)

    def perform(self, *args: DataTable, phase='unbilled', target_obj_dict=None, **kwargs):
        """ Records a Sale Activity. If target_obj is passed, records the sale data in the respective TM db table.
        1:N --> 1 TM Activity : MULTIPLE TM Transactions.
        1:1 --> 1 TM Transaction : 1 target_obj
        1:N --> 1 TM Transaction : MULTIPLE Amount objects.
        @param phase: Unbilled, Billed, Income, Expense.
        @param target_obj_dict: Dict of the form {obj: (amt1, amt2, ), }. None, single or multiple objects.
                There can be 1 or more Amounts assigned to target_obj.
                If multiple objects passed -> creates multiple Transactions: 1 per object.
        @return: idActivity from created in [TM Registro De Actividades] or None of error.
        """
        kwargs['fldFK_Actividad'] = self._activityID
        phase = phase.lower() if isinstance(phase, str) else ''
        kwargs['fldFK_Phase'] = phase if phase in self._moneyPhases else 'unbilled'
        if not kwargs.get('fldDate'):
            kwargs['fldDate'] = time_mt('datetime')
        if target_obj_dict and isinstance(target_obj_dict, dict):
            return self.fullTransact4TargetObjects(target_obj_dict=target_obj_dict, **kwargs)  # idActivityRA or None
        else:     # target objects not passed. Creates 1 idActivityRA, 1 Transaction, 1 Amount.
            transaction = self.createTransaction(**kwargs)
            if isinstance(transaction, self._objClass):
                return self.createAmountActivity(*args, id_transaction=transaction.ID, **kwargs)
            return None




class PurchaseActivity(TransactionActivity):          # Money inflow to the system: Ingresos
    __activityName = 'Compra'
    __method_name = 'purchase'
    _short_name = 'purch'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True  # IncomeActivity supports PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        self.perform(*args, **kwargs)

    def perform(self, *args: DataTable, phase='unbilled', target_obj_dict=None, **kwargs):
        kwargs['fldFK_Actividad'] = self._activityID
        phase = phase.lower() if isinstance(phase, str) else ''
        kwargs['fldFK_Phase'] = phase if phase in self._moneyPhases else 'unbilled'
        if not kwargs.get('fldDate'):
            kwargs['fldDate'] = time_mt('datetime')
        if target_obj_dict and isinstance(target_obj_dict, dict):
            return self.fullTransact4TargetObjects(target_obj_dict=target_obj_dict, **kwargs)  # idActivityRA or None
        else:  # target objects not passed. Creates 1 idActivityRA, 1 Transaction, 1 Amount.
            transaction = self.createTransaction(**kwargs)
            if isinstance(transaction, self._objClass):
                return self.createAmountActivity(*args, id_transaction=transaction.ID, **kwargs)
            return None


class ProvisionActivity(TransactionActivity):          # Money inflow to the system: Ingresos
    __activityName = 'Provision'
    __method_name = 'provision'
    _short_name = 'provis'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True  # IncomeActivity supports PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        self.perform(*args, **kwargs)

    def perform(self, *args: DataTable, **kwargs):
        kwargs['fldFK_Actividad'] = self._activityID
        if not kwargs.get('fldDate'):
            kwargs['fldDate'] = time_mt('datetime')
        transaction_obj = self.createTransaction(**kwargs)
        if isinstance(transaction_obj, self._objClass):
            return self.createAmountActivity(*args, id_transaction=transaction_obj.ID, **kwargs)
        return None


class AccrualActivity(TransactionActivity):          # Money inflow to the system: Ingresos
    __activityName = 'Percepcion'
    __method_name = 'accrual'
    _short_name = 'accru'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True  # IncomeActivity supports PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        self.perform(*args, **kwargs)

    def perform(self, *args: DataTable, **kwargs):
        kwargs['fldFK_Actividad'] = self._activityID
        if not kwargs.get('fldDate'):
            kwargs['fldDate'] = time_mt('datetime')
        transaction_obj = self.createTransaction(**kwargs)
        if isinstance(transaction_obj, self._objClass):
            ret = self.createAmountActivity(*args, id_transaction=transaction_obj.ID, **kwargs)
            return ret
        return None



class IncomeActivity(TransactionActivity):          # Money inflow to the system: Ingresos
    __activityName = 'Ingreso'
    __method_name = 'income'
    _short_name = 'income'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True  # IncomeActivity supports PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def set(self):
        pass

class ExpenseActivity(TransactionActivity):     # Money outflow from the system: Egresos.
    __activityName = 'Egreso'
    __method_name = 'expense'
    _short_name = 'expens'      # _short_name is 6 chars max.

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = True  # OutflowActivity does support PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def set(self):
        pass

class AdjustmentActivity(TransactionActivity):
    __activityName = 'Ajuste'
    __method_name = 'adjustment'
    _short_name = 'adjust'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False  # AdjustmentActivity does NOT support PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def log(self):           # Records an adjustment entry
        pass


class AmendActivity(TransactionActivity):         # Ammendment/correction of Amounts. Ledger Activity
    """ __call__() implemented: This class generates callable objects. Call as: money.amend(id_orig_amt, new_amt_obj).
    """
    __activityName = 'Enmienda De Montos'
    __activityID = 12                       # TODO: Probably better to use ids. See to implement later.
    __method_name = 'amend'
    _short_name = 'amend'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False  # IncomeActivity supports PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        self.amend_transaction(*args, **kwargs)


    def amend_transaction(self, id_amount: str, amt: Amount):
        """ Updates amount and currency on an existing Amount record.
        @param id_amount: fldObjectUID (str) for amount record to be modified. MUST BE PROVIDED, as a Transact can have
        multiple Amounts assigned to it.
        @param amt: Amount Object with new amount, currency values.
        @return: True/False
        """
        # modifies amount and/or currency in Amount object. it IS a Transaction and it IS ledgerActivity.
        if not isinstance(amt, Amount) or not isinstance(id_amount, int):
            return False
        orig_amt = Amount.fetch_obj(id_amount)
        if orig_amt:
            new_amt = orig_amt.amend(amt.amount, amt.currency)  # new_amt carries original amt.ID and new amount, curr.
            if isinstance(new_amt, Amount):
                # Creates a new record in tblRA, as amendment IS a Ledger activity and the change must be recorded.
                idActividadRA = setRecord(self._tblRAName, fldFK_NombreActividad=self._activityID,
                                          fldTimeStamp=time_mt('datetime'), fldFlag=1, fldFK_UserID=self.user,
                                          fldComment=f'Amendment for Montos Record # {new_amt.ID}')

                # Updates Enmiendas field in Montos: All Montos records keep track of the Amendment operations on them.
                amends = getRecords(self.tblMontosName, '', '', None, '*',
                                    fldObjectUID=new_amt.ID).getVal(0, 'fldAmendments')
                if not isinstance(amends, list):
                    amends = []
                amends.append(idActividadRA)
                _ = setRecord(self.tblMontosName, fldID=new_amt.recordID, fldAmendments=amends)
                return idActividadRA
        return False


# Price going into TransactionActivity to enable Transaction ID as the only data required by target objects TM tables.
class PriceActivity(TransactionActivity):
    __activityName = 'Precio'
    __method_name = 'price'
    _short_name = 'price'
    # __tblDataName = 'tblDataTMPrecios'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False  # Does NOT support PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def set(self, *args, **kwargs):
        pass

    def get(self):
        pass


class StatusActivity(MoneyActivity):            # Ver para que usar esto.
    __activityName = 'Status'
    __method_name = 'status'
    _short_name = 'status'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False  # Does NOT support PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)

    def set(self, *args, **kwargs):
        pass

    def get(self):
        pass


class DataUpdateActivity(MoneyActivity):
    """ Activity to update Person details, Bank details, Credit scores and details. All data in the Money db tables. """
    __activityName = 'Actualizacion De Datos'
    __method_name = 'update'
    _short_name = 'update'
    _isLedgerActivity = False

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False  # Does NOT support PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)



class InitializationActivity(MoneyActivity):            # Ver para que usar esto...
    __activityName = 'Inicializacion'
    __method_name = 'init'
    _short_name = 'init'

    def __init__(self, *args, **kwargs):
        kwargs['supportsPA'] = False  # Does NOT support PA.
        kwargs['decorator'] = self.__method_name
        super().__init__(self.__activityName, *args, **kwargs)


Amount.loadCurrencies()       # Inicializa el diccionario de Monedas.
# --------------------------------------------  END CLASSES MONEYACTIVITY ----------------------------------- #


@singleton
class MoneyHandler(EntityObject):
    def __init__(self):
        # No llama a super().__init__() aqui porque no se necesita nada de lo que hay en EntityObject.
        pass

    # Defining _activityObjList will call  _creatorActivityObjects() in EntityObject.__init_subclass__().
    _activityObjList = []  # List of Activity objects that will be created by ActivityMethod factory.
    _myActivityClass = MoneyActivity         # Will look for the property-creating classes starting in this class.
    """ A bunch of attributes are created here at run time: init, sale, purchase, price, status, adjustment, etc. """
#                        ----------------------- End Class MoneyHandler -----------------------------                #

money = MoneyHandler()      # money is a handler-object to execute all MoneyActivity activities.
# ------------------------------------------------------------------------------------------------------------------- #














# TODO: Cuentas bancarias: ALIAS puede no tener puntos. identificar CBU SOLO un string de 22 digitos.
#  Si no es string de 22 digitos decimales, considerar ALIAS.

# if not isinstance(uid, str):
#     return 'ERROR'
# accountUID = removeAccents(uid)
# if len(accountUID) != 22:
#     fldAlias = accountUID        # Si no contiene 22 digitos exactamente se procesa como Alias.
# else:
#     val = next((c for c in accountUID if c not in '0123456789'), None)
#     if val:
#         return 'ERROR'
#     fldUID = accountUID         # es CBU: string de 22 caracteres, todos digitos 0-9.


def createAmount00(self, *args: DataTable, **kwargs):
    """
    @param args: DataTable tables with obj_data to write. Enables writing of full tables (all fields). Write Tables:
    tblTMRegistroDeActividades, tblDataTMMontos. Any other tables are ignored.
    @param kwargs: {fName=fldValue, }-> Field Names in table [Data MoneyActivity Montos] ONLY. No other tables
    supported. kwargs provide a short version of params to operate the method without passing full parameters.
    MANDATORY: fldQuantity, fldUnitAmount.
    If fldUnitAmount is not passed, returns ERR_Sys_InvalidParameter
    @return: [MoneyActivity Registro De Actividades].ID_Actividad (int) or errorCode (string)
    """
    tblRA = setupArgs(self._tblRAName, *args)  # arma tblRA con argumentos en dataList[0]
    tblMontos = setupArgs(self.__tblMontosName, *args, **kwargs)  # arma tblMontos con argumentos en dataList[0]

    # Seteo de Valores de tabla Data Montos
    qty = tblMontos.getVal(0, 'fldQuantity')  # qty=None si fldQuantity no esta entre los argumentos
    unitAmt = tblMontos.getVal(0, 'fldUnitAmount')  # unitAmt=None si fldUnitAmount no esta entre los argumentos
    if qty is None or unitAmt is None:
        retValue = f'ERR_INP_InvalidArgument: Quantity,Unit Amount - MoneyActivity.PY createAmount({lineNum()})'
        print(f'{retValue} / {callerFunction()}')
        return retValue  # Faltan Unit Amount o Quantity: Sale con error

    totalAmt = float(tblMontos.getVal(0, 'fldUnitAmount') * tblMontos.getVal(0, 'fldQuantity'))
    tblMontos.setVal(0, 'fldAmountTotal', totalAmt)
    timeStamp = time_mt('datetime')
    userID = sessionActiveUser
    eventDate = valiDate(tblMontos.getVal(0, 'fldDate'), timeStamp)
    tblMontos.setVal(0, 'fldDate', eventDate)
    currency = int(tblMontos.getVal(0, 'fldFK_Moneda')) if tblMontos.getVal(0, 'fldFK_Moneda') is not None \
        else defaultCurrencyName
    tblMontos.setVal(0, 'fldFK_Moneda', currency)

    # Seteo de valores de TMRegistroDeActividades (SOLO SI SE DEBE insertar record en MoneyActivity Registro De Actividades)
    if tblRA.getVal(0, 'fldID') is not None:
        idActividad = int(tblRA.getVal(0, 'fldID'))  # Hace idActividad=fldID si existe
    else:
        tblRA.setVal(0, 'fldFK', self.activities['Asignacion De Monto'])  # Si no, es Asignacion de Monto
        tblRA.setVal(0, 'fldTimeStamp', timeStamp)  # El Sistema SIEMPRE sobreescribe fldTimeStamp y fldFK_UserID
        tblRA.setVal(0, 'fldFK_UserID', userID)
        idActividad = setRecord(tblRA.tblName,
                                **tblRA.unpackItem(0))  # No se paso idActividad -> Insertar registro en RA
    if type(idActividad) is str:
        retValue = f'ERR_DB_WriteError - MoneyActivity.PY Function/Method: createAmount({lineNum()})'
        print(f'{retValue} / {callerFunction()}')
        return retValue  # Sale con Error si no pudo escribir DB

    else:
        # Completa valores de tblMontos y escribe.
        tblRA.setUndoOnError(True)
        tblMontos.setVal(0, 'fldFK_Actividad', idActividad)
        idActividadMonto = setRecord(tblMontos.tblName, **tblMontos.unpackItem(0))
        if type(idActividadMonto) is not int:  # idActividadMonto tiene ErrorMsg si no es int
            if tblRA.getVal(0, 'fldID') is not None:  # Delete record  de [MoneyActivity RA] solo si se escribio antes
                _ = delRecord(tblRA.tblName, idActividad)
                # queryObj = QSqlQuery(self.db)
                # strDelete = strSQLDeleteRecord(tblRA.tblName, idActividad)
                # queryObj.exec_(strDelete)
            retValue = idActividadMonto + f'Function/Method: createAmount({lineNum()})'
            print(retValue)
        else:
            retValue = idActividad
    return retValue


def createTransaction00(self, *args: DataTable, **kwargs):
    """
    Inserts obj_data in tables MoneyActivity Registro De Actividades, Data MoneyActivity Transacciones Monetarias.
    @param args: Data Tables MoneyActivity Registro De Actividades, Data MoneyActivity Transacciones Monetarias.
    @param kwargs: {fName=fldValue}, fName -> Field Names in table [Data MoneyActivity Transacciones Monetarias].
    Mandatory arguments (in *args or **kwargs): fldAmount, fldFK_OwnershipTransferType, fldFK(Activity Name)
    @return:[MoneyActivity Registro De Actividades].ID_Actividad (int) or errorCode (string)
    """
    tblRA = setupArgs(self._tblRAName, *args)  # Arma tblRA con argumentos en dataList[0]
    tblTransact = setupArgs(self.__tblDataName, *args, **kwargs)  # tblTransact con argumentos en dataList[0]
    if type(tblRA) is str or type(tblTransact) is str:
        retValue = f'ERR_INP_InvalidArgument: Invalid table Names. MoneyActivity.PY creatTransaction({lineNum()})'
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
        tblTransact.setVal(0, fldFK_Moneda=defaultCurrencyName)

    idActividad = tblRA.getVal(0, 'fldID')
    if idActividad is None:
        tblRA.setVal(0, 'fldTimeStamp', timeStamp)  # Insertar registro en TMRegistroDeActividades
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
        retValue = idActividadTransact + f'Function/Method: createAmount({lineNum()}) '
        print(retValue)
    else:
        retValue = idActividad

    return retValue

def getTransactionTotals(self, idTransaction: int):
    """
    Returns tuple: Total Amount for idTransaction (addition of all Montos with that idTransaction) and ID_Moneda
    @param idTransaction: TransactionActivity in [Data MoneyActivity Transacciones] for which total amount is pulled.
    @return: [TotalAmount (float), Currency (int)] / errorCode (str) if invalid idTransacion is passed
    """
    if not isinstance(idTransaction, int) or idTransaction <= 0:
        retValue = f'ERR_INP_InvalidArgument: ID_Transaccion Monetaria - getTransactionTotals({lineNum()})'
        print(f'{retValue}')
        return retValue

    retTable = getRecords(self.__tblMontosName, '', '', None, 'fldID', 'fldAmountTotal', 'fldFK_Moneda',
                          fldFK_Transaccion=idTransaction)
    currency = retTable.getVal(0, 'fldFK_Moneda')   # currency es unico para todos los montos de una Transasccion
    currency = int(currency) if currency is not None else defaultCurrencyName
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
        retValue = f'ERR_INP_InvalidArgument: ID_Transaccion Monetaria - MoneyActivity.PY getTransactionTotals({lineNum()})'
        print(f'{retValue}')
        return retValue
    retTable = getRecords(self.__tblMontosName, '', '', None, '*', fldFK_Transaccion=idTransaction)
    retArray = []
    if retTable.dataLen:
        for i in range(retTable.dataLen):
            retArray.append(retTable.dataList[i])
        retValue = retArray
    else:
        retValue = [retArray, ]
    return retValue

