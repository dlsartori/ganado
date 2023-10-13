from krnl_tm import Amount, money
from krnl_config import time_mt
from krnl_db_access import writeObj
from krnl_async_buffer import AsyncBuffer
from krnl_object_instantiation import loadItemsFromDB
from uuid import uuid4
from custom_types import getRecords, DataTable
from krnl_bovine import Bovine

def createUID(tblName: str):
    temp = getRecords(tblName, '', '', None, '*')
    if isinstance(temp, DataTable) and temp.dataLen:
        for j in range(temp.dataLen):
            temp.setVal(j, fldObjectUID=str(uuid4().hex))
        temp.setRecords()
        return True
    return False


if __name__ == '__main__':

    transact_data = {'fldDescription': None,
                        'fldTransactionID': None,
                        'fldDate': time_mt('datetime'),
                        'fldName': '9d4b9edd5402403abe66c2fe532efd71',
                        'fldThirdPtyID': None,
                        'fldFK_Contrato': None,
                        'fldBankAccountUID': None,
                        'fldTransactionUID': None,
                        'fldTransactionSupportDocs': None,
                        'fldQuantity': 0,
                        'fldFK_Unidad': None,
                        'fldFK_TransaccionDeReferencia': None,  # Previous transaction linked to this one
                        'fldComment': ''
                     }

    tbl = getRecords('tblAnimales', '', '', None, '*', fldDateExit=None)
    tbl.unpackItem(0)

    exit(0)

    shortList = (0, 1, 4, 8, 11)        # 18, 27, 32, 130, 172, 210, 244, 280, 398, 41, 61, 92, 363, 368, 372)
    bovines = loadItemsFromDB(Bovine, items=shortList, init_tags=True)  # Abstract Factory funca lindo aqui...

    amt1 = Amount('12.49700000000001', 'USD')
    amt2 = Amount('15.49700000000001', 'USD')
    a = float(12.497)
    b = a
    sum_amt = amt1.amount + amt2.amount
    print('amt1=%.50f' % amt1.amount, 'ADDITION: {amt1.amount} + {amt2.amount} = %.50f' % sum_amt)
    print(f'a+b(float)={a+b} a+b(float) == sum_amt(Decimal): {a+b == sum_amt}')

    idAmount1 = (amt1 + amt2)
    idAmount1.record()
    amt1db = Amount.fetch_obj(idAmount1.ID)
    print('amt1: %.30f' % (amt1 + amt2).amount, '- amt1db: %.30f' % amt1db.amount)
    print(f'amt1 == amt1db: {(amt1 + amt2).amount == amt1db.amount}')

    money.sale(target_obj_dict={bovines[0]: (amt1, amt2)}, **transact_data)

    # money.purchase()
    amt2.amend(amt2.amount+100, amt2.currency)

    AsyncBuffer.flush_all()
    writeObj.stop()

