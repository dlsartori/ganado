from decimal import Decimal
# from krnl_config import time_mt
# from krnl_dataTable import getRecords, setRecord, DataTable
import money
# https://pypi.org/project/money/
from money import Money, xrates, XMoney

if __name__ == '__main__':
    xrates.install('money.exchange.SimpleBackend')
    money.xrates.backend = 'money.exchange.SimpleBackend'
    xrates.base = 'USD'
    print(f'backend: {money.xrates.backend}   /  BASE CURRENCY: {xrates.base}')

    xrates.setrate('ARS', Decimal('500.00'))       # set dollar rate.
    amusd = XMoney(1.0000, 'USD')
    amars = XMoney(1, 'ARS')

    print(f'Amount1: {amars.amount} {amars.currency} / Amount2: {amusd.amount} {amusd.currency}')
    print(f'In {amusd.currency}: {amusd + amars}')
    print(f'In {amars.currency}: {amars + amusd}')

    newamt = Money(1000, 'ARS')
    newamt.format(currency_digits=True)
