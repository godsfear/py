#!/python

from decimal import Decimal
from cbs import *
from xfuncs import *
import requests
from datetime import *
install('lxml')
from lxml import etree

def main():
    test = False
    who = 'ФПК'
    cfg = config('migration.json')
    conn = connect(cfg[who])
    codes_loan = {
        'amount':'amount',
        'currency':'currency_id',
        'account_db':'account_db',
        'valuedate':'mdate',
        'agreement_number':'agreement_number'
    }
    val = {
        '1':'KZT',
        '2':'USD'
    }
    dates = ['mdate']
    decimals = ['amount']
    bools = []
    skip = ['NULL']
    loans = txt2dict('storno.csv',codes_loan,dates,'%Y-%m-%d %H:%M:%S',decimals,bools,skip,'"',';')
    head = {'Content-type':'application/json','Accept':'text/plain'}
    url = cfg['target'][who]['URL'] + 'execute-event-list'
    for loan in loans:
        loan['currency_id'] = val[loan['currency_id']]
        ops = {
            'code':'IMPORT_ACCOUNT_BALANCES',
            'eventParameters': {
                'AMOUNT':str(-loan['amount']),
                'CURRENCY':loan['currency_id'],
                'ACCOUNT_DB':cfg['target'][who]['TRANZ'][loan['currency_id']],
                'ACCOUNT_CR':loan['account_db'],
                'VALUEDATE':loan['mdate'].strftime('%d.%m.%Y'),
                'PURPOSE':'Сторно',
                'AGREEMENT_NUMBER':loan['agreement_number'],
                'SUBACCOUNT_TYPE':'INTRST',
                'DB_CR':'DEBIT'
            }
        }
        if test:
            print(ops)
        else:
            print(ops)
            print('>---OPS---->>>>>>>>>',datetime.now())
            req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
            print('<---OPS----<<<<<<<<<',req.status_code,req.reason)
            if (req.status_code != requests.codes.ok):
                print(req.text)
                sys.exit(1)

    conn.commit()

if __name__ == '__main__':
    main()
