#!/python

from decimal import Decimal
from cbs import *
from xfuncs import *
import requests
from datetime import *
install('lxml')
from lxml import etree

def main():
    cfg = config('migration.json')
    conn = connect(cfg['МФО'])
    codes_loan = {
        '_loan_id':'EXT_ID',
        '_line_number':'_line_number'
    }
    dates = []
    decimals = []
    bools = []
    skip = ['NULL']
    loans = txt2dict('loan.csv',codes_loan,dates,'%Y-%m-%d',decimals,bools,skip,'"',',')
    for loan in loans:
        try:
            tmp = loan['_line_number']
        except:
            continue
        loan['_line_number'] = loan['_line_number'] + '_'
        qry = "SELECT agr.id,lon.id FROM common.agreement AS agr JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'EXT_ID') AND ext.value = '" + loan['EXT_ID'] + "' JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id"
        cur = query(conn,qry)
        tab = cur.fetchall()
        for r in tab:
            loan.update({'agreement_id':r[0],'loan_agreement_id':r[1]})

        qry = "SELECT lon.id FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id WHERE agr.agreement_number = '" + loan['_line_number'] + "'"
        cur = query(conn,qry)
        tab = cur.fetchall()
        for r in tab:
            loan.update({'line_id':r[0]})
        try:
            tmp = loan['line_id']
        except:
            continue
        qry = "UPDATE loans.loan_agreement SET parent_agreement_id = " + str(loan['line_id']) + " WHERE loans.loan_agreement.id = " + str(loan['loan_agreement_id'])
        cur = query(conn,qry)

    conn.commit()

if __name__ == '__main__':
    main()
