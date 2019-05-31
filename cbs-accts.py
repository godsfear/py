#!/python

from xfuncs import *
from cbs import *
install('requests')
import requests
import csv
install('progressbar')
import progressbar
install('lxml')
from lxml import etree

def main():
    test = False
    fname = 'cesna_loan2.csv'
    who = 'SECURITY'
    cfg = config('migration.json')
    conn = connect(cfg[who])
    
    head = {'Content-type':'application/json','Accept':'text/plain'}
    srok = {'КРАТКОСРОЧНЫЙ':['1'],'ДОЛГОСРОЧНЫЙ':['0']}

    roles = {
        "ACCOUNT": "PRNCPL",
        "ACC_COMMISS": "PNLT_PRNCPL_OVRD",
        "ACC_DEBTS_CR": "PRNCPL_OVRD",
        "ACC_DEBTS_PRC": "INTRST_OVRD",
        "ACC_DEBT_COMIS": "PNLT_PRNCPL_OVRD",
        "ACC_DISCONT": "PNLT_PRNCPL_OVRD",
        "ACC_PROFIT_PF": "INTRST",
        "POST_PRC": "INTRST",
        "STOP_POST_COM_IHC": "",
        "STOP_POST_PRC_IHC": "INTRST",
        "STOP_PRC_IHC": "INTRST",
        "V_PEN_CR": "",
        "V_PEN_PRC": "",
        "V_ACC_PROFIT_PF": "",
        "POST_COMIS": "",
        "X_ACC_COMMISS": "",
        "X_POST_COMIS": "",
        "X_PEN_COMIS": "PNLT_PRNCPL_OVRD",
        "X_ACC_DEBT_COMIS": "",
        "ACC_RESERT": "",
        "PENY_OVERDUE_KEEP_SELL": "",
        "V_ACC_DEBTS_CR": "",
        "V_ACC_DEBTS_PRC": "",
        "STOP_COM_IHC": "",
        "X_POST_PRC": "INTRST",
        "VNB_CALL": "",
        "ACC_RESERV": "",
        "AVANS": "",
        "ACC_PREMIUM": "",
        "A_VB_PEN_COMIS": "PNLT_PRNCPL_OVRD",
        "BOUNTY": "INTRST",
        "BROKERAGE": "PNLT_PRNCPL_OVRD",
        "NOT_STOP_TREB_CRD": "PRNCPL_OVRD",
        "NOT_STOP_TREB_DEL": "PNLT_PRNCPL_OVRD",
        "NOT_STOP_TREB_DEL_OTR": "",
        "NOT_STOP_TREB_PRC": "INTRST_OVRD",
        "OVERDUE_PROFIT": "INTRST_OVRD",
        "PENY_OVERDUE_ACCOUNT": "PNLT_PRNCPL_OVRD",
        "PENY_OVERDUE_COMIS": "PNLT_PRNCPL_OVRD",
        "PENY_OVERDUE_PROFIT": "PNLT_PRNCPL_OVRD",
        "PEN_COMIS": "PNLT_PRNCPL_OVRD",
        "PEN_CR": "PNLT_PRNCPL_OVRD",
        "PEN_PRC": "PNLT_PRNCPL_OVRD",
        "PNLT_PRNCPL_OVRD": "PNLT_PRNCPL_OVRD",
        "PROFIT": "INTRST_OVRD",
        "STOP_COMIS": "PNLT_PRNCPL_OVRD",
        "VNB_AMOUNT": "PNLT_PRNCPL_OVRD",
        "VNB_CALLA": "",
        "VNB_CALLA_OLD": "",
        "VNB_EXCEED_COM": "INTRST_OVRD",
        "VNB_EXCEED_CRED": "PRNCPL_OVRD",
        "VNB_EXCEED_PRC": "INTRST_OVRD",
        "VNB_PARDONA": "",
        "VNB_PARDONA_OLD": "",
        "VNB_PEN_COMIS": "PNLT_PRNCPL_OVRD",
        "VNB_PEN_CR": "PNLT_PRNCPL_OVRD",
        "VNB_PEN_PRC": "PNLT_PRNCPL_OVRD",
        "X_ACCOUNT": "PRNCPL",
        "X_ACC_DEBTS_CR": "PRNCPL_OVRD",
        "X_ACC_DEBTS_PRC": "INTRST_OVRD",
        "X_ACC_PROFIT_PF": "INTRST",
        "X_PEN_CR": "PNLT_PRNCPL_OVRD",
        "X_PEN_PRC": "PNLT_PRNCPL_OVRD",
        "ACC_RESERV_KZT": "",
        "MS_1880_BACK_RANSOM": "",
        "MS_2880_BACK_RANSOM": "",
        "V_ACCOUNT": "",
        "NOT_STOP_TREB_LOAN_URC": "",
        "KEEP_SELL_DEBT": "",
        "PENY_PROFIT_COM_BAL": "",
        "KEEP_SELL": "",
        "ACC_DEBTS_URC": "",
        "NOT_STOP_TREB_LOAN_COM": "",
        "ACC_PROFIT_UF": "",
        "VNB_PARDON": "",
        "VNB_EXCEED_URC": "",
        "TAX_PART": "",
        "ACC_BL_DEBT_CR": "",
        "VNB_TIME_OBLIG": "",
        "ОД": "PRNCPL",
        "%%": "INTRST",
        "П%%": "INTRST_OVRD",
        "ПЕН": "PNLT_PRNCPL_OVRD",
        "ПОД": "PRNCPL_OVRD",
        "FEE_ACR": "FEE_ACR",
        "FEE_OVRD": "FEE_OVRD",
        "PNLT_FEE_OVRD": "PNLT_FEE_OVRD"
    }

    
    codes = {
        'nom':'agreement_number',
        'pen':'amount',
    }

    dates = ['mdate']
    decimals = ['amount']
    bools = []
    skip = ['NULL']
    unquot = []

    floans = txt2dict(fname,codes,dates,'%Y/%m/%d',decimals,bools,skip,unquot,'"',';')
    xloans = []
    xtrans = []
    xcusts = {}

    k = 1
    with progressbar.ProgressBar(max_value=len(floans)) as bar:
        for loan in floans:
            bar.update(k)
            k += 1
            if 'role' not in loan.keys():
                loan.update({'role':'ПЕН'})
            try:
                loan['role'] = ('' if loan['role'] == '' else roles[loan['role']])
            except:
                print('\n',loan['agreement_number'],'Роль счета не определена:',loan['role'])
                return
            if loan['role'] == '':
                continue
            qry = "SELECT cus.code,ext.value,cus.id,agr.date_start,agr.date_end,cur.code,lon.id,agr.id FROM customers.customer AS cus JOIN common.agreement AS agr ON agr.agreement_number = '" + loan['agreement_number'] + "' AND agr.customer_id = cus.id JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id JOIN common.currency AS cur ON cur.id = lon.currency_id JOIN customers.customer_extended_field_values AS ext ON ext.customer_id = cus.id AND ext.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'IDN')"
            cur = query(conn,qry)
            tab = cur.fetchall()
            if len(tab) == 0:
                continue
            for r in tab:
                loan.update({'customerCode':r[0]})
                loan.update({'idn':r[1]})
                loan.update({'customer_id':r[2]})
                loan.update({'date_start':r[3].date()})
                loan.update({'date_end':r[4].date()})
                loan.update({'srok':('КРАТКОСРОЧНЫЙ' if loan_short(r[3].date(),r[4].date()) else 'ДОЛГОСРОЧНЫЙ')})
                loan.update({'currency_id':r[5]})
                loan.update({'loan_agreement_id':r[6]})
                loan.update({'agreement_id':r[7]})
            qry = "SELECT value FROM common.agreement_extended_field_values WHERE agreement_id = " + str(loan['agreement_id']) + " AND agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE')"
            cur = query(conn,qry)
            tab = cur.fetchall()
            if len(tab) == 0:
                continue
            for r in tab:
                loan['mdate'] = datetime.strptime(r[0],'%d.%m.%Y').date()
                if loan['mdate'] >= loan['date_end'] and loan['role'] == 'FEE_ACR':
                    loan['role'] = 'FEE_OVRD'
                    loan['nps'] = '1838'
            try:
                tmp = loan['nps']
            except:
                loan.update({'nps':cfg['НПС'][cfg['target'][who]['PLAN']]['КРЕДИТ'][loan['srok']][loan['role']]})
            try:
                loan['branch_id'] = cfg['target'][who]['BRANCH'][loan['branch_id']]
            except:
                loan['branch_id'] = cfg['target'][who]['BRANCH']['DEFAULT']
            if not ((loan['agreement_number'] + ',' + loan['role']) in xloans):
                xloans.append(loan['agreement_number'] + ',' + loan['role'])
            if not ((str(loan['customer_id']) + ',' + loan['currency_id'] + ',' + loan['branch_id']) in xtrans):
                xtrans.append(str(loan['customer_id']) + ',' + loan['currency_id'] + ',' + loan['branch_id'])
                xcusts.update({(str(loan['customer_id']) + ',' + loan['currency_id'] + ',' + loan['branch_id']):{'branch':loan['branch_id'],'accountType':'CLIENT','description':'Счет выдачи и погашений','npc':cfg['НПС'][cfg['target'][who]['PLAN']]['ТРАНЗ'],'currency':loan['currency_id'],'customerCode':loan['customerCode'],'idn':loan['idn'],'agreement_number':loan['agreement_number']}})

    loans = []
    for x in xloans:
        zloan = {}
        sum = 0
        chk = True
        for loan in (floans):
            if ((loan['agreement_number'] + ',' + loan['role']) == x):
                sum += loan['amount']
                if chk:
                    zloan.update({'branch_id':loan['branch_id']})
                    zloan.update({'role':loan['role']})
                    zloan.update({'nps':loan['nps']})
                    zloan.update({'currency_id':loan['currency_id']})
                    zloan.update({'customerCode':loan['customerCode']})
                    zloan.update({'idn':loan['idn']})
                    zloan.update({'customer_id':loan['customer_id']})
                    zloan.update({'agreement_number':loan['agreement_number']})
                    zloan.update({'mdate':loan['mdate']})
                    chk = False
        zloan.update({'amount':sum})
        loans.append(zloan)

    url = cfg['target'][who]['URL'] + 'create-account'
    for x in xtrans:
        ids = re.split(',',x)
        crt = True
        qry = "SELECT ch.code AS nps FROM accounts.account_customer AS acus JOIN accounts.account AS acc ON acc.id = acus.account_id AND acc.branch_id = (SELECT id FROM common.branch WHERE code = '" + ids[2] + "') JOIN accounts.account_ca AS ca ON ca.account_id = acc.id JOIN accounts.chart_of_accounts AS ch ON ch.id = ca.chart_of_accounts_id AND ch.code = '" + cfg['НПС'][cfg['target'][who]['PLAN']]['ТРАНЗ'] + "' JOIN accounts.account_currency AS acur ON acur.account_id = acc.id AND acur.currency_id = (SELECT id FROM common.currency WHERE code = '" + ids[1] + "') WHERE acus.customer_id = " + ids[0] + ";"
        cur = query(conn,qry)
        tab = cur.fetchall()
        if len(tab) > 0:
            crt = False
        if crt:
            if test:
                print(xcusts[x])
            else:
                print('>---TRANZ--->>>>>>>>>',datetime.now())
                req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=xcusts[x])
                print('<---TRANZ---<<<<<<<<<',req.status_code,req.reason)
                if (req.status_code != requests.codes.ok):
                    print(req.text)
                    sys.exit(1)
                doc = etree.XML(req.text)
                racct = doc.xpath(r'/data/account/number')[0].text
                acct2repay(conn,xcusts[x]['agreement_number'],racct,["REPAYMENT_ACCOUNT","DISB_ACCOUNT"],not test)
                print(racct)

    for loan in loans:
        qry = "SELECT acc.number FROM common.agreement AS agr JOIN accounts.account AS acc ON acc.acc_agreement_id = agr.id JOIN accounts.account_ca AS ca ON ca.account_id = acc.id AND ca.chart_of_accounts_id = (SELECT id FROM accounts.chart_of_accounts WHERE code = '" + loan['nps'] + "') WHERE agr.agreement_number = '" + loan['agreement_number'] + "';"
        cur = query(conn,qry)
        tab = cur.fetchall()
        if len(tab) == 0:
            acc = {
                'branch':loan['branch_id'],
                'accountType':cfg['target'][who]['ACCT_TYPE'],
                'description':cfg['ACCT_ROLE'][loan['nps']]['description'],
                'npc':loan['nps'],
                'currency':loan['currency_id'],
                'customerCode':loan['customerCode'],
                'idn':loan['idn'],
                'agreementNumber':loan['agreement_number']
            }
            if test:
                print(acc)
                loan.update({'ACCOUNT_DB':'test'})
            else:
                print('>---ACCT--->>>>>>>>>',datetime.now())
                req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=acc)
                print('<---ACCT---<<<<<<<<<',req.status_code,req.reason)
                if (req.status_code != requests.codes.ok):
                    print(req.text)
                    sys.exit(1)
                doc = etree.XML(req.text)
                racct = doc.xpath(r'/data/account/number')[0].text
                print(racct)
                loan.update({'ACCOUNT_DB':racct})
        else:
            for r in (tab):
                loan.update({'ACCOUNT_DB':r[0]})

    url = cfg['target'][who]['URL'] + 'execute-event-list'
    for loan in loans:
        if loan['amount'] > 0:
            skip = False
            qry = "SELECT ost.balance FROM accounts.account AS acc JOIN od.turnover AS ost ON ost.account_id = acc.id AND ost.currency_id = (SELECT id FROM common.currency WHERE code = '" + loan['currency_id'] + "') AND ost.value_date = '" + loan['mdate'].strftime('%Y-%m-%d') + "'::DATE WHERE acc.number = '" + loan['ACCOUNT_DB'] + "'"
            cur = query(conn,qry)
            tab = cur.fetchall()
            for r in (tab):
                if r[0] != 0:
                    skip = True
            if skip:
                continue
            ops = {
                'code':'IMPORT_ACCOUNT_BALANCES',
                'eventParameters': {
                    'AMOUNT':str(loan['amount']),
                    'CURRENCY':loan['currency_id'],
                    'ACCOUNT_CR':cfg['target'][who]['TRANZ'][loan['currency_id']],
                    'ACCOUNT_DB':loan['ACCOUNT_DB'],
                    'VALUEDATE':loan['mdate'].strftime('%d.%m.%Y'),
                    'PURPOSE':'Миграция',
                    'AGREEMENT_NUMBER':loan['agreement_number'],
                    'SUBACCOUNT_TYPE':loan['role'],
                    'DB_CR':'CREDIT'
                }
            }
            if test:
                print(ops)
            else:
                print('>---OPS---->>>>>>>>>',datetime.now())
                req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
                print('<---OPS----<<<<<<<<<',req.status_code,req.reason)
                if (req.status_code != requests.codes.ok):
                    print(req.text)
                    sys.exit(1)
                if not (loan['role'] in ['FEE_ACR','FEE_OVRD','PNLT_FEE_OVRD']):
                    ost2graf(conn,loan['agreement_number'],loan['role'],not test)

if __name__ == '__main__':
    main()
