#!/python

import requests
import csv
from xfuncs import *
install('progressbar')
import progressbar
install('lxml')
from lxml import etree

def main():
    test = False
    fname = 'ost.csv'
    who = 'МФО'
    cfg = config('migration.json')
    conn = connect(cfg['МФО'])
    
    conv = []
    head = {'Content-type':'application/json','Accept':'text/plain'}

    atype = {'CLIENT':'INTERNAL_CLIENT','1':'INTERNAL_CLIENT','0':cfg['target'][who]['ACCT_TYPE']}
    srok = {'КРАТКОСРОЧНЫЙ':['1'],'ДОЛГОСРОЧНЫЙ':['0']}
    accts = {}
    #----------------------------------------------------БТА
    accts.update({'ACCOUNT':{'description':'Долг','role':'PRNCPL'}})
    accts.update({'X_ACCOUNT':{'description':'Долг','role':'PRNCPL'}})
    accts.update({'ACC_DEBTS_CR':{'description':'Долг просроченный','role':'PRNCPL_OVRD'}})
    accts.update({'VNB_EXCEED_CRED':{'description':'Долг просроченный','role':'PRNCPL_OVRD'}})
    accts.update({'NOT_STOP_TREB_CRD':{'description':'Долг просроченный','role':'PRNCPL_OVRD'}})
    accts.update({'X_ACC_DEBTS_CR':{'description':'Долг просроченный','role':'PRNCPL_OVRD'}})
    accts.update({'ACC_PROFIT_PF':{'description':'Проценты','role':'INTRST'}})
    accts.update({'BOUNTY':{'description':'Проценты','role':'INTRST'}})
    accts.update({'X_ACC_PROFIT_PF':{'description':'Проценты','role':'INTRST'}})
    accts.update({'NOT_STOP_TREB_DEL_OTR':{'description':'Проценты','role':'INTRST'}})
    accts.update({'ACC_DEBTS_PRC':{'description':'Проценты просроченные','role':'INTRST_OVRD'}})
    accts.update({'X_ACC_DEBTS_PRC':{'description':'Проценты просроченные','role':'INTRST_OVRD'}})
    accts.update({'VNB_EXCEED_COM':{'description':'Проценты просроченные','role':'INTRST_OVRD'}})
    accts.update({'PROFIT':{'description':'Проценты просроченные','role':'INTRST_OVRD'}})
    accts.update({'OVERDUE_PROFIT':{'description':'Проценты просроченные','role':'INTRST_OVRD'}})
    accts.update({'NOT_STOP_TREB_PRC':{'description':'Проценты просроченные','role':'INTRST_OVRD'}})
    accts.update({'VNB_EXCEED_PRC':{'description':'Проценты просроченные','role':'INTRST_OVRD'}})
    accts.update({'PEN_CR':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'VNB_PEN_CR':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'PENY_OVERDUE_ACCOUNT':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'PNLT_PRNCPL_OVRD':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'VNB_PEN_PRC':{'description':'Пеня по процентам просроченным','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'PENY_OVERDUE_PROFIT':{'description':'Пеня по процентам просроченным','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'PEN_PRC':{'description':'Пеня по процентам просроченным','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'ACC_RESERV':{'description':'','role':''}})
    accts.update({'ACC_RESERT':{'description':'','role':''}})
    accts.update({'VNB_PARDONA_OLD':{'description':'','role':''}})
    accts.update({'VNB_PARDONA':{'description':'','role':''}})
    accts.update({'AVANS':{'description':'','role':''}})
    accts.update({'VNB_CALLA_OLD':{'description':'','role':''}})
    accts.update({'VNB_CALLA':{'description':'','role':''}})
    accts.update({'BROKERAGE':{'description':'Комиссия на долг','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'PENY_OVERDUE_COMIS':{'description':'Комиссия на долг','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'NOT_STOP_TREB_DEL':{'description':'Комиссия на долг','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'A_VB_PEN_COMIS':{'description':'Комиссия на долг','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'VNB_PEN_COMIS':{'description':'Комиссия на долг','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'PEN_COMIS':{'description':'Комиссия на долг','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'STOP_COMIS':{'description':'Комиссия на долг','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'X_PEN_CR':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'ACC_COMMISS':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'X_PEN_PRC':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'VNB_AMOUNT':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'ACC_DISCONT':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    accts.update({'ACC_DEBT_COMIS':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    #----------------------------------------------------МФО
    accts.update({'ОД':{'description':'Долг','role':'PRNCPL'}})
    accts.update({'ПОД':{'description':'Долг просроченный','role':'PRNCPL_OVRD'}})
    accts.update({'%%':{'description':'Проценты','role':'INTRST'}})
    accts.update({'П%%':{'description':'Проценты просроченные','role':'INTRST_OVRD'}})
    accts.update({'ПЕН':{'description':'Пеня по долгу просроченному','role':'PNLT_PRNCPL_OVRD'}})
    

    nps = {}
    nps.update({'ТРАНЗ,БАНК':'2860'})
    nps.update({'PRNCPL,КРАТКОСРОЧНЫЙ,БАНК':'1411'})
    nps.update({'PRNCPL_OVRD,КРАТКОСРОЧНЫЙ,БАНК':'1424'})
    nps.update({'INTRST,КРАТКОСРОЧНЫЙ,БАНК':'1740'})
    nps.update({'INTRST_OVRD,КРАТКОСРОЧНЫЙ,БАНК':'1741'})

    nps.update({'PRNCPL,ДОЛГОСРОЧНЫЙ,БАНК':'1417'})
    nps.update({'PRNCPL_OVRD,ДОЛГОСРОЧНЫЙ,БАНК':'1424'})
    nps.update({'INTRST,ДОЛГОСРОЧНЫЙ,БАНК':'1740'})
    nps.update({'INTRST_OVRD,ДОЛГОСРОЧНЫЙ,БАНК':'1741'})
    
    nps.update({'PNLT_PRNCPL_OVRD,КРАТКОСРОЧНЫЙ,БАНК':'1879'})
    nps.update({'PNLT_PRNCPL_OVRD,ДОЛГОСРОЧНЫЙ,БАНК':'1879'})

    nps.update({'ТРАНЗ,МКО':'339022'})
    nps.update({'PRNCPL,КРАТКОСРОЧНЫЙ,МКО':'111021'})
    nps.update({'PRNCPL_OVRD,КРАТКОСРОЧНЫЙ,МКО':'111022'})
    nps.update({'INTRST,КРАТКОСРОЧНЫЙ,МКО':'127025'})
    nps.update({'INTRST_OVRD,КРАТКОСРОЧНЫЙ,МКО':'127029'})

    nps.update({'PRNCPL,ДОЛГОСРОЧНЫЙ,МКО':'201021'})
    nps.update({'PRNCPL_OVRD,ДОЛГОСРОЧНЫЙ,МКО':'111022'})
    nps.update({'INTRST,ДОЛГОСРОЧНЫЙ,МКО':'217025'})
    nps.update({'INTRST_OVRD,ДОЛГОСРОЧНЫЙ,МКО':'217029'})

    nps.update({'PNLT_PRNCPL_OVRD,КРАТКОСРОЧНЫЙ,МКО':'128009'})
    nps.update({'PNLT_PRNCPL_OVRD,ДОЛГОСРОЧНЫЙ,МКО':'128009'})
    
    npses = ['1411','1424','1740','1741','1417','1879','1061','111021','111022','127025','127029','201021','217025','217029']

    if os.path.exists(fname):
        with open(fname,'r',encoding='utf-8') as f:
            try:
                _csv = csv.reader(f,delimiter=';',quotechar='"')
                txt = ''
                for row in _csv:
                    txt = txt + ('\n' if txt != '' else '') + '`'.join(row)
            except IOError:
                print ("Could not read file:",fname)
                sys.exit(1)
    else:
        print("File not exist:",fname)
        sys.exit(1)

    lines = re.split('\n',txt.replace('\r',''))
    acctlist = []
    list2860 = []
    clients2860 = []
    loans = []
    k = 1
    with progressbar.ProgressBar(max_value=len(lines)) as bar:
        for line in (lines):
            bar.update(k)
            k += 1
            if (line == '' or k == 2):
                continue
            line = line.replace('"','')
            line = re.split('`',line)
            if (line[3] in ['1428','1434','1435'] or 'RESERV' in line[5] or line[5] == '' or accts[line[5]]['role'] == ''):
                continue
            loan = {}
            loan.update({'branch':cfg['target'][who]['BRANCH'][line[0]]})
            loan.update({'accountType':atype[line[1]]})
            loan.update({'npc':(line[3] if line[3] != '' else nps[accts[line[5]]['role'] + ',' + ('КРАТКОСРОЧНЫЙ' if (line[2] in srok['КРАТКОСРОЧНЫЙ']) else 'ДОЛГОСРОЧНЫЙ') + ',' + cfg['target'][who]['PLAN']])})
            if not (loan['npc'] in npses):
                loan['npc'] = nps[accts[line[5]]['role'] + ',ДОЛГОСРОЧНЫЙ,' + cfg['target'][who]['PLAN']]
            loan.update({'summa':line[4]})
            loan.update({'acctrole':accts[line[5]]['role']})
            loan.update({'description':accts[line[5]]['description']})
            loan.update({'currency':('KZT' if line[6] == '' else line[6])})
            loan.update({'agreementNumber':line[9]})
            cust = "SELECT cus.code,ext.value,cus.id FROM customers.customer AS cus JOIN common.agreement AS agr ON agr.agreement_number = '" + loan['agreementNumber'] + "' AND agr.customer_id = cus.id JOIN customers.customer_extended_field_values AS ext ON ext.customer_id = cus.id AND ext.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'IDN')"
            cur = query(conn,cust)
            tab = cur.fetchall()
            for r in (tab):
                loan.update({'customerCode':r[0]})
                loan.update({'idn':r[1]})
                loan.update({'customer_id':r[2]})
            loan.update({'mdate':line[11]})
            loans.append(loan)
            if not ((loan['agreementNumber'] + '|' + loan['acctrole']) in acctlist):
                acctlist.append(loan['agreementNumber'] + '|' + loan['acctrole'])
            if not ((loan['customerCode'] + '|' + loan['currency']) in list2860):
                list2860.append(loan['customerCode'] + '|' + loan['currency'])
                clients2860.append({'customerCode':loan['customerCode'],'idn':loan['idn'],'currency':loan['currency'],'branch':loan['branch'],'customer_id':loan['customer_id']})
    xloans = []
    for a in (acctlist):
        xloan = {}
        sum = 0
        chk = True
        for loan in (loans):
            if ((loan['agreementNumber'] + '|' + loan['acctrole']) == a):
                sum += float(loan['summa'] if loan['summa'] != '' else '0')
                if chk:
                    xloan.update({'branch':loan['branch']})
                    xloan.update({'accountType':loan['accountType']})
                    xloan.update({'npc':loan['npc']})
                    xloan.update({'acctrole':loan['acctrole']})
                    xloan.update({'description':loan['description']})
                    xloan.update({'currency':loan['currency']})
                    xloan.update({'customerCode':loan['customerCode']})
                    xloan.update({'idn':loan['idn']})
                    xloan.update({'customer_id':loan['customer_id']})
                    xloan.update({'agreementNumber':loan['agreementNumber']})
                    try:
                        xloan.update({'mdate':datetime.strptime(loan['mdate'],'%Y/%m/%d')})
                    except:
                        try:
                            xloan.update({'mdate':datetime.strptime(loan['mdate'],'%Y-%m-%d')})
                        except:
                            try:
                                xloan.update({'mdate':datetime.strptime(loan['mdate'],'%d.%m.%Y')})
                            except:
                                print('Date format error:',loan['mdate'])
                                sys.exit(1)
                    chk = False
        xloan.update({'summa':sum})
        xloans.append(xloan)

    url = cfg['target'][who]['URL'] + 'create-account'
    for client in (clients2860):
        crt = True
        acc = "SELECT ch.code AS nps,cur.code AS cur FROM accounts.account_customer AS acus JOIN accounts.account AS acc ON acc.id = acus.account_id JOIN accounts.account_ca AS ca ON ca.account_id = acc.id JOIN accounts.chart_of_accounts AS ch ON ch.id = ca.chart_of_accounts_id AND ch.code IN ('" + nps['ТРАНЗ,' + cfg['target'][who]['PLAN']] + "') JOIN accounts.account_currency AS acur ON acur.account_id = acc.id JOIN common.currency AS cur ON cur.id = acur.currency_id WHERE acus.customer_id = " + str(client['customer_id']) + ";"
        cur = query(conn,acc)
        tab = cur.fetchall()
        if len(tab) > 0:
            crt = False
        if crt:
            acc = {'branch':client['branch'],'accountType':'CLIENT','description':'Счет выдачи и погашений','npc':nps['ТРАНЗ,' + cfg['target'][who]['PLAN']],'currency':client['currency'],'customerCode':client['customerCode'],'idn':client['idn']}
            print('>---ACCT--->>>>>>>>>',datetime.now())
            print(acc)
            if not test:
                req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=acc)
                print('<---ACCT---<<<<<<<<<',req.status_code,req.reason)
                if (req.status_code != requests.codes.ok):
                    print(req.text)
                    sys.exit(1)
                doc = etree.XML(req.text)
                racct = doc.xpath(r'/data/account/number')[0].text
                print(racct)

    url = cfg['target'][who]['URL'] + 'create-account'
    for loan in (xloans):
        acc = {'branch':loan['branch'],'accountType':loan['accountType'],'description':loan['description'],'npc':loan['npc'],'currency':loan['currency'],'customerCode':loan['customerCode'],'idn':loan['idn'],'agreementNumber':loan['agreementNumber']}
        print('>---ACCT--->>>>>>>>>',datetime.now())
        print(acc)
        if not test:
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
            loan.update({'ACCOUNT_DB':'test'})

    url = cfg['target'][who]['URL'] + 'execute-event-list'
    for loan in (xloans):
        if loan['summa'] > 0:
            ops = {}
            ops.update({'code':'IMPORT_ACCOUNT_BALANCES'})
            ops.update({'eventParameters':{'AMOUNT':str(loan['summa']),'CURRENCY':loan['currency'],'ACCOUNT_CR':cfg['target'][who]['TRANZ'][loan['currency']],'ACCOUNT_DB':loan['ACCOUNT_DB'],'VALUEDATE':loan['mdate'].strftime('%d.%m.%Y'),'PURPOSE':'Миграция','AGREEMENT_NUMBER':loan['agreementNumber'],'SUBACCOUNT_TYPE':loan['acctrole']}})
            print('>---OPS---->>>>>>>>>',datetime.now())
            print(ops)
            if not test:
                req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
                print('<---OPS----<<<<<<<<<',req.status_code,req.reason)
                if (req.status_code != requests.codes.ok):
                    print(req.text)
                    sys.exit(1)

    """with open('qqq.csv','r',encoding='utf-8') as f:
        try:
            txt = f.read()
        except IOError:
            print ("Could not read file:",fname)
            sys.exit(1)
    lines = re.split('\n',txt.replace('\r',''))
    url = cfg['target'][who]['URL'] + 'execute-event-list'
    for line in (lines):
        line = line.replace('"','')
        if line == '':
            continue
        line = re.split(';',line)
        for loan in (xloans):
            if (loan['agreementNumber'] == line[1] and loan['npc'] == line[2]):
                racct = line[0]
                if loan['summa'] > 0:
                    ops = {}
                    ops.update({'code':'IMPORT_ACCOUNT_BALANCES'})
                    ops.update({'eventParameters':{'AMOUNT':str(loan['summa']),'CURRENCY':loan['currency'],'ACCOUNT_CR':cfg['target'][who]['TRANZ'][loan['currency']],'ACCOUNT_DB':racct,'VALUEDATE':loan['mdate'].strftime('%d.%m.%Y'),'PURPOSE':'Миграция','AGREEMENT_NUMBER':loan['agreementNumber'],'SUBACCOUNT_TYPE':loan['acctrole']}})
                    print('>---OPS---->>>>>>>>>',datetime.now())
                    print(ops)
                    req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
                    print('<---OPS----<<<<<<<<<',req.status_code,req.reason)
                    if (req.status_code != requests.codes.ok):
                        sys.exit(1)
                    print(req.text)"""

    """with open('ost-conv.csv','r',encoding='utf-8') as f:
        try:
            txt = f.read()
        except IOError:
            print ("Could not read file:",'')
            sys.exit(1)
    conv = re.split('\n',txt.replace('\r','').replace('"',''))

    with open('data-1549628796579.csv','r',encoding='utf-8') as f:
        try:
            txt = f.read()
        except IOError:
            print ("Could not read file:",'')
            sys.exit(1)
    lines = re.split('\n',txt.replace('\r',''))
    url = cfg['target'][who]['URL'] + 'execute-event-list'
    for line in (lines):
        line = line.replace('"','')
        if line == '':
            continue
        line = re.split(';',line)
        for loan in (xloans):
            if (loan['agreementNumber'] == line[1] and loan['npc'] == line[2]):
                racct = line[0]
                if loan['summa'] > 0:
                    for ccc in (conv):
                        ccc = ccc.replace('"','')
                        if ccc == '':
                            continue
                        ccc = re.split(';',ccc)
                        if ccc[0] == loan['agreementNumber']:
                            ops = {}
                            ops.update({'code':'IMPORT_ACCOUNT_BALANCES'})
                            ops.update({'eventParameters':{'AMOUNT':str(loan['summa']),'CURRENCY':loan['currency'],'ACCOUNT_CR':racct,'ACCOUNT_DB':cfg['target'][who]['TRANZ'][loan['currency']],'VALUEDATE':loan['mdate'].strftime('%d.%m.%Y'),'PURPOSE':'Миграция','AGREEMENT_NUMBER':loan['agreementNumber'],'SUBACCOUNT_TYPE':loan['acctrole']}})
                            print('>---OPS---->>>>>>>>>',datetime.datetime.now())
                            print(ops)
                            #req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
                            #print('<---OPS----<<<<<<<<<',req.status_code,req.reason)
                            #if (req.status_code != requests.codes.ok):
                            #    sys.exit(1)
                            #print(req.text)
                            sum = {'PRNCPL_OVRD':ccc[1],'INTRST_OVRD':ccc[3],'PNLT_PRNCPL_OVRD':ccc[5]}
                            ops = {}
                            ops.update({'code':'IMPORT_ACCOUNT_BALANCES'})
                            ops.update({'eventParameters':{'AMOUNT':sum[loan['acctrole']],'CURRENCY':'KZT','ACCOUNT_CR':cfg['target'][who]['TRANZ']['KZT'],'ACCOUNT_DB':racct,'VALUEDATE':loan['mdate'].strftime('%d.%m.%Y'),'PURPOSE':'Миграция','AGREEMENT_NUMBER':loan['agreementNumber'],'SUBACCOUNT_TYPE':loan['acctrole']}})
                            print('>---OPS---->>>>>>>>>',datetime.datetime.now())
                            print(ops)
                            #req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
                            #print('<---OPS----<<<<<<<<<',req.status_code,req.reason)
                            #if (req.status_code != requests.codes.ok):
                            #    sys.exit(1)
                            #print(req.text)"""

if __name__ == '__main__':
    main()
