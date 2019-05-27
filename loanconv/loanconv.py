#!/python

from xfuncs import *
import os
import json
from datetime import *
import time
import sys
import glob
from cbs import *
import requests
from lxml import etree

def lconv(who,loan,rate,beg_date,noask,commit):
    debug = True
    if who == '':
        who = 'DEFAULT'
    head = {'Content-type':'application/json','Accept':'text/plain'}
    cfg = config('migration.json')
    conn = connect(cfg[who])

    pogash = ['2860']
    laccts = ['1411','1417','1424','1740','1741','1879','1860','111021','111022','127025','127029','201021','217025','217029','128009']
    _od = ['1411','1417','111021','201021']
    _pod = ['1424','111022']
    _pr = ['1740','127025','217025']
    _ppr = ['1741','127029','217029']
    _pen = ['1879','128009']

    qry = "SELECT id FROM common.agreement WHERE agreement_number = '" + loan + "_KZT'"
    cur = query(conn,qry)
    tab = cur.fetchall()
    if len(tab) > 0:
        print('Договор уже конвертирован!')
        return
    
    info = "SELECT cus.id AS cust_id,agr.id AS agreement,la.id AS loan,cur.code AS currency,cus.code AS customer,brn.code AS branch,ext.value AS inn FROM common.agreement AS agr JOIN loans.loan_agreement AS la ON la.agreement_id = agr.id JOIN common.currency AS cur ON cur.id = la.currency_id JOIN common.branch AS brn ON brn.id = agr.branch_id JOIN customers.customer AS cus ON cus.id = agr.customer_id JOIN customers.customer_extended_field_values AS ext ON ext.customer_id = cus.id AND ext.cust_ext_field_id = 6 WHERE agr.agreement_number = '" + loan + "';"
    cur = query(conn,info)
    tab = cur.fetchall()
    info = {}
    for r in (tab):
        info.update({'cust_id':r[0],'agreement':r[1],'loan':r[2],'currency':r[3],'customer':r[4],'branch':r[5],'inn':r[6]})

    zacct = ''
    pog = ','.join("'{0}'".format(w) for w in pogash)
    pog = "SELECT ch.code AS nps,acc.number AS cur FROM accounts.account_customer AS acus JOIN accounts.account AS acc ON acc.id = acus.account_id JOIN accounts.account_ca AS ca ON ca.account_id = acc.id JOIN accounts.chart_of_accounts AS ch ON ch.id = ca.chart_of_accounts_id AND ch.code IN (" + pog + ") JOIN accounts.account_currency AS acur ON acur.account_id = acc.id JOIN common.currency AS cur ON cur.id = acur.currency_id AND cur.code = 'KZT' WHERE acus.customer_id = " + str(info['cust_id']) + ";"
    cur = query(conn,pog)
    tab = cur.fetchall()
    _create = True
    for r in (tab):
        zacct = r[1]
        _create = False
    accs = ','.join("'{0}'".format(w) for w in laccts)
    accs = "SELECT ch.code AS nps,ch.description AS role,acc.description AS name,- ost.balance AS sum,cur.code AS cur,acc.code AS number FROM accounts.account AS acc JOIN od.turnover AS ost ON ost.account_id = acc.id AND ost.balance <> 0 AND ost.value_date = (SELECT MAX(value_date) FROM od.turnover WHERE od.turnover.account_id = acc.id) JOIN accounts.account_ca AS ca ON ca.account_id = acc.id JOIN accounts.chart_of_accounts AS ch ON ch.id = ca.chart_of_accounts_id AND ch.code IN (" + accs + ") JOIN accounts.account_currency AS acur ON acur.account_id = acc.id JOIN common.currency AS cur ON cur.id = acur.currency_id WHERE acc.acc_agreement_id = " + str(info['agreement']) + ";"
    cur = query(conn,accs)
    tab = cur.fetchall()
    ost = []
    plan_kzt = []
    plan_cur = []
    lsumm = 0
    for r in (tab):
        ost.append({'СЧЕТ':r[5],'НПС':r[0],'ТИП':[r[1],r[2]],'БАЛАНС':r[3],'ВАЛЮТА':r[4]})
        if r[0] in ('1411','1417','1424'):
            lsumm = lsumm + r[3]
    lsumm = round(lsumm * rate,2)
    _makeloan_cur = False
    _makeloan_kzt = False
    csumm = 0
    for x in ost:
        label = ''
        for f in x:
            label = label + f + ':' + (str(x[f]) if not isinstance(x[f],list) else str(x[f][0])) + ' '
        if noask:
            sum = x['БАЛАНС']
        else:
            sum = str2dec(input("Укажите сумму конвертации по " + label).strip() or str(x['БАЛАНС']))
        x.update({'КОНВЕРТИРОВАТЬ':sum})
        x.update({'ОСТАТОК':(x['БАЛАНС'] - sum)})
        if x['НПС'] in ('1411','1417','1424'):
            csumm = csumm + x['ОСТАТОК']
        if sum > 0:
            _makeloan_kzt = True
            if sum < x['БАЛАНС']:
                _makeloan_cur = True
        x.update({'ЭКВИВАЛЕНТ':round(x['КОНВЕРТИРОВАТЬ'] * rate,2)})
        print(label,'-->',x['ЭКВИВАЛЕНТ'],'KZT')
    if not _makeloan_kzt:
        print('Нечего конвертировать!')
        return
    for x in ost:
        try:
            typ = cfg['ACCT_ROLE'][x['НПС']]['main_type']
        except:
            typ = cfg['ACCT_ROLE'][x['НПС']]['pay_type']
        plan_kzt.append({'amount':x['ЭКВИВАЛЕНТ'],'amount_principal_after':0,'amount_principal_before':lsumm,'is_delayed':(True if (x['НПС'] in _pod) or (x['НПС'] in _ppr) else False),'target_date':beg_date,'is_migrated':False,'is_migrated_balance_item':True,'repayment_type_id':typ,'date_delayed':(beg_date if ((x['НПС'] in _pod) or (x['НПС'] in _ppr)) else None)})
        if _makeloan_cur:
            plan_cur.append({'amount':x['ОСТАТОК'],'amount_principal_after':0,'amount_principal_before':csumm,'is_delayed':(True if (x['НПС'] in _pod) or (x['НПС'] in _ppr) else False),'target_date':beg_date,'is_migrated':False,'is_migrated_balance_item':True,'repayment_type_id':typ,'date_delayed':(beg_date if ((x['НПС'] in _pod) or (x['НПС'] in _ppr)) else None)})
    qry = "SELECT lacc.account_number FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id JOIN loans.loan_repayment_account AS lacc ON lacc.loan_agreement_id = lon.id AND lacc.account_sign_type_id = (SELECT id FROM loans.account_sign_type WHERE code = 'REPAYMENT_ACCOUNT') WHERE agr.agreement_number = '" + loan + "'"
    cur = query(conn,qry)
    tab = cur.fetchall()
    if len(tab) == 0:
        print('Счет погашения не найден на исходном договоре:',loan)
        return
    for r in tab:
        pacct = r[0]
    newloan = makeloan(conn,loan,{'agreement':{'agreement_number':loan + '_KZT','date_created':datetime.today().date(),'date_start':beg_date,'date_end':beg_date},'loan_agreement':{'accrual_starts_on_next_day':False,'amount':lsumm,'currency_id':'KZT','period_id':[1,1]},'loan_agreement_rp_value':[{'interest_rate_penalty_type_id':'BASE','repayment_type_id':'REWARD','start_date':beg_date,'value_currency_id':'KZT'},{'interest_rate_penalty_type_id':'PENI','repayment_type_id':'PENALTY_FOR_DEBT','start_date':beg_date,'value_currency_id':'KZT'}],'agreement_extended_field_values':[{'agreement_ext_field_id':'EXT_ID','value':'-'}],'loan_agreement_parameter':[{'#force#':True,'agreement_parameter_type_id':'ACCRUEMENT_START_DATE','value':add2date(beg_date,days=1).strftime('%d.%m.%Y'),'value_type':'DATE'},{'#force#':True,'agreement_parameter_type_id':'OVERDUE_START_DATE_REWARD','value':add2date(beg_date,days=1).strftime('%d.%m.%Y'),'value_type':'DATE'},{'#force#':True,'agreement_parameter_type_id':'OVERDUE_START_DATE_DEBT','value':add2date(beg_date,days=1).strftime('%d.%m.%Y'),'value_type':'DATE'}],'loan_repayment_schedule':{'create_date':beg_date,'date_end':beg_date,'date_start':beg_date,'loan_repayment_schedule_item':plan_kzt}},True if commit else False)
    if _makeloan_cur:
        newloan_cur = makeloan(conn,loan,{'agreement':{'agreement_number':loan + '_OST','date_created':datetime.today().date(),'date_start':beg_date,'date_end':beg_date},'loan_agreement':{'accrual_starts_on_next_day':False,'amount':csumm,'currency_id':info['currency'],'period_id':[1,1]},'loan_agreement_rp_value':[{'interest_rate_penalty_type_id':'BASE','repayment_type_id':'REWARD','start_date':beg_date,'value_currency_id':info['currency']},{'interest_rate_penalty_type_id':'PENI','repayment_type_id':'PENALTY_FOR_DEBT','start_date':beg_date,'value_currency_id':info['currency']}],'agreement_extended_field_values':[{'agreement_ext_field_id':'EXT_ID','value':'-'}],'loan_agreement_parameter':[{'#force#':True,'agreement_parameter_type_id':'ACCRUEMENT_START_DATE','value':add2date(beg_date,days=1).strftime('%d.%m.%Y'),'value_type':'DATE'},{'#force#':True,'agreement_parameter_type_id':'OVERDUE_START_DATE_REWARD','value':add2date(beg_date,days=1).strftime('%d.%m.%Y'),'value_type':'DATE'},{'#force#':True,'agreement_parameter_type_id':'OVERDUE_START_DATE_DEBT','value':add2date(beg_date,days=1).strftime('%d.%m.%Y'),'value_type':'DATE'}],'loan_repayment_schedule':{'create_date':beg_date,'date_end':beg_date,'date_start':beg_date,'loan_repayment_schedule_item':plan_cur}},True if commit else False)
    
    if _create:
        url = cfg['target'][who]['URL'] + 'create-account'
        acc = {'branch':info['branch'],'accountType':'CLIENT','description':'Счет выдачи и погашений','npc':cfg['НПС'][cfg['target'][who]['PLAN']]['ТРАНЗ'],'currency':'KZT','customerCode':info['customer'],'idn':info['inn']}
        if commit:
            if debug:
                print(acc)
            req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=acc)
            print('Открытие счета выдачи и погашений',req.status_code,req.reason)
            if (req.status_code != requests.codes.ok):
                print(req.text)
                sys.exit(1)
            doc = etree.XML(req.text)
            zacct = doc.xpath(r'/data/account/number')[0].text
        else:
            print(acc)
    if zacct == '':
        print('Счет выдачи/погашений для конвертированного договора отсутствует!')
        return
    acct2repay(conn,loan + '_KZT',zacct,["REPAYMENT_ACCOUNT","DISB_ACCOUNT"],commit)
    if _makeloan_cur:
        acct2repay(conn,loan + '_OST',pacct,["REPAYMENT_ACCOUNT","DISB_ACCOUNT"],commit)
    sum = 0
    for a in ost.copy():
        acc = {'branch':info['branch'],'accountType':cfg['target'][who]['ACCT_TYPE'],'description':a['ТИП'][1],'npc':a['НПС'],'currency':'KZT','customerCode':info['customer'],'idn':info['inn'],'agreementNumber':(loan + '_KZT')}
        url = cfg['target'][who]['URL'] + 'create-account'
        if commit:
            if debug:
                print(acc)
            req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=acc)
            print('Открытие счета ' + a['ТИП'][1],'KZT',(loan + '_KZT'),req.status_code,req.reason)
            if (req.status_code != requests.codes.ok):
                print(req.text)
                sys.exit(1)
            doc = etree.XML(req.text)
            racct = doc.xpath(r'/data/account/number')[0].text
            a.update({'ACCOUNT_DB':racct})
        else:
            print(acc)
            a.update({'ACCOUNT_DB':'test'})
        url = cfg['target'][who]['URL'] + 'execute-event-list'
        ops = {'code':'IMPORT_ACCOUNT_BALANCES','eventParameters':{'AMOUNT':str(a['ЭКВИВАЛЕНТ']),'CURRENCY':'KZT','ACCOUNT_CR':zacct,'ACCOUNT_DB':a['ACCOUNT_DB'],'VALUEDATE':beg_date.strftime('%d.%m.%Y'),'PURPOSE':'Конвертация','AGREEMENT_NUMBER':(loan + '_KZT'),'SUBACCOUNT_TYPE':cfg['ACCT_ROLE'][a['НПС']]['role'],'DB_CR':'CREDIT'}}
        sum += a['ЭКВИВАЛЕНТ']
        if commit:
            if debug:
                print(ops)
            req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
            print('Операция формирования остатка ' + a['ТИП'][1],'KZT',(loan + '_KZT'),req.status_code,req.reason)
            if (req.status_code != requests.codes.ok):
                print(req.text)
                sys.exit(1)
        else:
            print(ops)

    ops = {'code':'CURRENCY_CONVERSION','eventParameters':{'AMOUNT':str(sum),'CURRENCY':'KZT','ACCOUNT_DB':zacct,'ACCOUNT_CR':pacct,'VALUE_DATE':beg_date.strftime('%d.%m.%Y'),'BRANCH_CODE':info['branch'],'RATE':str(rate)}}
    if commit:
        if debug:
            print(ops)
        req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
        print('Операция закрытия остатка ' + a['ТИП'][1],req.status_code,req.reason)
        if (req.status_code != requests.codes.ok):
            print(req.text)
            sys.exit(1)
    else:
        print(ops)

    if _makeloan_cur:
        sum = 0
        for a in ost.copy():
            acc = {'branch':info['branch'],'accountType':cfg['target'][who]['ACCT_TYPE'],'description':a['ТИП'][1],'npc':a['НПС'],'currency':info['currency'],'customerCode':info['customer'],'idn':info['inn'],'agreementNumber':(loan + '_OST')}
            url = cfg['target'][who]['URL'] + 'create-account'
            if commit:
                if debug:
                    print(acc)
                req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=acc)
                print('Открытие счета ' + a['ТИП'][1],info['currency'],(loan + '_OST'),req.status_code,req.reason)
                if (req.status_code != requests.codes.ok):
                    print(req.text)
                    sys.exit(1)
                doc = etree.XML(req.text)
                racct = doc.xpath(r'/data/account/number')[0].text
                a.update({'ACCOUNT_DB':racct})
            else:
                print(acc)
                a.update({'ACCOUNT_DB':'test'})
            url = cfg['target'][who]['URL'] + 'execute-event-list'
            ops = {'code':'IMPORT_ACCOUNT_BALANCES','eventParameters':{'AMOUNT':str(a['ОСТАТОК']),'CURRENCY':info['currency'],'ACCOUNT_CR':pacct,'ACCOUNT_DB':a['ACCOUNT_DB'],'VALUEDATE':beg_date.strftime('%d.%m.%Y'),'PURPOSE':'Остаток после конвертации','AGREEMENT_NUMBER':(loan + '_OST'),'SUBACCOUNT_TYPE':cfg['ACCT_ROLE'][a['НПС']]['role'],'DB_CR':'CREDIT'}}
            sum += a['ОСТАТОК']
            if commit:
                if debug:
                    print(ops)
                req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
                print('Операция формирования остатка ' + a['ТИП'][1],info['currency'],(loan + '_OST'),req.status_code,req.reason)
                if (req.status_code != requests.codes.ok):
                    print(req.text)
                    sys.exit(1)
            else:
                print(ops)

    if commit:
        conn.commit()

def main():
    commit = True
    who = ''
    loan = ''
    if loan == '':
        loan = input("Введите номер договора: ")
    beg_date = str2date(input("Введите дату открытия для договора конвертации (ДД/ММ/ГГГГ): "))
    rate = str2dec(input("Введите курс конвертации: "))
    lconv(who,loan,rate,beg_date,False,commit)

if __name__ == '__main__':
    main()
