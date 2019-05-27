#!/python

import datetime
import sys,locale,os
import json
import re
import requests
from xfuncs import *
install('progressbar')
import progressbar
install('lxml')
from lxml import etree

def main():
    test = True
    fname_cli = 'client.csv'
    fname_doc = ''
    fname_dop = ''
    who = 'МФО'
    portf = 'БТА'

    cfg = config('migration.json')

    portfolio = {'БТА':'1'}
    head = {'Content-type':'application/json','Accept':'text/plain'}
    codes = {
        '_branch':'_BRANCH_ID',
        '_client_id':'EXT_ID',
        '_last_name':'LNAME',
        '_first_name':'FNAME',
        '_pathron':'MNAME',
        '_iin':'IDN',
        '_bin':'IDN',
        '_rezident':'RESIDENT',
        '_birth':'BIRTH',
        '_full_name':'J_NAME',
        '_client_type':'_CAT',
        '_number':'DOC_NO_PASS',
        '_issue':'ISSUED_BY_PASS',
        '_open_date':'ISSUE_DATE_PASS',
        '_end_date':'EXPIRY_PASS',
        '_type':'ID_DOC',
        '_recv':'_REC',
        '_value':'_VAL'
    }
    
    clicat = {'CL_ORG':'JURIDICAL','CL_PRIV':'INDIVIDUAL','0':'INDIVIDUAL','1':'JURIDICAL'}

    sprav = ['ID_DOC']
    dates = ['BIRTH','ISSUE_DATE_PASS','EXPIRY_PASS']
    bools = ['RESIDENT']

    iddoc = {'Удостоверение личности РК':'96'}

    recv = {
        'Адрес проживания':'adr_j',
        'Адрес прописки':'adr_p',
        'Телефон':'tel_',
        'Телефон домашний':'tel_d',
        'Факс':'tel_r'
    }

    rowid = {
        'adr_j':'5100',
        'adr_p':'5101',
        'tel_':'5160',
        'tel_d':'5159',
        'tel_r':'5158'
    }

    url = cfg['target'][who]['URL'] + 'create-customer'
    clients = txt2dict(fname_cli,codes,dates,'%Y-%m-%d',bools,'"',',')
    for cli in clients:
        try:
            cli['_BRANCH_ID'] = cfg['target'][who]['BRANCH'][cli['_BRANCH_ID']]
        except:
            pass
        try:
            tmp = cli['J_SHORT_NAME']
        except:
            cli.update({'J_SHORT_NAME':cli['J_NAME']})
        try:
            tmp = cli['RESIDENT']
        except:
            cli.update({'RESIDENT':'true'})
    documents = txt2dict(fname_doc,codes,dates,'%Y-%m-%d',bools,'"',',')
    for doc in documents:
        try:
            doc['ID_DOC'] = iddoc[doc['ID_DOC']]
        except:
            pass
    dops = txt2dict(fname_dop,codes,dates,'%Y-%m-%d',bools,'"',',')
    for dop in dops:
        try:
            dop['_REC'] = recv[dop['_REC']]
        except:
            pass

    for cli in (clients):
        try:
            tmp = cli['EXT_ID']
        except:
            continue
        try:
            cat = cli['_CAT']
        except:
            continue
        rez = {}
        try:
            rez.update({'branchCode':cli['_BRANCH_ID']})
        except:
            rez.update({'branchCode':cfg['target'][who]['BRANCH']['DEFAULT']})
        rez.update({'customerType':(clicat[cli['_CAT']])})
        groups = []
        anketa = {}
        anketa.update({'code':'ANKETA'})
        values = []
        for cod in (cli):
            if cod.startswith('_'):
                continue
            values.append({'code':cod,'value':(cli[cod] if not (cod in dates) else cli[cod].strftime('%d.%m.%Y'))})
        anketa.update({'values':values})
        groups.append(anketa)
        d = False
        if clicat[cli['_CAT']] == 'INDIVIDUAL':
            passport = {}
            passport.update({'code':'PASSPORT_DETAILS'})
            values = []
            for doc in (documents):
                try:
                    tmp = doc['EXT_ID']
                except:
                    continue
                if doc['EXT_ID'] == cli['EXT_ID']:
                    for cod in (doc):
                        if cod.startswith('_') or cod == 'EXT_ID':
                            continue
                        d = True
                        values.append({'code':cod,('rowId' if (cod in sprav) else 'value'):(doc[cod] if not (cod in dates) else doc[cod].strftime('%d.%m.%Y'))})
            passport.update({'values':values})
            if d:
                groups.append(passport)

        recvisit = {}
        recvisit.update({'code':'ADDRESS'})
        rows = []
        for dop in (dops):
            if dop['EXT_ID'] == cli['EXT_ID']:
                values = []
                if dop['_REC'].startswith('adr_'):
                    values.append({'code':'ADD_TYPE','rowId':rowid[dop['_REC']]})
                    values.append({'code':'ADD_FULL','value':dop['_VAL']})
                    if len(values) > 0:
                        rows.append({'values':values})

        if len(rows) > 0:
            recvisit.update({'rows':rows})
            groups.append(recvisit)

        recvisit = {}
        recvisit.update({'code':'TELEPHONE'})
        rows = []
        for dop in (dops):
            if dop['EXT_ID'] == cli['EXT_ID']:
                values = []
                if dop['_REC'].startswith('tel_'):
                    values.append({'code':'PHONE_TYPE','rowId':rowid[dop['_REC']]})
                    values.append({'code':'MOBILE_CONTACT','value':dop['_VAL']})
                    if len(values) > 0:
                        rows.append({'values':values})

        if len(rows) > 0:
            recvisit.update({'rows':rows})
            groups.append(recvisit)

        if who == 'ФПК':
            recvisit = {}
            recvisit.update({'code':'PORTFOLIO'})
            rows = []
            values = []
            values.append({'code':'PORTFOLIO_CODE','rowId':portfolio[portf]})
            values.append({'code':'EXT_CODE','value':client['EXT_ID']})
            rows.append({'values':values})
            recvisit.update({'rows':rows})
            groups.append(recvisit)

        rez.update({'groups':groups})
        printb(rez)
        if not test:
            req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=rez)
            print('<---CLI----<<<<<<<<<',req.status_code,req.reason)
            if (req.status_code != requests.codes.ok):
                print(req.text)

if __name__ == '__main__':
    main()
