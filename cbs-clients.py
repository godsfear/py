#!/python

import datetime
import sys,locale,os
import json
import re
import requests
from xfuncs import *

def main():
    test = True
    fname_cli = '8_Клиент_ЮЛ1.csv'
    fname_doc = ''
    fname_dop = ''
    who = 'SECURITY'
    portf = 'TSESNA'

    cfg = config('migration.json')
    conn = connect(cfg[who])

    head = {'Content-type':'application/json','Accept':'text/plain'}
    codes = {
        'P_SID': 'EXT_ID',
        'P_SID': 'EXT_CODE',
        'LONGNAME': 'J_NAME',
        'NAME_LEGAL': 'J_NAME',
        'NAME': 'J_SHORT_NAME',
        'NAME_SHORT': 'J_SHORT_NAME',
        'CLITAXCODE': 'IDN',
        'TAX_NUMBER': 'IDN',
        'CLIRESIDFL': 'RESIDENT',
        'RESIDENT_FLAG': 'RESIDENT',
        'CLI_TYPE': 'customerType',
        'SECTOR_ID': 'customerType',
        '_branch':'branchCode',
        'branchcode':'branchCode',
        '_client_id':'EXT_ID',
        'ext_id':'EXT_CODE',
        '_last_name':'LNAME',
        'lname':'LNAME',
        '_first_name':'FNAME',
        'fname':'FNAME',
        '_patron':'MNAME',
        'mname':'MNAME',
        '_iin':'IDN',
        '_bin':'IDN',
        'bin_iin':'IDN',
        'rnn':'rnn',
        '_rezident':'RESIDENT',
        'resident':'RESIDENT',
        '_birth':'BIRTH',
        'birth':'BIRTH',
        '_full_name':'J_NAME',
        'j_name':'J_NAME',
        '_client_type':'customerType',
        'customertype':'customerType',
        '_number':'DOC_NO_PASS',
        'doc_no_pass':'DOC_NO_PASS',
        '_issue':'ISSUED_BY_PASS',
        'issued_by_pass':'ISSUED_BY_PASS',
        '_open_date':'ISSUE_DATE_PASS',
        'issue_date_pass':'ISSUE_DATE_PASS',
        '_end_date':'EXPIRY_PASS',
        'expiry_pass':'EXPIRY_PASS',
        '_type':'ID_DOC',
        '_recv':'_REC',
        '_value':'_VAL'
    }
    
    clicat = {'CL_ORG':'JURIDICAL','CL_PRIV':'INDIVIDUAL','0':'INDIVIDUAL','1':'JURIDICAL','Юридические лица':'JURIDICAL','7':'JURIDICAL'}
    resident = {'0':'true','1':'false'}

    sprav = ['ID_DOC']
    dates = ['BIRTH','ISSUE_DATE_PASS','EXPIRY_PASS']
    bools = []
    decimals = []
    skip = ['NULL']
    unquot = ['J_NAME','J_SHORT_NAME']

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

    templ = config('cbs-templates.json')
    url = cfg['target'][who]['URL'] + 'create-customer'
    clients = txt2dict(fname_cli,codes,dates,'%Y-%m-%d',decimals,bools,skip,unquot,'\'',';')
    for cli in clients:
        if not any(k in cli.keys() for k in ('EXT_ID','EXT_CODE')):
            continue
        if not 'EXT_ID' in cli.keys():
            cli.update({'EXT_ID':cli['EXT_CODE']})
        inn = 0
        try:
           inn = int(cli['IDN'])
        except:
            pass
        if inn == 0:
            if 'rnn' in cli.keys() and not (cli['rnn'] is None) and not (cli['rnn'] in ['','None','000000000000','0']):
                cli['IDN'] = cli['rnn']
            else:
                cli['IDN'] = ''
        if 'branchCode' in cli.keys():
            cli['branchCode'] = cfg['target'][who]['BRANCH'][cli['branchCode']]
        else:
            cli['branchCode'] = cfg['target'][who]['BRANCH']['DEFAULT']
        if 'J_NAME' in cli.keys() and not ('J_SHORT_NAME' in cli.keys()):
            cli.update({'J_SHORT_NAME':cli['J_NAME']})
        if not ('RESIDENT' in cli.keys()):
            cli.update({'RESIDENT':'true'})
        else:
            cli['RESIDENT'] = resident[cli['RESIDENT']]
        if 'customerType' in cli.keys():
            cli['customerType'] = clicat[cli['customerType']]
        if fname_doc == '' and not 'ID_DOC' in cli.keys() and cli['customerType'] == 'INDIVIDUAL':
            cli['ID_DOC'] = iddoc['Удостоверение личности РК']
        client = copy.deepcopy(templ['CLIENT']['MAIN'])
        cbs_fill(client,cli,'%d.%m.%Y')
        cli.update({'request':client})

    documents = txt2dict(fname_doc,codes,dates,'%Y-%m-%d',decimals,bools,skip,unquot,'"',',')
    for doc in documents:
        if not 'EXT_ID' in doc.keys():
            continue
        if 'ID_DOC' in doc.keys():
            doc['ID_DOC'] = iddoc[doc['ID_DOC']]
        else:
            continue
        for cli in clients:
            if doc['EXT_ID'] != cli['EXT_ID']:
                continue
            cbs_fill(cli['request'],doc,'%d.%m.%Y')

    dops = txt2dict(fname_dop,codes,dates,'%Y-%m-%d',decimals,bools,skip,unquot,'"',',')
    for dop in dops:
        if not 'EXT_ID' in dop.keys():
            continue
        try:
            dop['_REC'] = recv[dop['_REC']]
        except:
            pass
        for cli in clients:
            if dop['EXT_ID'] != cli['EXT_ID']:
                continue
            if dop['_REC'].startswith('adr_'):
                dop.update({'ADD_TYPE':rowid[dop['_REC']]})
                dop.update({'ADD_FULL':dop['_VAL']})
                client_grp = copy.deepcopy(templ['CLIENT']['GROUPS']['ADDRESS'])
                cbs_group(cli['request'],'ADDRESS',client_grp,dop,'%d.%m.%Y')
            if dop['_REC'].startswith('tel_'):
                dop.update({'PHONE_TYPE':rowid[dop['_REC']]})
                dop.update({'MOBILE_CONTACT':dop['_VAL']})
                client_grp = copy.deepcopy(templ['CLIENT']['GROUPS']['TELEPHONE'])
                cbs_group(cli['request'],'TELEPHONE',client_grp,dop,'%d.%m.%Y')

    for client in clients:
        if not 'request' in client.keys():
            continue
        if 'EXT_CODE' in client.keys():
            qry = "SELECT val.row_id FROM mdm.dictionary_fields AS fld JOIN customers.customer_extended_fields AS ext ON ext.mdm_dict_field_id = fld.id AND ext.code = 'PORTFOLIO_CODE' JOIN mdm.field_values AS val ON val.field_id = fld.id AND val.value = '" + portf + "';"
            cur = query(conn,qry)
            tab = cur.fetchall()
            for r in tab:
                portfolio = r[0]
            client_grp = copy.deepcopy(templ['CLIENT']['GROUPS']['PORTFOLIO'])
            cbs_group(client['request'],'PORTFOLIO',client_grp,{'PORTFOLIO_CODE':portfolio,'EXT_CODE':client['EXT_CODE']},'%d.%m.%Y')

        client['request'] = cbs_clear(client['request'])
        if test:
            print(client['request'])
        else:
            print(client['request'])
            req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=client['request'])
            print('<---CLI----<<<<<<<<<',req.status_code,req.reason)
            if (req.status_code != requests.codes.ok):
                print(req.text)

if __name__ == '__main__':
    main()
