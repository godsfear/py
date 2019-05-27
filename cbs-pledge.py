#!/python

from xfuncs import *
import requests
import csv
import sys,locale,os
import json,re
install('lxml')
from lxml import etree

def main():
    test = True
    who = 'МФО'
    cfg = config('migration.json')
    conn = connect(cfg[who])

    codes = {
        "ext_code":"_LOAN_ID",
        "_loan_id":"_LOAN_ID",
        "_open_date":"dateStart",
        "_end_date":"_END_DATE",
        "agreement_number":"_LOAN_NUM",
        "_number":"EXT_COLL_ID",
        "cust_code":"code",
        "branch_id":"branchCode",
        "_descr":"description",
        "summa":"COLL_PAWN_COST",
        "summa2":"COLL_NOK_COST",
        "_fld1690":"COLL_REG_OFFICE",
        "_fld1691":"REG_CERT_COLL",
        "_fld1692":"COLL_IN_DOC_NO",
        "bin":"idn",
        "_object_code":"objectTypeCode",
        "_type":"objectTypeDescription",
        "_description":"title"
    }

    dates = ['dateStart','_END_DATE']
    bools = []
    decimals = []
    skip = ['NULL']
    closed = []

    types = {
        "автобетоносмеситель":"47eab7cb-c03d-11e6-80d5-005056",
        "автокран":"47eab7cb-c03d-11e6-80d5-005056",
        "Автомобили":"409e45ae-c03d-11e6-80d5-005056",
        "автомобиль":"409e45ae-c03d-11e6-80d5-005056",
        "автотранспорт":"409e45ae-c03d-11e6-80d5-005056",
        "прицеп":"47eab7cb-c03d-11e6-80d5-005056",
        "спецтехника":"47eab7cb-c03d-11e6-80d5-005056",
        "эксаватор":"47eab7cb-c03d-11e6-80d5-005056",
        "Жилая недвижимость":"6ee414cb-c12a-11e6-80d5-005056",
        "Квартира":"47eab7b2-c03d-11e6-80d5-005056",
        "Коммерческая недвижимость":"7fd4b786-c12a-11e6-80d5-005056"
    }

    head = {'Content-type':'application/json','Accept':'text/plain'}

    fname_zal = 'zalog.csv'
    zalogs = txt2dict(fname_zal,codes,dates,'%Y-%m-%d',decimals,bools,skip,'"',",")

    xzalog = []
    for zalog in zalogs:
        try:
            tmp = zalog['_LOAN_ID']
        except:
            continue
        if zalog['_END_DATE'] <= datetime.strptime(cfg['target'][who]['MDATE'],'%Y-%m-%d'):
            continue
        qry = "SELECT agr.agreement_number,cus.code,cext.value,brn.code FROM common.agreement_extended_field_values AS ext JOIN common.agreement AS agr ON agr.id = ext.agreement_id JOIN common.branch AS brn ON brn.id = agr.branch_id JOIN customers.customer AS cus ON cus.id = agr.customer_id JOIN customers.customer_extended_field_values AS cext ON cext.customer_id = cus.id AND cext.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'IDN') WHERE ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'EXT_ID') AND ext.value = '" + zalog['_LOAN_ID'] + "'"
        cur = query(conn,qry)
        tab = cur.fetchall()
        if len(tab) == 0:
            qry = "SELECT agr.agreement_number,cus.code,cext.value,brn.code FROM common.agreement AS agr JOIN common.branch AS brn ON brn.id = agr.branch_id JOIN customers.customer AS cus ON cus.id = agr.customer_id JOIN customers.customer_extended_field_values AS cext ON cext.customer_id = cus.id AND cext.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'IDN') WHERE agr.agreement_number = '" + zalog['_LOAN_ID'] + "'"
            cur = query(conn,qry)
            tab = cur.fetchall()
            if len(tab) == 0:
                continue
        loans = []
        for r in tab:
            loans.append(r[0])
            zalog.update({'code':r[1]})
            zalog.update({'idn':r[2]})
            zalog.update({'branchCode':r[3]})
        zalog.update({'agreementNumbers':loans})
        try:
            tmp = zalog['currency']
        except:
            zalog.update({'currency':'KZT'})
        try:
            tmp = zalog['description']
        except:
            try:
                zalog.update({'description':zalog['title']})
            except:
                pass
        try:
            tmp = zalog['period']
        except:
            zalog.update({'period':str(months(zalog['_END_DATE'],zalog['dateStart']))})
            zalog.update({'periodUnit':'month'})
        for field in zalog.copy():
            if field == 'EXT_COLL_ID':
                zalog['EXT_COLL_ID'] = zalog['EXT_COLL_ID'].replace(',','')
            if field == 'objectTypeDescription':
                try:
                    tmp = zalog['objectTypeCode']
                except:
                    zalog.update({'objectTypeCode':types[zalog[field]]})
            if field == 'COLL_PAWN_COST':
                try:
                    tmp = zalog['amount']
                except:
                    zalog.update({'amount':zalog[field]})
        if not ((zalog['EXT_COLL_ID'] + ',' + zalog['code']) in xzalog):
            xzalog.append(zalog['EXT_COLL_ID'] + ',' + zalog['code'])
    
    _zalogs = []
    for key in xzalog:
        key = re.split(',',key)
        zalog = {}
        loans = []
        for _z in zalogs:
            if not all(k in _z.keys() for k in ('_LOAN_ID','code','EXT_COLL_ID','agreementNumbers')):
                continue
            if len(_z['agreementNumbers']) == 0:
                continue
            if not (_z['EXT_COLL_ID'] == key[0] and _z['code'] == key[1]):
                continue
            for field in _z:
                if field != 'agreementNumbers':
                    zalog.update({field:_z[field]})
                else:
                    for _a in _z['agreementNumbers']:
                        if not (_a in loans):
                            loans.append(_a)
            zalog.update({'agreementNumbers':loans})
        if len(zalog) > 0:
            _zalogs.append(zalog)

    templ = config('cbs-templates.json')
    for _zal in _zalogs:
        pledge = copy.deepcopy(templ['PLEDGE']['OBJECT']['MAIN'])
        cbs_fill(pledge,_zal,'%d.%m.%Y')
        pledge_grp = copy.deepcopy(templ['PLEDGE']['OBJECT']['GROUPS'])
        for elem in pledge_grp:
            cbs_group(pledge,elem,pledge_grp[elem],_zal,'%d.%m.%Y')
        pledge = cbs_clear(pledge)
        if test:
            _zal.update({'pawnObjectId':'1'})
            print(pledge)
        else:
            print('REQUEST (OBJECT):',datetime.now())
            print(pledge)
            url = cfg['target'][who]['URL'] + 'pawn-object-create'
            req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=pledge)
            print('RESULT (OBJECT):',req.status_code,req.reason)
            if (req.status_code != requests.codes.ok):
                print(req.text)
                return
            doc = etree.XML(req.text)
            _zal.update({'pawnObjectId':doc.xpath(r'/data/id')[0].text})

        agreement = copy.deepcopy(templ['PLEDGE']['AGREEMENT'])
        cbs_fill(agreement,_zal,'%d.%m.%Y')
        agreement = cbs_clear(agreement)
        if test:
            print(agreement)
        else:
            print('REQUEST (AGREEMENT):',datetime.now())
            print(agreement)
            url = cfg['target'][who]['URL'] + 'create-pawn-agreement'
            req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=agreement)
            print('RESULT (AGREEMENT):',req.status_code,req.reason)
            if (req.status_code != requests.codes.ok):
                print(req.text)
                return

if __name__ == '__main__':
    main()
