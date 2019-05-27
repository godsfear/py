#!/python

from xfuncs import *
from loanconv import *

def main():
    commit = True
    who = 'ФПК_ПРОД'
    cfg = config('migration.json')
    conn = connect(cfg[who])
    codes = {
        'number':'number',
        'rate':'rate'
    }
    log = open('_to_conv.log',"w+",encoding='utf-8')
    to_conv = txt2dict('_to_conv.csv',codes,[],'',['rate'],[],[],'"',';')
    for conv in to_conv:
        conv['number'] = conv['number'].replace('ГКС','').replace('№','').replace(' ','')
        qry = "SELECT agr.id,TO_DATE(ext.value,'DD.MM.YYYY') FROM common.agreement AS agr JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') WHERE agr.agreement_number = '" + conv['number'] + "'"
        cur = query(conn,qry)
        tab = cur.fetchall()
        num = conv['number']
        if len(tab) == 0:
            qry = "SELECT agr.id,TO_DATE(ext.value,'DD.MM.YYYY') FROM common.agreement AS agr JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') WHERE agr.agreement_number = '" + conv['number'].replace('А','A') + "'"
            cur = query(conn,qry)
            tab = cur.fetchall()
            num = conv['number'].replace('А','A')
            if len(tab) == 0:
                qry = "SELECT agr.id,TO_DATE(ext.value,'DD.MM.YYYY') FROM common.agreement AS agr JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') WHERE agr.agreement_number = '" + conv['number'].replace('A','А') + "'"
                cur = query(conn,qry)
                tab = cur.fetchall()
                num = conv['number'].replace('A','А')
                if len(tab) == 0:
                    log.write(conv['number'] + '\n')
                    continue

        for r in tab:
            print(num,'---------------------------------------------------------')
            lconv(who,num,conv['rate'],r[1],True,commit)

if __name__ == '__main__':
    main()
