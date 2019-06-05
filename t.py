#!/python

from xfuncs import *
import requests
import sys
from cbs import *
import time
import uuid
from xclasses import *
import cyrtranslit
from metaphone import doublemetaphone
import glob
from openpyxl import load_workbook
import xlrd

def main():
    who = 'SECURITY'
    cfg = config('migration.json')
    conn = connect(cfg[who])
    """r = '829e2c4d54674cb511e7e1670843bcda'
    z = '-'.join([r[24:],r[20:24],r[16:20],r[0:4],r[4:16]])
    print(z)"""
    
    """path = "z:\\akim\\ФПК\\Цесна\\Выгрузка\\Таблица с информацией\\График"
    files = [f for f in glob.glob(path + "**/*.xlsx", recursive=True)]

    for f in files:
        wb = load_workbook(f)
        ok = False
        try:
            wbl = wb['Отчет (лист 1)']
        except:
            print(f,'no_list')
            continue
        idx = None
        for i,row in enumerate(wbl):
            if i == 0 and str(row[0].value) != 'Приложение №1':
                print(f,'bad')
                break
            if i == 1:
                nom = str(row[0].value).replace('к договору банковского займа №  ','').strip().split(' ')[0]
            if i < 20:
                continue
            if str(row[0].value).strip()[:5] == '№ п/п':
                if str(row[7].value).strip()[:7] == 'Остаток':
                    idx = 7
                else:
                    idx = 8
            if str(row[0].value).strip() == '1' and str(row[1].value).strip() == '2':
                ok = True
                continue
            if ok and str(row[1].value).strip()[:5] == 'Итого':
                break
            if ok:
                if str(row[4].value).replace(' ','') == '' and str(row[5].value).replace(' ','') == '':
                    continue
                print(nom,';',str(row[2].value).replace(' ',''),';',str(row[4].value).replace(' ',''),';',str(row[5].value).replace(' ',''),';',str(row[idx].value).replace(' ',''))
            
    files = [f for f in glob.glob(path + "**/*.xls", recursive=True)]
    for f in files:
        wb = xlrd.open_workbook(f)
        ok = False
        try:
            wbl = wb.sheet_by_name('Отчет (лист 1)')
        except:
            print(f,'no_list')
            continue
        idx = None
        for i in range(wbl.nrows):
            row = wbl.row_values(i)
            if i == 0 and str(row[0]) != 'Приложение №1':
                print(f,'bad')
                break
            if i == 1:
                nom = str(row[0]).replace('к договору банковского займа №  ','').strip().split(' ')[0]
            if i < 20:
                continue
            if str(row[0]).strip()[:5] == '№ п/п':
                if str(row[7]).strip()[:7] == 'Остаток':
                    idx = 7
                else:
                    idx = 8
            if str(row[0]).strip() == '1' and str(row[1]).strip() == '2':
                ok = True
                continue
            if ok and str(row[1]).strip()[:5] == 'Итого':
                break
            if ok:
                if str(row[4]).replace(' ','') == '' and str(row[5]).replace(' ','') == '':
                    continue
                print(nom,';',str(row[2]).replace(' ',''),';',str(row[4]).replace(' ',''),';',str(row[5]).replace(' ',''),';',str(row[idx]).replace(' ',''))"""

    """clients = txt2dict('ast_cli_2.csv',{'id':'EXT_ID','inn':'idn','resident':'resident','name':'J_NAME'},[],'%Y-%m-%d',[],[],[],['name'],'"',';')
    for cli in clients:
        cust = "SELECT ext.customer_id FROM customers.customer AS cus JOIN customers.customer_extended_field_values AS ext ON ext.customer_id = cus.id AND ext.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'IDN') AND ext.value = '" + cli['idn'] + "'"
        cur = query(conn,cust)
        tab = cur.fetchall()
        for r in tab:
            qry = "INSERT INTO customers.customer_extended_field_values (id,status,date_start,value,customer_id,cust_ext_field_id) SELECT NEXTVAL('customers.customer_extended_field_values_seq_id') AS id,'ACTIVE' AS status,'2019-06-03'::DATE AS date_start,'" + cli['J_NAME'] + "' AS value," + str(r[0]) + " AS customer_id,(SELECT id FROM customers.customer_extended_fields WHERE code = 'J_NAME')"
            cur = query(conn,qry)
            qry = "INSERT INTO customers.customer_extended_field_values (id,status,date_start,value,customer_id,cust_ext_field_id) SELECT NEXTVAL('customers.customer_extended_field_values_seq_id') AS id,'ACTIVE' AS status,'2019-06-03'::DATE AS date_start,'" + cli['J_NAME'] + "' AS value," + str(r[0]) + " AS customer_id,(SELECT id FROM customers.customer_extended_fields WHERE code = 'J_SHORT_NAME')"
            cur = query(conn,qry)
            qry = "UPDATE customers.customer_extended_field_values SET value = " + cli['J_NAME'] + " WHERE cust_ext_field_id = 5 AND customer_id = " + str(r[0])
            print(qry)

    conn.commit()"""
    
    """import subprocess,os
    rez = subprocess.Popen(["pg_dump","-s","-F","p","-h","172.16.137.3","-U","db_core","-d","executor","-f","fffffff.sql"],stdout = subprocess.PIPE,universal_newlines = True,env = dict(os.environ,PGPASSWORD="df464DFL360aleKKfw3516KJ3KL"))
    for line in rez.stdout:
        sys.stdout.write(line)"""

if __name__ == '__main__':
    main()
