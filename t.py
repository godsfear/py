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
    """who = 'МФО'
    cfg = config('migration.json')
    conn = connect(cfg[who])"""
    """r = '829e2c4d54674cb511e7e1670843bcda'
    z = '-'.join([r[24:],r[20:24],r[16:20],r[0:4],r[4:16]])
    print(z)"""
    
    path = "z:\\akim\\ФПК\\Цесна\\Выгрузка\\Таблица с информацией\\График"
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
                print(nom,';',str(row[2]).replace(' ',''),';',str(row[4]).replace(' ',''),';',str(row[5]).replace(' ',''),';',str(row[idx]).replace(' ',''))
    
    
if __name__ == '__main__':
    main()
