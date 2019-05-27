#!/python

import xlsxwriter
import json
import re
import sys
import os
import time
import progressbar
from db_api import *

def main():
    fname = 'db2xlsx.json'
    if len(sys.argv) > 1 and not ((sys.argv[1] == "") or (sys.argv[1] is None)):
        fname = sys.argv[1]
    if os.path.exists(fname):
        with open(fname,'r',encoding='utf-8') as f:
            try:
                cfg = json.load(f)
            except IOError:
                print ("Could not read file:",fname)
                sys.exit(1)
    else:
        print("File not exist:",fname)
        sys.exit(1)

    conn = connect(cfg['source'])
    workbook = xlsxwriter.Workbook(cfg["export"])

    cell_header = workbook.add_format({'bold':True,'border':6,'align':'center','valign':'vcenter','bg_color':'#DDDDDD','text_wrap':True})

    types = {}
    for t in (cfg['types']):
        f = workbook.add_format()
        f.set_num_format(cfg['types'][t])
        types.update({t:f})

    k = 1
    with progressbar.ProgressBar(max_value=len(cfg["querys"])) as bar:
        for q in (cfg["querys"]):
            cnames = re.split(';',q["colons"])
            ctypes = re.split(';',q["type"])
            cwidth = re.split(';',q["width"])
            worksheet = workbook.add_worksheet(q["name"])
            worksheet.set_row(0,50)
            cur = query(conn,q["query"])
            #cnames = [desc[0] for desc in cur.description]
            tab = cur.fetchall()
            y = 1
            x = 0
            for c in (cnames):
                worksheet.set_column(x,x,int(cwidth[x]))
                worksheet.write(0,x,c,cell_header)
                x += 1
            for r in (tab):
                x = 0
                for c in (cnames):
                    worksheet.write(y,x,r[x],types[ctypes[x]])
                    x += 1
                y += 1
            cur.close()
            bar.update(k)
            k += 1


    workbook.close()
    if conn is not None:
        conn.close()

if __name__ == '__main__':
    main()
