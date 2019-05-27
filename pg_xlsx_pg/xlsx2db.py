#!/python
from openpyxl import load_workbook
import sys
import os
import json
import re
import time
import progressbar
import glob
from db_api import *

if __name__ == '__main__':
    fname = 'xlsx2db.json'
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
    conn = connect(cfg["destination"])
    if cfg["execute"] != "" and cfg["execute"] != None:
        if os.path.exists(cfg["execute"]):
            with open(cfg["execute"],'r',encoding='utf-8') as f:
                try:
                    query(conn,f.read())
                except IOError:
                    print ("Could not read file:",cfg["execute"])
                    sys.exit(1)
        else:
            print("File not exist:",cfg["execute"])
            sys.exit(1)
    k = 1
    with progressbar.ProgressBar(max_value=len(cfg["files"])) as bar:
        for xfile in (cfg["files"]):
            for fn in glob.glob(xfile["name"]):
                wb = load_workbook(fn)
                j = 0
                for row in (wb[xfile["query"]["page"]]):
                    j += 1
                    if j < xfile["begin_row"]:
                        continue
                    val = re.split(',',xfile["query"]["values"])
                    typ = re.split(',',xfile["query"]["type"])
                    i = 0
                    values = ""
                    for v in (val):
                        z = str(row[int(v) - 1].value)
                        if typ[i] == 'date':
                            z = z.split(" ")[0]
                        z.replace(",","\\,")
                        z.replace('"','\\"')
                        z.replace("'","\\'")
                        if typ[i] == 'varchar' or typ[i] == 'date':
                            z = "'" + z + "'"
                        z = z + "::" + typ[i]
                        if values == "":
                            values = z
                        else :
                            values += "," + z
                        i += 1
                    insert(conn,xfile["query"]["table"],xfile["query"]["fields"],values)
            bar.update(k)
            k += 1

    if conn is not None:
        conn.close()
