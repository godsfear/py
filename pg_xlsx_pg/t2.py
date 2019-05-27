#!/python
import sys
import os
import re
import json

if __name__ == '__main__':
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
    print(cfg['source'])
