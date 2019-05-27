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
import urllib

def main():
    """who = 'МФО'
    cfg = config('migration.json')
    conn = connect(cfg[who])"""
    """r = '829e2c4d54674cb511e7e1670843bcda'
    z = '-'.join([r[24:],r[20:24],r[16:20],r[0:4],r[4:16]])
    print(z)"""
    
    #print(str_meta_comp('Дегтярёв','Диктяров'))
    
    """for lon in ['5/003-2006']:
        ost2graf(conn,lon,'INTRST_OVRD',True)"""
    print(uqt('"Товарищество с ограниченной ответственностью ""Ишим-Астык"""','"'))
    
if __name__ == '__main__':
    main()
