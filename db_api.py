#!/python
import sys,os
from datetime import *
import json,re

def config(fname):
    if os.path.exists(fname):
        with open(fname,'r',encoding='utf-8') as f:
            try:
                cfg = json.load(f)
            except IOError:
                print ("Не могу прочитать файл конфигурации: ",fname)
                return None
    else:
        print("Файл конфигурации не найден: ",fname)
        sys.exit(1)
        return None
    return cfg


def months(d1,d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month
    
class _period(object):
    def __init__(self,**kwargs):
        for attribute,value in kwargs.items():
            setattr(self,attribute,value)
    def years(self):
        return self.date1.year - self.date2.year
    def months(self):
        return (self.date1.year - self.date2.year) * 12 + self.date1.month - self.date2.month
    def days(self):
        if self.days30:
            rez = 0
            for d in (self.date1 - timedelta(n) for n in range(int((self.date1 - self.date2).days + 1))):
                rez += 1
                if ((d - self.date2).days != 0) and (d.day == 1):
                    rez = rez + 30 - int((d - timedelta(days=1)).day)
            return rez
        else:
            return (self.date1 - self.date2).days
                
def prep_txt(txt):
    txt = txt.replace('"',' ')
    txt = txt.replace('    ',' ')
    txt = txt.replace('   ',' ')
    txt = txt.replace('  ',' ')
    txt = txt.replace('  ',' ')
    txt = txt.replace('; ',';')
    txt = txt.replace(' ;',';')
    return txt

def printb(*objects,sep=' ',end='\n',file=sys.stdout):
    enc = file.encoding
    if enc == 'UTF-8':
        print(*objects,sep=sep,end=end,file=file)
    else:
        f = lambda obj: str(obj).encode(enc,errors='backslashreplace').decode(enc)
        print(*map(f,objects),sep=sep,end=end,file=file)

def install(package):
    """ Install packages """
    import subprocess
    rez = subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def connect(params):
    """ Connect to database server """
    conn = None
    prm = {'type':'','host':'','database':'','jdbc_class':'','jdbc_path':'','url':'','user':'','password':'','driver':''}
    for i in range(len(prm)):
        try:
            prm[list(prm.keys())[i]] = params[list(prm.keys())[i]]
        except:
            prm[list(prm.keys())[i]] = ''
    if prm['type'] == 'pgsql':
        conn = connect_pgsql(prm['host'],prm['database'],prm['user'],prm['password'])
    elif prm['type'] == 'jdbc':
        conn = connect_jdbc(prm['jdbc_class'],prm['url'],prm['user'],prm['password'],prm['jdbc_path'])
    elif prm['type'] == 'odbc':
        conn = connect_odbc(prm['driver'])
    return conn

def connect_pgsql(host,base,user,pasw):
    """ Connect to the PostgreSQL database server """
    install('psycopg2')
    import psycopg2
    conn = None
    try:
        conn = psycopg2.connect(host=host,database=base,user=user,password=pasw)
    except (Exception,RuntimeError) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def connect_jdbc(jdbc_class,url,user,pwd,jdbc_path):
    """ Connect to the JDBC database server """
    install('jaydebeapi')
    import jaydebeapi as jdbc
    conn = None
    try:
        conn = jdbc.connect(jdbc_class,[url,user,pwd],jdbc_path)
        conn.jconn.setAutoCommit(True)
    except (Exception,RuntimeError) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def connect_odbc(drv):
    """ Connect to the ODBC database server """
    install('pyodbc')
    import pyodbc
    conn = None
    d = ''
    driver = drv.split(';',1)
    try:
        d = re.search('{(.+?)}',driver[0]).group(1)
    except AttributeError:
        print('No driver info!')
        sys.exit(1)
    try:
        for x in pyodbc.drivers():
            if d in x:
                drv = x
                break
        conn = pyodbc.connect('DRIVER={' + drv + '};' + driver[1],autocommit=True)
    except (Exception,RuntimeError) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def query(conn,xquery):
    """ Query data from table """
    cur = conn.cursor()
    try:
        cur.execute(xquery)
    except (Exception,RuntimeError) as error:
        print(error)
    return cur

def insert(conn,tab,fld,val):
    """ Insert data into table """
    try:
        cur = conn.cursor()
        sql = "INSERT INTO " + tab + "(" + fld + ") VALUES(" + val + ");"
        cur.execute(sql)
        cur.close()
    except (Exception,RuntimeError) as error:
        print(error)
