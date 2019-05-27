#!/python
import sys
import re

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
    except (Exception,Error) as error:
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
    except (Exception,Error) as error:
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
    except (Exception,Error) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def query(conn,xquery):
    """ Query data from table """
    cur = conn.cursor()
    try:
        cur.execute(xquery)
    except (Exception,Error) as error:
        print(error)
    return cur

def insert(conn,tab,fld,val):
    """ Insert data into table """
    try:
        cur = conn.cursor()
        sql = "INSERT INTO " + tab + "(" + fld + ") VALUES(" + val + ");COMMIT;"
        cur.execute(sql)
        cur.close()
    except (Exception,Error) as error:
        print(error)
