#!/python
import sys
import re

def connect(params):
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
    import psycopg2
    conn = None
    try:
        conn = psycopg2.connect(host=host,database=base,user=user,password=pasw)
    except (Exception,psycopg2.Error) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def connect_jdbc(jdbc_class,url,user,pwd,jdbc_path):
    """ Connect to the JDBC database server """
    import jaydebeapi as jdbc
    conn = None
    try:
        conn = jdbc.connect(jdbc_class,[url,user,pwd],jdbc_path)
        conn.jconn.setAutoCommit(True)
    except (Exception,jdbc.Error) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def connect_odbc(drv):
    """ Connect to the ODBC database server """
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
    except (Exception,pyodbc.Error) as error:
        print('Unable to connect!\n')
        print(error)
        sys.exit(1)
    return conn

def query(conn,xquery):
    """ Query data from table """
    cur = conn.cursor()
    try:
        cur.execute(xquery)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return cur

def insert(conn,tab,fld,val):
    """ Insert data into table """
    try:
        cur = conn.cursor()
        sql = "INSERT INTO " + tab + "(" + fld + ") VALUES(" + val + ");COMMIT;"
        cur.execute(sql)
        cur.close()
    except (Exception,psycopg2.DatabaseError) as error:
        print(error)
