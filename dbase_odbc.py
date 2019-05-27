#!/python
import pyodbc
import sys
import re

def connect(drv):
    """ Connect to the PostgreSQL database server """
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
            if x.startswith(d):
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
    except (Exception, pyodbc.DatabaseError) as error:
        print(error)
    return cur

def insert(conn,tab,fld,val):
    """ Insert data into table """
    try:
        cur = conn.cursor()
        sql = "INSERT INTO " + tab + "(" + fld + ") VALUES(" + val + ");COMMIT;"
        cur.execute(sql)
        cur.close()
    except (Exception,pyodbc.DatabaseError) as error:
        print(error)
