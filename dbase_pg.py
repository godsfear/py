#!/python
import psycopg2

def connect(host,base,user,pasw):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        conn = psycopg2.connect(host=host,database=base,user=user,password=pasw)
    except (Exception,psycopg2.Error) as error:
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
