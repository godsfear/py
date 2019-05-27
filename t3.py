#!/python

#from db_api import *

if __name__ == '__main__':
    j = {}
    j.update({'1':'one'})
    j.update({'2':'two'})
    j.update({'0':'zero'})
    for e in (j):
        print(e)
    """conn = connect({'driver':r'DRIVER={Microsoft Excel Driver};DBQ=c:\Users\ddegtyaryov\Documents\work\py\out111.xls','type':'odbc'})
    cur = query(conn,"SELECT loan.[ID ЗАЙМА],loan.[№ ДОГОВОРА],loan.[СУММА ВЫДАЧИ],loan.[ID КЛИЕНТА],cust.[ЗНАЧЕНИЕ] FROM [ЗАЙМ$] AS loan INNER JOIN [КЛИЕНТ$] AS cust ON (loan.[ID КЛИЕНТА] = cust.[ID КЛИЕНТА] AND cust.[КОД РЕКВИЗИА] = 'IDN');")
    columns = [desc[0] for desc in cur.description]
    print(columns)
    tab = cur.fetchall()
    for row in tab:
        print(row)
    cur.close()
    conn.close()"""
