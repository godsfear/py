#!/python

from tkinter import *
import pass2
import psycopg2
import xlsxwriter
import datetime

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

def query(cur,xquery):
    """ Query data from table """
    try:
        cur.execute(xquery)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return cur

if __name__ == '__main__':
    """conn = connect("172.16.137.9","executor_fpl","db_core","db_core")"""
    conn = connect("10.128.128.13","executor","Admin","cbs2016")

    workbook = xlsxwriter.Workbook("cbs2xls.xlsx")
    cell_dec = workbook.add_format()
    cell_dec.set_num_format("### ### ### ##0.00;[Red]-### ### ### ##0.00")
    cell_dat = workbook.add_format()
    cell_dat.set_num_format("dd.mm.yyyy")
    cell_int = workbook.add_format()
    cell_int.set_num_format("### ### ### ##0;[Red]-### ### ### ##0")
    cell_inn = workbook.add_format()
    cell_inn.set_num_format("000000000000")

    worksheet = workbook.add_worksheet("ЗАЙМ")
    cur = query(conn.cursor(),"CREATE TEMP TABLE xloans AS SELECT agre.id,agreement_number,agre.date_start,agre.date_end,brn.code AS branch_code,customer_id,prod.name,customer_type_id,loans.amount,loans.repayment_interest_date,loans.repayment_maindebt_date,loans.accrual_basis_id,loans.accrual_method_id,cur.code,loans.maindebt_repayment_freq_id,loans.interest_repayment_freq_id FROM common.agreement AS agre JOIN customers.customer AS cust ON agre.customer_id = cust.id JOIN common.branch AS brn ON agre.branch_id = brn.id JOIN loans.loan_agreement AS loans ON agre.id = loans.agreement_id JOIN common.product AS prod ON agre.product_id = prod.id JOIN common.currency AS cur ON loans.currency_id = cur.id WHERE agre.status = 'ACTIVE';SELECT * FROM xloans;")
    cnames = [desc[0] for desc in cur.description]
    tab = cur.fetchall()
    row = 1
    col = 0
    for c in (cnames):
        worksheet.write(0,col,c)
        col += 1
    col = 0
    for c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16 in (tab):
        worksheet.write(row,col,c1,cell_int)
        worksheet.write(row,col + 1,c2)
        worksheet.write(row,col + 2,c3,cell_dat)
        worksheet.write(row,col + 3,c4,cell_dat)
        worksheet.write(row,col + 4,c5)
        worksheet.write(row,col + 5,c6,cell_int)
        worksheet.write(row,col + 6,c7)
        worksheet.write(row,col + 7,c8)
        worksheet.write(row,col + 8,c9,cell_dec)
        worksheet.write(row,col + 9,c10,cell_int)
        worksheet.write(row,col + 10,c11,cell_int)
        worksheet.write(row,col + 11,c12,cell_int)
        worksheet.write(row,col + 12,c13,cell_int)
        worksheet.write(row,col + 13,c14)
        worksheet.write(row,col + 14,c15,cell_int)
        worksheet.write(row,col + 15,c16,cell_int)
        row += 1
    cur.close()

    worksheet = workbook.add_worksheet("КЛИЕНТ")
    cur = query(conn.cursor(),"SELECT cust.id,cust.customer_type_id,extfld.code,extfld.name,extval.row_id,extval.value FROM customers.customer AS cust JOIN (SELECT DISTINCT customer_id FROM xloans) AS ccc ON cust.id = ccc.customer_id JOIN customers.customer_extended_field_values AS extval ON extval.customer_id = cust.id JOIN customers.customer_extended_fields AS extfld ON extval.cust_ext_field_id = extfld.id")
    cnames = [desc[0] for desc in cur.description]
    tab = cur.fetchall()
    row = 1
    col = 0
    for c in (cnames):
        worksheet.write(0,col,c)
        col += 1
    col = 0
    for c1,c2,c3,c4,c5,c6 in (tab):
        worksheet.write(row,col,c1,cell_int)
        worksheet.write(row,col + 1,c2,cell_int)
        worksheet.write(row,col + 2,c3)
        worksheet.write(row,col + 3,c4)
        worksheet.write(row,col + 4,c5,cell_int)
        worksheet.write(row,col + 5,c6)
        row += 1
    cur.close()

    worksheet = workbook.add_worksheet("ЗАЙМ ДОП.ПАРАМ")
    cur = query(conn.cursor(),"SELECT agre.id,extfld.code,extfld.name,extval.row_id,extval.value FROM common.agreement AS agre JOIN (SELECT DISTINCT id FROM xloans) AS lll ON agre.id = lll.id JOIN common.agreement_extended_field_values AS extval ON agre.id = extval.agreement_id JOIN common.agreement_extended_fields AS extfld ON extval.agreement_ext_field_id = extfld.id")
    cnames = [desc[0] for desc in cur.description]
    tab = cur.fetchall()
    row = 1
    col = 0
    for c in (cnames):
        worksheet.write(0,col,c)
        col += 1
    col = 0
    for c1,c2,c3,c4,c5 in (tab):
        worksheet.write(row,col,c1,cell_int)
        worksheet.write(row,col + 1,c2)
        worksheet.write(row,col + 2,c3)
        worksheet.write(row,col + 3,c4,cell_int)
        worksheet.write(row,col + 4,c5)
        row += 1
    cur.close()

    worksheet = workbook.add_worksheet("ЗАЙМ СТАВКИ")
    cur = query(conn.cursor(),"SELECT agre.id,typ.code,typ.description,rate.start_date,rate.is_percent,cur.code,rate.value FROM common.agreement AS agre JOIN (SELECT DISTINCT id FROM xloans) AS lll ON agre.id = lll.id JOIN loans.loan_agreement AS la ON la.agreement_id = agre.id JOIN loans.loan_agreement_rp_value AS rate ON la.id = rate.loan_agreement_id JOIN common.currency AS cur ON rate.value_currency_id = cur.id JOIN loans.repayment_type AS typ ON rate.repayment_type_id = typ.id")
    cnames = [desc[0] for desc in cur.description]
    tab = cur.fetchall()
    row = 1
    col = 0
    for c in (cnames):
        worksheet.write(0,col,c)
        col += 1
    col = 0
    for c1,c2,c3,c4,c5,c6,c7 in (tab):
        worksheet.write(row,col,c1,cell_int)
        worksheet.write(row,col + 1,c2)
        worksheet.write(row,col + 2,c3)
        worksheet.write(row,col + 3,c4,cell_dat)
        worksheet.write(row,col + 4,c5)
        worksheet.write(row,col + 5,c6)
        worksheet.write(row,col + 6,c7,cell_dec)
        row += 1
    cur.close()

    worksheet = workbook.add_worksheet("ЗАЙМ ОСТАТКИ")
    cur = query(conn.cursor(),"SELECT agre.id,typ.code AS stype,typ.description,cur.code AS currency,sub.account_number,ost.balance FROM common.agreement AS agre JOIN (SELECT DISTINCT id FROM xloans) AS lll ON agre.id = lll.id JOIN accounts.subaccount AS sub ON sub.agreement_id = agre.id JOIN accounts.subaccount_type AS typ ON typ.id = sub.subaccount_type_id JOIN common.currency AS cur ON sub.currency_id = cur.id JOIN accounts.subaccount_turnover AS ost ON ost.subaccount_id = sub.id")
    cnames = [desc[0] for desc in cur.description]
    tab = cur.fetchall()
    row = 1
    col = 0
    for c in (cnames):
        worksheet.write(0,col,c)
        col += 1
    col = 0
    for c1,c2,c3,c4,c5,c6 in (tab):
        worksheet.write(row,col,c1,cell_int)
        worksheet.write(row,col + 1,c2)
        worksheet.write(row,col + 2,c3)
        worksheet.write(row,col + 3,c4)
        worksheet.write(row,col + 4,c5)
        worksheet.write(row,col + 5,c6,cell_dec)
        row += 1
    cur.close()

    worksheet = workbook.add_worksheet("ЗАЙМ ГРАФИК")
    cur = query(conn.cursor(),"SELECT agre.id,pay.target_date,pay.amount,pay.amount_principal_before,pay.amount_principal_after FROM common.agreement AS agre JOIN (SELECT DISTINCT id FROM xloans) AS lll ON agre.id = lll.id JOIN loans.loan_agreement AS la ON la.agreement_id = agre.id JOIN loans.loan_repayment_schedule AS rep ON la.id = rep.loan_agreement_id AND rep.type = 'TYPE_PAYMENTS' AND rep.status = 'ACTIVE' JOIN loans.loan_repayment_schedule_item AS pay ON pay.loan_repayment_schedule_id = rep.id JOIN loans.repayment_type AS subtyp ON pay.repayment_type_id = subtyp.id")
    cnames = [desc[0] for desc in cur.description]
    tab = cur.fetchall()
    row = 1
    col = 0
    for c in (cnames):
        worksheet.write(0,col,c)
        col += 1
    col = 0
    for c1,c2,c3,c4,c5 in (tab):
        worksheet.write(row,col,c1,cell_int)
        worksheet.write(row,col + 1,c2,cell_dat)
        worksheet.write(row,col + 2,c3,cell_dec)
        worksheet.write(row,col + 3,c4,cell_dec)
        worksheet.write(row,col + 4,c5,cell_dec)
        row += 1
    cur.close()

    workbook.close()
    if conn is not None:
        conn.close()
