from xfuncs import *
import xlsxwriter
from datetime import *
from xclasses import *
import shutil

def acct1c(type,acct):
    accs = {
        '3390.22':'1010'
    }
    if acct in accs.keys():
        acct = accs[acct]
        if acct == '1010' and type in ['JURIDICAL']:
            acct = '1030'
    return acct

def checkarray(arr,key,val):
    for elm in arr:
        if elm[key] == val:
            return True
    return False

def main():
    test = False
    who = 'МФО'
    cfg = Config('cbs-exp-1c.json')
    print('Подключение к ' + who + '...')
    conn = connect(cfg.data[who])
    print('Успешно...')
    if str2date(cfg.data['PARAMS']['SEQUENCE_DATE']) != datetime.today().date():
        cfg.data['PARAMS']['SEQUENCE_DATE'] = datetime.today().date().strftime('%Y-%m-%d')
        cfg.data['PARAMS']['SEQUENCE'] = 0
    idate = add2date(datetime.today().date(),days=cfg.data['PARAMS']['DATE_START']).strftime('%Y-%m-%d')
    groups = {}
    for a in ['217025','217029','128009','201021','111022','127025','127029','111021','1740','1741','1879','1417','1424','1411','1401','1818','1838']:
        qry = "SELECT acc.id,acc.number,agr.agreement_number,cur.code,nam.value,inn.value,typ.code,agr.date_start FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id JOIN accounts.account AS acc ON acc.acc_agreement_id = agr.id JOIN accounts.account_ca AS ca ON ca.account_id = acc.id AND ca.chart_of_accounts_id = (SELECT id FROM accounts.chart_of_accounts WHERE code = '" + a + "') JOIN customers.customer AS cus ON cus.id = agr.customer_id JOIN customers.customer_type AS typ ON typ.id = cus.customer_type_id JOIN customers.customer_extended_field_values AS nam ON nam.customer_id = cus.id AND nam.status = 'ACTIVE' AND nam.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'CLIENT_NAME') JOIN customers.customer_extended_field_values AS inn ON inn.customer_id = cus.id AND inn.status = 'ACTIVE' AND inn.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'IDN') JOIN common.currency AS cur ON cur.id = lon.currency_id;"
        cur = query(conn,qry)
        tab = cur.fetchall()
        for r in tab:
            qry = "SELECT op.id,op.amount,op.purpose,nps.code AS cr,op.value_date FROM od.payment_operation AS op JOIN accounts.account_ca AS ca ON ca.account_id = op.acc_credit_id JOIN accounts.chart_of_accounts AS nps ON nps.id = ca.chart_of_accounts_id WHERE op.value_date >= '" + idate + "'::DATE AND op.paym_oper_status_id = (SELECT id FROM od.po_status WHERE code = 'APPROVED') AND op.acc_debit_id = " + str(r[0]) + ";"
            cur = query(conn,qry)
            ops = cur.fetchall()
            for o in ops:
                qry = "SELECT id FROM od.po_extended_field_value WHERE paym_oper_id = " + str(o[0]) + " AND paym_ext_field_id = (SELECT id FROM od.po_extended_field WHERE code = 'INFO_1C')"
                cur = query(conn,qry)
                ext = cur.fetchall()
                if len(ext) > 0: continue
                if (acct1c(r[6],nps_1c(a)) + ':' + acct1c(r[6],nps_1c(o[3]))) not in groups.keys():
                    groups.update({(acct1c(r[6],nps_1c(a)) + ':' + acct1c(r[6],nps_1c(o[3]))):[]})
                if not checkarray(groups[(acct1c(r[6],nps_1c(a)) + ':' + acct1c(r[6],nps_1c(o[3])))],'id',o[0]):
                    groups[(acct1c(r[6],nps_1c(a)) + ':' + acct1c(r[6],nps_1c(o[3])))].append({'db':acct1c(r[6],nps_1c(a)),'cr':acct1c(r[6],nps_1c(o[3])),'sum':o[1],'cur':r[3],'det':('№' + r[2] + ' ' + r[4] + ' ' + r[5]),'dat':o[4].strftime('%Y-%m-%d'),'det2':o[2],'id':o[0],'date':o[4].strftime('%Y-%m-%d'),'loan':r[2],'name':r[4],'inn':r[5],'type':r[6],'open':r[7].strftime('%d.%m.%Y'),'side':'db'})
            qry = "SELECT op.id,op.amount,op.purpose,nps.code AS db,op.value_date FROM od.payment_operation AS op JOIN accounts.account_ca AS ca ON ca.account_id = op.acc_debit_id JOIN accounts.chart_of_accounts AS nps ON nps.id = ca.chart_of_accounts_id WHERE op.value_date >= '" + idate + "'::DATE AND op.paym_oper_status_id = (SELECT id FROM od.po_status WHERE code = 'APPROVED') AND op.acc_credit_id = " + str(r[0]) + ";"
            cur = query(conn,qry)
            ops = cur.fetchall()
            for o in ops:
                qry = "SELECT id FROM od.po_extended_field_value WHERE paym_oper_id = " + str(o[0]) + " AND paym_ext_field_id = (SELECT id FROM od.po_extended_field WHERE code = 'INFO_1C')"
                cur = query(conn,qry)
                ext = cur.fetchall()
                if len(ext) > 0: continue
                if (acct1c(r[6],nps_1c(o[3])) + ':' + acct1c(r[6],nps_1c(a))) not in groups.keys():
                    groups.update({(acct1c(r[6],nps_1c(o[3])) + ':' + acct1c(r[6],nps_1c(a))):[]})
                if not checkarray(groups[(acct1c(r[6],nps_1c(o[3])) + ':' + acct1c(r[6],nps_1c(a)))],'id',o[0]):
                    groups[(acct1c(r[6],nps_1c(o[3])) + ':' + acct1c(r[6],nps_1c(a)))].append({'db':acct1c(r[6],nps_1c(o[3])),'cr':acct1c(r[6],nps_1c(a)),'sum':o[1],'cur':r[3],'det':('№' + r[2] + ' ' + r[4] + ' ' + r[5]),'dat':o[4].strftime('%Y-%m-%d'),'det2':o[2],'id':o[0],'date':o[4].strftime('%Y-%m-%d'),'loan':r[2],'name':r[4],'inn':r[5],'type':r[6],'open':r[7].strftime('%d.%m.%Y'),'side':'cr'})

    for ops in groups:
        cfg.data['PARAMS']['SEQUENCE'] = cfg.data['PARAMS']['SEQUENCE'] + 1
        fname = "CBS_EXP_" + cfg.data['PARAMS']['SEQUENCE_DATE'] + "_" + str(cfg.data['PARAMS']['SEQUENCE']) + ".xlsx"
        workbook = xlsxwriter.Workbook(fname)
        worksheet = workbook.add_worksheet("ПРОВОДКИ")
        for i,op in enumerate(groups[ops]):
            worksheet.write(i,0,op['db'])
            worksheet.write(i,1,op['cr'])
            worksheet.write(i,2,op['sum'])
            worksheet.write(i,3,op['cur'])
            worksheet.write(i,4,op['dat'])
            worksheet.write(i,5,op['det'])
            worksheet.write(i,6,op['loan'])
            worksheet.write(i,7,op['name'])
            worksheet.write(i,8,op['inn'])
            worksheet.write(i,9,op['det2'])
            worksheet.write(i,10,op['type'])
            worksheet.write(i,11,op['open'])
            worksheet.write(i,12,op['side'])
            qry = "INSERT INTO od.po_extended_field_value (id,status,date_start,value,paym_ext_field_id,paym_oper_id) SELECT NEXTVAL('od.po_ext_field_value_seq_id') AS id,'ACTIVE' AS status,'" + op['date'] + "'::DATE AS date_start,'" + fname + "' AS value,(SELECT id FROM od.po_extended_field WHERE code = 'INFO_1C') AS paym_ext_field_id," + str(op['id']) + " AS paym_oper_id"
            cur = query(conn,qry)
        workbook.close()
        shutil.copy2(fname,cfg.data['PARAMS']['DESTINATION'] + fname)
        shutil.move(fname,cfg.data['PARAMS']['ARCHIVE'] + fname)
    if test:
        conn.rollback()
    else:
        conn.commit()
        cfg.save()
    if conn is not None:
        conn.close()

if __name__ == '__main__':
    main()
