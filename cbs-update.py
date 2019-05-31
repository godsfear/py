#!/python

from xfuncs import *
from cbs import *
from datetime import *
install('requests')
import requests

def plandate(elem):
    return elem['target_date']

def main():
    test = False
    who = 'SECURITY'
    cfg = config('migration.json')
    conn = connect(cfg[who])
    """qry = "SELECT lon.id,agr.date_start,agr.date_end FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        qry = "UPDATE loans.loan_agreement SET period_id = (SELECT id FROM common.period WHERE period_unit_id = 2 AND value = " + str(months(r[2],r[1])) + ") WHERE id = " + str(r[0])
        print(qry)
        cur = query(conn,qry)"""

    """qry = "SELECT rep.id FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        qry = "UPDATE loans.loan_repayment_schedule_item SET is_migrated = FALSE WHERE loan_repayment_schedule_id = " + str(r[0]) + " AND is_migrated_balance_item = FALSE AND target_date >= '2019-02-27'::DATE"
        cur = query(conn,qry)"""

    """qry = "SELECT rep.id FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        qry = "SELECT pln.amount_principal_before,pln.amount_principal_after,pln.target_date,pln.repayment_type_id FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[0]) + " AND pln.is_migrated_balance_item = FALSE AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'DEBT') AND pln.target_date <= '2019-02-27'::DATE ORDER BY pln.target_date DESC LIMIT 1"
        cur = query(conn,qry)
        tab2 = cur.fetchall()
        for r2 in tab2:
            qry = "UPDATE loans.loan_repayment_schedule_item SET amount_principal_before = " + str(r2[0]) + ", amount_principal_after = " + str(r2[1]) + " WHERE loan_repayment_schedule_id = " + str(r[0]) + " AND is_migrated_balance_item = TRUE AND repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'DEBT')"
            cur = query(conn,qry)
            print(qry)"""

    """qry = "SELECT lon.id,agr.agreement_number,agr.id,lon.loan_line_available_amount,lon.amount FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id AND lon.is_loan_line = TRUE"
    cur = query(conn,qry)
    lin = cur.fetchall()
    for l in lin:
        qry = "SELECT lon.id,agr.agreement_number,ost.balance,lon.currency_id FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id JOIN accounts.subaccount AS sub ON sub.agreement_id = agr.id AND sub.subaccount_type_id = (SELECT id FROM accounts.subaccount_type WHERE code = 'PRNCPL') JOIN accounts.subaccount_turnover AS ost ON ost.subaccount_id = sub.id WHERE lon.parent_agreement_id = " + str(l[0])
        cur = query(conn,qry)
        trn = cur.fetchall()
        for t in trn:
            #print(l[1],l[4],' : ',t[1],t[2],l[3])
            qry = "INSERT INTO loans.loan_line_history (id,available_amount,oper_date,loan_line_operation_type_id,loan_agreement_id,loan_line_id,amount_of_operation,currency_id) SELECT NEXTVAL('loans.loan_line_history_seq_id') AS id," + str(l[3]) + " AS available_amount,'2019-02-27'::DATE AS oper_date,(SELECT id FROM loans.loan_line_operation_type WHERE code = 'ISSUE_OF_TRANCHE') AS loan_line_operation_type_id," + str(t[0]) + " AS loan_agreement_id," + str(l[0]) + " AS loan_line_id," + str(t[2]) + " AS amount_of_operation," + str(t[3]) + " AS currency_id"
            print(qry)
            cur = query(conn,qry)"""

    """qry = "SELECT agr.id,lon.id,ost.balance,lon.currency_id,ext.value,rep.id FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id JOIN accounts.subaccount AS sub ON sub.agreement_id = agr.id AND sub.subaccount_type_id = (SELECT id FROM accounts.subaccount_type WHERE code = 'INTRST') JOIN accounts.subaccount_turnover AS ost ON ost.subaccount_id = sub.id JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        print('--------------------------------------')
        sum = r[2]
        if sum <= 0:
            continue
        alldfr = sum
        qry = "SELECT pln.amount,pln.target_date,pln.id FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[5]) + " AND pln.is_migrated_balance_item = FALSE AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND pln.target_date > TO_DATE('" + r[4] + "','DD.MM.YYYY') ORDER BY pln.target_date"
        cur = query(conn,qry)
        grf = cur.fetchall()
        was = False
        for p in grf:
            dfr = p[0]
            if dfr >= sum:
                dfr = sum
            sum -= dfr
            qry = "UPDATE loans.loan_repayment_schedule_item SET deferred_amount = " + str(dfr) + " WHERE id = " + str(p[2])
            print(qry)
            cur = query(conn,qry)
            was = True
        if was:
            qry = "UPDATE loans.loan_agreement SET interest_repayment_freq_id = 9,maindebt_repayment_freq_id = 9,accrual_method_id = 3 WHERE id = " + str(r[1])
            print(qry)
            cur = query(conn,qry)

            qry = "SELECT pln.id FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[5]) + " AND pln.is_migrated_balance_item = TRUE AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND pln.target_date >= TO_DATE('" + r[4] + "','DD.MM.YYYY') AND pln.is_delayed = FALSE LIMIT 1"
            cur = query(conn,qry)
            mig = cur.fetchall()
            for m in mig:
                qry = "UPDATE loans.loan_repayment_schedule_item SET deferred_amount = " + str(alldfr) + " WHERE id = " + str(m[0])
                print(qry)
                cur = query(conn,qry)

            qry = "SELECT pln.id FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[5]) + " AND pln.is_migrated_balance_item = TRUE AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'DEBT') AND pln.target_date >= TO_DATE('" + r[4] + "','DD.MM.YYYY') AND pln.is_delayed = FALSE LIMIT 1"
            cur = query(conn,qry)
            mig = cur.fetchall()
            for m in mig:
                qry = "UPDATE loans.loan_repayment_schedule_item SET accrued_amount = " + str(alldfr) + " WHERE id = " + str(m[0])
                print(qry)
                cur = query(conn,qry)"""

    """qry = "SELECT agr.id,lon.id,ext.value,rep.id FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        qry = "SELECT pln.id FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[3]) + " AND pln.is_migrated_balance_item = FALSE AND pln.repayment_type_id IN (SELECT id FROM loans.repayment_type WHERE code IN ('DEBT','REWARD')) AND pln.target_date < TO_DATE('" + r[2] + "','DD.MM.YYYY') ORDER BY pln.target_date"
        cur = query(conn,qry)
        pln = cur.fetchall()
        for p in pln:
            qry = "UPDATE loans.loan_repayment_schedule_item SET is_migrated = TRUE WHERE id = " + str(p[0])
            print(qry)
            cur = query(conn,qry)"""

    """qry = "SELECT agr.id,lon.id,rp1.loan_product_rp_item_id,rp2.loan_product_rp_item_id,rp1.value_currency_id,rp1.id,agr.agreement_number FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id JOIN loans.loan_agreement_rp_value AS rp1 ON rp1.loan_agreement_id = lon.id AND rp1.repayment_type_id = 19 LEFT OUTER JOIN loans.loan_agreement_rp_value AS rp2 ON rp2.loan_agreement_id = lon.id AND rp2.repayment_type_id = 18 ORDER BY lon.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        if not r[3] is None:
            continue
        qry = "UPDATE loans.loan_agreement_rp_value SET repayment_type_id = 18, loan_product_rp_item_id = (CASE WHEN " + str(r[4]) + " = (SELECT id FROM common.currency WHERE code = 'KZT') THEN 210 WHEN " + str(r[4]) + " = (SELECT id FROM common.currency WHERE code = 'USD') THEN 208 WHEN " + str(r[4]) + " = (SELECT id FROM common.currency WHERE code = 'EUR') THEN 202 WHEN " + str(r[4]) + " = (SELECT id FROM common.currency WHERE code = 'RUB') THEN 206 END) WHERE id = " + str(r[5])
        print(r[6],qry)"""
    
    """qry="SELECT par.id,rp.value FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id JOIN loans.loan_agreement_parameter AS par ON par.loan_agreement_id = lon.id AND par.agreement_parameter_type_id = (SELECT id FROM loans.agreement_parameter_type WHERE code = 'PERCENT_APR') JOIN loans.loan_agreement_rp_value AS rp ON rp.loan_agreement_id = lon.id AND rp.interest_rate_penalty_type_id = (SELECT id FROM loans.interest_rate_penalty_type WHERE code = 'BASE') AND rp.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD')"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        qry="UPDATE loans.loan_agreement_parameter SET value = " + str(r[1]) + " WHERE id = " + str(r[0])
        cur = query(conn,qry)
        print(qry)"""

    """qry = "SELECT lon.id,agr.customer_id,lon.agreement_id,lon.currency_id,agr.agreement_number FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        qry = "SELECT id FROM loans.loan_repayment_account WHERE loan_agreement_id = " + str(r[0]) + " AND account_sign_type_id = (SELECT id FROM loans.account_sign_type WHERE code = 'REPAYMENT_ACCOUNT')"
        cur = query(conn,qry)
        acc = cur.fetchall()
        if len(acc) > 0:
            continue
        qry = "SELECT acc.number FROM accounts.account AS acc JOIN accounts.account_customer AS acs ON acs.account_id = acc.id AND acs.customer_id = " + str(r[1]) + " JOIN accounts.account_currency AS acr ON acr.account_id = acc.id AND acr.currency_id = " + str(r[3]) + " JOIN accounts.account_ca AS aca ON aca.account_id = acc.id AND aca.chart_of_accounts_id = (SELECT id FROM accounts.chart_of_accounts WHERE code = '2860' AND status = 'ACTIVE')"
        cur = query(conn,qry)
        acc = cur.fetchall()
        for a in acc:
            print(a[0])
            acct2repay(conn,r[4],a[0],['REPAYMENT_ACCOUNT','DISB_ACCOUNT'],False)"""

    """sum = 1123091.80
    ost = 0.00
    qry = "SELECT agr.id,lon.id,ext.value,rep.id FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND agr.agreement_number = '0101/07/414/61' JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        qry = "SELECT pln.id,pln.amount FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[3]) + " AND pln.is_migrated_balance_item = FALSE AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND pln.target_date >= TO_DATE('" + r[2] + "','DD.MM.YYYY') ORDER BY pln.target_date"
        cur = query(conn,qry)
        pln = cur.fetchall()
        print(qry)
        for p in pln:
            if sum > 0:
                ost = float(sum if sum < p[1] else p[1])
                sum = sum - ost
                qry = "UPDATE loans.loan_repayment_schedule_item SET deferred_amount = " + str(ost) + " WHERE id = " + str(p[0])
                print(qry)
                cur = query(conn,qry)"""
    
    """number = '0101/07/414/61'
    dates = ['target_date']
    decimals = ['amount']
    bools = []
    skip = []
    plan = txt2dict('_plan.csv',{'p_date':'target_date','pattern_code':'type','summa':'amount'},dates,'%Y-%m-%d',decimals,bools,skip,'"',',')
    plan.sort(key=plandate)
    
    qry = "SELECT agr.id,lon.id,TO_DATE(ext.value,'DD.MM.YYYY'),rep.id,lon.amount FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND agr.agreement_number = '" + number + "' JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    if len(tab) == 0:
        return
    for r in tab:
        agr = r[0]
        lon = r[1]
        mgr = r[2]
        pln = r[3]
        sum = r[4]
    
    for p in plan:
        if 'PRC' in p['type']:
            p['type'] = 'REWARD'
            p.update({'amount_principal_before':0})
            p.update({'amount_principal_after':0})
        else:
            p['type'] = 'DEBT'
            p.update({'amount_principal_before':sum})
            sum = sum - p['amount']
            p.update({'amount_principal_after':sum})
            
    for p in plan:
        qry = "INSERT INTO loans.loan_repayment_schedule_item (id,accrued_amount,amount,amount_principal_after,amount_principal_before,deferred_amount,is_delayed,is_repaid,target_date,loan_repayment_schedule_id,repayment_type_id,is_migrated,is_migrated_balance_item) SELECT NEXTVAL('loans.loan_repay_sched_item_seq_id') AS id," + str(p['amount']) + " AS accrued_amount," + str(p['amount']) + " AS amount," + str(p['amount_principal_after']) + " AS amount_principal_after," + str(p['amount_principal_before']) + " AS amount_principal_before,0 AS deferred_amount,FALSE AS is_delayed,FALSE AS is_repaid,'" + p['target_date'].strftime('%Y-%m-%d') + "'::DATE AS target_date," + str(pln) + " AS loan_repayment_schedule_id,(SELECT id FROM loans.repayment_type WHERE code = '" + p['type'] + "') AS repayment_type_id," + ('FALSE' if mgr < p['target_date'].date() else 'TRUE') + " AS is_migrated,FALSE AS is_migrated_balance_item"
        cur = query(conn,qry)"""
    
    """oper2loan(conn,cfg,who,'0101/07/414/61','INTRST',1123091.80,True,str2date('14.12.2017'),'Миграция',True)"""
    """ost2graf(conn,'0101/07/414/61','PRNCPL',False)
    ost2graf(conn,'0101/07/414/61','INTRST_OVRD',False)"""
    
    """qry = "SELECT agr.id,agr.agreement_number FROM common.agreement AS agr WHERE agr.agreement_number LIKE '%_KZT'"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        qry = "SELECT ext.id,ext.value FROM common.agreement_extended_field_values AS ext WHERE ext.agreement_id = " + str(r[0]) + " AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE')"
        cur = query(conn,qry)
        ext = cur.fetchall()
        if len(ext) != 0:
            continue
        qry = "SELECT agr.id,agr.agreement_number FROM common.agreement AS agr WHERE agr.agreement_number = '" + r[1].replace('_KZT','') + "'"
        cur = query(conn,qry)
        lon = cur.fetchall()
        for l in lon:
            qry = "SELECT ext.value FROM common.agreement_extended_field_values AS ext WHERE ext.agreement_id = " + str(l[0]) + " AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE')"
            cur = query(conn,qry)
            dat = cur.fetchall()
            for d in dat:
                print(d[0],l[1],r[1])
                qry = "INSERT INTO common.agreement_extended_field_values (id,status,value,agreement_id,agreement_ext_field_id) SELECT NEXTVAL('common.agreement_extended_field_values_seq_id') AS id,'ACTIVE' AS status,'" + d[0] + "' AS value," + str(r[0]) + " AS agreement_id,(SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') AS agreement_ext_field_id"
                cur = query(conn,qry)"""

    """qry = "SELECT lon.id,agr.id,rep.id,agr.agreement_number,bas.code,lon.amount,TO_DATE(ext.value,'DD.MM.YYYY'),agr.date_start FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND NOT agr.agreement_number LIKE '%_KZT' JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id JOIN loans.accrual_basis AS bas ON bas.id = lon.accrual_basis_id JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') AND TO_DATE(ext.value,'DD.MM.YYYY') < agr.date_end"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for i,r in enumerate(tab):
        print(i,r)
        qry = "DELETE FROM loans.loan_agr_accr_mass_oper_d WHERE accruement_id in (SELECT id FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + " AND NOT is_deferred AND repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD'));"
        cur = query(conn,qry)
        qry = "DELETE FROM loans.loan_agr_accrl_oper_ref WHERE l_agr_accrl_id IN (SELECT id FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + " AND NOT is_deferred AND repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD'));"
        cur = query(conn,qry)
        qry = "DELETE FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + " AND NOT is_deferred AND repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD');"
        cur = query(conn,qry)"""

    """qry = "SELECT lon.id,agr.id,rep.id,agr.agreement_number,lon.amount,TO_DATE(ext.value,'DD.MM.YYYY'),agr.date_start FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND NOT agr.agreement_number LIKE '%_KZT' JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') AND TO_DATE(ext.value,'DD.MM.YYYY') < agr.date_end"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for i,r in enumerate(tab):
        print(i,r)
        qry = "DELETE FROM loans.loan_penalty_accrual_log WHERE loan_agreement_accrual_id IN (SELECT id FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + ");"
        cur = query(conn,qry)
        qry = qry + "DELETE FROM loans.loan_agr_accrl_oper_ref WHERE l_agr_accrl_id IN (SELECT id FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + ");"
        cur = query(conn,qry)
        qry = qry + "DELETE FROM loans.loan_agr_accr_mass_oper_d WHERE accruement_id IN (SELECT id FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + ");"
        cur = query(conn,qry)
        qry = qry + "DELETE FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + ";"
        cur = query(conn,qry)"""

    """codes = {
        'TAX_NUMBER':'IDN',
        'CLITAXCODE':'IDN'
    }
    clients = txt2dict('8_Клиент_ЮЛ1.csv',codes,[],'%Y-%m-%d',[],[],[],[],'"',';')
    print(clients)
    for cli in clients:
        cust = "SELECT ext.customer_id FROM customers.customer AS cus JOIN customers.customer_extended_field_values AS ext ON ext.customer_id = cus.id AND ext.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'IDN') AND ext.value = '" + cli['IDN'] + "'"
        cur = query(conn,cust)
        tab = cur.fetchall()
        for r in tab:
            qry = "UPDATE customers.customer SET branch_id = (SELECT id FROM common.branch WHERE code = '0300') WHERE customers.customer.id = " + str(r[0])
            cur = query(conn,qry)"""

    """qry = "SELECT lon.id FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND agr.branch_id = (SELECT id FROM common.branch WHERE code = '0300')"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        qry = "UPDATE loans.loan_agreement SET accrual_method_id = (SELECT id FROM loans.accrual_method WHERE code = 'FIXED_METHOD'),maindebt_repayment_freq_id = (SELECT id FROM loans.repayment_period WHERE code = 'ARBITRARY'),interest_repayment_freq_id = (SELECT id FROM loans.repayment_period WHERE code = 'ARBITRARY'),repayment_schedule_type_id = (SELECT id FROM loans.repayment_schedule_type WHERE code = 'DIFFERENTIAL') WHERE id = " + str(r[0])
        cur = query(conn,qry)"""
    

    if test:
        conn.rollback()
    else:
        conn.commit()

if __name__ == '__main__':
    main()
