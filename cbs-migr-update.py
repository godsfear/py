#!/python

from xfuncs import *
from cbs import *
from datetime import *

def main():
    test = False
    who = 'SECURITY'
    cfg = config('migration.json')
    conn = connect(cfg[who])

    qry = "SELECT pln.id,TO_DATE(ext.value,'DD.MM.YYYY') FROM loans.loan_repayment_schedule_item AS pln JOIN loans.loan_repayment_schedule AS sch ON sch.id = pln.loan_repayment_schedule_id JOIN loans.loan_agreement AS lon ON lon.id = sch.loan_agreement_id JOIN common.agreement AS agr ON agr.id = lon.agreement_id JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') WHERE pln.is_delayed AND pln.date_delayed IS NULL"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        print(r[0],r[1])
        qry = "UPDATE loans.loan_repayment_schedule_item SET date_delayed = '" + r[1].strftime('%Y-%m-%d') + "'::DATE WHERE id = " + str(r[0])
        cur = query(conn,qry)

    qry = "SELECT lon.id,agr.id,rep.id,agr.agreement_number,bas.code,lon.amount,TO_DATE(ext.value,'DD.MM.YYYY'),agr.date_start FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND NOT agr.agreement_number LIKE '%_KZT' AND agr.branch_id = (SELECT id FROM common.branch WHERE code = '0300') JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id JOIN loans.accrual_basis AS bas ON bas.id = lon.accrual_basis_id JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') AND TO_DATE(ext.value,'DD.MM.YYYY') < agr.date_end"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for i,r in enumerate(tab):
        print(i,r)
        bas = r[4].split('_')
        qry = "SELECT com.value FROM loans.loan_agreement_rp_value AS com WHERE com.loan_agreement_id = " + str(r[0]) + " AND com.interest_rate_penalty_type_id = (SELECT id FROM loans.interest_rate_penalty_type WHERE code = 'BASE') AND com.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND com.status = 'ACTIVE' ORDER BY com.start_date LIMIT 1"
        cur = query(conn,qry)
        com = cur.fetchall()
        for cm in com:
            qry = "SELECT pln.id,pln.target_date,pln.amount FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[2]) + " AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND NOT pln.is_migrated_balance_item AND pln.target_date >= '" + r[6].strftime('%Y-%m-%d') + "'::DATE AND pln.amount > 0 ORDER BY pln.target_date"
            cur = query(conn,qry)
            prc = cur.fetchall()
            for pr in prc:
                qry = "SELECT pln.id,pln.target_date,pln.amount FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[2]) + " AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND NOT pln.is_migrated_balance_item AND pln.target_date < '" + pr[1].strftime('%Y-%m-%d') + "'::DATE ORDER BY pln.target_date DESC LIMIT 1"
                cur = query(conn,qry)
                prb = cur.fetchall()
                if len(prb) == 0:
                    dys = days(r[7].date(),pr[1],bas[0] == '30')
                    pred = r[7].date()
                else:
                    for b in prb:
                        dys = days(b[1],pr[1],bas[0] == '30')
                        pred = b[1]
                
                qry = "SELECT pln.id,pln.target_date,pln.amount_principal_before,pln.amount_principal_after FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[2]) + " AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'DEBT') AND NOT pln.is_migrated_balance_item AND pln.target_date <= '" + pr[1].strftime('%Y-%m-%d') + "'::DATE ORDER BY pln.target_date DESC LIMIT 1"
                cur = query(conn,qry)
                ost = cur.fetchall()
                if len(ost) == 0:
                    sum = r[5]
                else:
                    for o in ost:
                        if o[1] == pred:
                            sum = o[3]
                        else:
                            sum = o[2]
                if bas[1] == '360':
                    yar = 360
                else:
                    yar = days(datetime(pr[1].year,1,1).date(),datetime(pr[1].year + 1,1,1).date())
                clc = round(sum * cm[0] * dys / (yar * 100),2)
                dff = pr[2] - clc
                if dff < 0:
                    dff = 0
                qry = "UPDATE loans.loan_repayment_schedule_item SET deferred_amount = " + str(dff) + " WHERE id = " + str(pr[0])
                cur = query(conn,qry)

    qry = "SELECT lon.id,agr.id,rep.id,agr.agreement_number,bas.code,lon.amount,TO_DATE(ext.value,'DD.MM.YYYY'),agr.date_start,ost.balance FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND NOT agr.agreement_number LIKE '%_KZT' AND agr.branch_id = (SELECT id FROM common.branch WHERE code = '0300') JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id JOIN loans.accrual_basis AS bas ON bas.id = lon.accrual_basis_id JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') AND TO_DATE(ext.value,'DD.MM.YYYY') < agr.date_end JOIN accounts.subaccount AS sub ON sub.agreement_id = agr.id AND sub.subaccount_type_id = (SELECT id FROM accounts.subaccount_type WHERE code = 'INTRST') JOIN accounts.subaccount_turnover AS ost ON ost.subaccount_id = sub.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for i,r in enumerate(tab):
        print(i,r)
        bas = r[4].split('_')
        qry = "SELECT com.value FROM loans.loan_agreement_rp_value AS com WHERE com.loan_agreement_id = " + str(r[0]) + " AND com.interest_rate_penalty_type_id = (SELECT id FROM loans.interest_rate_penalty_type WHERE code = 'BASE') AND com.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND com.status = 'ACTIVE' ORDER BY com.start_date LIMIT 1"
        cur = query(conn,qry)
        com = cur.fetchall()
        for cm in com:
            qry = "SELECT pln.id,pln.target_date,pln.amount FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[2]) + " AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND NOT pln.is_migrated_balance_item AND pln.target_date >= '" + r[6].strftime('%Y-%m-%d') + "'::DATE ORDER BY pln.target_date LIMIT 1"
            cur = query(conn,qry)
            pra = cur.fetchall()
            for pa in pra:
                qry = "SELECT pln.id,pln.target_date,pln.amount FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[2]) + " AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND NOT pln.is_migrated_balance_item AND pln.target_date < '" + pa[1].strftime('%Y-%m-%d') + "'::DATE ORDER BY pln.target_date DESC LIMIT 1"
                cur = query(conn,qry)
                prb = cur.fetchall()
                if len(prb) == 0:
                    pred = r[7].date()
                    dys = days(r[7].date(),r[6],bas[0] == '30')
                else:
                    for pb in prb:
                        pred = pb[1]
                        dys = days(pb[1],r[6],bas[0] == '30')
                qry = "SELECT pln.id,pln.target_date,pln.amount_principal_before,pln.amount_principal_after FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[2]) + " AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'DEBT') AND NOT pln.is_migrated_balance_item AND pln.target_date <= '" + pa[1].strftime('%Y-%m-%d') + "'::DATE ORDER BY pln.target_date DESC LIMIT 1"
                cur = query(conn,qry)
                ost = cur.fetchall()
                if len(ost) == 0:
                    sum = r[5]
                else:
                    for o in ost:
                        if o[1] == pred:
                            sum = o[3]
                        else:
                            sum = o[2]
                if bas[1] == '360':
                    yar = 360
                else:
                    yar = days(datetime(r[6].year,1,1).date(),datetime(r[6].year + 1,1,1).date())
                clc = round(sum * cm[0] * dys / (yar * 100),2)
                if r[8] < clc:
                    clc = r[8]
                print(cm[0],dys,yar,sum,'=',clc)
                qry = "UPDATE loans.loan_repayment_schedule_item SET int_prior_to_migration = " + str(clc) + " WHERE id = " + str(pa[0])
                cur = query(conn,qry)
                qry = "SELECT pln.id,pln.target_date,pln.amount FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[2]) + " AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND pln.is_migrated_balance_item AND pln.target_date >= '" + r[6].strftime('%Y-%m-%d') + "'::DATE ORDER BY pln.target_date LIMIT 1"
                cur = query(conn,qry)
                mgr = cur.fetchall()
                for pm in mgr:
                    qry = "UPDATE loans.loan_repayment_schedule_item SET int_prior_to_migration = " + str(clc) + " WHERE id = " + str(pm[0])
                    cur = query(conn,qry)

    if test:
        conn.rollback()
    else:
        conn.commit()

if __name__ == '__main__':
    main()
