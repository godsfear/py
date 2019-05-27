#!/python

from xfuncs import *
from cbs import *
from datetime import *
install('requests')
import requests

def main():
    test = True
    who = 'ФПК_ПРОД'
    cfg = config('migration.json')
    conn = connect(cfg[who])
    
    qry = "SELECT lon.id,agr.id,rep.id,agr.agreement_number,bas.code,lon.amount,TO_DATE(ext.value,'DD.MM.YYYY'),agr.date_start FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND NOT agr.agreement_number LIKE '%_KZT' JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id JOIN loans.accrual_basis AS bas ON bas.id = lon.accrual_basis_id JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE') AND TO_DATE(ext.value,'DD.MM.YYYY') < agr.date_end"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for i,r in enumerate(tab):
        qry = "DELETE FROM loans.loan_agr_accr_mass_oper_d WHERE accruement_id in (SELECT id FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + " AND NOT is_deferred AND repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD'));"
        #cur = query(conn,qry)
        print(qry)
        qry = "DELETE FROM loans.loan_agr_accrl_oper_ref WHERE l_agr_accrl_id IN (SELECT id FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + " AND NOT is_deferred AND repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD'));"
        #cur = query(conn,qry)
        print(qry)
        qry = "DELETE FROM loans.loan_agreement_accrual WHERE loan_agreement_id = " + str(r[0]) + " AND NOT is_deferred AND repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD');"
        #cur = query(conn,qry)
        print(qry)

    if test:
        conn.rollback()
    else:
        conn.commit()

if __name__ == '__main__':
    main()
