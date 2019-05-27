#!/python

from cbs import *

def main():
    test = False
    who = 'МФО'
    cfg = config('migration.json')
    conn = connect(cfg[who])
    qry = "SELECT agr.agreement_number,lon.id FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        acct2repay(conn,r[0],'#' + cfg['НПС'][cfg['target'][who]['PLAN']]['ТРАНЗ'],["REPAYMENT_ACCOUNT","DISB_ACCOUNT"],not test)

if __name__ == '__main__':
    main()
