#!/python

import os
import json
from datetime import *
import time
import sys
import glob
from xfuncs import *
from cbs import *

def plandate(elem):
    return elem['target_date']

def main():
    test = False
    line = False
    fname_loan = 'cesna_loan2.csv'
    fname_plan = 'cesna_plan2.csv'
    who = 'SECURITY'
    portf = 'TSESNA'
    cfg = config('migration.json')
    conn = connect(cfg[who])
    
    codes_loan = {
        'id':'EXT_ID',
        'nom':'agreement_number',
        'opn':'date_start',
        'end':'date_end',
        'cur':'currency_id',
        'rate':'BASE`REWARD',
        'rate_p':'PENALTY`PENALTY_FOR_DEBT',
        'amount':'amount',
        'effect':'PERCENT_APR`NUMBER',
        'base':'accrual_basis_id',
        'ann':'repayment_schedule_type_id',
        '_loan_id':'EXT_ID',
        'inn':'_CLIENT_EXT_ID',
        'line_id':'_LINE',
        '_type':'product_id',
        '_number':'agreement_number', 
        '_filial':'branch_id',
        'currency':'currency_id',
        '_effect':'PERCENT_APR`NUMBER',
        'rate':'BASE`REWARD',
        'open_date':'date_start',
        '_open_date':'date_start',
        'end_date':'date_end',
        '_srok':'srok',
        'summa':'amount',
        '_summa':'amount',
        '_product':'LOAN_NAME_OLD',
        '_peni':'PENALTY`PENALTY_FOR_DEBT',
        'interest_repayment_freq_id':'interest_repayment_freq_id',
        'maindebt_repayment_freq_id':'maindebt_repayment_freq_id',
        'migr_date':'MIGRATION_DATE',
        'fact':'accrual_basis_id'
    }
    codes_plan = {
        'C_SID':'EXT_ID',
        'DATE_REPAYMENT':'target_date',
        'PRINCIPAL_AMOUNT':'amount_od',
        'INTEREST_AMOUNT':'amount_pr',
        'ost':'amount_principal_after',
        '_loan_id':'EXT_ID',
        'idcrd':'EXT_ID',
        'cnum_dat':'agreement_number',
        '_pdate':'target_date',
        'p_date':'target_date',
        '_od':'amount_od',
        '_pr':'amount_pr',
        'pattern_code':'pay_code',
        'summa':'amount',
        '_ost_pre':'amount_principal_before',
        '_ost_aft':'amount_principal_after'
    }
    product = {
        'Потребительский кредит':'Потребительский',
        'Пополнение оборотных средств':'Пополнение оборотных средств'
    }
    dates = ['date_start','date_end','target_date','date_sign','MIGRATION_DATE']
    bools = []
    decimals = ['srok','amount','amount_od','amount_principal_after','amount_principal_before','amount_pr']
    skip = []
    rates = ['BASE`REWARD','PENALTY`PENALTY_FOR_DEBT']
    param = ['PERCENT_APR`NUMBER','ACCRUEMENT_START_DATE`DATE']
    extend = ['LOAN_NAME_OLD','EXT_ID','MIGRATION_DATE']
    unquot = []
    
    rep_period = {'индивидуальная':'ARBITRARY'}
    plan_type = {'2':'DIFFERENTIAL','1':'ANNUITY'}
    basis = {'факт/360':'FACT_360','30/360':'30_360','факт/факт':'FACT_FACT'}
    
    pay_code = {
        '<CRD>CUR':'DEBT',
        '<PRC>CUR':'REWARD',
        '<CRD>':'DEBT',
        '<PRC>':'REWARD',
        '<CRD>EQL':'DEBT',
        '<PRC>EQL':'REWARD'
    }
    
    loans = txt2dict(fname_loan,codes_loan,dates,'%Y-%m-%d',decimals,bools,skip,unquot,'"',';')
    plans = []
    if fname_plan != '':
        plans = txt2dict(fname_plan,codes_plan,dates,'%Y-%m-%d',decimals,bools,skip,unquot,'"',';')
    kol = len(loans)
    k = 0
    for loan in loans:
        k += 1
        try:
            loan['branch_id'] = cfg['target'][who]['BRANCH'][loan['branch_id'].replace(' ','')]
        except:
            loan.update({'branch_id':cfg['target'][who]['BRANCH']['DEFAULT']})
        if 'EXT_ID' not in loan.keys():
            loan.update({'EXT_ID':loan['agreement_number']})
        try:
            loan['accrual_basis_id'] = basis[loan['accrual_basis_id']]
        except: pass
        if 'repayment_schedule_type_id' in loan.keys():
            loan['repayment_schedule_type_id'] = plan_type[loan['repayment_schedule_type_id']]
        qry = "SELECT id FROM common.agreement WHERE agreement_number = '" + loan['agreement_number'] + "' AND branch_id = (SELECT id FROM common.branch WHERE code = '" + loan['branch_id'] + "');"
        cur = query(conn,qry)
        tab = cur.fetchall()
        if len(tab) != 0:
            continue
        if portf != '':
            cust = "SELECT ext.customer_id FROM customers.customer_extended_field_values AS ext JOIN customers.cust_ext_field_rows AS rws ON rws.id = ext.row_id JOIN common.extended_field_group AS grp ON grp.id = rws.cust_ext_field_group_id AND grp.group_type = 'CUSTOMERS' AND grp.code = 'PORTFOLIO' JOIN customers.customer_extended_fields AS fld ON fld.code = 'PORTFOLIO_CODE' AND fld.ext_field_group_id = grp.id JOIN customers.customer_extended_field_values AS prf ON prf.cust_ext_field_id = fld.id AND prf.row_id = ext.row_id AND prf.value = 'BTA' WHERE ext.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'EXT_CODE') AND ext.value = '" + loan['_CLIENT_EXT_ID'] + "';"
        else:
            cust = "SELECT customer_id FROM customers.customer_extended_field_values AS ext WHERE ext.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'EXT_ID' AND ext.value = '" + loan['_CLIENT_EXT_ID'] + "';"
        cur = query(conn,cust)
        tab = cur.fetchall()
        if len(tab) == 0:
            cust = "SELECT ext.customer_id FROM customers.customer AS cus JOIN customers.customer_extended_field_values AS ext ON ext.customer_id = cus.id AND ext.cust_ext_field_id = (SELECT id FROM customers.customer_extended_fields WHERE code = 'IDN') AND ext.value = '" + loan['_CLIENT_EXT_ID'] + "'"
            cur = query(conn,cust)
            tab = cur.fetchall()
            if len(tab) == 0:
                print('Клиент "' + loan['_CLIENT_EXT_ID'] + '" не найден!')
                return
        for r in (tab):
            loan.update({'customer_id':r[0]})
        try:
            loan['maindebt_repayment_freq_id'] = rep_period[loan['maindebt_repayment_freq_id']]
        except:
            pass
        try:
            loan['interest_repayment_freq_id'] = rep_period[loan['interest_repayment_freq_id']]
        except:
            pass
        try:
            tmp = loan['MIGRATION_DATE']
        except:
            try:
                loan.update({'MIGRATION_DATE':datetime.strptime(cfg['target'][who]['MIGRATION_DATE'],'%Y-%m-%d').date()})
            except:
                pass
        try:
            tmp = loan['date_end']
        except:
            try:
                tmp = loan['srok']
                loan.update({'date_end':add2date(loan['date_start'],months=int(loan['srok']))})
            except:
                pass
        try:
            tmp = loan['ACCRUEMENT_START_DATE`DATE']
        except:
            try:
                loan.update({'ACCRUEMENT_START_DATE`DATE':add2date(loan['MIGRATION_DATE'],days=1)})
            except:
                loan.update({'ACCRUEMENT_START_DATE`DATE':add2date(datetime.strptime(cfg['target'][who]['MIGRATION_DATE'],'%Y-%m-%d'),days=1)})
        try:
            tmp = loan['currency_id']
        except:
            loan.update({'currency_id':'KZT'})
        try:
            tmp = loan['date_sign']
        except:
            loan.update({'date_sign':loan['date_start']})
        try:
            tmp = loan['period_id']
        except:
            try:
                loan.update({'period_id':[(loan['date_end'].year - loan['date_start'].year) * 12 + loan['date_end'].month - loan['date_start'].month,2]})
            except:
                loan.update({'period_id':[1,2]})
        try:
            loan['branch_id'] = cfg['target'][who]['BRANCH'][loan['branch_id'].replace(' ','')]
        except:
            loan.update({'branch_id':cfg['target'][who]['BRANCH']['DEFAULT']})
        try:
            loan['product_id'] = product[loan['product_id']]
        except:
            loan['product_id'] = cfg['target'][who]['DEFAULT']['product_id']
        #loan['amount'] = str2dec(loan['amount'])
        loan['origin'] = who
        if line:
            loan['agreement_number'] = loan['agreement_number'] + '_'
            loan['ACCRUEMENT_START_DATE`DATE'] = None
            loan['is_loan_line'] = True
            loan['without_loan_repayment_schedule'] = True
            
        rps = []
        for rp in rates:
            try:
                tmp = loan[rp]
            except:
                continue
            tmp = re.split('`',rp)
            rps.append({'interest_rate_penalty_type_id':tmp[0],'repayment_type_id':tmp[1],'start_date':loan['date_start'],'value_currency_id':loan['currency_id'],'value':str2dec(loan[rp])})
        loan.update({'repayments':rps})
        pms = []
        for pm in param:
            try:
                tmp = loan[pm]
                if tmp is None:
                    continue
            except:
                continue
            tmp = re.split('`',pm)
            pms.append({'agreement_parameter_type_id':tmp[0],'value_type':tmp[1],'value':loan[pm]})
        loan.update({'params':pms})
        exs = []
        for ex in extend:
            try:
                tmp = loan[ex]
            except:
                continue
            exs.append({'agreement_ext_field_id':ex,'value':x_str(loan[ex],'%d.%m.%Y')})
        loan.update({'extend':exs})
        plan_grp = {'EXT_ID':loan['EXT_ID'],'create_date':datetime.today().date(),'date_end':loan['date_end'],'date_start':loan['date_start'],'status':'ACTIVE','type':'TYPE_PAYMENTS'}
        plan = []
        repintday = 0
        reppriday = 0
        for _pl in plans:
            if _pl['EXT_ID'] != loan['EXT_ID']:
                continue
            if 'pay_code' in _pl.keys():
                _pl['pay_code'] = pay_code[_pl['pay_code']]
                if repintday == 0 and _pl['pay_code'] == 'REWARD':
                    repintday = _pl['target_date'].day
                if reppriday == 0 and _pl['pay_code'] == 'DEBT':
                    reppriday = _pl['target_date'].day
                plan.append({'repayment_type_id':_pl['pay_code'],'amount':_pl['amount'],'amount_principal_before':0,'amount_principal_after':0,'target_date':_pl['target_date'],'is_migrated':(True if _pl['target_date'] <= loan['MIGRATION_DATE'] else False),'is_migrated_balance_item':False})
            else:
                if 'amount_principal_before' not in _pl.keys():
                    _pl.update({'amount_principal_before':0})
                if 'amount_principal_after' not in _pl.keys():
                    _pl.update({'amount_principal_after':0})
                if repintday == 0 and _pl['amount_pr'] != 0:
                    repintday = _pl['target_date'].day
                if reppriday == 0 and _pl['amount_od'] != 0:
                    reppriday = _pl['target_date'].day
                if 'amount_principal_after' in _pl.keys():
                    if _pl['amount_principal_after'] > 0 and _pl['amount_od'] > 0:
                        _pl.update({'amount_principal_before':(_pl['amount_principal_after'] + _pl['amount_od'])})
                plan.append({'repayment_type_id':'DEBT','amount':_pl['amount_od'],'amount_principal_before':_pl['amount_principal_before'],'amount_principal_after':_pl['amount_principal_after'],'target_date':_pl['target_date'],'is_migrated':(True if _pl['target_date'] <= loan['MIGRATION_DATE'] else False),'is_migrated_balance_item':False})
                plan.append({'repayment_type_id':'REWARD','amount':_pl['amount_pr'],'amount_principal_before':None,'amount_principal_after':None,'target_date':_pl['target_date'],'is_migrated':(True if _pl['target_date'] <= loan['MIGRATION_DATE'] else False),'is_migrated_balance_item':False})
        plan.sort(key=plandate)
        sum = loan['amount']
        for _pl in plan:
            if _pl['repayment_type_id'] == 'DEBT':
                _pl['amount_principal_before'] = sum
                sum = sum - _pl['amount']
                _pl['amount_principal_after'] = sum
        if repintday == 0 and reppriday != 0:
            repintday = reppriday
        if repintday != 0 and reppriday == 0:
            reppriday = repintday
        #print(loan['agreement_number'])
        loan.update({'repayment_interest_date':repintday})
        loan.update({'repayment_maindebt_date':reppriday})
        plan_grp.update({'repayment_interest_date':repintday})
        plan_grp.update({'repayment_maindebt_date':reppriday})
        plan_grp.update({'loan_repayment_schedule_item':plan})
        if line:
            plan_grp = []
        xloan = {
            'agreement':loan,
            'loan_agreement':loan,
            'loan_agreement_rp_value':loan['repayments'],
            'agreement_extended_field_values':loan['extend'],
            'loan_agreement_parameter':loan['params'],
            'loan_repayment_schedule':plan_grp
        }
        if test:
            print(xloan)
        newloan = makeloan(conn,'',xloan,False)
        print('>>>',k,'из',kol,'<<<')

    if not test:
        conn.commit()

if __name__ == '__main__':
    main()
