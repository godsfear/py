#!/python

import os
import json
from datetime import *
import time
import sys
import glob
from xfuncs import *
import requests

def initfields(cols,rez,vals,defs):
    for c in cols:
        try:
            rez.update({c:vals[c]})
        except:
            try:
                rez.update({c:eval(defs[c])})
            except:
                try:
                    if defs[c] == '+':
                        print('Ошибка: реквизит "' + c + '" не может быть пустым!')
                        sys.exit(1)
                except:
                    pass
                rez.update({c:None})
    return rez

def makeloan(conn,loan,values,commit):
    defaults = config('makeloan.json')
    #---------------------------------------common.agreement
    _agr_templ_id = 0
    values['agreement'].update({'id':'NEXTVAL("common.agreement_seq_id")'})
    if loan != '':
        cur = query(conn,"SELECT * FROM common.agreement WHERE common.agreement.agreement_number = '" + loan + "' LIMIT 1")
    else:
        cur = query(conn,"SELECT * FROM common.agreement LIMIT 1")
    tab = cur.fetchall()
    if loan != '' and len(tab) == 0:
        print("Не найден договор: ",loan)
        return
    cols = [desc[0] for desc in cur.description]
    _agr = {}
    for r in (tab):
        i = 0
        for c in (cols):
            if loan != '' and c == 'id':
                _agr_templ_id = r[i]
            try:
                _agr.update({c:values['agreement'][c]})
            except:
                if loan != '':
                    _agr.update({c:r[i]})
                else:
                    try:
                        _agr.update({c:eval(defaults['agreement'][c])})
                    except:
                        try:
                            if defaults['agreement'][c] == '+':
                                print('Ошибка: реквизит "' + c + '" не может быть пустым!')
                                return
                        except:
                            pass
                        _agr.update({c:None})
            i += 1
    if len(tab) == 0:
        _agr = initfields(cols,_agr,values['agreement'],defaults['agreement'])
        
    _sel = ''
    _ins = ''
    for field in (_agr):
        _ins = _ins + ',' + field
        if type(_agr[field]) is str and field != 'id':
            _agr[field] = '"' + _agr[field] + '"'
        if type(_agr[field]) is date or type(_agr[field]) is datetime:
            _agr[field] = '"' + _agr[field].strftime('%Y-%m-%d') + '"::DATE'
        if field == 'branch_id':
            if type(_agr[field]) is str:
                _agr[field] = '(SELECT id FROM common.branch WHERE code = ' + _agr[field] + ')'
        if field == 'product_id':
            if type(_agr[field]) is str:
                _agr[field] = '(SELECT id FROM common.product WHERE name = ' + _agr[field] + ')'
        if field == 'user_id':
            if type(_agr[field]) is str:
                _agr[field] = '(SELECT id FROM security.u_user WHERE login = ' + _agr[field] + ')'
        if not (type(_agr[field]) is str):
            _agr[field] = str(_agr[field])
        _sel = _sel + ('NULL' if (_agr[field] == 'None') else _agr[field]) + ' AS ' + field + ','
    _sel = 'SELECT ' + _sel.strip(',').replace('"','\'')
    _ins = 'INSERT INTO common.agreement (' + _ins.strip(',') + ') ' + _sel + ' RETURNING id'
    cur = query(conn,_ins)
    tab = cur.fetchall()
    for r in (tab):
        _agr['id'] = r[0]
    if not commit:
        print('agreement = ',_agr['id'])

    #---------------------------------------loans.loan_agreement
    _lon_templ_id = 0
    values['loan_agreement']['agreement_id'] = _agr['id']
    values['loan_agreement'].update({'id':'NEXTVAL("loans.loan_agreement_seq_id")'})
    if loan != '':
        cur = query(conn,"SELECT * FROM loans.loan_agreement WHERE loans.loan_agreement.agreement_id = " + str(_agr_templ_id) + " LIMIT 1")
    else:
        cur = query(conn,"SELECT * FROM loans.loan_agreement LIMIT 1")
    tab = cur.fetchall()
    if loan != '' and len(tab) == 0:
        print("Не найден займ: ",loan)
        return
    cols = [desc[0] for desc in cur.description]
    _lon = {}
    for r in (tab):
        i = 0
        for c in (cols):
            if loan != '' and c == 'id':
                _lon_templ_id = r[i]
            try:
                _lon.update({c:values['loan_agreement'][c]})
            except:
                if loan != '':
                    _lon.update({c:r[i]})
                else:
                    try:
                        _lon.update({c:eval(defaults['loan_agreement'][c])})
                    except:
                        try:
                            if defaults['loan_agreement'][c] == '+':
                                print('Ошибка: реквизит "' + c + '" не может быть пустым!')
                                return
                        except:
                            pass
                        _lon.update({c:None})
            i += 1
    if len(tab) == 0:
        _lon = initfields(cols,_lon,values['loan_agreement'],defaults['loan_agreement'])

    _sel = ''
    _ins = ''
    for field in (_lon):
        _ins = _ins + ',' + field
        if type(_lon[field]) is str and field != 'id':
            _lon[field] = '"' + _lon[field] + '"'
        if type(_lon[field]) is date or type(_lon[field]) is datetime:
            _lon[field] = '"' + _lon[field].strftime('%Y-%m-%d') + '"::DATE'
        if field == 'accrual_method_id':
            if type(_lon[field]) is str:
                _lon[field] = '(SELECT id FROM loans.accrual_method WHERE code = ' + _lon[field] + ')'
        if field == 'repayment_schedule_type_id':
            if type(_lon[field]) is str:
                _lon[field] = '(SELECT id FROM loans.repayment_schedule_type WHERE code = ' + _lon[field] + ')'
        if field == 'accrual_basis_id':
            if type(_lon[field]) is str:
                _lon[field] = '(SELECT id FROM loans.accrual_basis WHERE code = ' + _lon[field] + ')'
        if field == 'period_id':
            if isinstance(_lon[field],list):
                _lon[field] = '(SELECT id FROM common.period WHERE period_unit_id = ' + str(_lon[field][1]) + ' AND value = ' + str(_lon[field][0]) + ')'
        if field == 'currency_id':
            if type(_lon[field]) is str:
                _lon[field] = '(SELECT id FROM common.currency WHERE code = ' + _lon[field] + ')'
        if field == 'interest_repayment_freq_id' or field == 'maindebt_repayment_freq_id':
            if type(_lon[field]) is str:
                _lon[field] = '(SELECT id FROM loans.repayment_period WHERE code = ' + _lon[field] + ')'
        if field == 'repayment_allocation_id':
            if type(_lon[field]) is str:
                _lon[field] = '(SELECT id FROM loans.repayment_allocation WHERE code = ' + _lon[field] + ')'
        if field == 'product_id':
            if type(_lon[field]) is str:
                _lon[field] = '(SELECT id FROM loans.loan_product WHERE product_id = (SELECT id FROM common.product WHERE name = ' + _lon[field] + '))'
        if not (type(_lon[field]) is str):
            _lon[field] = str(_lon[field])
        _sel = _sel + ('NULL' if (_lon[field] == 'None') else _lon[field]) + ' AS ' + field + ','
    _sel = 'SELECT ' + _sel.strip(',').replace('"','\'')
    _ins = 'INSERT INTO loans.loan_agreement (' + _ins.strip(',') + ') ' + _sel + ' RETURNING id'
    cur = query(conn,_ins)
    tab = cur.fetchall()
    for r in (tab):
        _lon['id'] = r[0]
    if not commit:
        print('loan_agreement = ',_lon['id'])

    #---------------------------------------loans.loan_agreement_initial_condition
    cur = query(conn,"SELECT * FROM loans.loan_agreement_initial_condition LIMIT 1")
    tab = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    _ini = {}
    _ini.update({'id':'NEXTVAL("loans.loan_agreement_initial_condition_id")'})
    for c in (cols):
        if c == 'id':
            continue
        if c == 'product_id':
            _ini.update({c:_agr[c]})
            continue
        try:
            _ini.update({c:_lon[c]})
        except:
            try:
                _ini.update({c:_agr[c]})
            except:
                if c == 'loan_agreement_id':
                    _ini.update({c:_lon['id']})
                elif c == 'create_date':
                    _ini.update({c:_agr['date_created']})
                elif c == 'oper_date':
                    _ini.update({c:_agr['date_start']})

    _sel = ''
    _ins = ''
    for field in (_ini):
        _ins = _ins + ',' + field
        _sel = _sel + ('NULL' if (_ini[field] == 'None') else str(_ini[field])) + ' AS ' + field + ','
    _sel = 'SELECT ' + _sel.strip(',').replace('"','\'')
    _ins = 'INSERT INTO loans.loan_agreement_initial_condition (' + _ins.strip(',') + ') ' + _sel + ' RETURNING id'
    cur = query(conn,_ins)
    tab = cur.fetchall()
    for r in (tab):
        _ini['id'] = r[0]
    if not commit:
        print('loan_agreement_initial_condition = ',_ini['id'])

    #---------------------------------------loans.loan_agreement_rp_value
    _rates = []
    for rate in (values['loan_agreement_rp_value']):
        rate.update({'id':'NEXTVAL("loans.loan_agreement_rp_value_seq_id")'})
        rate.update({'loan_agreement_id':_lon['id']})
        if loan != '':
            cur = query(conn,"SELECT * FROM loans.loan_agreement_rp_value AS rp WHERE rp.loan_agreement_id = " + str(_lon_templ_id) + " AND rp.status = 'ACTIVE' AND rp.interest_rate_penalty_type_id = (SELECT id FROM loans.interest_rate_penalty_type WHERE code = '" + rate['interest_rate_penalty_type_id'] + "')" + " AND rp.repayment_type_id = (SELECT id FROM loans.repayment_type where code = '" + rate['repayment_type_id'] + "')" + "ORDER BY rp.start_date DESC LIMIT 1")
        else:
            cur = query(conn,"SELECT * FROM loans.loan_agreement_rp_value LIMIT 1")
        tab = cur.fetchall()
        if loan != '' and len(tab) == 0:
            print("Не найдена ставка: ",rate['repayment_type_id'])
            return
        cols = [desc[0] for desc in cur.description]
        _rat = {}
        for r in (tab):
            i = 0
            for c in (cols):
                try:
                    _rat.update({c:rate[c]})
                except:
                    if loan != '':
                        _rat.update({c:r[i]})
                    else:
                        try:
                            _rat.update({c:eval(defaults['loan_agreement_rp_value'][c])})
                        except:
                            try:
                                if defaults['loan_agreement_rp_value'][c] == '+':
                                    print('Ошибка: реквизит "' + c + '" не может быть пустым!')
                                    return
                            except:
                                pass
                            _rat.update({c:None})
                i += 1
        if len(tab) == 0:
            _rat = initfields(cols,_rat,rate,defaults['loan_agreement_rp_value'])

        _sel = ''
        _ins = ''
        for field in (_rat):
            _ins = _ins + ',' + field
            if type(_rat[field]) is str and field != 'id':
                _rat[field] = '"' + _rat[field] + '"'
            if type(_rat[field]) is date or type(_rat[field]) is datetime:
                _rat[field] = '"' + _rat[field].strftime('%Y-%m-%d') + '"::DATE'
            if field == 'interest_rate_penalty_type_id':
                if type(_rat[field]) is str:
                    _rat[field] = "COALESCE((SELECT id FROM loans.interest_rate_penalty_type WHERE code = " + _rat[field] + "),(SELECT id FROM loans.interest_rate_penalty_type WHERE code = 'PENI'),(SELECT id FROM loans.interest_rate_penalty_type WHERE code = 'PENALTY'))"
            if field == 'repayment_type_id':
                if type(_rat[field]) is str:
                    _rat[field] = '(SELECT id FROM loans.repayment_type WHERE code = ' + _rat[field] + ')'
            if field == 'value_currency_id':
                if type(_rat[field]) is str:
                    _rat[field] = '(SELECT id FROM common.currency WHERE code = ' + _rat[field] + ')'
            if field == 'loan_product_rp_item_id':
                _cur = _rat['value_currency_id']
                if type(_cur) is str:
                    _cur = '(SELECT id FROM common.currency WHERE code = "' + _cur.strip('"') + '")'
                _typ = _rat['repayment_type_id']
                if type(_typ) is str:
                    _typ = '(SELECT id FROM loans.repayment_type WHERE code = "' + _typ.strip('"') + '")'
                _rat[field] = '(SELECT id FROM loans.loan_product_rp_item WHERE loan_product_rp_id = (SELECT id FROM loans.loan_product_rp WHERE currency_id = ' + _cur + ' AND loan_product_id = ' + _lon['product_id'] + ' AND repayment_type_id = ' + _typ + '))'
            if not (type(_rat[field]) is str):
                _rat[field] = str(_rat[field])
            _sel = _sel + ('NULL' if (_rat[field] == 'None') else _rat[field]) + ' AS ' + field + ','
        _sel = 'SELECT ' + _sel.strip(',').replace('"','\'')
        _ins = 'INSERT INTO loans.loan_agreement_rp_value (' + _ins.strip(',') + ') ' + _sel + ' RETURNING id'
        cur = query(conn,_ins)
        tab = cur.fetchall()
        for r in (tab):
            _rat['id'] = r[0]
        _rates.append(_rat)
        if not commit:
            print('loan_agreement_rp_value' + '[' + str(_rat['repayment_type_id']) + ']' + ' = ',_rat['id'])

    #---------------------------------------common.agreement_extended_field_values
    if len(values['agreement_extended_field_values']) > 0:
        _exts = []
        for ext in (values['agreement_extended_field_values']):
            ext.update({'id':'NEXTVAL("common.agreement_extended_field_values_seq_id")'})
            ext.update({'agreement_id':_agr['id']})
            if loan != '':
                cur = query(conn,"SELECT * FROM common.agreement_extended_field_values AS ext WHERE ext.agreement_id = " + str(_agr_templ_id) + " AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = '" + ext['agreement_ext_field_id'] + "') LIMIT 1")
            else:
                cur = query(conn,"SELECT * FROM common.agreement_extended_field_values LIMIT 1")
            tab = cur.fetchall()
            try:
                force = ext['#force#']
            except:
                force = False
            if loan != '' and len(tab) == 0:
                if force:
                    cur = query(conn,"SELECT * FROM common.agreement_extended_field_values LIMIT 1")
                    tab = cur.fetchall()
                else:
                    print("Не найден дополнительный реквизит: ",ext['agreement_ext_field_id'])
                    return
            cols = [desc[0] for desc in cur.description]
            _ext = {}
            for r in (tab):
                i = 0
                for c in (cols):
                    try:
                        _ext.update({c:ext[c]})
                    except:
                        if loan != '':
                            _ext.update({c:r[i]})
                        else:
                            try:
                                _ext.update({c:eval(defaults['agreement_extended_field_values'][c])})
                            except:
                                try:
                                    if defaults['agreement_extended_field_values'][c] == '+':
                                        print('Ошибка: реквизит "' + c + '" не может быть пустым!')
                                        return
                                except:
                                    pass
                                _ext.update({c:None})
                    i += 1
            if len(tab) == 0:
                _ext = initfields(cols,_ext,ext,defaults['agreement_extended_field_values'])
            if _ext['date_start'] is None:
                _ext['date_start'] = values['agreement']['date_start']

            _sel = ''
            _ins = ''
            for field in (_ext):
                _ins = _ins + ',' + field
                if type(_ext[field]) is str and field != 'id':
                    _ext[field] = '"' + _ext[field] + '"'
                if type(_ext[field]) is date or type(_ext[field]) is datetime:
                    _ext[field] = '"' + _ext[field].strftime('%Y-%m-%d') + '"::DATE'
                if field == 'agreement_ext_field_id':
                    if type(_ext[field]) is str:
                        _ext[field] = '(SELECT id FROM common.agreement_extended_fields WHERE code = ' + _ext[field] + ')'
                if not (type(_ext[field]) is str):
                    _ext[field] = str(_ext[field])
                _sel = _sel + ('NULL' if (_ext[field] == 'None') else _ext[field]) + ' AS ' + field + ','
            _sel = 'SELECT ' + _sel.strip(',').replace('"','\'')
            _ins = 'INSERT INTO common.agreement_extended_field_values (' + _ins.strip(',') + ') ' + _sel + ' RETURNING id'
            cur = query(conn,_ins)
            tab = cur.fetchall()
            for r in (tab):
                _ext['id'] = r[0]
            _exts.append(_ext)
            if not commit:
                print('agreement_extended_field_values' + '[' + str(_ext['agreement_ext_field_id']) + ']' + ' = ',_ext['id'])

    #---------------------------------------loans.loan_agreement_parameter
    if len(values['loan_agreement_parameter']) > 0:
        _pars = []
        for par in (values['loan_agreement_parameter']):
            par.update({'id':'NEXTVAL("loans.loan_agreement_parameter_id_seq")'})
            par.update({'loan_agreement_id':_lon['id']})
            if loan != '':
                cur = query(conn,"SELECT * FROM loans.loan_agreement_parameter AS par WHERE par.loan_agreement_id = " + str(_lon_templ_id) + " AND par.agreement_parameter_type_id = (SELECT id FROM loans.agreement_parameter_type WHERE code = '" + par['agreement_parameter_type_id'] + "') LIMIT 1")
            else:
                cur = query(conn,"SELECT * FROM loans.loan_agreement_parameter LIMIT 1")
            tab = cur.fetchall()
            try:
                force = par['#force#']
            except:
                force = False
            if loan != '' and len(tab) == 0:
                if force:
                    cur = query(conn,"SELECT * FROM loans.loan_agreement_parameter LIMIT 1")
                    tab = cur.fetchall()
                else:
                    print("Не найден параметр: ",par['agreement_parameter_type_id'])
                    return
            cols = [desc[0] for desc in cur.description]
            _par = {}
            for r in (tab):
                i = 0
                for c in (cols):
                    try:
                        _par.update({c:par[c]})
                    except:
                        if loan != '':
                            _par.update({c:r[i]})
                        else:
                            try:
                                _par.update({c:eval(defaults['loan_agreement_parameter'][c])})
                            except:
                                try:
                                    if defaults['loan_agreement_parameter'][c] == '+':
                                        print('Ошибка: реквизит "' + c + '" не может быть пустым!')
                                        return
                                except:
                                    pass
                                _par.update({c:None})
                    i += 1
            if len(tab) == 0:
                _par = initfields(cols,_par,par,defaults['loan_agreement_parameter'])
            if _par['date_start'] is None:
                _par['date_start'] = values['agreement']['date_start']

            _sel = ''
            _ins = ''
            for field in (_par):
                _ins = _ins + ',' + field
                if type(_par[field]) is str and field != 'id':
                    _par[field] = '"' + _par[field] + '"'
                if type(_par[field]) is date or type(_par[field]) is datetime:
                    if field == 'value':
                        _par[field] = '"' + _par[field].strftime('%d.%m.%Y') + '"'
                    else:
                        _par[field] = '"' + _par[field].strftime('%Y-%m-%d') + '"::DATE'
                if field == 'agreement_parameter_type_id':
                    if type(_par[field]) is str:
                        _par[field] = '(SELECT id FROM loans.agreement_parameter_type WHERE code = ' + _par[field] + ')'
                if not (type(_par[field]) is str):
                    _par[field] = str(_par[field])
                _sel = _sel + ('NULL' if (_par[field] == 'None') else _par[field]) + ' AS ' + field + ','
            _sel = 'SELECT ' + _sel.strip(',').replace('"','\'')
            _ins = 'INSERT INTO loans.loan_agreement_parameter (' + _ins.strip(',') + ') ' + _sel + ' RETURNING id'
            cur = query(conn,_ins)
            tab = cur.fetchall()
            for r in (tab):
                _par['id'] = r[0]
            _pars.append(_par)
            if not commit:
                print('loan_agreement_parameter' + '[' + str(_par['agreement_parameter_type_id']) + ']' + ' = ',_par['id'])

    #---------------------------------------loans.loan_repayment_schedule
    tmp = []
    try:
        tmp = values['loan_repayment_schedule']
    except:
        pass
    if len(tmp) > 0:
        values['loan_repayment_schedule'].update({'id':'NEXTVAL("loans.loan_repay_sched_seq_id")'})
        values['loan_repayment_schedule'].update({'loan_agreement_id':_lon['id']})
        if loan != '':
            cur = query(conn,"SELECT * FROM loans.loan_repayment_schedule AS sch WHERE sch.loan_agreement_id = " + str(_lon_templ_id) + " LIMIT 1")
        else:
            cur = query(conn,"SELECT * FROM loans.loan_repayment_schedule LIMIT 1")
        tab = cur.fetchall()
        if loan != '' and len(tab) == 0:
            print("Не найден займ: ",loan)
            return
        cols = [desc[0] for desc in cur.description]
        _sch = {}
        for r in (tab):
            i = 0
            for c in (cols):
                try:
                    _sch.update({c:values['loan_repayment_schedule'][c]})
                except:
                    if loan != '':
                        _sch.update({c:r[i]})
                    else:
                        try:
                            _sch.update({c:eval(defaults['loan_repayment_schedule'][c])})
                        except:
                            try:
                                if defaults['loan_repayment_schedule'][c] == '+':
                                    print('Ошибка: реквизит "' + c + '" не может быть пустым!')
                                    return
                            except:
                                pass
                            _sch.update({c:None})
                i += 1
        if len(tab) == 0:
            _sch = initfields(cols,_sch,values['loan_repayment_schedule'],defaults['loan_repayment_schedule'])

        _sel = ''
        _ins = ''
        for field in (_sch):
            _ins = _ins + ',' + field
            if type(_sch[field]) is str and field != 'id':
                _sch[field] = '"' + _sch[field] + '"'
            if type(_sch[field]) is date or type(_sch[field]) is datetime:
                _sch[field] = '"' + _sch[field].strftime('%Y-%m-%d') + '"::DATE'
            if not (type(_sch[field]) is str):
                _sch[field] = str(_sch[field])
            _sel = _sel + ('NULL' if (_sch[field] == 'None') else _sch[field]) + ' AS ' + field + ','
        _sel = 'SELECT ' + _sel.strip(',').replace('"','\'')
        _ins = 'INSERT INTO loans.loan_repayment_schedule (' + _ins.strip(',') + ') ' + _sel + ' RETURNING id'
        cur = query(conn,_ins)
        tab = cur.fetchall()
        for r in (tab):
            _sch['id'] = r[0]
        if not commit:
            print('loan_repayment_schedule = ',_sch['id'])

    #---------------------------------------loans.loan_repayment_schedule_item
    tmp = []
    try:
        tmp = values['loan_repayment_schedule']
    except:
        pass
    if len(tmp) > 0:
        _pays = []
        for pay in (values['loan_repayment_schedule']['loan_repayment_schedule_item']):
            pay.update({'id':'NEXTVAL("loans.loan_repay_sched_item_seq_id")'})
            pay.update({'loan_repayment_schedule_id':_sch['id']})
            cur = query(conn,"SELECT * FROM loans.loan_repayment_schedule_item LIMIT 1")
            tab = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            _pay = {}
            _pay = initfields(cols,_pay,pay,defaults['loan_repayment_schedule_item'])
            _sel = ''
            _ins = ''
            for field in (_pay):
                _ins = _ins + ',' + field
                if type(_pay[field]) is str and field != 'id':
                    _pay[field] = '"' + _pay[field] + '"'
                if type(_pay[field]) is date or type(_pay[field]) is datetime:
                    _pay[field] = '"' + _pay[field].strftime('%Y-%m-%d') + '"::DATE'
                if field == 'repayment_type_id':
                    if type(_pay[field]) is str:
                        _pay[field] = '(SELECT id FROM loans.repayment_type WHERE code = ' + _pay[field] + ')'
                if not (type(_pay[field]) is str):
                    _pay[field] = str(_pay[field])
                _sel = _sel + ('NULL' if (_pay[field] == 'None') else _pay[field]) + ' AS ' + field + ','
            _sel = 'SELECT ' + _sel.strip(',').replace('"','\'')
            _ins = 'INSERT INTO loans.loan_repayment_schedule_item (' + _ins.strip(',') + ') ' + _sel + ' RETURNING id'
            cur = query(conn,_ins)
            tab = cur.fetchall()
            for r in (tab):
                _pay['id'] = r[0]
            _pays.append(_pay)

    if commit:
        conn.commit()
    return {'agreement':_agr['id'],'loan_agreement':_lon['id']}

def acct2repay(conn,loan,acct,role,commit):
    rez = []
    qry = "SELECT lon.id,agr.customer_id,lon.currency_id,cur.code,agr.branch_id FROM common.agreement AS agr JOIN loans.loan_agreement AS lon ON lon.agreement_id = agr.id JOIN common.currency AS cur ON cur.id = lon.currency_id WHERE agreement_number = '" + loan + "'"
    cur = query(conn,qry)
    tab = cur.fetchall()
    if len(tab) == 0:
        print('Привязка счета ' + acct + ' к договору ' + loan + ' невозможна, договор не найден!')
        return 0
    for r in tab:
        loan = {'id':r[0],'number':loan}
        cst = r[1]
        val = {'id':r[2],'code':r[3]}
        brn = r[4]
    if acct.startswith('#'):
        qry = "SELECT acc.number FROM accounts.account AS acc JOIN accounts.account_customer AS cus ON cus.customer_id = " + str(cst) + " AND acc.id = cus.account_id JOIN accounts.account_ca AS ca ON ca.account_id = acc.id JOIN accounts.chart_of_accounts AS nps ON nps.code = '" + acct[1:] + "' AND nps.id = ca.chart_of_accounts_id JOIN accounts.account_currency AS cur ON cur.account_id = acc.id AND cur.currency_id = " + str(val['id']) + " WHERE acc.branch_id = " + str(brn)
        cur = query(conn,qry)
        tab = cur.fetchall()
        if len(tab) == 0:
            print('Привязка счета с НПС ' + acct[1:] + ' к договору ' + loan['number'] + ' невозможна, счет в валюте ' + val['code'] + ' не найден!')
            return 0
        for r in tab:
            acct = r[0]
    else:
        qry = "SELECT acc.number,cur.currency_id,ccc.code FROM accounts.account AS acc JOIN accounts.account_currency AS cur ON cur.account_id = acc.id JOIN common.currency AS ccc ON ccc.id = cur.currency_id WHERE number = '" + acct + "'"
        cur = query(conn,qry)
        tab = cur.fetchall()
        if len(tab) == 0:
            print('Привязка счета ' + acct + ' к договору ' + loan['number'] + ' невозможна, счет не найден!')
            return 0
        for r in tab:
            acct = r[0]
            val = {'id':r[1],'code':r[2]}
    for _r in role:
        qry = "SELECT id FROM loans.loan_repayment_account WHERE account_number = '" + acct + "' AND status = 'ACTIVE' AND currency_id = " + str(val['id']) + " AND loan_agreement_id = " + str(loan['id']) + " AND account_sign_type_id = (SELECT id FROM loans.account_sign_type WHERE code = '" + _r + "')"
        cur = query(conn,qry)
        tab = cur.fetchall()
        if len(tab) > 0:
            print('Привязка счета ' + acct + ' к договору ' + loan['number'] + ' с ролью ' + _r + ' уже есть!')
            continue
        qry = "INSERT INTO loans.loan_repayment_account (id,account_number,description,priority,status,currency_id,loan_agreement_id,account_sign_type_id) SELECT NEXTVAL('loans.loan_repay_acc_seq_id') AS id,'" + acct + "' AS account_number,(SELECT description FROM loans.account_sign_type WHERE code = '" + _r + "') AS description,1 AS priority,'ACTIVE' AS status," + str(val['id']) + " AS currency_id," + str(loan['id']) + " AS loan_agreement_id,(SELECT id FROM loans.account_sign_type WHERE code = '" + _r + "') AS account_sign_type_id RETURNING id"
        cur = query(conn,qry)
        tab = cur.fetchall()
        for r in tab:
            rez.append(r[0])

    if commit:
        conn.commit()

    return rez

def acct2gl(conn,acct,commit):
    rez = 0
    qry = "SELECT acc.id,nps.code FROM accounts.account AS acc JOIN accounts.account_ca AS ca ON ca.account_id = acc.id JOIN accounts.chart_of_accounts AS nps ON nps.id = ca.chart_of_accounts_id WHERE acc.number = '" + acct + "'"
    cur = query(conn,qry)
    tab = cur.fetchall()
    if len(tab) == 0:
        print('Cчет ' + acct + ' не найден!')
        return 0
    for r in tab:
        acct = {'id':r[0],'number':acct,'GL_CODE':r[1]}

    qry = "SELECT id FROM accounts.account_extended_field_value AS ext WHERE ext.account_id = " + str(acct['id']) + " AND ext.acc_ext_field_id = (SELECT id FROM accounts.account_extended_field WHERE code = 'GL_CODE')"
    cur = query(conn,qry)
    tab = cur.fetchall()
    if len(tab) != 0:
        print('Для счета ' + acct['number'] + ' код GL_CODE уже существует!')
        return 0

    qry = "INSERT INTO accounts.account_extended_field_value (id,status,date_start,value,account_id,acc_ext_field_id) SELECT NEXTVAL('accounts.acc_ext_field_value_seq_id') AS id,'ACTIVE' AS status,'" + datetime.today().strftime('%Y-%m-%d') + "'::DATE AS date_start,'" + acct['GL_CODE'] + "' AS value," + str(acct['id']) + " AS account_id,(SELECT id FROM accounts.account_extended_field WHERE code = 'GL_CODE') AS acc_ext_field_id RETURNING id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        rez = r[0]

    if commit:
        conn.commit()
    return rez

def ost2graf(conn,number,role,commit):
    role2pay = {
        'PRNCPL': 'DEBT',
        'PRNCPL_OVRD': 'DEBT',
        'INTRST': 'REWARD',
        'INTRST_OVRD': 'REWARD',
        'PNLT_PRNCPL_OVRD': 'PENALTY_FOR_DEBT',
        'PNLT_INTRST_OVRD': 'PENALTY_FOR_REWARD'
    }
    if role not in role2pay.keys():
        return
    qry = "SELECT lon.id,ost.balance,rep.id,TO_DATE(ext.value,'dd.mm.YYYY') FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND agreement_number = '" + number + "' JOIN accounts.subaccount AS sub ON sub.agreement_id = agr.id AND sub.subaccount_type_id = (SELECT id FROM accounts.subaccount_type WHERE code = '" + role + "') JOIN accounts.subaccount_turnover AS ost ON ost.subaccount_id = sub.id JOIN loans.loan_repayment_schedule AS rep ON rep.loan_agreement_id = lon.id JOIN common.agreement_extended_field_values AS ext ON ext.agreement_id = agr.id AND ext.agreement_ext_field_id = (SELECT id FROM common.agreement_extended_fields WHERE code = 'MIGRATION_DATE')"
    cur = query(conn,qry)
    tab = cur.fetchall()
    for r in tab:
        bfo = 0
        aft = 0
        if role in ['PRNCPL','PRNCPL_OVRD']:
            qry = "SELECT pln.amount_principal_after,pln.amount_principal_before FROM loans.loan_repayment_schedule_item AS pln WHERE pln.loan_repayment_schedule_id = " + str(r[2]) + " AND pln.is_migrated_balance_item = FALSE AND pln.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'DEBT') AND pln.target_date <= '" + r[3].strftime('%Y-%m-%d') + "'::DATE ORDER BY pln.target_date DESC LIMIT 1"
            cur = query(conn,qry)
            pln = cur.fetchall()
            for p in pln:
                aft = p[0]
                bfo = p[1]

        qry = "SELECT id FROM loans.loan_repayment_schedule_item WHERE loan_repayment_schedule_id = " + str(r[2]) + " AND is_migrated_balance_item = TRUE AND target_date = '" + add2date(r[3],days=1).strftime('%Y-%m-%d') + "'::DATE AND repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = '" + role2pay[role] + "') AND is_delayed = " + ('TRUE' if role in ['PRNCPL_OVRD','INTRST_OVRD'] else 'FALSE')
        cur = query(conn,qry)
        chk = cur.fetchall()
        if len(chk) == 0:
            qry = "INSERT INTO loans.loan_repayment_schedule_item (id,accrued_amount,amount,amount_principal_after,amount_principal_before,deferred_amount,is_delayed,is_repaid,target_date,loan_repayment_schedule_id,repayment_type_id,is_migrated,is_migrated_balance_item) SELECT NEXTVAL('loans.loan_repay_sched_item_seq_id') AS id," + (str(r[1]) if role in ['INTRST','INTRST_OVRD','PNLT_PRNCPL_OVRD','PNLT_INTRST_OVRD'] else 'NULL') + " AS accrued_amount," + str(r[1]) + " AS amount," + str(aft) + " AS amount_principal_after," + str(bfo) + " AS amount_principal_before," + (str(r[1]) if role in ['INTRST','INTRST_OVRD','PNLT_PRNCPL_OVRD','PNLT_INTRST_OVRD'] else 'NULL') + " AS deferred_amount," + ('TRUE' if role in ['PRNCPL_OVRD','INTRST_OVRD'] else 'FALSE') + " AS is_delayed,FALSE AS is_repaid,'" + add2date(r[3],days=1).strftime('%Y-%m-%d') + "'::DATE AS target_date," + str(r[2]) + " AS loan_repayment_schedule_id,(SELECT id FROM loans.repayment_type WHERE code = '" + role2pay[role] + "') AS repayment_type_id,FALSE AS is_migrated,TRUE AS is_migrated_balance_item RETURNING id"
            if not commit:
                print(qry)
            cur = query(conn,qry)
            tab = cur.fetchall()
            for r in tab:
                rez = r[0]
        else:
            for p in chk:
                qry = "UPDATE loans.loan_repayment_schedule_item SET accrued_amount = " + (str(r[1]) if role in ['INTRST','INTRST_OVRD','PNLT_PRNCPL_OVRD','PNLT_INTRST_OVRD'] else 'NULL') + ",amount = " + str(r[1]) + ",amount_principal_after = " + str(aft) + ", amount_principal_before = " + str(bfo) + ",deferred_amount = " + (str(r[1]) if role in ['INTRST','INTRST_OVRD','PNLT_PRNCPL_OVRD','PNLT_INTRST_OVRD'] else 'NULL') + ",is_delayed = " + ('TRUE' if role in ['PRNCPL_OVRD','INTRST_OVRD','PNLT_PRNCPL_OVRD','PNLT_INTRST_OVRD'] else 'FALSE') + ",is_repaid = FALSE,target_date = '" + add2date(r[3],days=1).strftime('%Y-%m-%d') + "'::DATE,repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = '" + role2pay[role] + "'),is_migrated = FALSE,is_migrated_balance_item = TRUE WHERE id = " + str(p[0])
                if not commit:
                    print(qry)
                cur = query(conn,qry)
    
    if commit:
        conn.commit()
    return

def oper2loan(conn,cfg,who,number,role,amount,debit,idate,details,commit):
    qry = "SELECT lon.id AS lon_id,agr.id AS agr_id,agr.date_start,agr.date_end,cur.code AS curr FROM loans.loan_agreement AS lon JOIN common.agreement AS agr ON agr.id = lon.agreement_id AND agreement_number = '" + number + "' JOIN common.currency AS cur ON cur.id = lon.currency_id"
    cur = query(conn,qry)
    tab = cur.fetchall()
    if len(tab) == 0:
        print('Договор "' + number + '" не найден!')
        return
    for r in tab:
        lon = r[0]
        agr = r[1]
        beg = r[2]
        end = r[3]
        val = r[4]
    nps = cfg['НПС'][cfg['target'][who]['PLAN']]['КРЕДИТ']['КРАТКОСРОЧНЫЙ' if loan_short(beg,end) else 'ДОЛГОСРОЧНЫЙ'][role]
    qry = "SELECT acc.number FROM accounts.account AS acc JOIN accounts.account_ca AS ca ON ca.account_id = acc.id JOIN accounts.chart_of_accounts AS nps ON nps.id = ca.chart_of_accounts_id AND nps.code = '" + nps + "' WHERE acc.acc_agreement_id = " + str(agr)
    cur = query(conn,qry)
    tab = cur.fetchall()
    if len(tab) == 0:
        print('Счет с ролью "' + role + '" на договоре "' + number + '" не найден!')
        return
    for r in tab:
        acc = r[0]

    head = {'Content-type':'application/json','Accept':'text/plain'}
    url = cfg['target'][who]['URL'] + 'execute-event-list'

    ops = {
        'code':'IMPORT_ACCOUNT_BALANCES',
        'eventParameters': {
            'AMOUNT': str(amount),
            'CURRENCY': val,
            'ACCOUNT_CR': (cfg['target'][who]['TRANZ'][val] if debit else acc),
            'ACCOUNT_DB': (acc if debit else cfg['target'][who]['TRANZ'][val]),
            'VALUEDATE': idate.strftime('%d.%m.%Y'),
            'PURPOSE': details,
            'AGREEMENT_NUMBER': number,
            'SUBACCOUNT_TYPE': role,
            'DB_CR': ('CREDIT' if debit else 'DEBIT')
        }
    }

    if commit:
        req = requests.post(url,auth=(cfg['target'][who]['LOGIN'],cfg['target'][who]['PASSWORD']),headers=head,json=ops)
        if (req.status_code != requests.codes.ok):
            print(req.text)
            sys.exit(1)
    else:
        print(ops)
    ost2graf(conn,number,role,commit)

    return
