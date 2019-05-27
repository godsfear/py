DROP FUNCTION pay_period(num VARCHAR(100),typ BIGINT);
CREATE OR REPLACE FUNCTION pay_period(num VARCHAR(100),typ BIGINT) RETURNS TEXT AS $pay_period$
DECLARE
    r1 RECORD;
    pdate DATE;
    rez TEXT;
    p FLOAT;
BEGIN
	SELECT rep.id AS rep,agr.date_end::DATE AS edate,agr.date_start::DATE AS bdate
	FROM common.agreement AS agr
	JOIN loans.loan_agreement AS la ON la.agreement_id = agr.id
	JOIN loans.loan_repayment_schedule AS rep ON la.id = rep.loan_agreement_id AND rep.type = 'TYPE_PAYMENTS' AND rep.status = 'ACTIVE'
	WHERE agr.agreement_number = num INTO r1;
    SELECT target_date::DATE FROM loans.loan_repayment_schedule_item AS plan WHERE plan.loan_repayment_schedule_id = r1.rep AND plan.target_date < r1.edate AND plan.repayment_type_id = typ AND NOT plan.is_migrated_balance_item ORDER BY plan.target_date DESC LIMIT 1 INTO pdate;
    IF pdate = r1.bdate OR pdate IS NULL THEN
    	rez = 'AT_THE_END';
    ELSE
		p := (r1.edate - pdate);
    	p := ROUND(p / 30);
        rez = CASE
        	WHEN p = 1 THEN 'MONTHLY'
            WHEN p = 2 THEN 'EVERY_2_MONTH'
            WHEN p = 3 THEN 'EVERY_3_MONTH'
            WHEN p = 5 OR p = 6 THEN 'EVERY_6_MONTH'
            WHEN p = 8 OR p = 9 THEN 'EVERY_9_MONTH'
            WHEN p = 11 OR p = 12 THEN 'EVERY_12_MONTH'
            ELSE NULL
			--p::TEXT || ',' || pdate::TEXT || ',' || r1.edate::TEXT
        END;
    END IF;
    RETURN rez;
END;
$pay_period$ LANGUAGE plpgsql VOLATILE;

SELECT pay_period(tab.num,13),tab.num FROM (SELECT agr.agreement_number::VARCHAR(100) AS num FROM common.agreement AS agr WHERE agr.agreement_number
                                            IN ('6024/А.','4617/А','6016/А.','6019/А.','6020/А.','6017/А.','6015/А.','6004/А.','6005/А.','6006/А.','0001/308/RG')) AS tab;

CREATE OR REPLACE FUNCTION stage2cust() RETURNS SETOF RECORD AS $stage2cust$
DECLARE
    r1 RECORD;
    r2 RECORD;
BEGIN
	FOR r1 IN (SELECT DISTINCT(bin_iin) FROM stagearea.cln4fond WHERE NOT bin_iin IS NULL AND bin_iin::BIGINT <> 0) LOOP
		RETURN QUERY SELECT bin_iin,clnid,customers.customer_extended_field_values.customer_id FROM stagearea.cln4fond JOIN
        customers.customer_extended_field_values ON customers.customer_extended_field_values.cust_ext_field_id = 6 AND
        customers.customer_extended_field_values.value = bin_iin WHERE stagearea.cln4fond.bin_iin = r1.bin_iin LIMIT 1;
	END LOOP;
END;
$stage2cust$ LANGUAGE plpgsql VOLATILE;

UPDATE customers.customer_extended_field_values SET value = tvals.clnid::VARCHAR(255)
FROM (SELECT tab.bin_iin,tab.clnid AS clnid,ex.value,ex.customer_id AS customer_id FROM stage2cust() AS tab(bin_iin TEXT,clnid NUMERIC,customer_id BIGINT)
      JOIN customers.customer_extended_field_values AS ex ON ex.customer_id = tab.customer_id AND ex.cust_ext_field_id = 10) AS tvals
WHERE customers.customer_extended_field_values.customer_id = tvals.customer_id AND customers.customer_extended_field_values.cust_ext_field_id = 10;

CREATE OR REPLACE FUNCTION cust2portf() RETURNS SETOF RECORD AS $cust2portf$
DECLARE
   r RECORD;
   rw BIGINT;
   cod BIGINT;
   val BIGINT;
BEGIN
	FOR r IN (SELECT cus.id,ex.value,tab.clnid FROM customers.customer AS cus JOIN customers.customer_extended_field_values AS ex ON ex.customer_id = cus.id AND ex.cust_ext_field_id = 6 JOIN stage2cust() AS tab(bin_iin TEXT,clnid NUMERIC,customer_id BIGINT) ON cus.id = tab.customer_id WHERE NOT cus.id IN (SELECT ex.customer_id FROM customers.customer_extended_field_values AS ex WHERE ex.cust_ext_field_id = 10)) LOOP
        rw := nextval('customers.cust_ext_field_rows_seq');
        INSERT INTO customers.cust_ext_field_rows (id,cust_ext_field_group_id) VALUES (rw,109);
        cod := nextval('customers.customer_extended_field_values_seq_id');
        INSERT INTO customers.customer_extended_field_values (id,status,value,mdm_row_id,customer_id,row_id,cust_ext_field_id)
        VALUES (cod,'ACTIVE','BTA',1,r.id,rw,11);
        val := nextval('customers.customer_extended_field_values_seq_id');
        INSERT INTO customers.customer_extended_field_values (id,status,value,mdm_row_id,customer_id,row_id,cust_ext_field_id)
        VALUES (val,'ACTIVE',r.clnid::VARCHAR(255),NULL,r.id,rw,10);
        RETURN QUERY SELECT cod,val;
	END LOOP;
END;
$cust2portf$ LANGUAGE plpgsql VOLATILE;

SELECT * FROM cust2portf() AS tab(cod_id BIGINT,val_id BIGINT);

/*DROP FUNCTION _plan(_plan DATE,_od NUMERIC,_pr NUMERIC,_ost NUMERIC,_def NUMERIC);
CREATE OR REPLACE FUNCTION _plan(num VARCHAR(100),xdate DATE,back BOOLEAN) RETURNS TABLE(_plan DATE,_od NUMERIC,_pr NUMERIC,_ost NUMERIC,_def NUMERIC) AS $_plan$
DECLARE
	rep BIGINT;
	pr RECORD;
	od RECORD;
BEGIN
	SELECT rep.id AS rep,la.amount
	FROM common.agreement AS agr
	JOIN loans.loan_agreement AS la ON la.agreement_id = agr.id
	JOIN loans.loan_repayment_schedule AS rep ON la.id = rep.loan_agreement_id AND rep.type = 'TYPE_PAYMENTS' AND rep.status = 'ACTIVE'
	WHERE agr.agreement_number = num INTO rep;
	IF back THEN
		SELECT target_date::DATE,amount,amount_principal_before,deferred_amount FROM loans.loan_repayment_schedule_item AS plan WHERE plan.loan_repayment_schedule_id = rep AND plan.target_date <= xdate AND plan.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND NOT plan.is_migrated_balance_item ORDER BY plan.target_date DESC LIMIT 1 INTO pr;
		SELECT target_date::DATE,amount,amount_principal_before,deferred_amount FROM loans.loan_repayment_schedule_item AS plan WHERE plan.loan_repayment_schedule_id = rep AND plan.target_date <= xdate AND plan.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'DEBT') AND NOT plan.is_migrated_balance_item ORDER BY plan.target_date DESC LIMIT 1 INTO od;
	ELSE
		SELECT target_date::DATE,amount,amount_principal_before,deferred_amount FROM loans.loan_repayment_schedule_item AS plan WHERE plan.loan_repayment_schedule_id = rep AND plan.target_date >= xdate AND plan.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'REWARD') AND NOT plan.is_migrated_balance_item ORDER BY plan.target_date DESC LIMIT 1 INTO pr;
		SELECT target_date::DATE,amount,amount_principal_before,deferred_amount FROM loans.loan_repayment_schedule_item AS plan WHERE plan.loan_repayment_schedule_id = rep AND plan.target_date >= xdate AND plan.repayment_type_id = (SELECT id FROM loans.repayment_type WHERE code = 'DEBT') AND NOT plan.is_migrated_balance_item ORDER BY plan.target_date DESC LIMIT 1 INTO od;
	END IF;
	IF (od IS NULL) AND NOT (pr IS NULL) THEN
		IF od.target_date = pr.target_date THEN
			RETURN SELECT od.target_date,od.amount,pr.amount,od.amount_principal_before,pr.deferred_amount;
		ELSE
			IF ABS(od.target_date - xdate) < ABS(pr.target_date - xdate) THEN
				RETURN SELECT od.target_date,od.amount,0::NUMERIC,od.amount_principal_before,0::NUMERIC;
			END IF;
		END IF;
	END IF;
END;
$_plan$ LANGUAGE plpgsql VOLATILE;

SELECT * FROM _plan('6061/А'::VARCHAR(100),13::BIGINT,NOW()::DATE,TRUE);*/

CREATE OR REPLACE FUNCTION _days360(beg_date DATE,end_date DATE) RETURNS INTEGER AS $_days360$
DECLARE
	rez INTEGER;
	ddd DATE;
BEGIN
	ddd := beg_date;
	rez := 0;
	WHILE (ddd <= end_date) LOOP
		ddd := ddd + 1;
		rez := rez + 1;
		IF ddd <> beg_date AND EXTRACT(DAY FROM ddd) = 1 THEN
			rez = rez + 30 - EXTRACT(DAY FROM (ddd - 1));
		END IF;
	END LOOP;
	RETURN rez - 1;
END;
$_days360$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION _deferred(num VARCHAR(100),xdate DATE) RETURNS TABLE(def NUMERIC,acr NUMERIC) AS $_deferred$
DECLARE
	r1 RECORD;
	r2 RECORD;
	d1 DATE;
	pr RECORD;
	def NUMERIC;
	acr NUMERIC;
	rew BIGINT;
	dbt BIGINT;
	ost NUMERIC;
	prc NUMERIC;
	d INTEGER;
BEGIN
	SELECT id FROM loans.repayment_type WHERE code = 'REWARD' INTO rew;
	SELECT id FROM loans.repayment_type WHERE code = 'DEBT' INTO dbt;
	SELECT rep.id AS pln,a.date_start AS opn,a.id AS ida,rp.value AS rate,bas.days_per_month AS dmonth,rp.days_per_year AS dyear FROM common.agreement AS a JOIN loans.loan_agreement AS la ON la.agreement_id = a.id JOIN loans.loan_repayment_schedule AS rep ON la.id = rep.loan_agreement_id AND rep.type = 'TYPE_PAYMENTS' AND rep.status = 'ACTIVE' JOIN loans.loan_agreement_rp_value AS rp ON rp.loan_agreement_id = la.id AND rp.interest_rate_penalty_type_id = (SELECT id FROM loans.interest_rate_penalty_type WHERE code = 'BASE') AND rp.repayment_type_id = (SELECT id FROM loans.repayment_type where code = 'REWARD') JOIN loans.accrual_basis AS bas ON bas.id = la.accrual_basis_id WHERE a.agreement_number = num ORDER BY rp.start_date DESC LIMIT 1 INTO r1;
	IF xdate IS NULL THEN
		SELECT TO_DATE(ext.value,'DD.MM.YYYY') FROM common.agreement_extended_field_values AS ext WHERE ext.agreement_id = r1.ida AND ext.agreement_ext_field_id = 4 INTO xdate;
	END IF;
	d1 := NULL;
	def := 0;
	SELECT amount_principal_before FROM loans.loan_repayment_schedule_item AS plan WHERE plan.loan_repayment_schedule_id = r1.pln AND plan.target_date >= xdate AND plan.repayment_type_id = dbt AND NOT plan.is_migrated_balance_item ORDER BY plan.target_date LIMIT 1 INTO ost;
	SELECT target_date::DATE FROM loans.loan_repayment_schedule_item AS plan WHERE plan.loan_repayment_schedule_id = r1.pln AND plan.target_date <= xdate AND plan.repayment_type_id = rew AND NOT plan.is_migrated_balance_item ORDER BY plan.target_date DESC LIMIT 1 INTO d1;
	IF (d1 IS NULL) THEN
		d1 := r1.opn::DATE;
	END IF;
	pr := NULL;
	acr := 0;
	SELECT target_date::DATE,amount,deferred_amount FROM loans.loan_repayment_schedule_item AS plan WHERE plan.loan_repayment_schedule_id = r1.pln AND plan.target_date >= xdate AND plan.repayment_type_id = rew AND NOT plan.is_migrated_balance_item ORDER BY plan.target_date LIMIT 1 INTO pr;
	IF NOT (pr IS NULL) AND ABS(pr.deferred_amount) <> 0 THEN
		IF r1.dmonth = 0 THEN
			d := pr.target_date - xdate;
		ELSE
			d := _days360(xdate,pr.target_date);
		END IF;
		prc := ROUND(ost * r1.rate * d / r1.dyear / 100,2);
		def := pr.amount - prc;
		acr := def;
	ELSE
		def := 0;
	END IF;
	FOR r2 IN (SELECT deferred_amount FROM loans.loan_repayment_schedule_item AS plan WHERE plan.loan_repayment_schedule_id = r1.pln AND plan.target_date > pr.target_date AND plan.repayment_type_id = rew AND NOT plan.is_migrated_balance_item ORDER BY plan.target_date) LOOP
		acr := acr + r2.deferred_amount;
	END LOOP;
	IF def = 0 THEN
		def = NULL;
	END IF;
	RETURN QUERY SELECT def,acr;
END;
$_deferred$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION _f(num TEXT,bal TEXT,brn TEXT,typ TEXT,des TEXT) RETURNS BIGINT AS $_f$
DECLARE
	id_a BIGINT;
BEGIN
END;
$_f$ LANGUAGE plpgsql VOLATILE;

SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = 'executor_mig';
DROP DATABASE executor_mig;
