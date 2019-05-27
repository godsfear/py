CREATE SCHEMA IF NOT EXISTS xlsx2pg AUTHORIZATION postgres;
SET search_path TO xlsx2pg;

DROP TABLE IF EXISTS xlsx2pg.address;
CREATE TABLE xlsx2pg.address
(
    cust_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    pledge_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    post character varying(20) COLLATE pg_catalog."default",
    country character varying(20) COLLATE pg_catalog."default",
    region character varying(50) COLLATE pg_catalog."default",
    city character varying(50) COLLATE pg_catalog."default",
    street character varying(50) COLLATE pg_catalog."default",
    home character varying(20) COLLATE pg_catalog."default",
    app character varying(20) COLLATE pg_catalog."default",
    CONSTRAINT address_pkey PRIMARY KEY (cust_id, pledge_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.address OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.client;
CREATE TABLE xlsx2pg.client
(
    cust_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    type character varying(3) COLLATE pg_catalog."default" NOT NULL,
    inn character varying(12) COLLATE pg_catalog."default",
    name text COLLATE pg_catalog."default" NOT NULL,
    country character varying(50) COLLATE pg_catalog."default",
    gender character varying(10) COLLATE pg_catalog."default",
    form character varying(20) COLLATE pg_catalog."default",
    date_beg date,
    econ character(1) COLLATE pg_catalog."default",
    CONSTRAINT client_pkey PRIMARY KEY (cust_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.client OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.client_opt;
CREATE TABLE xlsx2pg.client_opt
(
    cust_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    ceo_id character varying(20) COLLATE pg_catalog."default",
    branch character varying(50) COLLATE pg_catalog."default",
    activity character varying(50) COLLATE pg_catalog."default",
    sphere character varying(50) COLLATE pg_catalog."default",
    subject character varying(50) COLLATE pg_catalog."default",
    rate character varying(20) COLLATE pg_catalog."default",
    ave_active_val numeric,
    ave_emploee integer,
    position_ character varying(100) COLLATE pg_catalog."default",
    organisation character varying(100) COLLATE pg_catalog."default",
    partner character varying(100) COLLATE pg_catalog."default",
    fatca boolean,
    CONSTRAINT client_opt_pkey PRIMARY KEY (cust_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.client_opt OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.cust_roles;
CREATE TABLE xlsx2pg.cust_roles
(
    cust_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    type character varying(50) COLLATE pg_catalog."default" NOT NULL,
    date_beg date,
    date_end date,
    CONSTRAINT cust_roles_pkey PRIMARY KEY (cust_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.cust_roles OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.document;
CREATE TABLE xlsx2pg.document
(
    cust_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    type character varying(50) COLLATE pg_catalog."default" NOT NULL,
    number_ character varying(20) COLLATE pg_catalog."default" NOT NULL,
    series character varying(20) COLLATE pg_catalog."default",
    date_beg date,
    issure character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT document_pkey PRIMARY KEY (cust_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.document OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.loan;
CREATE TABLE xlsx2pg.loan
(
    cust_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    loan_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    line_id character varying(50) COLLATE pg_catalog."default",
    gens_id character varying(50) COLLATE pg_catalog."default",
    type character varying(20) COLLATE pg_catalog."default" NOT NULL DEFAULT 1,
    number_ character varying(50) COLLATE pg_catalog."default" NOT NULL,
    branch character varying(50) COLLATE pg_catalog."default",
    product character varying(50) COLLATE pg_catalog."default",
    date_sog date,
    date_beg date NOT NULL,
    date_end date NOT NULL,
    summa numeric NOT NULL,
    currency character varying(20) COLLATE pg_catalog."default",
    day_od numeric,
    day_vn numeric,
    period_od character varying(20) COLLATE pg_catalog."default",
    period_vn character varying(20) COLLATE pg_catalog."default",
    annuitet boolean,
    grace_od numeric,
    grace_vn numeric,
    base character varying COLLATE pg_catalog."default",
    CONSTRAINT loan_pkey PRIMARY KEY (cust_id, loan_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.loan OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.loan_opt;
CREATE TABLE xlsx2pg.loan_opt
(
    loan_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    funding_type character varying(50) COLLATE pg_catalog."default",
    funding_src character varying(50) COLLATE pg_catalog."default",
    object_cred character varying(50) COLLATE pg_catalog."default",
    aim_cred character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT loan_opt_pkey PRIMARY KEY (loan_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.loan_opt OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.loan_roles;
CREATE TABLE xlsx2pg.loan_roles
(
    loan_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    cust_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    date_beg date,
    date_end date,
    CONSTRAINT loan_roles_pkey PRIMARY KEY (loan_id, cust_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.loan_roles OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.loan_sum;
CREATE TABLE xlsx2pg.loan_sum
(
    loan_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    summa numeric NOT NULL,
    CONSTRAINT loan_sum_pkey PRIMARY KEY (loan_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.loan_sum OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.phone;
CREATE TABLE xlsx2pg.phone
(
    cust_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    type character varying(20) COLLATE pg_catalog."default",
    number_ character varying(200) COLLATE pg_catalog."default",
    CONSTRAINT phone_pkey PRIMARY KEY (cust_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.phone OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.pledge;
CREATE TABLE xlsx2pg.pledge
(
    pledge_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    date_beg date NOT NULL,
    date_end date,
    date_bal date,
    type character varying(50) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT pledge_pkey PRIMARY KEY (pledge_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.pledge OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.pledge_cust;
CREATE TABLE xlsx2pg.pledge_cust
(
    pledge_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    cust_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT pledge_cust_pkey PRIMARY KEY (pledge_id, cust_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.pledge_cust OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.pledge_desc;
CREATE TABLE xlsx2pg.pledge_desc
(
    pledge_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    pledge character varying(100) COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT pledge_description_pkey PRIMARY KEY (pledge_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.pledge_desc OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.pledge_loan;
CREATE TABLE xlsx2pg.pledge_loan
(
    pledge_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    loan_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT pledge_loan_pkey PRIMARY KEY (pledge_id, loan_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.pledge_loan OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.pledge_sum;
CREATE TABLE xlsx2pg.pledge_sum
(
    pledge_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    since date,
    type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    appraiser character varying(200) COLLATE pg_catalog."default",
    document character varying(20) COLLATE pg_catalog."default",
    number_ character varying(20) COLLATE pg_catalog."default",
    currency character varying(20) COLLATE pg_catalog."default",
    summa numeric NOT NULL,
    CONSTRAINT pledge_sum_pkey PRIMARY KEY (pledge_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.pledge_sum OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.rates;
CREATE TABLE xlsx2pg.rates
(
    loan_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    fixed boolean NOT NULL,
    since date,
    currency character varying(20) COLLATE pg_catalog."default",
    rate numeric NOT NULL,
    float_ boolean NOT NULL DEFAULT false,
    CONSTRAINT rates_pkey PRIMARY KEY (loan_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.rates OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.repayment;
CREATE TABLE xlsx2pg.repayment
(
    loan_id character varying(20) COLLATE pg_catalog."default" NOT NULL,
    since date NOT NULL,
    type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    summa numeric NOT NULL,
    CONSTRAINT repayment_pkey PRIMARY KEY (loan_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.repayment OWNER to postgres;

DROP TABLE IF EXISTS xlsx2pg.restruct;
CREATE TABLE xlsx2pg.restruct
(
    loan_id character varying(50) COLLATE pg_catalog."default" NOT NULL,
    since date NOT NULL,
    type character varying(20) COLLATE pg_catalog."default" NOT NULL,
    comment text COLLATE pg_catalog."default",
    CONSTRAINT restruct_pkey PRIMARY KEY (loan_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE xlsx2pg.restruct OWNER to postgres;

COMMIT;
