SET client_min_messages = notice;
BEGIN;

\set ch_user default
\getenv ch_user CLICKHOUSE_USER
\set ch_pass
\getenv ch_pass CLICKHOUSE_PASSWORD
\set ch_host localhost
\getenv ch_host CLICKHOUSE_HOST

CREATE EXTENSION pg_clickhouse;
CREATE SERVER ch_tpch_svr FOREIGN DATA WRAPPER clickhouse_fdw
    OPTIONS(dbname 'tpch', driver 'binary', host :'ch_host');
CREATE USER MAPPING FOR CURRENT_USER SERVER ch_tpch_svr OPTIONS (user :'ch_user', password :'ch_pass');

CREATE SCHEMA ch;
IMPORT FOREIGN SCHEMA tpch FROM SERVER ch_tpch_svr INTO ch;

CREATE SCHEMA pg;
SET search_path = pg;

CREATE TABLE REGION
(
    R_REGIONKEY INTEGER  NOT NULL PRIMARY KEY,
    R_NAME      CHAR(25) NOT NULL,
    R_COMMENT   VARCHAR(152)
);

CREATE TABLE NATION
(
    N_NATIONKEY INTEGER  NOT NULL PRIMARY KEY,
    N_NAME      CHAR(25) NOT NULL,
    N_REGIONKEY INTEGER  NOT NULL,
    N_COMMENT   VARCHAR(152),
    FOREIGN KEY (N_REGIONKEY) REFERENCES REGION (R_REGIONKEY)
);

CREATE TABLE PART
(
    P_PARTKEY     INTEGER        NOT NULL PRIMARY KEY,
    P_NAME        VARCHAR(55)    NOT NULL,
    P_MFGR        CHAR(25)       NOT NULL,
    P_BRAND       CHAR(10)       NOT NULL,
    P_TYPE        VARCHAR(25)    NOT NULL,
    P_SIZE        INTEGER        NOT NULL,
    P_CONTAINER   CHAR(10)       NOT NULL,
    P_RETAILPRICE DECIMAL(15, 2) NOT NULL,
    P_COMMENT     VARCHAR(23)    NOT NULL
);
CREATE INDEX ON PART (P_NAME);

CREATE TABLE SUPPLIER
(
    S_SUPPKEY   INTEGER        NOT NULL PRIMARY KEY,
    S_NAME      CHAR(25)       NOT NULL,
    S_ADDRESS   VARCHAR(40)    NOT NULL,
    S_NATIONKEY INTEGER        NOT NULL,
    S_PHONE     CHAR(15)       NOT NULL,
    S_ACCTBAL   DECIMAL(15, 2) NOT NULL,
    S_COMMENT   VARCHAR(101)   NOT NULL,
    FOREIGN KEY (S_NATIONKEY) REFERENCES NATION (N_NATIONKEY)
);
CREATE INDEX ON SUPPLIER (S_NAME);

CREATE TABLE PARTSUPP
(
    PS_PARTKEY    INTEGER        NOT NULL,
    PS_SUPPKEY    INTEGER        NOT NULL,
    PS_AVAILQTY   INTEGER        NOT NULL,
    PS_SUPPLYCOST DECIMAL(15, 2) NOT NULL,
    PS_COMMENT    VARCHAR(199)   NOT NULL,
    PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY),
    FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER (S_SUPPKEY),
    FOREIGN KEY (PS_PARTKEY) REFERENCES PART (P_PARTKEY)
);

CREATE TABLE CUSTOMER
(
    C_CUSTKEY    INTEGER        NOT NULL PRIMARY KEY,
    C_NAME       VARCHAR(25)    NOT NULL,
    C_ADDRESS    VARCHAR(40)    NOT NULL,
    C_NATIONKEY  INTEGER        NOT NULL,
    C_PHONE      CHAR(15)       NOT NULL,
    C_ACCTBAL    DECIMAL(15, 2) NOT NULL,
    C_MKTSEGMENT CHAR(10)       NOT NULL,
    C_COMMENT    VARCHAR(117)   NOT NULL,
    FOREIGN KEY (C_NATIONKEY) REFERENCES NATION (N_NATIONKEY)
);

CREATE TABLE ORDERS
(
    O_ORDERKEY      INTEGER        NOT NULL PRIMARY KEY,
    O_CUSTKEY       INTEGER        NOT NULL,
    O_ORDERSTATUS   CHAR(1)        NOT NULL,
    O_TOTALPRICE    DECIMAL(15, 2) NOT NULL,
    O_ORDERDATE     DATE           NOT NULL,
    O_ORDERPRIORITY CHAR(15)       NOT NULL,
    O_CLERK         CHAR(15)       NOT NULL,
    O_SHIPPRIORITY  INTEGER        NOT NULL,
    O_COMMENT       VARCHAR(79)    NOT NULL,
    FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER (C_CUSTKEY)
);
CREATE INDEX ON ORDERS (O_ORDERDATE);

-- DROP TABLE LINEITEM;
CREATE TABLE LINEITEM
(
    L_ORDERKEY      INTEGER        NOT NULL,
    L_PARTKEY       INTEGER        NOT NULL,
    L_SUPPKEY       INTEGER        NOT NULL,
    L_LINENUMBER    INTEGER        NOT NULL,
    L_QUANTITY      NUMERIC(15, 2) NOT NULL,
    L_EXTENDEDPRICE NUMERIC(15, 2) NOT NULL,
    L_DISCOUNT      NUMERIC(15, 2) NOT NULL,
    L_TAX           NUMERIC(15, 2) NOT NULL,
    L_RETURNFLAG    CHAR(1)        NOT NULL,
    L_LINESTATUS    CHAR(1)        NOT NULL,
    L_SHIPDATE      DATE           NOT NULL,
    L_COMMITDATE    DATE           NOT NULL,
    L_RECEIPTDATE   DATE           NOT NULL,
    L_SHIPINSTRUCT  CHAR(25)       NOT NULL,
    L_SHIPMODE      CHAR(10)       NOT NULL,
    L_COMMENT       VARCHAR(44)    NOT NULL
    -- PRIMARY KEY (L_ORDERKEY, L_LINENUMBER),
    -- FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS (O_ORDERKEY)
    -- FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP (PS_PARTKEY, PS_SUPPKEY)
);

\set ECHO queries
-- Scaling factor 1
COPY region   FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/1/region.tbl'   DELIMITER '|';
COPY nation   FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/1/nation.tbl'   DELIMITER '|';
COPY part     FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/1/part.tbl'     DELIMITER '|';
COPY supplier FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/1/supplier.tbl' DELIMITER '|';
COPY partsupp FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/1/partsupp.tbl' DELIMITER '|';
COPY customer FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/1/customer.tbl' DELIMITER '|';
COPY orders   FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/1/orders.tbl'   DELIMITER '|';
COPY lineitem FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/1/lineitem.tbl' DELIMITER '|';

/*

-- Scaling factor 100
COPY region   FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/100/region.tbl'   DELIMITER '|';
COPY nation   FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/100/nation.tbl'   DELIMITER '|';
COPY part     FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/100/part.tbl'     DELIMITER '|';
COPY supplier FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/100/supplier.tbl' DELIMITER '|';
COPY partsupp FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/100/partsupp.tbl' DELIMITER '|';
COPY customer FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/100/customer.tbl' DELIMITER '|';
COPY orders   FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/100/orders.tbl'   DELIMITER '|';
COPY lineitem FROM PROGRAM 'curl -sSL https://clickhouse-datasets.s3.amazonaws.com/h/100/lineitem.tbl' DELIMITER '|';

*/

CREATE INDEX ON LINEITEM (L_PARTKEY, L_SUPPKEY);
CREATE INDEX ON LINEITEM (L_ORDERKEY);
CREATE INDEX ON LINEITEM (L_SHIPDATE);
ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER);
-- ALTER TABLE LINEITEM ADD FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP (PS_PARTKEY, PS_SUPPKEY);
-- ALTER TABLE LINEITEM ADD FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS (O_ORDERKEY);
ANALYZE;

COMMIT;
