CREATE TABLE date (
  d_datekey INT NOT NULL,
  d_date CHAR(25) NOT NULL,
  d_dayofweek CHAR(10) NOT NULL,
  d_month CHAR(10) NOT NULL,
  d_year INT NOT NULL,
  d_yearmonthnum INT NOT NULL,
  d_yearmonth CHAR(10) NOT NULL,
  d_daynuminweek INT NOT NULL,
  d_daynuminmonth INT NOT NULL,
  d_monthnuminyear INT NOT NULL,
  d_weeknuminyear INT NOT NULL,
  d_daynuminyear INT NOT NULL,
  d_sellingseason CHAR(10) NOT NULL,
  d_lastdayinmonthfl CHAR(1) NOT NULL,
  d_holidayfl CHAR(1) NOT NULL,
  d_weekdayfl CHAR(1) NOT NULL,
  d_weekendfl CHAR(1) NOT NULL
) WITH BLOCKPROPERTIES (
  TYPE split_rowstore,
  BLOCKSIZEMB 4);

CREATE TABLE supplier (
  s_suppkey INT NOT NULL,
  s_name CHAR(25) NOT NULL,
  s_address VARCHAR(40) NOT NULL,
  s_city CHAR(25) NOT NULL,
  s_nation CHAR(25) NOT NULL,
  s_region CHAR(25) NOT NULL,
  s_phone CHAR(15) NOT NULL
) WITH BLOCKPROPERTIES (
  TYPE split_rowstore,
  BLOCKSIZEMB 4);

CREATE TABLE customer (
  c_custkey INT NOT NULL,
  c_name VARCHAR(25) NOT NULL,
  c_address VARCHAR(40) NOT NULL,
  c_city CHAR(25) NOT NULL,
  c_nation CHAR(25) NOT NULL,
  c_region CHAR(25) NOT NULL,
  c_phone CHAR(15) NOT NULL,
  c_mktsegment CHAR(10) NOT NULL
) WITH BLOCKPROPERTIES (
  TYPE split_rowstore,
  BLOCKSIZEMB 4);

CREATE TABLE part (
  p_partkey INT NOT NULL,
  p_name VARCHAR(55) NOT NULL,
  p_mfgr CHAR(25) NOT NULL,
  p_category CHAR(25) NOT NULL,
  p_brand CHAR(25) NOT NULL,
  p_color CHAR(25) NOT NULL,
  p_type VARCHAR(25) NOT NULL,
  p_size INT NOT NULL,
  p_container CHAR(10) NOT NULL,
) WITH BLOCKPROPERTIES (
  TYPE split_rowstore,
  BLOCKSIZEMB 4);

CREATE TABLE lineorder (
  lo_orderkey INT NOT NULL,
  lo_linenumber INT NOT NULL,
  lo_custkey INT NOT NULL,
  lo_partkey INT NOT NULL,
  lo_suppkey INT NOT NULL,
  lo_orderdate DATE NOT NULL,
  lo_orderpriority CHAR(15) NOT NULL,
  lo_shippriority INT NOT NULL,
  lo_quantity DECIMAL NOT NULL,
  lo_extendedprice DECIMAL NOT NULL,
  lo_ordtotalprice DECIMAL NOT NULL,
  lo_discount DECIMAL NOT NULL,
  lo_revenue DECIMAL NOT NULL,
  lo_supplycost DECIMAL NOT NULL,
  lo_tax DECIMAL NOT NULL,
  lo_commitdate DATE NOT NULL,
  lo_shipmode CHAR(10) NOT NULL
) WITH BLOCKPROPERTIES (
  TYPE compressed_columnstore,
  SORT lo_orderkey,
  COMPRESS ALL,
  BLOCKSIZEMB 4);
-- CREATE INDEX sma_index ON lineitem(l_shipdate,l_receiptdate,l_quantity) USING SMA;
