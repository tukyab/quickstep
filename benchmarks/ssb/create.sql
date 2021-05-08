CREATE TABLE datetable (
  d_datekey INT NOT NULL,
  d_date VARCHAR(18) NOT NULL,
  d_dayofweek VARCHAR(9) NOT NULL,
  d_month VARCHAR(9) NOT NULL,
  d_year INT NOT NULL,
  d_yearmonthnum INT NOT NULL,
  d_yearmonth VARCHAR(7) NOT NULL,
  d_daynuminweek INT NOT NULL,
  d_daynuminmonth INT NOT NULL,
  d_daynuminyear INT NOT NULL,
  d_monthnuminyear INT NOT NULL,
  d_weeknuminyear INT NOT NULL,
  d_sellingseason VARCHAR(12) NOT NULL,
  d_lastdayinweekfl VARCHAR(2) NOT NULL,
  d_lastdayinmonthfl VARCHAR(2) NOT NULL,
  d_holidayfl VARCHAR(2) NOT NULL,
  d_weekdayfl VARCHAR(2) NOT NULL
) WITH BLOCKPROPERTIES (
  TYPE split_rowstore,
  BLOCKSIZEMB 4);

CREATE TABLE supplier (
  s_suppkey INT NOT NULL,
  s_name VARCHAR(25) NOT NULL,
  s_address VARCHAR(25) NOT NULL,
  s_city VARCHAR(10) NOT NULL,
  s_nation VARCHAR(15) NOT NULL,
  s_region VARCHAR(12) NOT NULL,
  s_phone VARCHAR(15) NOT NULL
) WITH BLOCKPROPERTIES (
  TYPE split_rowstore,
  BLOCKSIZEMB 4);

CREATE TABLE customer (
  c_custkey INT NOT NULL,
  c_name VARCHAR(25) NOT NULL,
  c_address VARCHAR(25) NOT NULL,
  c_city VARCHAR(10) NOT NULL,
  c_nation VARCHAR(15) NOT NULL,
  c_region VARCHAR(12) NOT NULL,
  c_phone VARCHAR(15) NOT NULL,
  c_mktsegment VARCHAR(20) NOT NULL
) WITH BLOCKPROPERTIES (
  TYPE split_rowstore,
  BLOCKSIZEMB 4);

CREATE TABLE part (
  p_partkey INT NOT NULL,
  p_name VARCHAR(22) NOT NULL,
  p_mfgr VARCHAR(6) NOT NULL,
  p_category VARCHAR(7) NOT NULL,
  p_brand VARCHAR(10) NOT NULL,
  p_color VARCHAR(11) NOT NULL,
  p_type VARCHAR(25) NOT NULL,
  p_size INT NOT NULL,
  p_container VARCHAR(10) NOT NULL
) WITH BLOCKPROPERTIES (
  TYPE split_rowstore,
  BLOCKSIZEMB 4);

CREATE TABLE lineorder (
  lo_orderkey INT NOT NULL,
  lo_linenumber INT NOT NULL,
  lo_custkey INT NOT NULL,
  lo_partkey INT NOT NULL,
  lo_suppkey INT NOT NULL,
  lo_orderdate INT NOT NULL,
  lo_orderpriority VARCHAR(20) NOT NULL,
  lo_shippriority INT NOT NULL,
  lo_quantity INT NOT NULL,
  lo_extendedprice INT NOT NULL,
  lo_ordtotalprice INT NOT NULL,
  lo_discount INT NOT NULL,
  lo_revenue INT NOT NULL,
  lo_supplycost INT NOT NULL,
  lo_tax INT NOT NULL,
  lo_commitdate INT NOT NULL,
  lo_shipmode VARCHAR(10) NOT NULL
) WITH BLOCKPROPERTIES (
  TYPE compressed_columnstore,
  SORT lo_orderkey,
  COMPRESS ALL,
  BLOCKSIZEMB 4);
-- CREATE INDEX sma_index ON lineitem(l_shipdate,l_receiptdate,l_quantity) USING SMA;
