select
  sum(lo_extendedprice*lo_discount) as revenue
from
  lineorder, datetable
where
  lo_orderdate = d_datekey
  and d_yearmonthnum = 199401
  and lo_discount between 3 and 7 and
  lo_quantity between 26 and 35;
