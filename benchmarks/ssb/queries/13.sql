select
  d_year, s_city, sum(lo_revenue - lo_supplycost) as profit
from
  datetable, customer, supplier, part, lineorder
where
  lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_partkey = p_partkey
  and lo_orderdate = d_datekey
  and s_nation = 'UNITED STATES'
  and c_nation = 'UNITED STATES'
  and p_category = 'MFGR#14'
  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
  and (d_year = 1997 or d_year = 1998)
group by
  d_year, s_city, p_brand
order by
  d_year asc, s_city asc, p_brand asc;
