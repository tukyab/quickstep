select
  d_year, s_nation, sum(lo_revenue - lo_supplycost) as profit
from
  datetable, customer, supplier, part, lineorder
where
  lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_partkey = p_partkey
  and lo_orderdate = d_datekey
  and c_region = 'AMERICA'
  and s_region = 'AMERICA'
  and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
  and (d_year = 1997 or d_year = 1998)
group by
  d_year, p_category, s_nation
order by
  d_year asc, p_category asc, s_nation asc;