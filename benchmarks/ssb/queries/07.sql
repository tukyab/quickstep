select
  c_nation, s_nation, d_year, sum(lo_revenue) as revenue
from
  customer, lineorder, supplier, datetable
where
  lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_orderdate = d_datekey
  and c_region = 'ASIA'
  and s_region = 'ASIA'
  and d_year between 1992 and 1997
group by
  c_nation, s_nation, d_year
order by
  d_year asc, revenue desc;
