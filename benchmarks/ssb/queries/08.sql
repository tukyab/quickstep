select
  c_city, s_city, d_year, sum(lo_revenue) as revenue
from
  customer, lineorder, supplier, datetable
where
  lo_custkey = c_custkey
  and lo_suppkey = s_suppkey
  and lo_orderdate = d_datekey
  and c_nation = 'UNITED STATES'
  and s_nation = 'UNITED STATES'
  and d_year between 1992 and 1997
group by
  c_city, s_city, d_year
order by
  d_year asc, revenue desc;
