select sum(lo_revenue), d_year, p_brand from lineorder, datetable, part, supplier
 where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey =
 s_suppkey and s_region = 'ASIA' and p_brand >= 'MFGR#2221' and p_brand <= 'MFGR#2228'
 group by d_year, p_brand order by d_year, p_brand;
