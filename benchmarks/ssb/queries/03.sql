select sum(lo_extendedprice*lo_discount) as revenue from LINEORDER, datetable
 where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year = 1994
 and lo_discount between 4 and 8 and lo_quantity between 36 and 40;
