lineitem = scan('LINEITEM');

-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
q6 = select
	sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue
from
	lineitem
where
	L_SHIPDATE >= '1994-01-01'
	and L_SHIPDATE < '1995-01-01' --date ':1' + interval '1' year
	and L_DISCOUNT >= :2 - 0.01
    and L_DISCOUNT <= :2 + 0.01
	and L_QUANTITY < :3;

store(q6, q6);
