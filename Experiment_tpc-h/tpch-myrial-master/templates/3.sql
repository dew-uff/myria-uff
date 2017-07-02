lineitem = scan('LINEITEM');
customer = scan('CUSTOMER');
orders = scan('ORDERS');
-- $ID$
-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- Functional Query Definition
-- Approved February 1998

q3 = select
	L_ORDERKEY,
	sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,
	O_ORDERDATE,
	O_SHIPPRIORITY
from
	customer,
	orders,
	lineitem
where
	C_MKTSEGMENT = ':1'
	and C_CUSTKEY = O_CUSTKEY
	and L_ORDERKEY = O_ORDERKEY
	and O_ORDERDATE < ':2'
	and L_SHIPDATE > ':2';
--group by         -- myrial doesn't need these
--	l_orderkey,
--	o_orderdate,
--	o_shippriority
--TODO order by
--TODO 	revenue desc,
--TODO 	o_orderdate;

store(q3, q3);
