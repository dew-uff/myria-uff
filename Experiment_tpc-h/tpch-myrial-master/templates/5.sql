lineitem = scan('LINEITEM');
supplier = scan('SUPPLIER');
nation = scan('NATION');
region = scan('REGION');
customer = scan('CUSTOMER');
orders = scan('ORDERS');

-- $ID$
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- Functional Query Definition
-- Approved February 1998
q5 = select
	N_NAME,
	sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	C_CUSTKEY = O_CUSTKEY
	and L_ORDERKEY = O_ORDERKEY
	and L_SUPPKEY = S_SUPPKEY
	and C_NATIONKEY = S_NATIONKEY
	and S_NATIONKEY = N_NATIONKEY
	and N_REGIONKEY = R_REGIONKEY
	and R_NAME = ':1'
	and O_ORDERDATE >= ':2'
	and O_ORDERDATE < "1995-01-01";
--group by  -- not needed in myriaL
--	n_name
--order by
--	revenue desc;

store(q5, q5);
