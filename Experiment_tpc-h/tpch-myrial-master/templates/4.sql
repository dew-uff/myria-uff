lineitem = scan('LINEITEM');
orders = scan('ORDERS');

-- $ID$
-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998
exist = select
    O_ORDERKEY as e_key,
    COUNT(O_ORDERKEY) as e_count
    from LINEITEM,
         orders
    where
	    L_ORDERKEY = O_ORDERKEY
	    and L_COMMITDATE < L_RECEIPTDATE;

q4 = select
    O_ORDERPRIORITY,
	count(O_ORDERPRIORITY) as ORDER_COUNT
    from
        orders,
        exist
    where
        e_key = O_ORDERKEY
        and O_ORDERDATE >= ':1'
        and O_ORDERDATE < '1993-10-01' -- ':1' + interval '3' month
        and E_COUNT > 0;

--group by            -- not needed in myriaL
--	o_orderpriority
--order by
--	o_orderpriority;

store(q4, q4);
