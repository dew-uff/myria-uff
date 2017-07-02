lineitem = scan('LINEITEM');

q1 = select L_RETURNFLAG,
       L_LINESTATUS,
       sum(L_QUANTITY) as sum_qty,
       sum(L_EXTENDEDPRICE) as sum_base_price,
       sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as sum_disc_price,
       sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)) as sum_charge,
       FLOAT(sum(L_QUANTITY))/count(L_QUANTITY) as avg_qty, -- avg
       FLOAT(sum(L_EXTENDEDPRICE))/count(L_EXTENDEDPRICE) as avg_price, -- avg
       FLOAT(sum(L_DISCOUNT))/count(L_DISCOUNT) as avg_qty, -- avg
       count(L_QUANTITY) as count_order  -- really COUNT(*)
from
       lineitem
where
  L_SHIPDATE <= "1998-09-01";       -- TODO fill in delta automatically; validation is 90 days delta
--auto group by l_returnflag, l_linestatus

-- TODO
-- order by l_returnflag, l_linestatus

store(q1, q1);
