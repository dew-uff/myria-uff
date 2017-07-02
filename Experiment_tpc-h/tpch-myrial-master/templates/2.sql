part = scan('PART');
lineitem = scan('LINEITEM');
supplier = scan('SUPPLIER');
nation = scan('NATION');
region = scan('REGION');
partsupp = scan('PARTSUPP');

-- NOTES: we might rewrite the myl query to force some optimizations:
-- from TPC-H analyzed:
-- CP5.2: Q2 shows a frequent pattern: a correlated subquery which computes an aggregate that is subsequently used in a selection predicate of a similarly looking outer query ("select the minimum cost part supplier for a certain part"). Here the outer query has additional restrictions (on part type and size) that are not present in the correlated subquery, but should be propagated to it.

-- note that since Raco doesn't support correlated subqueries,
-- we need to rewrite 2.sql so that the subquery groups by partkey and selects only relevant parts
-- then we add a join by that partkey in the outer query
-- (see min_partkey=p_partkey)

min_ps_supplycost_relation = select
			P_PARTKEY as min_partkey,
            min(PS_SUPPLYCOST) as min_ps_supplycost
		from
      part,
			partsupp,
			supplier,
			nation,
			region
		where
			P_PARTKEY = PS_PARTKEY
	    and P_SIZE = :1   -- [1 , 50]
			and P_TYPE like '%:2' -- syllable3
			and S_SUPPKEY = PS_SUPPKEY
			and S_NATIONKEY = N_NATIONKEY
			and N_REGIONKEY = R_REGIONKEY
			and R_NAME = ':3';

q2 = select
	S_ACCTBAL,
	S_NAME,
	N_NAME,
	P_PARTKEY,
	P_MFGR,
	S_ADDRESS,
	S_PHONE,
	S_COMMENT
from
	part,
	partsupp,
	supplier,
	nation,
	region,
    min_ps_supplycost_relation
where
	P_PARTKEY = PS_PARTKEY
	and S_SUPPKEY = PS_SUPPKEY
	and P_SIZE = :1   -- [1 , 50]
	and P_TYPE like '%:2' -- syllable3
	and S_NATIONKEY = N_NATIONKEY
	and N_REGIONKEY = R_REGIONKEY
	and R_NAME = ':3' -- R_NAME
	and PS_SUPPLYCOST = min_ps_supplycost
    and MIN_PARTKEY = P_PARTKEY;
--order by
--	s_acctbal desc,
--	n_name,
--	s_name,
--	p_partkey;


store(q2, q2);
