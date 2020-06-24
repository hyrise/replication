-- using 1472396759 as a seed to the RNG


select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part,
        (select l_partkey as agg_partkey, 0.2 * avg(l_quantity) as avg_quantity from lineitem group by l_partkey) part_agg
where
    p_partkey = l_partkey
        and agg_partkey = l_partkey
    and p_brand = '{BRAND}'
    and p_container = '{CONTAINER}'
    and l_quantity < avg_quantity
limit 1;
