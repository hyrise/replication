select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '{DATE}'
    and l_shipdate < date '{DATE}' + interval '1' year
    and l_discount between {DISCOUNT:.2f} - 0.01 and {DISCOUNT:.2f} + 0.01
    and l_quantity < {QUANTITY};