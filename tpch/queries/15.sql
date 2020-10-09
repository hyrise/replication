CREATE VIEW revenue{STREAM_ID}(supplier_no, total_revenue) AS
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount))
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '{DATE}'
        AND l_shipdate < DATE '{DATE}' + interval '3' month
    GROUP BY
        l_suppkey;

select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue{STREAM_ID}
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue{STREAM_ID}
    )
order by
    s_suppkey;

DROP VIEW revenue{STREAM_ID};