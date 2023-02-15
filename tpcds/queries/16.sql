-- define YEAR = random(1999, 2002, uniform);
-- define MONTH = random(2,5,uniform);
-- define STATE = dist(fips__county,3,1);
-- define COUNTYNUMBER = ulist(random(1, rowcount("active_counties", "call_center"), uniform), 5);
-- define COUNTY_A = distmember(fips__county, [COUNTYNUMBER.1], 2);
-- define COUNTY_B = distmember(fips__county, [COUNTYNUMBER.2], 2);
-- define COUNTY_C = distmember(fips__county, [COUNTYNUMBER.3], 2);
-- define COUNTY_D = distmember(fips__county, [COUNTYNUMBER.4], 2);
-- define COUNTY_E = distmember(fips__county, [COUNTYNUMBER.5], 2);
-- define _LIMIT=100;

select
   count(distinct cs_order_number) as "order count"
  ,sum(cs_ext_ship_cost) as "total shipping cost"
  ,sum(cs_net_profit) as "total net profit"
from
   catalog_sales cs1
  ,date_dim
  ,customer_address
  ,call_center
where
    d_date between '{YEAR}-{MONTH}-01' and
           (cast('{YEAR}-{MONTH}-01' as date) + interval '60 days')
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = '{STATE}'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('{COUNTY_A}','{COUNTY_B}','{COUNTY_C}','{COUNTY_D}',
                  '{COUNTY_E}'
)
and exists (select *
            from catalog_sales cs2
            where cs1.cs_order_number = cs2.cs_order_number
              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
and not exists(select *
               from catalog_returns cr1
               where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
limit 100;
