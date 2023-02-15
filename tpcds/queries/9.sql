-- define AGGCTHEN= text({"ss_ext_discount_amt",1},{"ss_ext_sales_price",1},{"ss_ext_list_price",1},{"ss_ext_tax",1});
-- define AGGCELSE= text({"ss_net_paid",1},{"ss_net_paid_inc_tax",1},{"ss_net_profit",1});
-- define RC=ulist(random(1, rowcount("store_sales")/5,uniform),5);

select case when (select count(*)
                  from store_sales 
                  where ss_quantity between 1 and 20) > {RC[1]}
            then (select avg({AGGCTHEN})
                  from store_sales 
                  where ss_quantity between 1 and 20) 
            else (select avg({AGGCELSE})
                  from store_sales
                  where ss_quantity between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 21 and 40) > {RC[2]}
            then (select avg({AGGCTHEN})
                  from store_sales
                  where ss_quantity between 21 and 40) 
            else (select avg({AGGCELSE})
                  from store_sales
                  where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 41 and 60) > {RC[3]}
            then (select avg({AGGCTHEN})
                  from store_sales
                  where ss_quantity between 41 and 60)
            else (select avg({AGGCELSE})
                  from store_sales
                  where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 61 and 80) > {RC[4]}
            then (select avg({AGGCTHEN})
                  from store_sales
                  where ss_quantity between 61 and 80)
            else (select avg({AGGCELSE})
                  from store_sales
                  where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 81 and 100) > {RC[5]}
            then (select avg({AGGCTHEN})
                  from store_sales
                  where ss_quantity between 81 and 100)
            else (select avg({AGGCELSE})
                  from store_sales
                  where ss_quantity between 81 and 100) end bucket5
from reason
where r_reason_sk = 1
;
