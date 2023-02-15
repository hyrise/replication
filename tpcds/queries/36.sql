-- define YEAR=random(1998,2002,uniform);
-- define STATENUMBER=ulist(random(1, rowcount("active_states", "store"), uniform),8);
-- define STATE_A=distmember(fips__county,[STATENUMBER.1], 3);
-- define STATE_B=distmember(fips__county,[STATENUMBER.2], 3);
-- define STATE_C=distmember(fips__county,[STATENUMBER.3], 3);
-- define STATE_D=distmember(fips__county,[STATENUMBER.4], 3);
-- define STATE_E=distmember(fips__county,[STATENUMBER.5], 3);
-- define STATE_F=distmember(fips__county,[STATENUMBER.6], 3);
-- define STATE_G=distmember(fips__county,[STATENUMBER.7], 3);
-- define STATE_H=distmember(fips__county,[STATENUMBER.8], 3);
-- define _LIMIT=100;

select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end 
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    store_sales
   ,date_dim       d1
   ,item
   ,store
 where
    d1.d_year = {YEAR}
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk  = ss_item_sk 
 and s_store_sk  = ss_store_sk
 and s_state in ('{STATE_A}','{STATE_B}','{STATE_C}','{STATE_D}',
                 '{STATE_E}','{STATE_F}','{STATE_G}','{STATE_H}')
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc
  ,case when grouping(i_category)+grouping(i_class) = 0 then i_category end
  ,rank_within_parent
  limit 100;
