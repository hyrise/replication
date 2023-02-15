-- define YEAR = random(1998, 2002, uniform);
-- define GEN= dist(gender, 1, 1);
-- define MS= dist(marital_status, 1, 1);
-- define ES= dist(education, 1, 1);
-- define STATENUMBER=ulist(random(1, rowcount("active_states", "store"), uniform),6);
-- define STATE_A=distmember(fips__county,[STATENUMBER.1], 3);
-- define STATE_B=distmember(fips__county,[STATENUMBER.2], 3);
-- define STATE_C=distmember(fips__county,[STATENUMBER.3], 3);
-- define STATE_D=distmember(fips__county,[STATENUMBER.4], 3);
-- define STATE_E=distmember(fips__county,[STATENUMBER.5], 3);
-- define STATE_F=distmember(fips__county,[STATENUMBER.6], 3);
-- define _LIMIT=100;

select  i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = '{GEN}' and
       cd_marital_status = '{MS}' and
       cd_education_status = '{ES}' and
       d_year = {YEAR} and
       s_state in ('{STATE_A}','{STATE_B}', '{STATE_C}', '{STATE_D}', '{STATE_E}', '{STATE_F}')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
 limit 100;
