-- define HOUR= text({"20",1},{"15",1},{"16",1},{"8",1});
-- define DEPCNT=random(0,9,uniform);
-- define _LIMIT=100;

select  count(*)
from store_sales
    ,household_demographics 
    ,time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk   
    and ss_hdemo_sk = household_demographics.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = {HOUR}
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = {DEPCNT}
    and store.s_store_name = 'ese'
order by count(*)
limit 100;
