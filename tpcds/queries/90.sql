-- define DEPCNT=random(0,9,uniform);
-- define HOUR_AM = random(6,12,uniform);
-- define HOUR_PM = random(13,21,uniform);
-- define _LIMIT=100;

select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between {HOUR_AM} and {HOUR_AM}+1
         and household_demographics.hd_dep_count = {DEPCNT}
         and web_page.wp_char_count between 5000 and 5200) at,
      ( select count(*) pmc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between {HOUR_PM} and {HOUR_PM}+1
         and household_demographics.hd_dep_count = {DEPCNT}
         and web_page.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 limit 100;
