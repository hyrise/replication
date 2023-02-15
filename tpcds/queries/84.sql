-- define CITY = dist(cities, 1, large);
-- define INCOME = random(0, 70000, uniform);
-- define _LIMIT=100;

select  c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from customer
     ,customer_address
     ,customer_demographics
     ,household_demographics
     ,income_band
     ,store_returns
 where ca_city	        =  '{CITY}'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  {INCOME}
   and ib_upper_bound   <=  {INCOME} + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by c_customer_id
 limit 100;
