-- define MS= ulist(dist(marital_status, 1, 1), 3);
-- define ES= ulist(dist(education, 1, 1), 3);
-- define STATE= ulist(dist(fips__county, 3, 1), 9);
-- define YEAR= random(1998,2002, uniform);

select sum (ss_quantity)
 from store_sales, store, customer_demographics, customer_address, date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = {YEAR}
 and  
 (
  (
   cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = '{MS[1]}'
   and 
   cd_education_status = '{ES[1]}'
   and 
   ss_sales_price between 100.00 and 150.00  
   )
 or
  (
  cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = '{MS[2]}'
   and 
   cd_education_status = '{ES[2]}'
   and 
   ss_sales_price between 50.00 and 100.00   
  )
 or 
 (
  cd_demo_sk = ss_cdemo_sk
  and 
   cd_marital_status = '{MS[3]}'
   and 
   cd_education_status = '{ES[3]}'
   and 
   ss_sales_price between 150.00 and 200.00  
 )
 )
 and
 (
  (
  ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('{STATE[1]}', '{STATE[2]}', '{STATE[3]}')
  and ss_net_profit between 0 and 2000  
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('{STATE[4]}', '{STATE[5]}', '{STATE[6]}')
  and ss_net_profit between 150 and 3000 
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('{STATE[7]}', '{STATE[8]}', '{STATE[9]}')
  and ss_net_profit between 50 and 25000 
  )
 )
;
