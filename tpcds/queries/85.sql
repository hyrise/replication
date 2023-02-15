-- define MS= ulist(dist(marital_status, 1, 1), 3);
-- define ES= ulist(dist(education, 1, 1), 3);
-- define STATE= ulist(dist(fips__county, 3, 1), 9);
-- define YEAR= random(1998,2002, uniform);
-- define _LIMIT=100;

select  substr(r_reason_desc,1,20)
       ,avg(ws_quantity)
       ,avg(wr_refunded_cash)
       ,avg(wr_fee)
 from web_sales, web_returns, web_page, customer_demographics cd1,
      customer_demographics cd2, customer_address, date_dim, reason 
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = d_date_sk and d_year = {YEAR}
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk 
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and ca_address_sk = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = '{MS[1]}'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '{ES[1]}'
     and 
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100.00 and 150.00
    )
   or
    (
     cd1.cd_marital_status = '{MS[2]}'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '{ES[2]}' 
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50.00 and 100.00
    )
   or
    (
     cd1.cd_marital_status = '{MS[3]}'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '{ES[3]}'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150.00 and 200.00
    )
   )
   and
   (
    (
     ca_country = 'United States'
     and
     ca_state in ('{STATE[1]}', '{STATE[2]}', '{STATE[3]}')
     and ws_net_profit between 100 and 200  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('{STATE[4]}', '{STATE[5]}', '{STATE[6]}')
     and ws_net_profit between 150 and 300  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('{STATE[7]}', '{STATE[8]}', '{STATE[9]}')
     and ws_net_profit between 50 and 250  
    )
   )
group by r_reason_desc
order by substr(r_reason_desc,1,20)
        ,avg(ws_quantity)
        ,avg(wr_refunded_cash)
        ,avg(wr_fee)
limit 100;
