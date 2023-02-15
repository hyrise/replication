-- define YEAR = random(1998, 2002, uniform);
-- define GEN= dist(gender, 1, 1);
-- define ES= dist(education, 1, 1);
-- define STATE=ulist(dist(fips__county,3,1),7);
-- define MONTH=ulist(random(1,12,uniform),6);
-- define _LIMIT=100;

select  i_item_id,
        ca_country,
        ca_state, 
        ca_county,
        avg( cast(cs_quantity as decimal(12,2))) agg1,
        avg( cast(cs_list_price as decimal(12,2))) agg2,
        avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
        avg( cast(cs_sales_price as decimal(12,2))) agg4,
        avg( cast(cs_net_profit as decimal(12,2))) agg5,
        avg( cast(c_birth_year as decimal(12,2))) agg6,
        avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
 from catalog_sales, customer_demographics cd1, 
      customer_demographics cd2, customer, customer_address, date_dim, item
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd1.cd_demo_sk and
       cs_bill_customer_sk = c_customer_sk and
       cd1.cd_gender = '{GEN}' and 
       cd1.cd_education_status = '{ES}' and
       c_current_cdemo_sk = cd2.cd_demo_sk and
       c_current_addr_sk = ca_address_sk and
       c_birth_month in ({MONTH[1]},{MONTH[2]},{MONTH[3]},{MONTH[4]},{MONTH[5]},{MONTH[6]}) and
       d_year = {YEAR} and
       ca_state in ('{STATE[1]}','{STATE[2]}','{STATE[3]}'
                   ,'{STATE[4]}','{STATE[5]}','{STATE[6]}','{STATE[7]}')
 group by rollup (i_item_id, ca_country, ca_state, ca_county)
 order by ca_country,
        ca_state, 
        ca_county,
	i_item_id
limit 100;
