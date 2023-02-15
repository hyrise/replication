-- define YEAR= random(1998, 2002, uniform);
-- define MONTH=random(11,12,uniform);
-- define MGR_IDX = dist(i_manager_id, 1, 1);
-- define MANAGER=random(distmember(i_manager_id, [MGR_IDX], 2), distmember(i_manager_id, [MGR_IDX], 3),uniform);
-- define _LIMIT=100;

select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item,customer,customer_address,store
 where d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id={MANAGER}
   and d_moy={MONTH}
   and d_year={YEAR}
   and ss_customer_sk = c_customer_sk 
   and c_current_addr_sk = ca_address_sk
   and substr(ca_zip,1,5) <> substr(s_zip,1,5) 
   and ss_store_sk = s_store_sk 
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
limit 100;
