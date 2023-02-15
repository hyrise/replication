-- define YEAR= random(1998, 2002, uniform);
-- define MONTH=random(11,12,uniform);
-- define MANAGER=random(1,100,uniform);
-- define _LIMIT=100;

select  i_brand_id brand_id, i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id={MANAGER}
 	and d_moy={MONTH}
 	and d_year={YEAR}
 group by i_brand, i_brand_id
 order by ext_price desc, i_brand_id
limit 100;
