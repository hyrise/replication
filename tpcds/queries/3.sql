-- define AGGC= text({"ss_ext_sales_price",1},{"ss_sales_price",1},{"ss_ext_discount_amt",1},{"ss_net_profit",1});
-- define MONTH = random(11,12,uniform);
-- define MANUFACT= random(1,1000,uniform);
-- define _LIMIT=100;

select  dt.d_year
       ,item.i_brand_id brand_id 
       ,item.i_brand brand
       ,sum({AGGC}) sum_agg
 from  date_dim dt 
      ,store_sales
      ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = {MANUFACT}
   and dt.d_moy={MONTH}
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 limit 100;
