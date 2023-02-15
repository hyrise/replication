--define YEAR = random(1998,2002, uniform);
--define MONTH = random(11,12,uniform);
--define GMT = text({"-6",1},{"-7",1});
--define CATEGORY = text({"Books",1},{"Home",1},{"Electronics",1},{"Jewelry",1},{"Sports",1});
--define _LIMIT=100;

select  promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
from
  (select sum(ss_ext_sales_price) promotions
   from  store_sales
        ,store
        ,promotion
        ,date_dim
        ,customer
        ,customer_address 
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk 
   and   ca_gmt_offset = {GMT}
   and   i_category = '{CATEGORY}'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = {GMT}
   and   d_year = {YEAR}
   and   d_moy  = {MONTH}) promotional_sales,
  (select sum(ss_ext_sales_price) total
   from  store_sales
        ,store
        ,date_dim
        ,customer
        ,customer_address
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = {GMT}
   and   i_category = '{CATEGORY}'
   and   s_gmt_offset = {GMT}
   and   d_year = {YEAR}
   and   d_moy  = {MONTH}) all_sales
order by promotions, total
limit 100;
