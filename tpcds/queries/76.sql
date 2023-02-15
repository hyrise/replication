-- define NULLCOLCS=text({"cs_bill_customer_sk",1},{"cs_bill_hdemo_sk",1},{"cs_bill_addr_sk",1},{"cs_ship_customer_sk",1},{"cs_ship_cdemo_sk",1},{"cs_ship_hdemo_sk",1},{"cs_ship_addr_sk",1},{"cs_ship_mode_sk",1},{"cs_warehouse_sk",1},{"cs_promo_sk",1});
-- define NULLCOLSS= text({"ss_customer_sk",1},{"ss_cdemo_sk",1},{"ss_hdemo_sk",1},{"ss_addr_sk",1},{"ss_store_sk",1},{"ss_promo_sk",1});
-- define NULLCOLWS=text({"ws_bill_customer_sk",1},{"ws_bill_hdemo_sk",1},{"ws_bill_addr_sk",1},{"ws_ship_customer_sk",1},{"ws_ship_cdemo_sk",1},{"ws_ship_hdemo_sk",1},{"ws_ship_addr_sk",1},{"ws_web_page_sk",1},{"ws_web_site_sk",1},{"ws_ship_mode_sk",1},{"ws_warehouse_sk",1},{"ws_promo_sk",1});
-- define _LIMIT=100;

select  channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
        SELECT 'store' as channel, '{NULLCOLSS}' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price
         FROM store_sales, item, date_dim
         WHERE {NULLCOLSS} IS NULL
           AND ss_sold_date_sk=d_date_sk
           AND ss_item_sk=i_item_sk
        UNION ALL
        SELECT 'web' as channel, '{NULLCOLWS}' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price
         FROM web_sales, item, date_dim
         WHERE {NULLCOLWS} IS NULL
           AND ws_sold_date_sk=d_date_sk
           AND ws_item_sk=i_item_sk
        UNION ALL
        SELECT 'catalog' as channel, '{NULLCOLCS}' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price
         FROM catalog_sales, item, date_dim
         WHERE {NULLCOLCS} IS NULL
           AND cs_sold_date_sk=d_date_sk
           AND cs_item_sk=i_item_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
limit 100;
