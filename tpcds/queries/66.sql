-- define YEAR= random(1998, 2002, uniform);
-- define TIMEONE= random(1, 57597, uniform);
-- define SMC = ulist(dist(ship_mode_carrier, 1, 1),2);
-- define NETONE = text({"ws_net_paid",1},{"ws_net_paid_inc_tax",1},{"ws_net_paid_inc_ship",1},{"ws_net_paid_inc_ship_tax",1},{"ws_net_profit",1});
-- define NETTWO = text({"cs_net_paid",1},{"cs_net_paid_inc_tax",1},{"cs_net_paid_inc_ship",1},{"cs_net_paid_inc_ship_tax",1},{"cs_net_profit",1});
-- define SALESONE = text({"ws_sales_price",1},{"ws_ext_sales_price",1},{"ws_ext_list_price",1});
-- define SALESTWO = text({"cs_sales_price",1},{"cs_ext_sales_price",1},{"cs_ext_list_price",1});
-- define _LIMIT=100;

select
         w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
        ,ship_carriers
        ,year
 	,sum(jan_sales) as jan_sales
 	,sum(feb_sales) as feb_sales
 	,sum(mar_sales) as mar_sales
 	,sum(apr_sales) as apr_sales
 	,sum(may_sales) as may_sales
 	,sum(jun_sales) as jun_sales
 	,sum(jul_sales) as jul_sales
 	,sum(aug_sales) as aug_sales
 	,sum(sep_sales) as sep_sales
 	,sum(oct_sales) as oct_sales
 	,sum(nov_sales) as nov_sales
 	,sum(dec_sales) as dec_sales
 	,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
 	,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
 	,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
 	,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
 	,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
 	,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
 	,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
 	,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
 	,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
 	,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
 	,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
 	,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
 	,sum(jan_net) as jan_net
 	,sum(feb_net) as feb_net
 	,sum(mar_net) as mar_net
 	,sum(apr_net) as apr_net
 	,sum(may_net) as may_net
 	,sum(jun_net) as jun_net
 	,sum(jul_net) as jul_net
 	,sum(aug_net) as aug_net
 	,sum(sep_net) as sep_net
 	,sum(oct_net) as oct_net
 	,sum(nov_net) as nov_net
 	,sum(dec_net) as dec_net
 from (
     select 
 	w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,'{SMC[1]}' || ',' || '{SMC[2]}' as ship_carriers
       ,d_year as year
 	,sum(case when d_moy = 1 
 		then {SALESONE}* ws_quantity else 0 end) as jan_sales
 	,sum(case when d_moy = 2 
 		then {SALESONE}* ws_quantity else 0 end) as feb_sales
 	,sum(case when d_moy = 3 
 		then {SALESONE}* ws_quantity else 0 end) as mar_sales
 	,sum(case when d_moy = 4 
 		then {SALESONE}* ws_quantity else 0 end) as apr_sales
 	,sum(case when d_moy = 5 
 		then {SALESONE}* ws_quantity else 0 end) as may_sales
 	,sum(case when d_moy = 6 
 		then {SALESONE}* ws_quantity else 0 end) as jun_sales
 	,sum(case when d_moy = 7 
 		then {SALESONE}* ws_quantity else 0 end) as jul_sales
 	,sum(case when d_moy = 8 
 		then {SALESONE}* ws_quantity else 0 end) as aug_sales
 	,sum(case when d_moy = 9 
 		then {SALESONE}* ws_quantity else 0 end) as sep_sales
 	,sum(case when d_moy = 10 
 		then {SALESONE}* ws_quantity else 0 end) as oct_sales
 	,sum(case when d_moy = 11
 		then {SALESONE}* ws_quantity else 0 end) as nov_sales
 	,sum(case when d_moy = 12
 		then {SALESONE}* ws_quantity else 0 end) as dec_sales
 	,sum(case when d_moy = 1 
 		then {NETONE} * ws_quantity else 0 end) as jan_net
 	,sum(case when d_moy = 2
 		then {NETONE} * ws_quantity else 0 end) as feb_net
 	,sum(case when d_moy = 3 
 		then {NETONE} * ws_quantity else 0 end) as mar_net
 	,sum(case when d_moy = 4 
 		then {NETONE} * ws_quantity else 0 end) as apr_net
 	,sum(case when d_moy = 5 
 		then {NETONE} * ws_quantity else 0 end) as may_net
 	,sum(case when d_moy = 6 
 		then {NETONE} * ws_quantity else 0 end) as jun_net
 	,sum(case when d_moy = 7 
 		then {NETONE} * ws_quantity else 0 end) as jul_net
 	,sum(case when d_moy = 8 
 		then {NETONE} * ws_quantity else 0 end) as aug_net
 	,sum(case when d_moy = 9 
 		then {NETONE} * ws_quantity else 0 end) as sep_net
 	,sum(case when d_moy = 10 
 		then {NETONE} * ws_quantity else 0 end) as oct_net
 	,sum(case when d_moy = 11
 		then {NETONE} * ws_quantity else 0 end) as nov_net
 	,sum(case when d_moy = 12
 		then {NETONE} * ws_quantity else 0 end) as dec_net
     from
          web_sales
         ,warehouse
         ,date_dim
         ,time_dim
 	  ,ship_mode
     where
            ws_warehouse_sk =  w_warehouse_sk
        and ws_sold_date_sk = d_date_sk
        and ws_sold_time_sk = t_time_sk
 	and ws_ship_mode_sk = sm_ship_mode_sk
        and d_year = {YEAR}
 	and t_time between {TIMEONE} and {TIMEONE}+28800 
 	and sm_carrier in ('{SMC[1]}','{SMC[2]}')
     group by 
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
       ,d_year
 union all
     select 
 	w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,'{SMC[1]}' || ',' || '{SMC[2]}' as ship_carriers
       ,d_year as year
 	,sum(case when d_moy = 1 
 		then {SALESTWO}* cs_quantity else 0 end) as jan_sales
 	,sum(case when d_moy = 2 
 		then {SALESTWO}* cs_quantity else 0 end) as feb_sales
 	,sum(case when d_moy = 3 
 		then {SALESTWO}* cs_quantity else 0 end) as mar_sales
 	,sum(case when d_moy = 4 
 		then {SALESTWO}* cs_quantity else 0 end) as apr_sales
 	,sum(case when d_moy = 5 
 		then {SALESTWO}* cs_quantity else 0 end) as may_sales
 	,sum(case when d_moy = 6 
 		then {SALESTWO}* cs_quantity else 0 end) as jun_sales
 	,sum(case when d_moy = 7 
 		then {SALESTWO}* cs_quantity else 0 end) as jul_sales
 	,sum(case when d_moy = 8 
 		then {SALESTWO}* cs_quantity else 0 end) as aug_sales
 	,sum(case when d_moy = 9 
 		then {SALESTWO}* cs_quantity else 0 end) as sep_sales
 	,sum(case when d_moy = 10 
 		then {SALESTWO}* cs_quantity else 0 end) as oct_sales
 	,sum(case when d_moy = 11
 		then {SALESTWO}* cs_quantity else 0 end) as nov_sales
 	,sum(case when d_moy = 12
 		then {SALESTWO}* cs_quantity else 0 end) as dec_sales
 	,sum(case when d_moy = 1 
 		then {NETTWO} * cs_quantity else 0 end) as jan_net
 	,sum(case when d_moy = 2 
 		then {NETTWO} * cs_quantity else 0 end) as feb_net
 	,sum(case when d_moy = 3 
 		then {NETTWO} * cs_quantity else 0 end) as mar_net
 	,sum(case when d_moy = 4 
 		then {NETTWO} * cs_quantity else 0 end) as apr_net
 	,sum(case when d_moy = 5 
 		then {NETTWO} * cs_quantity else 0 end) as may_net
 	,sum(case when d_moy = 6 
 		then {NETTWO} * cs_quantity else 0 end) as jun_net
 	,sum(case when d_moy = 7 
 		then {NETTWO} * cs_quantity else 0 end) as jul_net
 	,sum(case when d_moy = 8 
 		then {NETTWO} * cs_quantity else 0 end) as aug_net
 	,sum(case when d_moy = 9 
 		then {NETTWO} * cs_quantity else 0 end) as sep_net
 	,sum(case when d_moy = 10 
 		then {NETTWO} * cs_quantity else 0 end) as oct_net
 	,sum(case when d_moy = 11
 		then {NETTWO} * cs_quantity else 0 end) as nov_net
 	,sum(case when d_moy = 12
 		then {NETTWO} * cs_quantity else 0 end) as dec_net
     from
          catalog_sales
         ,warehouse
         ,date_dim
         ,time_dim
 	 ,ship_mode
     where
            cs_warehouse_sk =  w_warehouse_sk
        and cs_sold_date_sk = d_date_sk
        and cs_sold_time_sk = t_time_sk
 	and cs_ship_mode_sk = sm_ship_mode_sk
        and d_year = {YEAR}
 	and t_time between {TIMEONE} AND {TIMEONE}+28800 
 	and sm_carrier in ('{SMC[1]}','{SMC[2]}')
     group by 
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
       ,d_year
 ) x
 group by 
        w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,ship_carriers
       ,year
 order by w_warehouse_name
 limit 100;
