-- define IMID  = random(1,1000,uniform);
-- define YEAR  = random(1998,2002,uniform);
-- define WSDATE = date([YEAR]+"-01-01",[YEAR]+"-04-01",sales);
-- define _LIMIT=100;

select
   sum(ws_ext_discount_amt)  as "Excess Discount Amount" 
from 
    web_sales 
   ,item 
   ,date_dim
where
i_manufact_id = {IMID}
and i_item_sk = ws_item_sk 
and d_date between '{WSDATE}' and
        (cast('{WSDATE}' as date) + interval '90 days')
and d_date_sk = ws_sold_date_sk 
and ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * avg(ws_ext_discount_amt) 
         FROM 
            web_sales 
           ,date_dim
         WHERE 
              ws_item_sk = i_item_sk 
          and d_date between '{WSDATE}' and
                             (cast('{WSDATE}' as date) + interval '90 days')
          and d_date_sk = ws_sold_date_sk 
      ) 
order by sum(ws_ext_discount_amt)
limit 100;
