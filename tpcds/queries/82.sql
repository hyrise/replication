-- define YEAR=random(1998,2002,uniform);
-- define PRICE=random(0,90,uniform);
-- define INVDATE=date([YEAR]+"-01-01",[YEAR]+"-07-24",sales);
-- define MANUFACT_ID=ulist(random(1,1000,uniform),4);
-- define _LIMIT=100;

select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between {PRICE} and {PRICE}+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('{INVDATE}' as date) and (cast('{INVDATE}' as date) +  interval '60 days')
 and i_manufact_id in ({MANUFACT_ID[1]},{MANUFACT_ID[2]},{MANUFACT_ID[3]},{MANUFACT_ID[4]})
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;
