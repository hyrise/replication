-- define YEAR=random(1998,2002,uniform);
-- define SDATE=date([YEAR]+"-01-01",[YEAR]+"-07-01",sales);
-- define CATEGORY=ulist(dist(categories,1,1),3);
-- define _LIMIT=100;

select  i_item_id
      ,i_item_desc
      ,i_category
      ,i_class
      ,i_current_price
      ,sum(ws_ext_sales_price) as itemrevenue
      ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
          (partition by i_class) as revenueratio
from
	web_sales
	,item
    ,date_dim
where
	ws_item_sk = i_item_sk
  	and i_category in ('{CATEGORY[1]}', '{CATEGORY[2]}', '{CATEGORY[3]}')
  	and ws_sold_date_sk = d_date_sk
	and d_date between cast('{SDATE}' as date)
				and (cast('{SDATE}' as date) + interval '30 days')
group by
	i_item_id
    ,i_item_desc
    ,i_category
    ,i_class
    ,i_current_price
order by
	i_category
    ,i_class
    ,i_item_id
    ,i_item_desc
    ,revenueratio
limit 100;