-- define YEAR = random(1998, 2002, uniform);
-- define IDX = ulist(random(1, rowcount("categories"), uniform), 6);
-- define CAT_A = distmember(categories, [IDX.1], 1);
-- define CLASS_A = DIST(distmember(categories, [IDX.1], 2), 1, 1);
-- define CAT_B = distmember(categories, [IDX.2], 1);
-- define CLASS_B = DIST(distmember(categories, [IDX.2], 2), 1, 1);
-- define CAT_C = distmember(categories, [IDX.3], 1);
-- define CLASS_C = DIST(distmember(categories, [IDX.3], 2), 1, 1);
-- define CAT_D = distmember(categories, [IDX.4], 1);
-- define CLASS_D = DIST(distmember(categories, [IDX.4], 2), 1, 1);
-- define CAT_E = distmember(categories, [IDX.5], 1);
-- define CLASS_E = DIST(distmember(categories, [IDX.5], 2), 1, 1);
-- define CAT_F = distmember(categories, [IDX.6], 1);
-- define CLASS_F = DIST(distmember(categories, [IDX.6], 2), 1, 1);
-- define _LIMIT=100;

select  *
from(
select i_category, i_class, i_brand,
       s_store_name, s_company_name,
       d_moy,
       sum(ss_sales_price) sum_sales,
       avg(sum(ss_sales_price)) over
         (partition by i_category, i_brand, s_store_name, s_company_name)
         avg_monthly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
      ss_sold_date_sk = d_date_sk and
      ss_store_sk = s_store_sk and
      d_year in ({YEAR}) and
        ((i_category in ('{CAT_A}','{CAT_B}','{CAT_C}') and
          i_class in ('{CLASS_A}','{CLASS_B}','{CLASS_C}')
         )
      or (i_category in ('{CAT_D}','{CAT_E}','{CAT_F}') and
          i_class in ('{CLASS_D}','{CLASS_E}','{CLASS_F}') 
        ))
group by i_category, i_class, i_brand,
         s_store_name, s_company_name, d_moy) tmp1
where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
order by sum_sales - avg_monthly_sales, s_store_name
limit 100;
