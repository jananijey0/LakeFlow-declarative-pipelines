create or refresh materialized view gold.category_sales_summary as 
select 
  category,
  year(order_date) as year,
  sum(revenue) as total_revenue
  from 
  lakeflow_dlt_uc.silver.cleaned_sales_data
  group by category, year(order_date)
  order by category,year;

  create or refresh materialized view gold.revenue_by_customers_in_each_region as 
  select 
  *
  from (
    select 
    customer_id,
    first_name,
    last_name,
    region_name,
    sum(revenue) as total_revenue,
    rank() over (partition by region_name order by sum(revenue) desc) as revenue_rank from 
    lakeflow_dlt_uc.silver.cleaned_sales_data
    group by customer_id,first_name,last_name,region_name) ranked_customers
    where revenue_rank <=5 
    order by region_name,revenue_rank;

    create or refresh materialized view gold.customer_liftime_value_estimation_time as

  select 
  customer_id,
  first_name,
  last_name,
  sum(revenue) as lifetime_revenue,
  count(distinct order_date ) as purchase_days,
  avg(revenue) as avg_revenue_per_purchase
  from lakeflow_dlt_uc.silver.cleaned_sales_data
  group by customer_id,first_name,last_name
  order by lifetime_revenue desc;
  