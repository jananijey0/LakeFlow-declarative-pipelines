

--fact sales data cleaning
create streaming live table silver.fact_sales
comment "fact sales data cleaning"
as
select 
    cast(sale_id as int) as sale_id,
    to_date(order_date,'dd/MM/yyyy') as order_date,
    cast (customer_id as int) as customer_id,
    cast(product_id as int) as product_id,
    cast(quantity as int) as quantity,
    cast(discount as int) as discount,
    cast(region_id as int)as region_id,
    cast(channel as string) as channel,
    cast(promo_code as string) as promo_code
    from stream (lakeflow_dlt_uc.bronze.fact_sales);

    --customer table data cleaning
    create streaming live table silver.customers
    comment "customer table data cleaning"
    as 
    select 
        cast(customer_id as int) as customer_id,
        cast(first_name as string) as first_name,
        cast(last_name as string) as last_name,
        cast(email as string) as email,
        to_date(join_date ,'dd/MM/yyyy')as join_date,
        cast(vip as string) as vip
        from stream (lakeflow_dlt_uc.bronze.cutomers);


        --product table data cleaning
        create streaming live table silver.products
        comment "product table data cleaning"
        as 
        select
            cast(product_id as int) as product_id,
            cast(product_name as string) as product_name,
            cast(category as string) as category,
            cast(price as int) as price,
            cast(in_stock as int) as in_stock
            from stream(lakeflow_dlt_uc.bronze.products);

        -- region table data cleaning
        create streaming live table silver.regions
        comment "regions table data cleaning"
        as 
        select 
        cast(region_id as int) as region_id,
        cast(region_name as string) as region_name,
        cast(country as string) as country
        from stream(lakeflow_dlt_uc.bronze.regions);



