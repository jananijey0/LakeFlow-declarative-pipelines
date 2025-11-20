-- materialized data
create or refresh materialized view silver.cleaned_sales_data
(
    constraint sales_quantity_check expect(quantity is not null) on violation drop row,
    constraint sales_channel_check expect(channel is not null) on violation drop row,
    constraint sales_promo_code_check expect(promo_code is not null) on violation drop row,
    constraint customer_email_check expect(email is not null) on violation drop row)
    comment "Cleaned sales data"
    as 
    select 
        fs.sale_id,
        fs.order_date,
        fs.customer_id,
        fs.product_id,
        fs.quantity,
        fs.channel,
        fs.promo_code,
        c.first_name,
        c.last_name,
        c.email,
        c.join_date,
        c.vip,
        p.product_name,
        p.category,
        p.price,
        p.in_stock,
        r.region_name,
        r.country,
        fs.quantity * p.price as Revenue
    from 
        lakeflow_dlt_uc.silver.fact_sales fs
    left join
        lakeflow_dlt_uc.silver.customers c
    on  
        fs.customer_id = c.customer_id
    left join
        lakeflow_dlt_uc.silver.products p
    on  
        fs.product_id = p.product_id
    left join
        lakeflow_dlt_uc.silver.regions r
    on  
        fs.region_id = r.region_id   

    
