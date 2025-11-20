--Read fact sales from volume bronze schema
create streaming live table bronze.fact_sales
comment "Raw fact sales from volume to bronze schema"
as
select * from cloud_files(
  '/Volumes/lakeflow_dlt_uc/landing_zone/facts_and_dimensions_files/fact_sales/',
  'csv',
  map('header','true')
);

--Read products from volume to bronze schema
create streaming live table bronze.products
comment "Raw Products from volume to bronze schema"
as
select * from cloud_files(
  '/Volumes/lakeflow_dlt_uc/landing_zone/facts_and_dimensions_files/dim_products/',
  'csv',
  map('header','true')
);


-- Read customers from volume to bronze schema
create streaming live table bronze.cutomers
comment "Raw customers data from volume to bronze schema"
as
select * from cloud_files(
'/Volumes/lakeflow_dlt_uc/landing_zone/facts_and_dimensions_files/dim_customers/',
'csv',
map('header','true')
);

--Read region from volume to bronze schema
create streaming live table bronze.regions
comment "Raw region data from volume to bronze schema"
as
select * from cloud_files(
  '/Volumes/lakeflow_dlt_uc/landing_zone/facts_and_dimensions_files/dim_region/',
  'csv',
  map('header','true')
);