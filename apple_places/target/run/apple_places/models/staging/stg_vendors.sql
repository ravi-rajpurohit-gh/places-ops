
  
  create view "places_database"."main"."stg_vendors__dbt_tmp" as (
    with source as (
    select * from read_csv_auto('../raw_data/vendors.csv')
)

select
    vendor_id,
    vendor_name,
    reliability_score
from source
  );
