with source as (
    select * from read_csv_auto('../raw_data/vendors.csv')
)

select
    vendor_id::integer as vendor_id,
    trim(vendor_name) as vendor_name,
    reliability_score::integer as reliability_score
from source
