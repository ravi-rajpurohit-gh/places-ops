
    
    

select
    vendor_id as unique_field,
    count(*) as n_records

from "places_database"."main"."stg_vendors"
where vendor_id is not null
group by vendor_id
having count(*) > 1


