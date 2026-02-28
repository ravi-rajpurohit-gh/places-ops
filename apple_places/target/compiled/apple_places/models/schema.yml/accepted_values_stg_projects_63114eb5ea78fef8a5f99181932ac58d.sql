
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from "places_database"."main"."stg_projects"
    group by status

)

select *
from all_values
where value_field not in (
    'In Progress','Completed','Delayed'
)


