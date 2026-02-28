
    
    

select
    expense_id as unique_field,
    count(*) as n_records

from "places_database"."main"."fct_project_spend"
where expense_id is not null
group by expense_id
having count(*) > 1


