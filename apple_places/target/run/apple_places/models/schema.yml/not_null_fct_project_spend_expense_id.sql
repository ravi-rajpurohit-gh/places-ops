
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select expense_id
from "places_database"."main"."fct_project_spend"
where expense_id is null



  
  
      
    ) dbt_internal_test