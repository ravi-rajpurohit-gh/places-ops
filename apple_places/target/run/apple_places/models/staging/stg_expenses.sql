
  
    
    

    create  table
      "places_database"."main"."stg_expenses__dbt_tmp"
  
    as (
      

with source as (
    select * from read_csv_auto('../raw_data/expenses.csv')
)

select
    expense_id,
    project_id,
    vendor_id,
    expense_date::date as expense_date,
    amount,
    category
from source
    );
  
  