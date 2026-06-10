with source as (
    select * from read_csv_auto('../raw_data/expenses.csv')
)

select
    expense_id::integer as expense_id,
    project_id::integer as project_id,
    vendor_id::integer as vendor_id,
    expense_date::date as expense_date,
    amount::decimal(18, 2) as amount,
    trim(category) as category
from source
