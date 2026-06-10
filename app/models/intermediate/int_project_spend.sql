with expenses as (
    select * from {{ ref('stg_expenses') }}
)

select
    project_id,
    count(*) as expense_count,
    count(distinct vendor_id) as vendor_count,
    min(expense_date) as first_expense_date,
    max(expense_date) as latest_expense_date,
    sum(amount) as total_spend
from expenses
group by project_id
