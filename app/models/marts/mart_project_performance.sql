with projects as (
    select * from {{ ref('stg_projects') }}
),

project_spend as (
    select * from {{ ref('int_project_spend') }}
)

select
    p.project_id,
    p.project_name,
    p.campus,
    p.status as delivery_status,
    p.budget_allocated,
    coalesce(s.total_spend, 0) as total_spend,
    p.budget_allocated - coalesce(s.total_spend, 0) as budget_remaining,
    {{ safe_divide('coalesce(s.total_spend, 0)', 'p.budget_allocated') }} as budget_utilization_ratio,
    {{ budget_health_status('coalesce(s.total_spend, 0)', 'p.budget_allocated') }} as budget_health_status,
    coalesce(s.expense_count, 0) as expense_count,
    coalesce(s.vendor_count, 0) as vendor_count,
    s.first_expense_date,
    s.latest_expense_date
from projects p
left join project_spend s on p.project_id = s.project_id
