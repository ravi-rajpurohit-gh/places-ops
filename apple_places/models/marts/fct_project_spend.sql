{{ config(materialized='table') }}

with expenses as (
    select * from {{ ref('stg_expenses') }}
),

projects as (
    select * from {{ ref('stg_projects') }}
),

vendors as (
    select * from {{ ref('stg_vendors') }}
)

select
    e.expense_id,
    e.expense_date,
    e.amount,
    e.category,
    p.project_name,
    p.campus,
    p.status,
    p.budget_allocated,
    v.vendor_name,
    v.reliability_score
from expenses e
left join projects p on e.project_id = p.project_id
left join vendors v on e.vendor_id = v.vendor_id