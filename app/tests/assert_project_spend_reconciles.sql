with fact_spend as (
    select sum(amount) as total_spend
    from {{ ref('fct_project_spend') }}
),

project_spend as (
    select sum(total_spend) as total_spend
    from {{ ref('mart_project_performance') }}
)

select
    f.total_spend as fact_total_spend,
    p.total_spend as project_total_spend
from fact_spend f
cross join project_spend p
where abs(f.total_spend - p.total_spend) > 0.01
