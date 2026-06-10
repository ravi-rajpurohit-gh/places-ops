{% snapshot project_status_snapshot %}

{{
    config(
        unique_key='project_id',
        strategy='check',
        check_cols=['status', 'budget_allocated'],
        schema='snapshots'
    )
}}

select
    project_id,
    project_name,
    campus,
    budget_allocated,
    status
from {{ ref('stg_projects') }}

{% endsnapshot %}
