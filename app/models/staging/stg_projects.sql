with source as (
    select * from read_csv_auto('../raw_data/projects.csv')
)

select
    project_id::integer as project_id,
    trim(project_name) as project_name,
    trim(campus) as campus,
    budget_allocated::decimal(18, 2) as budget_allocated,
    trim(status) as status
from source
