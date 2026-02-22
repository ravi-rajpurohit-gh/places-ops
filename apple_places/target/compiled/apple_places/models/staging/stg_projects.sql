with source as (
    select * from read_csv_auto('../raw_data/projects.csv')
)

select
    project_id,
    project_name,
    campus,
    budget_allocated,
    status
from source