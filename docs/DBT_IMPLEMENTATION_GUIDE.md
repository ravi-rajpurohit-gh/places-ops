# dbt Implementation Guide

## Purpose

This guide explains the modeling, testing, lineage, and deployment decisions in the PlacesOps dbt project. The current implementation extends the original staging-and-fact workflow with patterns used to maintain shared analytical data products.

## Patterns Implemented

| Pattern | PlacesOps implementation | Why it matters |
| --- | --- | --- |
| Layered DAG | Staging views feed `int_project_spend`, `fct_project_spend`, and `mart_project_performance`. | Keeps source cleanup, reusable transformations, and business marts separate. |
| Jinja macros | `safe_divide` and `budget_health_status` centralize reusable SQL and threshold logic. | Prevents metric definitions from drifting across models. |
| Project variables | `budget_warning_threshold` is configured in `dbt_project.yml`. | Business thresholds can change without rewriting model SQL. |
| Custom generic test | `accepted_range` validates numeric domains across multiple models and columns. | Encodes a reusable data contract instead of repeating singular SQL tests. |
| Referential tests | Expense and fact foreign keys are tested against project and vendor models. | Detects orphaned operational records before they reach BI. |
| Reconciliation test | `assert_project_spend_reconciles.sql` compares fact-level and project-level spend totals. | Protects business totals across model grains. |
| SCD Type 2 snapshot | `project_status_snapshot` tracks changes to project status and approved budget. | Preserves history when mutable source records change. |
| Exposure | `places_ops_dashboard` links the data product to its upstream dbt marts. | Makes downstream ownership and lineage visible in dbt docs. |
| CI | Pull requests that affect dbt assets run `dbt parse` and `dbt build`. | Blocks invalid SQL, failed tests, and broken lineage before merge. |

## Jinja And Macro Design

`budget_health_status` composes the smaller `safe_divide` macro and reads the warning threshold from a project variable. This keeps the classification readable in the mart while compiling to ordinary warehouse SQL.

The custom `accepted_range` generic test accepts optional minimum and maximum arguments. The same test protects budget, spend, reliability, count, and ratio columns without copying test SQL.

## Snowflake Deployment Path

The local project runs on DuckDB so reviewers can execute it without credentials. A Snowflake deployment would preserve the dbt DAG and business logic but change the ingestion and physical-design layer:

1. Land operational extracts in an external or internal stage and load typed raw tables with Snowpipe, tasks, or orchestrated `COPY INTO` operations.
2. Replace DuckDB `read_csv_auto` calls in staging with dbt `source()` references and configure source freshness checks.
3. Use separate Snowflake databases/schemas and role-based grants for raw, development, analytics, and BI-serving layers.
4. Review materializations by workload: views for light staging, incremental tables for high-volume facts, and tables or dynamic tables where serving performance warrants them.
5. Add warehouse sizing, query tags, resource monitors, and clustering only after measuring query profiles and pruning behavior.
6. Keep credentials and target-specific settings in environment variables or dbt Cloud connections, not in the repository.

The current executable target is DuckDB. The Snowflake section documents the concrete deployment path without implying that the repository has been executed against a production Snowflake account.

## Validation Result

On June 10, 2026, `dbt build --profiles-dir .` completed successfully:

- 6 models
- 1 SCD Type 2 snapshot
- 52 data tests
- 1 exposure
- `PASS=59 WARN=0 ERROR=0 SKIP=0 NO-OP=1 TOTAL=60`
