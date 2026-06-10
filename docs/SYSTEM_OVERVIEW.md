# PlacesOps System Overview

## One-Line Summary

PlacesOps is a construction and corporate analytics command center that turns project, vendor, budget, and expense data into trusted metrics, platform-health visibility, and governed AI-assisted dashboard insights.

## Case Study Narrative

Operations analytics often fails when business dashboards, data quality, documentation, and platform health are treated as separate concerns. PlacesOps treats them as one product surface: the same app that shows spend, delayed exposure, cost categories, and vendor reliability also shows dbt health, metric definitions, model documentation, and governed natural-language analysis.

The project is compact by design, but the architecture mirrors production data engineering patterns: source-aligned staging models, a reusable intermediate aggregate, expense and project marts, Jinja macros, tested data contracts, artifact observability, and an AI layer constrained to trusted definitions.

## Tech Stack

| Layer | Local Implementation | Production Analogue |
| --- | --- | --- |
| Source data | Generated project, vendor, and expense records | ERP, procurement, construction systems, finance exports |
| Storage/warehouse | DuckDB | Snowflake |
| Transformations | dbt Core staging and mart models | dbt jobs, CI, docs, tests, semantic layer |
| App layer | Streamlit | Qlik, Power BI, internal analytics portal, governed data app |
| Visualization | Altair and custom Streamlit UI | BI dashboards and operational analytics views |
| Observability | dbt `run_results.json` | CloudWatch, Datadog, Airflow/Step Functions, warehouse telemetry |
| AI workflow | Governed function router | Approved AI/MCP service over certified metrics |

## Architecture

```mermaid
flowchart LR
    A["Project source records"] --> D["dbt staging models"]
    B["Vendor source records"] --> D
    C["Expense source records"] --> D
    D --> E["fct_project_spend"]
    E --> F["Executive Operations"]
    E --> G["Cost & Vendor Risk"]
    H["dbt run_results.json"] --> I["Platform Health"]
    J["models/schema.yml"] --> K["Dictionary"]
    E --> L["Governed Insights Assistant"]
    I --> L
    K --> L
```

## Data Model

```mermaid
erDiagram
    PROJECTS ||--o{ EXPENSES : receives
    VENDORS ||--o{ EXPENSES : paid_by
    EXPENSES ||--|| FCT_PROJECT_SPEND : modeled_into
    PROJECTS {
        string project_id PK
        string project_name
        string campus
        number budget_allocated
        string status
    }
    VENDORS {
        string vendor_id PK
        string vendor_name
        number reliability_score
    }
    EXPENSES {
        string expense_id PK
        string project_id FK
        string vendor_id FK
        date expense_date
        number amount
        string category
    }
    FCT_PROJECT_SPEND {
        string expense_id PK
        date expense_date
        number amount
        string category
        string project_name
        string campus
        string status
        number budget_allocated
        string vendor_name
        number reliability_score
    }
```

## Lifecycle

```mermaid
timeline
    title PlacesOps Build Lifecycle
    2026-05-24 : Built dbt + DuckDB + Streamlit foundation
               : Added generated project, vendor, budget, and expense data
               : Created staging models, spend mart, tests, and first dashboard
    2026-05-25 : Reframed as construction and corporate analytics hub
               : Added executive, cost risk, platform health, dictionary, and assistant workflows
               : Added governed visual assistant with trace metadata
    2026-05-26 : Completed production theme and UI consistency pass
               : Added governed readouts across dashboard tabs
               : Expanded metric dictionary and model documentation
               : Published architecture and lifecycle documentation
```

## Technical Review Points

- Built an end-to-end analytics product, not only a dashboard.
- Modeled operational data into a trusted fact mart with dbt.
- Combined business metrics and engineering health in one surface.
- Added governed AI patterns without relying on unstable API quotas or hallucinated SQL.
- Documented the project as a production migration path from DuckDB/Streamlit to Snowflake/dbt/Qlik or Power BI/AWS observability.
