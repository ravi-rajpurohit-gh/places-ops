# PlacesOps Case Study

## Overview

PlacesOps is a production-minded analytics application for transforming construction, vendor, budget, and expense data into trusted corporate operations insights. It combines dbt modeling, data quality, dashboarding, dbt artifact observability, generated documentation, and governed natural-language analysis in one compact product surface.

The application is designed around a realistic enterprise analytics pattern: operational source data is standardized through staging models, joined into a trusted spend mart, monitored through dbt artifacts, and consumed through business dashboards, metric documentation, and governed AI-assisted workflows.

## Product Goals

- Turn project, vendor, and expense records into reliable spend and risk metrics.
- Make budget variance, delayed-project exposure, cost categories, regional spend, and vendor reliability easy to inspect.
- Treat dbt pipeline health and documentation as part of the data product.
- Provide natural-language access to approved metrics without unrestricted SQL generation.
- Preserve a clear migration path to AWS, Snowflake, dbt, Qlik/Power BI, observability, and approved AI/MCP tooling.

## Lifecycle Timeline

```mermaid
timeline
    title PlacesOps Development Lifecycle
    2026-05-24 : Created dbt + DuckDB + Streamlit foundation
               : Generated project, vendor, budget, and expense source data
               : Built staging models and fct_project_spend mart
               : Added dbt tests, run artifacts, and data dictionary surface
    2026-05-25 : Reframed product as construction and corporate analytics hub
               : Added budget variance and delayed-project exposure metrics
               : Neutralized generated operating regions and model documentation
               : Rebuilt Streamlit into executive operations, cost risk, platform health, dictionary, and assistant workflows
               : Added governed visual analyst with contextual charts and trace metadata
               : Polished domain-aligned UI theme, assistant prompts, native controls, and portfolio attribution
               : Refreshed DuckDB warehouse and verified app in browser
               : Documented checkpoint across README, changelog, tracker, engineering notes, and case study
    2026-05-26 : Completed production UI consistency pass
               : Replaced remaining white native surfaces with dark themed controls and tables
               : Added cross-tab governed insight panels for cost risk, platform health, and metric definitions
               : Added scrollable vendor reliability review table and aligned chart palette
               : Expanded schema.yml documentation and portfolio-ready case study assets
```

## Delivery Phases

| Phase | Outcome | Evidence |
| --- | --- | --- |
| Product Framing | Defined the analytics hub vision, users, business metrics, and production mapping. | Root README and app README. |
| Data Generation | Created privacy-safe project, vendor, and expense records. | `generate_mock_data.py` and `raw_data/`. |
| Data Modeling | Built staging models and a project spend fact mart. | `apple_places/models/`. |
| Quality & Observability | Added dbt tests, run artifacts, success-rate metrics, and runtime telemetry. | `schema.yml`, `target/run_results.json`, Platform Health tab. |
| Business Analytics | Added executive KPIs, region spend, budget variance, delayed exposure, cost categories, and vendor reliability risk. | Streamlit app tabs. |
| Governed AI Workflow | Added natural-language routing to approved analytical functions with chart responses and trace metadata. | Insights Assistant tab in `apple_places/app.py`. |
| Cross-Tab AI Readouts | Embedded governed narrative insights into executive, cost/risk, platform, and dictionary workflows. | `render_insight_panel` in `apple_places/app.py`. |
| UX Theming | Styled the app around a warm construction/corporate analytics visual system with dark controls and production-style assistant prompts. | `CUSTOM_CSS` and Streamlit app surface. |
| Documentation | Added tracker, engineering notes, changelog, and case study checkpoint. | `docs/` and `CHANGELOG.md`. |

## Decision Log

| Decision | Rationale | Tradeoff |
| --- | --- | --- |
| Keep the app as one Streamlit surface | The product is easier to scan across business, engineering, documentation, and assistant workflows. | A larger long-lived product may eventually benefit from separate pages. |
| Use DuckDB locally | Enables fast local development and a portable warehouse artifact. | Production would move marts to Snowflake. |
| Use dbt artifacts in the app | Shows that pipeline health is part of the product experience. | Artifact freshness depends on dbt build discipline. |
| Read dictionary docs from schema.yml | Keeps model documentation current in the app without requiring a manifest refresh after every copy edit. | Manifest remains a fallback rather than the primary dictionary source. |
| Use a governed assistant router | Gives natural-language access without API keys, quota failures, or hallucinated metrics. | It is deterministic rather than fully open-ended. |
| Add contextual assistant charts | Makes answers more useful and closer to AI-powered dashboards. | Charts are limited to approved routes. |
| Neutralize generated regions | Keeps the project public, reusable, and portfolio-safe. | It is representative rather than tied to a real operating portfolio. |

## Architecture

```mermaid
flowchart LR
    A["Generated projects"] --> D["dbt staging models"]
    B["Generated vendors"] --> D
    C["Generated expenses"] --> D
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

## Portfolio Assets

Use these as the core visuals and talking points for a portfolio case study:

- **Problem frame:** Cross-functional operations teams need trusted spend, budget, vendor, and delivery-risk metrics from messy operational data.
- **Architecture diagram:** Source data to dbt staging models, `fct_project_spend`, Streamlit workflows, dbt artifacts, schema documentation, and governed assistant.
- **Data model:** Projects, vendors, expenses, and the joined spend fact mart.
- **Lifecycle timeline:** 2026-05-24 foundation, 2026-05-25 product rebuild, 2026-05-26 production polish and governed AI readouts.
- **AI dashboard angle:** Governed readouts and assistant routes answer approved business questions with traceable metrics, charts, token estimates, rows considered, latency, API calls, and API cost.
- **Production mapping:** DuckDB to Snowflake, CSV sources to enterprise systems, Streamlit to Qlik/Power BI/internal portal, local artifacts to CloudWatch-style observability.

## Validation Strategy

- Unique and non-null tests protect project, vendor, expense, and fact-table keys.
- Accepted-value tests validate project status values.
- Not-null tests protect spend amounts.
- dbt run artifacts expose model/test results and runtime telemetry.
- Browser smoke testing verifies the business dashboard and assistant surface.
- Warehouse verification confirms generated neutral regions are reflected in `fct_project_spend`.

## Production Evolution

| Local Implementation | Production Direction |
| --- | --- |
| CSV source files | ERP, finance, procurement, construction/project systems, and corporate SaaS sources |
| DuckDB | Snowflake |
| dbt Core | dbt jobs, CI, exposures, docs, tests, and semantic definitions |
| Streamlit | Qlik/Power BI, internal analytics portal, or governed data app |
| dbt artifacts | Orchestration metadata, lineage, observability, and alerting |
| Governed assistant router | MCP/approved AI workflow over certified metrics and documentation |

## Checkpoint

As of 2026-05-26, PlacesOps is in a stable portfolio-ready checkpoint:

- app has five product workflows,
- generated data and warehouse artifact use neutral operating regions,
- dbt artifacts report 14 passing nodes/tests,
- assistant answers questions with text, visuals, and trace metadata,
- governed insight panels appear across the business, platform, and dictionary workflows,
- UI uses a domain-aligned production theme with dark controls, custom dark tables, and portfolio attribution at `ravirajpurohit.com`,
- documentation is aligned across README, tracker, engineering notes, changelog, and case study,
- remaining work is optional polish or deployment rather than core functionality.
