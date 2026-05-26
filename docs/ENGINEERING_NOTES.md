# Engineering Notes

Last updated: 2026-05-25

## Purpose

PlacesOps is a compact enterprise analytics reference implementation for construction operations, corporate analytics, vendor risk, budget tracking, data platform health, and governed AI-assisted reporting.

The project is intentionally small, but it mirrors production habits:

- generated operational source data,
- explicit staging models,
- a dashboard-ready spend fact table,
- dbt tests and artifacts,
- data dictionary generation from transformation metadata,
- Streamlit business and engineering workflows,
- and governed natural-language analysis over trusted marts.

## Design Decisions

### Neutral Operating Data

The generated data uses privacy-safe operating regions, projects, vendors, budgets, expenses, and statuses. It avoids company-private data and keeps the public project usable as a general construction/corporate analytics case study.

### DuckDB And dbt Core

DuckDB keeps local iteration fast, while dbt Core provides the production modeling pattern: staging layers, mart creation, tests, artifacts, and documentation. A production version would move the same modeling pattern into Snowflake with dbt Cloud/Core, CI, and orchestrated refreshes.

### Single Product Surface

The app stays as one Streamlit surface with tabs. That keeps business metrics, vendor risk, platform health, metric documentation, and AI-assisted analysis easy to scan in one walkthrough.

### Domain-Aligned UI Theme

The visual system uses a warm, restrained construction/corporate analytics palette instead of generic Streamlit defaults. Native controls, assistant prompts, chat input, sidebar filters, and insight panels are styled consistently so the product reads like an internal operations analytics application rather than a hobby dashboard.

The app intentionally remains dark because the product surface combines executive operations, platform health, and governed assistant workflows. The final theme removes bright chart gridlines, uses dark dropdown menus, makes KPI values readable in card surfaces, and highlights governed insight labels with a subtle left-to-right sheen.

### Governed Insights Assistant

The assistant is a function router, not a free-form SQL generator. It maps natural-language questions to approved analytical routes, returns deterministic answers, attaches contextual visuals, and shows trace metadata. This demonstrates the direction of AI-powered dashboards while avoiding API keys, hosted quota limits, local model setup, and hallucinated metric definitions.

### Platform Health Beside Business Metrics

Pipeline success, dbt node runtime, tests, and artifact visibility are shown in the same app as operational metrics. This reinforces the core data engineering idea that business trust depends on platform health.

## Production Mapping

| Local Implementation | Production Direction |
| --- | --- |
| CSV project/vendor/expense data | Source systems, SaaS exports, ERP, construction/project management systems |
| DuckDB | Snowflake analytical warehouse |
| dbt Core models | dbt DAG, tests, docs, exposures, semantic layer, and CI |
| Streamlit | Internal data app, Qlik/Power BI serving layer, or operational analytics portal |
| dbt artifacts | Orchestration metadata, CI checks, model runtime telemetry, and lineage |
| Governed assistant router | Approved AI/MCP workflow over governed metrics and documented marts |
| Local freshness timestamp | CloudWatch, Datadog, Airflow/Step Functions, or warehouse observability |

## Review Checklist

- Generate data: `python generate_mock_data.py`
- Build models and artifacts: `cd apple_places && dbt build --profiles-dir .`
- Run app: `streamlit run apple_places/app.py`
- Confirm tabs render: Executive Operations, Cost & Vendor Risk, Data Platform Health, Metric Dictionary, Insights Assistant
- Confirm regions are neutral operating regions, not company-specific campuses
- Confirm assistant returns text, a contextual chart, and analysis trace metadata
- Confirm dbt artifacts show passing model/test results

## Checkpoint Record

2026-05-25 checkpoint:

- Streamlit app enhanced into a corporate analytics command center.
- DuckDB warehouse refreshed with neutral generated regions.
- dbt artifact log shows `PASS=14 WARN=0 ERROR=0 SKIP=0`.
- Browser smoke test confirmed the main dashboard and assistant render correctly.
- Assistant tab theme was polished with dark controls, production-style prompt cards, and portfolio attribution at `ravirajpurohit.com`.
- Documentation updated across README, app README, changelog, tracker, engineering notes, and case study.
