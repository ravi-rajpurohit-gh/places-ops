# Changelog

## 2026-05-26

- Added a portfolio brief and refreshed case study materials with current architecture, data model, lifecycle timeline, production mapping, and portfolio talking points.
- Aligned Cost & Risk chart colors with the application palette, added a fixed-height scrollable vendor risk table, moved Current Stack above Production Mirror, and embedded governed insight panels across Cost & Risk, Platform Health, and Dictionary.
- Expanded the Dictionary tab with business metric definitions and switched dbt model documentation to read from `models/schema.yml` so documentation edits appear without requiring a manifest refresh.
- Fixed remaining white Streamlit surfaces by moving the app theme config to the dark PlacesOps palette, replacing native dataframes with dark HTML tables, and rendering assistant turns as custom bordered dark message bubbles.
- Refined the PlacesOps theme consistency pass with centered portfolio-only footer attribution, visible footer divider, dark dropdown hover states, darker table/data dictionary surfaces, stronger assistant chat boundaries, and readable chat input styling.
- Reworked Executive Ops KPI semantics so the headline card shows `Remaining Budget` instead of a negative raw variance while retaining spend-minus-budget analysis in the project-level chart.
- Added the current local project stack to the sidebar beside the production mirror and rotated chart axis labels for easier scanning.

## 2026-05-25

- Tightened the app-wide UI theme with readable dark KPI cards, dark dropdown menus, hidden chart gridlines, and subtle animated insight headers.
- Polished the Streamlit theme toward a warm construction/corporate analytics visual system with dark native controls, readable assistant prompts, and portfolio attribution.
- Replaced multi-link attribution with `ravirajpurohit.com`.
- Reframed PlacesOps as a construction and corporate analytics command center.
- Rebuilt the Streamlit app around Executive Operations, Cost & Vendor Risk, Data Platform Health, Metric Dictionary, and Insights Assistant workflows.
- Added governed natural-language analysis for portfolio summary, delayed-project exposure, vendor reliability risk, cost categories, regional spend, and dbt pipeline health.
- Added contextual chart responses and analysis trace metadata with selected tool, confidence, estimated tokens, rows considered, latency, API calls, and API cost.
- Neutralized generated operating regions and dbt documentation away from company-specific campus language.
- Refreshed DuckDB tables from generated project, vendor, and expense data.
- Updated README, nested app README, tracker, engineering notes, and case study documentation.

## 2026-05-24

- Created initial dbt + DuckDB + Streamlit project for project spend, vendor reliability, budget tracking, dbt artifact monitoring, and data dictionary generation.
