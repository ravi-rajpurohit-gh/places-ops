# PlacesOps Project Tracker

Last updated: 2026-06-10

## Goal

Build PlacesOps as a maintainable analytics reference implementation for construction operations, corporate reporting, budget tracking, vendor risk, dbt modeling, dashboarding, data quality, platform observability, and governed AI-assisted insights.

## Success Criteria

- Keep the app business-first: construction projects, vendors, budgets, delayed work, cost categories, and operational risk.
- Keep engineering trust visible: dbt models, tests, run artifacts, model documentation, and pipeline health.
- Keep the repository public-safe: no company-private data, employer-specific positioning, or application-process notes.
- Preserve lifecycle detail, engineering decisions, validation results, and operational boundaries.
- Maintain a credible production migration path around AWS, Snowflake, dbt, Qlik/Power BI, observability, and approved AI/MCP workflows.

## Current Status

- Repository is framed as `PlacesOps: Construction & Corporate Analytics Hub`.
- App folder was renamed from a legacy project-specific name to `app`.
- Streamlit app has five workflows: Executive Ops, Cost & Risk, Platform Health, Dictionary, and Assistant.
- Generated data and DuckDB tables use neutral operating regions.
- Governed Assistant supports portfolio, delayed exposure, vendor risk, cost category, regional spend, and dbt health questions.
- Cross-tab governed insight panels appear in Executive Ops, Cost & Risk, Platform Health, and Dictionary.
- UI uses a warm, dark, production-style operations theme with custom dark controls, tables, and chat surfaces.
- Dictionary includes business metric definitions plus current `models/schema.yml` model documentation.
- Supporting documentation includes architecture, data model, lifecycle, production mapping, decision log, and a dbt implementation guide.
- dbt implementation now includes reusable Jinja macros, layered staging/intermediate/mart models, 52 data tests, an SCD Type 2 snapshot, a dashboard exposure, and pull-request CI.

## Key Decisions

| Decision | Rationale | Result |
| --- | --- | --- |
| Use one Streamlit app with tabs | Keeps business, platform, dictionary, and assistant workflows easy to scan. | App remains simple to review and deploy. |
| Keep generated data neutral | Public repository should not imply access to company-private data. | Regions, vendors, projects, and expenses are synthetic and reusable. |
| Use DuckDB locally | Fast local iteration and portable app startup. | Production path maps to Snowflake. |
| Use dbt Core | Shows staging, mart modeling, tests, docs, and artifacts. | Platform health and dictionary workflows are grounded in transformation metadata. |
| Add governed AI patterns | Natural-language analytics should use trusted metric routes rather than unrestricted SQL generation. | Assistant returns deterministic answers, visuals, and trace metadata. |
| Read dictionary docs from `schema.yml` | Documentation edits should show in the app without requiring a manifest rebuild. | Model documentation remains current in the public app. |
| Remove build artifacts from git | Generated `target/` artifacts are noisy and can contain stale paths. | `.gitignore` excludes dbt build output and local logs. |

## Progress Log

| Date | Progress |
| --- | --- |
| 2026-05-24 | Created dbt + DuckDB + Streamlit foundation with generated project, vendor, budget, and expense source data. |
| 2026-05-24 | Built staging models, `fct_project_spend`, dbt tests, and initial dashboard surfaces. |
| 2026-05-25 | Reframed product as a construction and corporate analytics hub. |
| 2026-05-25 | Rebuilt Streamlit into Executive Ops, Cost & Risk, Platform Health, Dictionary, and Assistant workflows. |
| 2026-05-25 | Added governed assistant routes with contextual charts, selected tool, confidence, estimated tokens, rows considered, latency, API calls, and API cost. |
| 2026-05-25 | Neutralized generated regions and schema documentation away from company-specific language. |
| 2026-05-26 | Polished UI/UX with domain-aligned colors, dark Streamlit controls, production-style prompt cards, and portfolio attribution. |
| 2026-05-26 | Fixed native white surfaces with dark Streamlit theme config, custom dark tables, visible assistant bubbles, and dark chat input. |
| 2026-05-26 | Added cross-tab governed insight panels, aligned chart colors, scrollable vendor risk table, and expanded model documentation. |
| 2026-05-26 | Added a concise system overview and refreshed case study diagrams, architecture, data model, tech stack, and technical review points. |
| 2026-05-26 | Renamed app folder to `app`, removed public job-application language, and updated gitignore rules for local artifacts. |
| 2026-06-10 | Deepened dbt implementation with reusable Jinja macros, project variables, an intermediate aggregate, a project-performance mart, custom generic and reconciliation tests, an SCD Type 2 snapshot, a dashboard exposure, and pull-request CI. |

## Open Questions

- Whether to add screenshots or short GIFs to the case study.
- Whether to execute the documented Snowflake migration plan in a trial account.
- Whether to add a Qlik/Power BI serving-layer mock or screenshot.
- Whether to add an optional provider-backed LLM interpretation layer behind the governed router.

## Next Actions

- Keep the public README concise and product-oriented.
- Keep `docs/CASE_STUDY.md` aligned with implemented behavior and validation evidence.
- Rebuild the tracked sample warehouse and `run_results.json` when model logic changes.
- Execute the documented Snowflake deployment path in a trial or development account when credentials are available.
