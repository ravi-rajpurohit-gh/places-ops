# Lennar PlacesOps Project Tracker

Last updated: 2026-05-25

## Goal

Use `places-ops` as the Lennar-facing proof project for construction operations, corporate analytics, budget tracking, vendor risk, dbt modeling, dashboarding, data quality, and pipeline health.

## Success Criteria

- Be ready to mention the project briefly on the 2026-05-26 recruiter call if relevant.
- Keep the project business-first for Lennar: construction, vendors, budgets, delayed work, cost categories, and operational risk.
- Use the project more deeply in later technical rounds after learning more from Glenn and the team.
- Position the production version around AWS, Snowflake, dbt, Qlik/Power BI, CI/CD, CloudWatch-style observability, and governed AI dashboards.

## Current Status

- Existing `places-ops` project reviewed at `/Users/ravirajpurohit/Downloads/Developer/places-ops`.
- README updated to frame the project as `PlacesOps: Construction & Corporate Analytics Hub`.
- Nested app README updated with Lennar-facing production mapping and AI dashboard direction.
- Streamlit app visible branding changed from Apple-specific language to neutral construction/corporate analytics language.
- Added dashboard KPIs for Budget Variance and Delayed-Project Exposure.
- Browser verification completed at `http://localhost:8502`.
- App enhanced into five workflows: Executive Operations, Cost & Vendor Risk, Data Platform Health, Metric Dictionary, and Insights Assistant.
- Generated data and DuckDB tables now use neutral operating regions instead of company-specific campuses.
- Governed Insights Assistant added for portfolio, delayed exposure, vendor risk, cost category, regional spend, and dbt health questions.
- Documentation added for changelog, engineering notes, and case study checkpoint.

## Key Decisions

- Do not build a second Lennar-specific project before the recruiter call.
- Use `places-ops` as the Lennar proof point because it already maps to construction operations, vendors, budgets, dbt, dashboarding, and pipeline health.
- Keep the recruiter call conversational; do not over-demo unless Glenn asks.
- Wait until after the recruiter call to decide whether to add more Lennar-specific changes.
- Use a governed assistant router rather than a free-form LLM dependency so the AI dashboard story works without API keys, quota limits, or hallucinated metrics.
- Keep assistant visuals tied to approved analytical routes: portfolio summary, vendor risk, delayed exposure, cost category, regional spend, and dbt health.

## Progress Log

| Date | Progress |
| --- | --- |
| 2026-05-24 | Reviewed `places-ops` README, Streamlit app, and dbt model structure. |
| 2026-05-24 | Updated root README and nested app README with Lennar interview positioning, production mapping, observability, and AI dashboard direction. |
| 2026-05-24 | Updated Streamlit app title/sidebar and added Budget Variance plus Delayed-Project Exposure KPIs. |
| 2026-05-24 | Verified updated app in browser at `http://localhost:8502`. |
| 2026-05-25 | Rewrote READMEs around project vision, target users, Lennar role alignment, production mapping, and production-grade proof-of-concept positioning. |
| 2026-05-25 | Rebuilt Streamlit surface into Executive Operations, Cost & Vendor Risk, Data Platform Health, Metric Dictionary, and Insights Assistant workflows. |
| 2026-05-25 | Added governed visual analyst with contextual charts, selected tool, confidence, estimated tokens, rows considered, latency, API calls, and API cost. |
| 2026-05-25 | Neutralized generated regions and schema documentation away from company-specific campus naming. |
| 2026-05-25 | Refreshed DuckDB warehouse from regenerated project/vendor/expense CSVs and verified neutral regions in `fct_project_spend`. |
| 2026-05-25 | Added changelog, engineering notes, and case study documentation for project lifecycle and checkpoint tracking. |

## Open Questions

- What exact stack and domains Glenn confirms on the recruiter call.
- Whether the next Lennar round expects a live demo, system design discussion, or coding/SQL screen.
- Whether to add Qlik-specific language, Snowflake DDL examples, or an AI dashboard assistant after the recruiter call.
- Whether to deploy the updated app publicly after final screenshot/README polish.
- Whether to add a Snowflake-style DDL/dbt production appendix.
- Whether to add an optional provider-backed LLM interpretation layer behind the governed router.

## Next Actions

- Practice a 30-90 second walkthrough centered on business value first, then engineering trust.
- Prepare answers for dbt modeling, Snowflake migration, Qlik/Power BI serving, AWS orchestration, data quality, cost optimization, and AI dashboard governance.
- Optionally add README screenshots and deployment notes after the visual design is final.

## Recruiter Call Pitch

Use only if relevant:

> I recently built a dbt + Streamlit operations analytics prototype around construction projects, vendors, budgets, and pipeline health. It felt close to the cross-functional analytics work Connor described: taking messy operational data, modeling it into trusted business metrics, and giving both business users and engineers visibility into performance and reliability.

## Storyline

Lead with: "I build data platforms that turn messy cross-functional business data into trusted metrics and self-serve analytics."

Anchor examples:

- State of Michigan AWS lakehouse consolidating 15+ sources.
- Executive reporting reduced from 3 days to under 1 hour.
- Dimensional dashboards for non-technical stakeholders.
- Nutanix performance analytics and cloud cost optimization.
- Observability with CloudWatch, Prometheus, Grafana, and Splunk.
