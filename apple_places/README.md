# PlacesOps: Construction & Corporate Analytics Hub

PlacesOps is a lightweight, production-minded proof of concept for construction and corporate operations analytics. It models project, vendor, budget, and expense data into a dashboard that helps business stakeholders understand budget variance, delayed-project exposure, cost categories, and vendor reliability while giving data engineers visibility into pipeline health.

The data is synthetic. The project is designed to mirror the shape of enterprise analytics work without using or implying access to company-private data.

## Vision

The vision for PlacesOps is a small but realistic enterprise analytics hub:

- ingest operational data from projects, vendors, and expenses,
- standardize it through staging models,
- publish dashboard-ready marts,
- expose dbt pipeline health and data documentation,
- and provide a foundation for governed AI dashboards over trusted metrics.

## Who It Is Built For

- Corporate analytics teams supporting finance, marketing, HR, legal, construction operations, and leadership reporting.
- Business stakeholders who need fast answers about budget performance, project risk, vendor reliability, and cost drivers.
- Data engineering teams responsible for dbt models, warehouse marts, pipeline health, data quality, and BI enablement.
- AI product teams exploring governed dashboards and natural-language analytics over approved business metrics.

## What It Shows

- **Executive Operations:** projects, total spend, budget variance, delayed-project exposure, regional spend, and governed executive insight.
- **Cost & Vendor Risk:** cost categories, daily spend trend, reliability thresholding, and vendor review queues.
- **Data Platform Health:** model execution, test results, success rate, and model bottlenecks from dbt artifacts.
- **Metric Dictionary:** generated model and column documentation from the dbt manifest.
- **Insights Assistant:** governed natural-language analysis with text answers, contextual visuals, selected analytical tool, estimated tokens, rows considered, latency, API call count, and API cost.

## Why This Maps to Lennar

Lennar's Data Engineer II role emphasizes AWS, Snowflake, dbt, SQL/Python, Qlik, business insights, reporting platforms, cost efficiency, cross-functional analytics, and AI initiatives. PlacesOps mirrors that environment in a focused local prototype by turning construction and budget data into governed business metrics with visible pipeline health.

## Technology Stack

Built locally with Python, DuckDB, dbt Core, Streamlit, Altair, and Pandas.

Mirrors a production environment with AWS S3, AWS Glue, Lambda or Step Functions, Snowflake, dbt, Qlik or Power BI, CloudWatch-style observability, and approved AI/MCP services over trusted marts.

## How to Run Locally

1. Clone the repository.
2. Generate data from the repository root: `python generate_mock_data.py`
3. Change directory to `apple_places`.
4. Rebuild dbt models and artifacts: `dbt build --profiles-dir .`
5. Install dependencies: `pip install -r requirements.txt`
6. Run the application: `streamlit run app.py`
