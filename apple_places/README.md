# Apple Places: Data Architecture & Operations Prototype

A lightweight, end-to-end data pipeline and monitoring dashboard built to handle complex business domains (construction, vendors, budgets) while maintaining strict engineering standards.

## The Architecture

This project simulates a modern, modular data stack designed for rapid internal tooling:

- **Ingestion:** Python scripts generating mock relational data (Facilities, Vendors, Expenses).
- **Warehouse:** **DuckDB** acting as the highly performant, local OLAP database.
- **Transformation:** **dbt-core** orchestrating the DAG with 3 staging models and 1 final business-logic mart.
- **Serving/UI:** **Streamlit** providing a dual-view application for both business stakeholders and data engineering.

## Dashboard Features

The application is split into two distinct views:

1.  **Places Operations (Business View):** Tracks capital expenditure against allocated budgets across campuses and highlights vendor reliability risks.
2.  **dbt Pipeline Health (Engineering View):** Parses dbt's native `run_results.json` artifacts to monitor model execution times, helping identify bottlenecks in complex DAGs.

## How to Run Locally

1. Clone the repository.
2. Change directory to 'apple_places'.
3. Install dependencies: `pip install -r requirements.txt`
4. Run the application: `streamlit run app.py`
