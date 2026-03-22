# 🏢 PlacesOps: Business Intelligence & Operations Prototype

![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)

> A lightweight, end-to-end data pipeline and monitoring dashboard built to handle complex business domains (construction, vendors, budgets) while maintaining strict engineering standards.

**🚀 Live Demo:** [Live App](https://places-ops.streamlit.app/)

---

## 🏗️ The Architecture

This project simulates a modern, modular data stack designed for rapid internal tooling.

- **Ingestion:** Python scripts generating mock relational data for Facilities, Vendors, and Expenses.
- **Warehouse:** **DuckDB** acting as the highly performant, local OLAP database.
- **Transformation:** **dbt-core** orchestrating the DAG with 3 staging models and 1 final business-logic mart.
- **Serving/UI:** **Streamlit** providing a dual-view application.

## 🎭 Two Views, One Application

I designed this dashboard to serve two very different audiences simultaneously:

1. **📊 Places Operations (The Business View):** Tracks capital expenditure against allocated budgets across campuses. It highlights vendor reliability risks and helps stakeholders make strategic, data-driven decisions.
2. **⚙️ dbt Pipeline Health (The Engineering View):** Parses dbt's native `run_results.json` artifacts to monitor model execution times, instantly identifying bottlenecks in complex DAGs.

## 💻 Run It Locally

```bash
git clone [https://github.com/ravi-rajpurohit-gh/apple_places.git](https://github.com/ravi-rajpurohit-gh/apple_places.git)
cd apple_places
pip install -r requirements.txt
streamlit run app.py
```
