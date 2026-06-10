# PlacesOps App Runbook

This folder contains the Streamlit application, dbt project, DuckDB sample warehouse, and model documentation for PlacesOps.

For the product narrative, architecture, lifecycle, and dbt design decisions, start with the repository-level [README](../README.md), [case study](../docs/CASE_STUDY.md), and [dbt implementation guide](../docs/DBT_IMPLEMENTATION_GUIDE.md).

## Local Stack

- Python
- DuckDB
- dbt Core
- Streamlit
- Altair
- Pandas
- PyYAML

## Folder Map

| Path | Purpose |
| --- | --- |
| `app.py` | Streamlit dashboard and governed assistant surface. |
| `models/staging/` | dbt staging models for generated project, vendor, and expense data. |
| `models/intermediate/` | Reusable project-level spend aggregation. |
| `models/marts/` | Expense-grain and project-performance marts. |
| `macros/` | Reusable Jinja business rules and generic tests. |
| `snapshots/` | SCD Type 2 project status and budget history. |
| `tests/` | Cross-model reconciliation assertions. |
| `models/schema.yml` | dbt tests plus model and column documentation used by the Dictionary tab. |
| `places_database.duckdb` | Portable sample warehouse used by the public Streamlit app. |
| `.streamlit/config.toml` | App theme configuration. |

## Run Locally

From the repository root:

```bash
python generate_mock_data.py
cd app
pip install -r requirements.txt
dbt build --profiles-dir .
streamlit run app.py
```

Use `python generate_mock_data.py --seed 42 --as-of-date YYYY-MM-DD` when reproducing a specific source-data fixture.

## Notes

- `target/`, `logs/`, `.user.yml`, and local dbt packages are ignored because they are machine-generated local artifacts.
- `places_database.duckdb` is intentionally tracked so the deployed app can load a validated sample warehouse immediately.
- Platform Health falls back to project-defined model/test inventory when generated dbt run artifacts are not bundled.
- Re-run `dbt build --profiles-dir .` after changing model SQL or tests.
