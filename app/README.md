# PlacesOps App Runbook

This folder contains the Streamlit application, dbt project, DuckDB sample warehouse, and model documentation for PlacesOps.

For the product narrative, architecture, lifecycle, and portfolio case study, start with the repository-level [README](../README.md), [case study](../docs/CASE_STUDY.md), and [portfolio brief](../docs/PORTFOLIO_BRIEF.md).

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
| `models/marts/` | dbt mart model for `fct_project_spend`. |
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

## Notes

- `target/`, `logs/`, `.user.yml`, and local dbt packages are ignored because they are machine-generated local artifacts.
- `places_database.duckdb` is intentionally tracked so the public app can load a sample warehouse immediately.
- Re-run `dbt build --profiles-dir .` after changing model SQL or tests.
