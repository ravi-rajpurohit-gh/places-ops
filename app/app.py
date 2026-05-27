from __future__ import annotations

import datetime
import html
import json
import os
import time
from typing import Optional

import altair as alt
import duckdb
import pandas as pd
import streamlit as st
import yaml


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(CURRENT_DIR, "places_database.duckdb")
TARGET_PATH = os.path.join(CURRENT_DIR, "target", "run_results.json")
MANIFEST_PATH = os.path.join(CURRENT_DIR, "target", "manifest.json")
SCHEMA_PATH = os.path.join(CURRENT_DIR, "models", "schema.yml")

PALETTE = {
    "bg": "#070705",
    "panel": "#12120f",
    "panel_2": "#181814",
    "border": "#303028",
    "text": "#f7f4eb",
    "muted": "#a8a294",
    "dim": "#736f66",
    "sage": "#b8c48a",
    "gold": "#d8a63a",
    "copper": "#c46f3d",
    "blue": "#4e7db8",
    "red": "#d06455",
    "green": "#7fb069",
    "purple": "#9b88c8",
}

CHART_COLORS = [PALETTE["sage"], PALETTE["blue"], PALETTE["copper"], PALETTE["gold"]]
CATEGORY_COLORS = [PALETTE["gold"], PALETTE["sage"], PALETTE["copper"], PALETTE["blue"], PALETTE["green"], PALETTE["purple"]]


CUSTOM_CSS = f"""
<style>
.stApp {{
    background:
        radial-gradient(circle at 82% -12%, rgba(184,196,138,0.12), transparent 36rem),
        radial-gradient(circle at 8% 8%, rgba(196,111,61,0.08), transparent 26rem),
        linear-gradient(180deg, #090907 0%, #070705 52%, #070705 100%);
    color: {PALETTE["text"]};
}}

.stApp header {{
    background: transparent !important;
}}

.block-container {{
    max-width: 1440px;
    padding-top: 1.3rem;
    padding-bottom: 3rem;
}}

[data-testid="stSidebar"] {{
    background: #080806;
    border-right: 1px solid {PALETTE["border"]};
}}

[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
[data-testid="stCaptionContainer"] {{
    color: {PALETTE["muted"]};
}}

[data-testid="stSidebar"] label,
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] {{
    color: {PALETTE["muted"]} !important;
}}

h1, h2, h3 {{
    letter-spacing: 0;
}}

h2 {{
    color: {PALETTE["muted"]} !important;
    font-size: 0.78rem !important;
    font-weight: 800 !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase;
}}

[data-testid="stTabs"] [role="tab"] {{
    color: {PALETTE["muted"]};
    font-size: 0.68rem;
    font-weight: 800;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding-left: 0.65rem;
    padding-right: 0.65rem;
}}

[data-testid="stTabs"] [role="tab"][aria-selected="true"] {{
    color: {PALETTE["text"]};
    border-bottom-color: {PALETTE["gold"]};
}}

div[data-baseweb="select"] > div {{
    background: {PALETTE["panel"]} !important;
    border: 1px solid {PALETTE["border"]} !important;
    border-radius: 7px !important;
    color: {PALETTE["text"]} !important;
}}

div[data-baseweb="select"] span {{
    color: {PALETTE["text"]} !important;
}}

[data-baseweb="popover"],
[data-baseweb="popover"] > div,
[data-baseweb="menu"],
ul[role="listbox"] {{
    background: {PALETTE["panel_2"]} !important;
    border: 1px solid {PALETTE["border"]} !important;
    color: {PALETTE["text"]} !important;
}}

li[role="option"],
div[role="option"],
[data-baseweb="menu"] li,
[data-baseweb="menu"] div {{
    background: {PALETTE["panel_2"]} !important;
    color: {PALETTE["text"]} !important;
}}

li[role="option"]:hover,
div[role="option"]:hover,
[data-baseweb="menu"] li:hover,
[data-baseweb="menu"] div:hover {{
    background: rgba(216,166,58,0.16) !important;
    color: {PALETTE["text"]} !important;
}}

[role="listbox"] [role="option"][aria-selected="true"],
[data-baseweb="menu"] [role="option"][aria-selected="true"],
[role="listbox"] [role="option"]:focus,
[data-baseweb="menu"] [role="option"]:focus {{
    background: rgba(216,166,58,0.22) !important;
    color: {PALETTE["text"]} !important;
}}

[data-testid="stMetric"] {{
    background: rgba(18,18,15,0.48);
    border: 1px solid rgba(48,48,40,0.72);
    border-radius: 8px;
    padding: 0.85rem 0.95rem;
}}

[data-testid="stMetricLabel"] p {{
    color: {PALETTE["muted"]} !important;
    font-size: 0.72rem !important;
    font-weight: 800 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase;
}}

[data-testid="stMetricValue"] {{
    color: {PALETTE["text"]} !important;
    font-weight: 800 !important;
    font-size: 1.62rem !important;
    line-height: 1.12 !important;
    text-shadow: 0 1px 18px rgba(216,166,58,0.10);
}}

[data-testid="stMetricValue"] div {{
    overflow: visible !important;
    text-overflow: clip !important;
}}

[data-testid="stMetricDelta"] {{
    color: {PALETTE["muted"]} !important;
}}

[data-testid="stButton"] button {{
    background: linear-gradient(180deg, rgba(255,255,255,0.045), rgba(255,255,255,0.018)) !important;
    border: 1px solid {PALETTE["border"]} !important;
    border-radius: 7px !important;
    color: {PALETTE["text"]} !important;
    min-height: 2.65rem;
    font-weight: 650 !important;
}}

[data-testid="stButton"] button:hover {{
    border-color: rgba(216,166,58,0.70) !important;
    color: {PALETTE["text"]} !important;
    background: linear-gradient(180deg, rgba(216,166,58,0.14), rgba(255,255,255,0.025)) !important;
}}

[data-testid="stChatInput"] {{
    background: transparent !important;
}}

[data-testid="stChatInput"] > div {{
    background: rgba(18,18,15,0.74) !important;
    border: 1px solid rgba(216,166,58,0.34) !important;
    border-radius: 9px !important;
}}

[data-testid="stChatInput"] textarea {{
    background: {PALETTE["panel_2"]} !important;
    border: 1px solid rgba(216,166,58,0.34) !important;
    border-radius: 8px !important;
    color: {PALETTE["text"]} !important;
}}

textarea,
input,
[data-baseweb="input"] > div,
[data-baseweb="textarea"] > div {{
    background: {PALETTE["panel_2"]} !important;
    color: {PALETTE["text"]} !important;
    border-color: rgba(216,166,58,0.34) !important;
}}

[data-testid="stChatInput"] textarea::placeholder {{
    color: {PALETTE["muted"]} !important;
}}

[data-testid="stChatInput"] button {{
    background: {PALETTE["gold"]} !important;
    color: #090907 !important;
    border-radius: 7px !important;
}}

[data-testid="stVerticalBlockBorderWrapper"] {{
    border-color: rgba(216,166,58,0.28) !important;
    background: rgba(18,18,15,0.58) !important;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.035);
}}

[data-testid="stDataFrame"] {{
    background: rgba(18,18,15,0.72) !important;
    border: 1px solid rgba(216,166,58,0.24) !important;
    border-radius: 8px !important;
    overflow: hidden;
}}

[data-testid="stDataFrame"] div {{
    color: {PALETTE["text"]};
}}

.po-table-wrap {{
    border: 1px solid rgba(216,166,58,0.26);
    border-radius: 8px;
    background: rgba(18,18,15,0.82);
    overflow: auto;
    margin: 0.35rem 0 1rem;
}}

.po-table-wrap.fixed {{
    max-height: 330px;
}}

.po-table {{
    border-collapse: collapse;
    width: 100%;
    min-width: 540px;
    color: {PALETTE["text"]};
    font-size: 0.86rem;
}}

.po-table thead tr {{
    background: rgba(216,166,58,0.13);
}}

.po-table th {{
    color: {PALETTE["gold"]};
    font-size: 0.72rem;
    letter-spacing: 0.08em;
    padding: 0.75rem 0.85rem;
    text-align: left;
    text-transform: uppercase;
    border-bottom: 1px solid rgba(216,166,58,0.24);
}}

.po-table td {{
    color: {PALETTE["text"]};
    padding: 0.68rem 0.85rem;
    border-bottom: 1px solid rgba(255,255,255,0.055);
}}

.po-table tbody tr:nth-child(even) {{
    background: rgba(255,255,255,0.025);
}}

.po-table tbody tr:hover {{
    background: rgba(216,166,58,0.10);
}}

label,
[data-testid="stWidgetLabel"] p {{
    color: {PALETTE["muted"]} !important;
    font-weight: 750 !important;
}}

[data-testid="stSlider"] label p {{
    color: {PALETTE["muted"]} !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-size: 0.72rem !important;
}}

.po-header {{
    border-bottom: 1px solid {PALETTE["border"]};
    padding: 0.35rem 0 1.1rem;
    margin-bottom: 1.1rem;
}}

.po-kicker {{
    color: {PALETTE["gold"]};
    font-size: 0.82rem;
    font-weight: 800;
    letter-spacing: 0.15em;
    text-transform: uppercase;
}}

.po-title {{
    color: {PALETTE["text"]};
    font-size: 2.05rem;
    line-height: 1.05;
    font-weight: 800;
    margin-top: 0.25rem;
}}

.po-subtitle {{
    color: {PALETTE["muted"]};
    max-width: 860px;
    line-height: 1.55;
    margin-top: 0.45rem;
}}

.panel {{
    border: 1px solid {PALETTE["border"]};
    border-radius: 8px;
    background: rgba(17,20,18,0.78);
    padding: 1rem;
}}

.panel-title {{
    display: inline-block;
    font-size: 0.78rem;
    font-weight: 800;
    letter-spacing: 0.13em;
    text-transform: uppercase;
    margin-bottom: 0.65rem;
    background: linear-gradient(90deg, {PALETTE["gold"]}, {PALETTE["sage"]}, {PALETTE["gold"]});
    background-size: 220% auto;
    -webkit-background-clip: text;
    background-clip: text;
    -webkit-text-fill-color: transparent;
    animation: insight-shine 7s linear infinite;
}}

@keyframes insight-shine {{
    0% {{ background-position: 220% center; }}
    100% {{ background-position: -220% center; }}
}}

.insight-copy {{
    color: {PALETTE["text"]};
    font-size: 0.98rem;
    line-height: 1.65;
}}

.caption-mono {{
    color: {PALETTE["dim"]};
    font-family: monospace;
    font-size: 0.7rem;
    letter-spacing: 0.04em;
    margin-top: 0.7rem;
}}

.sidebar-footer {{
    border-top: 1px solid {PALETTE["border"]};
    margin-top: 1.25rem;
    padding-top: 0.95rem;
    color: {PALETTE["muted"]};
    font-size: 0.76rem;
    line-height: 1.6;
}}

.sidebar-footer strong {{
    color: {PALETTE["text"]};
}}

.sidebar-footer a {{
    color: {PALETTE["gold"]};
    text-decoration: none;
}}

.app-footer {{
    border-top: 1px solid rgba(216,166,58,0.30);
    margin-top: 2.3rem;
    padding-top: 1.05rem;
    text-align: center;
    color: {PALETTE["muted"]};
    font-size: 0.82rem;
}}

.app-footer a {{
    color: {PALETTE["gold"]};
    font-weight: 750;
    text-decoration: none;
}}

.assistant-intro {{
    border: 1px solid {PALETTE["border"]};
    border-radius: 8px;
    background: rgba(18,18,15,0.68);
    padding: 1rem;
    margin: 0.35rem 0 1rem;
}}

.assistant-intro strong {{
    color: {PALETTE["text"]};
}}

.assistant-intro span {{
    color: {PALETTE["muted"]};
}}

.assistant-message {{
    border: 1px solid rgba(216,166,58,0.32);
    border-radius: 8px;
    padding: 0.95rem 1rem;
    margin: 0.8rem 0;
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.035), 0 14px 32px rgba(0,0,0,0.18);
}}

.assistant-message.user {{
    background: linear-gradient(180deg, rgba(216,166,58,0.16), rgba(18,18,15,0.84));
    margin-left: 28%;
}}

.assistant-message.ai {{
    background: linear-gradient(180deg, rgba(184,196,138,0.11), rgba(18,18,15,0.88));
    margin-right: 16%;
}}

.assistant-role {{
    color: {PALETTE["gold"]};
    font-size: 0.7rem;
    font-weight: 850;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-bottom: 0.45rem;
}}

.assistant-body {{
    color: {PALETTE["text"]};
    line-height: 1.55;
}}
</style>
"""


def money(value: float) -> str:
    return f"${value:,.0f}"


def option_label(value: str) -> str:
    return str(value).replace("_", " ").replace("-", " ").title()


def estimate_tokens(text: str) -> int:
    return max(1, round(len(text) / 4))


def style_chart(chart: alt.Chart, height: int = 300) -> alt.Chart:
    return (
        chart.properties(height=height)
        .configure_view(strokeWidth=0)
        .configure_axis(
            grid=False,
            gridOpacity=0,
            gridColor="transparent",
            domain=False,
            domainOpacity=0,
            ticks=False,
            tickOpacity=0,
            labelColor=PALETTE["muted"],
            titleColor=PALETTE["muted"],
            labelAngle=-45,
            labelLimit=180,
        )
        .configure_axisX(grid=False, gridOpacity=0, domain=False, ticks=False)
        .configure_axisY(grid=False, gridOpacity=0, domain=False, ticks=False)
        .configure_legend(labelColor=PALETTE["muted"], titleColor=PALETTE["muted"])
        .configure(background="transparent")
    )


def render_dark_table(data: pd.DataFrame, fixed_height: bool = False) -> None:
    table = data.copy()
    html_table = table.to_html(index=False, escape=True, classes="po-table", border=0)
    wrapper_class = "po-table-wrap fixed" if fixed_height else "po-table-wrap"
    st.markdown(f'<div class="{wrapper_class}">{html_table}</div>', unsafe_allow_html=True)


def safe_html(text: object) -> str:
    return html.escape(str(text)).replace("\n", "<br>")


def render_insight_panel(title: str, body: str, source: str) -> None:
    st.markdown(
        f"""
<div class="panel">
  <div class="panel-title">{safe_html(title)}</div>
  <div class="insight-copy">{safe_html(body)}</div>
  <div class="caption-mono">{safe_html(source)}</div>
</div>
""",
        unsafe_allow_html=True,
    )


def x_axis(field: str, title: Optional[str] = None, sort: Optional[str] = None, label_angle: int = -45) -> alt.X:
    return alt.X(
        field,
        title=title,
        sort=sort,
        axis=alt.Axis(grid=False, domain=False, ticks=False, labelAngle=label_angle, labelLimit=180),
    )


def y_axis(field: str, title: str) -> alt.Y:
    return alt.Y(field, title=title, axis=alt.Axis(grid=False, domain=False, ticks=False))


@st.cache_data
def load_data() -> pd.DataFrame:
    with duckdb.connect(DB_PATH, read_only=True) as conn:
        return conn.execute("select * from main.fct_project_spend").df()


@st.cache_data
def load_run_results() -> dict:
    if not os.path.exists(TARGET_PATH):
        return {"results": []}
    with open(TARGET_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


@st.cache_data
def load_manifest() -> dict:
    if not os.path.exists(MANIFEST_PATH):
        return {"nodes": {}}
    with open(MANIFEST_PATH, "r", encoding="utf-8") as file:
        return json.load(file)


@st.cache_data
def load_schema_docs() -> list[dict[str, object]]:
    if not os.path.exists(SCHEMA_PATH):
        return []
    with open(SCHEMA_PATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file).get("models", [])


def project_summary(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data.groupby(["project_name", "campus", "status", "budget_allocated"], as_index=False)
        .agg(
            total_spend=("amount", "sum"),
            vendor_count=("vendor_name", "nunique"),
            avg_vendor_reliability=("reliability_score", "mean"),
            last_expense_date=("expense_date", "max"),
        )
        .assign(
            budget_variance=lambda d: d["total_spend"] - d["budget_allocated"],
            budget_used_pct=lambda d: d["total_spend"] * 100 / d["budget_allocated"],
        )
        .sort_values("budget_variance", ascending=False)
    )


def portfolio_metrics(data: pd.DataFrame) -> dict[str, float]:
    projects = project_summary(data)
    total_budget = projects["budget_allocated"].sum()
    total_spend = data["amount"].sum()
    delayed = projects[projects["status"] == "Delayed"]
    risky_vendors = data[["vendor_name", "reliability_score"]].drop_duplicates()
    risky_vendors = risky_vendors[risky_vendors["reliability_score"] < 85]
    return {
        "project_count": float(len(projects)),
        "total_budget": float(total_budget),
        "total_spend": float(total_spend),
        "budget_variance": float(total_spend - total_budget),
        "remaining_budget": float(total_budget - total_spend),
        "over_budget_exposure": float(projects.loc[projects["budget_variance"] > 0, "budget_variance"].sum()),
        "budget_used_pct": float(total_spend * 100 / total_budget) if total_budget else 0,
        "delayed_exposure": float(delayed["budget_allocated"].sum()),
        "risky_vendor_count": float(len(risky_vendors)),
        "avg_vendor_reliability": float(data[["vendor_name", "reliability_score"]].drop_duplicates()["reliability_score"].mean()),
    }


def dbt_health_summary() -> tuple[pd.DataFrame, dict[str, float]]:
    results = load_run_results().get("results", [])
    rows = []
    for result in results:
        unique_id = result.get("unique_id", "")
        rows.append(
            {
                "node_type": unique_id.split(".")[0] if unique_id else "unknown",
                "node_name": unique_id.split(".")[-1] if unique_id else "unknown",
                "status": result.get("status", "unknown").upper(),
                "execution_time_s": round(result.get("execution_time", 0), 3),
                "source": "dbt artifact",
            }
        )
    if not rows:
        rows = [
            {"node_type": "model", "node_name": "stg_expenses", "status": "READY", "execution_time_s": 0.000, "source": "project definition"},
            {"node_type": "model", "node_name": "stg_projects", "status": "READY", "execution_time_s": 0.000, "source": "project definition"},
            {"node_type": "model", "node_name": "stg_vendors", "status": "READY", "execution_time_s": 0.000, "source": "project definition"},
            {"node_type": "model", "node_name": "fct_project_spend", "status": "READY", "execution_time_s": 0.000, "source": "project definition"},
            {"node_type": "test", "node_name": "primary_key_checks", "status": "DEFINED", "execution_time_s": 0.000, "source": "schema.yml"},
            {"node_type": "test", "node_name": "accepted_status_values", "status": "DEFINED", "execution_time_s": 0.000, "source": "schema.yml"},
        ]
    telemetry = pd.DataFrame(rows)
    total_nodes = len(telemetry)
    successful_statuses = ["SUCCESS", "PASS", "READY", "DEFINED"]
    successful = int(telemetry["status"].isin(successful_statuses).sum()) if total_nodes else 0
    metrics = {
        "total_nodes": float(total_nodes),
        "success_rate": float(successful * 100 / total_nodes) if total_nodes else 0,
        "total_time": float(telemetry["execution_time_s"].sum()) if total_nodes else 0,
        "artifact_available": bool(results),
    }
    return telemetry, metrics


def metric_dictionary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Metric": "Total Spend",
                "Definition": "Sum of posted expense line amounts in the current filter context.",
                "Business Use": "Track capital outflow by region, project status, vendor, and category.",
            },
            {
                "Metric": "Remaining Budget",
                "Definition": "Allocated project budget minus posted spend for projects in scope.",
                "Business Use": "Surface how much budget capacity remains before additional commitments.",
            },
            {
                "Metric": "Delayed Exposure",
                "Definition": "Allocated budget tied to projects currently marked Delayed.",
                "Business Use": "Prioritize operational review by budget at risk, not just project count.",
            },
            {
                "Metric": "Vendor Reliability Risk",
                "Definition": "Vendors below the selected reliability threshold.",
                "Business Use": "Create a governed review queue for procurement and project operations.",
            },
            {
                "Metric": "Pipeline Success Rate",
                "Definition": "Share of latest dbt nodes with SUCCESS or PASS status in run artifacts.",
                "Business Use": "Keep trust in dashboard metrics visible beside executive analytics.",
            },
        ]
    )


def governed_answer(prompt: str, data: pd.DataFrame, filters: dict[str, str]) -> tuple[str, dict[str, object]]:
    started = time.perf_counter()
    question = prompt.lower()
    metrics = portfolio_metrics(data)
    projects = project_summary(data)
    selected_tool = "summarize_portfolio"
    confidence = "medium"

    if any(term in question for term in ["vendor", "contractor", "reliability", "risk"]):
        selected_tool = "summarize_vendor_risk"
        confidence = "high"
        vendors = data[["vendor_name", "reliability_score"]].drop_duplicates().sort_values("reliability_score")
        weakest = vendors.iloc[0]
        answer = (
            f"The current filter has {int(metrics['risky_vendor_count'])} vendors below the 85 reliability threshold. "
            f"The lowest-scoring vendor is {weakest['vendor_name']} at {weakest['reliability_score']:.0f}. "
            "This is a practical review queue for procurement, project operations, and finance controls."
        )
        rows_considered = int(len(vendors))
    elif any(term in question for term in ["delay", "delayed", "exposure", "project risk"]):
        selected_tool = "summarize_delayed_project_exposure"
        confidence = "high"
        delayed = projects[projects["status"] == "Delayed"].sort_values("budget_allocated", ascending=False)
        top = delayed.iloc[0] if not delayed.empty else None
        top_text = f" The largest delayed project is {top['project_name']} at {money(top['budget_allocated'])} budget exposure." if top is not None else ""
        answer = f"Delayed-project exposure is {money(metrics['delayed_exposure'])} across the current portfolio.{top_text}"
        rows_considered = int(len(projects))
    elif any(term in question for term in ["category", "materials", "labor", "permits", "equipment"]):
        selected_tool = "summarize_cost_category"
        confidence = "high"
        categories = data.groupby("category", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
        top = categories.iloc[0]
        answer = (
            f"The largest cost category is {option_label(top['category'])} at {money(top['amount'])}. "
            "This helps prioritize cost optimization conversations by spend driver instead of by anecdote."
        )
        rows_considered = int(len(data))
    elif any(term in question for term in ["pipeline", "dbt", "quality", "test", "freshness", "model"]):
        selected_tool = "summarize_pipeline_health"
        confidence = "high"
        _, health = dbt_health_summary()
        health_source = "latest dbt artifact" if health["artifact_available"] else "project-defined dbt model and test inventory"
        answer = (
            f"The {health_source} shows {int(health['total_nodes'])} tracked nodes, "
            f"{health['success_rate']:.0f}% ready/pass coverage, and {health['total_time']:.2f}s recorded execution time. "
            "That keeps model reliability visible beside business metrics."
        )
        rows_considered = int(health["total_nodes"])
    elif any(term in question for term in ["region", "campus", "division", "area"]):
        selected_tool = "summarize_region_performance"
        confidence = "high"
        regions = data.groupby("campus", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
        top = regions.iloc[0]
        answer = f"{option_label(top['campus'])} has the highest spend in the current data at {money(top['amount'])}."
        rows_considered = int(len(regions))
    else:
        answer = (
            f"The current portfolio includes {int(metrics['project_count'])} projects, {money(metrics['total_spend'])} in spend, "
            f"{money(metrics['total_budget'])} in allocated budget, {money(metrics['remaining_budget'])} remaining budget, "
            f"and {money(metrics['delayed_exposure'])} in delayed-project exposure. "
            f"Average vendor reliability is {metrics['avg_vendor_reliability']:.1f}."
        )
        rows_considered = int(len(data))

    latency = time.perf_counter() - started
    metadata = {
        "mode": "governed_function_router",
        "selected_tool": selected_tool,
        "confidence": confidence,
        "api_calls": 0,
        "api_cost_usd": 0.0,
        "prompt_tokens_estimated": estimate_tokens(prompt),
        "completion_tokens_estimated": estimate_tokens(answer),
        "total_tokens_estimated": estimate_tokens(prompt) + estimate_tokens(answer),
        "rows_considered": rows_considered,
        "latency_s": round(latency, 3),
        "filters": filters,
    }
    return answer, metadata


def assistant_visual(selected_tool: str, data: pd.DataFrame) -> Optional[alt.Chart]:
    if selected_tool in {"summarize_portfolio", "summarize_region_performance"}:
        regional = data.groupby("campus", as_index=False)["amount"].sum()
        regional["region"] = regional["campus"].map(option_label)
        return alt.Chart(regional).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=x_axis("region:N", sort="-y"),
            y=y_axis("amount:Q", "Spend"),
            color=alt.Color("region:N", legend=None, scale=alt.Scale(range=CHART_COLORS)),
            tooltip=[alt.Tooltip("region:N", title="Region"), alt.Tooltip("amount:Q", title="Spend", format="$,.0f")],
        )

    if selected_tool == "summarize_vendor_risk":
        vendors = data[["vendor_name", "reliability_score"]].drop_duplicates().sort_values("reliability_score").head(10)
        return alt.Chart(vendors).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=x_axis("vendor_name:N", sort="y"),
            y=y_axis("reliability_score:Q", "Reliability Score"),
            color=alt.Color("reliability_score:Q", legend=None, scale=alt.Scale(range=[PALETTE["red"], PALETTE["gold"], PALETTE["sage"]])),
            tooltip=[alt.Tooltip("vendor_name:N", title="Vendor"), alt.Tooltip("reliability_score:Q", title="Reliability", format=".0f")],
        )

    if selected_tool == "summarize_delayed_project_exposure":
        projects = project_summary(data)
        delayed = projects[projects["status"] == "Delayed"].sort_values("budget_allocated", ascending=False).head(10)
        return alt.Chart(delayed).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=x_axis("project_name:N", sort="-y"),
            y=y_axis("budget_allocated:Q", "Delayed Budget Exposure"),
            color=alt.value(PALETTE["red"]),
            tooltip=[
                alt.Tooltip("project_name:N", title="Project"),
                alt.Tooltip("budget_allocated:Q", title="Budget Exposure", format="$,.0f"),
                alt.Tooltip("campus:N", title="Region"),
            ],
        )

    if selected_tool == "summarize_cost_category":
        categories = data.groupby("category", as_index=False)["amount"].sum()
        categories["category_label"] = categories["category"].map(option_label)
        return alt.Chart(categories).mark_arc(innerRadius=70).encode(
            theta=alt.Theta("amount:Q"),
            color=alt.Color("category_label:N", title="Category", scale=alt.Scale(range=CATEGORY_COLORS)),
            tooltip=[alt.Tooltip("category_label:N", title="Category"), alt.Tooltip("amount:Q", title="Spend", format="$,.0f")],
        )

    if selected_tool == "summarize_pipeline_health":
        telemetry, _ = dbt_health_summary()
        if telemetry.empty:
            return None
        return alt.Chart(telemetry).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=x_axis("node_name:N", sort="-y"),
            y=y_axis("execution_time_s:Q", "Execution Time (s)"),
            color=alt.Color("node_type:N", title="Node Type"),
            tooltip=[
                alt.Tooltip("node_name:N", title="Node"),
                alt.Tooltip("node_type:N", title="Type"),
                alt.Tooltip("status:N", title="Status"),
                alt.Tooltip("execution_time_s:Q", title="Execution Time", format=".3f"),
            ],
        )

    return None


def render_trace(metadata: dict[str, object]) -> None:
    with st.expander("Analysis trace", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tool", option_label(str(metadata["selected_tool"])))
        c2.metric("Estimated tokens", f"{metadata['total_tokens_estimated']:,}")
        c3.metric("Rows considered", f"{metadata['rows_considered']:,}")
        c4.metric("Latency", f"{metadata['latency_s']:.3f}s")
        c5, c6 = st.columns(2)
        c5.metric("API calls", f"{metadata['api_calls']}")
        c6.metric("API cost", f"${metadata['api_cost_usd']:.2f}")
        st.json(metadata)


def render_assistant_turn(message: dict[str, object], data: pd.DataFrame) -> None:
    content = safe_html(message["content"])
    if message["role"] == "user":
        st.markdown(
            f"""
<div class="assistant-message user">
  <div class="assistant-role">You</div>
  <div class="assistant-body">{content}</div>
</div>
""",
            unsafe_allow_html=True,
        )
        return

    usage = message.get("usage")
    st.markdown(
        f"""
<div class="assistant-message ai">
  <div class="assistant-role">PlacesOps Analyst · Governed Visual Analysis</div>
  <div class="assistant-body">{content}</div>
</div>
""",
        unsafe_allow_html=True,
    )
    if isinstance(usage, dict):
        chart = assistant_visual(str(usage["selected_tool"]), data)
        if chart is not None:
            st.altair_chart(style_chart(chart, height=300), use_container_width=True)
        render_trace(usage)


st.set_page_config(page_title="PlacesOps", layout="wide", initial_sidebar_state="expanded")
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

df = load_data()
all_regions = ["All"] + sorted(df["campus"].dropna().unique().tolist())
all_statuses = ["All"] + sorted(df["status"].dropna().unique().tolist())

with st.sidebar:
    st.markdown("## PlacesOps")
    st.caption("Construction, vendor, budget, and corporate operations analytics.")
    st.markdown("## Filters")
    selected_region = st.selectbox("Region", all_regions, format_func=option_label)
    selected_status = st.selectbox("Project Status", all_statuses, format_func=option_label)
    st.markdown("## Current Stack")
    st.caption("Python · DuckDB · dbt Core · Streamlit · Altair · Pandas")
    st.markdown("## Production Mirror")
    st.caption("AWS S3/Glue · Snowflake · dbt · Qlik/Power BI · CloudWatch · governed AI")
    freshness_path = TARGET_PATH if os.path.exists(TARGET_PATH) else DB_PATH
    if os.path.exists(freshness_path):
        mod_time = os.path.getmtime(freshness_path)
        timestamp = datetime.datetime.fromtimestamp(mod_time).strftime("%b %d, %Y - %I:%M %p")
    else:
        timestamp = "Unknown"
    st.caption(f"Data freshness: {timestamp}")
    st.markdown(
        """
<div class="sidebar-footer">
  <strong>Built by Ravi Rajpurohit</strong><br>
  Data Engineering · Corporate Analytics · Governed AI<br>
  <a href="https://ravirajpurohit.com" target="_blank">ravirajpurohit.com</a>
</div>
""",
        unsafe_allow_html=True,
    )

filtered_df = df.copy()
if selected_region != "All":
    filtered_df = filtered_df[filtered_df["campus"] == selected_region]
if selected_status != "All":
    filtered_df = filtered_df[filtered_df["status"] == selected_status]

metrics = portfolio_metrics(filtered_df)
projects = project_summary(filtered_df)

st.markdown(
    """
<div class="po-header">
  <div class="po-kicker">PlacesOps Data Hub</div>
  <div class="po-title">Construction & Corporate Analytics Command Center</div>
  <div class="po-subtitle">
    Budget performance, delayed-project exposure, vendor reliability, cost drivers, dbt health, and governed
    natural-language analysis over trusted operational marts.
  </div>
</div>
""",
    unsafe_allow_html=True,
)

tab_ops, tab_cost, tab_health, tab_dictionary, tab_assistant = st.tabs(
    ["Executive Ops", "Cost & Risk", "Platform Health", "Dictionary", "Assistant"]
)

with tab_ops:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Projects", f"{int(metrics['project_count']):,}")
    c2.metric("Total Spend", money(metrics["total_spend"]))
    c3.metric("Remaining Budget", money(metrics["remaining_budget"]))
    c4.metric("Delayed Exposure", money(metrics["delayed_exposure"]))
    st.progress(min(metrics["budget_used_pct"] / 100, 1.0), text=f"{metrics['budget_used_pct']:.1f}% of allocated budget consumed")

    left, right = st.columns([1.15, 0.85])
    with left:
        st.markdown("## Spend By Region")
        region_spend = filtered_df.groupby("campus", as_index=False)["amount"].sum()
        region_spend["region"] = region_spend["campus"].map(option_label)
        chart = alt.Chart(region_spend).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=x_axis("region:N", sort="-y"),
            y=y_axis("amount:Q", "Spend"),
            color=alt.Color("region:N", legend=None, scale=alt.Scale(range=CHART_COLORS)),
            tooltip=[alt.Tooltip("region:N", title="Region"), alt.Tooltip("amount:Q", title="Spend", format="$,.0f")],
        )
        st.altair_chart(style_chart(chart, height=320), use_container_width=True)
    with right:
        delayed_count = int((projects["status"] == "Delayed").sum())
        insight = (
            f"The current portfolio has {int(metrics['project_count'])} projects and {money(metrics['delayed_exposure'])} "
            f"in delayed-project exposure across {delayed_count} delayed projects. Remaining budget is {money(metrics['remaining_budget'])} "
            f"after {money(metrics['total_spend'])} in posted spend, so the highest-value review path is delayed work, "
            "high-spend categories, and low-reliability vendors."
        )
        render_insight_panel("Governed Executive Insight", insight, "Derived from fct_project_spend and current filters.")

    st.markdown("## Project Spend Vs Budget")
    variance_chart = alt.Chart(projects.head(12)).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        x=x_axis("project_name:N", sort="-y"),
        y=y_axis("budget_variance:Q", "Spend Minus Budget"),
        color=alt.Color("budget_variance:Q", legend=None, scale=alt.Scale(range=[PALETTE["sage"], PALETTE["gold"], PALETTE["red"]])),
        tooltip=[
            alt.Tooltip("project_name:N", title="Project"),
            alt.Tooltip("campus:N", title="Region"),
            alt.Tooltip("status:N", title="Status"),
            alt.Tooltip("budget_variance:Q", title="Budget Variance", format="$,.0f"),
        ],
    )
    st.altair_chart(style_chart(variance_chart, height=330), use_container_width=True)

with tab_cost:
    left, right = st.columns(2)
    with left:
        st.markdown("## Cost By Category")
        categories = filtered_df.groupby("category", as_index=False)["amount"].sum()
        categories["category_label"] = categories["category"].map(option_label)
        category_chart = alt.Chart(categories).mark_arc(innerRadius=78).encode(
            theta=alt.Theta("amount:Q"),
            color=alt.Color("category_label:N", title="Category", scale=alt.Scale(range=CATEGORY_COLORS)),
            tooltip=[alt.Tooltip("category_label:N", title="Category"), alt.Tooltip("amount:Q", title="Spend", format="$,.0f")],
        )
        st.altair_chart(style_chart(category_chart, height=330), use_container_width=True)
    with right:
        st.markdown("## Vendor Reliability Risk")
        threshold = st.slider("Reliability Threshold", min_value=70, max_value=100, value=85, step=1)
        vendors = filtered_df[["vendor_name", "reliability_score"]].drop_duplicates()
        risky_vendors = vendors[vendors["reliability_score"] < threshold].sort_values("reliability_score")
        render_dark_table(risky_vendors.rename(columns={"vendor_name": "Vendor", "reliability_score": "Reliability Score"}), fixed_height=True)

    top_category = categories.sort_values("amount", ascending=False).iloc[0] if not categories.empty else None
    weakest_vendor = vendors.sort_values("reliability_score").iloc[0] if not vendors.empty else None
    if top_category is not None and weakest_vendor is not None:
        cost_insight = (
            f"{option_label(top_category['category'])} is the largest spend category at {money(top_category['amount'])}. "
            f"{len(risky_vendors)} vendors are below the current {threshold} reliability threshold; the lowest-scoring vendor is "
            f"{weakest_vendor['vendor_name']} at {weakest_vendor['reliability_score']:.0f}. This links cost optimization to vendor governance."
        )
        render_insight_panel("Governed Cost And Risk Insight", cost_insight, "Derived from category spend, vendor reliability, and the selected threshold.")

    st.markdown("## Daily Spend Trend")
    trend = filtered_df.groupby("expense_date", as_index=False)["amount"].sum()
    trend_chart = alt.Chart(trend).mark_line(point=True, strokeWidth=2.5).encode(
        x=x_axis("expense_date:T", title="Date"),
        y=y_axis("amount:Q", "Daily Spend"),
        color=alt.value(PALETTE["gold"]),
        tooltip=[alt.Tooltip("expense_date:T", title="Date"), alt.Tooltip("amount:Q", title="Spend", format="$,.0f")],
    )
    st.altair_chart(style_chart(trend_chart, height=310), use_container_width=True)

with tab_health:
    telemetry, health = dbt_health_summary()
    c1, c2, c3 = st.columns(3)
    c1.metric("Nodes Tracked", f"{int(health['total_nodes']):,}")
    c2.metric("Ready/Pass Coverage", f"{health['success_rate']:.0f}%")
    c3.metric("Recorded Runtime", f"{health['total_time']:.2f}s")
    st.progress(min(health["success_rate"] / 100, 1.0), text=f"{health['success_rate']:.1f}% dbt readiness coverage")

    health_source = "latest dbt run artifact" if health["artifact_available"] else "project-defined model and test inventory"
    health_insight = (
        f"The {health_source} reports {int(health['total_nodes'])} tracked nodes with {health['success_rate']:.0f}% ready/pass coverage "
        f"and {health['total_time']:.2f}s recorded runtime. Keeping this beside the business dashboard makes metric trust observable."
    )
    source_caption = "Derived from dbt run_results.json artifact telemetry." if health["artifact_available"] else "Derived from dbt project definitions because generated run artifacts are not bundled."
    render_insight_panel("Governed Platform Insight", health_insight, source_caption)

    st.markdown("## Model And Test Telemetry")
    render_dark_table(
        telemetry.rename(
            columns={
                "node_type": "Node Type",
                "node_name": "Node Name",
                "status": "Status",
                "execution_time_s": "Execution Time (s)",
                "source": "Source",
            }
        )
    )
    st.markdown("## Execution Bottlenecks")
    st.altair_chart(style_chart(assistant_visual("summarize_pipeline_health", filtered_df), height=320), use_container_width=True)

with tab_dictionary:
    st.markdown("## Governed Metric Dictionary")
    render_dark_table(metric_dictionary())
    render_insight_panel(
        "Governed Metric Insight",
        "The dashboard separates business-facing metric definitions from dbt model documentation. That gives an AI assistant approved language for answering executive questions while preserving lineage back to modeled tables.",
        "Definitions are curated in the application layer and backed by dbt model documentation.",
    )

    st.markdown("## dbt Metric And Model Dictionary")
    schema_models = load_schema_docs()
    if not schema_models:
        manifest = load_manifest()
        nodes = manifest.get("nodes", {})
        schema_models = [value for value in nodes.values() if value.get("resource_type") == "model"]
    if not schema_models:
        st.warning("dbt schema documentation not found. Add model descriptions to models/schema.yml.")
    for model_data in schema_models:
        with st.container(border=True):
            st.markdown(f"### `{model_data.get('name', 'unknown')}`")
            st.write(model_data.get("description", "No description provided."))
            columns = []
            raw_columns = model_data.get("columns", {})
            column_items = raw_columns.items() if isinstance(raw_columns, dict) else [(col.get("name", ""), col) for col in raw_columns]
            for column_name, column_details in column_items:
                description = column_details.get("description", "")
                if description:
                    columns.append({"Column": column_name, "Description": description})
            if columns:
                render_dark_table(pd.DataFrame(columns))
            else:
                st.caption("No column-level documentation available.")

with tab_assistant:
    st.markdown("## Governed Insights Assistant")
    st.markdown(
        """
<div class="assistant-intro">
  <strong>Ask operational questions in plain language.</strong><br>
  <span>The assistant routes each question to governed portfolio, cost, vendor, project-risk, or platform-health functions, then returns a precise answer, chart, and trace metadata.</span>
</div>
""",
        unsafe_allow_html=True,
    )
    suggestions = [
        "Summarize the current portfolio.",
        "Where is delayed-project exposure highest?",
        "Which vendors are risky?",
        "What is the biggest cost category?",
        "How healthy is the dbt pipeline?",
        "Which region has the most spend?",
    ]
    if "places_assistant_messages" not in st.session_state:
        st.session_state["places_assistant_messages"] = []

    if not st.session_state["places_assistant_messages"]:
        cols = st.columns(2)
        for idx, suggestion in enumerate(suggestions):
            if cols[idx % 2].button(suggestion, use_container_width=True):
                st.session_state["places_assistant_prompt"] = suggestion

    if st.button("Clear assistant history"):
        st.session_state["places_assistant_messages"] = []
        st.rerun()

    for message in st.session_state["places_assistant_messages"]:
        render_assistant_turn(message, filtered_df)

    prompt = st.chat_input("Ask about operations, cost, vendor risk, project exposure, or platform health...")
    prompt = prompt or st.session_state.pop("places_assistant_prompt", None)
    if prompt:
        user_message = {"role": "user", "content": prompt}
        st.session_state["places_assistant_messages"].append(user_message)
        answer, metadata = governed_answer(
            prompt,
            filtered_df,
            {"region": selected_region, "project_status": selected_status},
        )
        st.session_state["places_assistant_messages"].append({"role": "assistant", "content": answer, "usage": metadata})
        st.rerun()

st.markdown(
    """
<div class="app-footer">
  Built by <a href="https://ravirajpurohit.com" target="_blank">Ravi Rajpurohit</a>
</div>
""",
    unsafe_allow_html=True,
)
