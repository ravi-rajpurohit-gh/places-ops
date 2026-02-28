import streamlit as st
import duckdb
import pandas as pd
import json
import os
import datetime

# Get the absolute path of the directory where app.py lives
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(CURRENT_DIR, 'places_database.duckdb')
TARGET_PATH = os.path.join(CURRENT_DIR, 'target', 'run_results.json')

# 1. Page Configuration
st.set_page_config(page_title="Places Data Hub", layout="wide")
st.title("Apple Places: Operations & Data Health")

# --- Hide Streamlit Branding ---
hide_st_style = """
            <style>
            [data-testid="stToolbar"] {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
# st.markdown(hide_st_style, unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.sidebar.title("Architecture Stack")
    st.markdown("""
    **Warehouse:** DuckDB (Local)  
    **Transformation:** dbt-core (1 Mart, 3 Staging)  
    **UI/UX:** Streamlit  
    **Language:** Python  
    """)
    st.caption("Built as a rapid prototype for the Apple Places data ecosystem.")
    st.divider()
    # Dynamic Data Freshness Timestamp
    if os.path.exists(TARGET_PATH):
        mod_time = os.path.getmtime(TARGET_PATH)
        timestamp = datetime.datetime.fromtimestamp(mod_time).strftime('%b %d, %Y - %I:%M %p')
    else:
        timestamp = "Unknown"
        
    st.caption(f"🔄 **Data Freshness:** {timestamp}")
    st.divider()
    st.markdown("**Goal:** Demonstrate end-to-end modeling of complex business data (construction/vendors) and dbt artifact monitoring.")

# --- DATA LOADING (Cached for Performance) ---
@st.cache_data
def load_data():
    # Using a context manager ensures the connection safely closes
    with duckdb.connect(DB_PATH, read_only=True) as conn:
        return conn.execute("SELECT * FROM main.fct_project_spend").df()

df = load_data()

# 2. Create the Two Tabs
tab1, tab2 = st.tabs(["Places Operations", "DBT Pipeline Health"])

# ==========================================
# TAB 1: The Business View (For Construction PMs)
# ==========================================
with tab1:
    st.header("Campus Construction & Budget Tracking")
    
    # Calculate High-Level KPIs
    # Drop duplicates to avoid overcounting budgets that appear on multiple expense rows
    project_budgets = df[['project_name', 'budget_allocated']].drop_duplicates()
    total_budget = project_budgets['budget_allocated'].sum()
    total_spend = df['amount'].sum()
    total_remaining = total_budget - total_spend
    percentage_spend = 100 * total_spend / total_budget if total_budget > 0 else 0
    
    # Display KPIs
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Allocated Budget", f"${total_budget:,.0f}")
    col2.metric("Total Capital Spent", f"${total_spend:,.0f}", delta="Expected Burn Rate", delta_color="off")
    col3.metric("Remaining Budget", f"${total_remaining:,.0f}", delta=f"{(100 - percentage_spend):.1f}% Buffer", delta_color="normal")
    
    # Cap the progress bar at 1.0 to prevent Streamlit errors if spend exceeds budget
    st.progress(min(percentage_spend / 100, 1.0), text=f"{percentage_spend:,.2f}% of budget consumed")
    
    st.divider()
    
    # Interactive Campus Filter
    campuses = df['campus'].unique()
    selected_campus = st.selectbox("Select a Campus to drill down:", campuses)
    
    filtered_df = df[df['campus'] == selected_campus]
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Spend by Project")
        spend_by_project = filtered_df.groupby('project_name')['amount'].sum().reset_index()
        spend_by_project = spend_by_project.rename(columns={
            'project_name': 'Project Name', 
            'amount': 'Amount Spent ($)'
        })
        st.bar_chart(spend_by_project, x='Project Name', y='Amount Spent ($)')
 
    with col_b:
        # Daily Trend Line Chart
        st.subheader("Daily Spend Trend")
        trend_df = filtered_df.groupby('expense_date')['amount'].sum().reset_index()
        trend_df = trend_df.rename(columns={
            'expense_date': 'Date', 
            'amount': 'Daily Spend ($)'
        })
        st.line_chart(trend_df, x='Date', y='Daily Spend ($)')
        
    col_c, _ = st.columns(2)
    
    with col_c:
        st.subheader("⚠️ Vendor Risk Assessment")
        reliability_threshold = st.slider("Flag vendors with reliability scores below threshold:", 
                                          min_value=0, max_value=100, value=90,
                                          step=1, help="Adjust the threshold to instantly filter out high-risk contractors.")
        
        # Find risky vendors for this specific campus
        risk_df = filtered_df[['vendor_name', 'reliability_score']].drop_duplicates()
        risky_vendors = risk_df[risk_df['reliability_score'] < reliability_threshold].sort_values('reliability_score')
        
        # Rename columns for the UI
        risky_vendors = risky_vendors.rename(columns={
            'vendor_name': 'Vendor Name', 
            'reliability_score': 'Reliability Score'
        })

        # Apply gradient
        styled_table = risky_vendors.style.background_gradient(
            subset=['Reliability Score'], 
            cmap='Reds_r', 
            vmin=50, 
            vmax=100
        )

        st.dataframe(styled_table, use_container_width=True, hide_index=True)

# ==========================================
# TAB 2: The Engineering View
# ==========================================
with tab2:
    st.header("DBT Pipeline Health & Telemetry")
    st.write("Monitoring execution telemetry to optimize the DAG and ensure data reliability.")
    
    try:
        # Read the metadata dbt just generated
        with open(TARGET_PATH, 'r') as f:
            run_results = json.load(f)
        
        results = run_results.get('results', [])
        
        # 1. Calculate High-Level KPIs
        total_nodes = len(results)
        successful_nodes = sum(1 for r in results if r.get('status') in ['success', 'pass'])
        success_rate = (successful_nodes / total_nodes) * 100 if total_nodes > 0 else 0
        total_time = sum(r.get('execution_time', 0) for r in results)
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Nodes Executed", total_nodes)
        col2.metric("Pipeline Success Rate", f"{success_rate:.0f}%")
        col3.metric("Total Execution Time", f"{total_time:.2f}s")

        st.progress(min(success_rate / 100, 1.0), text=f"{success_rate:,.2f}% success rate")
        
        st.divider()
        
        # 2. Parse out Models vs. Tests
        models_data = []
        tests_data = []
        
        for result in results:
            unique_id = result.get('unique_id', '')
            node_type = unique_id.split('.')[0]  # Extracts 'model' or 'test'
            name = unique_id.split('.')[-1]
            status = result.get('status', 'unknown').upper()
            exec_time = result.get('execution_time', 0)
            
            row = {'Node Name': name, 'Status': status, 'Time (s)': round(exec_time, 2)}
            
            if node_type == 'model':
                models_data.append(row)
            elif node_type == 'test':
                tests_data.append(row)
                
        # 3. Visualizing Model Bottlenecks
        st.subheader("Model Execution Bottlenecks")
        if models_data:
            models_df = pd.DataFrame(models_data).sort_values('Time (s)', ascending=False)
            st.bar_chart(models_df, x="Node Name", y="Time (s)")
        else:
            st.info("No model telemetry found.")

        # 4. Splitting the view for Tests and Raw Data
        col_t1, col_t2 = st.columns(2)
        
        with col_t1:
            st.subheader("Data Quality Tests")
            st.write("Results from schema and custom assertions:")
            if tests_data:
                tests_df = pd.DataFrame(tests_data)
                
                # Apply color coding to statuses (Green for PASS, Red for FAIL)
                def color_status(val):
                    color = '#28a745' if val == 'PASS' else '#dc3545' if val == 'FAIL' else '#ffc107'
                    return f'color: {color}; font-weight: bold'
                    
                styled_tests = tests_df.style.map(color_status, subset=['Status'])
                st.dataframe(styled_tests, use_container_width=True, hide_index=True)
            else:
                st.info("No data quality tests were executed in this run.")
                
        with col_t2:
            st.subheader("Raw Telemetry Log")
            st.write("Detailed artifact output:")
            all_df = pd.DataFrame(models_data + tests_data)
            st.dataframe(all_df, use_container_width=True, hide_index=True)
            
    except FileNotFoundError:
        st.warning("run_results.json not found.")

# --- FOOTER ---
st.divider()
st.markdown("Built by [Ravi Rajpurohit](https://www.linkedin.com/in/ravi-rajpurohit/) — Feedback: ravirajpurohit414@gmail.com", help="Data Engineering & Infrastructure", text_alignment="center")
st.markdown("[LinkedIn](https://www.linkedin.com/in/ravi-rajpurohit/) | [GitHub](https://github.com/ravi-rajpurohit-gh/) | [Medium](https://ravi-rajpurohit.medium.com/)", text_alignment="center")