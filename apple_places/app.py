import streamlit as st
import duckdb
import pandas as pd
import json
import os
import datetime
import altair as alt

# Get the absolute path of the directory where app.py lives
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(CURRENT_DIR, 'places_database.duckdb')
TARGET_PATH = os.path.join(CURRENT_DIR, 'target', 'run_results.json')
MANIFEST_PATH = os.path.join(CURRENT_DIR, 'target', 'manifest.json')

# 1. Page Configuration
st.set_page_config(page_title="Places Data Hub", layout="wide")
st.title("Apple Places: Operations & Data Health")

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
tab1, tab2, tab3 = st.tabs(["Places Operations", "DBT Pipeline Health", "Data Dictionary"])

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
    
    st.subheader("Spend by Project")
    spend_by_project = filtered_df.groupby('project_name')['amount'].sum().reset_index()
    spend_by_project = spend_by_project.rename(columns={
        'project_name': 'Project Name', 
        'amount': 'Amount Spent ($)'
    })
    st.bar_chart(spend_by_project, x='Project Name', y='Amount Spent ($)')
 
    # Daily Trend Line Chart
    st.subheader("Daily Spend Trend")
    trend_df = filtered_df.groupby('expense_date')['amount'].sum().reset_index()
    trend_df = trend_df.rename(columns={
        'expense_date': 'Date', 
        'amount': 'Daily Spend ($)'
    })
    st.line_chart(trend_df, x='Date', y='Daily Spend ($)')

    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Spend by Category")
        category_df = filtered_df.groupby('category')['amount'].sum().reset_index()
        # Rename columns for the UI
        category_df = category_df.rename(columns={
            'category': 'Category', 
            'amount': 'Amount'
        })
        total_category_spend = category_df['Amount'].sum()
        
        # 1. Base Donut Chart
        base_chart = alt.Chart(category_df).mark_arc(innerRadius=80).encode(
            theta=alt.Theta(field="Amount", type="quantitative"),
            color=alt.Color(
                field="Category", 
                type="nominal", 
                legend=alt.Legend(title="Category", orient="left")
            ),
            tooltip=['Category', 'Amount']
        )
        
        # 2. Text Chart (Total dollar amount formatted for the center)
        text_chart = alt.Chart(pd.DataFrame({'total': [f"${total_category_spend:,.0f}"]})).mark_text(
            size=22, 
            fontWeight='bold',
            color='#1d1d1f' # Apple's dark grey text color
        ).encode(
            text='total:N'
        )
        
        # 3. Layer them together
        donut_chart = alt.layer(base_chart, text_chart).properties(height=300)
        
        st.altair_chart(donut_chart, use_container_width=True)

    with col_b:
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

# ==========================================
# TAB 3: Data Dictionary - Dynamically generated from dbt
# ==========================================
with tab3:
    st.header("Data Dictionary")
    st.write("Automatically generated from dbt `manifest.json`. This ensures documentation to be perfectly in sync with codebase.")
    
    try:
        with open(MANIFEST_PATH, 'r') as f:
            manifest = json.load(f)
            
        nodes = manifest.get('nodes', {})
        # Filter for actual models, ignoring tests or seeds
        models = {k: v for k, v in nodes.items() if v.get('resource_type') == 'model'}
        
        for node_id, model_data in models.items():
            model_name = model_data.get('name', 'Unknown')
            model_desc = model_data.get('description', 'No description provided.')
            
            st.subheader(f"`{model_name}`")
            st.write(model_desc)
            
            # Extract column descriptions if they exist
            columns = model_data.get('columns', {})
            col_list = []
            for col_name, col_details in columns.items():
                desc = col_details.get('description', '')
                if desc:  # Only show columns we explicitly documented
                    col_list.append({"Column Name": col_name, "Description": desc})
            
            if col_list:
                st.dataframe(pd.DataFrame(col_list), hide_index=True, use_container_width=True)
            else:
                st.caption("No column-level documentation available for this model.")
            
            st.markdown("\n")
            
    except FileNotFoundError:
        st.warning("Data dictionary unavailable. Run `dbt build` locally.")


# --- FOOTER ---
st.divider()
st.markdown("Built by [Ravi Rajpurohit](http://linktr.ee/hey_ravi) — Feedback: ravirajpurohit414@gmail.com", help="Data Engineering & Infrastructure", text_alignment="center")
st.markdown("[LinkedIn](https://www.linkedin.com/in/ravi-rajpurohit/) | [GitHub](https://github.com/ravi-rajpurohit-gh/) | [Medium](https://ravi-rajpurohit.medium.com/)", text_alignment="center")