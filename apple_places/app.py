import streamlit as st
import duckdb
import pandas as pd
import json

# 1. Page Configuration
st.set_page_config(page_title="Places Data Hub", layout="wide")
st.title("Apple Places: Operations & Data Health")

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("## Architecture Stack")
    st.markdown("""
    **Warehouse:** DuckDB (Local)  
    **Transformation:** dbt-core (1 Mart, 3 Staging)  
    **UI/UX:** Streamlit  
    **Language:** Python  
    """)
    st.caption("Built as a rapid prototype for the Apple Places data ecosystem.")
    st.divider()
    st.markdown("**Goal:** Demonstrate end-to-end modeling of complex business data (construction/vendors) and dbt artifact monitoring.")

# 2. Create the Two Tabs
tab1, tab2 = st.tabs(["Places Operations", "DBT Pipeline Health"])

# ==========================================
# TAB 1: The Business View (For Construction PMs)
# ==========================================
with tab1:
    st.header("Campus Construction & Budget Tracking")
    
    # Connect to DuckDB (read_only=True ensures it doesn't lock the database)
    conn = duckdb.connect('places_database.duckdb', read_only=True)
    
    # Query the dbt mart model
    df = conn.execute("SELECT * FROM main.fct_project_spend").df()
    
    # Calculate High-Level KPIs
    # Drop duplicates to avoid overcounting budgets that appear on multiple expense rows
    project_budgets = df[['project_name', 'budget_allocated']].drop_duplicates()
    total_budget = project_budgets['budget_allocated'].sum()
    total_spend = df['amount'].sum()
    total_remaining = total_budget - total_spend
    
    # Display KPIs
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Allocated Budget", f"${total_budget:,.0f}")
    col2.metric("Total Capital Spent", f"${total_spend:,.0f} ({100*total_spend/total_budget:,.2f}%)")
    col3.metric("Remaining Budget", f"${(total_remaining):,.0f} ({100*total_remaining/total_budget:,.2f}%)")
    
    st.divider()
    
    # Interactive Campus Filter
    campuses = df['campus'].unique()
    selected_campus = st.selectbox("Select a Campus to drill down:", campuses)
    
    filtered_df = df[df['campus'] == selected_campus]
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Spend by Project")
        spend_by_project = filtered_df.groupby('project_name')['amount'].sum().reset_index()
        st.bar_chart(spend_by_project, x='project_name', y='amount')
        
    with col_b:
        st.subheader("⚠️ Vendor Risk Assessment")
        reliability_threshold = st.number_input("Flagging vendors with reliability scores below:", min_value=1, max_value=100, value=90)
        # Find risky vendors for this specific campus
        risk_df = filtered_df[['vendor_name', 'reliability_score']].drop_duplicates()
        risky_vendors = risk_df[risk_df['reliability_score'] < reliability_threshold].sort_values('reliability_score')
        # Apply a minimalist red color gradient to highlight risk
        st.dataframe(
            risky_vendors.style.background_gradient(
                subset=['reliability_score'], 
                cmap='Reds_r', 
                vmin=50, 
                vmax=100
            ), 
            use_container_width=True, 
            hide_index=True
        )

# ==========================================
# TAB 2: The Engineering View
# ==========================================
with tab2:
    st.header("DBT Artifact Analyzer (Model Performance)")
    st.write("Monitoring execution times to optimize the DAG and prevent bottlenecks.")
    
    try:
        # Read the metadata by dbt
        with open('target/run_results.json', 'r') as f:
            run_results = json.load(f)
        
        models = []
        times = []
        
        # Parse the JSON to extract model names and how long they took to run
        for result in run_results.get('results', []):
            unique_id = result.get('unique_id', '')
            if unique_id.startswith('model.'):
                # Clean up the name (e.g., 'model.apple_places.stg_expenses' -> 'stg_expenses')
                clean_name = unique_id.split('.')[-1]
                models.append(clean_name)
                times.append(result.get('execution_time', 0))
                
        perf_df = pd.DataFrame({'Model Node': models, 'Execution Time (seconds)': times})
        
        st.subheader("Pipeline Execution Speeds")
        st.bar_chart(perf_df, x='Model Node', y='Execution Time (seconds)')
        
        st.subheader("Raw Artifact Data")
        st.dataframe(perf_df, use_container_width=True, hide_index=True)
        
    except FileNotFoundError:
        st.warning("run_results.json not found")