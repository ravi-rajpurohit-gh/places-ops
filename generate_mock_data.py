import pandas as pd
import numpy as np
import datetime
import os

# Create data directory
os.makedirs('raw_data', exist_ok=True)

# 1. Generate Vendors
vendors = pd.DataFrame({
    'vendor_id': range(101, 121),
    'vendor_name': [f'Contractor_{i}' for i in range(101, 121)],
    'reliability_score': np.random.randint(70, 100, 20)
})
vendors.to_csv('raw_data/vendors.csv', index=False)

# 2. Generate Projects (Apple Campuses/Buildings)
campuses = ['Apple Park', 'Infinite Loop', 'Austin Campus', 'Culver City']
project_types = ['Expansion', 'Renovation', 'Remodeling', 'Retrofitting']
projects = pd.DataFrame({
    'project_id': range(1001, 1051),
    'project_name': [f'{np.random.choice(project_types)} {i}' for i in range(1001, 1051)],
    'campus': np.random.choice(campuses, 50),
    'budget_allocated': np.random.uniform(50000, 500000, 50).round(2),
    'status': np.random.choice(['In Progress', 'Completed', 'Delayed'], 50, p=[0.6, 0.2, 0.2])
})
projects.to_csv('raw_data/projects.csv', index=False)

# 3. Generate Daily Expenses
dates = [datetime.date.today() - datetime.timedelta(days=x) for x in range(90)]
expenses = []

for _ in range(500): # 500 expense records
    expenses.append({
        'expense_id': _ + 1,
        'project_id': np.random.choice(projects['project_id']),
        'vendor_id': np.random.choice(vendors['vendor_id']),
        'expense_date': np.random.choice(dates),
        'amount': round(np.random.uniform(1000, 25000), 2),
        'category': np.random.choice(['Materials', 'Labor', 'Permits', 'Equipment'])
    })

pd.DataFrame(expenses).to_csv('raw_data/expenses.csv', index=False)

print("Mock construction data generated successfully in '/raw_data'")