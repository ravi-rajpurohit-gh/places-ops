import argparse
import datetime
import os

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic PlacesOps source data.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed used for reproducible datasets.")
    parser.add_argument(
        "--as-of-date",
        type=datetime.date.fromisoformat,
        default=datetime.date.today(),
        help="Latest expense date in YYYY-MM-DD format.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = np.random.default_rng(args.seed)
    os.makedirs("raw_data", exist_ok=True)

    vendors = pd.DataFrame(
        {
            "vendor_id": range(101, 121),
            "vendor_name": [f"Contractor_{i}" for i in range(101, 121)],
            "reliability_score": rng.integers(70, 100, 20),
        }
    )
    vendors.to_csv("raw_data/vendors.csv", index=False)

    campuses = ["Southeast Region", "Texas Region", "West Region", "Mid-Atlantic Region"]
    project_types = ["Community Buildout", "Land Development", "Model Home Refresh", "Infrastructure Upgrade"]
    projects = pd.DataFrame(
        {
            "project_id": range(1001, 1051),
            "project_name": [f"{rng.choice(project_types)} {i}" for i in range(1001, 1051)],
            "campus": rng.choice(campuses, 50),
            "budget_allocated": rng.uniform(50000, 500000, 50).round(2),
            "status": rng.choice(["In Progress", "Completed", "Delayed"], 50, p=[0.6, 0.2, 0.2]),
        }
    )
    projects.to_csv("raw_data/projects.csv", index=False)

    dates = [args.as_of_date - datetime.timedelta(days=x) for x in range(90)]
    expenses = pd.DataFrame(
        [
            {
                "expense_id": expense_id,
                "project_id": rng.choice(projects["project_id"]),
                "vendor_id": rng.choice(vendors["vendor_id"]),
                "expense_date": rng.choice(dates),
                "amount": round(rng.uniform(1000, 25000), 2),
                "category": rng.choice(["Materials", "Labor", "Permits", "Equipment"]),
            }
            for expense_id in range(1, 501)
        ]
    )
    expenses.to_csv("raw_data/expenses.csv", index=False)

    print(
        f"Generated {len(projects)} projects, {len(vendors)} vendors, and {len(expenses)} expenses "
        f"with seed={args.seed} and as_of_date={args.as_of_date}."
    )


if __name__ == "__main__":
    main()
