import pandas as pd
from datetime import datetime, timedelta

# Create sample data for jan_dhan_registry_advanced.xlsx
data = {
    "Citizen_ID": ["123456789012", "987654321098", "555566667777", "111122223333"],
    "Name": ["Rahul Sharma", "Priya Patel", "Amit Kumar", "Sita Devi"],
    "Account_Status": ["Active", "Active", "Inactive", "Active"],
    "Aadhaar_Linked": [True, True, True, False],
    "Scheme_Eligibility": ["Health_Scheme", "Education_Scheme", "Health_Scheme", "Health_Scheme"],
    "Scheme_Amount": [5000, 10000, 5000, 5000],
    "Claim_Count": [2, 1, 5, 0],
    "Last_Claim_Date": [
        (datetime.today() - timedelta(days=45)).strftime("%Y-%m-%d"),
        (datetime.today() - timedelta(days=60)).strftime("%Y-%m-%d"),
        (datetime.today() - timedelta(days=10)).strftime("%Y-%m-%d"),
        (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")
    ]
}

df = pd.DataFrame(data)
df.to_excel("jan_dhan_registry_advanced.xlsx", index=False)
print("Sample data file created successfully!")
print(df)
