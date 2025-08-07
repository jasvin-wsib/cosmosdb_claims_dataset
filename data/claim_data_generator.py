import pandas as pd
import os
from faker import Faker
import random

# Initialize Faker to generate fake dates
fake = Faker()

# Get the directory path where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Build full path to the CSV file assumed to be in the same directory
csv_path = os.path.join(script_dir, "original_sample_data.csv")

# Read the CSV data into a DataFrame
df = pd.read_csv(csv_path)

# Take the first 100 rows as a subset to work with
df_subset = df.head(100).copy()

# If the "age" column exists, drop it
if "age" in df_subset.columns:
    df_subset.drop(columns=["age"], inplace=True)

# Generate a unique numeric claimant ID for each row (1 to N)
df_subset["claimant_id"] = range(1, len(df_subset) + 1)

# Assign numeric agent IDs directly (without names)
total_agents = 25
df_subset["assigned_agent_id"] = [random.randint(1, total_agents) for _ in range(len(df_subset))]
df_subset["close_agent_id"] = [random.randint(1, total_agents) for _ in range(len(df_subset))]

# Add a filed_on column with fake dates within the last 5 years
df_subset["filed_on"] = [fake.date_between(start_date="-5y", end_date="today").isoformat() for _ in range(len(df_subset))]

# Rename accident types to more descriptive names (assumes column already exists)
df_subset["accident_type"] = df_subset["accident_type"].replace({
    "home": "slip and fall",
    "car": "repetitive strain injury",
    "workplace": "overexertion"
})

# Drop any old-style column names if they exist
for col in ["Claimant Name", "Claimant ID", "Assigned Agent", "Assigned Agent ID", "Close Agent", "Close Agent ID", "Filed On", "claimant_name"]:
    if col in df_subset.columns:
        df_subset.drop(columns=[col], inplace=True)

# Build the JSON file path in the script directory
json_path = os.path.join(script_dir, "claim_data.json")

# Save the dataframe subset as a JSON file with pretty indentation
df_subset.to_json(json_path, orient="records", indent=2)

print(f"Claim data saved to {json_path}")
