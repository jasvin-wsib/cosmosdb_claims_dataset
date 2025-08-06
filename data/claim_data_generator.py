import pandas as pd
import os
from faker import Faker
import random

# Initialize Faker to generate fake names and dates
fake = Faker()

# Get the directory path where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Build full path to the CSV file assumed to be in the same directory
csv_path = os.path.join(script_dir, "original_sample_data.csv")

# Read the CSV data into a DataFrame
df = pd.read_csv(csv_path)

# Take the first 100 rows as a subset to work with
df_subset = df.head(100).copy()

# If the "age" column exists, drop it from this subset
if "age" in df_subset.columns:
    df_subset.drop(columns=["age"], inplace=True)

# Generate mostly unique claimant names with a few duplicates
num_duplicates = 5
unique_claimants = [fake.name() for _ in range(len(df_subset) - num_duplicates)]
duplicates = random.sample(unique_claimants, num_duplicates)
all_claimants = unique_claimants + duplicates
random.shuffle(all_claimants)
df_subset["claimant_name"] = all_claimants  # new snake_case column

# Create a mapping of each unique claimant name to a unique integer ID starting from 1
claimant_id_map = {name: idx+1 for idx, name in enumerate(df_subset["claimant_name"].unique())}
df_subset["claimant_id"] = df_subset["claimant_name"].map(claimant_id_map)

# Generate a list of 25 unique fake agent names
unique_agents = [fake.name() for _ in range(25)]

# Randomly assign one of these agents as the assigned agent for each row
df_subset["assigned_agent"] = [random.choice(unique_agents) for _ in range(len(df_subset))]
assigned_agent_id_map = {name: idx+1 for idx, name in enumerate(df_subset["assigned_agent"].unique())}
df_subset["assigned_agent_id"] = df_subset["assigned_agent"].map(assigned_agent_id_map)

# Randomly assign one of these agents as the close agent for each row
df_subset["close_agent"] = [random.choice(unique_agents) for _ in range(len(df_subset))]
close_agent_id_map = {name: idx+1 for idx, name in enumerate(df_subset["close_agent"].unique())}
df_subset["close_agent_id"] = df_subset["close_agent"].map(close_agent_id_map)

# Add a filed_on column with fake dates within the last 5 years
df_subset["filed_on"] = [fake.date_between(start_date="-5y", end_date="today").isoformat() for _ in range(len(df_subset))]

# Rename accident types to more descriptive names (assumes column already exists)
df_subset["accident_type"] = df_subset["accident_type"].replace({
    "home": "slip and fall",
    "car": "repetitive strain injury",
    "workplace": "overexertion"
})

# Drop old column names if they exist
for col in ["Claimant Name", "Claimant ID", "Assigned Agent", "Assigned Agent ID", "Close Agent", "Close Agent ID", "Filed On"]:
    if col in df_subset.columns:
        df_subset.drop(columns=[col], inplace=True)

# Build the JSON file path in the script directory
json_path = os.path.join(script_dir, "claim_data.json")

# Save the dataframe subset as a JSON file with pretty indentation
df_subset.to_json(json_path, orient="records", indent=2)
