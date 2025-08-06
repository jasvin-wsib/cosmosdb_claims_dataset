import pandas as pd
import os
from faker import Faker

# Initialize Faker
fake = Faker()

# Get current directory (or specify path if needed)
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to the existing JSON file created earlier
input_json_path = os.path.join(script_dir, "claim_data.json")

# Load existing claim data JSON into DataFrame
df_claims = pd.read_json(input_json_path)

# Extract unique claimant names and their IDs with snake_case column names
unique_claimants = df_claims[["claimant_name", "claimant_id"]].drop_duplicates()

# Prepare list for additional claimant info
claimant_info = []

for _, row in unique_claimants.iterrows():
    claimant_info.append({
        "claimant_name": row["claimant_name"],
        "claimant_id": row["claimant_id"],
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
        "address": fake.address().replace("\n", ", "),
        "job_title": fake.job()
    })

# Convert list to DataFrame
claimant_info_df = pd.DataFrame(claimant_info)

# Path to save new JSON file with additional claimant info
output_json_path = os.path.join(script_dir, "claimant_data.json")

# Save to JSON with pretty formatting
claimant_info_df.to_json(output_json_path, orient="records", indent=2)

print(f"Additional claimant info saved to {output_json_path}")
