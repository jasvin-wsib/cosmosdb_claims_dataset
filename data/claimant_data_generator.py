import pandas as pd
import os
from faker import Faker

# Initialize Faker
fake = Faker()

# Get the current directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to the existing JSON file with claims
input_json_path = os.path.join(script_dir, "claim_data.json")

# Load the claim data
df_claims = pd.read_json(input_json_path)

# Get unique claimant IDs
unique_claimant_ids = df_claims["claimant_id"].drop_duplicates().tolist()

# Build claimant info records
claimant_info = []

for claimant_id in unique_claimant_ids:
    claimant_info.append({
        "claimant_id": claimant_id,
        "claimant_name": fake.name(),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
        "address": fake.address().replace("\n", ", "),
        "job_title": fake.job()
    })

# Convert to DataFrame
claimant_info_df = pd.DataFrame(claimant_info)

# Save the claimant info to a new JSON file
output_json_path = os.path.join(script_dir, "claimant_data.json")
claimant_info_df.to_json(output_json_path, orient="records", indent=2)

print(f"Claimant info saved to {output_json_path}")
