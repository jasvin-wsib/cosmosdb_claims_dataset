import pandas as pd
import os
from faker import Faker
import random
import re

# Initialize Faker
fake = Faker()

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to the claim data JSON
claim_data_path = os.path.join(script_dir, "claim_data.json")

# Load the claims data
df = pd.read_json(claim_data_path)

# Combine assigned_agent and close_agent, and extract unique names
assigned_agents = df["assigned_agent"].unique()
close_agents = df["close_agent"].unique()
all_agents = pd.unique(pd.Series(list(assigned_agents) + list(close_agents)))

# Helper to clean and convert name to email format
def name_to_email(name):
    # Remove non-alphanumeric characters and split into parts
    parts = re.sub(r"[^a-zA-Z\s]", "", name).strip().lower().split()
    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        return f"{first}.{last}@insurancecorp.com"
    else:
        return f"{parts[0]}@insurancecorp.com"

# Create agent info records
agent_info = []
for idx, name in enumerate(all_agents, start=1):
    email = name_to_email(name)
    phone = fake.phone_number()
    is_active = random.choice([True, False])  # Randomly assign active status

    agent_info.append({
        "agent_name": name,
        "agent_id": idx,
        "email": email,
        "phone_number": phone,
        "currently_active": is_active
    })

# Convert to DataFrame
agent_info_df = pd.DataFrame(agent_info)

# Save to JSON
agent_json_path = os.path.join(script_dir, "agent_data.json")
agent_info_df.to_json(agent_json_path, orient="records", indent=2)

print(f"Agent info saved to {agent_json_path}")
