import pandas as pd
import os
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to the claim data JSON
claim_data_path = os.path.join(script_dir, "claim_data.json")

# Load the claims data
df = pd.read_json(claim_data_path)

# Combine assigned_agent_id and close_agent_id to extract all unique agent IDs
all_agent_ids = pd.unique(pd.concat([df["assigned_agent_id"], df["close_agent_id"]]))

# Create a unique name for each ID using Faker
agent_info = []
used_names = set()

for agent_id in all_agent_ids:
    # Generate a unique name (ensure no duplicates)
    while True:
        name = fake.name()
        if name not in used_names:
            used_names.add(name)
            break

    email = f"{name.lower().replace(' ', '.')}@insurancecorp.com"
    phone = fake.phone_number()
    is_active = random.choice([True, False])

    agent_info.append({
        "agent_id": agent_id,
        "agent_name": name,
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
