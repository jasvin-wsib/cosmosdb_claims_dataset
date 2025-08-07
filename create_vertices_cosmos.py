#!/usr/bin/env python3

import json
import os
import glob
from dotenv import load_dotenv
from gremlin_python.driver import client, serializer
from gremlin_python.process.traversal import Cardinality

# Load environment variables
load_dotenv()

# Cosmos DB config from .env
HOSTNAME = os.getenv("AZURE_COSMOS_HOSTNAME")
PORT = int(os.getenv("AZURE_COSMOS_PORT", 443))
USERNAME = os.getenv("AZURE_COSMOS_USERNAME")
PASSWORD = os.getenv("AZURE_COSMOS_PASSWORD")
PARTITION_KEY = os.getenv("AZURE_COSMOS_PARTITION_KEY", "pk")  # default assumed

# Set up Cosmos DB Gremlin client
def connect_to_cosmos():
    gremlin_client = client.Client(
        f"wss://{HOSTNAME}:{PORT}/gremlin",
        "g",
        username=USERNAME,
        password=PASSWORD,
        message_serializer=serializer.GraphSONSerializersV2d0()
    )
    return gremlin_client

def add_vertex(client, label="vertex", unique_key=None, **properties):
    if unique_key is None or unique_key not in properties:
        raise ValueError("You must provide unique_key and it must exist in properties")

    unique_val = str(properties[unique_key])
    partition_val = properties.get(PARTITION_KEY)
    if not partition_val:
        raise ValueError(f"Partition key '{PARTITION_KEY}' must be set in each vertex")

    gremlin_query = (
        f"g.V().has('{label}', '{unique_key}', '{unique_val}').fold().coalesce("
        f"unfold(), "
        f"addV('{label}').property('id', '{unique_val}')"
    )

    # Add each property
    for k, val in properties.items():
        if isinstance(val, (dict, list)):
            val = json.dumps(val)
        elif k.endswith('_id'):
            val = str(val)
        gremlin_query += f".property('{k}', {json.dumps(val)})"

    gremlin_query += ")"

    try:
        result = client.submit(gremlin_query).all().result()
        return result
    except Exception as e:
        print(f"[ERROR] Vertex '{unique_val}' insertion failed: {e}")
        return None

def load_vertices_from_dir(directory, client, label, unique_key, file_pattern="*.json"):
    directory = os.path.abspath(directory)
    if not os.path.isdir(directory):
        print(f"[WARN] Directory not found, skipping: {directory}")
        return 0

    pattern = os.path.join(directory, file_pattern)
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[INFO] No JSON files found in {directory}")
        return 0

    count = 0
    for filepath in files:
        fname = os.path.basename(filepath)
        if fname.startswith(".") or not fname.lower().endswith(".json"):
            continue
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                obj = json.load(f)

            if isinstance(obj, list):
                for item in obj:
                    if PARTITION_KEY not in item:
                        item[PARTITION_KEY] = label  # Default partition value
                    add_vertex(client, label=label, unique_key=unique_key, **item)
                    count += 1
            elif isinstance(obj, dict):
                if PARTITION_KEY not in obj:
                    obj[PARTITION_KEY] = label
                add_vertex(client, label=label, unique_key=unique_key, **obj)
                count += 1
            else:
                print(f"[SKIP] {fname}: JSON root is not object or list")
                continue

            print(f"[OK] Processed {fname}")
        except Exception as e:
            print(f"[ERROR] Processing {fname}: {e} — skipping")

    print(f"[DONE] Loaded {count} {label} vertices from {directory}")
    return count

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    claims_dir = os.path.join(script_dir, "data/claim_data")
    claimants_dir = os.path.join(script_dir, "data/claimant_data")
    agents_dir = os.path.join(script_dir, "data/agent_data")

    gremlin_client = None
    try:
        gremlin_client = connect_to_cosmos()
        total = 0
        total += load_vertices_from_dir(claims_dir, gremlin_client, label="claim", unique_key="claim_id")
        total += load_vertices_from_dir(claimants_dir, gremlin_client, label="claimant", unique_key="claimant_name")
        total += load_vertices_from_dir(agents_dir, gremlin_client, label="agent", unique_key="agent_id")
        print(f"[SUMMARY] Total vertices processed: {total}")
    except Exception as e:
        print(f"[FATAL] Exception during run: {e}")
        raise
    finally:
        if gremlin_client:
            gremlin_client.close()

if __name__ == "__main__":
    main()
