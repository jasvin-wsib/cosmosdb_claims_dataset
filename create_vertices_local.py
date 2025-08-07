#!/usr/bin/env python3
"""
create_vertices.py

Usage:
    (venv) $ python create_vertices.py

Expect directories next to this script:
    ./data/claim_data        <-- contains claim_<id>.json files (one JSON object per file)
    ./data/claimant_data     <-- contains claimant_<id>.json files
    ./data/agent_data        <-- contains agent_<id>.json files
"""
import json
import os
import glob
import sys
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import Cardinality

# ---- Config ----
GREMLIN_WS = "ws://localhost:8182/gremlin"   # Gremlin server websocket URL; change if server location differs
# ----------------

def connect_to_gremlin_server(ws_url=GREMLIN_WS):
    """
    Connect to Gremlin server and return traversal source and connection.
    This sets up communication with the graph database.
    """
    graph = Graph()
    connection = DriverRemoteConnection(ws_url, 'g')
    g = graph.traversal().withRemote(connection)
    return g, connection

def add_vertex(g, label="vertex", unique_key=None, **properties):
    """
    Find existing or create new vertex identified by (label, unique_key).
    Ensures unique vertices by this key, sets properties with single cardinality.
    Converts IDs and unique_key values to strings for consistency.
    Returns the vertex added or found.
    """
    if unique_key is None or unique_key not in properties:
        raise ValueError("You must provide unique_key and it must exist in properties")

    # Convert unique key value to string to avoid mismatches on repeated runs
    unique_val = str(properties[unique_key])

    # Find vertex by label and unique key or create it if not found
    v = (
        g.V().has(label, unique_key, unique_val)
        .fold()
        .coalesce(
            __.unfold(),
            __.addV(label).property(unique_key, unique_val)
        )
    )

    # Set all other properties on vertex, converting complex types to JSON strings
    for k, val in properties.items():
        if k == unique_key:
            continue
        if isinstance(val, (list, dict)):
            try:
                prop_val = json.dumps(val, ensure_ascii=False)
            except Exception:
                prop_val = str(val)
        else:
            # Convert properties ending with '_id' to string for uniformity
            if k.endswith('_id'):
                prop_val = str(val)
            else:
                prop_val = val
        v = v.property(Cardinality.single, k, prop_val)

    # Return the created or existing vertex
    return v.next()

def load_vertices_from_dir(directory, g, label, unique_key, file_pattern="*.json"):
    """
    Load all JSON files from given directory.
    Each file should contain one JSON object or a list of objects.
    For each object, add a vertex with the given label and unique key.
    Returns count of vertices processed.
    """
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
        # Skip hidden or non-JSON files
        if fname.startswith(".") or not fname.lower().endswith(".json"):
            continue
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                obj = json.load(f)

            # Support both list of objects or single object JSON files
            if isinstance(obj, list):
                for item in obj:
                    add_vertex(g, label=label, unique_key=unique_key, **item)
                    count += 1
            elif isinstance(obj, dict):
                add_vertex(g, label=label, unique_key=unique_key, **obj)
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
    # Determine absolute path to directories relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Define expected data directories
    claims_dir = os.path.join(script_dir, "data/claim_data")
    claimants_dir = os.path.join(script_dir, "data/claimant_data")
    agents_dir = os.path.join(script_dir, "data/agent_data")

    g, connection = None, None
    try:
        # Connect to Gremlin server once
        g, connection = connect_to_gremlin_server()

        # Load claim vertices from claim JSON files
        total = 0
        total += load_vertices_from_dir(claims_dir, g, label="claim", unique_key="claim_id")

        # Load claimant vertices from claimant JSON files
        total += load_vertices_from_dir(claimants_dir, g, label="claimant", unique_key="claimant_name")

        # Load agent vertices from agent JSON files
        total += load_vertices_from_dir(agents_dir, g, label="agent", unique_key="agent_id")

        print(f"[SUMMARY] Total vertices processed: {total}")
    except Exception as e:
        print(f"[FATAL] Exception during run: {e}")
        raise
    finally:
        # Always close connection on exit
        if connection:
            try:
                connection.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()
