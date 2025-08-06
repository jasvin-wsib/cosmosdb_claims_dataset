#!/usr/bin/env python3
"""
create_vertices.py

Usage:
    (venv) $ python create_vertices.py

Expect directories next to this script:
    ./claim_data        <-- contains claim_<id>.json files (one JSON object per file)
    ./claimant_data     <-- contains claimant_<id>.json files
    ./agent_data        <-- contains agent_<id>.json files
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
GREMLIN_WS = "ws://localhost:8182/gremlin"   # change if your server is elsewhere
# ----------------

def connect_to_gremlin_server(ws_url=GREMLIN_WS):
    graph = Graph()
    connection = DriverRemoteConnection(ws_url, 'g')
    g = graph.traversal().withRemote(connection)
    return g, connection

def add_vertex(g, label="vertex", unique_key=None, **properties):
    """
    Find-or-create a vertex by (label, unique_key) and set properties (Cardinality.single).
    Coerces the unique_key value to string to avoid mismatched types between runs.
    Also coerces any property ending with '_id' to string for consistency.
    Returns the added/updated vertex (server vertex object).
    """
    if unique_key is None or unique_key not in properties:
        raise ValueError("You must provide unique_key and it must exist in properties")
    # Coerce unique_key value to string for consistency
    unique_val = str(properties[unique_key])
    v = (
        g.V().has(label, unique_key, unique_val)
        .fold()
        .coalesce(
            __.unfold(),
            __.addV(label).property(unique_key, unique_val)
        )
    )
    for k, val in properties.items():
        if k == unique_key:
            continue
        if isinstance(val, (list, dict)):
            try:
                prop_val = json.dumps(val, ensure_ascii=False)
            except Exception:
                prop_val = str(val)
        else:
            if k.endswith('_id'):
                prop_val = str(val)
            else:
                prop_val = val
        v = v.property(Cardinality.single, k, prop_val)
    return v.next()

def load_vertices_from_dir(directory, g, label, unique_key, file_pattern="*.json"):
    """
    Read each JSON file in `directory` matching file_pattern. Each file should
    contain a single JSON object (a dict). Call add_vertex for each object.
    Returns number of records processed.
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
        if fname.startswith(".") or not fname.lower().endswith(".json"):
            continue
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                obj = json.load(f)

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
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Directories expected next to this script
    claims_dir = os.path.join(script_dir, "data/claim_data")
    claimants_dir = os.path.join(script_dir, "data/claimant_data")
    agents_dir = os.path.join(script_dir, "data/agent_data")

    g, connection = None, None
    try:
        g, connection = connect_to_gremlin_server()
        total = 0
        total += load_vertices_from_dir(claims_dir, g, label="claim", unique_key="claim_id")
        total += load_vertices_from_dir(claimants_dir, g, label="claimant", unique_key="claimant_name")
        total += load_vertices_from_dir(agents_dir, g, label="agent", unique_key="agent_id")
        print(f"[SUMMARY] Total vertices processed: {total}")
    except Exception as e:
        print(f"[FATAL] Exception during run: {e}")
        raise
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()
