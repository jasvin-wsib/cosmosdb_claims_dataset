from gremlin_python.driver import client, serializer
import os
from dotenv import load_dotenv
import json

# Load environment
load_dotenv()

# Cosmos DB config
HOSTNAME = os.getenv("AZURE_COSMOS_HOSTNAME")
PORT = int(os.getenv("AZURE_COSMOS_PORT", 443))
USERNAME = os.getenv("AZURE_COSMOS_USERNAME")
PASSWORD = os.getenv("AZURE_COSMOS_PASSWORD")

def connect_to_cosmos():
    return client.Client(
        f"wss://{HOSTNAME}:{PORT}/gremlin",
        "g",
        username=USERNAME,
        password=PASSWORD,
        message_serializer=serializer.GraphSONSerializersV2d0()
    )

def create_edge_if_missing(client, out_v_id, in_v_id, edge_label):
    check_query = f"g.V('{out_v_id}').outE('{edge_label}').where(inV().hasId('{in_v_id}')).limit(1)"
    try:
        results = client.submit(check_query).all().result()
        exists = len(results) > 0
    except Exception as e:
        print(f"[ERROR] Failed edge existence check: {e}")
        return

    if not exists:
        create_query = f"g.V('{out_v_id}').addE('{edge_label}').to(g.V('{in_v_id}'))"
        try:
            client.submit(create_query).all().result()
            print(f"[OK] Created '{edge_label}' edge from {out_v_id} to {in_v_id}")
        except Exception as e:
            print(f"[ERROR] Failed to create edge '{edge_label}' from {out_v_id} to {in_v_id}: {e}")
    else:
        print(f"[SKIP] '{edge_label}' edge already exists from {out_v_id} to {in_v_id}")

def connect_claimants_to_claims(client):
    query = (
        "g.V().hasLabel('claim').as('c')"
        ".project('claim_id', 'claimant_id', 'claim_vid')"
        ".by('claim_id').by('claimant_id').by(__.id())"
    )
    results = client.submit(query).all().result()

    for record in results:
        claim_id = record['claim_id']
        claimant_id = str(record['claimant_id'])
        claim_vid = record['claim_vid']

        # Get claimant vertex ID
        query_claimant = (
            f"g.V().hasLabel('claimant').has('claimant_id', '{claimant_id}').id()"
        )
        try:
            claimant_vid = client.submit(query_claimant).all().result()
        except Exception as e:
            print(f"[ERROR] Failed to find claimant {claimant_id} for claim {claim_id}: {e}")
            continue

        if not claimant_vid:
            print(f"[WARN] No claimant found with claimant_id {claimant_id} for claim {claim_id}")
            continue

        create_edge_if_missing(client, claimant_vid[0], claim_vid, 'filed')

def connect_claims_to_assigned_agent(client):
    query = (
        "g.V().hasLabel('claim').as('c')"
        ".project('claim_id', 'assigned_agent_id', 'claim_vid')"
        ".by('claim_id').by('assigned_agent_id').by(__.id())"
    )
    results = client.submit(query).all().result()

    for record in results:
        claim_id = record['claim_id']
        agent_id = record['assigned_agent_id']
        claim_vid = record['claim_vid']

        if agent_id is None:
            print(f"[SKIP] Claim {claim_id} has no assigned_agent_id")
            continue

        agent_id = str(agent_id)
        query_agent = f"g.V().hasLabel('agent').has('agent_id', '{agent_id}').id()"

        try:
            agent_vid = client.submit(query_agent).all().result()
        except Exception as e:
            print(f"[ERROR] Finding agent {agent_id} for claim {claim_id}: {e}")
            continue

        if not agent_vid:
            print(f"[WARN] No agent found with agent_id {agent_id} for claim {claim_id}")
            continue

        create_edge_if_missing(client, claim_vid, agent_vid[0], 'assigned_to')

def connect_claims_to_closing_agent(client):
    query = (
        "g.V().hasLabel('claim').as('c')"
        ".project('claim_id', 'close_agent_id', 'claim_vid')"
        ".by('claim_id').by('close_agent_id').by(__.id())"
    )
    results = client.submit(query).all().result()

    for record in results:
        claim_id = record['claim_id']
        agent_id = record['close_agent_id']
        claim_vid = record['claim_vid']

        if agent_id is None:
            print(f"[SKIP] Claim {claim_id} has no close_agent_id")
            continue

        agent_id = str(agent_id)
        query_agent = f"g.V().hasLabel('agent').has('agent_id', '{agent_id}').id()"

        try:
            agent_vid = client.submit(query_agent).all().result()
        except Exception as e:
            print(f"[ERROR] Finding closing agent {agent_id} for claim {claim_id}: {e}")
            continue

        if not agent_vid:
            print(f"[WARN] No agent found with agent_id {agent_id} for claim {claim_id}")
            continue

        create_edge_if_missing(client, claim_vid, agent_vid[0], 'closed_by')

def main():
    client_conn = None
    try:
        client_conn = connect_to_cosmos()
        connect_claimants_to_claims(client_conn)
        connect_claims_to_assigned_agent(client_conn)
        connect_claims_to_closing_agent(client_conn)
    except Exception as e:
        print(f"[FATAL] Error running edge connections: {e}")
    finally:
        if client_conn:
            client_conn.close()

if __name__ == "__main__":
    main()
