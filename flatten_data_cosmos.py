#!/usr/bin/env python3

import json
import os
import sys
from dotenv import load_dotenv
from gremlin_python.driver import client, serializer
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import PartitionStrategy
from gremlin_python.process.anonymous_traversal import traversal

# Load environment variables
load_dotenv()

# --- Cosmos DB config from .env ---
HOSTNAME = os.getenv("AZURE_COSMOS_HOSTNAME")
PORT = int(os.getenv("AZURE_COSMOS_PORT", 443))
USERNAME = os.getenv("AZURE_COSMOS_USERNAME")
PASSWORD = os.getenv("AZURE_COSMOS_PASSWORD")
PARTITION_KEY = os.getenv("AZURE_COSMOS_PARTITION_KEY", "pk")

def connect_to_gremlin():
    """
    Establishes a connection to the Gremlin server and returns the client.
    """
    gremlin_client = client.Client(
        f"wss://{HOSTNAME}:{PORT}/",
        "g",
        username=USERNAME,
        password=PASSWORD,
        message_serializer=serializer.GraphSONSerializersV2d0()
    )
    
    print("Connection to Cosmos DB successful.")
    return gremlin_client

def get_flattened_claim_data(gremlin_client, claim_id):
    """
    Fetches a specific claim and its related claimant and agent data in a flattened structure
    by submitting a raw Gremlin query string.

    Args:
        gremlin_client: The Gremlin client object.
        claim_id (str): The ID of the claim to query.

    Returns:
        list: A list containing the projected claim data, or an empty list if not found.
    """
    print(f"\nQuerying for claim: {claim_id}...")
    
    try:
        # This query is structured for maximum compatibility with Cosmos DB's Gremlin API.
        # It replaces elementMap() with a nested project() to explicitly fetch id, label, and properties.
        # It also uses coalesce() on all traversals to prevent errors if a related vertex is missing.
        query_string = f"""
        g.V().has('claim', 'claim_id', '{claim_id}').as('claim').
          project('claim', 'claimant', 'assigned_agent', 'close_agent').
            by(
                select('claim').project('id', 'label', 'properties')
                    .by(id())
                    .by(label())
                    .by(valueMap())
            ).
            by(
                coalesce(
                    select('claim').in('filed').project('id', 'label', 'properties')
                        .by(id())
                        .by(label())
                        .by(valueMap()),
                    constant('Not Found')
                )
            ).
            by(
                coalesce(
                    select('claim').out('assigned_to').project('id', 'label', 'properties')
                        .by(id())
                        .by(label())
                        .by(valueMap()),
                    constant('Not Found')
                )
            ).
            by(
                coalesce(
                    select('claim').out('closed_by').project('id', 'label', 'properties')
                        .by(id())
                        .by(label())
                        .by(valueMap()),
                    constant('Claim Not Closed')
                )
            )
        """
        
        # Submit the query string to the server
        result_set = gremlin_client.submit(query_string)
        
        # Wait for all results to be returned and convert the result set to a list
        claim_data = result_set.all().result()

        if claim_data:
            print(f"Found data for claim {claim_id}.")
        else:
            print(f"No data found for claim {claim_id}.")
            
        return claim_data
        
    except Exception as e:
        print(f"An error occurred during the query: {e}", file=sys.stderr)
        return None

def main():
    """
    Main function to connect, query for a specific claim, and print the result.
    """
    if not all([HOSTNAME, USERNAME, PASSWORD]):
        print("Error: Missing required environment variables (HOSTNAME, USERNAME, PASSWORD).", file=sys.stderr)
        sys.exit(1)
        
    gremlin_client = None
    try:
        # Establish connection
        gremlin_client = connect_to_gremlin()

        # --- Example Usage ---
        # Specify the ID of the claim you want to fetch
        target_claim_id = "C0001" 
        
        # Get the flattened data for the specified claim
        flattened_data = get_flattened_claim_data(gremlin_client, target_claim_id)

        # Print the results in a readable format
        if flattened_data:
            print("\n--- Query Result ---")
            print(json.dumps(flattened_data, indent=2))
            print("--------------------\n")

    except Exception as e:
        print(f"\nAn unexpected error occurred in main: {e}", file=sys.stderr)
    
    finally:
        # Ensure the client connection is closed
        if gremlin_client:
            gremlin_client.close()
            print("\nConnection closed.")

if __name__ == "__main__":
    main()
