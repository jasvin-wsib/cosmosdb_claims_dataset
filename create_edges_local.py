from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import __  # for anonymous traversals

def connect_claimants_to_claims():
    # Connect to Gremlin server and create traversal source
    graph = Graph()
    connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    g = graph.traversal().withRemote(connection)

    try:
        # Get all claim vertices with claim_id, claimant_id, and the vertex itself
        claims = g.V().hasLabel('claim').project('claim_id', 'claimant_id', 'claim_vertex') \
                      .by('claim_id').by('claimant_id').by(__.identity()).toList()

        for c in claims:
            claim_id = c['claim_id']
            claimant_id = str(c['claimant_id'])  # Ensure claimant_id is string for consistency
            claim_vertex = c['claim_vertex']

            # Find the claimant vertex matching claimant_id
            claimant_vertices = g.V().hasLabel('claimant').has('claimant_id', claimant_id).toList()
            if not claimant_vertices:
                print(f"Claimant vertex with claimant_id {claimant_id} not found for claim {claim_id}")
                continue
            claimant_vertex = claimant_vertices[0]

            # Check if 'filed' edge from claimant to claim already exists
            edge_exists = g.V(claimant_vertex.id).outE('filed').where(__.inV().hasId(claim_vertex.id)).hasNext()

            if not edge_exists:
                # Create the 'filed' edge linking claimant to claim
                g.V(claimant_vertex.id).addE('filed').to(__.V(claim_vertex.id)).next()
                print(f"Created 'filed' edge from claimant {claimant_id} to claim {claim_id}")
            else:
                print(f"'filed' edge already exists from claimant {claimant_id} to claim {claim_id}")

    finally:
        # Always close the connection to free resources
        connection.close()

def connect_claims_to_assigned_agent():
    # Same setup: connect to Gremlin server
    graph = Graph()
    connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    g = graph.traversal().withRemote(connection)

    try:
        # Get all claims with assigned_agent_id and vertex reference
        claims = g.V().hasLabel('claim').project('claim_id', 'assigned_agent_id', 'claim_vertex') \
                  .by('claim_id').by('assigned_agent_id').by(__.identity()).toList()

        for c in claims:
            claim_id = c['claim_id']
            assigned_agent_id = c['assigned_agent_id']

            # Skip if no agent assigned
            if assigned_agent_id is None:
                print(f"Claim {claim_id} has no assigned_agent_id")
                continue

            assigned_agent_id = str(assigned_agent_id)  # Normalize to string
            claim_vertex = c['claim_vertex']

            # Find the agent vertex by agent_id
            agent_vertices = g.V().hasLabel('agent').has('agent_id', assigned_agent_id).toList()
            if not agent_vertices:
                print(f"Agent vertex with agent_id {assigned_agent_id} not found for claim {claim_id}")
                continue
            agent_vertex = agent_vertices[0]

            # Check if 'assigned_to' edge from claim to agent exists
            edge_exists = g.V(claim_vertex.id).outE('assigned_to').where(__.inV().hasId(agent_vertex.id)).hasNext()

            if not edge_exists:
                # Create 'assigned_to' edge linking claim to agent
                g.V(claim_vertex.id).addE('assigned_to').to(__.V(agent_vertex.id)).next()
                print(f"Created 'assigned_to' edge from claim {claim_id} to agent {assigned_agent_id}")
            else:
                print(f"'assigned_to' edge already exists from claim {claim_id} to agent {assigned_agent_id}")

    finally:
        connection.close()

def connect_claims_to_closing_agent():
    # Connect to Gremlin server
    graph = Graph()
    connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    g = graph.traversal().withRemote(connection)

    try:
        # Get all claims with close_agent_id and vertex reference
        claims = g.V().hasLabel('claim').project('claim_id', 'close_agent_id', 'claim_vertex') \
                  .by('claim_id').by('close_agent_id').by(__.identity()).toList()

        for c in claims:
            claim_id = c['claim_id']
            close_agent_id = c['close_agent_id']

            # Skip if no closing agent assigned
            if close_agent_id is None:
                print(f"Claim {claim_id} has no close_agent_id")
                continue

            close_agent_id = str(close_agent_id)  # Normalize to string
            claim_vertex = c['claim_vertex']

            # Find the agent vertex by agent_id
            agent_vertices = g.V().hasLabel('agent').has('agent_id', close_agent_id).toList()
            if not agent_vertices:
                print(f"Agent vertex with agent_id {close_agent_id} not found for claim {claim_id}")
                continue
            agent_vertex = agent_vertices[0]

            # Check if 'closed_by' edge from claim to agent exists
            edge_exists = g.V(claim_vertex.id).outE('closed_by').where(__.inV().hasId(agent_vertex.id)).hasNext()

            if not edge_exists:
                # Create 'closed_by' edge linking claim to closing agent
                g.V(claim_vertex.id).addE('closed_by').to(__.V(agent_vertex.id)).next()
                print(f"Created 'closed_by' edge from claim {claim_id} to agent {close_agent_id}")
            else:
                print(f"'closed_by' edge already exists from claim {claim_id} to agent {close_agent_id}")

    finally:
        connection.close()


if __name__ == '__main__':
    # Run all three connection functions sequentially
    connect_claimants_to_claims()
    connect_claims_to_assigned_agent()
    connect_claims_to_closing_agent()
