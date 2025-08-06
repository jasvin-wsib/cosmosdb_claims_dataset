# Development Dataset

This project contains scripts for creating datasets for development and then importing datapoints and edges into a Gremlin Server.

## data

This directory contains a '.csv' file for a claims dataset found on Kaggle. It also contains Python scripts for generating new data and JSON files to simulate how files would be worked with in Cosmos DB.

1. Run 'run_generators.py' to create JSON files with dummy data for claims, claimants and agents.
2. Run 'json_to_files.py' to split the claims, claimants and agents JSON files into multiple, single-entry files in their respective directories.

### Generators

The generator files use the 'original_sample_data.csv' file to generate new dummy data for use in the Gremlin server. Dummy data was generated using the Faker library.

## Creating Vertices

This script reads JSON files from specified folders, each representing claims, claimants, or agents. For each JSON object, it connects to a Gremlin graph database and either finds or creates a vertex with a unique identifier. It sets properties on these vertices consistently, ensuring no duplicates. The process repeats for all files, building a graph of independent entities in the database.

Run create_vertices.py to add vertices to the graph.

## Creating Edges

This code connects to a Gremlin graph database to link claims with related entities. For each claim, it finds the corresponding claimant and agents (assigned and closing). It checks if the relationship edges exist, and if not, creates them. This builds connections between claim vertices and their associated claimant and agent vertices in the graph.

claimant -> filed -> claim
claim -> assigned_to -> assign_agent
claim -> closed_by -> close_agent

Run create_edges.py to add edges to the graph.