import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = "traffic_study"
NEIGHBORHOODS_COLLECTION = "network_neighborhoods"
NODES_COLLECTION = "network_nodes"
EDGES_COLLECTION = "network_edges"
NODES_TOPOLOGIC_COLLECTION = "network_nodes_topologic"
EDGES_TOPOLOGIC_COLLECTION = "network_edges_topologic"
CROSS_REF_COLLECTION = "cross_references"
DEMOGRAPHICS_COLLECTION = "neighborhood_demographics"
