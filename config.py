import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE_NAME = "traffic_study"
NEIGHBORHOODS_COLLECTION = "network_neighborhoods"
NODES_COLLECTION = "network_nodes"
CROSS_REF_COLLECTION = "cross_references"
