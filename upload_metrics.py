import json
import logging
from datetime import datetime, timezone

import certifi
from pymongo import MongoClient

from config import (
    DATABASE_NAME,
    MONGODB_URI,
    NEIGHBORHOODS_COLLECTION,
    NODES_COLLECTION,
)
from find_exits import classify_nodes, find_exit_edges
from network_metrics import (
    calculate_basic_stats,
    calculate_centrality,
    calculate_connectivity,
    calculate_polygon_area_km2,
    load_neighborhood_graph,
    summarize_centrality,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("traffic_analyzer.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

NEIGHBORHOOD_KEY_OVERRIDES = {
    "givat_rambam": "rambam",
}


def resolve_neighborhood_key(raw_key):
    return NEIGHBORHOOD_KEY_OVERRIDES.get(raw_key, raw_key)


def build_neighborhood_doc(neighborhood_key, city_key, neighborhood_config, basic_stats, centrality_summary,
                           connectivity, exit_count, area_km2):
    return {
        "neighborhood_key": neighborhood_key,
        "city_key": city_key,
        "name_he": neighborhood_config["name_he"],
        "name_en": neighborhood_config["name_en"],
        "basic_stats": basic_stats,
        "connectivity": connectivity,
        "centrality_summary": centrality_summary,
        "exit_count": exit_count,
        "area_km2": round(area_km2, 4),
        "computed_at": datetime.now(timezone.utc),
    }


def build_node_docs(graph, neighborhood_key, city_key, per_node_centrality, exit_node_ids, computed_at):
    docs = []
    for node_id in graph.nodes:
        node_data = graph.nodes[node_id]
        lat, lng = node_data["y"], node_data["x"]
        centrality = per_node_centrality[node_id]
        docs.append({
            "osm_node_id": node_id,
            "neighborhood_key": neighborhood_key,
            "city_key": city_key,
            "lat": lat,
            "lng": lng,
            "location": {"type": "Point", "coordinates": [lng, lat]},
            "degree": graph.degree(node_id),
            "betweenness_centrality": centrality["betweenness"],
            "closeness_centrality": centrality["closeness"],
            "is_exit_node": node_id in exit_node_ids,
            "computed_at": computed_at,
        })
    return docs


def create_indexes(db):
    db[NODES_COLLECTION].create_index([("location", "2dsphere")])
    db[NODES_COLLECTION].create_index("neighborhood_key")
    db[NEIGHBORHOODS_COLLECTION].create_index("neighborhood_key", unique=True)
    logger.info("Indexes created.")


def process_neighborhood(db, neighborhood_key_raw, city_key, neighborhood_config):
    neighborhood_key = resolve_neighborhood_key(neighborhood_key_raw)
    logger.info(f"Processing: {neighborhood_config['name_en']} ({neighborhood_key})...")

    graph = load_neighborhood_graph(neighborhood_config)
    logger.info(f"  Graph loaded: {len(graph.nodes)} nodes, {len(graph.edges)} edges")

    area_km2 = calculate_polygon_area_km2(neighborhood_config)
    basic_stats = calculate_basic_stats(graph, area_km2)
    logger.info(f"  Basic stats computed.")

    per_node_centrality = calculate_centrality(graph)
    centrality_summary = summarize_centrality(per_node_centrality)
    logger.info(f"  Centrality computed.")

    connectivity = calculate_connectivity(graph)
    logger.info(f"  Connectivity computed.")

    from shapely.geometry import shape
    polygon = shape(neighborhood_config["boundary"])
    node_classes = classify_nodes(graph, polygon)
    exits = find_exit_edges(graph, node_classes)
    exit_node_ids = {exit_edge["from_node"] for exit_edge in exits}
    logger.info(f"  Exits detected: {len(exits)}")

    computed_at = datetime.now(timezone.utc)

    neighborhood_doc = build_neighborhood_doc(
        neighborhood_key, city_key, neighborhood_config,
        basic_stats, centrality_summary, connectivity,
        len(exits), area_km2,
    )
    db[NEIGHBORHOODS_COLLECTION].update_one(
        {"neighborhood_key": neighborhood_key},
        {"$set": neighborhood_doc},
        upsert=True,
    )
    logger.info(f"  Neighborhood doc upserted.")

    node_docs = build_node_docs(graph, neighborhood_key, city_key, per_node_centrality, exit_node_ids, computed_at)
    db[NODES_COLLECTION].delete_many({"neighborhood_key": neighborhood_key})
    if node_docs:
        db[NODES_COLLECTION].insert_many(node_docs)
    logger.info(f"  {len(node_docs)} node docs inserted.")


def main():
    with open("neighborhoods.json", "r") as config_file:
        config = json.load(config_file)

    client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
    db = client[DATABASE_NAME]
    create_indexes(db)

    for city_key, city_data in config["cities"].items():
        for neighborhood_key_raw, neighborhood_config in city_data["neighborhoods"].items():
            process_neighborhood(db, neighborhood_key_raw, city_key, neighborhood_config)

    logger.info("All neighborhoods processed.")
    client.close()


if __name__ == "__main__":
    main()
