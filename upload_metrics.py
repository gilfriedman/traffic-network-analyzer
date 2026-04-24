import json
import logging
from datetime import datetime, timezone

import certifi
from pymongo import MongoClient
from shapely.geometry import shape

from config import (
    DATABASE_NAME,
    EDGES_COLLECTION,
    EDGES_TOPOLOGIC_COLLECTION,
    MONGODB_URI,
    NEIGHBORHOODS_COLLECTION,
    NODES_COLLECTION,
    NODES_TOPOLOGIC_COLLECTION,
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
from topologic_graph import build_topologic_graph

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


def compute_representation(graph, polygon, area_km2):
    basic_stats = calculate_basic_stats(graph, area_km2)
    per_node_centrality = calculate_centrality(graph)
    centrality_summary = summarize_centrality(per_node_centrality)
    connectivity = calculate_connectivity(graph)
    node_classes = classify_nodes(graph, polygon)
    exits = find_exit_edges(graph, node_classes)
    exit_details = [
        {
            "street_name": edge["street_name"],
            "from_coords": list(edge["from_coords"]),
            "to_coords": list(edge["to_coords"]),
        }
        for edge in exits
    ]
    return {
        "basic_stats": basic_stats,
        "connectivity": connectivity,
        "centrality_summary": centrality_summary,
        "per_node_centrality": per_node_centrality,
        "node_classes": node_classes,
        "exits": exits,
        "exit_details": exit_details,
    }


def build_representation_summary(representation_result):
    return {
        "basic_stats": representation_result["basic_stats"],
        "connectivity": representation_result["connectivity"],
        "centrality_summary": representation_result["centrality_summary"],
        "exit_count": len(representation_result["exits"]),
        "exits": representation_result["exit_details"],
    }


def build_neighborhood_doc(neighborhood_key, city_key, neighborhood_config, area_km2, boundary,
                           geometric, topologic):
    return {
        "neighborhood_key": neighborhood_key,
        "city_key": city_key,
        "name_he": neighborhood_config["name_he"],
        "name_en": neighborhood_config["name_en"],
        "area_km2": round(area_km2, 4),
        "boundary": boundary,
        "computed_at": datetime.now(timezone.utc),
        "geometric": build_representation_summary(geometric),
        "topologic": build_representation_summary(topologic),
    }


def build_node_docs(graph, neighborhood_key, city_key, representation_result, computed_at):
    per_node_centrality = representation_result["per_node_centrality"]
    node_classes = representation_result["node_classes"]
    exit_node_ids = {exit_edge["from_node"] for exit_edge in representation_result["exits"]}

    docs = []
    for node_id in graph.nodes:
        node_data = graph.nodes[node_id]
        lat, lng = node_data["y"], node_data["x"]
        centrality = per_node_centrality[node_id]
        docs.append({
            "osm_node_id": _stringify_node_id(node_id),
            "neighborhood_key": neighborhood_key,
            "city_key": city_key,
            "lat": lat,
            "lng": lng,
            "location": {"type": "Point", "coordinates": [lng, lat]},
            "degree": graph.degree(node_id),
            "betweenness_centrality": centrality["betweenness"],
            "closeness_centrality": centrality["closeness"],
            "is_exit_node": node_id in exit_node_ids,
            "classification": node_classes.get(node_id, "exterior"),
            "computed_at": computed_at,
        })
    return docs


def build_edge_docs(graph, neighborhood_key, city_key, node_classes, computed_at):
    docs = []
    for node_u, node_v in graph.edges():
        u_data, v_data = graph.nodes[node_u], graph.nodes[node_v]
        is_exit = node_classes.get(node_u) == "interior" and node_classes.get(node_v) == "perimeter"
        docs.append({
            "neighborhood_key": neighborhood_key,
            "city_key": city_key,
            "from_lat": u_data["y"],
            "from_lng": u_data["x"],
            "to_lat": v_data["y"],
            "to_lng": v_data["x"],
            "is_exit_edge": is_exit,
            "computed_at": computed_at,
        })
    return docs


def _stringify_node_id(node_id):
    if isinstance(node_id, (list, tuple)):
        return "_".join(str(part) for part in node_id)
    return node_id


def upload_nodes_and_edges(db, graph, neighborhood_key, city_key, representation_result,
                           computed_at, nodes_collection, edges_collection):
    node_docs = build_node_docs(graph, neighborhood_key, city_key, representation_result, computed_at)
    db[nodes_collection].delete_many({"neighborhood_key": neighborhood_key})
    if node_docs:
        db[nodes_collection].insert_many(node_docs)
    logger.info(f"    {len(node_docs)} nodes inserted into {nodes_collection}.")

    edge_docs = build_edge_docs(graph, neighborhood_key, city_key,
                                representation_result["node_classes"], computed_at)
    db[edges_collection].delete_many({"neighborhood_key": neighborhood_key})
    if edge_docs:
        db[edges_collection].insert_many(edge_docs)
    logger.info(f"    {len(edge_docs)} edges inserted into {edges_collection}.")


def create_indexes(db):
    for nodes_collection in (NODES_COLLECTION, NODES_TOPOLOGIC_COLLECTION):
        db[nodes_collection].create_index([("location", "2dsphere")])
        db[nodes_collection].create_index("neighborhood_key")
    for edges_collection in (EDGES_COLLECTION, EDGES_TOPOLOGIC_COLLECTION):
        db[edges_collection].create_index("neighborhood_key")
    db[NEIGHBORHOODS_COLLECTION].create_index("neighborhood_key", unique=True)
    logger.info("Indexes created.")


def process_neighborhood(db, neighborhood_key_raw, city_key, neighborhood_config):
    neighborhood_key = resolve_neighborhood_key(neighborhood_key_raw)
    logger.info(f"Processing: {neighborhood_config['name_en']} ({neighborhood_key})...")

    geometric_graph = load_neighborhood_graph(neighborhood_config)
    logger.info(f"  Geometric: {len(geometric_graph.nodes)} nodes, {len(geometric_graph.edges)} edges")

    topologic_graph = build_topologic_graph(geometric_graph)

    area_km2 = calculate_polygon_area_km2(neighborhood_config)
    polygon = shape(neighborhood_config["boundary"])

    logger.info("  Computing geometric representation...")
    geometric_result = compute_representation(geometric_graph, polygon, area_km2)
    logger.info(f"    Exits: {len(geometric_result['exits'])}")

    logger.info("  Computing topologic representation...")
    topologic_result = compute_representation(topologic_graph, polygon, area_km2)
    logger.info(f"    Exits: {len(topologic_result['exits'])}")

    computed_at = datetime.now(timezone.utc)

    neighborhood_doc = build_neighborhood_doc(
        neighborhood_key, city_key, neighborhood_config, area_km2,
        neighborhood_config["boundary"]["coordinates"][0],
        geometric_result, topologic_result,
    )
    legacy_fields_to_unset = {
        "basic_stats": "",
        "connectivity": "",
        "centrality_summary": "",
        "exit_count": "",
        "exits": "",
        "topologic_stats": "",
        "topologic_connectivity": "",
        "topologic_centrality_summary": "",
        "topologic_exit_count": "",
        "topologic_exits": "",
    }
    db[NEIGHBORHOODS_COLLECTION].update_one(
        {"neighborhood_key": neighborhood_key},
        {"$set": neighborhood_doc, "$unset": legacy_fields_to_unset},
        upsert=True,
    )
    logger.info("  Neighborhood doc upserted.")

    upload_nodes_and_edges(
        db, geometric_graph, neighborhood_key, city_key, geometric_result, computed_at,
        NODES_COLLECTION, EDGES_COLLECTION,
    )
    upload_nodes_and_edges(
        db, topologic_graph, neighborhood_key, city_key, topologic_result, computed_at,
        NODES_TOPOLOGIC_COLLECTION, EDGES_TOPOLOGIC_COLLECTION,
    )


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
