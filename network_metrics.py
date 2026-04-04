import logging

import networkx as nx
import osmnx as ox
from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform

logger = logging.getLogger(__name__)

WGS84_TO_UTM36N = Transformer.from_crs("EPSG:4326", "EPSG:32636", always_xy=True)


def load_neighborhood_graph(neighborhood_config):
    polygon = shape(neighborhood_config["boundary"])
    buffer_meters = neighborhood_config.get("buffer_meters", 50)
    buffer_degrees = buffer_meters / 111000
    return ox.graph_from_polygon(polygon.buffer(buffer_degrees), network_type="drive")


def calculate_polygon_area_km2(neighborhood_config):
    polygon = shape(neighborhood_config["boundary"])
    projected = transform(WGS84_TO_UTM36N.transform, polygon)
    return projected.area / 1_000_000


def calculate_basic_stats(graph, area_km2):
    area_m2 = area_km2 * 1_000_000
    stats = ox.stats.basic_stats(graph, area=area_m2)
    return {
        "node_count": stats["n"],
        "edge_count": stats["m"],
        "total_street_length_m": round(stats["street_length_total"], 1),
        "avg_street_length_m": round(stats["street_length_avg"], 1),
        "intersection_count": stats["intersection_count"],
        "intersection_density_per_km2": round(stats["intersection_density_km"], 1),
        "street_density_m_per_km2": round(stats["street_density_km"], 1),
        "avg_node_degree": round(stats["k_avg"], 2),
        "circuity": round(stats["circuity_avg"], 4) if stats["circuity_avg"] else None,
    }


def calculate_centrality(graph):
    betweenness = nx.betweenness_centrality(graph)
    closeness = nx.closeness_centrality(graph)
    return {
        node_id: {
            "betweenness": round(betweenness[node_id], 6),
            "closeness": round(closeness[node_id], 6),
        }
        for node_id in graph.nodes
    }


def summarize_centrality(per_node_centrality):
    betweenness_values = [node["betweenness"] for node in per_node_centrality.values()]
    closeness_values = [node["closeness"] for node in per_node_centrality.values()]
    return {
        "avg_betweenness": round(sum(betweenness_values) / len(betweenness_values), 6),
        "max_betweenness": round(max(betweenness_values), 6),
        "avg_closeness": round(sum(closeness_values) / len(closeness_values), 6),
        "max_closeness": round(max(closeness_values), 6),
    }


def calculate_connectivity(graph):
    logger.info("    Computing edge connectivity...")
    edge_conn = nx.edge_connectivity(graph)
    logger.info("    Computing average node connectivity (may take a while)...")
    avg_node_conn = nx.average_node_connectivity(graph)
    return {
        "avg_node_connectivity": round(avg_node_conn, 4),
        "edge_connectivity": edge_conn,
    }
