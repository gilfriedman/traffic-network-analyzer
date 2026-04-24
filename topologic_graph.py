import logging

import networkx as nx
import osmnx as ox

logger = logging.getLogger(__name__)

DEFAULT_CONSOLIDATE_TOLERANCE_METERS = 12
ROUNDABOUT_JUNCTION_VALUES = {"roundabout", "circular"}


def _is_roundabout_edge(edge_data):
    junction = edge_data.get("junction", "")
    if isinstance(junction, list):
        return any(value in ROUNDABOUT_JUNCTION_VALUES for value in junction)
    return junction in ROUNDABOUT_JUNCTION_VALUES


def _find_roundabout_components(graph):
    roundabout_subgraph = nx.Graph()
    for node_u, node_v, edge_data in graph.edges(data=True):
        if _is_roundabout_edge(edge_data):
            roundabout_subgraph.add_edge(node_u, node_v)
    return [set(component) for component in nx.connected_components(roundabout_subgraph)]


def _redirect_edges_to_canonical_node(graph, source_node, canonical_node, component_nodes):
    incoming = list(graph.in_edges(source_node, keys=True, data=True))
    for predecessor, _, _, edge_data in incoming:
        if predecessor in component_nodes:
            continue
        graph.add_edge(predecessor, canonical_node, **edge_data)
    outgoing = list(graph.out_edges(source_node, keys=True, data=True))
    for _, successor, _, edge_data in outgoing:
        if successor in component_nodes:
            continue
        graph.add_edge(canonical_node, successor, **edge_data)


def _collapse_tagged_roundabouts(graph):
    components = _find_roundabout_components(graph)
    if not components:
        return graph

    graph = graph.copy()
    merged = 0
    for component in components:
        nodes = list(component)
        if len(nodes) < 2:
            continue
        canonical_node = nodes[0]
        xs = [graph.nodes[node_id]["x"] for node_id in nodes]
        ys = [graph.nodes[node_id]["y"] for node_id in nodes]
        graph.nodes[canonical_node]["x"] = sum(xs) / len(xs)
        graph.nodes[canonical_node]["y"] = sum(ys) / len(ys)
        for non_canonical_node in nodes[1:]:
            _redirect_edges_to_canonical_node(graph, non_canonical_node, canonical_node, component)
            graph.remove_node(non_canonical_node)
        merged += 1

    logger.info(f"    Pre-collapsed {merged} OSM-tagged roundabout(s).")
    return graph


def _deduplicate_parallel_edges(graph):
    seen_pairs = set()
    edges_to_remove = []
    for node_u, node_v, key in list(graph.edges(keys=True)):
        pair = frozenset((node_u, node_v))
        if pair in seen_pairs:
            edges_to_remove.append((node_u, node_v, key))
        else:
            seen_pairs.add(pair)
    for node_u, node_v, key in edges_to_remove:
        graph.remove_edge(node_u, node_v, key=key)
    return graph


def build_topologic_graph(geometric_graph, tolerance_meters=DEFAULT_CONSOLIDATE_TOLERANCE_METERS):
    """Return a topologic graph: roundabouts collapsed, close intersections merged, parallel edges deduped."""
    graph = _collapse_tagged_roundabouts(geometric_graph)
    projected_graph = ox.project_graph(graph)
    consolidated_graph = ox.consolidate_intersections(
        projected_graph,
        tolerance=tolerance_meters,
        rebuild_graph=True,
        dead_ends=True,
        reconnect_edges=True,
    )
    topologic_graph = ox.project_graph(consolidated_graph, to_crs="EPSG:4326")
    topologic_graph = _deduplicate_parallel_edges(topologic_graph)
    logger.info(
        f"    Topologic graph: {len(topologic_graph.nodes)} nodes, {len(topologic_graph.edges)} edges "
        f"(from {len(geometric_graph.nodes)}/{len(geometric_graph.edges)} geometric)"
    )
    return topologic_graph
