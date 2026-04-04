import json
import logging
import osmnx as ox
import matplotlib.pyplot as plt
from shapely.geometry import shape, Point

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("traffic_analyzer.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

with open("neighborhoods.json", "r") as config_file:
    config = json.load(config_file)

PERIMETER_THRESHOLD_METERS = 25
perimeter_threshold_degrees = PERIMETER_THRESHOLD_METERS / 111000


def classify_nodes(graph, boundary_polygon):
    """Classify all graph nodes as interior, perimeter, or exterior."""
    boundary_line = boundary_polygon.boundary
    classifications = {}
    for node_id in graph.nodes:
        x, y = graph.nodes[node_id]["x"], graph.nodes[node_id]["y"]
        point = Point(x, y)
        distance_to_boundary = boundary_line.distance(point)
        if distance_to_boundary <= perimeter_threshold_degrees:
            classifications[node_id] = "perimeter"
        elif boundary_polygon.contains(point):
            classifications[node_id] = "interior"
        else:
            classifications[node_id] = "exterior"
    return classifications


def find_exit_edges(graph, node_classes):
    """Find directed edges from interior nodes to perimeter nodes."""
    exits = []
    for node_u, node_v, edge_data in graph.edges(data=True):
        if node_classes[node_u] == "interior" and node_classes[node_v] == "perimeter":
            x_u, y_u = graph.nodes[node_u]["x"], graph.nodes[node_u]["y"]
            x_v, y_v = graph.nodes[node_v]["x"], graph.nodes[node_v]["y"]
            exits.append({
                "from_node": node_u,
                "to_node": node_v,
                "from_coords": (y_u, x_u),
                "to_coords": (y_v, x_v),
                "street_name": edge_data.get("name", "unnamed"),
            })
    return exits


if __name__ == "__main__":
    # Target city
    city_key = "beer_sheva"
    city_data = config["cities"][city_key]
    neighborhoods = city_data["neighborhoods"]

    for neighborhood_key, neighborhood_config in neighborhoods.items():
        polygon = shape(neighborhood_config["boundary"])
        buffer_meters = neighborhood_config.get("buffer_meters", 50)
        buffer_degrees = buffer_meters / 111000
        buffered_polygon = polygon.buffer(buffer_degrees)

        logger.info(f"Loading: {neighborhood_config['name_he']}...")
        graph = ox.graph_from_polygon(buffered_polygon, network_type="drive")
        logger.info(f"  Nodes: {len(graph.nodes)}, Edges: {len(graph.edges)}")

        node_classes = classify_nodes(graph, polygon)
        exits = find_exit_edges(graph, node_classes)
        logger.info(f"  Exits: {len(exits)}")

        print(f"\n{'=' * 60}")
        print(f"  {neighborhood_config['name_he']} ({neighborhood_config['name_en']}) — {len(exits)} exits")
        print(f"{'=' * 60}")
        for exit_edge in exits:
            street = exit_edge["street_name"]
            from_lat, from_lon = exit_edge["from_coords"]
            to_lat, to_lon = exit_edge["to_coords"]
            print(f"  {street}")
            print(f"    from: ({from_lat:.6f}, {from_lon:.6f})")
            print(f"    to:   ({to_lat:.6f}, {to_lon:.6f})")

        fig, ax = plt.subplots(1, 1, figsize=(10, 10))
        ox.plot_graph(graph, ax=ax, show=False, close=False, node_size=0, edge_color="#444444", edge_linewidth=0.5)

        for geom in ([polygon] if polygon.geom_type == "Polygon" else polygon.geoms):
            xs, ys = geom.exterior.xy
            ax.plot(xs, ys, color="red", linewidth=2)

        for node_id, cls in node_classes.items():
            x, y = graph.nodes[node_id]["x"], graph.nodes[node_id]["y"]
            color = {"interior": "deepskyblue", "perimeter": "orange", "exterior": "gray"}[cls]
            size = 12 if cls == "perimeter" else 4
            ax.plot(x, y, "o", color=color, markersize=size, zorder=3 if cls == "perimeter" else 2)

        for exit_edge in exits:
            from_lat, from_lon = exit_edge["from_coords"]
            to_lat, to_lon = exit_edge["to_coords"]
            ax.annotate(
                "",
                xy=(to_lon, to_lat),
                xytext=(from_lon, from_lat),
                arrowprops=dict(arrowstyle="-|>", color="lime", lw=2.5),
            )

        ax.set_title(
            f"{neighborhood_config['name_he']} — {len(exits)} exits\n"
            "Blue=interior, Orange=perimeter, Gray=exterior, Green=exits",
            fontsize=12,
        )
        plt.tight_layout()
        plt.show()
