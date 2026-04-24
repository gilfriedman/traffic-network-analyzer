import json
import logging
import osmnx as ox
import matplotlib.pyplot as plt
from shapely.geometry import shape

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

logger.info("Loading neighborhoods config...")
with open("neighborhoods.json", "r") as config_file:
    config = json.load(config_file)

# Select which neighborhoods to plot (empty = all)
selected = []

all_neighborhoods = []
if selected:
    for city_key, neighborhood_key in selected:
        city_data = config["cities"][city_key]
        neighborhood_data = city_data["neighborhoods"][neighborhood_key]
        all_neighborhoods.append((city_data["name_he"], neighborhood_data))
else:
    for city_key, city_data in config["cities"].items():
        for neighborhood_key, neighborhood_data in city_data["neighborhoods"].items():
            all_neighborhoods.append((city_data["name_he"], neighborhood_data))


def draw_graph(ax, graph, polygon, title, show_directions):
    ox.plot_graph(graph, ax=ax, show=False, close=False, node_size=5, edge_color="#999999", edge_linewidth=0.6)
    ax.set_title(title, fontsize=12)

    for geom in ([polygon] if polygon.geom_type == "Polygon" else polygon.geoms):
        xs, ys = geom.exterior.xy
        ax.plot(xs, ys, color="red", linewidth=2)

    if show_directions:
        for node_u, node_v, _ in graph.edges(data=True):
            x_u, y_u = graph.nodes[node_u]["x"], graph.nodes[node_u]["y"]
            x_v, y_v = graph.nodes[node_v]["x"], graph.nodes[node_v]["y"]
            ax.annotate("", xy=(x_v, y_v), xytext=(x_u, y_u),
                        arrowprops=dict(arrowstyle="->", color="yellow", lw=0.5))


num_neighborhoods = len(all_neighborhoods)
show_directions = config.get("display", {}).get("show_directions", True)
fig, axes = plt.subplots(num_neighborhoods, 2, figsize=(12, 6 * num_neighborhoods), squeeze=False)

for idx, (city_name, neighborhood_config) in enumerate(all_neighborhoods):
    polygon = shape(neighborhood_config["boundary"])
    buffer_meters = neighborhood_config.get("buffer_meters", 50)
    buffer_degrees = buffer_meters / 111000

    logger.info(f"Loading: {neighborhood_config['name_he']} ({city_name}), buffer={buffer_meters}m...")
    geometric_graph = ox.graph_from_polygon(polygon.buffer(buffer_degrees), network_type="drive")
    logger.info(f"  Geometric: {len(geometric_graph.nodes)} nodes, {len(geometric_graph.edges)} edges")

    topologic_graph = build_topologic_graph(geometric_graph)

    draw_graph(
        axes[idx][0], geometric_graph, polygon,
        f"{neighborhood_config['name_he']}, {city_name} — geometric ({len(geometric_graph.nodes)}n)",
        show_directions,
    )
    draw_graph(
        axes[idx][1], topologic_graph, polygon,
        f"{neighborhood_config['name_he']}, {city_name} — topologic ({len(topologic_graph.nodes)}n)",
        show_directions,
    )

plt.tight_layout()
plt.show()
