import json
import logging
import osmnx as ox
import matplotlib.pyplot as plt
from shapely.geometry import shape

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("traffic_analyzer.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Load neighborhood config
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

num_neighborhoods = len(all_neighborhoods)
cols = min(3, num_neighborhoods)
rows = (num_neighborhoods + cols - 1) // cols
fig, axes = plt.subplots(rows, cols, figsize=(6 * cols, 6 * rows), squeeze=False)
axes = axes.flatten()

for idx, (city_name, neighborhood_config) in enumerate(all_neighborhoods):
    polygon = shape(neighborhood_config["boundary"])
    buffer_meters = neighborhood_config.get("buffer_meters", 50)
    buffer_degrees = buffer_meters / 111000

    logger.info(f"Loading: {neighborhood_config['name_he']} ({city_name}), buffer={buffer_meters}m...")
    graph = ox.graph_from_polygon(polygon.buffer(buffer_degrees), network_type="drive")
    logger.info(f"  Nodes: {len(graph.nodes)}, Edges: {len(graph.edges)}")

    ax = axes[idx]
    ox.plot_graph(graph, ax=ax, show=False, close=False, node_size=5)
    ax.set_title(f"{neighborhood_config['name_he']}, {city_name}", fontsize=12)

    # Draw neighborhood boundary
    for geom in ([polygon] if polygon.geom_type == "Polygon" else polygon.geoms):
        xs, ys = geom.exterior.xy
        ax.plot(xs, ys, color="red", linewidth=2)

    # Draw arrows to show edge direction
    if config.get("display", {}).get("show_directions", True):
        for node_u, node_v, edge_data in graph.edges(data=True):
            x_u, y_u = graph.nodes[node_u]["x"], graph.nodes[node_u]["y"]
            x_v, y_v = graph.nodes[node_v]["x"], graph.nodes[node_v]["y"]
            ax.annotate("", xy=(x_v, y_v), xytext=(x_u, y_u),
                        arrowprops=dict(arrowstyle="->", color="yellow", lw=0.5))

# Hide unused subplots
for idx in range(num_neighborhoods, len(axes)):
    axes[idx].set_visible(False)

plt.tight_layout()
plt.show()
