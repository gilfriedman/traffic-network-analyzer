import osmnx as ox

print("OSMnx version:", ox.__version__)

G = ox.graph_from_place(
    "Be'er Sheva, Israel",
    network_type="drive"
)

print("Nodes:", len(G.nodes))
print("Edges:", len(G.edges))