import json
import logging
import math
from datetime import datetime, timezone

import certifi
from pymongo import MongoClient

from config import (
    CROSS_REF_COLLECTION,
    DATABASE_NAME,
    MONGODB_URI,
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

MATCH_THRESHOLD_METERS = 200

NEIGHBORHOOD_KEY_OVERRIDES = {
    "givat_rambam": "rambam",
}


def resolve_neighborhood_key(raw_key):
    return NEIGHBORHOOD_KEY_OVERRIDES.get(raw_key, raw_key)


def haversine_meters(lat1, lon1, lat2, lon2):
    r = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def find_closest_route(exit_lat, exit_lon, routes):
    best_route = None
    best_distance = float("inf")
    for route in routes:
        for point_key in ["origin", "destination"]:
            point = route[point_key]
            distance = haversine_meters(exit_lat, exit_lon, point["lat"], point["lng"])
            if distance < best_distance:
                best_distance = distance
                best_route = route
    return best_route, best_distance


def load_exits_for_neighborhood(neighborhood_key_raw):
    exits_path = f"exits/{neighborhood_key_raw}.json"
    try:
        with open(exits_path, "r") as exits_file:
            data = json.load(exits_file)
            return data.get("exits", [])
    except FileNotFoundError:
        logger.warning(f"  No exits file found: {exits_path}")
        return []


def fetch_routes_for_neighborhood(db, neighborhood_key):
    pipeline = [
        {"$match": {"route_id": {"$regex": f"^{neighborhood_key}"}}},
        {"$group": {
            "_id": "$route_id",
            "route_name": {"$first": "$route_name"},
            "origin": {"$first": "$origin"},
            "destination": {"$first": "$destination"},
        }},
    ]
    return list(db["traffic_data"].aggregate(pipeline))


def match_exits_to_routes(db, neighborhood_key_raw, neighborhood_key):
    exits = load_exits_for_neighborhood(neighborhood_key_raw)
    if not exits:
        return []

    routes = fetch_routes_for_neighborhood(db, neighborhood_key)
    if not routes:
        logger.warning(f"  No traffic routes found for {neighborhood_key}")
        return []

    matches = []
    for exit_data in exits:
        exit_from = exit_data["from"]
        exit_to = exit_data["to"]
        exit_midpoint_lat = (exit_from["lat"] + exit_to["lat"]) / 2
        exit_midpoint_lon = (exit_from["lon"] + exit_to["lon"]) / 2

        closest_route, distance = find_closest_route(exit_midpoint_lat, exit_midpoint_lon, routes)
        if distance > MATCH_THRESHOLD_METERS:
            continue

        matches.append({
            "type": "exit_route_match",
            "neighborhood_key": neighborhood_key,
            "exit_street_name": exit_data["street_name"],
            "exit_from": {"lat": exit_from["lat"], "lng": exit_from["lon"]},
            "exit_to": {"lat": exit_to["lat"], "lng": exit_to["lon"]},
            "matched_route_id": closest_route["_id"],
            "matched_route_name": closest_route["route_name"],
            "distance_meters": round(distance, 1),
            "computed_at": datetime.now(timezone.utc),
        })

    return matches


def upload_cross_references(db, neighborhood_key, matches):
    db[CROSS_REF_COLLECTION].delete_many({"neighborhood_key": neighborhood_key, "type": "exit_route_match"})
    if matches:
        db[CROSS_REF_COLLECTION].insert_many(matches)


def main():
    with open("neighborhoods.json", "r") as config_file:
        config = json.load(config_file)

    client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
    db = client[DATABASE_NAME]

    for city_key, city_data in config["cities"].items():
        for neighborhood_key_raw, neighborhood_config in city_data["neighborhoods"].items():
            neighborhood_key = resolve_neighborhood_key(neighborhood_key_raw)
            logger.info(f"Cross-referencing: {neighborhood_config['name_en']} ({neighborhood_key})...")

            matches = match_exits_to_routes(db, neighborhood_key_raw, neighborhood_key)
            upload_cross_references(db, neighborhood_key, matches)
            logger.info(f"  {len(matches)} exit-route matches stored.")

    logger.info("Cross-referencing complete.")
    client.close()


if __name__ == "__main__":
    main()
