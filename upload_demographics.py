import json
import logging
from datetime import datetime, timezone

import certifi
from pymongo import MongoClient

from config import DATABASE_NAME, DEMOGRAPHICS_COLLECTION, MONGODB_URI

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

# Data sources:
# - Population (2022), household size, age distribution, car ownership (HH):
#     CBS Census 2022, "Selected Data by Localities and Statistical Areas"
#     https://www.cbs.gov.il/he/publications/census2022pub/מפקד-2022.xlsx
# - Socioeconomic cluster, income, education, employment, cars/100 residents:
#     CBS Socioeconomic Index 2019, Publication 1903, Table 12
#     https://www.cbs.gov.il/he/publications/doclib/2023/socio_eco19_1903/t12.pdf
# - Population (2019): CBS Socioeconomic Index 2019, Table 12
# - Historical: Hebrew Wikipedia "שכונות באר שבע"
#
# Notes:
# - Shchuna He and Ramot Bet stat areas were renumbered in Census 2022;
#   Census data unavailable for those neighborhoods, using 2019 data only.
# - Neve Ze'ev Census 2022 data covers areas 411-414 only (area 415 renumbered).
# - Population density computed from population / area_km2 (from network_neighborhoods).
# - "cars_per_100_residents" = cars per 100 residents aged 17+ (CBS definition).
# - "employment_rate" = % aged 25-54 with income from work (CBS definition).

DEMOGRAPHICS_DATA = {
    "old_city": {
        "demographics": {
            "population": 11610,
            "area_km2": 1.0594,
            "population_density_per_km2": 10958,
            "pct_adults_18_plus": 73.0,
            "avg_household_size": 2.6,
        },
        "socioeconomic": {
            "socioeconomic_cluster": 6,
            "avg_income_per_capita": 6949,
            "pct_academic_degree": 33.8,
        },
        "transportation": {
            "cars_per_100_residents": 48.2,
            "pct_households_0_cars": 28.8,
            "pct_households_2_plus_cars": 19.7,
        },
        "public_transit": {
            "bus_stops_per_km2": None,
            "bus_lines_count": None,
            "pct_using_public_transit": None,
        },
        "employment": {
            "employment_rate": 87.5,
            "pct_working_outside_neighborhood": None,
        },
        "urban_planning": {
            "housing_density_per_km2": None,
            "pct_apartments": None,
            "avg_building_floors": None,
        },
        "historical": {
            "year_established": 1907,
            "year_populated": 1907,
        },
    },
    "shchuna_bet": {
        "demographics": {
            "population": 9560,
            "area_km2": 0.7638,
            "population_density_per_km2": 12516,
            "pct_adults_18_plus": 83.0,
            "avg_household_size": 2.1,
        },
        "socioeconomic": {
            "socioeconomic_cluster": 6,
            "avg_income_per_capita": 6916,
            "pct_academic_degree": 38.4,
        },
        "transportation": {
            "cars_per_100_residents": 44.0,
            "pct_households_0_cars": 30.6,
            "pct_households_2_plus_cars": 18.2,
        },
        "public_transit": {
            "bus_stops_per_km2": None,
            "bus_lines_count": None,
            "pct_using_public_transit": None,
        },
        "employment": {
            "employment_rate": 85.7,
            "pct_working_outside_neighborhood": None,
        },
        "urban_planning": {
            "housing_density_per_km2": None,
            "pct_apartments": None,
            "avg_building_floors": None,
        },
        "historical": {
            "year_established": 1950,
            "year_populated": 1953,
        },
    },
    "shchuna_he": {
        "demographics": {
            "population": 8343,
            "area_km2": 1.2017,
            "population_density_per_km2": 6943,
            "pct_adults_18_plus": None,
            "avg_household_size": None,
        },
        "socioeconomic": {
            "socioeconomic_cluster": 4,
            "avg_income_per_capita": 5369,
            "pct_academic_degree": 36.5,
        },
        "transportation": {
            "cars_per_100_residents": 34.9,
            "pct_households_0_cars": None,
            "pct_households_2_plus_cars": None,
        },
        "public_transit": {
            "bus_stops_per_km2": None,
            "bus_lines_count": None,
            "pct_using_public_transit": None,
        },
        "employment": {
            "employment_rate": 82.1,
            "pct_working_outside_neighborhood": None,
        },
        "urban_planning": {
            "housing_density_per_km2": None,
            "pct_apartments": None,
            "avg_building_floors": None,
        },
        "historical": {
            "year_established": 1968,
            "year_populated": 1968,
        },
    },
    "ramot_bet": {
        "demographics": {
            "population": 18368,
            "area_km2": 1.5242,
            "population_density_per_km2": 12050,
            "pct_adults_18_plus": None,
            "avg_household_size": None,
        },
        "socioeconomic": {
            "socioeconomic_cluster": 7,
            "avg_income_per_capita": 6878,
            "pct_academic_degree": 44.2,
        },
        "transportation": {
            "cars_per_100_residents": 58.9,
            "pct_households_0_cars": None,
            "pct_households_2_plus_cars": None,
        },
        "public_transit": {
            "bus_stops_per_km2": None,
            "bus_lines_count": None,
            "pct_using_public_transit": None,
        },
        "employment": {
            "employment_rate": 89.6,
            "pct_working_outside_neighborhood": None,
        },
        "urban_planning": {
            "housing_density_per_km2": None,
            "pct_apartments": None,
            "avg_building_floors": None,
        },
        "historical": {
            "year_established": 1985,
            "year_populated": 1985,
        },
    },
    "neve_zeev": {
        "demographics": {
            "population": 30410,
            "area_km2": 1.5125,
            "population_density_per_km2": 20106,
            "pct_adults_18_plus": 72.0,
            "avg_household_size": 2.5,
        },
        "socioeconomic": {
            "socioeconomic_cluster": 6,
            "avg_income_per_capita": 6593,
            "pct_academic_degree": 32.3,
        },
        "transportation": {
            "cars_per_100_residents": 48.7,
            "pct_households_0_cars": 44.0,
            "pct_households_2_plus_cars": 12.4,
        },
        "public_transit": {
            "bus_stops_per_km2": None,
            "bus_lines_count": None,
            "pct_using_public_transit": None,
        },
        "employment": {
            "employment_rate": 88.5,
            "pct_working_outside_neighborhood": None,
        },
        "urban_planning": {
            "housing_density_per_km2": None,
            "pct_apartments": None,
            "avg_building_floors": None,
        },
        "historical": {
            "year_established": 1992,
            "year_populated": 1992,
        },
    },
    "rambam": {
        "demographics": {
            "population": 7390,
            "area_km2": 0.3531,
            "population_density_per_km2": 20929,
            "pct_adults_18_plus": 76.7,
            "avg_household_size": 2.1,
        },
        "socioeconomic": {
            "socioeconomic_cluster": 9,
            "avg_income_per_capita": 10494,
            "pct_academic_degree": 63.7,
        },
        "transportation": {
            "cars_per_100_residents": 54.9,
            "pct_households_0_cars": 26.2,
            "pct_households_2_plus_cars": 23.8,
        },
        "public_transit": {
            "bus_stops_per_km2": None,
            "bus_lines_count": None,
            "pct_using_public_transit": None,
        },
        "employment": {
            "employment_rate": 90.0,
            "pct_working_outside_neighborhood": None,
        },
        "urban_planning": {
            "housing_density_per_km2": None,
            "pct_apartments": None,
            "avg_building_floors": None,
        },
        "historical": {
            "year_established": 1933,
            "year_populated": 1933,
        },
    },
}


def build_demographic_doc(neighborhood_key, neighborhood_config, city_key, data):
    return {
        "neighborhood_key": neighborhood_key,
        "city_key": city_key,
        "name_he": neighborhood_config["name_he"],
        "name_en": neighborhood_config["name_en"],
        "stat_areas": neighborhood_config["stat_areas"],
        **data,
        "data_sources": {
            "primary": "CBS (Israel Central Bureau of Statistics)",
            "publications": [
                "CBS Census 2022 — Selected Data by Localities and Statistical Areas",
                "CBS Socioeconomic Index 2019, Publication 1903, Table 12",
            ],
            "historical_source": "Hebrew Wikipedia — Neighborhoods of Beersheba",
            "stat_areas": neighborhood_config["stat_areas"],
            "notes": "Values aggregated across statistical areas using population-weighted averages. "
                     "Census 2022 data unavailable for Shchuna He and Ramot Bet (stat areas renumbered).",
        },
        "uploaded_at": datetime.now(timezone.utc),
    }


def main():
    with open("neighborhoods.json", "r") as config_file:
        config = json.load(config_file)

    client = MongoClient(MONGODB_URI, tlsCAFile=certifi.where())
    db = client[DATABASE_NAME]

    db[DEMOGRAPHICS_COLLECTION].create_index("neighborhood_key", unique=True)
    logger.info("Index created on neighborhood_demographics.neighborhood_key")

    for city_key, city_data in config["cities"].items():
        for raw_key, neighborhood_config in city_data["neighborhoods"].items():
            neighborhood_key = NEIGHBORHOOD_KEY_OVERRIDES.get(raw_key, raw_key)
            data = DEMOGRAPHICS_DATA.get(neighborhood_key)
            if not data:
                logger.warning(f"No demographic data for {neighborhood_key}, skipping.")
                continue

            doc = build_demographic_doc(neighborhood_key, neighborhood_config, city_key, data)
            db[DEMOGRAPHICS_COLLECTION].update_one(
                {"neighborhood_key": neighborhood_key},
                {"$set": doc},
                upsert=True,
            )
            logger.info(f"Upserted demographics for {neighborhood_config['name_en']} ({neighborhood_key})")

    logger.info("All demographic data uploaded.")
    client.close()


if __name__ == "__main__":
    main()
