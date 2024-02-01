import random
import os
from dotenv import dotenv_values
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import signal
import sys
import time


class EcommerceDataGenerator:
    def __init__(self):
        self.config = dotenv_values(f"{os.path.dirname(os.path.realpath(__file__))}/.env")
        self.es = Elasticsearch(
            cloud_id=self.config.get("CLOUD_ID"),
            basic_auth=(self.config.get("USER"), self.config.get("PASSWORD"))
        )

        self.index = "ecommerce-events"
        self.previous_products = {}

        self.seed = "random_seed_for_ecommerce"
        random.seed(self.seed)

        self.dell_product_catalog = {
            "Laptops": {
                "Dell XPS 13": {"price_range": (999, 1600), "memory": "16GB", "cpu": "Intel Core i7",
                                "disk": "512GB SSD", "video_card": "Intel Iris Xe Graphics"},
                "Dell XPS 15": {"price_range": (1100, 2000), "memory": "32GB", "cpu": "Intel Core i9",
                                "disk": "1TB SSD", "video_card": "NVIDIA GeForce GTX 1650 Ti"},
                "Dell Inspiron 15": {"price_range": (850, 1000), "memory": "8GB", "cpu": "Intel Core i5",
                                     "disk": "256GB SSD", "video_card": "Intel UHD Graphics"},
                "Dell Inspiron 17": {"price_range": (920, 1100), "memory": "12GB", "cpu": "AMD Ryzen 7",
                                     "disk": "512GB SSD", "video_card": "AMD Radeon RX Vega 10"},
                "Dell Latitude 14": {"price_range": (700, 1400), "memory": "16GB", "cpu": "Intel Core i7",
                                     "disk": "512GB SSD", "video_card": "Intel UHD Graphics"},
                "Dell Vostro 14": {"price_range": (650, 1300), "memory": "16GB", "cpu": "Intel Core i7",
                                   "disk": "512GB SSD", "video_card": "NVIDIA GeForce MX250"}
            },
            "Accessories": {
                "Dell Wireless Mouse": {"price_range": (15, 60), "type": "Wireless", "connectivity": "USB-A",
                                        "color": "Black"},
                "Dell USB-C Adapter": {"price_range": (20, 90), "type": "Adapter", "compatibility": "USB-C",
                                       "color": "White"},
                "Dell Premier Backpack": {"price_range": (82, 120), "type": "Backpack", "size": "15.6-inch",
                                          "color": "Gray"},
                "Dell External Portable Hard Drive": {"price_range": (12, 180), "type": "External Hard Drive",
                                                      "capacity": "1TB", "color": "Silver"},
                "Dell USB-C Mobile Adapter": {"price_range": (6, 80), "type": "Adapter", "connectivity": "USB-C",
                                              "color": "Black"}
            },
            "Servers": {
                "Dell PowerEdge R340": {"price_range": (1800, 5200), "cpu": "Intel Xeon E-2234", "memory": "32GB DDR4",
                                        "disk": "1TB HDD"},
                "Dell PowerEdge R640": {"price_range": (4800, 9200), "cpu": "Intel Xeon Gold 5218",
                                        "memory": "64GB DDR4", "disk": "2TB HDD"}
            }
        }

    def generate_random_events(self):
        while True:
            geo_location = self.generate_weighted_geo_location()
            user_type = random.choice(["business_users", "students", "consumers", "brokers", "channel_partners"])
            if user_type == "consumers":
                user_type += "_non_students"

            if user_type in ["students", "consumers_non_students"]:
                age_group = random.choice(["18-25", "25-40", "over_40"])
                gender = random.choice(["male", "female"])
                if gender == "female" and geo_location["lat"] > 32 and geo_location[
                    "lat"] < 42:  # California latitude range
                    product_type = "Laptops"
                    product_name = random.choice(["Dell XPS 15", "Dell XPS 13"])
                else:
                    product_type = random.choice(["Laptops", "Accessories"])
                    product_name = random.choice(list(self.dell_product_catalog[product_type].keys()))
            else:
                age_group = None
                gender = None
                product_type = random.choice(["Laptops", "Accessories"])
                product_name = random.choice(list(self.dell_product_catalog[product_type].keys()))

            if user_type not in self.previous_products:
                self.previous_products[user_type] = {}

            event_type = random.choices(["click", "select", "buy", "abandoned"], weights=[0.6, 0.3, 0.05, 0.05])[0]

            price_range = self.dell_product_catalog[product_type][product_name]["price_range"]
            price = random.randint(price_range[0], price_range[1])

            event = {
                "timestamp": datetime.utcnow(),
                "location": geo_location,
                "user_type": user_type,
                "age_group": age_group,
                "gender": gender,
                "event_type": event_type,
                "product_name": product_name,
                "product_type": product_type,
                "price": price,
                "memory": self.dell_product_catalog[product_type][product_name].get("memory"),
                "cpu": self.dell_product_catalog[product_type][product_name].get("cpu"),
                "disk": self.dell_product_catalog[product_type][product_name].get("disk"),
                "video_card": self.dell_product_catalog[product_type][product_name].get("video_card")
            }

            yield event

            if product_name not in self.previous_products[user_type]:
                self.previous_products[user_type][product_name] = {"product_type": product_type, "price": price}

    def generate_weighted_geo_location(self):
        # Weighted random choice for geo-location, focusing on the Americas and major cities
        major_cities = {
            "New York": {"lat": 40.7128, "lon": -74.0060},
            "Los Angeles": {"lat": 34.0522, "lon": -118.2437},
            "Chicago": {"lat": 41.8781, "lon": -87.6298},
            "Houston": {"lat": 29.7604, "lon": -95.3698},
            "Toronto": {"lat": 43.65107, "lon": -79.347015},
            "Montreal": {"lat": 45.5017, "lon": -73.5673},
            "Vancouver": {"lat": 49.2827, "lon": -123.1207},
            "Mexico City": {"lat": 19.4326, "lon": -99.1332},
            "Rio de Janeiro": {"lat": -22.9068, "lon": -43.1729},
            "Buenos Aires": {"lat": -34.6037, "lon": -58.3816},
            "São Paulo": {"lat": -23.5505, "lon": -46.6333}
        }

        cities = list(major_cities.keys())
        weights = [4] * 4 + [3] * 7  # Increase weight for major cities

        chosen_city = random.choices(cities, weights=weights, k=1)[0]
        return major_cities[chosen_city]

    def index_events_to_es(self):
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "location": {"type": "geo_point"},
                    "user_type": {"type": "keyword"},
                    "age_group": {"type": "keyword"},
                    "gender": {"type": "keyword"},
                    "event_type": {"type": "keyword"},
                    "product_name": {"type": "text"},
                    "product_type": {"type": "keyword"},
                    "price": {"type": "integer"},
                    "memory": {"type": "keyword"},
                    "cpu": {"type": "keyword"},
                    "disk": {"type": "keyword"},
                    "video_card": {"type": "keyword"}
                }
            }
        }

        if not self.es.indices.exists(index=self.index):
            self.es.indices.create(index=self.index, body=mapping)

        events = self.generate_random_events()

        while True:
            bulk_data = []
            for _ in range(25):  # Bulk indexing 1000 events at a time
                event = next(events)
                bulk_data.append({"_index": self.index, "_source": event})


            bulk(self.es, bulk_data)
            time.sleep(1)
            print("Indexed 25 events.")


def signal_handler(sig, frame):
    print("Halting event generation...")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    data_generator = EcommerceDataGenerator()
    data_generator.index_events_to_es()
