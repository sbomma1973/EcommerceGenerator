# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
# for basic randomness and seedgen
import random
import random_address


import requests

import os
from dotenv import dotenv_values
from datetime import datetime, timedelta
import time

# You know, for search
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk


class LogisticsData:
    def __init__(self):
        self.config = dotenv_values(f"{os.path.dirname(os.path.realpath(__file__))}/.env")
        self.es = Elasticsearch(
            cloud_id=self.config.get("CLOUD_ID"),
            basic_auth=(self.config.get("USER"), self.config.get("PASSWORD"))
        )

        # Current time to ground the date generation
        self.current_time = datetime.utcnow()

        self.index = "logistics-data"
        self.pickup_index = "pickup-logistics-data"
        self.pickup_locations = []
        self.number_of_orders = 100

        self.company_names = []
        self.brokers = []

        self.company_data = {}
        self.load_id_counter = 0
        self.batch_id_counter = 0

        self.seed = "gotta have that randomness"
        random.seed(self.seed)

        self.documents_to_index = []

        self.steps = [
            # initial document will include: requestor company, requestor individual, proposed driver name
            "ReceiveOrder",
            # timestamp, source, destination
            "OrderNegotiation",
            # approval timestamp
            "AssignToDriver",
            # assigned driver
            "OutForDelivery",
            # complete or incomplete
            "DeliveryVerification",
            # link to verification proof
            "PaymentDeliveredToProvider",
            # time service provider was paid
            "PaymentDeliveredToDriver"
        ]

    def get_random_address(self, state=""):
        # to regenerate the address when it isn't suitable
        if state:
            rand_addr = random_address.real_random_address_by_state(state)
            if 'city' not in rand_addr:
                while 'city' not in rand_addr:
                    rand_addr = random_address.real_random_address_by_state(state)
        else:
            rand_addr = random_address.real_random_address()
            if 'city' not in rand_addr:
                while 'city' not in rand_addr:
                    rand_addr = random_address.real_random_address()

        return rand_addr

    # Loads company names, names, geo points, and addresses to be used for randomness
    def load_randomness_data(self):
        # Loads fake company data I grabbed from random generation websites
        ### ---- LOADS NAMES --- ###
        f = open("fakecompanynames.txt", "r")
        self.company_names = f.read()
        f.close()

        # Unpacks it into a dictionary so I can add to it
        self.company_names = self.company_names.split("\n")

        # Loads a list of random names
        f = open("fakenames.txt")
        self.names = f.read().split("\n")
        f.close()

        #Grabs the first 20 brokers
        for broker_id in range(0,20):
            broker_name_index = random.randint(0, len(self.names) - 1)
            self.brokers.append(
                {
                    "name": self.names[broker_name_index],
                    "email": ".".join(self.names[broker_name_index].split(" ")).lower() + "@tql.com"
                }
            )
            self.names.remove(self.names[broker_name_index])

        # ties all of these together in a company list
        # TODO: remove slice below
        for company in self.company_names:
            name_index = random.randint(0, len(self.names) - 1)

            rand_addr = self.get_random_address()

            self.company_data[company] = {
                "data": {
                    "company": company,
                    "geo": {
                        "location": {
                            "lat":rand_addr['coordinates']['lat'],
                            "lon":rand_addr['coordinates']['lng']
                        },
                        "city_name": rand_addr['city'],
                        "country_iso_code": "USA",
                        "postal_code":rand_addr['postalCode'],
                        "region_iso_code": rand_addr['state'],
                        "address": rand_addr['address1']
                    },
                    "contact": {
                        "name": self.names[name_index],
                        "email": ".".join(self.names[name_index].split(" ")).lower() + "@" + company.replace(" ","").lower() + ".com"
                    }
                }
            }

            self.names.remove(self.names[name_index])

    def index_to_es(self):
        # indexing all of the fresh APIs from data.gov right after we delete the old ones
        docs = self.documents_to_index.copy()
        pickup = self.pickup_locations.copy()

        # Indexes all base information about the load transaction
        for success, info in parallel_bulk(
                client=self.es,
                actions=self.documents_to_index,
                index=self.index,
        ):
            print(success,info)
            if not success:
                print("UNSUCCESSFUL: ", info)


        # Indexes information about pickup locations
        for success, info in parallel_bulk(
                client=self.es,
                actions=self.pickup_locations,
                index=self.pickup_index,
        ):
            print(success,info)
            if not success:
                print("UNSUCCESSFUL: ", info)

        self.documents_to_index = []
        self.pickup_locations = []


    def random_time_between_two_dates(self, first, second):
        first_epoch = first.strftime("%s")
        second_epoch = second.strftime("%s")
        random_date_epoch = random.randint(int(first_epoch), int(second_epoch))
        return datetime.fromtimestamp(random_date_epoch)

    def _get_route(self, start, end):
        route_url = f'http://router.project-osrm.org/route/v1/driving/{start[0]},{start[1]};{end[0]},{end[1]}?alternatives=false&steps=true'
        r = requests.get(route_url)
        res = r.json()

        coord_list = []
        if res['code'].lower() != "ok":
            print("Failed to get route!!")
            return [], route_url

        for route in res['routes']:
            for leg in route['legs']:
                duration = leg['duration']
                distance = leg['distance']

                for step in leg['steps']:
                    for intersection in step['intersections']:
                        coord_list.append(intersection['location'])
               #coord_list.append(coord)

        return coord_list, route_url, duration, distance

    def _get_multi_route(self, points, destination):
        coord_list = []
        for point in points:
            coord_list.append([point['load']['geo']['location']['lon'],point['load']['geo']['location']['lat']])
        coord_list.append([destination['geo']['location']['lon'], destination['geo']['location']['lat']])

        route_path = ";".join([f'{x[0],x[1]}'.replace("(","").replace(")","").replace(" ","") for x in coord_list])
        route_url = f'http://router.project-osrm.org/route/v1/driving/{route_path}?alternatives=true&steps=true'
        r = requests.get(route_url)
        res = r.json()


        if res['code'].lower() != "ok":
            print("Route lookup failed!!")
            return []

        ret_list = []
        for route in res['routes']:
            for leg in route['legs']:
                for step in leg['steps']:
                    for intersection in step['intersections']:
                        ret_list.append(intersection['location'])

        return ret_list, route_url

    def _generate_load_set(self, company, number_of_loads):
        company_list = list(self.company_data.keys())

        range_top_end_price_per_unit = random.randint(100, 200)
        range_low_end_price_per_unit = random.randint(15, 90)

        for load_counter in range(0, int(number_of_loads)):
            destination_company_index = random.randint(0, len(self.company_data) - 1)
            destination_company = self.company_data[company_list[destination_company_index]]['data']
            phase_index = random.randint(0, len(self.steps) - 1)
            name_index = random.randint(0, len(self.names) - 1)

            # Gets number of units to be shipped
            number_of_units = random.randint(50, 100)

            # Gets proposed value
            price = random.randint(range_low_end_price_per_unit, range_top_end_price_per_unit)
            broker_index = random.randint(0, len(self.brokers) - 1)

            document_to_index = {
                "load": {
                    "id":self.load_id_counter,
                    "metrics": {
                        "weight": random.randint(40000, 45000),
                    },

                    "batch_id": self.batch_id_counter,
                    "phase": self.steps[phase_index],
                    "timestamps": {},
                    "broker": self.brokers[broker_index],
                },
                "payment": {
                    "total_price": price * number_of_units,
                },

                "destination": destination_company,

                "shipper": self.company_data[company]['data'],
            }

            # Generates the initial receipt of an order + its deadline
            if phase_index >= 0:
                document_to_index, current_pickup_locations, order_placed_timestamp, delivery_deadline = self.receive_order(
                    document_to_index,
                    name_index,
                    broker_index,
                    destination_company,
                    company
                )
                current_pickup_locations = self.define_driver_pickup_route(document_to_index, current_pickup_locations)

            if phase_index >= 1:
                document_to_index, negotiation_complete_time = self.negotiation(order_placed_timestamp, document_to_index)
            # Assign to driver
            if phase_index >= 2:
                assign_driver_time, document_to_index = self.assign_driver(document_to_index, negotiation_complete_time, name_index)
            if phase_index in [3, 4]:
                current_pickup_locations = self.place_driver_on_map(current_pickup_locations)
            if phase_index >= 4:
                document_to_index, delivery_verification_timestamp = self.verify_delivery(document_to_index, delivery_deadline)
            if phase_index >= 5:
                document_to_index, payment_timestamp = self.pay_tql(document_to_index,delivery_verification_timestamp)
            if phase_index >=6:
                self.pay_driver(document_to_index, payment_timestamp)

            self.documents_to_index.append(document_to_index)
            self.pickup_locations += current_pickup_locations

            self.load_id_counter += 1

        self.batch_id_counter += 1


    def pay_driver(self, document_to_index, payment_timestamp):
        driver_paid_timestamp = self.random_time_between_two_dates(
            payment_timestamp,
            payment_timestamp + timedelta(days=2)
        )

        document_to_index['load']['timestamps']['driver_paid'] = driver_paid_timestamp
        document_to_index['payment']['driver_amount'] = document_to_index['payment']['total_price'] * .3


    def pay_tql(self, document_to_index, delivery_verification_timestamp):
        # >= 5 - Defines the payment process to TQL
        #TQL gets paid full amount at a random time after delivery verification
        payment_timestamp = self.random_time_between_two_dates(
            delivery_verification_timestamp,
            delivery_verification_timestamp + timedelta(days=3)
        )

        document_to_index['load']['timestamps']['payment_received'] = payment_timestamp
        document_to_index['payment']['tql_amount'] = document_to_index['payment']['total_price'] * .7
        return document_to_index, payment_timestamp

    def receive_order(self, document_to_index, name_index, broker_index, destination_company, company):
        # Gets an initial timestamp that is within the last 2 days
        order_placed_timestamp = self.random_time_between_two_dates(
            self.current_time - timedelta(days=6),
            self.current_time
        )
        document_to_index['load']['timestamps']['order_placed'] = order_placed_timestamp.strftime('%Y-%m-%dT%H:%M:%S%z')+"Z"

        # TODO: Consider taking the sum of all duration for a load and add 1 day to it to get the deadline
        # Gets a deadline that is randomly 2-5 days out from the order date
        deadline_timestamp = self.random_time_between_two_dates(
            order_placed_timestamp,
            order_placed_timestamp + timedelta(days=random.randint(1, 6))
        )
        document_to_index['load']['timestamps']['delivery_deadline'] = deadline_timestamp.strftime('%Y-%m-%dT%H:%M:%S%z')+"Z"

        current_pickup_locations = []
        # Generates load sources
        for number_of_sources in range(0, random.randint(1, 3)):
            random_location = self.get_random_address(state=document_to_index['shipper']['geo']['region_iso_code'])
            current_pickup_locations.append(
                {
                    "load":{
                        "id": document_to_index['load']['id'],
                        "metrics": {},
                        "geo": {
                            "location": {
                                "lat": random_location['coordinates']['lat'],
                                "lon": random_location['coordinates']['lng']
                            },
                            "city_name": random_location['city'],
                            "country_iso_code": "USA",
                            "postal_code": random_location['postalCode'],
                            "region_iso_code": random_location['state'],
                            "address": random_location['address1'],
                        },
                        "driver": {
                            "name": self.names[name_index]
                        },
                        "broker": self.brokers[broker_index]
                    },
                    "destination": destination_company,
                    "shipper": self.company_data[company]['data'],
                }
            )

        return document_to_index, current_pickup_locations, order_placed_timestamp, deadline_timestamp

    def negotiation(self, order_placed_timestamp, document_to_index):
        # Generates details related to the negotiation of a load and order

        negotiation_complete_time = self.random_time_between_two_dates(
            order_placed_timestamp,
            order_placed_timestamp + timedelta(days=random.randint(1,2))
        )
        document_to_index['load']['timestamps']['negotiation_complete'] = negotiation_complete_time.strftime('%Y-%m-%dT%H:%M:%S%z')+"Z"

        return document_to_index, negotiation_complete_time

    def assign_driver(self, document_to_index, negotiation_complete_time, name_index):
        assign_driver_time = self.random_time_between_two_dates(
            negotiation_complete_time,
            negotiation_complete_time + timedelta(days=1)
        )

        document_to_index['load']['driver'] = {}
        document_to_index['load']['driver']['name'] = self.names[name_index]
        document_to_index['load']['timestamps']['driver_assigned'] = assign_driver_time.strftime('%Y-%m-%dT%H:%M:%S%z')+"Z"
        return assign_driver_time, document_to_index

    def define_driver_pickup_route(self, document_to_index, current_pickup_locations):
        # Decides if the stop has been completed or not
        for stop in current_pickup_locations:
            current_index = current_pickup_locations.index(stop)

            if current_index == len(current_pickup_locations) - 1:
                route, route_url, duration, distance = self._get_route(
                    (
                        stop['load']['geo']['location']['lon'],
                        stop['load']['geo']['location']['lat']
                    ),
                    (
                        document_to_index['destination']['geo']['location']['lon'],
                        document_to_index['destination']['geo']['location']['lat']

                    )
                )

            else:
                route, route_url, duration, distance = self._get_route(
                    (
                        stop['load']['geo']['location']['lon'],
                        stop['load']['geo']['location']['lat'],

                    ),
                    (
                        current_pickup_locations[current_index + 1]['load']['geo']['location']['lon'],
                        current_pickup_locations[current_index + 1]['load']['geo']['location']['lat'],

                    )
                )

            current_pickup_locations[current_index]['load']['path'] = {}
            current_pickup_locations[current_index]['load']['path']['coordinates'] = route
            current_pickup_locations[current_index]['load']['path']['type'] = "LineString"

            current_pickup_locations[current_index]['load']['metrics']['distance'] = distance * 0.000621371
            current_pickup_locations[current_index]['load']['metrics']['duration'] = duration

        return current_pickup_locations

    def place_driver_on_map(self, current_pickup_locations):
        segment_index = random.randint(0, len(current_pickup_locations)-1)
        placement_index = random.randint(0, len(current_pickup_locations[segment_index]['load']['path']['coordinates'])-1)

        lat = current_pickup_locations[segment_index]['load']['path']['coordinates'][placement_index][1]
        lon = current_pickup_locations[segment_index]['load']['path']['coordinates'][placement_index][0]
        current_pickup_locations[segment_index]['load']['driver']['location'] = {
            "lat": lat,
            "lon": lon
        }
        return current_pickup_locations

    def verify_delivery(self, document_to_index, delivery_deadline):
        # >= 4 - Defines the verification process of the driver sending the load
        # Will get the deadline time and get some time 1 day before or 2 days after
        delivery_verification_timestamp = self.random_time_between_two_dates(
            delivery_deadline - timedelta(days=1),
            delivery_deadline + timedelta(days=1)
        )

        document_to_index['load']['timestamps']['delivery_verified'] = delivery_verification_timestamp

        return document_to_index, delivery_verification_timestamp

    # generates loads based on the information generated
    def generate_loads(self):
        progress = 1
        number_of_companies = len(self.company_data.keys())
        for company in self.company_data:
            number_of_loads = random.randint(1, 2)
            print(f"Processing {number_of_loads} loads for the company: {company}. {round(progress / number_of_companies * 100, 2)}% Complete!")

            self._generate_load_set(company, number_of_loads)
            progress += 1
            self.index_to_es()


def main():
    data_generator = LogisticsData()
    data_generator.load_randomness_data()
    data_generator.generate_loads()
    data_generator.index_to_es()

if __name__ == "__main__":
    main()



# TODO: Cleanup map tooltips
# TODO: Add complaince data for other steps
#TODO: Pretty up dashboard

# TODO: consider adding phase to the other index too