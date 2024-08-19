import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
import yaml
from sqlalchemy import text
import datetime


random.seed(100)


class AWSDBConnector:

    def __init__(self, db_creds="./db_creds.yaml"):

        with open (db_creds, "r") as stream:
            creds = yaml.safe_load(stream)

        self.HOST = creds["HOST"]
        self.USER = creds["USER"]
        self.PASSWORD = creds["PASSWORD"]
        self.DATABASE = creds["DATABASE"]
        self.PORT = creds["PORT"]
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:

        pin_stream_url = "https://kbd3ai5zhc.execute-api.us-east-1.amazonaws.com/test/streams/streaming-0e33e87dfa09-pin/record"
        geo_stream_url = "https://kbd3ai5zhc.execute-api.us-east-1.amazonaws.com/test/streams/streaming-0e33e87dfa09-geo/record"
        user_stream_url = "https://kbd3ai5zhc.execute-api.us-east-1.amazonaws.com/test/streams/streaming-0e33e87dfa09-user/record"

        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)


            def serialise_datetimes(obj):
                if isinstance(obj, datetime.datetime):
                    return obj.isoformat()

            print(pin_result)
            pin_stream_payload = json.dumps({
                "StreamName": "streaming-0e33e87dfa09-pin",
                "Data": pin_result,
                "PartitionKey": "pin_partition"
            })


            print(geo_result)
            geo_stream_payload = json.dumps({
                "StreamName": "streaming-0e33e87dfa09-geo",
                "Data": geo_result,
                "PartitionKey": "geo_partition"
            }, default=serialise_datetimes)


            print(user_result)
            user_stream_payload = json.dumps({
                "StreamName": "streaming-0e33e87dfa09-user",
                "Data": user_result,
                "PartitionKey": "user_partition"
            }, default=serialise_datetimes)

            headers = {"Content-Type": "application/json"}
            pin_stream_response = requests.request("PUT", pin_stream_url, headers=headers, data=pin_stream_payload)
            geo_stream_response = requests.request("PUT", geo_stream_url, headers=headers, data=geo_stream_payload)
            # user_stream_response = requests.request("PUT", user_stream_url, headers=headers, data=user_topic_data)
            print("PIN STREAM STATUS: ", pin_stream_response.status_code)
            print(pin_stream_response.json())
            print("GEO STREAM STATUS: ", geo_stream_response.status_code)
            print(geo_stream_response.json())
            # print("USER STREAM STATUS: ", user_stream_response.status_code)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


