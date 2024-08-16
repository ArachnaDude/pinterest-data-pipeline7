import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
import yaml
from sqlalchemy import text


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

        pin_topic_url = "https://kbd3ai5zhc.execute-api.us-east-1.amazonaws.com/test/topics/0e33e87dfa09.pin"
        geo_topic_url = "https://kbd3ai5zhc.execute-api.us-east-1.amazonaws.com/test/topics/0e33e87dfa09.geo"
        user_topic_url = "https://kbd3ai5zhc.execute-api.us-east-1.amazonaws.com/test/topics/0e33e87dfa09.user"

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

                        
            print(pin_result)
            pin_topic_data = json.dumps(
                {"records": [{"value": pin_result}]}, default=str
            )
            print(geo_result)
            geo_topic_data = json.dumps(
                {"records": [{"value": geo_result}]}, default=str
            )
            print(user_result)
            user_topic_data = json.dumps(
                {"records": [{"value": user_result}]}, default=str
            )

            headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
            pin_topic_response = requests.request("POST", pin_topic_url, headers=headers, data=pin_topic_data)
            geo_topic_response = requests.request("POST", geo_topic_url, headers=headers, data=geo_topic_data)
            user_topic_response = requests.request("POST", user_topic_url, headers=headers, data=user_topic_data)
            print(pin_topic_response.status_code)
            print(geo_topic_response.status_code)
            print(user_topic_response.status_code)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


