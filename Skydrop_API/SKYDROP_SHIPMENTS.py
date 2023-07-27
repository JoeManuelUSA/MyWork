from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv, set_key
from dateutil.parser import parse
import requests
import os
load_dotenv()
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
MongoLocalHost=os.environ.get('MongoLocalConnectionHost')
MongoLocalServer=os.environ.get('MongoLocalConnectionServer')
client = MongoClient(MongoOnlineConnection)
#client=MongoClient(f'{MongoLocalHost}', int(MongoLocalServer))
access_token=os.environ.get('SKYDROP_ACCESS_TOKEN')
cnn = client["GAON_SKYDROP"]

def obtain_shipments(cnn,access_token):
    collection = cnn["Shipments"]
    url="https://radar-api.skydropx.com/v1/shipments"
    headers = {
        "Authorization": f"Token token={access_token}",
        "content-type": "application/json"
    }
    response = requests.get(url, headers=headers)
    orders = response.json()
    page_max=orders['meta']['total_pages']
    page=0
    while page<=page_max:
        page = orders['meta']['current_page']
        for shipment in orders['shipments']:
            date_created = shipment['created_at']
            shipment_id = shipment['id']
            client_name = shipment['addresses']['address_to']['name']
            destination = shipment['addresses']['address_to']['street1']
            status = shipment['status']
            date_created = parse(date_created)
            order_data = {
                "Shipment_ID": shipment_id,
                "Client_Name": client_name,
                "Destination": destination,
                "Status": status,
                "Date_Created": date_created
            }
            collection.replace_one({"Shipment_ID": order_data["Shipment_ID"]}, order_data, upsert=True)
        url = f"https://radar-api.skydropx.com/v1/shipments?page={page + 1}"
        response = requests.get(url, headers=headers)
        orders = response.json()

obtain_shipments(cnn,access_token)