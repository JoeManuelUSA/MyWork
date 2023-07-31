#this code obtains shipments from Skydrop API and stores the information to Mongo DB
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
from dateutil.parser import parse
import requests
import os
#Load .env file
load_dotenv()
#Obtain the Mongo Conection String and DB
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
client = MongoClient(MongoOnlineConnection)
#Obtain Credentials
access_token=os.environ.get('SKYDROP_ACCESS_TOKEN')
cnn = client["GAON_SKYDROP"]
def obtain_shipments(cnn,access_token):#Function for Obtaining Shipments from Skydrop API
    collection = cnn["Shipments"]#Selects the collection from Mongo DB
    url="https://radar-api.skydropx.com/v1/shipments"#Request URL
    #Header for authorization token
    headers = {
        "Authorization": f"Token token={access_token}",
        "content-type": "application/json"
    }
    response = requests.get(url, headers=headers)#Executes GET request
    orders = response.json()#Converting the response to JSON format
    page_max=orders['meta']['total_pages']#Consulting Page limit for while condition
    page=0
    while page<=page_max:#While Condition,makes sure that each shipment is obtained.
        page = orders['meta']['current_page']#Identifies the current page
        for shipment in orders['shipments']:#for loop for obtaining information an each shipment order
            #Shipment Information
            date_created = shipment['created_at']
            shipment_id = shipment['id']
            client_name = shipment['addresses']['address_to']['name']
            destination = shipment['addresses']['address_to']['street1']
            status = shipment['status']
            date_created = parse(date_created)
            #Creates the table format of each shipment 
            order_data = {
                "Shipment_ID": shipment_id,
                "Client_Name": client_name,
                "Destination": destination,
                "Status": status,
                "Date_Created": date_created
            }
            #Updates or Inserts each shipment to DB
            collection.replace_one({"Shipment_ID": order_data["Shipment_ID"]}, order_data, upsert=True)
        #Generates a new API request for next page
        url = f"https://radar-api.skydropx.com/v1/shipments?page={page + 1}"
        response = requests.get(url, headers=headers)
        orders = response.json()

obtain_shipments(cnn,access_token)