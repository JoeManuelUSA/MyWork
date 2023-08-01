#This code obtains the general sales of MercadoLibre
#The sales data is obtained from MongoDB
from datetime import datetime, timedelta
from pymongo import MongoClient
from dateutil.parser import parse
from dotenv import load_dotenv
import os
#Load .env file
load_dotenv()
#Obtain Mongo Connection String
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
client = MongoClient(f"{MongoOnlineConnection}")
cnn = client["GAON_MLB"]
def importar_historic_MLB(cnn,mes_inicial, mes_final):#function for obtaining sales by date
    collection = cnn["Sale_Detail_Package"]#Selects collection from Mongo DB
    pipeline_month = [
        # First stage: filter the documents
        {
            "$match": {"Date_Created": {"$gte": mes_inicial, "$lte": mes_final}}
        },
        # Second stage: group information 
        {
            "$project": {
                "_id": 1,
                "Status":"$Status",
                "Date_Created":"$Date_Created",
                "Monto":"$Final_Amount"
            }
        }
    ]
    result_month = collection.aggregate(pipeline_month)#Executes pipeline
    results_list = []
    a = 0
    for doc in result_month:#Obtains the information for each sale
        Shipping_ID = doc["_id"]
        Status = doc["Status"]
        Date_Created = doc["Date_Created"]
        Date_Created = Date_Created.strftime("%Y-%m-%d %H:%M:%S")
        Monto=doc["Monto"]
        results_list.append((Shipping_ID, Status, Date_Created,Monto))#appends information to list
    return results_list #returns list
mes_inicial=datetime(2023,1,1)#Start date
mes_final=datetime(2023,5,1)#End Date
#Obtains sales using start and end date
result_general=importar_historic_MLB(cnn,mes_inicial,mes_final)
