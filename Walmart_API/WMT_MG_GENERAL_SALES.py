#This code obtains the general sales by month of Walmart
#The sales data is obtianed from MongoDB
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
#load .env file
load_dotenv()
#Obtain Mongo Connection String
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
client = MongoClient(MongoOnlineConnection)
cnn = client["GAON_WMT"]#Selects MONGO DB
def import_historic_WMT(cnn,mes_inicial, mes_final):#Imports Historic Walmart Sales
    collection = cnn["Sale_Detail_Package"]#Selects Collection from MongoDB
    pipeline_month = [
        # First stage: filter the documents
        {
            "$match": {"Date_Created": {"$gte": mes_inicial, "$lte": mes_final}}
#
        },
        # Second stage: group by month and sum up the quantities
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$Date_Created"},
                    "month": {"$month": "$Date_Created"},
                },
                "Ventas_Totales": {"$sum": "$Total_Paid"}
            }
        },
        {
            "$sort": {"_id.month": 1}
        }
    ]
    result_month = collection.aggregate(pipeline_month)#Executes the pipeline to MongoDB
    results_list = []#Creates a list
    a = 0
    for doc in result_month:#Obtains the information for each month
        year = doc["_id"]["year"]
        month = doc["_id"]["month"]
        total_quantity = doc["Ventas_Totales"]
        a += total_quantity
        results_list.append((year, month, total_quantity))#appends information to list
    return results_list, a #Returns a list and "A"

mes_inicial=datetime(2023,5,1)#Starting Date
mes_final=datetime(2023,6,1)#End date
result_general_WMT,WMT_Total=import_historic_WMT(cnn,mes_inicial,mes_final)
print(result_general_WMT)
print(WMT_Total)

