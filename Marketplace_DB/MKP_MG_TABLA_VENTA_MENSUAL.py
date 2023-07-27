from datetime import datetime, timedelta
from pymongo import MongoClient
from dateutil.parser import parse
from dotenv import load_dotenv, set_key
import os
load_dotenv()
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
client = MongoClient(MongoOnlineConnection)
cnn1 = client["GAON_MLB"]
cnn2=client["GAON_WMT"]
def importar_historico_MLB(cnn,mes_inicial, mes_final):
    collection = cnn["Sale_Detail_Package"]
    pipeline_month = [
        # First stage: filter the documents
        {
            "$match": {"Date_Created": {"$gte": mes_inicial, "$lte": mes_final}}
        },
        # Second stage: group by month and sum up the quantities
        {
            "$project": {
                "_id": 1,
                "Status":"$Status",
                "Date_Created":"$Date_Created",
                "Monto":"$Final_Amount"
            }
        }
    ]
    result_month = collection.aggregate(pipeline_month)
    results_list = []
    a = 0
    for doc in result_month:
        Shipping_ID = doc["_id"]
        Status = doc["Status"]
        Date_Created = doc["Date_Created"]
        Date_Created = Date_Created.strftime("%Y-%m-%d %H:%M:%S")
        Monto=doc["Monto"]
        results_list.append((Shipping_ID, Status, Date_Created,Monto))
        #print(results_list)
    return results_list
def importar_historico_WMT(cnn,mes_inicial, mes_final):
    collection = cnn["Sale_Detail_Package"]
    pipeline_month = [
        # First stage: filter the documents
        {
            "$match": {"Date_Created": {"$gte": mes_inicial, "$lte": mes_final}}
        },
        # Second stage: group by month and sum up the quantities
        {
            "$project": {
                "_id": 1,
                "Order_ID":"$Order_ID",
                "Date_Created":"$Date_Created",
                "Status":"$Shipping_Status",
                "Monto":"$Total_Paid"
            }
        }
    ]
    result_month = collection.aggregate(pipeline_month)
    results_list = []
    a = 0
    for doc in result_month:
        Order_ID = doc["Order_ID"]
        status=doc["Status"]
        Date_Created = doc["Date_Created"]
        Date_Created = Date_Created.strftime("%Y-%m-%d %H:%M:%S")
        Monto=doc["Monto"]
        results_list.append((Order_ID,status, Date_Created,Monto))
        print(results_list)
    return results_list
MLB_mes_inicial=datetime(2023,1,1)
MLB_mes_final=datetime(2023,5,1)
WMT_mes_inicial=datetime(2023,1,1)
WMT_mes_final=datetime(2023,5,1)
MKP_DIC={}
result_general_MLB=importar_historico_MLB(cnn1,MLB_mes_inicial,MLB_mes_final)
result_general_WMT=importar_historico_WMT(cnn2,WMT_mes_inicial,WMT_mes_final)
MKP_DIC["MLB"]=[result_general_MLB]
MKP_DIC["WMT"]=[result_general_WMT]
print()

