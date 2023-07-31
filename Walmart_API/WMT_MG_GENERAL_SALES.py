from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
#Obtain Mongo Connection String
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
client = MongoClient(MongoOnlineConnection)
cnn = client["GAON_WMT"]
def importar_historico_WMT(cnn,mes_inicial, mes_final):
    collection = cnn["Sale_Detail_Package"]
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
    result_month = collection.aggregate(pipeline_month)
    results_list = []
    a = 0
    for doc in result_month:
        year = doc["_id"]["year"]
        month = doc["_id"]["month"]
        total_quantity = doc["Ventas_Totales"]
        a += total_quantity
        results_list.append((year, month, total_quantity))
    return results_list, a

mes_inicial=datetime(2023,5,1)
mes_final=datetime(2023,6,1)
result_general_WMT,WMT_Total=importar_historico_WMT(cnn,mes_inicial,mes_final)
print(result_general_WMT)
print(WMT_Total)

