from datetime import datetime, timedelta
from pymongo import MongoClient
from dateutil.parser import parse
client = MongoClient("mongodb+srv://olimpo:0limpo_soft@olimpo.18mvdkl.mongodb.net/")
cnn = client["GAON_MLB"]
def importar_historico_producto(cnn,mes_inicial, mes_final):
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
mes_inicial=datetime(2023,1,1)
mes_final=datetime(2023,5,1)
result_general=importar_historico_producto(cnn,mes_inicial,mes_final)
