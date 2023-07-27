from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv, set_key
import os
load_dotenv()
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
client = MongoClient(MongoOnlineConnection)
cnn1 = client["GAON_MLB"]
cnn2= client['GAON_WMT']
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
def importar_historico_MLB1(cnn,mes_inicial, mes_final):
    collection = cnn["Sale_Detail_Package"]
    pipeline_month = [
        # First stage: filter the documents
        {
            "$match": {
                "$and": [
                    {"Status": "paid"},
                    {"Date_Created": {"$gte": mes_inicial, "$lte": mes_final}}
                ]
            }
        },
        # Second stage: group by month and sum up the quantities
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$Date_Created"},
                    "month": {"$month": "$Date_Created"},
                },
                "Ventas_Totales": {"$sum": "$Final_Amount"},

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
def importar_historico_MLB(cnn,mes_inicial, mes_final):
    collection = cnn["Sale_Detail"]
    pipeline_month = [
        # First stage: filter the documents
        {
            "$match": {
                    "Date_Created": {"$gte": mes_inicial, "$lte": mes_final}
            }
        },
        # Second stage: group by month and sum up the quantities
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$Date_Created"},
                    "month": {"$month": "$Date_Created"},
                },
                "Ventas_Totales": {"$sum": {"$multiply": ["$Unit_Price", "$Quantity"]}},
                "Quantity":{"$sum": "$Quantity"}
            }
        },
        {
            "$sort": {"_id.month": 1}
        }
    ]
    result_month = collection.aggregate(pipeline_month)
    results_list = []
    a = 0
    b=0
    for doc in result_month:
        year = doc["_id"]["year"]
        month = doc["_id"]["month"]
        total_quantity = doc["Ventas_Totales"]
        unit_quantity=doc["Quantity"]
        a += total_quantity
        b+=unit_quantity
        results_list.append((year, month, total_quantity,unit_quantity))
    return results_list, a,b

# Assuming the time zone difference during Central Daylight Time is -5 hours
cdmx_time_zone_diff_hours = -6
MLB_dia_mes_inicial=datetime(2023,1,1)
MLB_dia_mes_final=datetime(2023,6,1)
# Convert CDMX time range to UTC
MLB_dia_mes_inicial_utc = MLB_dia_mes_inicial - timedelta(hours=cdmx_time_zone_diff_hours)
MLB_dia_mes_final_utc = MLB_dia_mes_final - timedelta(hours=cdmx_time_zone_diff_hours)



WMT_dia_mes_inicial=datetime(2023,5,1)
WMT_dia_mes_final=datetime(2023,6,1)

MKP_DIC={}
result_general_MLB,MLB_Total,unit=importar_historico_MLB(cnn1,MLB_dia_mes_inicial_utc,MLB_dia_mes_final_utc)
result_general_WMT,WMT_Total=importar_historico_WMT(cnn2,WMT_dia_mes_inicial,WMT_dia_mes_final)

MKP_DIC["MLB"]=[result_general_MLB]
MKP_DIC["WMT"]=[result_general_WMT]
print(result_general_MLB)
print(MLB_Total)
print(unit)

