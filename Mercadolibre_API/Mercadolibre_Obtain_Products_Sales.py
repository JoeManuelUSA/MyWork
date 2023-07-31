import requests
import datetime
from datetime import datetime, timedelta
import pytz
from bson import ObjectId
from pymongo import MongoClient
from dateutil.parser import parse
from dotenv import load_dotenv, set_key
import os
load_dotenv()
from pymongo import UpdateOne
#Obtain credentials
access_token =os.environ.get('Token_MLB')
seller_id=os.environ.get('MLB_Seller_ID')
#Obtiene el enlace de conexion a Mongo DB,ya sea local o al cluster
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
MongoLocalHost=os.environ.get('MongoLocalConnectionHost')
MongoLocalServer=os.environ.get('MongoLocalConnectionServer')
#Elecion de Base de Datos, Local o Cluster
client = MongoClient(f"{MongoOnlineConnection}")
#client=MongoClient(f'{MongoLocalHost}', int(MongoLocalServer))
cnn=client["GAON_MLB"]
shipping_list=[]
def inserta_actualiza_categoria(cnn, access_token):  # Inserta/Actualiza la categoria
    if "Categories" in cnn.list_collection_names():
        collection = cnn["Categories"]
    else:
        collection = cnn.create_collection("Categories")
    collection = cnn["Categories"]
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.mercadolibre.com/sites/MLM/categories"
    response = requests.get(url, headers=headers)
    data = response.json()

    for category in data:
        Categoria = category["id"]
        Nombre = category["name"]
        filter = {"Category_ID": Categoria}
        doc = {"Category_ID": Categoria, "Name": Nombre}
        collection.replace_one(filter, doc, upsert=True)
def inserta_actualiza_tienda(cnn, access_token,seller_id):
    collection = cnn["Stores"]
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.mercadolibre.com/users/{seller_id}/brands"
    response = requests.get(url, headers=headers)
    data = response.json()
    for tienda in data["brands"]:
        store_id = tienda["official_store_id"]
        nombre = tienda["name"]
        fantasy_name = tienda["fantasy_name"]
        status = tienda["status"]
        permalink = tienda["permalink"]
        filter = {"Store_ID": store_id}
        doc = {"Store_ID": store_id, "Name": nombre, "Fantasy_Name": fantasy_name, "Status": status,"Permalink": permalink}
        collection.replace_one(filter, doc, upsert=True)
def get_or_insert_category(subcategories_collection, Sub_Category_ID, Name_Category, Category_ID):
    filter = {"Sub_Category_ID": Sub_Category_ID}
    doc = {"Category_ID": Category_ID, "Sub_Category_ID": Sub_Category_ID, "Name": Name_Category}
    subcategories_collection.replace_one(filter, doc, upsert=True)
def get_item_details(MLM, headers):
    url = f"https://api.mercadolibre.com/items/{MLM}"
    response = requests.get(url, headers=headers)
    data = response.json()
    return data
def get_shipping_details(buyer, headers):
    url = f"https://api.mercadolibre.com/shipments/{buyer}"
    response = requests.get(url, headers=headers)
    data = response.json()
    return data
def get_shipping_cost(Shipping_ID, headers):
    url = f"https://api.mercadolibre.com/shipments/{Shipping_ID}"
    response = requests.get(url, headers=headers)
    data = response.json()
    # data = data.get("shipping_option", {}).get("list_cost", 0)
    return data
def consulta_shipping_details(cnn, access_token):
    headers = {"Authorization": f"Bearer {access_token}"}
    if "Buyer_Information" in cnn.list_collection_names():
        buyer_collection = cnn["Buyer_Information"]
    else:
        buyer_collection = cnn.create_collection("Buyer_Information")
    buyer_collection = cnn["Buyer_Information"]
    shipping_collection = cnn["Sale_Detail"]
    for doc in shipping_collection.find({}, {'Shipping_ID': 1, '_id': 0}):
        if doc is not None:
            try:
                shipping_details = get_shipping_details(doc['Shipping_ID'], headers)
                Buyer_Name = shipping_details['receiver_address']['receiver_name']
                Buyer_City = shipping_details['receiver_address']['city']['name']
                Buyer_State = shipping_details['receiver_address']['state']['name']
                Buyer_CP = shipping_details['receiver_address']['zip_code']
                Buyer_Country = shipping_details['receiver_address']['country']['name']
                Buyer_Address = f"CP {Buyer_CP} / {shipping_details['receiver_address']['address_line']} - {shipping_details['receiver_address']['comment']} - {Buyer_City},{Buyer_State}"
                Buyer_Information = {
                    'Shipping_ID': doc['Shipping_ID'],
                    'Buyer_Name': Buyer_Name,
                    #'Buyer_Address': Buyer_Address,
                    'Buyer_City': Buyer_City,
                    'Buyer_State': Buyer_State,
                    #'Buyer_CP': Buyer_CP,
                    #'Buyer_Country': Buyer_Country,
                }
                buyer_collection.replace_one({"Shipping_ID": Buyer_Information["Shipping_ID"]}, Buyer_Information,upsert=True)
            except:
                continue
            return Buyer_Information
def consulta_productos(cnn, access_token,seller_id):
    if "Sub_Categories" in cnn.list_collection_names():
        subcategories_collection = cnn["Sub_Categories"]
    else:
        subcategories_collection = cnn.create_collection("Sub_Categories")
    if "Products_MLB" in cnn.list_collection_names():
        products_collection = cnn["Products_MLB"]
    else:
        products_collection = cnn.create_collection("Products_MLB")
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.mercadolibre.com/users/{seller_id}/items/search?status="
    all_products = []
    total_products = 0
    while url:
        response = requests.get(url, headers=headers)
        data = response.json()
        all_products.extend(data["results"])
        total_products += len(data["results"])
        if data['paging']['total'] > data['paging']['offset'] + data['paging']['limit']:
            offset = data['paging']['offset'] + data['paging']['limit']
            # url = f"https://api.mercadolibre.com/sites/MLM/search?seller_id=609389670&offset={offset}"
            url = f"https://api.mercadolibre.com/users/{seller_id}/items/search?status=&offset={offset}&limit=50"
        else:
            url = None
    for MLM in all_products:
        item_details = get_item_details(MLM, headers)
        ID_Sub_Category = item_details["category_id"]
        Title = item_details["title"]
        Original_Price = item_details["original_price"]
        Offer_Price = item_details["price"]
        Permalink = item_details["permalink"]
        Status_Product = item_details.get("status")
        Status_Product_Motivo = ", ".join(item_details.get("sub_status"))
        if Status_Product == "active":
            Status_Product_Motivo = "active"
        if Status_Product == "closed":
            Status_Product_Motivo = "closed"
        Oficial_Store_ID = item_details.get("official_store_id")
        Available_Quantity = item_details.get("available_quantity")
        Sold_Quantity = item_details.get("sold_quantity")
        category_name_url = f"https://api.mercadolibre.com/categories/{ID_Sub_Category}"
        response = requests.get(category_name_url)
        category_data = response.json()
        Category_Name = category_data["name"]
        Category_ID = category_data["path_from_root"][0]["id"]
        # print(Category_ID)
        get_or_insert_category(subcategories_collection, ID_Sub_Category, Category_Name, Category_ID)
        if Offer_Price is None:
            Offer_Price = 0
        if Original_Price is None:
            Original_Price = 0
        if Offer_Price > 0 and Offer_Price < Original_Price:
            Campaign = "SI"
        else:
            Campaign = "NO"
        Variations = []
        for variation in item_details['variations']:
            variation_id = variation['id']
            variation_inventory_id=variation['inventory_id']
            variation_price = variation['price']
            variation_available_quantity = variation['available_quantity']
            variation_sold_quantity = variation['sold_quantity']
            attributes = {}
            for attribute in variation.get('attribute_combinations', []):
                variation_name = attribute.get('name')
                variation_value_name = attribute.get('value_name')
                attributes[variation_name] = variation_value_name
            var = {
                'Variation_ID': variation_id,
                'Inventory_ID':variation_inventory_id,
                'Variation_Price': variation_price,
                'Variation_Available_Quantity': variation_available_quantity,
                'Variation_Sold_Quantity': variation_sold_quantity,
                'Variation_Attributes': attributes
            }
            Variations.append(var)
        filter = {"MLM": MLM}
        doc = {
            "MLM": MLM,
            "Sub_Category_ID": ID_Sub_Category,
            "Store_ID": Oficial_Store_ID,
            "Title_Products": Title,
            "Offer_Price": Offer_Price,
            "Original_Price": Original_Price,
            "Sold_Quantity": Sold_Quantity,
            "Available_Quantity": Available_Quantity,
            "Permalink": Permalink,
            "Campaign": Campaign,
            "Status_Product": Status_Product,
            "Status_Motive": Status_Product_Motivo,
            "Variations": Variations
        }

        products_collection.replace_one(filter, doc, upsert=True)

def obtain_partial_refund(Order_ID,headers):
    url=f"https://api.mercadolibre.com/orders/{Order_ID}"
    response = requests.get(url, headers=headers)
    data = response.json()
    return data
def consulta_detalle_venta(cnn, access_token,seller_id, start_date, end_date):
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    utc = pytz.timezone('UTC')
    # start_date = utc.localize(start_date)
    # end_date = utc.localize(end_date)
    num_days = (end_date - start_date).days
    if "Sale_Detail" in cnn.list_collection_names():
        collection = cnn["Sale_Detail"]
    else:
        collection = cnn.create_collection("Sale_Detail")
    collection = cnn["Sale_Detail"]
    headers = {"Authorization": f"Bearer {access_token}"}
    limit = 51
    a = 1
    for day in range(num_days):
        day_start = (start_date + timedelta(days=day)).strftime("%Y-%m-%dT00:00:00.000-00:00")
        day_end = (start_date + timedelta(days=day + 1)).strftime("%Y-%m-%dT23:00:00.000-00:00")
        #day_start = (start_date + timedelta(days=day)).strftime("%Y-%m-%dT22:00:00.000-00:00")
        #day_end = (start_date + timedelta(days=day + 1)).strftime("%Y-%m-%dT02:59:59.000-00:00")
        url_base = f"https://api.mercadolibre.com/orders/search?seller={seller_id}&order.date_created.from={day_start}&order.date_created.to={day_end}"
        # print(url_base)
        offset = 0
        more_results = True
        Buyer_Information = []
        while more_results:
            response = requests.get(url_base, params={"offset": offset, "limit": limit}, headers=headers)
            #print(a)
            a+=1
            if response.status_code == 200:
                orders = response.json()
                for order in orders['results']:
                    #total_quantity = sum(item['quantity'] for item in order['order_items'])
                    date_approved = order['payments'][0].get('date_approved', None) if order['payments'] else None
                    date_created = parse(order['date_created'])
                    #total_paid_amount=order['payments'][0].get('total_paid_amount', None) if order['payments'] else None
                    paid_amount = order['total_amount']
                    #shipping_cost=order['payments'][0].get('shipping_cost',None)if order['payments'] else 0
                    Shipping_ID = order['shipping']['id']
                    if paid_amount!=0 and Shipping_ID is not None:
                        buyer_information = get_shipping_cost(Shipping_ID, headers)
                        #print(a)
                        a+=1
                        #if shipping_cost==0:
                        shipping_cost = buyer_information.get("shipping_option", {}).get("cost", 0)
                        shipping_list_cost = buyer_information.get("shipping_option", {}).get("list_cost", 0)
                        shipping_cost_total = shipping_list_cost - shipping_cost
                        if buyer_information['receiver_address'] is not None:
                            Buyer_Name = buyer_information['receiver_address']['receiver_name']
                            Buyer_City = buyer_information['receiver_address']['city']['name']
                            Buyer_State = buyer_information['receiver_address']['state']['name']
                            #Buyer_CP = buyer_information['receiver_address']['zip_code']
                            #Buyer_Country = buyer_information['receiver_address']['country']['name']
                            #Buyer_Address = f"CP {Buyer_CP} / {buyer_information['receiver_address']['address_line']} - {buyer_information['receiver_address']['comment']} - {Buyer_City},{Buyer_State}"
                            Buyer_Information = {
                                'Shipping_ID': Shipping_ID,
                                'Buyer_Name': Buyer_Name,
                                #'Buyer_Address': Buyer_Address,
                                'Buyer_City': Buyer_City,
                                'Buyer_State': Buyer_State,
                                #'Buyer_CP': Buyer_CP,
                                #'Buyer_Country': Buyer_Country,
                            }
                        else:
                            buyer_information=[]
                    else:
                        Buyer_Information = []
                        shipping_cost_total=0
                    if date_approved is not None:
                        date_approved = parse(date_approved)
                    for item in order['order_items']:
                        MLM = item['item']['id']
                        Variation_ID = item['item']['variation_id']
                        Order_ID = order['id']
                        sale_fee = item.get('sale_fee', 0)
                        status=order['status']

                        if status=="partially_refunded":
                            partialorder=obtain_partial_refund(Order_ID,headers)
                            paid_amount = partialorder['paid_amount']
                            for parOrd in partialorder['order_items']:
                                quantity=parOrd["picked_quantity"]["value"]
                        else:
                            quantity = item['quantity']
                        total_sale_fee = sale_fee * quantity
                        if status!="paid" and status!="partially_refunded":
                            shipping_cost_total=paid_amount=total_sale_fee=0
                        full_unit_price=item['full_unit_price']
                        unit_price=item['unit_price']
                        if 0 < unit_price < full_unit_price:
                            Campaign = "SI"
                        else:
                            Campaign = "NO"
                        order_data = {
                            "Order_ID": Order_ID,
                            "Shipping_ID": Shipping_ID,
                            "Status":status ,
                            "MLM": MLM,
                            "Variation_ID": Variation_ID,
                            "Date_Created": date_created,
                            "Date_Approved": date_approved,
                            "Full_Unit_Price":full_unit_price,
                            "Unit_Price":unit_price,
                            "Campaign":Campaign,
                            "Quantity": quantity,
                            "Total_Paid_Amount": paid_amount,
                            "Sale_Fee": sale_fee,
                            "Total_Sale_Fee":total_sale_fee,
                            "Shipping_Cost":shipping_cost_total,
                            "Buyer_Information": Buyer_Information
                        }
                        collection.replace_one({"Order_ID": order_data["Order_ID"]}, order_data, upsert=True)
                        if Shipping_ID is not None:shipping_list.append(Shipping_ID)
                        #if Shipping_ID is not None:
                          #  aggregate_orders(collection, Shipping_ID,headers)
                        # print("Num:",a)
                        # a+=1

                if len(orders['results']) < limit:
                    more_results = False
                else:
                    offset += limit
            else:
                print(f"Error: {response.status_code}, {response.text}")
                more_results = False
    return shipping_list
def aggregate_orders(cnn,shipping_list):
    if "Sale_Detail_Package" in cnn.list_collection_names():
        new_collection = cnn["Sale_Detail_Package"]
    else:
        cnn.create_collection("Sale_Detail_Package")
        new_collection = cnn["Sale_Detail_Package"]
    collection = cnn["Sale_Detail"]
    aux=0
    unique_shipping = list(set(shipping_list))
    for shipping_id in unique_shipping:
        pipeline = [
            { "$match": { "Shipping_ID": shipping_id } },
            {
                "$group": {
                    "_id": "$Shipping_ID",  # Group by Shipping_ID
                    "Status": {
                        "$first": "$Status"  # Push the entire document into Orders array
                    },
                    "Shipping_ID": {
                        "$first": "$Shipping_ID"  # Push the entire document into Orders array
                    },
                    "Date_Created": {
                        "$first": "$Date_Created"  # Push the entire document into Orders array
                    },
                    "Date_Approved": {
                        "$first": "$Date_Approved"  # Push the entire document into Orders array
                    },
                    "Orders": {
                        "$push": "$$ROOT"  # Push the entire document into Orders array
                    },
                    "Total_Paid_Amount": {
                        "$sum": "$Total_Paid_Amount"  # Sum Total_Paid_Amount
                    },
                    "Total_Sale_Fee": {
                        "$sum": "$Total_Sale_Fee"  # Sum Sale_Fee
                    },
                    "Shipping_Cost": {
                        "$first": "$Shipping_Cost"  # Get the first occurrence of Shipping_Cost
                    },
                }
            },
            {
                "$addFields": {
                    "Final_Amount": {
                        "$subtract": [
                            "$Total_Paid_Amount",
                            {"$add": ["$Total_Sale_Fee", "$Shipping_Cost"]}
                        ]
                    }
                }
            },
            {
                "$project": {
                    "Shipping_ID":1,
                    "Status": 1,
                    "Date_Created": 1,
                    "Date_Approved": 1,
                    "Orders": 1,
                    "Total_Paid_Amount": 1,
                    "Total_Sale_Fee": 1,
                    "Shipping_Cost": 1,
                    "Final_Amount": 1
                }
            }
        ]
        grouped_orders = list(collection.aggregate(pipeline))
        aux += 1
        if grouped_orders:
            for order_data in grouped_orders:
                new_collection.replace_one({"Shipping_ID": shipping_id}, order_data, upsert=True)
    return aux
def aggregate_orders2(cnn):
    collection = cnn["Sale_Detail"]
    pipeline = [
        {
            "$sort": {"Shipping_ID": 1, "Date_Created": 1}  # Sort by Shipping_ID and Date_Created
        },
        {
            "$group": {
                "_id": "$Shipping_ID",  # Group by Shipping_ID
                "Status": {
                    "$first": "$Status"  # Push the entire document into Orders array
                },
                "Shipping_ID": {
                    "$first": "$Shipping_ID"  # Push the entire document into Orders array
                },
                "Date_Created": {
                    "$first": "$Date_Created"  # Push the entire document into Orders array
                },
                "Date_Approved": {
                    "$first": "$Date_Approved"  # Push the entire document into Orders array
                },
                "Orders": {
                    "$push": "$$ROOT"  # Push the entire document into Orders array
                },
                "Total_Paid_Amount": {
                    "$sum": "$Total_Paid_Amount"  # Sum Total_Paid_Amount
                },
                "Total_Sale_Fee": {
                    "$sum": "$Total_Sale_Fee"  # Sum Sale_Fee
                },
                "Shipping_Cost": {
                    "$first": "$Shipping_Cost"  # Get the first occurrence of Shipping_Cost
                },
            }
        },
        {
            "$addFields": {
                "Final_Amount": {
                    "$subtract": [
                        "$Total_Paid_Amount",
                        {"$add": ["$Total_Sale_Fee", "$Shipping_Cost"]}
                    ]
                }
            }
        },
        {
            "$project": {
                "Shipping_ID": 1,
                "Status": 1,
                "Date_Created": 1,
                "Date_Approved": 1,
                "Orders": 1,
                "Total_Paid_Amount": 1,
                "Total_Sale_Fee": 1,
                "Shipping_Cost": 1,
                "Final_Amount": 1
            }
        },
        {
            "$out": "Sale_Detail_Package"  # Output to a new collection
        }
    ]
    collection.aggregate(pipeline)

print("Mercadolibre API GAON")
#print("Consultando Categoria y Tienda")
#inserta_actualiza_tienda(cnn,access_token,seller_id)
#inserta_actualiza_categoria(cnn,access_token)
#print("Consultando productos")
#consulta_productos(cnn,access_token,seller_id)
start_date = "2022-12-31"  # or datetime(2023, 1, 1)
end_date = "2023-1-2"  # or datetime(2023, 1, 31)
try:
    print("Consultando ventas")
    shipping_list=consulta_detalle_venta(cnn, access_token,seller_id, start_date, end_date)
    #print(shipping_list)
except Exception as e:
    print(f'Error in consulta_detalle_venta: {e}')
finally:
    print("Numero de Shipping_Id:", len(shipping_list))
    print("Realizando Paquetes de Venta")
    num_orders=aggregate_orders(cnn,shipping_list)
    print("Ordenes Insertados",num_orders)
#consulta_shipping_details(cnn,access_token)
client.close()
