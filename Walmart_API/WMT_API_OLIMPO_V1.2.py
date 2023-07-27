import datetime
import json
from datetime import datetime, timedelta
import pytz
from pymongo import MongoClient
from dateutil.parser import parse
import base64
import requests
import uuid
import time
from dotenv import load_dotenv, set_key
import os
load_dotenv()
#Obtiene el enlace de conexion a Mongo DB,ya sea local o al cluster
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
MongoLocalHost=os.environ.get('MongoLocalConnectionHost')
MongoLocalServer=os.environ.get('MongoLocalConnectionServer')
#Elecion de Base de Datos, Local o Cluster
client = MongoClient(f"{MongoOnlineConnection}")
#client=MongoClient(f'{MongoLocalHost}', int(MongoLocalServer))
cnn = client["GAON_WMT"]
#Obtiene las credentiales
access_token =os.environ.get('WMT_ACCESS_TOKEN')
url_token=os.environ.get('url_token')
api_key=os.environ.get('api_key')
secret_key=os.environ.get('secret_key')
credentials = f"{api_key}:{secret_key}"
# Encode the credentials using Base64
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
correlation_id = str(uuid.uuid4())
headers = {
    "Authorization": f"Basic {encoded_credentials}",
    "WM_SEC.ACCESS_TOKEN": f'{access_token}',
    "Accept": "application/json",
    "WM_QOS.CORRELATION_ID": correlation_id,
    "WM_SVC.NAME": "Walmart Marketplace",
    "WM_MARKET": "mx"
}
# Set the request body parameters
data = {
    "grant_type": "client_credentials",
    "content-type": "application/x-www-form-urlencoded"
}
shipping_list=[]
# Function to check if it's time to activate the code generator
def checar_vigencia_token(last_activation_time):
    elapsed_time = time.time() - last_activation_time
    return elapsed_time >= 840  # 840 seconds = 14 minutes
def consulta_productos(cnn,headers,url_token):
    if "Products_WMT" in cnn.list_collection_names():
        products_collection = cnn["Products_WMT"]
    else:
        products_collection = cnn.create_collection("Products_WMT")
    url = "https://marketplace.walmartapis.com/v3/items/?lifecycleStatus=ACTIVE&publishedStatus=PUBLISHED"
    #response = requests.get(url, headers=headers, data=data)
    all_products = []
    total_products = 0
    limit=50
    last_activation_time = time.time()
    while url:
        response = requests.get(url, headers=headers)
        data = response.json()
        all_products.extend(data["ItemResponse"])
        total_products += len(data["ItemResponse"])
        if data['totalItems'] > total_products:
            offset = total_products
            url = f"https://marketplace.walmartapis.com/v3/items/?lifecycleStatus=ACTIVE&publishedStatus=PUBLISHED&offset={offset}&limit={limit}"
        else:
            url = None
        if checar_vigencia_token(last_activation_time):
            headers, last_activation_time = genera_token(url_token)
    for item in all_products:
        sku=item.get('sku')
        print("SKU: ", sku)
        upc=item.get('upc')
        gtin=item.get('gtin')
        Title = item.get("productName")
        Original_Price = item.get('price', {}).get('amount')
        Quantity =obtain_inventory(headers,sku)
        shelf = item.get('shelf')
        if shelf is not None:
            categories = json.loads(shelf)
            ID_Category = categories[1] if len(categories) >= 2 else ""
            ID_Sub_Category = categories[-1] if len(categories) >= 1 else ""
        else:
            ID_Category=""
            ID_Sub_Category=""
        filter = {"UPC": upc}
        doc = {
            "UPC": upc,
            "SKU":sku,
            "GTIN":gtin,
            "Category_ID":ID_Category,
            "Sub_Category_ID": ID_Sub_Category,
            "Title_Products": Title,
            "Price": Original_Price,
            "Available_Quantity": Quantity
        }
        products_collection.replace_one(filter, doc, upsert=True)
        if checar_vigencia_token(last_activation_time):
            headers,last_activation_time=genera_token(url_token)


def consulta_detalle_venta(cnn,headers,url_token,start_date, end_date):
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    utc = pytz.timezone('UTC')
    # start_date = utc.localize(start_date)
    # end_date = utc.localize(end_date)
    num_days = (end_date - start_date).days
    if "Sale_Detail_Package" in cnn.list_collection_names():
        collection = cnn["Sale_Detail_Package"]
    else:
        collection = cnn.create_collection("Sale_Detail_Package")
    collection = cnn["Sale_Detail_Package"]
    limit = 100
    a = 1
    last_activation_time = time.time()
    for day in range(num_days):
        day_start = (start_date + timedelta(days=day)).strftime("%Y-%m-%dT00:00:00.000-00:00")
        day_end = (start_date + timedelta(days=day + 1)).strftime("%Y-%m-%dT00:00:00.000-00:00")
        url_base = f"https://marketplace.walmartapis.com/v3/orders?createdStartDate={day_start}&createdEndDate={day_end}"
        offset = 0
        more_results = True
        while more_results:
            response = requests.get(url_base, params={"offset": offset, "limit": limit}, headers=headers)
            #print(a)
            a+=1
            if response.status_code == 200:
                orders = response.json()
                for order in orders['order']:
                    order_list = []
                    total_sale_fee=0
                    total_shipping=0
                    total_paid=0
                    order_id=order.get('purchaseOrderId')
                    date_created= order.get('orderDate')
                    order_total=float(order['orderTotal']['amount'])
                    for shipping in order['shipments']:
                        shipping_status=shipping.get('status')
                    Buyer_Name = order['shippingInfo']['postalAddress']['name']
                    Buyer_City = order['shippingInfo']['postalAddress']['city']
                    Buyer_State = order['shippingInfo']['postalAddress']['state']
                    Buyer_Information = {
                        'Order_Id': order_id,
                        'Buyer_Name': Buyer_Name,
                        'Buyer_City': Buyer_City,
                        'Buyer_State': Buyer_State,
                    }
                    if date_created is not None:
                        date_created = parse(date_created)
                    for item in order['orderLines']:
                        UPC = item['item']['upc']
                        SKU = item['item']['sku']
                        product_name=item['item']['productName']
                        unit_price = float(item['item']['unitPrice']['amount'])
                        quantity = float(item['orderLineQuantity']['amount'])
                        sale_fee = float(item['item']['commission']['amount'])
                        total_product_paid = unit_price * quantity
                        total_product_sale_fee=sale_fee * quantity


                        """
                        for charge in item['charges']:
                            charge_type = charge['chargeType']
                            charge_name = charge['chargeName']
                            print(f"Charge Type: {charge_type}")
                            print(f"Charge Name: {charge_name}")
                            # Check if the charge has chargeAmount
                            if 'chargeAmount' in charge:
                                charge_amount = charge['chargeAmount']
                                currency = charge_amount['currency']
                                amount = charge_amount['amount']
                                print(f"Currency: {currency}")
                                print(f"Amount: {amount}")

                            # Check if the charge has tax information
                            if 'tax' in charge:
                                taxes = charge['tax']
                                for tax in taxes:
                                    tax_name = tax['taxName']
                                    tax_amount = tax['taxAmount']
                                    tax_currency = tax_amount['currency']
                                    tax_value = tax_amount['amount']
                                    print(f"Tax Name: {tax_name}")
                                    print(f"Tax Currency: {tax_currency}")
                                    print(f"Tax Amount: {tax_value}")
                        """
                        #total_product_shipping=shipping-shipping_discount+shipping_tax
                        #total_shipping += total_product_shipping
                        total_sale_fee += total_product_sale_fee
                        total_paid += total_product_paid

                        #total=total_product_paid-total_product_sale_fee-total_product_shipping
                        products_list = {
                            "UPC": UPC,
                            "SKU": SKU,
                            "Product":product_name ,
                            "Unit_Price": unit_price,
                            "Quantity": quantity,
                            "Total_Paid":total_product_paid,
                            "Sale_Fee": sale_fee,
                            "Total_Sale_Fee": total_product_sale_fee,
                            #"Shipping_Cost":total_product_shipping,
                            #"Total":total
                        }
                        order_list.append(products_list)
                    final_amount=total_paid-total_sale_fee-total_shipping
                    order_data = {
                        "Order_ID": order_id,
                        "Date_Created": date_created,
                        "Shipping_Status":shipping_status,
                        "Buyer_Information": Buyer_Information,
                        "Orders":order_list,
                        "Total_Paid": order_total,
                        "Total_Product_Paid":total_paid,
                        "Total_Sale_Fee": total_sale_fee,
                        "Total_Shipping_Cost": total_shipping,
                        "Final_Amount": final_amount
                    }
                    collection.replace_one({"Order_ID": order_data["Order_ID"]}, order_data, upsert=True)
                    if checar_vigencia_token(last_activation_time):
                        headers, last_activation_time = genera_token(url_token)
                if len(orders['order']) < limit:
                    more_results = False
                else:
                    offset += limit
            else:
                print(f"Error: {response.status_code}, {response.text}")
                more_results = False
    return shipping_list

def genera_token(url_token):
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Accept": "application/json",
        "WM_QOS.CORRELATION_ID": correlation_id,
        "WM_SVC.NAME": "Walmart Marketplace",
        "WM_MARKET": "mx"
    }
    # Set the request body parameters
    data = {
        "grant_type": "client_credentials",
        "content-type": "application/x-www-form-urlencoded"
    }
    response = requests.post(url_token, headers=headers, data=data)
    # Handle the API response
    if response.status_code == 200:
        # Successful response
        print("API request succeeded!")
        response_data = response.json()
        # print("Response:", response_data)
        access_token = response_data.get('access_token')
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "WM_SEC.ACCESS_TOKEN": f'{access_token}',
            "Accept": "application/json",
            "WM_QOS.CORRELATION_ID": correlation_id,
            "WM_SVC.NAME": "Walmart Marketplace",
            "WM_MARKET": "mx"
        }
        last_activation_time = time.time()
        return headers,last_activation_time
    else:
        print("Error Generando Token")
        return 1,1

def obtain_inventory(headers,sku):
    url=f"https://marketplace.walmartapis.com/v3/inventory?sku={sku}"
    response = requests.get(url, headers=headers)
    data = response.json()
    quantity = data.get('quantity', {}).get('amount')
    return quantity
start_date = "2023-1-1"  # or datetime(2023, 1, 1)
end_date = "2023-7-19"  # or datetime(2023, 1, 31)
#consulta_productos(cnn,headers,url_token)
consulta_detalle_venta(cnn,headers,url_token,start_date,end_date)