#This code obtains the products and sale data from Wallmart API and stores the information to MONGO DB
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
#Load .env file
load_dotenv()
#Obtain Mongo Connection String
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
client = MongoClient(f"{MongoOnlineConnection}")
cnn = client["GAON_WMT"]#Selects Mongo DB
#Obtain credentials
access_token =os.environ.get('WMT_ACCESS_TOKEN')
url_token=os.environ.get('url_token')
api_key=os.environ.get('api_key')
secret_key=os.environ.get('secret_key')
credentials = f"{api_key}:{secret_key}"
# Encode the credentials using Base64
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
correlation_id = str(uuid.uuid4())
headers = {#Authorization Header
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
# Function to check if it's time to activate the access token generator
def validate_token(last_activation_time):
    elapsed_time = time.time() - last_activation_time
    return elapsed_time >= 840  # 840 seconds = 14 minutes
#Function that generates a new token
def genera_token(url_token):
    headers = {#Authorization Header
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
    #Executes POST request
    response = requests.post(url_token, headers=headers, data=data)
    # Handle the API response
    if response.status_code == 200:
        # Successful response
        print("API request succeeded!")
        response_data = response.json()
        access_token = response_data.get('access_token')
        headers = {#Updates Headers
            "Authorization": f"Basic {encoded_credentials}",
            "WM_SEC.ACCESS_TOKEN": f'{access_token}',
            "Accept": "application/json",
            "WM_QOS.CORRELATION_ID": correlation_id,
            "WM_SVC.NAME": "Walmart Marketplace",
            "WM_MARKET": "mx"
        }
        last_activation_time = time.time()#Updates the time
        return headers,last_activation_time#returns headers and time
    else:
        print("Error Generating TOKEN")
        return 1,1
# Function that obtains product inventory
def obtain_inventory(headers,sku):
    #Request URL
    url=f"https://marketplace.walmartapis.com/v3/inventory?sku={sku}"
    response = requests.get(url, headers=headers)#exceutes GET request
    data = response.json()#converts response to JSON format
    quantity = data.get('quantity', {}).get('amount')#Obtains the product inventory
    return quantity #returns the product inventory
#Function for obtaining the products in Wallmart API
def consult_products(cnn,headers,url_token):
    #Creates or selects the collection from Mongo DB
    if "Products_WMT" in cnn.list_collection_names():
        products_collection = cnn["Products_WMT"]
    else:
        products_collection = cnn.create_collection("Products_WMT")
    #Request URL
    url = "https://marketplace.walmartapis.com/v3/items/?lifecycleStatus=ACTIVE&publishedStatus=PUBLISHED"
    all_products = []
    total_products = 0
    limit=50
    #Obtain the time for token validation
    last_activation_time = time.time()
    while url:
        response = requests.get(url, headers=headers)#Executes GET request
        data = response.json()#converts response to JSON format
        all_products.extend(data["ItemResponse"])
        total_products += len(data["ItemResponse"])#Obtains the total number of products
        if data['totalItems'] > total_products:#Verifies it there is more products
            offset = total_products
            #updates request url with offset and limit
            url = f"https://marketplace.walmartapis.com/v3/items/?lifecycleStatus=ACTIVE&publishedStatus=PUBLISHED&offset={offset}&limit={limit}"
        else:
            url = None
        if validate_token(last_activation_time):#checks if token is still valid
            headers, last_activation_time = genera_token(url_token)#Executes function to obtain a new token
    for item in all_products:#loops through all products
        #Obtains Product Information
        sku=item.get('sku')
        print("SKU: ", sku)
        upc=item.get('upc')
        gtin=item.get('gtin')
        Title = item.get("productName")
        Original_Price = item.get('price', {}).get('amount')
        Quantity =obtain_inventory(headers,sku)#Obtains the Inventory
        shelf = item.get('shelf')
        #Obtains the category and subcategory of each product
        if shelf is not None:
            categories = json.loads(shelf)
            ID_Category = categories[1] if len(categories) >= 2 else ""
            ID_Sub_Category = categories[-1] if len(categories) >= 1 else ""
        else:
            ID_Category=""
            ID_Sub_Category=""
        filter = {"UPC": upc}#Uses UPC as a primary key
        doc = {#Document structure of each product
            "UPC": upc,
            "SKU":sku,
            "GTIN":gtin,
            "Category_ID":ID_Category,
            "Sub_Category_ID": ID_Sub_Category,
            "Title_Products": Title,
            "Price": Original_Price,
            "Available_Quantity": Quantity
        }
        #Updates or inserts the product information to MONGO Db
        products_collection.replace_one(filter, doc, upsert=True)
        if validate_token(last_activation_time):#Checks if token is still valid
            headers,last_activation_time=genera_token(url_token)#generates a new token
#funtion for obtaining sale data by date
def consulta_detalle_venta(cnn,headers,url_token,start_date, end_date):
    #Converts the dates to pytz format
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    utc = pytz.timezone('UTC')
    num_days = (end_date - start_date).days #calculates num. of days
    #Selects or creates collection
    if "Sale_Detail_Package" in cnn.list_collection_names():
        collection = cnn["Sale_Detail_Package"]
    else:
        collection = cnn.create_collection("Sale_Detail_Package")
    collection = cnn["Sale_Detail_Package"]
    limit = 100
    a = 1
    last_activation_time = time.time()#Obtains time
    for day in range(num_days):#for loop for accesing all sale data by day
        day_start = (start_date + timedelta(days=day)).strftime("%Y-%m-%dT00:00:00.000-00:00")#start date format
        day_end = (start_date + timedelta(days=day + 1)).strftime("%Y-%m-%dT00:00:00.000-00:00")#end date format
        #Request URL
        url_base = f"https://marketplace.walmartapis.com/v3/orders?createdStartDate={day_start}&createdEndDate={day_end}"
        offset = 0
        more_results = True
        while more_results:
            #exceutes GET request
            response = requests.get(url_base, params={"offset": offset, "limit": limit}, headers=headers)
            a+=1
            if response.status_code == 200:
                orders = response.json()
                #Obtains Sale Information
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
                    #Obtains Buyer information
                    Buyer_Name = order['shippingInfo']['postalAddress']['name']
                    Buyer_City = order['shippingInfo']['postalAddress']['city']
                    Buyer_State = order['shippingInfo']['postalAddress']['state']
                    Buyer_Information = {#Table format for Buyer_Information 
                        'Order_Id': order_id,
                        'Buyer_Name': Buyer_Name,
                        'Buyer_City': Buyer_City,
                        'Buyer_State': Buyer_State,
                    }
                    if date_created is not None:
                        date_created = parse(date_created)
                    for item in order['orderLines']:
                        #Obtains Product Information
                        UPC = item['item']['upc']
                        SKU = item['item']['sku']
                        product_name=item['item']['productName']
                        unit_price = float(item['item']['unitPrice']['amount'])
                        quantity = float(item['orderLineQuantity']['amount'])
                        sale_fee = float(item['item']['commission']['amount'])
                        total_product_paid = unit_price * quantity
                        total_product_sale_fee=sale_fee * quantity              
                        total_sale_fee += total_product_sale_fee
                        total_paid += total_product_paid
                        products_list = {#Table format for each product
                            "UPC": UPC,
                            "SKU": SKU,
                            "Product":product_name ,
                            "Unit_Price": unit_price,
                            "Quantity": quantity,
                            "Total_Paid":total_product_paid,
                            "Sale_Fee": sale_fee,
                            "Total_Sale_Fee": total_product_sale_fee,
                        }
                        order_list.append(products_list)
                    final_amount=total_paid-total_sale_fee-total_shipping
                    order_data = {#table format for the whole sale_detail
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
                    #Updates or Inserts the sale data to Mongo DB
                    collection.replace_one({"Order_ID": order_data["Order_ID"]}, order_data, upsert=True)
                    if validate_token(last_activation_time):#Checks if token is valid
                        headers, last_activation_time = genera_token(url_token)#generates a new token
                if len(orders['order']) < limit:#Checks if there is more sale data
                    more_results = False
                else:
                    offset += limit
            else:
                print(f"Error: {response.status_code}, {response.text}")
                more_results = False
    return shipping_list

start_date = "2023-1-1"  # Start Date
end_date = "2023-7-19"  # End Date
#Consults Products
consult_products(cnn,headers,url_token)
#Consults the sale data using a start and end date
consulta_detalle_venta(cnn,headers,url_token,start_date,end_date)