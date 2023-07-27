import base64
import requests
import uuid
from dotenv import load_dotenv, set_key
import os
load_dotenv()
#Obtiene URL request,api key y secret key
url = os.environ.get('url_token')
api_key=os.environ.get('api_key')
secret_key=os.environ.get('secret_key')
# Encode the credentials using Base64
credentials = f"{api_key}:{secret_key}"
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
# Generate a random UUID for WM_QOS.CORRELATION_ID
#print(encoded_credentials)
correlation_id = str(uuid.uuid4())

#print(correlation_id)
headers = {
    "Authorization": f"Basic {encoded_credentials}",
    "Accept":"application/json",
    "WM_QOS.CORRELATION_ID": correlation_id,
    "WM_SVC.NAME":"Walmart Marketplace",
    "WM_MARKET":"mx"
}
# Set the request body parameters
data = {
    "grant_type": "client_credentials",
    "content-type": "application/x-www-form-urlencoded"
}
response = requests.post(url, headers=headers, data=data)
# Handle the API response
if response.status_code == 200:
    # Successful response
    print("API request succeeded!")
    response_data = response.json()
    access_token = response_data.get('access_token')
    token_type = response_data.get('token_type')
    expires = response_data.get('expires_in')
    print("Access Token:", access_token)
    print(access_token)
    print("Token Type:", token_type)
    print("Expires In:", expires)
    set_key('.env', 'WMT_ACCESS_TOKEN', f'{access_token}')
    #load_dotenv()
    #print(os.getenv('WMT_ACCESS_TOKEN'))
else:
    # Error response
    print("API request failed!")
    print("Status code:", response.status_code)
    print("Error message:", response.text)

