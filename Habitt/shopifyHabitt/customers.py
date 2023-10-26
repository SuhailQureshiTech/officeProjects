from dateutil import parser
from datetime import datetime,date
import sys
from http import client
import pandas as pds
import numpy as np
# from datalab.context import Context
# from google.cloud import storage
import os
import json
import requests
from pandas.io.json import json_normalize
from dateutil import parser
from datetime import datetime,date
from datetime import timedelta
import string,random


# API KEY
api_key = "1749a3c314889e82ec9704a20e95ddc8"
# Access Token
token = "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"

password=[]
email=[]

file = r"D:\\TEMP\\LahoreCustomerData.csv"
customer_df = pds.read_csv(file, encoding= 'unicode_escape')
# print(file)
url = f"https://restoreonline.myshopify.com/admin/api/2022-01/customers.json"

headers = {
  'X-Shopify-Access-Token': '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',
  'Content-Type': 'application/json',
  'Cookie': '_master_udr=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaEpJaWs0TkRnMVlqVTNPUzAyTUdVeUxUUmlabU10T1dZNU9TMWlZbVE0TmpJd1pEUXpOVElHT2daRlJnPT0iLCJleHAiOiIyMDI0LTA3LTIyVDA5OjMyOjU2Ljc0M1oiLCJwdXIiOiJjb29raWUuX21hc3Rlcl91ZHIifX0%3D--02a70f3d3e49c5ed101b1ba9f1cddc7d3fca9be5; _secure_admin_session_id=3bee420534f8a94f4bae452d83ca6e21; _secure_admin_session_id_csrf=3bee420534f8a94f4bae452d83ca6e21; _shopify_y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; _y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; identity-state=BAhbBkkiJWE0NTE1OGQ1MTY0ZmZlMGIyYjEzZDA0MzVlOWYyNjYxBjoGRUY%3D--2eb44354a0d3a3516888c46443349a5cad92eda3; identity-state-a45158d5164ffe0b2b13d0435e9f2661=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY2MzkxNjcyMS44NDg2NTM2SSIKbm9uY2UGOwBUSSIlYjFkOTUyNDA5YzU2MjlmYzZiMjc4N2VkMWJkMzNiZmYGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--b7baa7d8756d984143bc4a1e9aa02e7b18a4284a; secure_customer_sig='
}


customer_df = customer_df[customer_df['Email'].notnull()]
customer_df = customer_df.fillna('')
# customer_df = customer_df.drop_duplicates(subset=['Email'], keep='first')
customer_df['Phone'] = customer_df['Phone'].astype(str)
customer_df['Phone'] = customer_df['Phone'].apply(lambda x: x.split('.')[0])
# print(customer_df)
for index,row in customer_df.iterrows():
    pwd = "restor"+"".join(random.sample(string.digits, 6))
    #print(row['Phone'])
    payload = json.dumps({
    "customer": {
        "first_name": row['First Name'],
        "last_name": row['Last Name'],
        "email":  row['Email'],
        "phone": str(row['Phone']),
        "password": pwd,
        "password_confirmation": pwd,
        "verified_email": True,
        "tags": str(row["Tags"]),
        "note": str(row["Note"]),
        "addresses": [
        {
            "address1": row['Address 1'],
            "city": row['City'],
            "province": str(row['Province']),
            "zip": str(row['Zip']),
            "last_name": row['Last Name'],
            "first_name": row['First Name'],
            "country": str(row['Country'])
        }
        ]
    }
    })
    print(payload)
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)
    customer = json.loads(payload)
    password.append(customer['customer']['password'])
    email.append(customer['customer']['email'])

customer_df['password'] = password
customer_df.to_csv(r"D:\\TEMP\\\LahoreCustomerUpdated.csv", index=False)
