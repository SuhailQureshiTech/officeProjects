import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from airflow.utils.email import send_email
from dateutil import parser
from datetime import datetime,date
import sys
from http import client
import pandas as pds
import numpy as np
from datalab.context import Context
from google.cloud import storage
import os
from google.oauth2 import service_account
from google.cloud import bigquery
import requests
import json
import requests
from pandas.io.json import json_normalize 
from dateutil import parser
from datetime import datetime,date
from datetime import timedelta

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "SHOPIFY ORDERS SALESFLO DAG has executed successfully."
    subject = f"SHOPIFY ORDERS SALESFLO DAG has completed"
    send_email(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)

default_args = {
        'owner': 'admin',    
        'start_date': datetime(2022, 9, 30),
        # 'end_date': datetime(),
        'depends_on_past': False,
        'email_on_failure': True,
        'email': ['shehzad.lalani@iblgrp.com'],
        'on_success_callback':success_function,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        }


def post_shopify_orders_to_salesflo():

    # API KEY
    api_key = "1749a3c314889e82ec9704a20e95ddc8"
    # Access Token
    token = "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"
    prod_ids=[]
    cust_ids=[]
    prod_barcodes={}
    prod_skucodes={}
    cust_sapcodes={}

    # file1 = r"C:\\Users\\Shehzad.Lalani\\Downloads\\Copy of SAP vs Salesflo Product List (00000002).xlsx"
    # file2 = r"C:\\Users\\Shehzad.Lalani\\Downloads\\Copy of Korangi Branch UniverseStores_ (00000002).xlsx"

    # with pds.ExcelFile(file1) as reader:
    #     salesflo_df = pds.read_excel(reader, sheet_name='Sheet1')
    # with pds.ExcelFile(file2) as reader:
    #     branches_df = pds.read_excel(reader, sheet_name='UniverseStores')


    url = f"https://{api_key}:{token}@restoreonline.myshopify.com/admin/api/2022-01/orders.json"

    payload = ""
    headers = {
    'Cookie': '_master_udr=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaEpJaWs0TkRnMVlqVTNPUzAyTUdVeUxUUmlabU10T1dZNU9TMWlZbVE0TmpJd1pEUXpOVElHT2daRlJnPT0iLCJleHAiOiIyMDI0LTA3LTIyVDA5OjMyOjU2Ljc0M1oiLCJwdXIiOiJjb29raWUuX21hc3Rlcl91ZHIifX0%3D--02a70f3d3e49c5ed101b1ba9f1cddc7d3fca9be5; _secure_admin_session_id=3bee420534f8a94f4bae452d83ca6e21; _secure_admin_session_id_csrf=3bee420534f8a94f4bae452d83ca6e21; _shopify_y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; _y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; cart_sig=; secure_customer_sig='
    }


    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.content
    orders_json = json.loads(data)
    # print(orders_json['orders'][0]['line_items'])
    df_orders_shopify = pds.DataFrame.from_dict(orders_json['orders'])
    #df_products = df_orders_shopify['line_items'][0][1]['product_id']

    # print(df_products)
    for i in range(0,len(df_orders_shopify['line_items'])):
        for j in range(0, len(df_orders_shopify['line_items'][i])):
            prod_ids.append(df_orders_shopify['line_items'][i][j]['product_id'])
    #print(prod_ids)

    for a in range(0,len(prod_ids)):
        url_prod = f"https://{api_key}:{token}@restoreonline.myshopify.com/admin/api/2022-01/products/{prod_ids[a]}.json"
        payload_prod =""
        headers_prod = {
            'Content-Type': 'application/json',
            'Cookie': '_master_udr=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaEpJaWs0TkRnMVlqVTNPUzAyTUdVeUxUUmlabU10T1dZNU9TMWlZbVE0TmpJd1pEUXpOVElHT2daRlJnPT0iLCJleHAiOiIyMDI0LTA3LTIyVDA5OjMyOjU2Ljc0M1oiLCJwdXIiOiJjb29raWUuX21hc3Rlcl91ZHIifX0%3D--02a70f3d3e49c5ed101b1ba9f1cddc7d3fca9be5; _secure_admin_session_id=3bee420534f8a94f4bae452d83ca6e21; _secure_admin_session_id_csrf=3bee420534f8a94f4bae452d83ca6e21; _landing_page=%2F; _orig_referrer=; _s=afe68edd-3902-4a10-b2fb-09725cc7da69; _shopify_s=afe68edd-3902-4a10-b2fb-09725cc7da69; _shopify_y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; _y=2d72b54f-1939-4fde-89d3-7f4e7f20e359; cart=f3eb468c2860b7280b73c773081878cc; cart_sig=7bbc5f066eacf57aad2ad40c36b54299; cart_ts=1658483398; cart_ver=gcp-us-central1%3A1; identity-state=BAhbCkkiJTgwNmQwNmVhNmViYjQzYWIwYjkzMDU1Njk4NDAxODkzBjoGRUZJIiVkMzU2YTYzMjQ5ODNhMzU0YjIxNTVmMTdlYmJjMDRiMQY7AEZJIiVmNjdkNzBmYjA3NzBlOTYxMGM4NGM4OTA3ZTAyNmMwNAY7AEZJIiU5MTMxNjkzMTNhMTQxNTcxZjI5MDFiMWVkMGQ3MDZmZQY7AEZJIiVjYjI0YWQ1YWQyMjU1ZmRmMWY4YWMxZWQ3MmYwOWY3NAY7AEY%3D--e01ed4f2dca0d304ba6bf74674dba99e0794252e; identity-state-806d06ea6ebb43ab0b93055698401893=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTE3My45MTU5MzMxSSIKbm9uY2UGOwBUSSIlMzMxNjZlYjgwMzQzZjY3NmJiYzlhNTIyZWIwOGRlZDIGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--fce94b4576233e803f9d8cfc1e4862b5ab1c305f; identity-state-913169313a141571f2901b1ed0d706fe=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTUzNC40NzE3NzM0SSIKbm9uY2UGOwBUSSIlMDkyNWJlMTBmNzAyYmI5NjdiNTA3ZWZiYTNhZWExMjUGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--96a369bfd784ddb76b3a783b4d6717904755e6e1; identity-state-cb24ad5ad2255fdf1f8ac1ed72f09f74=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTgwMy42MjcwMDA4SSIKbm9uY2UGOwBUSSIlMjg0YmQyNTAxZDI2ODcwNGIxNjM2ZTJlZTU0NDZkMTMGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--420e6d7d9d90a196c3061494f16eb0b07838a709; identity-state-d356a6324983a354b2155f17ebbc04b1=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTQ3NS4wMDk1MTk2SSIKbm9uY2UGOwBUSSIlNmE2MWI0OTY5ZGE2ZjhkYjBiOTIzY2IzMzYwMDgxNmIGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--326bef54ec84536cbbdcdaedaac4df51c02d764b; identity-state-f67d70fb0770e9610c84c8907e026c04=BAh7DEkiDnJldHVybi10bwY6BkVUSSI5aHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9sb2dpbgY7AFRJIhFyZWRpcmVjdC11cmkGOwBUSSJFaHR0cHM6Ly9yZXN0b3Jlb25saW5lLm15c2hvcGlmeS5jb20vYWRtaW4vYXV0aC9pZGVudGl0eS9jYWxsYmFjawY7AFRJIhBzZXNzaW9uLWtleQY7AFQ6DGFjY291bnRJIg9jcmVhdGVkLWF0BjsAVGYXMTY1ODg5OTUyMy40MzQ4NzcySSIKbm9uY2UGOwBUSSIlYjQzODkxMWI4ZGU5NmNhZjVlMDVmYzJiODk1NmMxOTAGOwBGSSIKc2NvcGUGOwBUWw9JIgplbWFpbAY7AFRJIjdodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL2Rlc3RpbmF0aW9ucy5yZWFkb25seQY7AFRJIgtvcGVuaWQGOwBUSSIMcHJvZmlsZQY7AFRJIk5odHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3BhcnRuZXJzLmNvbGxhYm9yYXRvci1yZWxhdGlvbnNoaXBzLnJlYWRvbmx5BjsAVEkiMGh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvYmFua2luZy5tYW5hZ2UGOwBUSSJCaHR0cHM6Ly9hcGkuc2hvcGlmeS5jb20vYXV0aC9tZXJjaGFudC1zZXR1cC1kYXNoYm9hcmQuZ3JhcGhxbAY7AFRJIjxodHRwczovL2FwaS5zaG9waWZ5LmNvbS9hdXRoL3Nob3BpZnktY2hhdC5hZG1pbi5ncmFwaHFsBjsAVEkiN2h0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvZmxvdy53b3JrZmxvd3MubWFuYWdlBjsAVEkiPmh0dHBzOi8vYXBpLnNob3BpZnkuY29tL2F1dGgvb3JnYW5pemF0aW9uLWlkZW50aXR5Lm1hbmFnZQY7AFRJIg9jb25maWcta2V5BjsAVEkiDGRlZmF1bHQGOwBU--aeb8b045c1784bf84f441648de4f04ea85bfb227; localization=PK; secure_customer_sig='
        }
        response_prod = requests.request("GET", url_prod, headers=headers_prod, data=payload_prod)
        data_prod = response_prod.content
        prods_json = json.loads(data_prod)
        prod_barcodes[str(prods_json['product']['id']) + 'barcode'] = prods_json['product']['variants'][0]['barcode']

    y= {}
    z= {}

    salesflo_skucode={}
    salesflo_skucode_z={}

    line_products=[]
    line_quantity=[]
    ord=[]
    sku=[]
    for i in range(len(orders_json['orders'])):
        #print(orders_json['orders'][i]['line_items'])
        for j in range(len(orders_json['orders'][i]['line_items'])):
            y['barcode']=prod_barcodes[str(orders_json['orders'][i]['line_items'][j]['product_id'])+ 'barcode']
            orders_json['orders'][i]['line_items'][j].update( y )

    json.dumps(orders_json)
    #orders_json['orders'][0]['customer']['tags'] = 'SAP9090' ## need to remove this line

    for i in range(len(orders_json['orders'])):
        z[orders_json['orders'][i]['order_number']]=[]

    for i in range(len(orders_json['orders'])):
        for j in range(len(orders_json['orders'][i]['line_items'])):
            z[orders_json['orders'][i]['order_number']].append(orders_json['orders'][i]['line_items'][j]['barcode']) 

    data = pds.DataFrame(orders_json['orders'])
    #print(data['customer'])
    data_df = pds.DataFrame(data['customer'].values.tolist())
    data_df.columns = 'customer.'+ data_df.columns

    result = pds.concat([data,data_df],axis=1)


    # for i in range(len(result['line_items'])):
    #    for j in range(len(result['line_items'][i])):
    #        final_df = pd.merge(result['line_items'][i][j]['barcode'],line_df,left_on='contact_number',right_on='contact_number', how='inner')

    for i in range(len(orders_json['orders'])):
        for j in range(len(result['line_items'][i])):
            line_products.append(result['line_items'][i][j]['barcode'])
            line_quantity.append(result['line_items'][i][j]['quantity'])
            sku.append(result['line_items'][i][j]['sku'])
            ord.append(orders_json['orders'][i]['order_number'])

            #ord_df = pds.DataFrame(result['line_items'][i]['order_number'])
            
    data_tuples = list(zip(ord,line_products,line_quantity,sku))

    prod_df=pds.DataFrame(data_tuples, columns=['order_no','barcode','quantity','SKUDescription'])
    final_df = pds.merge(result, prod_df, left_on='order_number', right_on='order_no', how='inner')
    final_df['customer.last_name'].fillna("", inplace = True)
    final_df['StoreName'] = final_df['customer.first_name'] + final_df['customer.last_name']
    final_df["barcode"] = final_df["barcode"].astype(int)
    final_df['Distributor'] = final_df['customer.note']
    final_df['Store'] = final_df['customer.tags']

    # df = pds.merge(final_df,salesflo_df,left_on='barcode',right_on='SAP Material Code', how='inner')
    # df = pds.merge(df, branches_df, left_on='StoreName',right_on='Store Name', how='inner')

    df = final_df[['order_number','Distributor','Store','barcode','quantity']]
    df.insert(5,'SKUQTYCartons',0)
    df.insert(6,'SKUQTYVirtualPack',0)
    df=df.drop_duplicates()
    df.rename(columns = {'order_number':'CompanyOrderNumber','Distributor':'SalesfloDistributorCode','Store':'SalesfloStoreCode','barcode':'SalesfloSKUCode','quantity':'SKUQTYUnits'}, inplace = True)
    df['CompanyOrderNumber']=df['CompanyOrderNumber'].astype(str)
    df['SalesfloSKUCode']=df['SalesfloSKUCode'].astype(str)
    df['SalesfloDistributorCode'] = '63008044'
    # df['SalesfloSKUCode'].iat[0] = '1013000117'
    # df['SalesfloSKUCode'].iat[1] = '1013000118'
    # df['SalesfloSKUCode'].iat[2] = '1013000119'

    json_final = (df.groupby(['CompanyOrderNumber','SalesfloDistributorCode','SalesfloStoreCode'])
        .apply(lambda x: x[['SalesfloSKUCode','SKUQTYUnits','SKUQTYCartons','SKUQTYVirtualPack']].to_dict('records'))
        .reset_index()
        .rename(columns={0:'OrderDetails'})
        .to_json(orient='records'))
    #json='{"CompanyOrderNumber":"b09b7869","SalesfloDistributorCode":"61008044","SalesfloStoreCode":"2000069393","OrderDetails":[{"SalesfloSKUCode":"6119000006","SKUQTYUnits":10,"SKUQTYCartons":0,"SKUQTYVirtualPack":0},{"SalesfloSKUCode":"6119000012","SKUQTYUnits":20,"SKUQTYCartons":0,"SKUQTYVirtualPack":0},{"SalesfloSKUCode":"6119000052","SKUQTYUnits":30,"SKUQTYCartons":0,"SKUQTYVirtualPack":0},{"SalesfloSKUCode":"6119000053","SKUQTYUnits":40,"SKUQTYCartons":0,"SKUQTYVirtualPack":0}]}'
    item_dict = json.loads(json_final)
    #print(item_dict)
    url_post = "https://integration.salesflo.com/shopify-orders?AccessKey=MTYyMzM1NTM5OV9kbWhVcnBUbEw0bldIYUdvVjBXRVZmVEhIWmgzd041eFhKWFE1V2xzRU9VPQ==&SecretKey=ZUVHWjAwQXprMmtUUXhMa0pvVXRxUHVLRUlaVys4VXhrODdQaG5VSjVsWT0=&CompanyCode=ibloperations"
    headers_post = {
                'AccessKey': 'MTYyMzM0NTQ1NF9lRUdaMDBBemsya1RReExrSm9VdHFQdUtFSVpXKzhVeGs4N1BoblVKNWxZPQ==',
                'SecretKey': 'ZUVHWjAwQXprMmtUUXhMa0pvVXRxUHVLRUlaVys4VXhrODdQaG5VSjVsWT0=',
                'CompanyCode': 'ibloperations',
                'X-API-KEY': 'vOHr2X7EDe6QM3HYj6nj42zwhw1jTpFE1Tc9aGKr',
                'Content-Type': 'application/x-www-form-urlencoded'            
                }


    for i in range(len(item_dict)):
        payload='type=CreateOrder&OrderData='+str(item_dict[i]).replace("'","\"")
        print(payload)
        response_post = requests.request("POST", url_post, headers=headers_post, data=payload)
        print(response_post.text)

SHOPIFY_ORDERS_SALESFLO_POST = DAG(
    dag_id='SHOPIFY_ORDERS_SALESFLO_POST',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    #schedule_interval='*/30 * * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='SHOPIFY_ORDERS_SALESFLO_POST',
)
 
t0 = PythonOperator(
    task_id="SHOPIFY_ORDERS_POST_TO_SALESFLO",
    python_callable=post_shopify_orders_to_salesflo,
    dag=SHOPIFY_ORDERS_SALESFLO_POST
)








    
    








