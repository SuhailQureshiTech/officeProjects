import time
import requests
import regex as re
from pandas.io.json import json_normalize
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2 as pg
import psycopg2.extras
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from dateutil import parser
from datetime import datetime,date
from datalab.context import Context
from airflow.utils.email import send_email
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.contrib.operators import gcs_to_bq
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
#from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
import pyodbc
from hdbcli import dbapi
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import xlsxwriter

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Naheed DAG has executed successfully."
    subject = f" Naheed DAG has completed"
    send_email(to=['shehzad.lalani@iblgrp.com','muhammad.aamir@iblgrp.com'], subject=subject, html_content=msg)


default_args = {
        'owner': 'admin',    
        'start_date': datetime(2022, 5, 21),
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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
print(today)
curr_date = today.strftime("%d-%b-%Y")


def delete_Naheed_Data():
    #conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    cursor = conn.cursor()
    delete_sql = f"""DELETE FROM  "DW"."IBL_SCRAP_MART" WHERE company_code='Naheed'"""
    cursor.execute(delete_sql)
    conn.commit()





def extract_naheed_data_scrap_to_postgres():

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
    list1=['groceries-pets','health-beauty','phones-tablets','electronic-accessories','tv-home-appliances','computing-gaming','womens-fashion','men-s-fashion','home-lifestyle','medical-nutrition','kids-babies','naheed-products','deal-of-the-day']
    productss=[]
    for y in list1:
        for x in range(1,350):
            url='https://www.naheed.pk/'+y+'?p='
            r= requests.get(url+str(x))  
                
            soup = BeautifulSoup(r.content,'html.parser')
            Products=soup.find_all('div',class_="product-item-info per-product category-products-grid")
            print(Products)
            for product in Products:
                product_name= product.find('h2',class_="product name product-name product-item-name").text
                
                product_price= product.find('span',class_="price").text.replace('Rs. ','').replace(',','')
                product_img= product.find('img',class_="photo").attrs['src']
                product_info = {
                    'company_code':'Naheed',
                    'product_name':product_name,
                    'product_price':product_price,
                    'product_description':'',
                    'product_category': y,
                    'product_subcategory':'',
                    'product_image': product_img,
                    'date': date.today()
                }
                productss.append( product_info)

        
   
    df=pd.DataFrame(productss)
    filename =f'ibl_naheed_{curr_date}'
    writer = pd.ExcelWriter(f"/home/admin2/airflow/scrap_mart/{filename}.xlsx", engine='xlsxwriter')
    df.to_excel(writer,sheet_name = 'naheed', index=False)
    writer.save()

    #conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    cursor = conn.cursor()
    if len(df) > 0:
        df_columns = list(df)    
        columns = ",".join(df_columns)

    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {}".format('"DW"."IBL_SCRAP_MART"',columns,values)
    pg.extras.execute_batch(cursor, insert_stmt, df.values)
    conn.commit()

def insert_Naheed_Data_To_GCS():
    # Establishing Postgres Connection           
    #conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    # Read data from PostgreSQL database table and load into a DataFrame instance
    conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    df_naheed   = pd.read_sql('SELECT company_code, product_name, product_price, product_description, product_category, product_subcategory, product_image, "date", created_at FROM "DW"."IBL_SCRAP_MART" WHERE company_code=\'Naheed\'', conn);
    df = pd.DataFrame(data=df_naheed)      
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    filename =f'ibl_naheed_{curr_date}'
    bucket.blob(f'staging/scraping/naheed/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

def delete_Naheed_Data_BQ():
    naheed_data_delete_records_bq = BigQueryOperator(
        task_id="bq_naheed_data_del_records",
        bigquery_conn_id='bigquery',    
	    use_legacy_sql=False,
        sql='''    
	    #standardSQL	
	    DELETE FROM  `data-light-house-prod.EDW.IBL_SCRAP_MART` WHERE company_code = 'Naheed' 
	    ''',
        dag=Naheed_Scrap
)



Naheed_Scrap = DAG(
    dag_id='Naheed_Scrap',
    default_args=default_args,
     schedule_interval='00 18 * * 6',   
   # dagrun_timeout=timedelta(minutes=60),
    description='Naheed_product_info',
)


t0 = PythonOperator(
        task_id="delete_Naheed_Data",
        python_callable=delete_Naheed_Data,
        dag=Naheed_Scrap
)


t1 =    PythonOperator(
        task_id="Naheed_scrap_data",
        python_callable=extract_naheed_data_scrap_to_postgres,
        dag=Naheed_Scrap
)

t2=     PythonOperator(
        task_id="Naheed_postgres_to_gcs_data",
        python_callable=insert_Naheed_Data_To_GCS,
        dag=Naheed_Scrap
)

t3=     PythonOperator(
        task_id="Naheed_delete_data_BQ",
        python_callable=delete_Naheed_Data_BQ,
        dag=Naheed_Scrap
)

Naheed_data_gcs_to_bq=     GCSToBigQueryOperator(    
    task_id='Naheed_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/scraping/naheed/ibl_naheed_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_SCRAP_MART',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'company_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'product_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'product_price', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'product_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'product_category', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'product_subcategory', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'product_image', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}          
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=Naheed_Scrap
)


t0 >> t1 >> t2 >> t3 >> Naheed_data_gcs_to_bq


