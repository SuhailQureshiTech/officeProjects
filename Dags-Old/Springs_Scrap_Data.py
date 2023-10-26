import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from dateutil import parser
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import sys
from http import client
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import  pprint
import psycopg2 as pg
import psycopg2.extras
from datetime import datetime,date
import html
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
# from sqlalchemy import create_engine
import pandas_gbq
from IPython.display import display
import pysftp
import csv
import pyodbc
import xlsxwriter

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Springs Scrap DAG has executed successfully."
    subject = f"Springs Scrap DAG has completed"
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
curr_date = today.strftime("%d-%b-%Y")

def delete_Springs_Data():
    #conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    cursor = conn.cursor()
    delete_sql = f"""DELETE FROM  "DW"."IBL_SCRAP_MART" WHERE company_code='Springs'"""
    cursor.execute(delete_sql)
    conn.commit()




def get_insert_Springs_Scrap_Data():
    options = Options()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    chrome_path = r"/usr/bin/chromedriver"
    driver = webdriver.Chrome(chrome_path,chrome_options=options)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}

    url = 'https://springs.com.pk/'
    categories = []
    productlinks = []
    data=[]
    image=[]
    prod_info=[]
    prod=[]
    Main_Category_link=[]
    Category_name=[]
    k = requests.get(url).text
    soup=BeautifulSoup(k,'html.parser')
    category_list = soup.find_all("div", {"class":"spotlight-12-blocks"})
    category_inner_list = soup.find_all("div", {"class":"content spotlight-inner"})
    category_link = soup.find_all('a',  {"class":"shop_link"})
    for link in category_link:
        categories.append(link.get('href'))
    print(categories)
    for cat_link in categories:        
        f = requests.get(url+cat_link,headers=headers).text
        hun=BeautifulSoup(f,'html.parser')
        driver.get(url+cat_link)
        time.sleep(3)
        for links in hun.find_all("a", {"class":"product-grid-image"}):
            productlinks.append(links['href'])    
        for product_info in productlinks:
            l = requests.get(url+product_info,headers=headers).text    
            hn=BeautifulSoup(l,'html.parser')
            prod_info = hn.find_all("div", {"id":"ProductSection-product-template-supermarket"})            
            try:
                name=hn.find("h1",{"class":"product-title"}).text.replace('\n',"").strip()
                #print(name)                        
            except:
                name=None

            try: 
                cat_data = product_info.split('/')[2]                          
                    
            except:
                cat_data=None 
        
            try:
                Product_Price =hn.find("span",{"class":"price"}).text.replace('\n',"").replace('Rs.',"").replace(',',"").strip()
                #print(Product_Price)
            except:
                Product_Price = None
            try:
                Product_description =hn.find("div",{"class":"short-description"}).text.replace('\n',"").strip()
                #print(Product_description)
            except:
                Product_description = None
            try:
                product_image = hn.find("a",{"class":"fancybox"}).get('href')
            # for item_image in product_images:
                #print(product_image)
            except:  
                product_image = None
                
            _Product={
                        "company_code"       : 'Springs',
                        "product_name"       : name,
                        "product_price"      : Product_Price,
                        "product_description": Product_description,
                        "product_category"   : cat_data,
                        "product_subcategory"   : '',
                        "product_image"     : product_image                       
            } 
            data.append(_Product) 
            
    df=pd.DataFrame(data)    
    df['date']=date.today()
    df = df.drop_duplicates()
    filename =f'ibl_springs_{curr_date}'
    writer = pd.ExcelWriter(f"/home/admin2/airflow/scrap_mart/{filename}.xlsx", engine='xlsxwriter')
    df.to_excel(writer,sheet_name = 'springs', index=False)
    writer.save()

    #conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    cursor = conn.cursor()

    if len(df) > 0:
        df_columns = list(df)    
        columns = ",".join(df_columns)
        #print(columns)            
        
        # create VALUES('%s', '%s",...) one '%s' per column
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
        #create INSERT INTO table (columns) VALUES('%s',...)
    insert_stmt = "INSERT INTO {} ({}) {}".format('"DW"."IBL_SCRAP_MART"',columns,values)
    pg.extras.execute_batch(cursor, insert_stmt, df.values)
    conn.commit()


def insert_Springs_Data_To_GCS():
    # Establishing Postgres Connection           
    #conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")

    # Read data from PostgreSQL database table and load into a DataFrame instance
    df_springs   = pd.read_sql('SELECT company_code, product_name, product_price, product_description, product_category, product_subcategory, product_image, "date", created_at FROM "DW"."IBL_SCRAP_MART" WHERE company_code=\'Springs\'', conn);
    df = pd.DataFrame(data=df_springs)      
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    filename =f'ibl_springs_{curr_date}'
    bucket.blob(f'staging/scraping/springs/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

def delete_Springs_Data_BQ():
    springs_data_delete_records_bq = BigQueryOperator(
        task_id="bq_springs_data_del_records",
        bigquery_conn_id='bigquery',    
	    use_legacy_sql=False,
        sql='''    
	    #standardSQL	
	    DELETE FROM  `data-light-house-prod.EDW.IBL_SCRAP_MART` WHERE company_code = 'Springs' 
	    ''',
        dag=Springs_Scrap_Data
)




Springs_Scrap_Data = DAG(
    dag_id='Springs_Scrap_Data',
    default_args=default_args,
    schedule_interval='00 18 * * 6',
    #schedule_interval='*/30 * * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='Springs_Scrap_Data_Products_info',
)



t0 =   PythonOperator(
        task_id="Springs_delete_data",
        python_callable=delete_Springs_Data,
        dag=Springs_Scrap_Data

)

t1 =    PythonOperator(
        task_id="Springs_scrap_data",
        python_callable=get_insert_Springs_Scrap_Data,
        dag=Springs_Scrap_Data
)

t2=     PythonOperator(
        task_id="Springs_postgres_to_gcs_data",
        python_callable=insert_Springs_Data_To_GCS,
        dag=Springs_Scrap_Data
)

t3=     PythonOperator(
        task_id="Springs_delete_data_BQ",
        python_callable=delete_Springs_Data_BQ,
        dag=Springs_Scrap_Data
)

Springs_data_gcs_to_bq=     GCSToBigQueryOperator(    
    task_id='Springs_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/scraping/springs/ibl_springs_{curr_date}.csv',
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
    dag=Springs_Scrap_Data
)


t0 >> t1 >> t2 >> t3 >> Springs_data_gcs_to_bq
