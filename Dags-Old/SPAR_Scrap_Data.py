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
    msg = "Spar Scrap DAG has executed successfully."
    subject = f"Spar Scrap DAG has completed"
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

def delete_Spar_Data():
    #conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    cursor = conn.cursor()
    delete_sql = f"""DELETE FROM  "DW"."IBL_SCRAP_MART" WHERE company_code='Spar'"""
    cursor.execute(delete_sql)
    conn.commit()




def get_insert_Spar_Scrap_Data():
    options = Options()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    chrome_path = r"/usr/bin/chromedriver"
    driver = webdriver.Chrome(chrome_path,chrome_options=options)
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
    url = 'https://store.spar.pk/'
    productlinks = []
    #categories = ['/catalog/deals-discounts?id=3987', '/catalog/groceries?id=3909','/catalog/fresh?id=3910','/catalog/health-beauty?id=3911','/catalog/baby-care-food?id=3914',
    #'/catalog/dairy?id=3917','/catalog/dry-fruits-dates?id=4446','/catalog/snacks-confectionary?id=3916','/catalog/pet-care-pet-food?id=3915','/catalog/electronic-accessories?id=3913','/catalog/phones-tablets?id=3912','/catalog/mumuso?id=3972']
    categories = ['catalog/deals-discounts?id=3987','catalog/groceries?id=3909','catalog/fresh?id=3910','catalog/health-beauty?id=3911','/catalog/baby-care-food?id=3914',
    '/catalog/dairy?id=3917','/catalog/dry-fruits-dates?id=4446','/catalog/snacks-confectionary?id=3916','/catalog/pet-care-pet-food?id=3915','/catalog/electronic-accessories?id=3913','/catalog/phones-tablets?id=3912','/catalog/mumuso?id=3972']
    data=[]
    image=[]
    cat=[]
    cat_data=[]
    sub_cat=[]
    sub_cat_data=[]
    sub_cat_links=[]
    sec_links=[]
    name=[]
    desc=[]
    price=[]
    sale_price=[]
    discount_label=[]
    driver.get(url)
    

    select_city = driver.find_element_by_css_selector('.button.button--outline.input-group__button.append.cities-list__button').click()
    time.sleep(1)
    items = driver.find_elements_by_tag_name("li")
    for item in items:
        text = item.text	
        if text == 'Karachi':
            item.click()
            break
    select_area = driver.find_element_by_css_selector('.button.button--outline.input-group__button.append.areas-list__button').click()
    time.sleep(1)
    areas = driver.find_elements_by_tag_name("li")
    for area in areas:
        text = area.text	
        if text == 'Airport Karachi':
            area.click()
            break
    driver.find_element_by_css_selector('.button.save-location__button').click() 
    

    for cat_link in categories:
        driver.get(url+cat_link)
        html = driver.page_source
        soup = BeautifulSoup(html,'html.parser')
        for tag in soup.find_all("a", class_="sub-categories__categories__item"):
            sub_cat_links.append(tag['href'])
            sub_cat_links = [item.replace("https://store.spar.pk/", "") for item in sub_cat_links]


    ###### section data   

    for sub_cat_link in sub_cat_links:
        driver.get(url+sub_cat_link)
        new_html = driver.page_source
        new_soup = BeautifulSoup(new_html,'html.parser')
        for sec_tag in new_soup.find_all("a", class_="sub-category__product-navigation__link"):
            sec_links.append(sec_tag['href'])
    

    
    
    ###### subcategory data      

    for sub_cat_link in sub_cat_links:
        driver.get(url+sub_cat_link)
        new_html = driver.page_source
        new_soup = BeautifulSoup(new_html,'html.parser')
        main_data=new_soup.select("div.product__info > p.product__title")
        for product_name in main_data:
            try:
                name.append(product_name.text)
            except:
                name=None
        price_data=new_soup.select("p.product__price")    
        for product_price in price_data:
            try:
                price.append(product_price.text.replace('"','').strip().strip(' \t\n\r').replace("\n","").split('Rs. ').pop())                        
            except:
                price=None
        desc_data=new_soup.select("div.product__info > p.product__description")        
        for product_desc in desc_data:
            try:
                desc.append(product_desc.text)            
            except:
                desc=None

        cat_data=[a["href"] for a in new_soup.select(".product a")]
        del cat_data[1::2]    
        try: 
            cat_data = list(map(lambda x: x.split('/')[4], cat_data))                       
            cat.append(cat_data)         
        except:
            cat=None 
        
        sub_cat_data=[a["href"] for a in new_soup.select(".product a")]
        del sub_cat_data[1::2]
        try:        
            sub_cat_data = list(map(lambda x: x.split('/')[5], sub_cat_data))       
            sub_cat.append(sub_cat_data)       
        except:
            sub_cat=None    
            
        image_data=new_soup.select("button.btn-add-to-cart")
        for product_image in image_data:
            image.append(product_image['data-product-image']) 
    
    #print(sec_links)
    #print(len(sec_links))        
    broken_sec_links = [s for s in sec_links if 'https://store.spar.pk/' not in s]
    for broken in broken_sec_links:        
        mod_broken = broken[3:] 
        new_broken_list = 'https://store.spar.pk/' + mod_broken 
        sec_links.append(new_broken_list)
        sec_links.remove(broken)         
      
    #print(sec_links)
    #print(len(sec_links))
    for sec_link in sec_links:
            driver.get(sec_link)
            new_html = driver.page_source
            new_soup = BeautifulSoup(new_html,'html.parser')
            main_data=new_soup.select("div.product__info > p.product__title")
            for product_name in main_data:
                try:
                    name.append(product_name.text)
                except:
                    name=None
            price_data=new_soup.select("p.product__price")    
            for product_price in price_data:
                try:
                    price.append(product_price.text.replace('"','').strip().strip(' \t\n\r').replace("\n","").split('Rs. ').pop())                            
                except:
                    price=None  
            desc_data=new_soup.select("div.product__info > p.product__description")        
            for product_desc in desc_data:
                try:
                    desc.append(product_desc.text)            
                except:
                    desc=None

            cat_data=[a["href"] for a in new_soup.select(".product a")]
            del cat_data[1::2]    
            try: 
                cat_data = list(map(lambda x: x.split('/')[4], cat_data))                       
                cat.append(cat_data)         
            except:
                cat=None 
            
            sub_cat_data=[a["href"] for a in new_soup.select(".product a")]
            del sub_cat_data[1::2]
            try:        
                sub_cat_data = list(map(lambda x: x.split('/')[5], sub_cat_data))       
                sub_cat.append(sub_cat_data)       
            except:
                sub_cat=None    
                
            image_data=new_soup.select("button.btn-add-to-cart")
            for product_image in image_data:
                image.append(product_image['data-product-image'])   
        

        

    cat = [item for sublist in cat for item in sublist]
    sub_cat = [item for sublist in sub_cat for item in sublist]
    price = [item.replace(",", "") for item in price]

    products_dict = {'product_name':name, 'product_price':price, 'product_description':desc, 'product_category':cat, 'product_subcategory':sub_cat, 'product_image':image }
    #print(products_dict)
    #print(len(name), len(desc), len(price), len(cat), len(sub_cat), len(image))
    df = pd.DataFrame(products_dict)
    df=df.drop_duplicates()
    df.insert(0, 'company_code', 'Spar')
    df['date'] = datetime.today().strftime('%Y-%m-%d')
    filename =f'ibl_spar_{curr_date}'
    writer = pd.ExcelWriter(f"/home/admin2/airflow/scrap_mart/{filename}.xlsx", engine='xlsxwriter')
    df.to_excel(writer,sheet_name = 'spar', index=False)
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

def insert_Spar_Data_To_GCS():
    # Establishing Postgres Connection           
    #conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    # Read data from PostgreSQL database table and load into a DataFrame instance
    conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    df_spar   = pd.read_sql('SELECT company_code, product_name, product_price, product_description, product_category, product_subcategory, product_image, "date", created_at FROM "DW"."IBL_SCRAP_MART" WHERE company_code=\'Spar\'', conn);
    df = pd.DataFrame(data=df_spar)      
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    filename =f'ibl_spar_{curr_date}'
    bucket.blob(f'staging/scraping/spar/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

def delete_Spar_Data_BQ():
    spar_data_delete_records_bq = BigQueryOperator(
        task_id="bq_spar_data_del_records",
        bigquery_conn_id='bigquery',    
	    use_legacy_sql=False,
        sql='''    
	    #standardSQL	
	    DELETE FROM  `data-light-house-prod.EDW.IBL_SCRAP_MART` WHERE company_code = 'Spar' 
	    ''',
        dag=Spar_Scrap
)




Spar_Scrap = DAG(
    dag_id='Spar_Scrap',
    default_args=default_args,
    schedule_interval='00 18 * * 6',
    #schedule_interval='*/30 * * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='Spar_Scrap_Data_Products_info',
)



t0 =   PythonOperator(
        task_id="spar_delete_data",
        python_callable=delete_Spar_Data,
        dag=Spar_Scrap
)

t1 =    PythonOperator(
        task_id="spar_scrap_data",
        python_callable=get_insert_Spar_Scrap_Data,
        dag=Spar_Scrap
)

t2=     PythonOperator(
        task_id="spar_postgres_to_gcs_data",
        python_callable=insert_Spar_Data_To_GCS,
        dag=Spar_Scrap
)

t3=     PythonOperator(
        task_id="spar_delete_data_BQ",
        python_callable=delete_Spar_Data_BQ,
        dag=Spar_Scrap
)

spar_data_gcs_to_bq=     GCSToBigQueryOperator(    
    task_id='spar_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/scraping/spar/ibl_spar_{curr_date}.csv',
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
    dag=Spar_Scrap
)


t0 >> t1 >> t2 >> t3 >> spar_data_gcs_to_bq



                
      
   







 
              


        
                     

   



    

    
        
    

   
    


    


