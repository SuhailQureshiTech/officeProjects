import imp
from fileinput import filename
import psycopg2 as pg
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from dateutil import parser
from datetime import datetime, date
import sys
from http import client
import pandas as pds
import io
import numpy as np
from datalab.context import Context
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.contrib.operators import gcs_to_bq
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
#from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
from airflow.utils.email import send_email_smtp
from airflow.operators.email_operator import EmailOperator
import sqlalchemy
# from sqlalchemy import create_engine
# import pandas_gbq
from IPython.display import display
# from pymysql import Date
# from pymysql import Date
# from pydantic import FilePath
import pysftp
import csv
import pyodbc
from hdbcli import dbapi
#import conf
import pandas as pd
from datetime import date, datetime, timedelta,timezone
from google.oauth2.service_account import Credentials
from airflow.contrib.operators import gcs_to_bq
import numpy as np
from regex import F
import re

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
creationDate = date + timedelta(hours=5)
print(
    date + timedelta(hours=5)
)

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))

if vTodayDate <= 5:
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    vdayDiff = int(vEndDate.strftime("%d"))
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))
    print('star date :', vStartDate)
    print('end date ', vEndDate)

else:

    print('else block')
    vStartDate = datetime.date(datetime.today().replace(day=1))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    print('else : from date :', vStartDate)
    print('else : enmd date :', vEndDate)

# vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
# vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"
vFirstDate = vStartDate
vLastDate = datetime.date(datetime.today())

# vEndDate = datetime.date(datetime.today()-timedelta(days=1))
vEndDate = datetime.date(datetime.today())
vStartDate = "'"+str(vStartDate)+"'"
vEndDate = "'"+str(vEndDate)+"'"
curr_date = datetime.today().strftime("%d-%b-%Y")
creationDate = date + timedelta(hours=5)
filePath = f'''/home/admin2/airflow/franchise/'''
global fileName
fileName = 'MARKITT_ITEMS.csv'
filenameItemCat = f'''markit_itemCatBrand_{curr_date}.csv'''

spec_chars = ["!", '"', "#", "%", "&", "'", "(", ")",
            "*", "+", ",", "-", ".", "/", ":", ";", "<",
            "=", ">", "?", "@", "[", "\\", "]", "^", "_",
            "`", "{", "|", "}", "~", "–"]

default_args = {
        'owner': 'admin',
        'start_date': datetime(2022, 10, 11),
        # 'end_date': datetime(),
        'depends_on_past': False,
        'email': ['shehzad.lalani@iblgrp.com'],
        'email_on_failure': True,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),

        }

MarkittMasterDataIntg = DAG(
    dag_id='MarkittMasterDataIntg',
    default_args=default_args,
    catchup=False,
    start_date=datetime(year=2022, month=11, day=20),
    schedule_interval='00 23 * * *',
    # dagrun_timeout=timedelta(minutes=60),
    description='Markitt Master Data Integeration.',
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()

deleteBQRecordsTask = BigQueryOperator(
    task_id='DeleteBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
        truncate table `data-light-house-prod.EDW.MARKITT_ITEMS`
        ''',
    dag=MarkittMasterDataIntg
)

deleteItemCatBQRecordsTask = BigQueryOperator(
    task_id='DeleteItemCatBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
        truncate table `data-light-house-prod.EDW.MARKITT_ITEM_CAT_BRAND`
        ''',
    dag=MarkittMasterDataIntg
)

def GenerateItemsData():
    global fileName
    global fran_sale_df
    print('oracle start date :', vStartDate)
    print('oracle end date : ', vEndDate)
    # print('global fileName : ',fileName)

    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    conn1 = dbapi.connect(address='10.210.134.204',
                        port='33015',   user='ETL',  password='Etl@2025')

    Location_df = pds.read_sql(f'''
                        SELECT NORMT,MATNR,MAKTX
                        FROM markitt_items''', conn1)
    Location_df.columns = Location_df.columns.str.strip()
    Location_df.replace(',', '', regex=True, inplace=True)

    for char in spec_chars:
        Location_df['MAKTX'] = Location_df['MAKTX'].str.replace(
            char, ' ', regex=True)
    Location_df['MAKTX'] = Location_df['MAKTX'].str.split().str.join(" ")
    bucket.blob(f'''staging/temp/{fileName}''').upload_from_string(Location_df.to_csv(index=False), 'text/csv')

sapItems = PythonOperator(
    task_id="ibl_sap_items",
    python_callable=GenerateItemsData,
    dag=MarkittMasterDataIntg
)

IblItems_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(

    # print('bq file name : ',fileName)
    task_id='sapItemsBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/{fileName}''',
    destination_project_dataset_table='data-light-house-prod.EDW.MARKITT_ITEMS',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'NORMT', 'type': 'STRING', 'mode': 'NULLABLE'}
        ,   {'name': 'MATNR', 'type': 'STRING', 'mode': 'NULLABLE'}
            ,   {'name': 'MAKTX', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=MarkittMasterDataIntg
)

def extract_itemCatBrand_to_gcs():
    # Establishing Postgres Connection
    server = '172.20.7.71\SQLSERVER2017'
    db = 'Markitt2021-2022'
    user = 'syed.shujaat'
    password = 'new$5201'
    schema = 'dbo'
    tablePOS = 'vw_item_brand_categ_data'

    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server +
                        ';DATABASE='+db+';UID='+user + ';PWD='+password+';TrustServerCertificate=Yes')
    cursor = conn.cursor()
    # query = f'''SELECT BarCode barcode,Item,CategoryCode,Category,BrandCode,Brand
    #     FROM [%s].%s.%s  ''' % (db, schema, tablePOS)

    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    # filename = f'''markit_itemCatBrand_{curr_date}'''

    query = f'''SELECT BarCode barcode,Item item, CategoryCode category_code,Category category,BrandCode brand_code,Brand brand,FirstRecv  FROM [%s].%s.%s  ''' % (
        db, schema, tablePOS)
    df = pds.read_sql(query, conn)
    print(df)
    for char in spec_chars:
        df['barcode']=df['barcode'].str.replace(char, ' ', regex=True)
        df['item'] = df['item'].str.replace(char, ' ', regex=True)
        df['category_code'] = df['category_code'].str.replace(
            char, ' ', regex=True)
        df['category'] = df['category'].str.replace(char, ' ', regex=True)
        # df['brand_code'] = df['brand_code'].str.replace(char, ' ', regex=True)
        # df['brand'] = df['brand'].str.replace(char, ' ', regex=True)

    df['barcode']=df['barcode'].str.split().str.join(" ")
    df['item'] = df['item'].str.split().str.join(" ")
    df['category_code'] = df['category_code'].str.split().str.join(" ")
    df['category'] = df['category'].str.split().str.join(" ")
    # df['brand_code'] = df['brand_code'].str.split().str.join(" ")
    # df['brand'] = df['brand'].str.split().str.join(" ")

    df['created_at'] =creationDate
    df = df[['barcode', 'item', 'category_code',
             'category', 'brand_code', 'brand', 'created_at', 'FirstRecv']
            ]

    bucket.blob(
        f'''staging/markitt/{filenameItemCat}''').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

markittItemCatBrand = PythonOperator(
    task_id="markittitemCatBrand",
    python_callable=extract_itemCatBrand_to_gcs,
    dag=MarkittMasterDataIntg
)

markittItemCatBrand_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(

    # print('bq file name : ',fileName)
    task_id='markittItemCatBrandBQ',
    bucket='ibloper',
    source_objects=f'''staging/markitt/{filenameItemCat}''',
    destination_project_dataset_table='data-light-house-prod.EDW.MARKITT_ITEM_CAT_BRAND',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
            {'name': 'barcode', 'type': 'STRING', 'mode': 'NULLABLE'}
        ,{'name': 'item', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'category_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'brand_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        , {'name': 'FirstRecv', 'type': 'DATE', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=MarkittMasterDataIntg
)

dummy_task = DummyOperator(
    task_id='dummy_task', retries=3, dag=MarkittMasterDataIntg)

dummy_task1 = DummyOperator(
    task_id='dummy_task1', retries=3, dag=MarkittMasterDataIntg)

# dummy_task1>> [deleteBQRecordsTask,markittItemCatBrand]>>dummy_task>> [sapItems,IblItems_to_BQ,itemCatBrand_to_BQ]

#following
dummy_task1 >>[deleteItemCatBQRecordsTask,markittItemCatBrand]>>markittItemCatBrand_to_BQ
dummy_task>>[deleteBQRecordsTask,sapItems]>>IblItems_to_BQ
