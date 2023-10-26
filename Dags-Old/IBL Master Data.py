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
from datetime import date, datetime, timedelta, timezone
from google.oauth2.service_account import Credentials
from airflow.contrib.operators import gcs_to_bq
import numpy as np
from regex import F
import re
import connectionClass
sapCon=connectionClass
conn1=sapCon.sapConn()

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
# fileName = 'MARKITT_ITEMS.csv'
filenameItemCat = f'''markit_itemCatBrand_{curr_date}.csv'''

spec_chars = ["!", '"', "#", "%", "&", "'", "(", ")",
            "*", "+", ",", "-", ".", "/", ":", ";", "<",
            "=", ">", "?", "@", "[", "\\", "]", "^", "_",
            "`", "{", "|", "}", "~", "–"]

V_GCS_PROJECT = 'data-light-house-prod'
V_GCS_BUCKET = 'ibloper'
V_GCS_BUCKET_PATH = 'staging/temp/MasterData'

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 10, 11),
    # 'end_date': datetime(),
    'depends_on_past': False,
    'email': ['shehzad.lalani@iblgrp.com'],
    'email_on_failure': True,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),

}

masterDataIntg = DAG(
    dag_id='IBL_MASTER_TABLES',
    default_args=default_args,
    start_date=datetime(year=2022, month=9, day=27),
    schedule_interval='00 01 * * 1,3,5,7',
    # dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    description='IBL_MASTER_TABLES'
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()

#Business Line
deleteBusinessLineBQRecordsTask = BigQueryOperator(
    task_id='deleteBusinessLineBQRecordsTask',
    bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f''' truncate table `data-light-house-prod.EDW.IBL_BUSINESS_LINE_DESC`    ''',
    dag=masterDataIntg
)


def generateBusinessLineData():
    global fileName
    global fran_sale_df
    print('oracle start date :', vStartDate)
    print('oracle end date : ', vEndDate)
    # print('global fileName : ',fileName)

    GCS_PROJECT = V_GCS_PROJECT
    GCS_BUCKET = V_GCS_BUCKET

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    # conn1 = dbapi.connect(address='10.210.134.204',
    #                       port='33015',   user='ETL',  password='Etl@2025')

    # conn1=sapCon.sapConn()
    df = pds.read_sql(f'''
                        SELECT
                        BUSLINE_CODE business_line_code ,BUSLINE_DESC business_line_desc
                        FROM ETL.ZPBI_BUSLINE x
                        ''', conn1)
    df.columns = df.columns.str.strip()
    df.replace(',', '', regex=True, inplace=True)
    df['transfer_date'] = creationDate
    fileName = 'SAP_BUSINESSLINE_DATA.csv'

    bucket.blob(f'''staging/temp/MasterData/{fileName}''').upload_from_string(
        df.to_csv(index=False), 'text/csv')


sapBusinessLineData = PythonOperator(
    task_id="generateBusinessLineData",
    python_callable=generateBusinessLineData,
    dag=masterDataIntg
)

sapBusinessLineBQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='sapBusinessLineBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/MasterData/SAP_BUSINESSLINE_DATA.csv''',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_BUSINESS_LINE_DESC',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'business_line_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'business_line_desc','type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'transfer_date',       'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
            ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=masterDataIntg
)

# BusinessLine End-----

deleteLocationBQRecordsTask = BigQueryOperator(
    task_id='DeleteBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
        truncate table `data-light-house-prod.EDW.LOCATION_DETAILS`
        ''',
    dag=masterDataIntg
)

def generateLocationData():
    global fileName
    global fran_sale_df
    print('oracle start date :', vStartDate)
    print('oracle end date : ', vEndDate)
    # print('global fileName : ',fileName)

    GCS_PROJECT = V_GCS_PROJECT
    GCS_BUCKET = V_GCS_BUCKET

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    # conn1 = dbapi.connect(address='10.210.134.204',
    #                         port='33015',   user='ETL',  password='Etl@2025')

    Location_df = pds.read_sql(f'''
                        SELECT * FROM ETL.VW_LOCATIONS x
                        ''', conn1)
    Location_df.columns = Location_df.columns.str.strip()
    Location_df.replace(',', '', regex=True, inplace=True)
    fileName = 'SAP_LOCATION_DATA.csv'

    # for char in spec_chars:
    #     Location_df['MAKTX'] = Location_df['MAKTX'].str.replace(
    #         char, ' ', regex=True)
    # Location_df['MAKTX'] = Location_df['MAKTX'].str.split().str.join(" ")

    bucket.blob(f'''staging/temp/MasterData/{fileName}''').upload_from_string(
        Location_df.to_csv(index=False), 'text/csv')


sapLocationData = PythonOperator(
    task_id="generateLocationData",
    python_callable=generateLocationData,
    dag=masterDataIntg
)

sapLocationBQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='sapLocationBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/MasterData/SAP_LOCATION_DATA.csv''',
    destination_project_dataset_table='data-light-house-prod.EDW.LOCATION_DETAILS',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
                {'name': 'WERKS', 'type': 'STRING', 'mode': 'NULLABLE'}
            ,   {'name': 'LGORT', 'type': 'STRING', 'mode': 'NULLABLE'}
            ,   {'name': 'LGOBE', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=masterDataIntg
)

# customerData Generation
deleteCustomerBQRecordsTask = BigQueryOperator(
    task_id='DeleteCustomerBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
        truncate table `data-light-house-prod.EDW.IBL_CUSTOMERS_DATA`
        ''',
    dag=masterDataIntg
)

def generateCustomerData():
    global fileName
    global fran_sale_df
    print('oracle start date :', vStartDate)
    print('oracle end date : ', vEndDate)
    # print('global fileName : ',fileName)

    GCS_PROJECT = V_GCS_PROJECT
    GCS_BUCKET = V_GCS_BUCKET

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    # conn1 = dbapi.connect(address='10.210.134.204',
    #                         port='33015',   user='ETL',  password='Etl@2025')

    df = pds.read_sql(f'''
                        SELECT
                        CLIENT_CODE,
                        CUSTOMER_NUMBER,
                        NAME1,
                        NAME2,
                        COUNTRY_KEY,
                        COUNTRY_DESC,
                        CITY,
                        PHONE,
                        ADDRESS_NUMBER,
                        STREET,
                        ADD1,
                        ADD2,
                        ADD3,
                        LOCATION,
                        CNIC,
                        NTN,
                        GST_REG_NUMBER
                        FROM ETL.ZPBI_CUSTOMERS x
                        ''', conn1)
    df.columns = df.columns.str.strip()
    df.replace(',', '', regex=True, inplace=True)
    df['transfer_date'] = creationDate
    fileName = 'SAP_CUSTOMER_DATA.csv'

    # for char in spec_chars:
    #     Location_df['MAKTX'] = Location_df['MAKTX'].str.replace(
    #         char, ' ', regex=True)
    # Location_df['MAKTX'] = Location_df['MAKTX'].str.split().str.join(" ")

    bucket.blob(f'''staging/temp/MasterData/{fileName}''').upload_from_string(
        df.to_csv(index=False), 'text/csv')


sapCustomerData = PythonOperator(
    task_id="generateCustomerData",
    python_callable=generateCustomerData,
    dag=masterDataIntg
)

sapCustomerBQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='sapCustomerBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/MasterData/SAP_CUSTOMER_DATA.csv''',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_CUSTOMERS_DATA',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'CLIENT_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CUSTOMER_NUMBER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'NAME1', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'NAME2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'COUNTRY_KEY', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'COUNTRY_DESC', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CITY', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'PHONE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ADDRESS_NUMBER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'STREET', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ADD1', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ADD2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ADD3', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'LOCATION', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CNIC', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'NTN', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'GST_REG_NUMBER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'TRANSFER_DATE', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=masterDataIntg
)


# items Generation
deleteItemsBQRecordsTask = BigQueryOperator(
    task_id='DeleteItemsBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
        truncate table `data-light-house-prod.EDW.MATNR_WITH_COMPANY_BUSLINE_STOCK`
        ''',
    dag=masterDataIntg
)

def generateItemsData():
    global fileName
    global fran_sale_df
    print('oracle start date :', vStartDate)
    print('oracle end date : ', vEndDate)
    # print('global fileName : ',fileName)

    GCS_PROJECT = V_GCS_PROJECT
    GCS_BUCKET = V_GCS_BUCKET

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    # conn1 = dbapi.connect(address='10.210.134.204',
    #                     port='33015',   user='ETL',  password='Etl@2025')

    df = pds.read_sql(f'''
                        SELECT
                        MANDT,MATNR,MATNR_DESC,MAPPING_CODE,COMPANY,BUSLINE_ID,BUSLINE_DESC
                        FROM ETL.MATNR_WITH_COMPANY_BUSLIEN
                        ''', conn1)
    df.columns = df.columns.str.strip()
    df.replace(',', '', regex=True, inplace=True)
    df['transfer_date'] = creationDate
    fileName = 'SAP_ITEMS_DATA.csv'

    for char in spec_chars:
        df['MATNR_DESC'] = df['MATNR_DESC'].str.replace(
            char, ' ', regex=True)
        df['MATNR_DESC'] = df['MATNR_DESC'].str.split().str.join(" ")

    bucket.blob(f'''staging/temp/MasterData/{fileName}''').upload_from_string(
        df.to_csv(index=False), 'text/csv')


sapItemsData = PythonOperator(
    task_id="generateItemsData",
    python_callable=generateItemsData,
    dag=masterDataIntg
)

sapItemsBQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='sapItemsBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/MasterData/SAP_ITEMS_DATA.csv''',
    destination_project_dataset_table='data-light-house-prod.EDW.MATNR_WITH_COMPANY_BUSLINE_STOCK',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'MANDT', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MATNR', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MATNR_DESC', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MAPPING_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'COMPANY', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BUSLINE_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BUSLINE_DESC', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'TRANSFER_DATE', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}

    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=masterDataIntg
)
dummy_task = DummyOperator(
    task_id='dummy_task', retries=3, dag=masterDataIntg)

# dummy_task1 = DummyOperator(
#     task_id='dummy_task1', retries=3, dag=MarkittMasterDataIntg)

dummy_task>>[deleteLocationBQRecordsTask>>sapLocationData>>sapLocationBQ
            ,deleteCustomerBQRecordsTask>>sapCustomerData>>sapCustomerBQ
            ,deleteItemsBQRecordsTask>>sapItemsData>>sapItemsBQ
            ,deleteBusinessLineBQRecordsTask>>sapBusinessLineData>>sapBusinessLineBQ
            ]

