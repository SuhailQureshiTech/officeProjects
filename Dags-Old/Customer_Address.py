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
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.utils.email import send_email
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
import pyodbc
from hdbcli import dbapi
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import numpy as np

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Customer Master DAG has executed successfully."
    subject = f"Customer Master DAG has completed"
    send_email(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)


default_args = {
        'owner': 'admin',
        'start_date': datetime(2022, 7, 28),
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

IBL_CUSTOMERS_ADDRESS = DAG(
    dag_id='IBL_CUSTOMERS_ADDRESS',
    default_args=default_args,
    schedule_interval='0 18 * * *',
    #schedule_interval='*/30 * * * *',
    # dagrun_timeout=timedelta(minutes=60),
    description='IBL_CUSTOMERS_ADD_TO_BQ',
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

def insert_MasterTable_Customers_To_GCS():
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    # Initialize your connection
    conn = dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2023'  )
    # Read data from KNA1
    df=pd.read_sql(f'''
        SELECT
        CLIENT as client_number
        , ADDRNUMBER AS Address_number
        , DATE_FROM
        , DATE_TO
        , NAME1
        , NAME2
        , NAME3
        , NAME_TEXT
        , COUNTRY
        , CITY1 CITY
        , CITY2 DISTRICT
        , CITY_CODE
        , CITYP_CODE DISTRICT_CODE
        , POST_CODE1 CITY_POSTAL_CODE
        , POST_CODE2 PO_BOX_POSTAL_CODE
        , PO_BOX
        , STREET
        , STR_SUPPL1 ADD1
        , STR_SUPPL2 ADD2
        , STR_SUPPL3 ADD3
        FROM
            SAPABAP1.ADRC
        WHERE
            1 = 1
            AND CLIENT = 300
    '''
    , conn)
    df['transfer_date'] = datetime.today()
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    bucket.blob(f'staging/master_tables/Customer/ibl_Customer_Address.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()



customerAddressToGCSTask = PythonOperator(
    task_id="customerAddressToGCSTask",
    python_callable=insert_MasterTable_Customers_To_GCS,
    dag=IBL_CUSTOMERS_ADDRESS
)

customerAddressToBQTask=GCSToBigQueryOperator(
    task_id='customerAddressToBQTask',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Customer/ibl_Customer_Address.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_CUSTOMER_ADDRESS',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
        {'name': 'client_number', 'type': 'STRING', 'mode': 'NULLABLE'} ,
        {'name': 'address_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'date_from', 'type': 'numeric', 'mode': 'NULLABLE'},
        {'name': 'date_to', 'type': 'numeric', 'mode': 'NULLABLE'},

        {'name': 'name1', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name3', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name_text', 'type': 'STRING', 'mode': 'NULLABLE'},

        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'district', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city_code', 'type': 'STRING', 'mode': 'NULLABLE'},

        {'name': 'district_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city_postal_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'po_box_postal_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'po_box', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'street', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'add1', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'add2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'add3', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=IBL_CUSTOMERS_ADDRESS
    )

deleteBQRecordsTask = BigQueryOperator(
    task_id='DeleteBQRecords'
    , bigquery_conn_id='bigquery'
    , use_legacy_sql=False
    , sql=f'''
        TRUNCATE TABLE  `data-light-house-prod.EDW.IBL_CUSTOMER_ADDRESS`
        '''
    , dag=IBL_CUSTOMERS_ADDRESS
)


deleteBQRecordsTask>>customerAddressToGCSTask >> customerAddressToBQTask
