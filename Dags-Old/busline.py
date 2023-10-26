import psycopg2 as pg
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
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
from hdbcli import dbapi
import connectionClass
a=connectionClass
default_args = {
        'owner': 'admin',
        'start_date': datetime(2022, 8, 13),
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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

def extract_busline_postgres_to_gcs():
     # Initialize your connection
    # conn = dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2023'  )
    conn=a.sapConn()
    cursor_sap = conn.cursor()
    df= pds.read_sql(f'''SELECT "MANDT", "MATNR", matnr_desc, mapping_code
                    ,company, busline_id, busline_desc
                    FROM ETL.MATNR_WITH_COMPANY_BUSLIEN''', conn);
    df_busline = pds.DataFrame(data=df)
    print(df_busline)
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    filename =f'ibl_busline_{curr_date}'

    bucket.blob(f'staging/stock/{filename}.csv').upload_from_string(df_busline.to_csv(index=False), 'text/csv')
    conn.close()





ibl_busline = DAG(
    dag_id='ibl_busline',
    default_args=default_args,
    schedule_interval='0 18 * * 6',
   # dagrun_timeout=timedelta(minutes=60),
    description='ibl_busline_data',
)



busline_data_postgres_to_gcs = PythonOperator(
        task_id="busline_data_postgres_to_gcs",
        python_callable=extract_busline_postgres_to_gcs,
        dag=ibl_busline
)



busline_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='busline_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/stock/ibl_busline_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.MATNR_WITH_COMPANY_BUSLINE_STOCK',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'mandt', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'matnr', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'matnr_desc', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'mapping_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'company', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'busline_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'busline_desc', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=ibl_busline
)


busline_data_postgres_to_gcs >> busline_data_gcs_to_bq




