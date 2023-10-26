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
from datetime import datetime,date,timedelta,timezone
import sys
from http import client
import pandas as pds
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
#from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
from airflow.utils.email import send_email_smtp
from airflow.operators.email_operator import EmailOperator
# from sqlalchemy import create_engine
import pandas_gbq
from IPython.display import display
import pysftp
import csv
import pyodbc
from hdbcli import dbapi
#import conf

dataDate = datetime.date(
    datetime.today()-timedelta(days=1)
)
dataDate = "'"+str(dataDate)+"'"

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
creationDate = date + timedelta(hours=5)
filename = f'SAP_ITEMS_MATERIAL_GROUP'
spec_chars = ["!", '"', "#", "%", "&", "'", "(", ")",
            "*", "+", ",", "-", ".", "/", ":", ";", "<",
            "=", ">", "?", "@", "[", "\\", "]", "^", "_",
            "`", "{", "|", "}", "~", "–"]

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Markitt Sales DAG to BigQuery has executed successfully."
    subject = f"Markitt Sales DAG to BigQuery has completed"
    send_email_smtp(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)

default_args = {
        'owner': 'admin',
        'start_date': datetime(2022, 9, 21),
        # 'end_date': datetime(),
        'email_on_failure': True,
        'email': ['shehzad.lalani@iblgrp.com'],
        'on_success_callback':success_function,
        #'on_success_callback': dag_success_alert,
        #'on_failure_callback': failure_email_function,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        }

SAP_ITEMS_MATERIAL_GROUP = DAG(
    dag_id='SAP_ITEMS_MATERIAL_GROUP',
    default_args=default_args,
    start_date=datetime(2022, 10, 17),
    schedule_interval='00 01 * * *',
    on_success_callback=success_function,
    #email_on_failure=failure_email_function,
    description='SAP_ITEMS_MATERIAL_GROUP',
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

def extract_sap_markitt_items_to_gcs():
    # Initialize your connection
    conn = dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2025'  )
    # cursor_sap = conn.cursor()

    df       = pds.read_sql(f'''SELECT
                        ITEM_CODE
                        , ITEM_DESC
                        , MATERIAL_TYPE
                        , MATERIAL_TYPE_DESC
                        , MATERIAL_GROUP
                        , MATERIAL_GROUP_DESC
                    FROM ETL.ZPBI_ITEMS ''', conn)
                        # df_busline = pds.DataFrame(data=df)
    for char in spec_chars:
        df['ITEM_DESC'] = df['ITEM_DESC'].str.replace(
            char, ' ', regex=True)
        df['MATERIAL_TYPE_DESC'] = df['MATERIAL_TYPE_DESC'].str.replace(
            char, ' ', regex=True)
        df['MATERIAL_GROUP_DESC'] = df['MATERIAL_GROUP_DESC'].str.replace(
            char, ' ', regex=True)

    df['ITEM_DESC'] = df['ITEM_DESC'].str.split().str.join(" ")
    df['MATERIAL_TYPE_DESC'] = df['MATERIAL_TYPE_DESC'].str.split().str.join(" ")
    df['MATERIAL_GROUP_DESC'] = df['MATERIAL_GROUP_DESC'].str.split().str.join(" ")


    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    bucket.blob(
        f'staging/master_tables/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

markitt_data_cis_to_gcs = PythonOperator(
        task_id="ZMARKITT_ITEMS_GCS",
    python_callable=extract_sap_markitt_items_to_gcs,
    dag=SAP_ITEMS_MATERIAL_GROUP
)

deleteItemsBQRecords = BigQueryOperator(
    task_id='DeleteItemsBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
            truncate table `data-light-house-prod.EDW.SAP_ITEM_MATERIAL_GROUP`
        ''',
    dag=SAP_ITEMS_MATERIAL_GROUP
)

markitt_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='markitt_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/master_tables/{filename}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.SAP_ITEM_MATERIAL_GROUP',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'ITEM_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ITEM_DESC', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MATERIAL_TYPE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MATERIAL_TYPE_DESC', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MATERIAL_GROUP', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MATERIAL_GROUP_DESC', 'type': 'STRING', 'mode': 'NULLABLE'}
        # {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=SAP_ITEMS_MATERIAL_GROUP
)

deleteItemsBQRecords>>markitt_data_cis_to_gcs >> markitt_data_gcs_to_bq



