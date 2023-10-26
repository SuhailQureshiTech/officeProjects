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
import connectionClass
#import conf

markittSqlServer=connectionClass.markittSqlServer()
sapAlchmy=connectionClass.sapConnAlchemy()

dataDate = datetime.date(
    datetime.today()-timedelta(days=1)
)
dataDate = "'"+str(dataDate)+"'"

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
creationDate = date + timedelta(hours=5)

def failure_function(context):
    #dag_run = context.get('dag_run')
    msg = "Markitt Sales DAG to BigQuery failed."
    subject = f"Markitt Sales DAG to BigQuery failed"
    send_email_smtp(to=['muhammad.suhail@iblgrp.com'], subject=subject, html_content=msg)

default_args = {
        'owner': 'admin',
        'start_date': datetime(2022, 9, 21),
        # 'end_date': datetime(),
        'email_on_failure': True,
        'email': ['shehzad.lalani@iblgrp.com'],
        # 'on_success_callback':success_function,
        #'on_success_callback': dag_success_alert,
        #'on_failure_callback': failure_email_function,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        }

SAP_ZMARKITT_ITEMS = DAG(
    dag_id='SAP_ZMARKITT_ITEMS',
    default_args=default_args,
    start_date=datetime(2022, 10, 2),
    # schedule_interval='00 01 * * *',
    catchup=False,
    # schedule_interval='*/30 * * * *',
    schedule_interval=None,
    # on_success_callback=success_function,
    on_failure_callback=failure_function,
    #email_on_failure=failure_email_function,
    description='ibl_markitt_data'
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

def saMarkittItems():
    conn=sapAlchmy
    df=pds.read_sql(f'''
                        SELECT NORMT,MATNR,MAKTX,ERNAM,MSTAE,ERSDA FROM MARKITT_ITEMS mi
                    ''')
    print(df)

sapMarkittItemsTask=PythonOperator(
    task_id='sapMarkittItemsTask',
    python_callable=saMarkittItems,
    dag=SAP_ZMARKITT_ITEMS
)

def extract_sap_markitt_items_to_gcs():
    # Initialize your connection
    conn = dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2025'  )
    # cursor_sap = conn.cursor()

    df=pds.read_sql(f'''
                    SELECT MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,
                    NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG FROM ETL.VW_ZMARKIT_ITEM ''', conn)
    # df_busline = pds.DataFrame(data=df)
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    filename =f'ZMARKITT_ITEMS_{curr_date}'

    bucket.blob(
        f'staging/markitt/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

#Markitt Duplicate items
def extract_markitt_duplicate_items_to_gcs():
    # Initialize your connection
    conn = dbapi.connect(address='10.210.134.204',
                            port='33015',   user='ETL',  password='Etl@2025')
    # cursor_sap = conn.cursor()

    df = pds.read_sql(f'''
                            SELECT NORMT barcode,MATNR sap_item_code,MAKTX sap_item_desc,cast(ERSDA AS date)creation_date
                            FROM  MARKITT_ITEMS mi2  WHERE NORMT  IN (
                            SELECT NORMT
                            FROM MARKITT_ITEMS mi
                            WHERE 1=1
                            GROUP BY NORMT
                            HAVING count(NORMT)>1
                            ) ORDER BY NORMT ,cast(ERSDA AS date) ''', conn)

    # df_busline = pds.DataFrame(data=df)
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    df['transfer_date']=creationDate
    filename = f'ZMARKITT_DUPLICATE_ITEMS_{curr_date}'

    bucket.blob(
        f'staging/markitt/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()


markitt_duplicate_items_to_gcs = PythonOperator(
    task_id="markitt_duplicate_items_to_gcs",
    python_callable=extract_markitt_duplicate_items_to_gcs,
    dag=SAP_ZMARKITT_ITEMS
)

markitt_data_cis_to_gcs = PythonOperator(
        task_id="ZMARKITT_ITEMS_GCS",
    python_callable=extract_sap_markitt_items_to_gcs,
    dag=SAP_ZMARKITT_ITEMS
)

def extract_sap_markitt_error_log_to_gcs():
    # Initialize your connection
    conn = dbapi.connect(address='10.210.134.204',
                            port='33015',   user='ETL',  password='Etl@2025')
    # cursor_sap = conn.cursor()

    df = pds.read_sql(f'''
                        SELECT
                        MANDT,
                        MARKITT_DOCUMENT_NUMBER,
                        BILLDATE,
                        SAP_ORDER,
                        SAP_ORDER_NUMBER,
                        BARCODE,
                        ITEM_CODE,
                        ITEM_DESC,
                        ORDER_QTY,
                        NET_VALUE,
                        ITEM_NO,
                        ORDER_NUMBER,
                        INVOICE_NUMBER,
                        LOG_REASON,
                        DATA_FLAG
                        FROM ETL.MARKITT_ERROR_LOG
                        '''
                                , conn)
    # df_busline = pds.DataFrame(data=df)
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    df['transfer_date'] = creationDate

    filename = f'MARKITT_ERROR_LOG_{curr_date}'

    bucket.blob(
        f'staging/markitt/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

markitt_error_log_to_gcs = PythonOperator(
    task_id="markitt_error_log_to_gcs",
    python_callable=extract_sap_markitt_error_log_to_gcs,
    dag=SAP_ZMARKITT_ITEMS
)

# deleteSalesBQRecords = BigQueryOperator(
#     task_id='DeleteSalesBQRecords', bigquery_conn_id='bigquery',
#     use_legacy_sql=False, sql=f'''
#             DELETE   FROM `data-light-house-prod.EDW.ZMARKIT_ITEM` WHERE BILLDATE>='2022-01-01'
#         ''',
#     dag=SAP_ZMARKITT_ITEMS
# )

deleteSalesBQRecords = BigQueryOperator(
    task_id='DeleteSalesBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
        truncate table `data-light-house-prod.EDW.ZMARKIT_ITEM`
        ''',
    dag=SAP_ZMARKITT_ITEMS
)

deleteErrorLogBQRecords = BigQueryOperator(
    task_id='deleteErrorLogBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
        truncate table `data-light-house-prod.EDW.MARKITT_ERROR_LOG`
        ''',
    dag=SAP_ZMARKITT_ITEMS
)

deleteDuplicateItemsBQRecords = BigQueryOperator(
    task_id='deleteDuplicateItemsBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
            truncate table `data-light-house-prod.EDW.MARKITT_DUPLICATE_ITEMS`
        ''',
    dag=SAP_ZMARKITT_ITEMS
)

markitt_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='markitt_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/markitt/ZMARKITT_ITEMS_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.ZMARKIT_ITEM',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'MANDT', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BILLNO', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'SERIALNO', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'BRANCH', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BILLDATE', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'MATERIAL', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'QUANTITY', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'RATE', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'AMOUNT', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'ITEMDISCOUNTAMOUNT', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'NETAMOUNT', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'ITEMGSTAMOUNT', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'ZMODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FLAG', 'type': 'STRING', 'mode': 'NULLABLE'}
        # {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=SAP_ZMARKITT_ITEMS
)

markitt_error_log_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='markitt_error_log_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/markitt/MARKITT_ERROR_LOG_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.MARKITT_ERROR_LOG',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'MANDT', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MARKITT_DOCUMENT_NUMBER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BILLDATE', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'SAP_ORDER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'SAP_ORDER_NUMBER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'BARCODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ITEM_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ITEM_DESC', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORDER_QTY', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'NET_VALUE', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'ITEM_NO', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ORDER_NUMBER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'INVOICE_NUMBER', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'LOG_REASON', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DATA_FLAG', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=SAP_ZMARKITT_ITEMS
)

#Markitt item duplicate
markitt_duplicate_items_gcs_to_bq = GCSToBigQueryOperator(
    task_id='markitt_duplicate_items_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/markitt/ZMARKITT_DUPLICATE_ITEMS_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.MARKITT_DUPLICATE_ITEMS',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'barcode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sap_item_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sap_item_desc', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'creation_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=SAP_ZMARKITT_ITEMS
)

sapMarkittItemsTask
deleteSalesBQRecords>>markitt_data_cis_to_gcs >> markitt_data_gcs_to_bq
deleteErrorLogBQRecords>>markitt_error_log_to_gcs>>markitt_error_log_data_gcs_to_bq
deleteDuplicateItemsBQRecords>>markitt_duplicate_items_to_gcs>>markitt_duplicate_items_gcs_to_bq



