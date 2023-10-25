from fileinput import filename
import imp
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

# vStartDate = datetime.date(datetime.today().replace(day=1))
# vEndDate = datetime.date(datetime.today()-timedelta(days=0))

# vFirstDate=vStartDate
# vLastDate = datetime.date(datetime.today())

# vStartDate = "'"+str(vStartDate)+"'"
# vEndDate = "'"+str(vEndDate)+"'"
# vStartDataDate = "'"+str(vStartDataDate)+"'"

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

filePath = f'''/home/admin2/airflow/franchise/'''
global fileName
fileName = 'IblLocationsData.csv'

default_args = {
        'owner': 'admin',
        'start_date': datetime(2022, 9, 27),
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

ibl_location = DAG(
    dag_id='ibl_location',
    default_args=default_args,
    start_date=datetime(year=2022, month=9, day=26),
    schedule_interval='00 06 * * *',
    # dagrun_timeout=timedelta(minutes=60),
    description='ibl_location_data',
)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")


deleteBQRecordsTask = BigQueryOperator(
    task_id='DeleteBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
        truncate table `data-light-house-prod.EDW.LOCATION_DETAILS`
        ''',
    dag=ibl_location
)


def GenerateLocationData():
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

    Location_df=pds.read_sql(f'''
                        SELECT WERKS,LGORT,LGOBE
                        FROM SAPABAP1.T001L WHERE MANDT =300
                        ''',conn1)
    Location_df['transfer_date'] = creationDate

    column_name = ["WERKS", "LGORT", "LGOBE", "transfer_date"]
    Location_df = Location_df.reindex(columns=column_name)

    bucket.blob(f'''staging/temp/{fileName}''').upload_from_string(
        Location_df.to_csv(index=False), 'text/csv')


franchiseSaleDataGenerationTask = PythonOperator(
    task_id="franchiseSaleDataGeneration",
    python_callable=GenerateLocationData,
    dag=ibl_location
)

IblLocation_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='franchiseSaleBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/{fileName}''',
    destination_project_dataset_table='data-light-house-prod.EDW.LOCATION_DETAILS',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
            {'name': 'WERKS', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LGORT', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LGOBE', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
            ],

    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=ibl_location
)
deleteBQRecordsTask >> franchiseSaleDataGenerationTask >> IblLocation_to_BQ
