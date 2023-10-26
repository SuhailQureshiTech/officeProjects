
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
from datetime import date, datetime, timedelta, timezone
from google.oauth2.service_account import Credentials
from airflow.contrib.operators import gcs_to_bq
import numpy as np
from regex import F

dataDate = datetime.date(
    datetime.today()-timedelta(days=1)
)
dataDate = "'"+str(dataDate)+"'"

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
creationDate = date + timedelta(hours=5)


filePath = f'''/home/admin2/airflow/franchise/'''
global fileName
fileName = 'FranchiseSales.csv'
# gcsFileName = 'FranchiseSaleData.csv'
GCS_PROJECT = 'data-light-house-prod'
fran_sale_df = pd.DataFrame()

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 8, 9),
    # 'end_date': datetime(),
    # 'email_on_failure': True,
    # 'email': ['shehzad.lalani@iblgrp.com']
    # 'on_success_callback': success_function,
    # 'on_success_callback': dag_success_alert,
    # 'on_failure_callback': failure_email_function,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

franchiseMasterData = DAG(
    dag_id='FranchiseMasterData',
    default_args=default_args,
    start_date=datetime(2022, 9, 29),
    # schedule_interval='00 01 * * *',
    # on_success_callback=success_function,
    # email_on_failure=failure_email_function,
    description='FranchiseMasterData'
)

# Example don't uncommit
# with DAG(
#     dag_id='SAP_SALES',
#     # start date:28-03-2017
#     start_date=datetime(year=2022, month=5, day=30),
#     # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
#         schedule_interval='00 01 * * *') as dag:


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# deleteTempDataFileTask = GoogleCloudStorageDeleteOperator(
#     task_id="DeleteTempDataFile",
#     bucket_name="ibloper",
#     prefix="staging/temp/FranchiseSales"
# )


deleteFranchiseCustomerAddBQRecords = BigQueryOperator(
    task_id='DeleteGrnBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
            truncate table `data-light-house-prod.EDW.FRANCHISE_ADDRESS`
        ''',
    dag=franchiseMasterData
)

def franchiseCustomerAddDataGeneration():
    global fileName
    global fran_sale_df
    print('oracle start date :', dataDate)

    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    conn = pg.connect(host="35.216.168.189", port='5433',
                        database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    df = pds.read_sql(
        f'''
        select
        "RDS_CODE",
        "FRANCHISE_CUSTOMER_CODE",
        "FRANCHISE_CUSTOMER_NAME",
        "IBL_CUSTOMER_CODE",
        "IBL_CUSTOMER_NAME",
        "CUSTOMER_ADDRESS",
        "City"
        from "DW"."FRANCHISE_ADDRESS"
                                ''', conn)

    df.columns = df.columns.str.strip()
    df.replace(',', '', regex=True, inplace=True)
    spec_chars = ["!", '"', "#", "%", "&", "'", "(", ")",
                "*", "+", ",", "-", ".", "/", ":", ";", "<",
                "=", ">", "?", "@", "[", "\\", "]", "^", "_",
                "`", "{", "|", "}", "~", "–"]

    for char in spec_chars:
        df['CUSTOMER_ADDRESS'] = df['CUSTOMER_ADDRESS'].str.replace(char, ' ',regex=True)
    df['CUSTOMER_ADDRESS'] = df['CUSTOMER_ADDRESS'].str.split().str.join(" ")

    # df['transfer_date'] = creationDate

    bucket.blob(f'''staging/temp/FRANCHISE_CUST_ADD.csv''').upload_from_string(
        df.to_csv(index=False), 'text/csv')

franchiseCustomerAddDataGenerationTask = PythonOperator(
    task_id="franchiseCustomerAdd",
    python_callable=franchiseCustomerAddDataGeneration,
    dag=franchiseMasterData
)

franchiseCustomerAddBq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='grnBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/FRANCHISE_CUST_ADD.csv''',
    destination_project_dataset_table='data-light-house-prod.EDW.FRANCHISE_ADDRESS',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'rds_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'franchise_customer_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'franchise_customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ibl_customer_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ibl_customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_address', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=franchiseMasterData
)


# deleteBQRecordsTask >> franchiseSaleDataGenerationTask >> [
#     franchiseSale_to_BQ, salesPostGresTask, salesGCSFilesLoadTask]

deleteFranchiseCustomerAddBQRecords>>[
    franchiseCustomerAddDataGenerationTask] >> franchiseCustomerAddBq
