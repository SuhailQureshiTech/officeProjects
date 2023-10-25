
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
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
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

# dataDate = datetime.date(
#     datetime.today()-timedelta(days=1)
# )

dataDate = datetime.date(datetime.today())

dataDate = "'"+str(dataDate)+"'"

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
creationDate = date + timedelta(hours=5)
# print('Data Date :', dataDate)

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

markitt_sap_reports = DAG(
    dag_id='markitt_sap_reports',
    default_args=default_args,
    catchup=False,
    start_date=datetime(2022, 11, 20),
    schedule_interval='00 01 * * *',
    # on_success_callback=success_function,
    # email_on_failure=failure_email_function,
    description='Markitt Sap T-Code Data Extraction'
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


deleteGrnBQRecords = BigQueryOperator(

    task_id='DeleteGrnBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
            truncate table `data-light-house-prod.EDW.MARKITT_GRN`
        ''',
    dag=markitt_sap_reports
)

# deletePoBQRecords = BigQueryOperator(
#     task_id='DeletePoBQRecords', bigquery_conn_id='bigquery',
#     use_legacy_sql=False, sql=f'''
#             truncate table `data-light-house-prod.EDW.MARKITT_PO`
#         ''',
#     dag=markitt_sap_reports
# )

deletePurchaseBQRecords = BigQueryOperator(
    task_id='DeletePurchaseBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
            delete from  `data-light-house-prod.EDW.MARKITT_PURCHASE` where  "REPORTING_DATE"={dataDate}
        ''',
    dag=markitt_sap_reports
)

def grnDataGeneration():
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
        f'''select
            "Material",
            "Plnt",
            "SLoc",
            "MvT",
            "S",
            "MatDoc",
            "Item",
            "PstngDate",
            "Qty"
            from "DW".vw_markitt_grn
                    ''', conn)

    # print(df.info())

    df['transfer_date'] = creationDate

    bucket.blob(f'''staging/temp/MARKITT_GRN.csv''').upload_from_string(
        df.to_csv(index=False), 'text/csv')

# Markitt PO Data Gen
# def poDataGeneration():
#     global fileName
#     global fran_sale_df
#     print('oracle start date :', dataDate)

#     GCS_PROJECT = 'data-light-house-prod'
#     GCS_BUCKET = 'ibloper'

#     storage_client = storage.Client.from_service_account_json(
#         r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

#     client = storage.Client(project=GCS_PROJECT)
#     bucket = client.get_bucket(GCS_BUCKET)

#     conn = pg.connect(host="35.216.168.189", port='5433',
#                     database="DATAWAREHOUSE", user="postgres",
#                     password="ibl@123@456")

#     df = pds.read_sql(
#         f'''
#         select
#         "REPORTING_DATE",
#         "Purch_Doc",
#         "Item1",
#         "Type",
#         "Cat",
#         "Vendor_supplying_plant",
#         "POrg",
#         "PGr",
#         "Document_Date",
#         "Material",
#         "Short_Text",
#         "Matl_Group",
#         "POH",
#         "D",
#         "I1",
#         "I2",
#         "A",
#         "TrackingNo",
#         "Plnt",
#         "SLoc",
#         "Quantity1",
#         "OUn",
#         "Quantity2",
#         "SKU",
#         "Net_price",
#         "Net_Value"
#         from "DW"."MARKITT_PO"
#                                 ''', conn)

#     df['transfer_date'] = creationDate

#     bucket.blob(f'''staging/temp/MARKITT_PO.csv''').upload_from_string(
#         df.to_csv(index=False), 'text/csv')

# #MARKITT PURCHase
def purchaseDataGeneration():
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
                    database="DATAWAREHOUSE", user="postgres",
                    password="ibl@123@456")

    df = pds.read_sql(
        f'''
        select
        "REPORTING_DATE"
        ,"Plant"
        ,"Batch"
        ,"Material"
        ,"From_Date"
        ,"To_Date"
        ,"Opening_Stock"
        ,"Total_Receipt_Quantaties"
        ,"Total_Issue_Quantities"
        ,"Closing_stock"
        ,"Bun"
        from "DW"."MARKITT_PURCHASE"
        WHERE"REPORTING_DATE"={dataDate}
        '''
        , conn)
    df['transfer_date'] = creationDate

    bucket.blob(f'''staging/temp/MARKITT_PURCHASE.csv''').upload_from_string(
        df.to_csv(index=False), 'text/csv')

grnDataGenerationTask = PythonOperator(
    task_id="markittGrnDataGeneration",
    python_callable=grnDataGeneration,
    dag=markitt_sap_reports
)

# poDataGenerationTask = PythonOperator(
#     task_id="markittPoDataGeneration",
#     python_callable=poDataGeneration,
#     dag=markitt_sap_reports
# )

purchaseDataGenerationTask = PythonOperator(
    task_id="markittPurchaseDataGeneration",
    python_callable=purchaseDataGeneration,
    dag=markitt_sap_reports
)


grnBq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='grnBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/MARKITT_GRN.csv''',
    destination_project_dataset_table='data-light-house-prod.EDW.MARKITT_GRN',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'Material', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Plnt', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'SLoc', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MvT', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'S', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MatDoc', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Item', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'pstngdate', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'QtyinUnE', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'TRANSFER_DATE', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=markitt_sap_reports
)

# poBq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
#     # print('bq file name : ',fileName)
#     task_id='poBQ',
#     bucket='ibloper',
#     source_objects=f'''staging/temp/MARKITT_PO.csv''',
#     destination_project_dataset_table='data-light-house-prod.EDW.MARKITT_PO',
#     schema_fields=[
#         # Define schema as per the csv placed in google cloud storage
#         {'name': 'reporting_date', 'type': 'DATE', 'mode': 'NULLABLE'},
#         {'name': 'purch_doc', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'item1', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'cat', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'vendor_supplying_plant', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'porg', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'pgr', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'document_date', 'type': 'DATE', 'mode': 'NULLABLE'},
#         {'name': 'material', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'short_text', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'matl_group', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'poh', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'd', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'i1', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'i2', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'a', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'trackingno', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'plnt', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'sloc', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'quantity1', 'type': 'FLOAT', 'mode': 'NULLABLE'},
#         {'name': 'oun', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'quantity2', 'type': 'FLOAT', 'mode': 'NULLABLE'},
#         {'name': 'sku', 'type': 'STRING', 'mode': 'NULLABLE'},
#         {'name': 'net_price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
#         {'name': 'net_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
#         {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
#     ],
#     write_disposition='WRITE_APPEND',
#     skip_leading_rows=1,
#     dag=markitt_sap_reports
# )

purchaseBq = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='purchaseBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/MARKITT_PURCHASE.csv''',
    destination_project_dataset_table='data-light-house-prod.EDW.MARKITT_PURCHASE',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'reporting_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'plant', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'batch', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'material', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'from_date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'to_date', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'opening_stock', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'total_receipt_quantaties', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'total_issue_quantities', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'closing_stock', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'bun', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=markitt_sap_reports
)


# dummyOperatorTask=DummyOperator(task_id='dummyTest')
dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=markitt_sap_reports)


# deleteGrnBQRecords >> deletePoBQRecords >> deletePurchaseBQRecords>>[
#     grnDataGenerationTask, poDataGenerationTask, purchaseDataGenerationTask] >> dummy_task >> [grnBq, poBq, purchaseBq]


deleteGrnBQRecords >> [
    grnDataGenerationTask,] >> dummy_task >> [grnBq]

deletePurchaseBQRecords >> [
    purchaseDataGenerationTask ] >> dummy_task >> [purchaseBq]

# [deleteGrnBQRecords,deletePurchaseBQRecords ] >> [
#     grnDataGenerationTask,purchaseDataGenerationTask] >> dummy_task >> [grnBq,purchaseBq]
