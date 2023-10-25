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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

def extract_markitt_sales_to_gcs():
    # Establishing Postgres Connection
    server = '172.20.7.71\SQLSERVER2017'
    db = 'Markitt2021-2022'
    user = 'syed.shujaat'
    password = 'new$5201'
    schema = 'dbo'
    tablePOS = 'INV_PointofSalesDetailTAB'

    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user +';PWD='+password+';TrustServerCertificate=Yes')
    cursor = conn.cursor()
    query = f''' SELECT BranchCode as branch_code, UserName as user_name, BillDate as bill_date, BillNo as bill_no, SerialNo as serial_no, BarCode as bar_code, UnitCode as unit_code, Quantity as quantity, StockQuantity as stock_quantity, Rate as rate, PurAvgRate as pur_avg_rate,Amount as amount, ItemDiscountPercentage as item_discount_percentage, ItemDiscountAmount as item_discount_amount,DiscountAmount as discount_amount, NetAmount as net_amount, isLoyalty as is_loyalty, SupplierCode as supplier_code,trDateTime as tr_datetime, IsImported as is_imported, IsExcluded as is_excluded, DealFactor as deal_factor, DealValue as deal_value,DealOperator as deal_operator, OfferAmount as offer_amount, OfferFactor as offer_factor, NoOfDeals as no_of_deals, DealID as deal_id,POQuantity as 	po_quantity, ItemPurchaseRate as item_purchase_rate, ItemGSTPercentage as item_GST_percentage,ItemGSTAmount as item_GST_amount, NetAmountWithoutGST as net_amount_without_gst
    FROM [%s].%s.%s where cast(BillDate as date)>='2022-01-01' ''' % (
        db, schema, tablePOS)
    df = pds.read_sql(query, conn)
    df['created_at'] = datetime.now() + timedelta(hours=5)
    df.insert(0,"integration_name",'Markitt')
    df.insert(1,"date", dataDate)
    df["date"]= pds.to_datetime(df["date"])
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    filename =f'ibl_markitt_{curr_date}'

    bucket.blob(f'staging/markitt/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()


ibl_markitt = DAG(
    dag_id='ibl_markitt',
    default_args=default_args,
    start_date= datetime(2022, 10, 2),
    schedule_interval='00 01 * * *',
    on_success_callback=success_function,
    #email_on_failure=failure_email_function,
    description='ibl_markitt_data',
)


markitt_data_cis_to_gcs = PythonOperator(
        task_id="markitt_data_cis_to_gcs",
        python_callable=extract_markitt_sales_to_gcs,
        dag=ibl_markitt
)

deleteSalesBQRecords = BigQueryOperator(
    task_id='DeleteSalesBQRecords', bigquery_conn_id='bigquery',
    use_legacy_sql=False, sql=f'''
            delete from  `data-light-house-prod.EDW.IBL_MARKITT_INV_POS` where  bill_date>='2022-01-01'
        ''',
    dag=ibl_markitt
)

markitt_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='markitt_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/markitt/ibl_markitt_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_MARKITT_INV_POS',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'integration', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'dated', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'branch_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'user_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'bill_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'bill_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'serial_no', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'bar_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'unit_code', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'quantity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'stock_quantity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'rate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'pur_avg_rate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'item_discount_percentage', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'item_discount_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'discount_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'net_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'is_loyalty', 'type': 'Bool', 'mode': 'NULLABLE'},
    {'name': 'supplier_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'tr_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'is_imported', 'type': 'Bool', 'mode': 'NULLABLE'},
    {'name': 'is_excluded', 'type': 'Bool', 'mode': 'NULLABLE'},
    {'name': 'deal_factor', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'deal_value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'deal_operator', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'offer_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'offer_factor', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'no_of_deals', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'deal_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'po_quantity', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'item_purchase_rate', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'item_GST_percentage', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'item_GST_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'net_amount_without_gst', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=ibl_markitt
)


deleteSalesBQRecords>>markitt_data_cis_to_gcs >> markitt_data_gcs_to_bq



