import psycopg2 as pg
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
import calendar
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
from dateutil import parser
from datetime import datetime,date
import sys
from http import client
import pandas as pds
import numpy as np
from datalab.context import Context
from datetime import timedelta, date
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.contrib.operators import gcs_to_bq
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
#from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
# from sqlalchemy import create_engine
import pandas_gbq
from IPython.display import display
import pysftp
import csv
from datetime import date
import pandas as pd
import calendar




os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=1)
d1 = past_date.strftime("%Y-%m-%d")


def Load_Sale_Data_to_gcs():  
    
    conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
   # print(current_Datetime)
    df = pds.read_sql(f'''select item_category,branch_id,branch_description,billing_date,document_no,booker_id,booker_name,supplier_id,supplier_name,business_line_id,business_line_description,channel,customer_id,customer_number,customer_name,sales_order_type,inventory_item_id,item_code,item_description,unit_selling_price,sold_qty,bonus_qty,claimable_discount,unclaimable_discount,tax_recoverable,net_amount,gross_amount,total_discount,reason_code,bill_type_description,company_code,billing_type,sales_order_no,address_1,address_2,address_3,salesflo_order_no,item_no,return_reason_code 
         from "DW"."IBL_SALES" where billing_date=current_date-1 ''',conn)
    df['transfer_date'] = datetime.today()
    # filename =f'IBL_SALES_{d1}'
    filename =f'IBL_SALES_{d1}'
    bucket.blob(f'staging/sales/FiscalYear/2021-22/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

default_args = {
        'owner': 'admin',
        'depends_on_past': False,
        'email': ['muhammad.arslan@iblgrp.com'],
	    'email_on_failure': True,
        # 'start_date': datetime(2022,2,11)

      }

# dag = models.DAG('Insert_Sales_Data', default_args=default_args,schedule_interval='@daily')
with DAG(
   dag_id='Incremental_Load_IBL_Sale',
   # start date:28-03-2017
   start_date= datetime(year=2022, month=5, day=7),
   # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
   schedule_interval='00 02 * * *'
) as dag:


    Load_Sale_Data_to_gcs = PythonOperator(
            task_id="Load_Sale_Data_to_gcs",
            python_callable=Load_Sale_Data_to_gcs,
            dag=dag,
    )

    Load_sale_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_sale_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/sales/FiscalYear/2021-22/IBL_SALES_{d1}.csv',
    # source_objects=f'staging/sales/FiscalYear/2021-22/IBL_SALES_2022-05-10.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_SALES',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'item_category', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'branch_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'branch_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'billing_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'document_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'booker_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'booker_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'supplier_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'supplier_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'business_line_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'business_line_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'channel', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'customer_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sales_order_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'inventory_item_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'item_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'item_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'unit_selling_price', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'sold_qty', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'bonus_qty', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'claimable_discount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'unclaimable_discount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'tax_recoverable', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'net_amount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'gross_amount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'total_discount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'reason_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'bill_type_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'company_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'billing_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sales_order_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'address_1', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'address_2', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'address_3', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'salesflo_order_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'item_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'return_reason_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=dag
    )

Load_Sale_Data_to_gcs>>Load_sale_data_gcs_to_bq


