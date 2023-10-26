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
curr_date = today.strftime("%d-%b-%Y")

def Load_Monthwise_Sale_Data_to_gcs():  
    start_date = date(2021, 7, 1)
    end_date = date(2021, 12, 15)

    dates = [start_date]
    for month in range(start_date.month, end_date.month+1):
        first_day_of_month = date(2021, month, 1)
        last_day_of_month = date(2021, month, calendar.monthrange(2021, month)[1])
# insertion in gcs
        conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
        file_path = r'/home/admin2/airflow/dag/Google_cloud'
        GCS_PROJECT = 'data-light-house-prod'
        GCS_BUCKET = 'ibloper'
        storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
        client = storage.Client(project=GCS_PROJECT)
        bucket = client.get_bucket(GCS_BUCKET)
        first_Day= "'"+first_day_of_month.strftime("%Y-%m-%d")+"'"
        last_day= "'"+last_day_of_month.strftime("%Y-%m-%d")+"'"
        df = pds.read_sql(f'''select item_category,branch_id,branch_description,billing_date,document_no,booker_id,booker_name,supplier_id,supplier_name,business_line_id,business_line_description,channel,customer_id,customer_number,customer_name,sales_order_type,inventory_item_id,item_code,item_description,unit_selling_price,sold_qty,bonus_qty,claimable_discount,unclaimable_discount,tax_recoverable,net_amount,gross_amount,total_discount,reason_code,bill_type_description,company_code,billing_type,sales_order_no,address_1,address_2,address_3,salesflo_order_no,item_no,return_reason_code from 
        "DW"."IBL_SALES" where billing_date between {first_Day} and {last_day} ''',conn)
        df['transfer_date'] = datetime.today()
        
        filename =f'IBL_SALES_{first_day_of_month}'
        bucket.blob(f'staging/sales/FiscalYear/2021-22/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + timedelta(n)


def Load_Daywise_Sale_Data_to_gcs():  
    
    conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    start_date = date(2022, 1, 1)
    end_date = date(2022, 4, 18)
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    for single_date in daterange(start_date, end_date):
        current_Datetime= "'"+single_date.strftime("%Y-%m-%d")+"'"
        current_Datetime_file_name= single_date.strftime("%Y-%m-%d")
        # print(current_Datetime)
        df = pds.read_sql(f'''select item_category,branch_id,branch_description,billing_date,document_no,booker_id,booker_name,supplier_id,supplier_name,business_line_id,business_line_description,channel,customer_id,customer_number,customer_name,sales_order_type,inventory_item_id,item_code,item_description,unit_selling_price,sold_qty,bonus_qty,claimable_discount,unclaimable_discount,tax_recoverable,net_amount,gross_amount,total_discount,reason_code,bill_type_description,company_code,billing_type,sales_order_no,address_1,address_2,address_3,salesflo_order_no,item_no,return_reason_code 
         from "DW"."IBL_SALES" where billing_date={current_Datetime} ''',conn)
        df['transfer_date'] = datetime.today()
        filename =f'IBL_SALES_{current_Datetime_file_name}'
        bucket.blob(f'staging/sales/FiscalYear/2021-22/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

def Load_All_GCS_to_bq():
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("item_category", "STRING"),
            bigquery.SchemaField("branch_id", "STRING"),
            bigquery.SchemaField("branch_description", "STRING"),
            bigquery.SchemaField("billing_date", "date"),
            bigquery.SchemaField("document_no", "STRING"),
            bigquery.SchemaField("booker_id", "STRING"),
            bigquery.SchemaField("booker_name", "STRING"),
            bigquery.SchemaField("supplier_id", "STRING"),
            bigquery.SchemaField("supplier_name", "STRING"),
            bigquery.SchemaField("business_line_id", "STRING"),
            bigquery.SchemaField("business_line_description", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("customer_number", "STRING"),
            bigquery.SchemaField("customer_name", "STRING"),
            bigquery.SchemaField("sales_order_type", "STRING"),
            bigquery.SchemaField("inventory_item_id", "STRING"),
            bigquery.SchemaField("item_code", "STRING"),
            bigquery.SchemaField("item_description", "STRING"),
            bigquery.SchemaField("unit_selling_price", "NUMERIC"),

            bigquery.SchemaField("sold_qty", "NUMERIC"),
            bigquery.SchemaField("bonus_qty", "NUMERIC"),
            bigquery.SchemaField("claimable_discount", "NUMERIC"),
            bigquery.SchemaField("unclaimable_discount", "NUMERIC"),
            bigquery.SchemaField("tax_recoverable", "NUMERIC"),
            bigquery.SchemaField("net_amount", "NUMERIC"),
            bigquery.SchemaField("gross_amount", "NUMERIC"),
            bigquery.SchemaField("total_discount", "NUMERIC"),
            bigquery.SchemaField("reason_code", "STRING"),
            bigquery.SchemaField("bill_type_description", "STRING"),

            bigquery.SchemaField("company_code", "STRING"),
            bigquery.SchemaField("billing_type", "STRING"),
            bigquery.SchemaField("sales_order_no", "STRING"),
            bigquery.SchemaField("address_1", "STRING"),
            bigquery.SchemaField("address_2", "STRING"),
            bigquery.SchemaField("address_3", "STRING"),
            bigquery.SchemaField("salesflo_order_no", "STRING"),
            bigquery.SchemaField("item_no", "STRING"),
            bigquery.SchemaField("return_reason_code", "STRING"),
            bigquery.SchemaField("transfer_date", "timestamp"),


        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "https://storage.cloud.google.com/ibloper/staging/sales/FiscalYear/2021-22/*.csv"

    load_job = client.load_table_from_uri(
        uri, 'data-light-house-prod.EDW.IBL_SALES', job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table('data-light-house-prod.EDW.IBL_SALES')  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))



default_args = {
        'owner': 'admin',
        'depends_on_past': False,
        'email': ['muhammad.arslan@iblgrp.com'],
	    'email_on_failure': True,
        # 'start_date': datetime(2022,2,11)

      }

# dag = models.DAG('Insert_Sales_Data', default_args=default_args,schedule_interval='@daily')
with DAG(
   dag_id='Full_Load_IBL_Sales',
   # start date:28-03-2017
   start_date= datetime(year=2022, month=3, day=16),
   # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
   schedule_interval='30 03 * * *'
) as dag:



    Load_Monthwise_Sale_Data_to_gcs = PythonOperator(
            task_id="Load_Monthwise_Sale_Data_to_gcs",
            python_callable=Load_Monthwise_Sale_Data_to_gcs,
            dag=dag,
    )
    Load_Daywise_Sale_Data_to_gcs = PythonOperator(
            task_id="Load_Daywise_Sale_Data_to_gcs",
            python_callable=Load_Daywise_Sale_Data_to_gcs,
            dag=dag,
    )
    Load_All_GCS_to_bq = PythonOperator(
            task_id="Load_All_GCS_to_bq",
            python_callable=Load_All_GCS_to_bq,
            dag=dag,
    )


    # Load_sales_data_gcs_to_bq = GCSToBigQueryOperator(
    # task_id='Load_sale_data_gcs_to_bq',
    # bucket='ibloper',
    # source_objects=f'staging/sales/FiscalYear/2021-22/IBL_SALES_2022-04-17.csv',
    # destination_project_dataset_table='data-light-house-prod.EDW.IBL_SALES',
    # schema_fields=[
    # # Define schema as per the csv placed in google cloud storage
    # {'name': 'item_category', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'branch_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'branch_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'billing_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    # {'name': 'document_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'booker_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'booker_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'supplier_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'supplier_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'business_line_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'business_line_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'channel', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'customer_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'sales_order_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'inventory_item_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'item_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'item_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'unit_selling_price', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'sold_qty', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'bonus_qty', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'claimable_discount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'unclaimable_discount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'tax_recoverable', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'net_amount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'gross_amount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'total_discount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'reason_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'bill_type_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'company_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'billing_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'sales_order_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'address_1', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'address_2', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'address_3', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'salesflo_order_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'item_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'return_reason_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    # ],
    # write_disposition='WRITE_APPEND',
    # skip_leading_rows = 1,
    # dag=dag
    # )

    Load_Monthwise_Sale_Data_to_gcs>>Load_Daywise_Sale_Data_to_gcs>>Load_All_GCS_to_bq