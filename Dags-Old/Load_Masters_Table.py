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
d1 = today.strftime("%Y-%m-%d")


def Load_Branches_Data_to_gcs():  
    
    conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
   # print(current_Datetime)
    df = pds.read_sql(f'''select branch_code ,branch_desc  from "DW"."BRANCHES_VW" ''',conn)
    df['transfer_date'] = datetime.today()
    # filename =f'IBL_SALES_{d1}'
    filename =f'Branches_{d1}'
    bucket.blob(f'staging/master_tables/Branches/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

def Load_Business_line_mapping_Data_to_gcs ():  
    
    conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
   # print(current_Datetime)
    df = pds.read_sql(f'''select code,business_line,business_unit,busline_group,record_status  from "DW"."BUSINESS_LINE_MAPPING"; ''',conn)
    df['transfer_date'] = datetime.today()
    # filename =f'IBL_SALES_{d1}'
    filename =f'Business_Line_Mapping_{d1}'
    bucket.blob(f'staging/master_tables/Business_line_mapping/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

def Load_Customers_Data_to_gcs ():  
    
    conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
   # print(current_Datetime)
    df = pds.read_sql(f'''select customer_number,customer_name  from "DW"."CUSTOMERS"; ''',conn)
    df['transfer_date'] = datetime.today()
    # filename =f'IBL_SALES_{d1}'
    filename =f'Customers_{d1}'
    bucket.blob(f'staging/master_tables/Customers/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

def Load_Business_Line_Desc_Data_to_gcs ():  
    
    conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
   # print(current_Datetime)
    df = pds.read_sql(f'''select business_line_code,business_line_desc from "DW"."BUSINESS_LINE_DESC"; ''',conn)
    df['transfer_date'] = datetime.today()
    # filename =f'IBL_SALES_{d1}'
    filename =f'Business_Line_Desc_{d1}'
    bucket.blob(f'staging/master_tables/Business_Line_Desc/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
    conn.close()

def Load_Productivity_Monthly_Target_Data_to_gcs ():  
    
    conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
   # print(current_Datetime)
    df = pds.read_sql(f'''select business_line_id,terget_month,target_year,target,business_line_name from "DW"."PRODUCTIVITY_MONTHLY_TARGET"; ''',conn)
    df['transfer_date'] = datetime.today()
    # filename =f'IBL_SALES_{d1}'
    filename =f'Productivity_Monthly_Target_{d1}'
    bucket.blob(f'staging/master_tables/Productivity_Monthly_Target/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
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
   dag_id='Load_Master_Tables',
   # start date:28-03-2017
   start_date= datetime(year=2022, month=3, day=16),
   # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
   schedule_interval='30 01 * * *'
) as dag:


    Load_Branches_Data_to_gcs = PythonOperator(
            task_id="Load_Branches_Data_to_gcs",
            python_callable=Load_Branches_Data_to_gcs,
            dag=dag,
    )

    Load_Branches_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_Branches_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Branches/Branches_{d1}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_BRANCHES',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'branch_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'branch_desc', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag
    )
# # # # # # # # # # # # # # # # BUSINESS_LINE_MAPPING # # # # # # # # # # # # # # # #
    Load_Business_line_mapping_Data_to_gcs = PythonOperator(
            task_id="Load_Business_line_mapping_Data_to_gcs",
            python_callable=Load_Business_line_mapping_Data_to_gcs,
            dag=dag,
    )

    Load_Business_line_mapping_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_Business_line_mapping_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Business_line_mapping/Business_Line_Mapping_{d1}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_BUSINESS_LINE_MAPPING',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'business_line_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'business_line_desc', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'business_unit', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'busline_group', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'record_status', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag
    )
# # # # # # # # # # # # # # # # CUSTOMERS # # # # # # # # # # # # # # # #

    Load_Customers_Data_to_gcs = PythonOperator(
            task_id="Load_Customers_Data_to_gcs",
            python_callable=Load_Customers_Data_to_gcs,
            dag=dag,
    )

    Load_Customers_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_Customers_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Customers/Customers_{d1}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_CUSTOMERS',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'customer_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag
    )
# # # # # # # # # # # # # # # # BUSINESS_LINE_DESC # # # # # # # # # # # # # # # #

    Load_Business_Line_Desc_Data_to_gcs = PythonOperator(
            task_id="Load_Business_Line_Desc_Data_to_gcs",
            python_callable=Load_Business_Line_Desc_Data_to_gcs,
            dag=dag,
    )

    Load_Business_Line_Desc_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_Business_Line_Desc_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Business_Line_Desc/Business_Line_Desc_{d1}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_BUSINESS_LINE_DESC',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'business_line_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'business_line_desc', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag
    )

# # # # # # # # # # # # # # # # PRODUCTIVITY_MONTHLY_TARGET # # # # # # # # # # # # # # # #

    Load_Productivity_Monthly_Target_Data_to_gcs = PythonOperator(
            task_id="Load_Productivity_Monthly_Target_Data_to_gcs",
            python_callable=Load_Productivity_Monthly_Target_Data_to_gcs,
            dag=dag,
    )

    Load_Productivity_Monthly_Target_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_Productivity_Monthly_Target_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Productivity_Monthly_Target/Productivity_Monthly_Target_{d1}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_PRODUCTIVITY_MONTHLY_TARGET',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'business_line_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'dated', 'type': 'date', 'mode': 'NULLABLE'},
    {'name': 'year', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'target_value', 'type': 'numeric', 'mode': 'NULLABLE'},
    {'name': 'business_line_desc', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag
    )
Load_Branches_Data_to_gcs>>Load_Branches_data_gcs_to_bq>>Load_Business_line_mapping_Data_to_gcs>>Load_Business_line_mapping_gcs_to_bq>>Load_Customers_Data_to_gcs>>Load_Customers_gcs_to_bq>>Load_Business_Line_Desc_Data_to_gcs>>Load_Business_Line_Desc_gcs_to_bq>>Load_Productivity_Monthly_Target_Data_to_gcs>>Load_Productivity_Monthly_Target_gcs_to_bq


