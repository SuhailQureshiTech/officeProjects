from datetime import date, datetime,timedelta
from hmac import trans_36
from airflow import DAG
from airflow import models
import airflow.operators.dummy
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import time
from datetime import datetime
from time import time, sleep
from fileinput import filename
# import imp
import  pysftp
from datetime import date
import pandas as pd
import glob
import pypyodbc as odbc
from pytest import param
import psycopg2 as pg
import math
import os
import shutil
from numpy import source
from airflow.providers.postgres.operators.postgres import PostgresOperator
from google.cloud import storage
import pandas as pds
import os
from google.oauth2 import service_account
from airflow.contrib.operators import gcs_to_bq
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator



os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"

df=pd.DataFrame()
params = None

today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=1)
d1 = past_date.strftime("%Y-%m-%d")


os.system('cls')
today = date.today()

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))

if vTodayDate == 1:
    print('two')

    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    vdayDiff = int(vEndDate.strftime("%d"))
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

else:
    print('else')

    vStartDate = datetime.date(datetime.today()-timedelta(days=6))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"





def Load_Branches():
    df = pd.read_csv(f'/home/admin2/airflow/dags/Master_Files/BRANCH.csv')

    # df.columns = ['ibl_distributor_code','distributor_item_code','ibl_item_code','distributor_item_description','lot_number','expiry_date','stock_qty','stock_value','dated']
    df.columns = ['Branch_Code','Branch_Name','SAP_Branch_Code']
    print(df.head())
    # df['dated'] = datetime.today().strftime('%Y-%m-%d')
    df['transfer_date'] = datetime.today()
    # display(df)


    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
    file_path = r'/home/arslan/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    # buckets = list(storage_client.list_buckets())
    # current_Datetime = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
    past_date = today - pd.DateOffset(days=1)
    current_Datetime = past_date.strftime("%Y-%m-%d")

    # current_Datetime = datetime.today().strftime('%Y-%m-%d')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    filename =f'Branch'
    #filename =f'Franchise_Stock_Data_2022-06-06_{company_code}'
    bucket.blob(f'staging/sales/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')

def Load_Product():
    df = pd.read_excel(f'/home/admin2/airflow/dags/Master_Files/PRODUCT.xlsx',dtype={'GRP_ID':str})
    # f = pd.read_excel(f'/home/admin2/airflow/dags/Master_Files/PRODUCT.xlsx',dtype={'GRP_ID':str})

    # df.columns = ['ibl_distributor_code','distributor_item_code','ibl_item_code','distributor_item_description','lot_number','expiry_date','stock_qty','stock_value','dated']
    df.columns = ['PROD_ID','GRP_ID','CTG','NPROD_ID','PROD_NAME','EFP','TP','CP','SAP_ITEM_CODE']
    print(df.head())
    # df['dated'] = datetime.today().strftime('%Y-%m-%d')
    df['transfer_date'] = datetime.today()
    # display(df)


    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
    file_path = r'/home/arslan/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    # buckets = list(storage_client.list_buckets())
    # current_Datetime = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
    past_date = today - pd.DateOffset(days=1)
    current_Datetime = past_date.strftime("%Y-%m-%d")

    # current_Datetime = datetime.today().strftime('%Y-%m-%d')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    filename =f'Product'
    #filename =f'Franchise_Stock_Data_2022-06-06_{company_code}'
    bucket.blob(f'staging/sales/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')

def Load_Groups():
    df = pd.read_excel(f'/home/admin2/airflow/dags/Master_Files/GROUPS.xlsx',dtype={'GRP_ID':str})

    # df.columns = ['ibl_distributor_code','distributor_item_code','ibl_item_code','distributor_item_description','lot_number','expiry_date','stock_qty','stock_value','dated']
    df.columns = ['GRP_ID','GRP_DESC']
    print(df.head())
    # df['dated'] = datetime.today().strftime('%Y-%m-%d')
    df['transfer_date'] = datetime.today()
    # display(df)


    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
    file_path = r'/home/arslan/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    # buckets = list(storage_client.list_buckets())
    # current_Datetime = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
    past_date = today - pd.DateOffset(days=1)
    current_Datetime = past_date.strftime("%Y-%m-%d")

    # current_Datetime = datetime.today().strftime('%Y-%m-%d')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    filename =f'Group'
    #filename =f'Franchise_Stock_Data_2022-06-06_{company_code}'
    bucket.blob(f'staging/sales/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')






default_args = {
        'owner': 'admin',
        'depends_on_past': False,
        'email': ['muhammad.arslan@iblgrp.com'],
	    'email_on_failure': True,
        # 'start_date': datetime(2022,2,11)

      }

# dag = models.DAG('Insert_Sales_Data', default_args=default_args,schedule_interval='@daily')
with DAG(
   dag_id='Reporting_Data',
   # start date:28-03-2017
   start_date= datetime(year=2022, month=6, day=23),
   # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
   schedule_interval='00 03 * * *') as dag:
   # ###############################################Salesflo########################################



    Load_Branches= PythonOperator(
		task_id='Load_Branches',
        python_callable= Load_Branches,
        dag=dag,
		
	)
    Load_Product= PythonOperator(
		task_id='Load_Product',
        python_callable= Load_Product,
        dag=dag,
		
	)
    Load_Group= PythonOperator(
		task_id='Load_Group',
        python_callable= Load_Groups,
        dag=dag,
		
	)
    Load_Branches_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_Branches_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/sales/Branch.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_BRANCH_MAPPING',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'Branch_Code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Branch_Name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'SAP_Branch_Code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag
    )

    Load_Products_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_Products_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/sales/Product.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_PRODUCT_MAPPING',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'PROD_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'GRP_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'CTG', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'NPROD_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'PROD_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'EFP', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'TP', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'CP', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
     {'name': 'SAP_ITEM_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag
    )

    Load_Groups_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_Groups_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/sales/Group.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_GROUP_MAPPING',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'GRP_ID', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'GRP_DESC', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag
    )

    Load_Branches>>Load_Branches_data_gcs_to_bq>>Load_Product>>Load_Products_data_gcs_to_bq>>Load_Group>>Load_Groups_data_gcs_to_bq

