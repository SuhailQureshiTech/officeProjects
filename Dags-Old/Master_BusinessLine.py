import time
import requests
import regex as re
from pandas.io.json import json_normalize
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2 as pg
import psycopg2.extras
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from dateutil import parser
from datetime import datetime,date
from datalab.context import Context
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.utils.email import send_email
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
# from sqlalchemy import create_engine
import pyodbc
from hdbcli import dbapi
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import numpy as np

def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "BusinessLine Master DAG has executed successfully."
    subject = f"BusinessLine Master DAG has completed"
    send_email(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)


default_args = {
        'owner': 'admin',    
        'start_date': datetime(2022, 5, 30),
        # 'end_date': datetime(),
        'depends_on_past': False,
        'email_on_failure': True,
        'email': ['shehzad.lalani@iblgrp.com','muhammad.aamir@iblgrp.com'],
        'on_success_callback':success_function,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        }


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

def insert_MasterTable_BUSINESSLINE_To_GCS():
    # Initialize your connection
     conn = dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2025'  )
    # Read data from TVM1T
     TVM1T   = pd.read_sql(f""" SELECT 
        TVM1T.MVGR1 AS business_line_code,
        TVM1T.BEZEI as business_line_description
        FROM SAPABAP1.TVM1T
        WHERE 1 = 1 AND TVM1T.MANDT = '300' AND TVM1T.SPRAS = 'E' """, conn);
     df_TVM1T = pd.DataFrame(data=TVM1T)
     

               
     file_path = r'/home/admin2/airflow/dag/Google_cloud'
     GCS_PROJECT = 'data-light-house-prod'
     GCS_BUCKET = 'ibloper'
     storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
     client = storage.Client(project=GCS_PROJECT)
     bucket = client.get_bucket(GCS_BUCKET)
     
     df_TVM1T['date'] = date.today()
     df_TVM1T['created_at'] = datetime.today()      
     filename_TVM1T =f'ibl_businessline_{curr_date}'   

    
     bucket.blob(f'staging/master_tables/Business_Line_Desc/{filename_TVM1T}.csv').upload_from_string(df_TVM1T.to_csv(index=False), 'text/csv')
     #df_join2.drop(['NEGATIVE_STOCKS_ALLOWED_IN_BRANCH', 'INVENTORY_BALANCE_ALLOWED_IN_BRANCH','MRP_INDICATOR_BRANCH','BRANCH_AUTHORIZATION_FOR_GOODS_MOVEMENT_ACTIVE','BRANCH_RESOURCE_ALLOCATED','HANDLING_UNIT_BRANCH_PARTNER','MES_BUSINESS_SYSTEM','TYPE_OF_INVENTORY_MANAGEMENT_FOR_PRODUCTION_BRANCH','TD_INTRANSIT_FLAG','SILO_MANAGEMENT_TASK_ASSIGNMENT_INDICATOR','BRANCH_PO_BOX','BRANCH_POSTAL_CODE','BATCH_STATUS_MANAGEMENT_ACTIVE','PLANT_LEVEL_CONDITIONS','SOURCE_LIST_REQUIREMENT','ACTIVATING_REQUIREMENTS_PLANNING','BRANCH_COUNTRY_CODE','MAINTENANCE_PLANNING_PLANT','TAX_JURISDICTION','BRANCH_SOP_PLANT','BRANCH_VARIANCE_KEY','TAX_INDICATOR_PLANT_PURCHASING','NO_OF_DAYS_FIRST_REMINDER','NO_OF_DAYS_SECOND_REMINDER','NO_OF_DAYS_THIRD_REMINDER','TEXT_NAME_1ST_DUNNING_VENDOR_DECLARATIONS','TEXT_NAME_2ND_DUNNING_VENDOR_DECLARATIONS','TEXT_NAME_3RD_DUNNING_VENDOR_DECLARATIONS','NO_OF_DAYS_PO_TOLERANCE','DISTRIBUTION_PROFILE_PLANT_LEVEL','NAME_FORMATION_STRUCTURE','EXCHANGE_VALUATION_INDICATOR'], inplace=True,axis = 1)	
     #filename_branches_t = f'ibl_BRANCHES_transformed_{curr_date}'
     #bucket.blob(f'staging/master_tables/Branch/{filename_branches_t}.csv').upload_from_string(df_join2.to_csv(index=False), 'text/csv')    
     
     conn.close()

IBL_BUSINESSLINE_SAP = DAG(
    dag_id='IBL_BUSINESSLINE_SAP',
    default_args=default_args,
    schedule_interval='0 18 * * *',
    catchup=False,
    #schedule_interval='*/30 * * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='IBL_BUSINESSLINE_SAP_TO_BQ',
)
 
t0 = PythonOperator(
    task_id="IBL_BUSINESSLINE_SAP_To_GCS",
    python_callable=insert_MasterTable_BUSINESSLINE_To_GCS,
    dag=IBL_BUSINESSLINE_SAP
)

t1=     GCSToBigQueryOperator(    
    task_id='IBL_BUSINESSLINE_SAP_To_BQ',
    bucket='ibloper',
    source_objects=f'staging/master_tables/Business_Line_Desc/ibl_businessline_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_BUSINESS_LINE',
    schema_fields=[
     # Define schema as per the csv placed in google cloud storage
    {'name': 'business_line_code', 'type': 'STRING', 'mode': 'NULLABLE'} ,
    {'name': 'business_line_description', 'type': 'STRING', 'mode': 'NULLABLE'},    
    {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'} ,
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}           
     ],
     write_disposition='WRITE_TRUNCATE',
     skip_leading_rows = 1,
     dag=IBL_BUSINESSLINE_SAP
  )

t0 >> t1


