from datetime import date, datetime,timedelta
from hmac import trans_36
from pickle import NONE

from airflow import DAG
# from airflow import models
# import airflow.operators.dummy
# from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
# from airflow.contrib.operators import gcs_to_bq
from airflow.exceptions import AirflowFailException
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryCreateEmptyDatasetOperator,
#     BigQueryDeleteDatasetOperator,
# )
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryCreateEmptyDatasetOperator,
#     BigQueryDeleteDatasetOperator,
# )
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator

import time
from datetime import datetime
from time import time, sleep
from fileinput import filename
# import imp
import  pysftp
from datetime import date
import pandas as pd
import glob
import psycopg2.extras
import psycopg2 as pg
import math
import os
import shutil
from numpy import source
# from airflow.providers.postgres.operators.postgres import PostgresOperator
import platform
from hdbcli import dbapi
import pandas as pd
import urllib
import psycopg2
import numpy as np
import pyodbc
from datetime import date, datetime, timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
from dateutil import parser
from datetime import datetime,date,timezone
import sys
# sys.path.append('/home/airflow/airflow/dags')
from http import client
import pandas as pds
import numpy as np
from datetime import timedelta, date

from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import os

# from sqlalchemy import create_engine
import pandas_gbq
import pysftp
import csv
from datetime import date
import pandas as pd
import calendar
from io import StringIO
import sqlalchemy
import connectionClass
connection=connectionClass
postgresEngine=connection.FranchiseAlchmy()
spec_chars=connectionClass.getSpecChars()


storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/data-light-house-prod.json"
today = date.today()
day_diff=4
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=day_diff)
d1 = past_date.strftime("%Y-%m-%d")
# global df
df = pd.DataFrame()
df1=pd.DataFrame()
# verify the architecture of Python
# print("Platform architecture: " + platform.architecture()[0])
today = date.today()

vEndDate = datetime.date(
    datetime.today()-timedelta(days=day_diff)).strftime("%Y%m%d")
vMaxDate = datetime.date(datetime.today()-timedelta(days=1)).strftime("%Y%m%d")

vEndDate1 = datetime.date(
    datetime.today()-timedelta(days=day_diff)).strftime("%Y-%m-%d")

vMaxDate1 = datetime.date(
    datetime.today()-timedelta(days=1)).strftime("%Y-%m-%d")

vEndDate = "'"+vEndDate+"'"
vMaxDate = "'"+vMaxDate+"'"

vEndDate1 = "'"+vEndDate1+"'"
vMaxDate1 = "'"+vMaxDate1+"'"

bigQueryTable ='data-light-house-prod.EDW.IBL_SALES'

# Initialize your connection

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
# creationDate = date + timedelta(hours=5)
creationDate = date

print('connected')

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['muhammad.arslan@iblgrp.com'],
    'email_on_failure': True,
    # 'start_date': datetime(2022,2,11)
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'gcp_conn_id': 'google_cloud_default'
}

franchise_master_data = DAG(
    dag_id='FranchiseMasterData',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=8, day=29),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    # schedule_interval=None,
    schedule_interval='0 6 * * *',
    dagrun_timeout=timedelta(minutes=120)
        )

def convert_to_preferred_format(sec):
   sec = sec % (24 * 3600)
   hour = sec // 3600
   sec %= 3600
   min = sec // 60
   sec %= 60
   return "%02d:%02d:%02d" % (hour, min, sec) 
   n = 10000
   return convert(n)

# getting Franchise Users
def users():
    import time
    start = time.time()
    table_id = 'data-light-house-prod.EDW.franchise_users'
    df=pd.DataFrame()
    userQuery=f'''SELECT 
                    id,company_code,email,distributor_id,username,"password",created_at
                    ,status,store_name,role_id,cast(location_id as text) location_id
                    FROM users x
    '''
    df=pd.read_sql(userQuery,con=postgresEngine)

    df.columns = df.columns.str.strip()

    df['id']=df['id'].astype('int')
    df['role_id']=df['role_id'].astype('int')
    df['location_id'].fillna(0,inplace=True)
    df['location_id']=df['location_id'].astype('int')
    df['created_at'] = pd.to_datetime(df['created_at'])   
    df['transfer_date'] = creationDate

    # for char in spec_chars:
    #     df['id'] = df['id'].str.replace(
    #         char, ' ', regex=True)
    #     df['id'] = df['id'].str.split().str.join(" ")


    df.to_parquet('users.parquet')
    df1=pd.read_parquet('users.parquet')

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json')

    project_id = 'data-light-house-prod'
    client=bigquery.Client(credentials=credentials,project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET
        )

    filePath = "users.parquet"
    with open(filePath,"rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )  

        load_job.result()  # Waits for the job to complete.

    print(f"Job Completed,{convert_to_preferred_format(time.time() - start)}")

syncUsersData = PythonOperator(
    task_id="syncUsersData"
    ,python_callable=users
    ,dag=franchise_master_data
)

# Locations
def locations():
    import time
    start = time.time()
    table_id = 'data-light-house-prod.EDW.franchise_locations'
    df=pd.DataFrame()
    locationQuery=f'''SELECT 
                    location_id,location_name,branch_code
                    FROM locations 
                '''
        
    df=pd.read_sql(locationQuery,con=postgresEngine)
    df.columns = df.columns.str.strip()
    
    df['location_id'] = df['location_id'].astype('int')
    df['location_name'] = df['location_name'].astype(pd.StringDtype())
    df['transfer_date'] = creationDate

    df.to_parquet('locations.parquet')
    df1=pd.read_parquet('locations.parquet')

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json'
        )

    project_id = 'data-light-house-prod'
    client=bigquery.Client(credentials=credentials,project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    filePath = "locations.parquet"
    with open(filePath,"rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )  

        load_job.result()  # Waits for the job to complete.

    print(f"Job Completed,{convert_to_preferred_format(time.time() - start)}")

syncLocationsData=PythonOperator(
                                    task_id="syncLocationsData"
                                    ,python_callable=locations
                                    ,dag=franchise_master_data
                                  )

# roles
def roles():
    import time
    start = time.time()
    table_id = 'data-light-house-prod.EDW.franchise_roles'
    df=pd.DataFrame()
    locationQuery=f'''SELECT 
                    id,roles_name      FROM roles
                        '''
        
    df=pd.read_sql(locationQuery,con=postgresEngine)
    df.columns = df.columns.str.strip()
    
    df['id'] = df['id'].astype('int')
    df['roles_name'] = df['roles_name'].astype(pd.StringDtype())
    df['transfer_date'] = creationDate

    df.to_parquet('roles.parquet')
    df1=pd.read_parquet('roles.parquet')

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json'
        )

    project_id = 'data-light-house-prod'
    client=bigquery.Client(credentials=credentials,project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    filePath = "roles.parquet"
    with open(filePath,"rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )  

        load_job.result()  # Waits for the job to complete.

    print(f"Job Completed,{convert_to_preferred_format(time.time() - start)}")

syncRolesData=PythonOperator(
                                    task_id="syncRolesData"
                                    ,python_callable=roles
                                    ,dag=franchise_master_data
                                  )

def deleteParquetFiles():
    dir = "./"
    filelist = glob.glob(os.path.join(dir, "*.parquet"))
    for f in filelist:
        os.remove(f)

delParquetFiles=PythonOperator(
                                    task_id="delParquetFiles"
                                    ,python_callable=deleteParquetFiles
                                    ,dag=franchise_master_data
                                  )

# testing functions
# users()
# locations()
# roles()

[syncUsersData,syncLocationsData,syncRolesData]>>delParquetFiles
