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
day_diff=7
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=day_diff)
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

    vStartDate = datetime.date(datetime.today()-timedelta(days=day_diff))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"


def downloadFiles():
    print('start date :', vStartDate)
    print('end date :',   vEndDate)

    start_date = datetime.strptime("2022-10-17", "%Y-%m-%d")
    end_date = datetime.strptime("2022-10-23", "%Y-%m-%d")

    # difference between each date. D means one day
    D = 'D'

    date_list = pd.date_range(start_date, end_date, freq=D)
    print(f"Creating list of dates starting from {start_date} to {end_date}")
    print(date_list)
    for i in (date_list):
        print(i)

    # if you want dates in string format then convert it into string
    print(date_list.strftime("%Y-%m-%d"))

default_args = {
      'owner': 'admin',
      'depends_on_past': False,
      'email': ['muhammad.arslan@iblgrp.com'],
      'email_on_failure': True,
      # 'start_date': datetime(2022,2,11)
      }

# dag = models.DAG('Insert_Sales_Data', default_args=default_args,schedule_interval='@daily')
with DAG(
    dag_id='LoadSalesOrderData',
    # start date:28-03-2017
    start_date= datetime(year=2023, month=4, day=27),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    # schedule_interval='0 01 * * *',
    schedule_interval=None,
    catchup=False
    ) as dag:

    t1 = PythonOperator(
		task_id='DOWNLOAD_FILE_FROM_FTP',
        python_callable= downloadFiles,
        dag=dag,

	)

t1
