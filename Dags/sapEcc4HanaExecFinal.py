from datetime import date, datetime,timedelta
from hmac import trans_36
from pickle import NONE
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
import psycopg2.extras
from pytest import param
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
from http import client
import pandas as pds
import numpy as np
from datalab.context import Context
from datetime import timedelta, date
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.contrib.operators import gcs_to_bq
from airflow.exceptions import AirflowFailException
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
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from io import StringIO
import sqlalchemy
import connectionClass

connection=connectionClass
sapConnection=connection.sapConn()
s4Connection=connection.s4HanaConnection()

conn = sapConnection
s4HanaConn = s4Connection

cursor = conn.cursor()
s4Cursor=s4HanaConn.cursor()

# Variables
vEndDate, vMaxDate, vEndDate1, vMaxDate1 = None, None, None, None

def getDate():
    global vEndDate, vMaxDate, vEndDate1, vMaxDate1
    today = date.today()
    day_diff = 12
    # curr_date = today.strftime("%d-%b-%Y")
    past_date = today - pd.DateOffset(days=day_diff)
    d1 = past_date.strftime("%Y-%m-%d")
    # global df
    df = pd.DataFrame()
    df1 = pd.DataFrame()
    # verify the architecture of Python
    # print("Platform architecture: " + platform.architecture()[0])
    today = date.today()

    vEndDate = datetime.date(
        datetime.today()-timedelta(days=day_diff)).strftime("%Y%m%d")
    vMaxDate = datetime.date(
        datetime.today()-timedelta(days=1)).strftime("%Y%m%d")

    vEndDate1 = datetime.date(
        datetime.today()-timedelta(days=day_diff)).strftime("%Y-%m-%d")

    vMaxDate1 = datetime.date(
        datetime.today()-timedelta(days=1)).strftime("%Y-%m-%d")

    vEndDate = "'"+vEndDate+"'"
    vMaxDate = "'"+vMaxDate+"'"

    vEndDate1 = "'"+vEndDate1+"'"
    vMaxDate1 = "'"+vMaxDate1+"'"

getDate()

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['muhammad.arslan@iblgrp.com'],
    'email_on_failure': True,
    # 'start_date': datetime(2022,2,11)
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

# def send_email(context):
#     #dag_run = context.get('dag_run')
#     msg = "SalesFlo Data Files, Completed...."
#     subject = f"SalesFlo Data Files"
#     send_email_smtp(to=['muhammad.suhail@iblgrp.com']                    # , cc=['muhammad.suhail@iblgrp.com', 'Fahad.Kasiri@habitt.com', 'Umer.Altaf@iblgrp.com', 'Arshad.Ali@habitt.com', 'Muhammad.Fayyaz@Markitt.com', 'Asad.Iqbal@Markitt.com', 'Ali.Babur@habitt.com', 'Noman.Ali@habitt.com', 'Muhammad.Shoaib@Markitt.com', 'orasyscojava@gmail.com']
#                     , subject=subject, html_content=msg
#                     )


eccDataDag = DAG(
    dag_id='EccS4hanaDataExecFinal',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=7, day=6),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    # schedule_interval='30 1 * * *',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120)
    # on_failure_callback=send_email
)


def eccData():
    getSapEccData=f'''
            select
            CONN_STRING,CLIENT,COMPANY,COMPANY_DESC,DOCUMENT_DATE,USER_ID,DOCUMENT_TYPE,DOCUMENT_DESC,PLANT
            ,PLANT_DESC,TRANSACTION_DATE,RECORD_COUNT
            from ecc_bill_execution_data
            where document_date between {vEndDate} and {vMaxDate}
    '''

    df=pd.read_sql(getSapEccData,conn)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    print(df.info())
    print(df)

    df["record_count"] = df["record_count"].astype("int")
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['document_date'] = pd.to_datetime(df['document_date'])

    credentials = service_account.Credentials.from_service_account_file(
        '/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    truncateQuery = f''' delete from EDW.ECC_DATA
                        where document_date>={vEndDate1}
            '''
    project_id = 'data-light-house-prod'
    credentials = service_account.Credentials.from_service_account_file(
        '/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    bigqueryClient = bigquery.Client(
        credentials=credentials, project=project_id)
    print(truncateQuery)
    job = bigqueryClient.query(truncateQuery)

    pandas_gbq.to_gbq(
        df
        ,credentials=credentials
        ,destination_table='data-light-house-prod.EDW.ECC_DATA'
        ,project_id='data-light-house-prod'
        , if_exists='append'
        # ,table_schema=table_schema
    )


def s4Data():
    getSapEccData = f'''
            select
            CONN_STRING,CLIENT,COMPANY,COMPANY_DESC,DOCUMENT_DATE,USER_ID,DOCUMENT_TYPE,DOCUMENT_DESC,PLANT
            ,PLANT_DESC,TRANSACTION_DATE,RECORD_COUNT
            from ecc_bill_execution_data
            where document_date between {vEndDate} and {vMaxDate}
    '''

    df = pd.read_sql(getSapEccData, s4HanaConn)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    print(df.info())
    print(df)

    df["record_count"] = df["record_count"].astype("int")
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['document_date'] = pd.to_datetime(df['document_date'])

    credentials = service_account.Credentials.from_service_account_file(
        '/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    truncateQuery = f''' delete from EDW.S4HANA_DATA
                        where document_date>={vEndDate1}
            '''
    project_id = 'data-light-house-prod'
    credentials = service_account.Credentials.from_service_account_file(
        '/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    bigqueryClient = bigquery.Client(
        credentials=credentials, project=project_id)
    print(truncateQuery)
    job = bigqueryClient.query(truncateQuery)

    pandas_gbq.to_gbq(
        df
        ,credentials=credentials
        , destination_table='data-light-house-prod.EDW.S4HANA_DATA'        , project_id='data-light-house-prod'
        ,if_exists='append'
        # ,table_schema=table_schema
    )


eccDataTask=PythonOperator(
    task_id='EccDataMerging',
    python_callable=eccData,
    dag=eccDataDag
)
s4DataTask = PythonOperator(
    task_id='s4DataMerging',
    python_callable=s4Data,
    dag=eccDataDag
)

eccDataTask
s4DataTask