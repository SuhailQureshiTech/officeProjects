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
from airflow.utils.email import send_email_smtp

connection=connectionClass
sapConnection=connection.sapConn()
conn = sapConnection
cursor = conn.cursor()

# Variables
vEndDate, vMaxDate, vEndDate1, vMaxDate1 = None, None, None, None

def getDate():
    global vEndDate, vMaxDate, vEndDate1, vMaxDate1
    today = date.today()
    day_diff = 4
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


eccDataDagEmail = DAG(
    dag_id='EccS4hanaEmail',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=7, day=6),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    # schedule_interval='30 1 * * *',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120)
    # on_failure_callback=send_email
)


# def send_email(context):
#     #dag_run = context.get('dag_run')
#     msg = "Markitt Sales -- DAG to BigQuery Completed."
#     subject = f"Markitt Sales Integration DAG to BigQuery Completed"
#     send_email_smtp(to=['muhammad.suhail@iblgrp.com']
#                     ,cc=['muhammad.suhail@iblgrp.com', 'Fahad.Kasiri@habitt.com', 'Umer.Altaf@iblgrp.com', 'Arshad.Ali@habitt.com', 'Muhammad.Fayyaz@Markitt.com', 'Asad.Iqbal@Markitt.com', 'Ali.Babur@habitt.com', 'Noman.Ali@habitt.com', 'Muhammad.Shoaib@Markitt.com', 'orasyscojava@gmail.com']
#                     ,subject=subject
#                     ,html_content=msg
#                     )

def eccData():

    getSapEccData=f'''
            select
            ecc_data,
            company,
            company_desc,
            plant,
            plant_desc,
            user_id,
            document_desc,
            document_date,
            s4hana_data,
            ecc_record_count,
            s4hana_record_count,
            count_diff
            from (
            SELECT
            ecc_data,company,company_desc,plant,plant_desc, user_id,document_desc,document_date,s4hana_data
            ,sum(ecc_record_count)ecc_record_count,sum(s4hana_record_count)s4hana_record_count,sum(count_diff)count_diff
            FROM data-light-house-prod.EDW.VW_ECC_S4HANA_EXEC_DATA
            group by ecc_data,company,company_desc,user_id,document_desc,s4hana_data,document_date,plant_desc,plant
            )
            where 1=1 and company='1000' and count_diff<>0
            order by document_date
    '''

    credentials = service_account.Credentials.from_service_account_file(
        '/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    df=pandas_gbq.read_gbq(
        getSapEccData
        ,credentials=credentials
        # ,destination_table='data-light-house-prod.EDW.ECC_DATA'
        ,project_id='data-light-house-prod'
        # ,dialect="legacy",
        # , if_exists='replace'
        # ,table_schema=table_schema
    )

    print(df)
    msg = df.to_html()
    send_email_smtp(to=['muhammad.suhail@iblgrp.com']
                    , cc=['Umer.Altaf@iblgrp.com', 'Waseem.Rasheed@iblgrp.com']
                    , subject='ECC & S4 Hana Parallel Execution Status........'
                    , html_content=msg)

eccDataEmailTask=PythonOperator(
    task_id='EccDataMergingEmail',
    python_callable=eccData,
    dag=eccDataDagEmail
)

eccDataEmailTask
