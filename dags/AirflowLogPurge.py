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
from google.oauth2.service_account import(Credentials)
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

from suhailLib import ftpUploadDirTransfer,returnDataDate,dirList
import connectionClass
connection=connectionClass
lorealEngine=connection.lorealConnectionAlchemy()
spec_chars=connectionClass.getSpecChars()

storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/data-light-house-prod.json"

vStartDate=None
vEndDate=None
vStartDate,vEndDate=returnDataDate()

creationDate = datetime.today()
now=datetime.now()


# Initialize your connection

utc = timezone.utc

# BigQuery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/data-light-house-prod.json"
credential_file="/home/airflow/airflow/data-light-house-prod.json"
credentials=Credentials.from_service_account_file(credential_file)
bigQueryClient = bigquery.Client()

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

airflowPurgeLogDag = DAG(
    dag_id='airflowPurgeLog',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=10, day=9),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    # schedule_interval=None,
    schedule_interval='0 13 1,15 * *',
    dagrun_timeout=timedelta(minutes=120)
        )

# def convert_to_preferred_format(sec):
#    sec = sec % (24 * 3600)
#    hour = sec // 3600
#    sec %= 3600
#    min = sec // 60
#    sec %= 60
#    return "%02d:%02d:%02d" % (hour, min, sec) 
#    n = 10000
#    return convert(n)

def purgeLog():
    getListDir=dirList('/home/airflow/airflow/logs')
    for x in getListDir:
        if x!='/home/airflow/airflow/logs/scheduler':
            print(x)
            print('----------')
            getSubDirList=dirList(x)
            for y in getSubDirList:
                print('Removing ...',y)
                # shutil.rmtree(y)

airFlowPurgeLogTask=PythonOperator(
                task_id='airFlowPurgeLog'
                ,python_callable=purgeLog
                ,dag=airflowPurgeLogDag
            )

airFlowPurgeLogTask
