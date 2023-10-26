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
from sqlalchemy import create_engine
import requests as req

import connectionClass
connection=connectionClass

sapConnString=connection.sapConnAlchemy()
pioneerEngine = connection.pioneerSqlAlchmy()
spec_chars=connectionClass.getSpecChars()
sapConnEngine=create_engine(sapConnString)

vStartDate=None
vEndDate=None

today = date.today()
creationDate = datetime.today()


storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/data-light-house-prod.json"

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

# bigQueryTable ='data-light-house-prod.EDW.IBL_SALES'

# Initialize your connection

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
# creationDate = date + timedelta(hours=5)
creationDate = date
sapSchema='sapabap1'

def convert_to_preferred_format(sec):
   sec = sec % (24 * 3600)
   hour = sec // 3600
   sec %= 3600
   min = sec // 60
   sec %= 60
   return "%02d:%02d:%02d" % (hour, min, sec) 
   n = 10000
   return convert(n)

def getDate():
    today = date.today()
    vTodayDate = datetime.date(datetime.today())
    vTodayDate = int(vTodayDate.strftime("%d"))
    global vStartDate, vEndDate
    if vTodayDate <= 5:
        from dateutil.relativedelta import relativedelta
        print('if block')
        from dateutil.relativedelta import relativedelta
        d = today - relativedelta(months=2)
        vStartDate = date(d.year, d.month, 1)
        # vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
        vStartDate = vStartDate
        vEndDate = date(today.year, today.month, 1) - relativedelta(days=1)

    else:

        vStartDate = datetime.date(datetime.today().replace(day=1))
        vEndDate = datetime.date(datetime.today()-timedelta(days=0))
        
        # print('else : from date :', vStartDate)
        # print('else : enmd date :', vEndDate)

        # print('else : from date :', vStartDate.strftime('%y%m%d'))
        # print('else : enmd date :', vEndDate)

        print('from date : ',vStartDate)
        print('to date : ',vEndDate)

def truncatePeriodData():    
    global vStartDate,vEndDate
    
    vStartDate1=vStartDate
    vEndDate1=vEndDate
    
    vStartDate1="'"+str(vStartDate1.strftime('%Y-%m-%d'))+"'"
    vEndDate1 = "'"+str(vEndDate1.strftime('%Y-%m-%d'))+"'"
   
    delSql = f'''
                delete from pioneer_schema.pioneer_attendance 
                where cast(punch_datetime as date) between {vStartDate1} and {vEndDate1}
                '''
    # print(delSql)
    pioneerEngine.execute(delSql)

    vStartDate1 = vStartDate
    vEndDate1 = vEndDate

    vStartDate1 = "'"+str(vStartDate1.strftime('%Y%m%d'))+"'"
    vEndDate1 = "'"+str(vEndDate1.strftime('%Y%m%d'))+"'"

    delSql = f'''
               DELETE FROM {sapSchema}.ZTMPOR 
               WHERE DATE1  between {vStartDate1} and {vEndDate1}
                '''

    sapConnEngine.execute(delSql)

def getBulkRecords():

    urlText=f'''http://pioneerattendance.com:94/api/EmployeeData/DateRange/{vStartDate}/{vEndDate}/'''
    api_url=urlText
    response = req.get(url=api_url)
    
    r = response.json()
    df = pd.DataFrame.from_dict(r)

    # os.chdir('d:\\Google Drive - Office\\PythonLab\\PoineerAttendance\\')
    # # print('working dir ',os.getcwd())
    # df.to_csv('bulkRecords.csv',index=False)

    df = df.rename(
        columns={'No': 'transaction_id', 'Employee ID': 'employee_id', 'PunchDatetime': 'punch_datetime'
                 ,'Device No': 'device_no', 'Status': 'status'
                 }
    )

    df['record_datetime'] = creationDate
    df.to_sql('pioneer_attendance',
              schema='pioneer_schema',
              con=pioneerEngine,
              index=False,
              if_exists='append'
              )

    attnSql = 'SELECT * FROM pioneer_schema.vw_pioneer_attendance_rec'
    attendanceDf=pd.read_sql(attnSql,pioneerEngine)
    attendanceDf.insert(0, 'mandt', '300')

    # print(attendanceDf)    
    # print(attendanceDf.info())
    
    attendanceDf['mandt'].astype('str')
    attendanceDf['tmid'].astype('str')
    attendanceDf['cardno'].astype('str')
    attendanceDf['date1'].astype('str')
    attendanceDf['p_day'].astype('str')
    attendanceDf['ismanual'].astype('str')
    attendanceDf['machine'].astype('str')
    attendanceDf['time'].astype('str')
    attendanceDf['inout1'].astype('str')
    attendanceDf['flag'].astype('str')
    
    attendanceDf.to_sql('ztmpor',
              schema=sapSchema,
              con=sapConnEngine,
              index=False,
              if_exists='append'
              )


getDate()
truncatePeriodData()
getBulkRecords()

print('done.................')  