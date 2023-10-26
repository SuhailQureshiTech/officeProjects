
# import....

from datetime import (date, datetime,timedelta,timezone)
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

from dateutil.relativedelta import relativedelta
from dateutil import parser

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

from suhailLib import ftpUploadDirTransfer,returnDataDate
import connectionClass
connection=connectionClass
lorealEngine=connection.lorealConnectionAlchemy()
spec_chars=connectionClass.getSpecChars()

# import -- End

# storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
# storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

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
today = date.today()

# Initialize your connection

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
# creationDate = date + timedelta(hours=5)
creationDate = date

print('connected')

os.system('clear')

filePath='/home/airflow/Documents/loreal/'
print('file path ............',filePath)

# today=date.today().strftime("%Y%m%d")
vStartDate=None
vEndDate=None

vStartDate,vEndDate=returnDataDate()
vStartDate=int(vStartDate.strftime('%Y%m%d'))+1
vEndDate=int(vEndDate.strftime('%Y%m%d'))
# vStartDate=2023-
print(vStartDate,vEndDate)

creationDate = datetime.today()
now=datetime.now()
current_time = now.time()
currentTime=str(current_time)
timeString=currentTime[0:8].replace(':','')
fileSubString='_CE014_'+str(today.strftime('%Y%m%d'))+str(timeString)


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

lorealDag = DAG(
    dag_id='LorealDataExtraction',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=8, day=29),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    # schedule_interval=None,
    schedule='50 8 * * *',
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



# LOREAL SALES
def lorealSales():
    global vStartDate,vEndDate
    # vStartDate1="'"

    print('vstart date : ',vStartDate)
    print('vend date : ',vEndDate)	
    sql=f''' 
                SELECT
                DISTRIBUTOR_CODE,
                TRANSACTION_TYPE,
                FINAL_CUSTOMER_NUMBER,
                SKU_DESCRIPTION,
                DISTRIBUTOR_PROD_CODE,
                BARCODE,
                ----TO_DATE(SUBSTR(dATE_,1,4)||'-'||SUBSTR(dATE_,5,2)||SUBSTR(dATE_,7,2),'YYYYMMDD')dATE_,
                to_char(to_number(date_))Date_,
                DESTINATION_WAREHOUSE,
                QTY,
                DISTRIBUTOR_CAT,
                NET_SALE,
                CURRENCY_CODE,
                VENDOR_CODE                        
            FROM (
            SELECT
                DISTRIBUTOR_CODE,
                TRANSACTION_TYPE,
                FINAL_CUSTOMER_NUMBER,
                SKU_DESCRIPTION,
                DISTRIBUTOR_PROD_CODE,
                BARCODE,                   
                date_,
                DESTINATION_WAREHOUSE,
                QTY,
                DISTRIBUTOR_CAT,
                NET_SALE,
                CURRENCY_CODE,
                VENDOR_CODE
            FROM
                LOREAL1.FSLS_LOREAL_1 x
                )
            WHERE 1=1 
            AND to_number(Date_) between {vStartDate} and {vEndDate}
            order by to_number(Date_)        
            '''
    lorealDf = pd.read_sql_query(f'''{sql}''',lorealEngine)
    
    # print(test_df)lorealEngine

    # Columns
    lorealDf.columns=['Distributor_code','Transaction_type','Final_Customer_Code','Sku_Description','Distributor_Product_Code'
                    ,'Bar_code','Date' ,'Destination_Warehouse','Quantity','Distributor_Cat','Net_Sale_Amount'
                    ,'Currency_Code','Vendor_Code'
                     ]

    fileName=f'''{filePath}FSLS{fileSubString}.txt'''
    print('file Name : ',fileName)
    lorealDf.to_csv(fileName,index=False,sep=';')

# Loreal FCST
def lorealCustomer():
    lorealFcstDf = pd.read_sql_query(f'''
                                select * from LOREAL1.VW_FCST_LOREAL_1
                                '''                    
                                , lorealEngine)
    
    fileName=f'''{filePath}FCST{fileSubString}.txt'''
    print('file Name : ',fileName)

    lorealFcstDf.to_csv(fileName,index=False,sep=';')

# LOREAL FSTK 
def lorealStock():
    lorealFstktDf = pd.read_sql_query(f'''
                                select * from LOREAL1.VW_FSTK_LOREAL_1
                                '''                    
                                , lorealEngine
    )
    fileName=f'''{filePath}FSTK{fileSubString}.txt'''
    # fileName=f'''{filePath}FCST{fileSubString}.txt'''
    print('file Name : ',fileName)

    lorealFstktDf.to_csv(fileName,index=False,sep=';')

def uploadDirectoryData():
    ftpUploadDirTransfer.sftpDirTransfer(hostName='52.116.35.89',userName='LOPAK_IBL_SFTP',password='heE&j528Hy'
                ,localDirectory='/home/airflow/Documents/loreal'
                ,remoteDirectory='/'
                )

def deleteTextFiles():
    dir = "/home/airflow/Documents/loreal"
    filelist = glob.glob(os.path.join(dir, "*.txt"))
    for f in filelist:
        os.remove(f)

# deleteTextFiles()
# lorealSales()
# lorealCustomer()
# lorealStock()
# uploadDirectoryData()

lorealSalesTask=PythonOperator(   
                    task_id="lorealSales"
                    ,python_callable=lorealSales
                    ,dag=lorealDag
                           )

lorealCustomerTask=PythonOperator(   
                    task_id="lorealCustomer"
                    ,python_callable=lorealCustomer
                    ,dag=lorealDag
                           )

lorealStockTask=PythonOperator(   
                    task_id="lorealStock"
                    ,python_callable=lorealStock
                    ,dag=lorealDag
                           )

lorealUploadFilesTask=PythonOperator(   
                    task_id="uploadFiles"
                    ,python_callable=uploadDirectoryData
                    ,dag=lorealDag
                           )

lorealDelTextFilesTask=PythonOperator(   
                    task_id="deleteTextFiles"
                    ,python_callable=deleteTextFiles
                    ,dag=lorealDag
                           )


lorealDelTextFilesTask>>[lorealSalesTask,lorealCustomerTask,lorealStockTask]>>lorealUploadFilesTask
