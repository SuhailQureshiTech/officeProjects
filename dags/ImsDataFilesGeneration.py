from datetime import date, datetime, timedelta
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
import pysftp
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
from datetime import datetime, date, timezone
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
connection = connectionClass
postgresEngine = connection.FranchiseAlchmy()
spec_chars = connectionClass.getSpecChars()

storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/airflow/airflow/data-light-house-prod.json"
jsonKey = '/home/airflow/airflow/data-light-house-prod.json'
vPath = '/var/sftp/ims/'

today = date.today()
d = today - relativedelta(months=1)

vStartDate = date(d.year, d.month, 1)
vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
vEndDate = date(today.year, today.month, 1) - relativedelta(days=1)
vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"

print('start date : ', vStartDate)
print('end date : ', vEndDate)

utc = timezone.utc
date = datetime.now(utc)
creationDate = date


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

imsDataFiles = DAG(
    dag_id='ImsDataFiles',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=8, day=29),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    # schedule_interval=None,
    schedule_interval='0 6 5 * *',
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


def salesRec():
    dataFrame1=pd.DataFrame()
    credentials = service_account.Credentials.from_service_account_file(
        f"{jsonKey}"
    )

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)
    sqlQuery = f'''
        SELECT
        rpad(CUSTOMER_NUMBER,15,' ')CUSTOMER_NUMBER
        ,rpad(substring(ITEM_CODE,9,30),15,' ')ITEM_CODE
        ,rpad(CAST(SOLD_QTY as String),10,' ')QTY
        ,rpad(CAST(UNIT_SELLING_PRICE as String),18,' ')UNIT_SELLING_PRICE
        ,rpad(CAST(GROSS_AMOUNT as String),10,' ')GROSS_AMOUNT
        FROM (
                SELECT
                CUSTOMER_NUMBER,
                ITEM_CODE,
                SUM(SOLD_QTY) SOLD_QTY,
                UNIT_SELLING_PRICE,
                SUM(gross_amount)  GROSS_AMOUNT
                FROM `data-light-house-prod.EDW.VW_IBL_SALES` isdmvb
                WHERE 1 = 1
                AND billing_date between {vStartDate} AND {vEndDate}
                AND company_code = '6300'
                AND isdmvb.business_line_description IN ('Bio Sciences', 'Healthcare Pharma', 'Mead Johnson', 'Nurturmil Water'
                        , 'Nutraceutical', 'Nutrition' , 'OBS NI', 'Searle Bio Sciences', 'Searle Core Brands', 'Searle Momentum Brands')
                GROUP BY CUSTOMER_NUMBER,
                        ITEM_CODE,
                        UNIT_SELLING_PRICE
                HAVING (GROSS_AMOUNT) <> 0
                ) a
            '''

#   OBS PHARMA EXCLUDED BY RANA 13JUN2022
    dataFrame1 = client.query(sqlQuery).to_dataframe()
    dataFrame1.columns=['customer_number'.ljust(15),'item_code'.ljust(15),'qty'.ljust(10),'unit_selling_price'.ljust(10),'value'.ljust(10)]
    dataFrame1.to_csv(f'''{vPath}IMS_SALES_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )

def customerRec():
    dataFrame1=pd.DataFrame()
    credentials = service_account.Credentials.from_service_account_file(f"{jsonKey}")

    project_id = 'data-light-house-prod'
    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)

    sqlQuery = f'''
            SELECT distinct
            rpad(BR_CD,8,' ')BR_CD
            ,rpad(CUSTOMER_NUMBER,15,' ')CUSTOMER_NUMBER
            ,rpad(CAST(CUSTOMER_NAME as String),30,' ')CUSTOMER_NAME
            ,rpad(CAST(address_1 as String),50,' ')ADD1
            ,rpad(CAST(address_2 as String),50,' ')ADD2
            ,rpad(CAST(address_3 as String),50,' ')ADD3
            FROM (
            SELECT
            branch_description BR_CD
                        ,CUSTOMER_NUMBER
                        ,CUSTOMER_NAME
                        ,address_1
                        ,address_2
                        ,address_3
                        ,SUM(GROSS_AMOUNT)GROSS_AMOUNT
            FROM `data-light-house-prod.EDW.VW_IBL_SALES` isdmvb
            WHERE 1 = 1
            AND billing_date between {vStartDate} AND {vEndDate}
            AND company_code = '6300'
            AND isdmvb.business_line_description IN ('Bio Sciences', 'Healthcare Pharma', 'Mead Johnson', 'Nurturmil Water', 'Nutraceutical', 'Nutrition'
            , 'OBS NI', 'Searle Bio Sciences', 'Searle Core Brands', 'Searle Momentum Brands')
            GROUP BY  branch_description
                        ,CUSTOMER_NUMBER
                        ,CUSTOMER_NAME
                        ,address_1
                        ,address_2
                        ,address_3
            HAVING (GROSS_AMOUNT) <> 0
            ) a
    '''

    dataFrame1 = client.query(sqlQuery).to_dataframe()
    # Removing special characters in dataframe
    dataFrame1['ADD1'] = dataFrame1['ADD1'].str.replace(r'\W+', " ")
    dataFrame1['ADD2'] = dataFrame1['ADD2'].str.replace(r'\W+', " ")
    dataFrame1['ADD3'] = dataFrame1['ADD3'].str.replace(r'\W+', " ")

    dataFrame1.columns=['br_cd'.ljust(8),'customer_number'.ljust(15),'customer_name'.ljust(30)
                ,'add1'.ljust(50),'add2'.ljust(50),'add3'.ljust(50)]

    dataFrame1.to_csv(f'''{vPath}IMS_CUSTOMER_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )

def productRec():
    dataFrame1=pd.DataFrame()
    credentials = service_account.Credentials.from_service_account_file(f"{jsonKey}")

    project_id = 'data-light-house-prod'
    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)

    sqlQuery = f'''
     SELECT distinct
  rpad(substring(ITEM_CODE,9,30),15,' ')ITEM_CODE
  ,rpad(DESCRIPTION,80,' ')DESCRIPTION

FROM (
        SELECT
            ITEM_CODE,item_description DESCRIPTION
        FROM `data-light-house-prod.EDW.VW_IBL_SALES` isdmvb
        WHERE 1 = 1
        AND billing_date between {vStartDate} AND {vEndDate}
        AND company_code = '6300'
        AND isdmvb.business_line_description IN ('Bio Sciences', 'Healthcare Pharma', 'Mead Johnson', 'Nurturmil Water', 'Nutraceutical', 'Nutrition'
        , 'OBS NI', 'Searle Bio Sciences', 'Searle Core Brands', 'Searle Momentum Brands')
        ) a
    '''
    dataFrame1 = client.query(sqlQuery).to_dataframe()
    print('inside product file')
    dataFrame1.columns=['item_code'.ljust(30),'description'.ljust(30)]
    dataFrame1.to_csv(f'''{vPath}IMS_PRODUCTS_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t')


generateImsData=PythonOperator(
    task_id='generateImsData',
    python_callable=salesRec,
    dag=imsDataFiles
)

generateImsCustomerData=PythonOperator(
    task_id='generateImsCustomerData',
    python_callable=customerRec,
    dag=imsDataFiles
)

generateImsItems=PythonOperator(
    task_id='generateImsItemsData',
    python_callable=productRec,
    dag=imsDataFiles
)

# OBS
def obsSalesRec():
    dataFrame1=pd.DataFrame()
    credentials = service_account.Credentials.from_service_account_file(
        f"{jsonKey}"
    )

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)
    sqlQuery = f'''
        SELECT
        rpad(CUSTOMER_NUMBER,15,' ')CUSTOMER_NUMBER
        ,rpad(substring(ITEM_CODE,9,30),15,' ')ITEM_CODE
        ,rpad(CAST(SOLD_QTY as String),10,' ')QTY
        ,rpad(CAST(UNIT_SELLING_PRICE as String),18,' ')UNIT_SELLING_PRICE
        ,rpad(CAST(GROSS_AMOUNT as String),10,' ')GROSS_AMOUNT
        FROM (
        SELECT
        CUSTOMER_NUMBER,
        ITEM_CODE,
        SUM(SOLD_QTY) SOLD_QTY,
        UNIT_SELLING_PRICE,
        SUM(gross_amount)  GROSS_AMOUNT
        FROM `data-light-house-prod.EDW.VW_IBL_SALES` isdmvb
        WHERE 1 = 1
        AND billing_date between {vStartDate} AND {vEndDate}
        AND company_code = '6300'
        AND isdmvb.business_line_description IN ('OBS Pharma')
        GROUP BY CUSTOMER_NUMBER,
                ITEM_CODE,
                UNIT_SELLING_PRICE
        HAVING (GROSS_AMOUNT) <> 0
        ) a
            '''

#   OBS PHARMA EXCLUDED BY RANA 13JUN2022
    dataFrame1 = client.query(sqlQuery).to_dataframe()
    dataFrame1.columns=['customer_number'.ljust(15),'item_code'.ljust(15),'qty'.ljust(10),'unit_selling_price'.ljust(10),'value'.ljust(10)]
    dataFrame1.to_csv(f'''{vPath}OBS_SALES_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )

def obsCustomerRec():
    dataFrame1=pd.DataFrame()
    credentials = service_account.Credentials.from_service_account_file(f"{jsonKey}")

    project_id = 'data-light-house-prod'
    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)

    sqlQuery = f'''
            SELECT distinct
            rpad(BR_CD,8,' ')BR_CD
            ,rpad(CUSTOMER_NUMBER,15,' ')CUSTOMER_NUMBER
            ,rpad(CAST(CUSTOMER_NAME as String),30,' ')CUSTOMER_NAME
            ,rpad(CAST(address_1 as String),50,' ')ADD1
            ,rpad(CAST(address_2 as String),50,' ')ADD2
            ,rpad(CAST(address_3 as String),50,' ')ADD3
            FROM (
            SELECT
            branch_description BR_CD
                        ,CUSTOMER_NUMBER
                        ,CUSTOMER_NAME
                        ,address_1
                        ,address_2
                        ,address_3
                        ,SUM(GROSS_AMOUNT)GROSS_AMOUNT
            FROM `data-light-house-prod.EDW.VW_IBL_SALES` isdmvb
            WHERE 1 = 1
            AND billing_date between {vStartDate} AND {vEndDate}
            AND company_code = '6300'
            AND isdmvb.business_line_description IN ('OBS Pharma')
            GROUP BY  branch_description
                        ,CUSTOMER_NUMBER
                        ,CUSTOMER_NAME
                        ,address_1
                        ,address_2
                        ,address_3
            HAVING (GROSS_AMOUNT) <> 0
            ) a
    '''

    dataFrame1 = client.query(sqlQuery).to_dataframe()
    # Removing special characters in dataframe
    dataFrame1['ADD1'] = dataFrame1['ADD1'].str.replace(r'\W+', " ")
    dataFrame1['ADD2'] = dataFrame1['ADD2'].str.replace(r'\W+', " ")
    dataFrame1['ADD3'] = dataFrame1['ADD3'].str.replace(r'\W+', " ")

    dataFrame1.columns=['br_cd'.ljust(8),'customer_number'.ljust(15),'customer_name'.ljust(30)
                ,'add1'.ljust(50),'add2'.ljust(50),'add3'.ljust(50)]

    dataFrame1.to_csv(f'''{vPath}OBS_CUSTOMER_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )

def obsProductRec():
    dataFrame1=pd.DataFrame()
    credentials = service_account.Credentials.from_service_account_file(f"{jsonKey}")

    project_id = 'data-light-house-prod'
    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)

    sqlQuery = f'''
            SELECT distinct
                    rpad(substring(ITEM_CODE,9,30),15,' ')ITEM_CODE
                    ,rpad(DESCRIPTION,80,' ')DESCRIPTION        
                FROM (
                        SELECT
                            ITEM_CODE,item_description DESCRIPTION
                        FROM `data-light-house-prod.EDW.VW_IBL_SALES` isdmvb
                        WHERE 1 = 1
                        AND billing_date between {vStartDate} AND {vEndDate}
                        AND company_code = '6300'
                        AND isdmvb.business_line_description IN ('OBS Pharma')
                        ) a

                    '''
    dataFrame1 = client.query(sqlQuery).to_dataframe()
    print('inside product file')
    dataFrame1.columns=['item_code'.ljust(30),'description'.ljust(30)]
    dataFrame1.to_csv(f'''{vPath}OBS_PRODUCTS_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t')

generateObsData=PythonOperator(
    task_id='generateObsData',
    python_callable=obsSalesRec,
    dag=imsDataFiles
)

generateObsCustomerData=PythonOperator(
    task_id='generateObsCustomerData',
    python_callable=obsCustomerRec,
    dag=imsDataFiles
)

generateObsItems=PythonOperator(
    task_id='generateObsItemsData',
    python_callable=obsProductRec,
    dag=imsDataFiles
)


[generateImsData,generateImsCustomerData,generateImsItems]
[generateObsData,generateObsCustomerData,generateObsItems]
