from datetime import date, datetime,timedelta
from hmac import trans_36
from pickle import NONE
from airflow import DAG
from airflow import models

# import airflow.operators.dummy
# from airflow.operators.dummy import DummyOperator

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
# sys.path.append('/home/airflow/airflow/dags')
from http import client
import pandas as pds
import numpy as np
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
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from io import StringIO
import sqlalchemy
import cx_Oracle as xo
import connectionClass
connection=connectionClass
sapConnection=connection.sapConn()
conn=sapConnection

oracleConnectionDB=connection.oracleIlgrpHcm()
oracleCursor=oracleConnectionDB.cursor()

oracleTable='EBS_INVOICE_DATA_TBL'
vPath = '/var/sftp/searle/'
conString = '$SPL$'

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

vday = 30
numberOfRecords = 0
total = 0

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))

vStartDate = datetime.date(datetime.today()-timedelta(days=vday))
vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"

print('day : ',vTodayDate)

if vTodayDate <=5:
    print('two')

    # from Previous month to current...
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    vdayDiff = int(vEndDate.strftime("%d"))
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))
    today = date.today()
    d = today - relativedelta(months=1)
    vStartDate = date(d.year, d.month, 1)

else:

    vStartDate = datetime.date(datetime.today().replace(day=1))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"

# vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

print('Start Date : ', vStartDate)
print('End   Date : ', vEndDate)

# Initialize your connection

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
# creationDate = date + timedelta(hours=5)
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

searle_merging = DAG(
    dag_id='Searle_Merging',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=8, day=10),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    # schedule_interval=None,
    schedule_interval='0 6 * * *',
    dagrun_timeout=timedelta(minutes=120)
)  

def deleteRecords():
    # >={vStartDate}
    try:
        QdelRecords = f'''DELETE FROM {oracleTable}
                        where 1=1 and to_char(BILL_dt,'yyyy-mm-dd') between '2023-07-01' and '2023-07-31'
        '''
        # print('deete qyery : ',QdelRecords)
        oracleCursor.execute(QdelRecords)
        oracleConnectionDB.commit()

    except Exception as e:
        oracleConnectionDB.rollback()

deleteInvoiceData=PythonOperator(    
                task_id="Deleting_Invoice_Data",
                python_callable=deleteRecords,
                dag=searle_merging)


def insertIblgrpHcmData():
    global dataFrame1

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json'
        )

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)
    # >={vStartDate}
    sqlQuery = f'''SELECT
                FORMAT_DATE('%d-%b-%y',BILLING_DATE) BILL_DT,
                item_code ITEM_CODE,item_desc PROD_NM,
                ORG_ID ORG_SAP_ID,    ORG_DESC ORG_DESC,
                CHANNEL_DESC CHANNEL,DATA_FLAG,
                sum(QUANTITY) SOLD_QTY
                ,sum(AMOUNT) GROSS,round(IFNULL(unit_selling_price,0))  unit_selling_price
                FROM `data-light-house-prod.EDW.VW_SEARLE_SALES`
                WHERE 1=1 and BILLING_DATE  between '2023-07-01' and '2023-07-31'
                GROUP BY
                FORMAT_DATE('%d-%b-%y',BILLING_DATE) ,        item_code ,item_desc ,
                ORG_ID ,    ORG_DESC ,  CHANNEL_DESC,DATA_FLAG,unit_selling_price
            '''

    dataFrame1 = client.query(sqlQuery).to_dataframe()
    dataFrame1['BILL_DT'] = pd.to_datetime(dataFrame1['BILL_DT'])
    print (dataFrame1)
    rows = [tuple(x) for x in dataFrame1.values]
    try:
        oracleCursor.executemany(f"insert into {oracleTable}(BILL_DT,ITEM_CODE,PROD_NM,ORG_SAP_ID,ORG_DESC,CHANNEL,DATA_FLAG,SOLD_QTY,GROSS,UNIT_SELLING_PRICE)values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)", rows)
        oracleConnectionDB.commit()

    except Exception as e:
        oracleConnectionDB.rollback()
        print(e)

insertInvoiceData=PythonOperator(    
                task_id="Inserting_Invoice_Data",
                python_callable=insertIblgrpHcmData,
                dag=searle_merging)

def QueryBigQuerySalesData():
    
    if vTodayDate==1:
        # from Previous month to current...
        vEndDate = datetime.date(datetime.today()-timedelta(days=1))
        vdayDiff = int(vEndDate.strftime("%d"))
        vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))
        today = date.today()
        d = today - relativedelta(months=1)
        vStartDate = date(d.year, d.month, 1)

    else:

        vStartDate = datetime.date(datetime.today().replace(day=1))
        vEndDate = datetime.date(datetime.today()-timedelta(days=1))

    vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
    vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json'
    )

    project_id = 'data-light-house-prod'
    # table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'

    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)
    sql = f'''SELECT
    *
    FROM(
        select branch_id BR_CD ,
        document_no AS BILL_NO,
        trx_date1 AS BILL_DT        ,
        replace(CUSTOMER_NUMBER,'.0','') AS EBS_CUST,
        CUSTOMER_NAME AS CUSTOMER_NAME,
        ifnull(address_1,'-')ADD1,
        ifnull(address_2,   '-') ADD2,
         Ifnull(address_3,'-') ADD3,
        CHANNEL AS CH_CD,
        ITEM_CODE AS ITEM_CODE,
        item_description AS description,
        ' '  AS BATCH_NO,
         unit_selling_price AS price,
            cast(SUM(sold_qty) as string) AS SOLD_QTY,
        cast(SUM(BONUS_QTY) as int) AS BON_QTY,
        cast(SUM(DISCOUNT) as float64) AS disc_amt,
        cast(SUM(NET_AMT) as float64) AS NET_amt,
        cast(SUM(GROSS_VALUE) as float64) AS GROSS_VALUE,
        cast(SUM(discounted_rate) as float64) AS discounted_rate,
         ifnull(case when esa.SALES_ORDER_TYPE = 'Bill Near Exp Sales'
        then 'Near Expiry'
        when esa.SALES_ORDER_TYPE in ('Cancel Bill NE Sales',
            'OPS Cancel. Invoice', 'OPS-Cancel Cred Memo') then 'Cancel'
        when esa.SALES_ORDER_TYPE = 'OPS Sales Tax Cash'
        then 'Sale'
        when esa.SALES_ORDER_TYPE = 'OPS-Sales Returns'
        then 'Return'
        when upper(esa.SALES_ORDER_TYPE) like '%RET%'  then 'Return'
        when upper(esa.SALES_ORDER_TYPE) NOT like '%RET%'  then 'Sale'
        end,' ') as reason   ,data_flag
        from `data-light-house-prod.EDW.VW_EBS_SAS_HC_ALL_LOC_DATA_NEW` ESA
        where 1 = 1 AND billing_date >={vStartDate}
        GROUP BY BR_CD,
        document_no,
        TRX_DATE1      ,
        replace(CUSTOMER_NUMBER,'.0',''),
        CUSTOMER_NAME,
        ESA.UNIT_SELLING_PRICE,
        SALES_ORDER_TYPE,
        address_1,
        address_2,
        address_3,
        CHANNEL,
        ITEM_CODE,
        DESCRIPTION   ,data_flag
		) A
		ORDER BY
	    BILL_DT
        '''

    df = client.query(sql).to_dataframe()
    dataFrame1=df
    dataFrame2=pd.DataFrame()
    print(dataFrame1.info())
    var = 'BR_CD'+conString + 'BILL_NO'+conString + 'BILL_DT'+conString+'EBS_CUST'+conString+'CH_CD'+conString+'PROD_CD'+conString+'PROD_NM'+conString+'BATCH_NO'+conString+'PRICE' + \
        conString+'SOLD_QTY'+conString+'BON_QTY'+conString+'DISC_AMT'+conString+'NET_AMT'+conString + \
        'GROSS_VALUE'+conString+'DISCOUNTED_RATE' +  conString+'REASON'+conString+'DATA_FLAG'+conString
    
    dataFrame2[var] = dataFrame1['BR_CD']+conString+dataFrame1['BILL_NO']+conString+dataFrame1['BILL_DT']+conString+dataFrame1['EBS_CUST']+conString+dataFrame1['CH_CD']+conString+dataFrame1['ITEM_CODE']+conString+dataFrame1['description']+conString+dataFrame1['BATCH_NO']+conString+dataFrame1['price'].astype(str)+conString+dataFrame1['SOLD_QTY'].astype(
        str)+conString+dataFrame1['BON_QTY'].astype(str)+conString+dataFrame1['disc_amt'].astype(str)+conString+dataFrame1['NET_amt'].astype(str)+conString+dataFrame1['GROSS_VALUE'].astype(str)+conString+dataFrame1['discounted_rate'].astype(str)+conString+dataFrame1['reason']+conString+dataFrame1['data_flag']+conString

    dataFrame2.to_csv(f'''{vPath}HC_ALL_LOC_SALE.txt''',
                    index=False, header=True
                    )

# 
def QueryBigQueryCustomerData():
    
    if vTodayDate==1:
        # from Previous month to current...
        vEndDate = datetime.date(datetime.today()-timedelta(days=1))
        vdayDiff = int(vEndDate.strftime("%d"))
        vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))
        today = date.today()
        d = today - relativedelta(months=1)
        vStartDate = date(d.year, d.month, 1)

    else:

        vStartDate = datetime.date(datetime.today().replace(day=1))
        vEndDate = datetime.date(datetime.today()-timedelta(days=1))

    vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
    vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json'
    )

    project_id = 'data-light-house-prod'
    # table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'

    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)
    sql = f'''select distinct
                branch_id BR_CD,replace(CUSTOMER_NUMBER,'.0','')  EBS_CUST,CUSTOMER_NAME
                ,ifnull(CHANNEL,'')  CH_CD
                ,ifnull(address_1,'') ADD1
                ,concat(ifnull(address_2,''),ifnull(address_3,'')) ADD2, data_flag
                from `data-light-house-prod.EDW.VW_EBS_SAS_HC_ALL_LOC_DATA_NEW`
                where billing_date >={vStartDate} and branch_id is not null
        '''

    dataFrame1 = client.query(sql).to_dataframe()
    dataFrame2 = pd.DataFrame()

    dataFrame1['CH_CD'] = dataFrame1['CH_CD'].str.replace(r'\W+', " ")
    dataFrame1['CH_CD'] = dataFrame1['CH_CD'].str.replace(r'\"', " ")
    dataFrame1['ADD1'] = dataFrame1['ADD1'].str.replace(r'\W+', " ")
    dataFrame1['ADD1'] = dataFrame1['ADD1'].str.replace(r'\"', " ")
    dataFrame1['ADD2'] = dataFrame1['ADD2'].str.replace(r'\W+', " ")
    dataFrame1['ADD2'] = dataFrame1['ADD2'].str.replace(r'\"', " ")

    var = 'BR_CD'+conString+'EBS_CUST'+conString+'CUST_NM'+conString+'CH_CD' + \
        conString+'ADD1'+conString+'ADD2'+conString+'DATA_FLAG'+conString

    dataFrame2[var] = dataFrame1['BR_CD']+conString+dataFrame1['EBS_CUST']+conString+dataFrame1['CUSTOMER_NAME']+conString + \
        dataFrame1['CH_CD']+conString+dataFrame1['ADD1']+conString + \
        dataFrame1['ADD2']+conString+dataFrame1['data_flag']+conString

    dataFrame2.to_csv(f'''{vPath}HC_ALL_LOC_INV_CUSTOMER.txt''',
                      index=False, header=True
                      )

generateSalesFileData=PythonOperator(
                    task_id="Sales_File_Data"
                    ,python_callable=QueryBigQuerySalesData
                    ,dag=searle_merging
                )

generateCustomerFileData=PythonOperator(
                    task_id="Customer_File_Data"
                    ,python_callable=QueryBigQueryCustomerData
                    ,dag=searle_merging
                )



[
    deleteInvoiceData>>insertInvoiceData
    ,generateSalesFileData,generateCustomerFileData

]
