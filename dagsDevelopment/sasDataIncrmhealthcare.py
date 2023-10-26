from pydantic import conset
from google.cloud import bigquery
from google.oauth2 import service_account
# from fastapi import FastAPI
import platform
from tkinter.tix import INTEGER
from hdbcli import dbapi
from numpy import append, integer
import pandas as pd
import sqlalchemy
import urllib
import psycopg2
from datetime import date, datetime, timedelta
import pandas_gbq
from pandas_gbq import schema
# from IBLOps import LogMergingTrans
# from Email import smtpEmailObjectHtml
from typing import Iterator
import pandas as pd
import time
from datetime import date, datetime, timedelta
from pandas.core.frame import DataFrame
from pandas.core.reshape.concat import concat
import os
import sys
import inspect
import warnings
from pathlib import Path
from sqlalchemy import null
warnings.simplefilter(action='ignore', category=FutureWarning)
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
# print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
# print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)

# import datetime


global connectionDB, sqlConnection, records, vStartDate, numberOfRecords
global oracleServerAdd, oraclePort, oracleServiceName, oracleUserName, oraclePassword
oracleConnectionDB = None
sqlConnection = None
records = None
sqlQuery = None
cursor = None
oracleCursor = None
dataFrame1 = pd.DataFrame()
dataFrame2 = pd.DataFrame()
df = pd.DataFrame()
vday = 1
numberOfRecords = 0
total = 0

vPath = '/var/sftp/searle/'
# vPath = Path('/var/sftp/searle')
vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))
conString = '$SPL$'

print(vTodayDate)
if vTodayDate == 1:
    print('two')

    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    vdayDiff = int(vEndDate.strftime("%d"))
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

else:

    vStartDate = datetime.date(datetime.today().replace(day=1))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"

print('Start Date : ', vStartDate)
print('End   Date : ', vEndDate)


def QueryBigQuerySalesData():
    # credentials = service_account.Credentials.from_service_account_file(
    #     'd://data-light-house-prod-0baa98f57152 (1).json')

    global df, dataFrame1, dataFrame2

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json'
    )

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
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


def genSalesTextFile(dataFrame1):

    var = 'BR_CD'+conString + 'BILL_NO'+conString + 'BILL_DT'+conString+'EBS_CUST'+conString+'CH_CD'+conString+'PROD_CD'+conString+'PROD_NM'+conString+'BATCH_NO'+conString+'PRICE' + \
        conString+'SOLD_QTY'+conString+'BON_QTY'+conString+'DISC_AMT'+conString+'NET_AMT'+conString + \
        'GROSS_VALUE'+conString+'DISCOUNTED_RATE' + \
        conString+'REASON'+conString+'DATA_FLAG'+conString
    dataFrame2[var] = dataFrame1['BR_CD']+conString+dataFrame1['BILL_NO']+conString+dataFrame1['BILL_DT']+conString+dataFrame1['EBS_CUST']+conString+dataFrame1['CH_CD']+conString+dataFrame1['ITEM_CODE']+conString+dataFrame1['description']+conString+dataFrame1['BATCH_NO']+conString+dataFrame1['price'].astype(str)+conString+dataFrame1['SOLD_QTY'].astype(
        str)+conString+dataFrame1['BON_QTY'].astype(str)+conString+dataFrame1['disc_amt'].astype(str)+conString+dataFrame1['NET_amt'].astype(str)+conString+dataFrame1['GROSS_VALUE'].astype(str)+conString+dataFrame1['discounted_rate'].astype(str)+conString+dataFrame1['reason']+conString+dataFrame1['data_flag']+conString

    dataFrame2.to_csv(f'''{vPath}HC_ALL_LOC_SALE.txt''',
                      index=False, header=True
                      )

    # print(dataFrame1.info())


def BigQueryCustomerData():

    global df, dataFrame1, dataFrame2

    sqlQuery = f'''
      select distinct
        branch_id BR_CD,replace(CUSTOMER_NUMBER,'.0','')  EBS_CUST,CUSTOMER_NAME
        ,ifnull(CHANNEL,'')  CH_CD
        ,ifnull(address_1,'') ADD1
        ,concat(ifnull(address_2,''),ifnull(address_3,'')) ADD2, data_flag
        from `data-light-house-prod.EDW.VW_EBS_SAS_HC_ALL_LOC_DATA_NEW`
         where billing_date >={vStartDate} and branch_id is not null
         ---and data_flag='OPS'
    '''

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json'
    )

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)
    dataFrame1 = client.query(sqlQuery).to_dataframe()


def genCustomerTextFile():

    global df, dataFrame1, dataFrame2
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


QueryBigQuerySalesData()
genSalesTextFile(df)

BigQueryCustomerData()
genCustomerTextFile()
