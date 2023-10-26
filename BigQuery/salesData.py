from pydantic import conset
from google.cloud import bigquery
import uvicorn
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
import pypyodbc as odbc
import pandas as pd
import time
from datetime import date, datetime, timedelta
from pandas.core.frame import DataFrame
from pandas.core.reshape.concat import concat
import os
import sys
import inspect
import warnings

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
vday = 1
numberOfRecords = 0
total = 0
vPath = r"\\192.168.130.81\\Searle\\"
# vPath = "D:\\TEMP\\SEARLEDATAEXT\\"

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))
# print(type(vTodayDate))

print(vTodayDate)
if vTodayDate == 1:
    print('two')

    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    vdayDiff = int(vEndDate.strftime("%d"))
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

else:

    vStartDate = datetime.date(datetime.today().replace(day=1))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

print('Start Date : ', vStartDate)
print('End   Date : ', vEndDate)

def QueryBigQuerySalesData():
    credentials = service_account.Credentials.from_service_account_file(
        'd://data-light-house-prod-0baa98f57152 (1).json')

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)

    sql = f'''
            SELECT
    *
    FROM(
        select BR_CD,
        TRX_NUMBER AS BILL_NO,
        TRX_DATE1 AS BILL_DT
        ,
        CUSTOMER_NUMBER AS EBS_CUST,
        CUSTOMER_NAME AS CUSTOMER_NAME
        ,
        ifnull(ADD1,'-')ADD1,
        ifnull(ADD2,
            '-') ADD2,
        Ifnull(ADD3,
            '-') ADD3
        ,
        CHANNEL AS CH_CD,
        ITEM_CODE AS ITEM_CODE,
        DESCRIPTION AS description,
        ' '
        AS BATCH_NO
        --MEASURE
        ,
        unit_selling_price AS price
        ,
        SUM(sold_qty) AS SOLD_QTY,
        SUM(BONUS_QTY) AS BON_QTY,
        SUM(DISCOUNT) AS disc_amt,
        SUM(NET_AMT) AS NET_amt,
        SUM(GROSS_VALUE) AS GROSS_VALUE,
        SUM(discounted_rate) AS discounted_rate
        ,
        case when esa.SALES_ORDER_TYPE = 'Bill Near Exp Sales'
        then 'Near Expiry'
        when esa.SALES_ORDER_TYPE in ('Cancel Bill NE Sales',
            'OPS Cancel. Invoice', 'OPS-Cancel Cred Memo') then 'Cancel'
        when esa.SALES_ORDER_TYPE = 'OPS Sales Tax Cash'
        then 'Sale'
        when esa.SALES_ORDER_TYPE = 'OPS-Sales Returns'
        then 'Return'
        end as reason from `data-light-house-prod.EDW.EBS_SAS_ALL_LOC_DATA_NEW` ESA
       where 1 = 1 AND trx_date >='2022-07-01'
        GROUP BY BR_CD,
        TRX_NUMBER,
        TRX_DATE1      ,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        ESA.UNIT_SELLING_PRICE,
        SALES_ORDER_TYPE,
        ADD1,
        ADD2,
        ADD3,
        CHANNEL,
        ITEM_CODE,
        DESCRIPTION
		) A
	ORDER BY
	BILL_DT
        '''


    df = client.query(sql).to_dataframe()
    print(df)

    # table = client.get_table('data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP')
    # pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='append')


QueryBigQuerySalesData()
