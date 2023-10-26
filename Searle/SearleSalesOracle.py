
import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
# print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
# print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)
from google.cloud import bigquery
from pandas.core.reshape.concat import concat
from pandas.core.frame import DataFrame
from datetime import date, datetime, timedelta
from google.oauth2 import service_account
import cx_Oracle as xo
# import datetime
import time
import pandas as pd
from typing import Iterator
import pandas_gbq
from pandas_gbq import schema
from dateutil.relativedelta import relativedelta

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "d:/BQKEY/data-light-house-prod.json"

xo.init_oracle_client(
    lib_dir=r"C:\oraclexe\app\oracle\product\11.2.0\server\bin")

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

oracleConnectionDB = xo.connect('IBLGRPHCM', 'iblgrp106hcm',
                               xo.makedsn('Sap-Router-e416ea262b67e5f4.elb.ap-southeast-1.amazonaws.com', 6464, 'cdb1'))

oracleCursor = oracleConnectionDB.cursor()

def deleteRecords(ftransdate):
    # >={vStartDate}
    try:
        QdelRecords = f'''DELETE FROM EBS_INVOICE_DATA_TBL
                        where 1=1 and to_char(BILL_dt,'yyyy-mm-dd') between {vStartDate} and {vEndDate}
        '''
        # print('deete qyery : ',QdelRecords)
        oracleCursor.execute(QdelRecords)
        oracleConnectionDB.commit()

    except Exception as e:
        oracleConnectionDB.rollback()

def oraRec4():

    global dataFrame1

    credentials = service_account.Credentials.from_service_account_file(
        'd:/BQKEY/data-light-house-prod.json'
        )

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)
    # >={vStartDate}
    sqlQuery = f'''
  SELECT
        FORMAT_DATE('%d-%b-%y',BILLING_DATE) BILL_DT,
        item_code ITEM_CODE,item_desc PROD_NM,
        ORG_ID ORG_SAP_ID,    ORG_DESC ORG_DESC,
        CHANNEL_DESC CHANNEL,DATA_FLAG,
        sum(QUANTITY) SOLD_QTY
        ,sum(AMOUNT) GROSS,round(IFNULL(unit_selling_price,0))  unit_selling_price
        FROM `data-light-house-prod.EDW.VW_SEARLE_SALES`
        WHERE 1=1 and BILLING_DATE  between {vStartDate} and {vEndDate}
        GROUP BY
        FORMAT_DATE('%d-%b-%y',BILLING_DATE) ,        item_code ,item_desc ,
        ORG_ID ,    ORG_DESC ,  CHANNEL_DESC,DATA_FLAG,unit_selling_price
            '''

    dataFrame1 = client.query(sqlQuery).to_dataframe()
    dataFrame1['BILL_DT'] = pd.to_datetime(dataFrame1['BILL_DT'])
    print (dataFrame1)
    rows = [tuple(x) for x in dataFrame1.values]
    insertSalesSql1(rows)

def insertSalesSql1(rec):
    print('insert sql1')

    try:
        oracleCursor.executemany(
            "insert into EBS_INVOICE_DATA_TBL(BILL_DT,ITEM_CODE,PROD_NM,ORG_SAP_ID,ORG_DESC,CHANNEL,DATA_FLAG,SOLD_QTY,GROSS,UNIT_SELLING_PRICE)values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)", rec)


        oracleConnectionDB.commit()
    except Exception as e:
        oracleConnectionDB.rollback()
        print(e)


def closeAllConnection():
    oracleCursor.close()
    oracleConnectionDB.close()
    cursor.close()
    sqlConnection.close()


deleteRecords(vStartDate)
oraRec4()

