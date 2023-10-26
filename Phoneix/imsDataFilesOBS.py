# from IBLOps import LogMergingTrans
# from Email import smtpEmailObjectHtml
from pydantic import conset
from typing import Iterator
import pypyodbc as odbc
import pandas as pd
import time
from datetime import date, datetime, timedelta
# from calendar import monthrange
import dateutil.relativedelta
from pandas.core.frame import DataFrame
from pandas.core.reshape.concat import concat
import os
import sys
import inspect
import warnings
import pandas_gbq
from pandas_gbq import schema
from google.oauth2 import service_account
from google.cloud import bigquery
from sqlalchemy import between
warnings.simplefilter(action='ignore', category=FutureWarning)
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
# print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
# print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)

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
# vPath = r"\\192.168.130.81\\Searle\\"
vPath = "D:\\IMS\\"

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))
from dateutil.relativedelta import relativedelta

today = date.today()
d = today - relativedelta(months=1)

vStartDate = date(d.year, d.month, 1)
# vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
print(vStartDate)

vEndDate = date(today.year, today.month, 1) - relativedelta(days=1)
# vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"


print(vEndDate)

conString = '$SPL$'

# oracleConnectionDB = xo.connect(
#     "apps", 'apps', "192.168.130.41/prod",  encoding="UTF-8")

# sqlConnection = odbc.connect("Driver={SQL Server Native Client 11.0};"
#                             "Server=192.168.130.81\sqldw;"
#                             "Database=ibl_dw    ;"
#                             "uid=pbironew;pwd=pbiro1234_456")


def salesRec():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1
    credentials = service_account.Credentials.from_service_account_file(
        'D://BQKEY//data-light-house-prod-0baa98f57152 (1).json')

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
# OBS PHARMA EXCLUDED BY RANA 13JUN2022

    dataFrame1 = client.query(sqlQuery).to_dataframe()

def genSalesTextFile():
    # global dataFrame1, dataFrame2
    print('inside salestext file')

    # print(dataFrame1.info())

    dataFrame1.columns=['customer_number'.ljust(15),'item_code'.ljust(15),'qty'.ljust(10),'unit_selling_price'.ljust(10),'value'.ljust(10)]
    dataFrame1.to_csv(f'''{vPath}IMS_OBS_SALES_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )

def customerRec():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1
    credentials = service_account.Credentials.from_service_account_file(
        'D://BQKEY//data-light-house-prod-0baa98f57152 (1).json')

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
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

def genCustomerTextFile():
    # global dataFrame1, dataFrame2
    print('inside CUSTOMER file')

    # print(dataFrame1.info())

    dataFrame1.columns=['br_cd'.ljust(8),'customer_number'.ljust(15),'customer_name'.ljust(30)
                ,'add1'.ljust(50),'add2'.ljust(50),'add3'.ljust(50)]

    dataFrame1.to_csv(f'''{vPath}IMS_OBS_CUSTOMER_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )


def productRec():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1
    credentials = service_account.Credentials.from_service_account_file(
        'D://BQKEY//data-light-house-prod-0baa98f57152 (1).json')

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
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

def genProductTextFile():
    # global dataFrame1, dataFrame2
    print('inside product file')

    # print(dataFrame1.info())

    dataFrame1.columns=['item_code'.ljust(30),'description'.ljust(30)]

    dataFrame1.to_csv(f'''{vPath}IMS_OBS_PRODUCTS_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )

salesRec()
genSalesTextFile()
customerRec()
genCustomerTextFile()
productRec()
genProductTextFile()
