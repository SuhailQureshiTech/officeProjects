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
vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
print(vStartDate)

vEndDate = date(today.year, today.month, 1) - relativedelta(days=1)
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"
print(vEndDate)

conString = '$SPL$'

# oracleConnectionDB = xo.connect(
#     "apps", 'apps', "192.168.130.41/prod",  encoding="UTF-8")

sqlConnection = odbc.connect("Driver={SQL Server Native Client 11.0};"
                            "Server=192.168.130.81\sqldw;"
                            "Database=ibl_dw    ;"
                            "uid=pbironew;pwd=pbiro1234_456")


def salesRec():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1
    sqlQuery = f'''
SELECT
  LEFT(CUSTOMER_NUMBER + REPLICATE(' ', 15), 15) CUSTOMER_NUMBER,
  LEFT(SUBSTRING(ITEM_CODE, 9, 30) + REPLICATE(' ', 15), 15) ITEM_CODE,
  LEFT(CAST(SOLD_QTY AS varchar) + REPLICATE(' ', 10), 10) QTY,
  LEFT(CAST(UNIT_SELLING_PRICE AS varchar) + REPLICATE(' ', 18), 18),
  LEFT(CAST(GROSS_AMOUNT AS varchar) + REPLICATE(' ', 10), 10) GROSS_AMOUNT
FROM (SELECT
  CUSTOMER_NUMBER,
  ITEM_CODE,
  SUM(SOLD_QTY) SOLD_QTY,
  UNIT_SELLING_PRICE,
  SUM(GROSS_AMOUNT) GROSS_AMOUNT
FROM IBL_SALE_DISCOUNT_MIS_VW_BO isdmvb
WHERE 1 = 1
AND TRX_DATE BETWEEN {vStartDate} AND {vEndDate}
AND COMPANY_CODE = 6300
AND isdmvb.BUSINESS_LINE IN ('Bio Sciences', 'Healthcare Pharma', 'Mead Johnson', 'Nurturmil Water', 'Nutraceutical', 'Nutrition'
, 'OBS NI', 'Searle Bio Sciences', 'Searle Core Brands', 'Searle Momentum Brands')
GROUP BY CUSTOMER_NUMBER,
         ITEM_CODE,
         UNIT_SELLING_PRICE
HAVING SUM(GROSS_AMOUNT) <> 0) a
    '''
# OBS PHARMA EXCLUDED BY RANA 13JUN2022

    dataFrame1 = pd.read_sql_query(sqlQuery, sqlConnection)

def genSalesTextFile():
    # global dataFrame1, dataFrame2
    print('inside salestext file')

    # print(dataFrame1.info())

    dataFrame1.columns=['customer_number'.ljust(15),'item_code'.ljust(15),'qty'.ljust(10),'unit_selling_price'.ljust(10),'value'.ljust(10)]
    dataFrame1.to_csv(f'''{vPath}IMS_SALES_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )

def customerRec():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1
    sqlQuery = f'''
      SELECT DISTINCT
        LEFT(BR_CD + REPLICATE(' ', 8), 8) BR_CD
         ,LEFT(CUSTOMER_NUMBER+REPLICATE (' ',15),15)CUSTOMER_NUMBER  
        ,LEFT(CUSTOMER_NAME +REPLICATE (' ',30),30)CUSTOMER_NAME
        ,LEFT(ADD1+REPLICATE(' ',50),50)ADD1 
        ,LEFT(ADD2+REPLICATE(' ',50),50)ADD2
        ,LEFT(ADD3+REPLICATE (' ',50),50)ADD3 
        FROM
        (
        SELECT
            ORG_DESC BR_CD
            ,CUSTOMER_NUMBER
            ,CUSTOMER_NAME
            ,ADD1
            ,ADD2
            ,ADD3
            ,SUM(GROSS_AMOUNT)GROSS_AMOUNT
            FROM
                IBL_SALE_DISCOUNT_MIS_VW_BO isdmvb
                WHERE
                1 = 1
                AND TRX_DATE between {vStartDate} AND {vEndDate}
                AND COMPANY_CODE = 6300
                AND isdmvb .BUSINESS_LINE  IN ('Bio Sciences','Healthcare Pharma','Mead Johnson','Nurturmil Water','Nutraceutical','Nutrition'
                ,'OBS NI','Searle Bio Sciences','Searle Core Brands','Searle Momentum Brands')
                    GROUP BY
            ORG_DESC
            ,CUSTOMER_NUMBER
            ,CUSTOMER_NAME
            ,ADD1
            ,ADD2
            ,ADD3
            HAVING SUM(GROSS_AMOUNT)<>0
        )A
    '''

    dataFrame1 = pd.read_sql_query(sqlQuery, sqlConnection)
    # Removing special characters in dataframe
    dataFrame1['add1'] = dataFrame1['add1'].str.replace(r'\W+', " ")
    dataFrame1['add2'] = dataFrame1['add2'].str.replace(r'\W+', " ")
    dataFrame1['add3'] = dataFrame1['add3'].str.replace(r'\W+', " ")

def genCustomerTextFile():
    # global dataFrame1, dataFrame2
    print('inside CUSTOMER file')

    # print(dataFrame1.info())

    dataFrame1.columns=['br_cd'.ljust(8),'customer_number'.ljust(15),'customer_name'.ljust(30)
                ,'add1'.ljust(50),'add2'.ljust(50),'add3'.ljust(50)]

    dataFrame1.to_csv(f'''{vPath}IMS_CUSTOMER_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )


def productRec():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1
    sqlQuery = f'''
     SELECT DISTINCT 
     LEFT(SUBSTRING(ITEM_CODE,9,30)+REPLICATE(' ',30),20)ITEM_CODE ,LEFT(DESCRIPTION+REPLICATE(' ',80),80)DESCRIPTION 
        FROM
                IBL_SALE_DISCOUNT_MIS_VW_BO isdmvb
                WHERE
                1 = 1
                AND TRX_DATE  between {vStartDate} AND {vEndDate}
                AND COMPANY_CODE = 6300
                AND isdmvb .BUSINESS_LINE  IN ('Bio Sciences','Healthcare Pharma','Mead Johnson','Nurturmil Water','Nutraceutical','Nutrition'
                ,'OBS NI','Searle Bio Sciences','Searle Core Brands','Searle Momentum Brands')
    '''

    dataFrame1 = pd.read_sql_query(sqlQuery, sqlConnection)

def genProductTextFile():
    # global dataFrame1, dataFrame2
    print('inside product file')

    # print(dataFrame1.info())

    dataFrame1.columns=['item_code'.ljust(30),'description'.ljust(30)]

    dataFrame1.to_csv(f'''{vPath}IMS_PRODUCTS_{vStartDate}.txt''',
                    index=False, header=True
                    ,sep='\t'
                    )

salesRec()
genSalesTextFile()
customerRec()
genCustomerTextFile()
productRec()
genProductTextFile()
