import os
import sys
import inspect
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
# print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
# print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)

from IBLOps import LogMergingTrans
from Email import smtpEmailObjectHtml
from typing import Iterator
import pypyodbc as odbc
import pandas as pd
import time
import datetime
import cx_Oracle as xo
from datetime import date, datetime, timedelta
from pandas.core.frame import DataFrame
from pandas.core.reshape.concat import concat


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
vday = 1
numberOfRecords = 0
total = 0
# vPath = r"\\192.168.130.81\\Searle\\"
vPath="D:\\TEMP\\SEARLEDATAEXT\\"

vStartDate = datetime.date(datetime.today().replace(day=1))
vEndDate = datetime.date(datetime.today()-timedelta(days=vday))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

print('Start Date : ',vStartDate)
print('End   Date : ',vEndDate)

oracleConnectionDB = xo.connect(
    "apps", 'apps', "192.168.130.41/prod",  encoding="UTF-8")

sqlConnectionPhoneix = odbc.connect("Driver={SQL Server Native Client 11.0};"
                                    "Server=192.168.130.81\sqldw;"
                                    "Database=ibl_dw    ;"
                                    "uid=pbironew;pwd=pbiro345-")


def oraRec4():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1
    sqlQuery = f'''
    select

    trx_number "Invoice",to_char(TRX_DATE,'DD/MM/YYYY') "Date",SAS.CUSTOMER_NUMBER "CustCode"
    ,NVL(SAS.LEGACY_ITEM_CODE,SAS.ITEM_CODE) "ProdCode",'-' "Batch",sas.unit_selling_price "Trade Price"
    ,SAS.SOLD_QTY "Units",SAS.BONUS_QTY "Bonus"
    ,nvl(sas.discount,0)"Discount"
    ,(SAS.SOLD_QTY*SAS.UNIT_SELLING_PRICE)+SAS.DISCOUNT "Net Value"
    ,CASE
        WHEN sas.channel like '%Institutions%' then 'I'
        WHEN sas.channel not like '%Institutions%' then 'S'
        WHEN SAS.REASON_CODE  NOT LIKE '%SALE%' THEN 'R'
    END "Flag"

    --customer
    ,customer_number "Code",cust_name "Name",add1 "Address1",add2 "Address2"
    ,'-' "Phone",'-' "License",'-' "AreaCode",br_cd "AreaName"

    from EBS_SAS_ALL_LOC_DATA_NEW SAS
    where 1=1  AND trx_date between {vStartDate} and {vEndDate}  ORDER BY TRX_DATE
                        '''
    dataFrame1 = pd.read_sql_query(sqlQuery, oracleConnectionDB)

def genSalesTextFile():
    global dataFrame1, dataFrame2
    print('inside salestext file')

    dataFrame1.to_csv(f'''{vPath}ALL_LOC_SALE.csv''',
                    index=False, header=True, sep='$'
                    )

def genCustomerData():
    print('inside customer file')
    dataFrame1.drop(columns=[
        'Invoice', 'ProdCode', 'Batch', 'Trade Price', 'Units', 'Bonus', 'Discount', 'Net Value'
        , 'Date', 'Flag', 'CustCode'], inplace=True)

    dataFrame1.drop_duplicates(inplace=True)

    dataFrame1.to_csv(f'''{vPath}ALL_LOC_INV_CUSTOMER.csv ''',
                    index=False, header=True, sep='$'
                    )

oraRec4()
genSalesTextFile()
genCustomerData()
