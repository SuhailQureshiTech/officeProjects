
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
from sqlalchemy.engine import create_engine
from pandas.core.frame import DataFrame
from datetime import date, datetime, timedelta
from google.oauth2 import service_account
import cx_Oracle
# import datetime
import time
import pandas as pd
import pypyodbc as odbc
import sqlalchemy
from typing import Iterator
from Email import smtpEmailObjectHtml
from IBLOps import LogMergingTrans
import pandas_gbq
from pandas_gbq import schema

# xo.init_oracle_client(
#     lib_dir=r"C:\oraclexe\app\oracle\product\11.2.0\server\bin")

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

# print(vStartDate)

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

# vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

print('Start Date : ', vStartDate)
print('End   Date : ', vEndDate)

sqlConnection = odbc.connect("Driver={SQL Server Native Client 11.0};"
					"Server=192.168.130.81\sqldw;"
					"Database=ibl_dw    ;"
					"uid=pbironew;pwd=pbiro1234_456")

cursor = sqlConnection.cursor()


#
def BigQueryData():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1

    # connect_string = 'postgresql+psycopg2://apiuser:apiIbl$$123$$456@35.216.168.189:5433/DATAWAREHOUSE'
    # engine1 = sqlalchemy.create_engine(connect_string)

    credentials = service_account.Credentials.from_service_account_file(
        'D://BQKEY//data-light-house-prod-0baa98f57152 (1).json')

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)

    sqlQuery = f'''
        SELECT
        FORMAT_DATE('%d-%b-%y',BILLING_DATE) BILL_DT,
        item_code ITEM_CODE,item_desc PROD_NM,
        ORG_ID ORG_SAP_ID,    ORG_DESC ORG_DESC,
        CHANNEL_DESC CHANNEL,
        sum(QUANTITY) SOLD_QTY
        ,sum(AMOUNT) GROSS
        FROM `data-light-house-prod.EDW.VW_SEARLE_SALES`
        WHERE BILLING_DATE>={vStartDate}
        GROUP BY
        FORMAT_DATE('%d-%b-%y',BILLING_DATE) ,        item_code ,item_desc ,
        ORG_ID ,    ORG_DESC ,  CHANNEL_DESC

    '''

    df = client.query(sqlQuery).to_dataframe()
    df['BILL_DT'] = pd.to_datetime(df['BILL_DT'])
    # print(df.columns)
    # print(df)

    DIALECT = 'oracle'
    SQL_DRIVER = 'cx_oracle'
    USERNAME = 'IBLGRPHCM' #enter your username
    PASSWORD = 'HRAPPS1406' #enter your password
    HOST = '196.16.16.106' #enter the oracle db host url
    PORT = 1521 # enter the oracle port number
    sid='sir'
    # SERVICE = 'ibl' # enter the oracle db service name

    sid = cx_Oracle.makedsn(HOST, PORT, sid=sid)

    cstr = 'oracle://{user}:{password}@{sid}'.format(
        user=USERNAME,
        password=PASSWORD,
        sid=sid
    )

    engine =  create_engine(
        cstr,
        convert_unicode=False,
        pool_recycle=10,
        pool_size=50,
        # echo=True
    )

    QdelRecords = f'''DELETE FROM ebs_invoice_data_tbl_6
                    where 1=1 and to_char(BILL_dt,'yyyy-mm-dd')>={vStartDate}
            '''

    engine.execute(QdelRecords)

    df.to_sql('ebs_invoice_data_tbl_6',
            con=engine,
            chunksize=1000,
            index=False,
            if_exists='append',
            )

def closeAllConnection():
    oracleCursor.close()
    oracleConnectionDB.close()
    cursor.close()
    sqlConnection.close()

BigQueryData()
