from pandas.core.reshape.concat import concat
from pandas.core.frame import DataFrame
from datetime import date, datetime, timedelta
import cx_Oracle as xo
import datetime
import time
import pandas as pd
import pypyodbc as odbc
from typing import Iterator
# from Email import smtpEmailObjectHtml
# from IBLOps import LogMergingTrans
import os
import sys
import inspect

import sqlalchemy as sa
import urllib
import math

currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
# print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
# print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)


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
vday = 1
numberOfRecords = 0
total = 0
df=DataFrame()

# vPath = r"\\192.168.130.81\\Searle\\"
# vPath = "D:\\TEMP\\SEARLEDATAEXT\\"

# xo.init_oracle_client(
#     lib_dir=r"C:\oraclexe\app\oracle\product\11.2.0\server\bin")


# vTodayDate = datetime.date(datetime.today())
# vTodayDate = int(vTodayDate.strftime("%d"))
# print(type(vTodayDate))

# print(vTodayDate)
# if vTodayDate == 1:
#     print('two')

#     vEndDate = datetime.date(datetime.today()-timedelta(days=21))
#     vdayDiff = int(vEndDate.strftime("%d"))
#     vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

# else:

#     vStartDate = datetime.date(datetime.today().replace(day=1))
#     vEndDate = datetime.date(datetime.today()-timedelta(days=1))

# vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
# vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

# print('Start Date : ', vStartDate)
# print('End   Date : ', vEndDate)

oracleConnectionDB = xo.connect('SALESFLO', 'SALES106FLO',
                                xo.makedsn('196.16.16.106', 1521, 'sir'))

oracleCursor = oracleConnectionDB.cursor()


def oraRec4():
    global dataFrame1
    sqlQuery = f'''
        SELECT
        DISTRIBUTOR_NAME,DISTRIBUTOR_CODE,SAP_DISTRIBUTOR_CODE,PJP_CODE,PJP_NAME
        ,PJP_NAME,STORE_NAME,STORE_CODE,SAP_STORE_CODE,SKU_DESCRIPTION
        ,SKU_MANUFACTURER_CODE,SAP_SKU_CODE
        ,ORDER_BOOKER_CODE,SAP_ORDER_BOOKER_CODE,ORDER_BOOKER_NAME,ORDER_NUMBER,ORDER_AMOUNT
        ,ORDER_UNITS,STATUS,ORDER_DATE,DELIVERY_DATE,ADDED_ON,UPDATED_ON
        FROM order_info_2

    '''
    recList = []
    Chunksize = 20000

    dataFrame1 = pd.read_sql_query(sqlQuery, oracleConnectionDB)
    # print(dataFrame1.info())
    dataFrame1.columns = dataFrame1.columns.str.strip()
    dataFrame1["SAP_SKU_CODE"] = dataFrame1["SAP_SKU_CODE"].astype(str)
    # print(dataFrame1)

def insertDataRecords():
    print('inside...')
    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                    "SERVER=192.168.130.81\sqldw;"
                                    "DATABASE=ibl_dw;"
                                    "UID=pbironew;"
                                    "PWD=pbiro345-")

    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params)
                            ,fast_executemany=True
                            )
    # print(engine)

    df_num_of_cols = len(dataFrame1.columns)
    chunknum = math.floor(2100/df_num_of_cols)

    dataFrame1.to_sql('ORDER_INFO',
            schema='dbo',
            con=engine,
            chunksize=chunknum,
            method='multi',
            index=False,
            if_exists='append',

            )

    print('done')

def deleteRecords():

    try:
        QdelRecords = f'''DELETE FROM order_info

        '''
        oracleCursor.execute(QdelRecords)
        oracleConnectionDB.commit()

    except Exception as e:
        oracleConnectionDB.rollback()


oraRec4()
insertDataRecords()
deleteRecords()
