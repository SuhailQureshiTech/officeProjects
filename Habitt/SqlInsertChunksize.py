
import sys
import os
import inspect
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
# print('Current directory ' + currentdir)
parentdir = os.path.dirname(currentdir)
# print('Parent Directory '+parentdir)
sys.path.insert(0, parentdir)

from IBLOps import LogMergingTrans
from Email import smtpEmailObjectHtml

from datetime import date, datetime, timedelta
from typing import Iterator
import cx_Oracle as xo
import pypyodbc as odbc
import pandas as pd
import time
import datetime

global connectionDB, sqlConnection, records, vStartDate, numberOfRecords
global oracleServerAdd, oraclePort, oracleServiceName, oracleUserName, oraclePassword
oracleConnectionDB = None
sqlConnection = None
records = None
cursor = None
oracleCursor = None
vday = 1
numberOfRecords = 0
total = 0

vStartDate = datetime.date.today()
# datetime.date.today()
vStartDate =vStartDate-timedelta(days=vday)

# vStartDate = datetime.strftime(vStartDate, '%d-%m-%Y')
# datetime.datetime.strftime(vStartDate, '%d-%b-%y')
vStartDate = "'"+str(vStartDate)+"'"
want = []

def sql_func():
    DRIVER = 'SQL Server Native Client 11.0'
    SERVER_NAME = '192.168.130.81\SQLDW'
    DATABASE_NAME = 'IBL_DW'
    PASSWORD = 'pbiro345-'
    UID = 'pbironew'

    return DRIVER, SERVER_NAME, DATABASE_NAME, PASSWORD, UID

DRIVER, SERVER_NAME, DATABASE_NAME, PASSWORD, UID = sql_func()

def connect_String(driver, server, database, uid, password):
    conn_string = f'''
        Driver={driver};
        Server={server};
        Database={database};
        UID={uid};
        PWD={password};
                '''
    return conn_string

def sqlConn():
    try:
        sqlConnection = odbc.connect(connect_String(
            DRIVER, SERVER_NAME, DATABASE_NAME, UID, PASSWORD), autocommit=False)

        # sqlConnection = odbc.connect(r'Driver=SQL Server;Server=192.168.130.81\SQLDW;Database=IBL_DW;Trusted_Connection=yes;')

        return sqlConnection
    except Exception as e:
        print('Exception Raised :', e)


# oracleConnectionDB = xo.connect("apps", 'apps', "192.168.130.41/prod")
oracleConnectionDB = xo.connect('mis', 'mismod',
                                xo.makedsn('196.16.16.106', 1521, 'sir'))

# print('Oracle Connection : ', oracleConnectionDB)

# Obtain a cursor2
start = time.perf_counter()
oracleCursor = oracleConnectionDB.cursor()
sqlConnection = sqlConn()
# print('sql connection : ',sqlConnection)
cursor = sqlConnection.cursor()

def oraRec4():
    sql = f''' select
                ORG_ID,
                TRX_DATE,
                TRX_NUMBER,
                BOOKER_ID,
                BOOKER_NAME,
                SUPPLIER_ID,
                SUPPLIER_NAME,
                BUSINESS_LINE_ID,
                BUSINESS_LINE,
                CHANNEL,
                CUSTOMER_ID,
                CUSTOMER_NUMBER,
                CUSTOMER_NAME,
                SALES_ORDER_TYPE,
                INVENTORY_ITEM_ID,
                ITEM_CODE,
                DESCRIPTION,
                UNIT_SELLING_PRICE,
                SOLD_QTY,
                nvl(BONUS_QTY, 0),
                nvl(CLAIMABLE_DISCOUNT, 0),
                nvl( UN_CLAIMABLE_DISCOUNT, 0),
                nvl( TAX_RECOVERABLE, 0),
                nvl(GROSS, 0)
                from
                IBL_SALE_DISCOUNT_MIS_VW
                where
                trx_date>=TO_DATE({vStartDate},'YYYY-MM-DD')
            '''
    recList = []
    Chunksize = 20000

    deleteRecords(vStartDate)

    for chunk in pd.read_sql_query(sql, oracleConnectionDB, chunksize=Chunksize):
        recList.append(chunk)
        insertSalesSql1(chunk.values.tolist())

def deleteRecords(ftransdate):
    # print(f'del trans date : '+ftransdate)
    # Delete Records....
    QdelRecords = f'''DELETE FROM IBL_SALE_DISCOUNT_MIS_VW
                    where 1=1 and trx_date>={ftransdate}
    '''
    try:
        cursor.execute(QdelRecords)
        cursor.commit()
    except Exception as e:
        cursor.rollback()
        print(f'delRecords Exception : {e}')

def insertSalesSql1(rec):
    sqlInsert = '''
            insert into IBL_SALE_DISCOUNT_MIS_VW
            (
            ORG_ID, TRX_DATE, TRX_NUMBER, BOOKER_ID, BOOKER_NAME, SUPPLIER_ID, SUPPLIER_NAME
            ,BUSINESS_LINE_ID, BUSINESS_LINE, CHANNEL, CUSTOMER_ID, CUSTOMER_NUMBER, CUSTOMER_NAME
            ,SALES_ORDER_TYPE, INVENTORY_ITEM_ID, ITEM_CODE, DESCRIPTION, UNIT_SELLING_PRICE
            ,SOLD_QTY,BONUS_QTY
            ,CLAIMABLE_DISCOUNT
            ,UNCLAIMABLE_DISCOUNT
            ,TAX_RECOVERABLE
            , GROSS
            )
            values
            (
                ?,?,?,?,?,?,?
                ,?,?,?,?,?,?
                ,?,?,?,?,?
                ,?,?
                ,?,?,?,?
            )
            '''
    try:
        cursor.executemany(sqlInsert, rec)
        cursor.commit()
    except Exception as e:
        cursor.rollback()
        print(e)

# def sendConfirmationEmail():
#     vSubjectText = 'Sales Merging'
#     vTo = 'muhammad.suhail@iblgrp.com,orasyscojava@gmail.com'
#     # 'muhammad.suhail@iblgrp.com,orasyscojava@gmail.com,Muhammad.Aamir@iblgrp.com,Mubashir.Amjad@iblgrp.com'
#     vBodyText = 'Sales Merging Completed Successfully'

#     SendGmail.sendGmailAlert(
#         vSubjectText, vTo, vBodyText)

oraRec4()
finish = time.perf_counter()
# print(finish)
totalExecutionTime = format(str(timedelta(seconds=finish-start)))

oracleCursor.close()
oracleConnectionDB.close()
LogMergingTrans.Merging_status('Sales', 'Completed')
smtpEmailObjectHtml.smtpSendEmail('dna@iblgrp.com','muhammad.suhail@iblgrp.com,Muhammad.Aamir@iblgrp.com','Sales Merging','Sales Merging Completed Successfully...')
cursor.close()
sqlConnection.close()
print(f'total execution time : {totalExecutionTime}')
