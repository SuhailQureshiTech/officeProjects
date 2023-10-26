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
# vPath = r"\\192.168.130.81\\Searle\\"
vPath="D:\\TEMP\\SEARLEDATAEXT\\"

vTodayDate = datetime.date(datetime.today())
vTodayDate=int( vTodayDate.strftime("%d"))
print(type(vTodayDate))

print(vTodayDate)
if  vTodayDate==1:
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

sqlConnection = odbc.connect("Driver={SQL Server Native Client 11.0};"
                                    "Server=192.168.130.81\sqldw;"
                                    "Database=ibl_dw    ;"
                                    "uid=pbironew;pwd=pbiro345-")

cursor = sqlConnection.cursor()

oracleConnectionDB = xo.connect('IBLGRPHCM', 'HRAPPS1406',
                                xo.makedsn('196.16.16.106', 1521, 'sir'))

oracleCursor = oracleConnectionDB.cursor()

# print('Oracle Connection : ', oracleConnectionDB)
# print('oracle Cursor : ',oracleCursor)

def deleteRecords(ftransdate):
    # global oracleConnectionDB,oracleCursor
    # print(vStartDate)
    # print('Oracle Connection : ', oracleConnectionDB)
    # print('oracle Cursor : ',oracleCursor)

    QdelRecords = f'''DELETE FROM SAP_SALES2
                    where 1=1 and trx_date>={ftransdate}
    '''
    try:
        oracleCursor.execute(QdelRecords)
        # oracleCursor.commit()
        oracleConnectionDB.commit()
        # oracleCursor.close()
    except Exception as e:
        oracleConnectionDB.rollback()
        print(f'delRecords Exception : {e}')


def oraRec4():
    # dataFrameHeader=['dddd','dddddxxxx']
    global dataFrame1
    sqlQuery = f'''
    SELECT
        ORG_ID,
        ORG_DESC,
        TRX_DATE,
        TRX_NUMBER,
        BOOKER_ID,
        BOOKER_NAME,
        SUPPLIER_ID,
        SUPPLIER_NAME,business_line_id,business_line,channel,customer_id,customer_number,customer_name
        ,sales_order_type,inventory_item_id,item_code,description,unit_selling_price,sold_qty
        ,bonus_qty,claimable_discount,unclaimable_discount,tax_recoverable,gross,mapping_item_code

    FROM SEARLE_SALES_VIEW
    WHERE TRX_DATE >={vStartDate}
        ORDER BY TRX_DATE
    '''
    recList = []
    Chunksize = 20000

    dataFrame1 = pd.read_sql_query(sqlQuery, sqlConnection)
    # print(dataFrame1)

    rows=[tuple(x) for x in dataFrame1.values]
    insertSalesSql1(rows)

def insertSalesSql1(rec):
    try:
        oracleCursor.executemany(
            "insert into sap_sales2 values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26)", rec
            )


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
closeAllConnection()
