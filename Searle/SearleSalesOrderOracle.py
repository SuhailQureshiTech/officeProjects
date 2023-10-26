
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
import pypyodbc as odbc
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

print('day : ',vTodayDate)

if vTodayDate <=5:
    print('two')

    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    vdayDiff = int(vEndDate.strftime("%d"))
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

else:
    print('else')

    vStartDate = datetime.date(datetime.today().replace(day=1))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"

# vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

print('Start Date : ', vStartDate)
print('End   Date : ', vEndDate)

oracleConnectionDB = xo.connect('IBLGRPHCM', 'HRAPPS1406',
                                xo.makedsn('196.16.16.106', 1521, 'sir'))

oracleCursor = oracleConnectionDB.cursor()

def deleteRecords(ftransdate):
    print('vstart date :',vStartDate)
    try:
        QdelRecords = f'''DELETE FROM EBS_INVOICE_ORDER_TBL
                        where 1=1 and to_char(INVOICE_DATE_IBL,'yyyy-mm-dd')>={vStartDate}
        '''
        # print('deete qyery : ',QdelRecords)
        oracleCursor.execute(QdelRecords)
        oracleConnectionDB.commit()

    except Exception as e:
        oracleConnectionDB.rollback()

def oraRec4():

    global dataFrame1

    credentials = service_account.Credentials.from_service_account_file(
        'D://BQKEY//data-light-house-prod-0baa98f57152 (1).json')

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)

    sqlQuery = f'''
  SELECT
        institution,
        branch_id,
        branch_name,
        distributor,
        ins_type,
        item_code,
        sku,
        selling_price,
        claimable_discount,
        un_claimable_discount,
        inst_discount,
        Month,
        order_ref_no,
        order_quantity,
        foc,
        total_qty,
        sales_vlaue,
        invoice_date_ibl,
        invoice_no_ibl,
        tax_recoverable
        FROM `data-light-house-prod.EDW.EBS_INVOICE_ORDER_VW`
        WHERE 1=1 and invoice_date_ibl>={vStartDate}
            '''

    dataFrame1 = client.query(sqlQuery).to_dataframe()
    print (dataFrame1.tail())
    # getting list of column contains Na
    # print(dataFrame1.columns[dataFrame1.isnull().any()])
    rows = [tuple(x) for x in dataFrame1.values]
    insertSalesSql1(rows)

def insertSalesSql1(rec):
 
    try:
        oracleCursor.executemany(
           "insert into EBS_INVOICE_ORDER_TBL(institution,branch_id,branch_name,distributor,ins_type,item_code,sku,selling_price,claimable_discount,un_claimable_discount,inst_discount,Month,order_ref_no,order_quantity,foc,total_qty,sales_vlaue,invoice_date_ibl,invoice_no_ibl,tax_recoverable)values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20)", rec)
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
