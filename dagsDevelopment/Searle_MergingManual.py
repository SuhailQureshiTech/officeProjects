
from datetime import date, datetime,timedelta
from hmac import trans_36
from pickle import NONE
from airflow import DAG
from airflow import models
from airflow.operators.python import PythonOperator
import time
from datetime import datetime
from time import time, sleep
from fileinput import filename

# import imp
import  pysftp
from datetime import date
import pandas as pd
import glob
import psycopg2.extras
from pytest import param
import psycopg2 as pg
import math
import os
import shutil
from numpy import source
import platform
from hdbcli import dbapi
import pandas as pd
import urllib
import psycopg2
import numpy as np
import pyodbc
from datetime import date, datetime, timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
from dateutil import parser
from datetime import datetime,date,timezone
import sys
# sys.path.append('/home/airflow/airflow/dags')
from http import client
import pandas as pds
import numpy as np
from datetime import timedelta, date
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.contrib.operators import gcs_to_bq
from airflow.exceptions import AirflowFailException
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
import pandas_gbq
import pysftp
import csv
from datetime import date
import pandas as pd
import calendar
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from io import StringIO
import sqlalchemy
import cx_Oracle as xo
from sqlalchemy.dialects.oracle import (
    BFILE,
    BLOB,
    CHAR,
    CLOB,
    DATE,
    DOUBLE_PRECISION,
    FLOAT,
    INTERVAL,
    LONG,
    NCLOB,
    NCHAR,
    NUMBER,
    NVARCHAR,
    NVARCHAR2,
    RAW,
    TIMESTAMP,
    VARCHAR,
    VARCHAR2
)
import connectionClass
from suhailLib import returnDataDate
# from google.auth._service_account_info import()
from google.oauth2.service_account import Credentials
connection=connectionClass
sapConnection=connection.sapConn()
conn=sapConnection

oracleConnectionDB=connection.oracleIlgrpHcm()
oracleAlchemy=connection.oracleIblGrpHcmAlchmy()
oracleCursor=oracleConnectionDB.cursor()


vPath = '/var/sftp/searle/'
conString = '$SPL$'

storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/data-light-house-prod.json"

credential_file="/home/airflow/airflow/data-light-house-prod.json"
credentials=Credentials.from_service_account_file(credential_file)

bigQueryClient = bigquery.Client()
today = date.today()

# global df

df = pd.DataFrame()
df1=pd.DataFrame()

vStartDate,vEndDate=returnDataDate()
vStartOracleDate=vStartDate.strftime("%d-%b-%Y")
vEndOracleDate=vEndDate.strftime("%d-%b-%Y")

# vStartDate='2023-10-01'
# vEndDate='2023-10-31'
# vStartOracleDate='01-Oct-23'
# vEndOracleDate='31-Oct-23'


vStartDate="'"+str(vStartDate)+"'"
vEndDate="'"+str(vEndDate)+"'"

vStartOracleDate="'"+str(vStartOracleDate)+"'"
vEndOracleDate="'"+str(vEndOracleDate)+"'"


print('Start Date : ', vStartDate)
print('End   Date : ', vEndDate)

print('Start Date Oracle: ', vStartOracleDate)
print('End   Date Oracle: ', vEndOracleDate)



# Initialize your connection

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
creationDate = date

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['muhammad.arslan@iblgrp.com'],
    'email_on_failure': True,
    # 'start_date': datetime(2022,2,11)
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'gcp_conn_id': 'google_cloud_default'
}

searle_merging = DAG(
    dag_id='Searle_Merging',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=8, day=10),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    # schedule_interval=None,
    schedule_interval='0 6 * * *',
    dagrun_timeout=timedelta(minutes=120)
)  

def deleteRecords(): 
    oracleTable='EBS_INVOICE_DATA_TBL'       
    print('del reco')
    print(vStartDate, ' ', vEndDate)
    try:
        QdelRecords = f'''DELETE FROM {oracleTable}
                        where 1=1 and to_char(BILL_dt,'yyyy-mm-dd') between {vStartDate} and {vEndDate}                        
        '''
        
        oracleCursor.execute(QdelRecords)
        oracleConnectionDB.commit()

    except Exception as e:
        oracleConnectionDB.rollback()

deleteInvoiceData=PythonOperator(    
                task_id="Deleting_Invoice_Data",
                python_callable=deleteRecords,
                dag=searle_merging)


def insertIblgrpHcmData():   
    oracleTable='EBS_INVOICE_DATA_TBL'
    print('insertIblgrpHcmData')
    print(vStartDate, ' ', vEndDate)

    global dataFrame1
    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)
    sqlQuery = f'''SELECT
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
    try:
        oracleCursor.executemany(f"insert into {oracleTable}(BILL_DT,ITEM_CODE,PROD_NM,ORG_SAP_ID,ORG_DESC,CHANNEL,DATA_FLAG,SOLD_QTY,GROSS,UNIT_SELLING_PRICE)values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)", rows)
        oracleConnectionDB.commit()

    except Exception as e:
        oracleConnectionDB.rollback()
        print(e)

insertInvoiceData=PythonOperator(    
                task_id="Inserting_Invoice_Data",
                python_callable=insertIblgrpHcmData,
                dag=searle_merging)

def QueryBigQuerySalesData():

    print('QueryBigQuerySalesData')
    print(vStartDate, ' ', vEndDate)

    credentials = service_account.Credentials.from_service_account_file(
        '/home/airflow/airflow/data-light-house-prod.json'
    )

    project_id = 'data-light-house-prod'
    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)

    sql = f'''SELECT
    *
    FROM(
        select branch_id BR_CD ,
        document_no AS BILL_NO,
        trx_date1 AS BILL_DT        ,
        replace(CUSTOMER_NUMBER,'.0','') AS EBS_CUST,
        CUSTOMER_NAME AS CUSTOMER_NAME,
        ifnull(address_1,'-')ADD1,
        ifnull(address_2,   '-') ADD2,
         Ifnull(address_3,'-') ADD3,
        CHANNEL AS CH_CD,
        ITEM_CODE AS ITEM_CODE,
        item_description AS description,
        ' '  AS BATCH_NO,
         unit_selling_price AS price,
            cast(SUM(sold_qty) as string) AS SOLD_QTY,
        cast(SUM(BONUS_QTY) as int) AS BON_QTY,
        cast(SUM(DISCOUNT) as float64) AS disc_amt,
        cast(SUM(NET_AMT) as float64) AS NET_amt,
        cast(SUM(GROSS_VALUE) as float64) AS GROSS_VALUE,
        cast(SUM(discounted_rate) as float64) AS discounted_rate,
         ifnull(case when esa.SALES_ORDER_TYPE = 'Bill Near Exp Sales'
        then 'Near Expiry'
        when esa.SALES_ORDER_TYPE in ('Cancel Bill NE Sales',
            'OPS Cancel. Invoice', 'OPS-Cancel Cred Memo') then 'Cancel'
        when esa.SALES_ORDER_TYPE = 'OPS Sales Tax Cash'
        then 'Sale'
        when esa.SALES_ORDER_TYPE = 'OPS-Sales Returns'
        then 'Return'
        when upper(esa.SALES_ORDER_TYPE) like '%RET%'  then 'Return'
        when upper(esa.SALES_ORDER_TYPE) NOT like '%RET%'  then 'Sale'
        end,' ') as reason   ,data_flag
        from `data-light-house-prod.EDW.VW_EBS_SAS_HC_ALL_LOC_DATA_NEW` ESA
        where 1 = 1 AND billing_date between {vStartDate} and {vEndDate}
        GROUP BY BR_CD,
        document_no,
        TRX_DATE1      ,
        replace(CUSTOMER_NUMBER,'.0',''),
        CUSTOMER_NAME,
        ESA.UNIT_SELLING_PRICE,
        SALES_ORDER_TYPE,
        address_1,
        address_2,
        address_3,
        CHANNEL,
        ITEM_CODE,
        DESCRIPTION   ,data_flag
		) A
		ORDER BY
	    BILL_DT
        '''

    df = client.query(sql).to_dataframe()
    dataFrame1=df
    dataFrame2=pd.DataFrame()
    print(dataFrame1.info())
    var = 'BR_CD'+conString + 'BILL_NO'+conString + 'BILL_DT'+conString+'EBS_CUST'+conString+'CH_CD'+conString+'PROD_CD'+conString+'PROD_NM'+conString+'BATCH_NO'+conString+'PRICE' + \
        conString+'SOLD_QTY'+conString+'BON_QTY'+conString+'DISC_AMT'+conString+'NET_AMT'+conString + \
        'GROSS_VALUE'+conString+'DISCOUNTED_RATE' +  conString+'REASON'+conString+'DATA_FLAG'+conString
    
    dataFrame2[var] = dataFrame1['BR_CD']+conString+dataFrame1['BILL_NO']+conString+dataFrame1['BILL_DT']+conString+dataFrame1['EBS_CUST']+conString+dataFrame1['CH_CD']+conString+dataFrame1['ITEM_CODE']+conString+dataFrame1['description']+conString+dataFrame1['BATCH_NO']+conString+dataFrame1['price'].astype(str)+conString+dataFrame1['SOLD_QTY'].astype(
        str)+conString+dataFrame1['BON_QTY'].astype(str)+conString+dataFrame1['disc_amt'].astype(str)+conString+dataFrame1['NET_amt'].astype(str)+conString+dataFrame1['GROSS_VALUE'].astype(str)+conString+dataFrame1['discounted_rate'].astype(str)+conString+dataFrame1['reason']+conString+dataFrame1['data_flag']+conString

    dataFrame2.to_csv(f'''{vPath}HC_ALL_LOC_SALE.txt''',
                    index=False, header=True
                    )
    
def QueryBigQueryCustomerData(): 
    print('QueryBigQueryCustomerData')
    print(vStartDate, ' ', vEndDate)

    project_id = 'data-light-house-prod'
    pandas_gbq.context.credentials = credentials
    client = bigquery.Client(credentials=credentials, project=project_id)

    sql = f'''select distinct
                branch_id BR_CD,replace(CUSTOMER_NUMBER,'.0','')  EBS_CUST,CUSTOMER_NAME
                ,ifnull(CHANNEL,'')  CH_CD
                ,ifnull(address_1,'') ADD1
                ,concat(ifnull(address_2,''),ifnull(address_3,'')) ADD2, data_flag
                from `data-light-house-prod.EDW.VW_EBS_SAS_HC_ALL_LOC_DATA_NEW`
                where billing_date  between {vStartDate} and {vEndDate} and branch_id is not null
        '''

    dataFrame1 = client.query(sql).to_dataframe()
    dataFrame2 = pd.DataFrame()

    dataFrame1['CH_CD'] = dataFrame1['CH_CD'].str.replace(r'\W+', " ")
    dataFrame1['CH_CD'] = dataFrame1['CH_CD'].str.replace(r'\"', " ")
    dataFrame1['ADD1'] = dataFrame1['ADD1'].str.replace(r'\W+', " ")
    dataFrame1['ADD1'] = dataFrame1['ADD1'].str.replace(r'\"', " ")
    dataFrame1['ADD2'] = dataFrame1['ADD2'].str.replace(r'\W+', " ")
    dataFrame1['ADD2'] = dataFrame1['ADD2'].str.replace(r'\"', " ")

    var = 'BR_CD'+conString+'EBS_CUST'+conString+'CUST_NM'+conString+'CH_CD' + \
        conString+'ADD1'+conString+'ADD2'+conString+'DATA_FLAG'+conString

    dataFrame2[var] = dataFrame1['BR_CD']+conString+dataFrame1['EBS_CUST']+conString+dataFrame1['CUSTOMER_NAME']+conString + \
        dataFrame1['CH_CD']+conString+dataFrame1['ADD1']+conString + \
        dataFrame1['ADD2']+conString+dataFrame1['data_flag']+conString

    dataFrame2.to_csv(f'''{vPath}HC_ALL_LOC_INV_CUSTOMER.txt''',
                      index=False, header=True
                      )

generateSalesFileData=PythonOperator(
                    task_id="Sales_File_Data"
                    ,python_callable=QueryBigQuerySalesData
                    ,dag=searle_merging
                )

generateCustomerFileData=PythonOperator(
                    task_id="Customer_File_Data"
                    ,python_callable=QueryBigQueryCustomerData
                    ,dag=searle_merging
                )

def delEbsRecords(): 
    print('delEbsRecords')
    print(vStartOracleDate, ' ', vEndOracleDate)
    
    oracleTable='ebs_invoice_order' 
    delQuery=f''' 
                delete from {oracleTable} 
                    where 1=1 and invoice_date_ibl between {vStartOracleDate} and {vEndOracleDate}
            '''

    oracleAlchemy.execute(delQuery)
    print('done.....................')

def getEbsInvoiceOrderDataDfSql():    
    print('getEbsInvoiceOrderDataDfSql')
    print(vStartOracleDate, ' ', vEndOracleDate)
    oracleTable='ebs_invoice_order'

    sqlData=f'''SELECT
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
                month,
                order_ref_no,
                date_of_order,
                order_quantity,
                foc,
                total_qty,
                sales_vlaue,
                (sales_vlaue+inst_discount)+tax_recoverable net_sale_value,
                invoice_date_ibl,
                invoice_no_ibl,
                tax_recoverable,
                customer_trx_id            
                FROM data-light-house-prod.EDW.EBS_INVOICE_ORDER_VW 
                where 1=1 and invoice_date_ibl between {vStartDate} and {vEndDate}
            '''
    
    df=pd.DataFrame()   
    df = bigQueryClient.query(sqlData).to_dataframe()

    oracle_dtypes = {
            'institution'   :VARCHAR2(250),
            'branch_id'     :VARCHAR2(250),
            'branch_name'   :VARCHAR2(250),
            'distributor'   :VARCHAR2(250),
            'ins_type'      :VARCHAR2(250),
            'item_code'     :VARCHAR2(250),
            'sku'           :VARCHAR2(250),
            'selling_price' :FLOAT,
            'claimable_discount'    :FLOAT,
            'un_claimable_discount' :FLOAT,
            'inst_discount'         :FLOAT,
            'month'                 :VARCHAR2(15),
            'order_ref_no'          :VARCHAR2(15),
            'date_of_order'         :DATE,
            'order_quantity'        :FLOAT,
            'foc'                   :FLOAT,
            'total_qty'             :FLOAT,
            'sales_vlaue'           :FLOAT,
            'net_sale_value'        :FLOAT,

            'invoice_date_ibl'      :DATE,
            'invoice_no_ibl'        :VARCHAR2(50),
            'tax_recoverable'       :FLOAT,
            'customer_trx_id'       :VARCHAR2(25)             
             }
    print(df)
    df.to_sql(oracleTable,schema='IBLGRPHCM',if_exists='append',con=oracleAlchemy,index=False,dtype=oracle_dtypes)
    print('done.....................')


delEbsOrder=PythonOperator(
    task_id='Deleting_Ebs_Order'
    ,python_callable=delEbsRecords
    ,dag=searle_merging
)

genEbsRecords=PythonOperator(
    task_id='Inserting_Ebs_Orders'
    ,python_callable=getEbsInvoiceOrderDataDfSql
    ,dag=searle_merging
)


deleteRecords()
insertIblgrpHcmData()
QueryBigQuerySalesData()
QueryBigQueryCustomerData()
delEbsRecords()
getEbsInvoiceOrderDataDfSql()

# [
#     deleteInvoiceData>>insertInvoiceData
#     ,generateSalesFileData,generateCustomerFileData
#     ,delEbsOrder>>genEbsRecords
# ]

