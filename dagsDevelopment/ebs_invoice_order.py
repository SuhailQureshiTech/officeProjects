
# import 
    # Google
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from google.oauth2.service_account import Credentials
    # AirFlow
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.email_operator import EmailOperator
from airflow.operators.email import EmailOperator
# from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from airflow.exceptions import AirflowException
from airflow.utils.state import State
# from airflow.contrib.operators import gcs_to_bq
import airflow.operators
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,BigQueryExecuteQueryOperator
)

# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.email import send_email_smtp

from fileinput import filename
# import imp
import psycopg2 as pg

from dateutil import parser
from datetime import datetime, date
import sys
from http import client
import pandas as pds
import io
import numpy as np
import os
import sqlalchemy
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

import pysftp
import csv
import pyodbc
from hdbcli import dbapi
    #import conf
import pandas as pd
import pandas_gbq
from datetime import date, datetime, timedelta

# from airflow.contrib.operators import gcs_to_bq

import numpy as np
from regex import F
from sqlalchemy_bigquery import INTEGER
from suhailLib import returnDataDate

    # Connections
import connectionClass

# import -- >> End
connection=connectionClass
franchiseEngine=connection.FranchiseAlchmy()
oracleAlchemy=connection.oracleIblGrpHcmAlchmy()
sapConn=connection.sapConn()

# franchiseDf=pd.DataFrame()

spec_chars=connectionClass.getSpecChars()
global fileName

filePath='/home/airflow/Documents/franchiseDataFiles/'
fileName = 'FranchiseSales.csv'

GCS_PROJECT = 'data-light-house-prod'
DATA_SET_ID='EDW'
# tableId='data-light-house-prod.EDW.FRANCHISE_SALES_NEW'
tableId='FRANCHISE_SALES_NEW1'
fran_sale_df=pd.DataFrame()

# storageClient = storage.Client.from_service_account_json(
#     r'/home/airflow/airflow/data-light-house-prod.json')


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/data-light-house-prod.json"
credential_file="/home/airflow/airflow/data-light-house-prod.json"
credentials=Credentials.from_service_account_file(credential_file)

bigQueryClient = bigquery.Client()

storage.blob._DEFAULT_CHUNKSIZE = 5*1024*1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5*1024*1024 # 5 MB

oracleTable='ebs_invoice_order'

vStartDate = None
vEndDate = None

vStartDate,vEndDate=returnDataDate()
vStartDate="'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate="'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

creationDate = datetime.today()
now = datetime.now()
current_time = now.time()

print('else : from date :', vStartDate)
print('else : enmd date :', vEndDate)


# def success_function(context):
#     #dag_run = context.get('dag_run')
#     msg = "Stock DAG has executed successfully."
#     subject = f"Stock DAG has completed"
#     send_email_smtp(to=['shehzad.lalani@iblgrp.com'],
#                     subject=subject, html_content=msg)


default_args = {
    'owner': 'SuhailQureshi',
    #'start_date': datetime(2023, 2, 14),
    # 'end_date': datetime(),
    # 'email_on_failure': True,
    # 'email': ['shehzad.lalani@iblgrp.com']
    # 'on_success_callback': success_function,
    # 'on_success_callback': dag_success_alert,
    # 'on_failure_callback': failure_email_function,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    # 'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'gcp_conn_id': 'google_cloud_default'
}

EBS_INVOICE_ORDER = DAG(
    dag_id='EBS_INVOICE_ORDER',
    default_args=default_args,
    catchup=False,
    start_date=datetime(2023, 2, 27),
    # schedule_interval='00 03 * * *',
    schedule_interval=None,
    # on_success_callback=success_function,
    # email_on_failure=failure_email_function,
    dagrun_timeout=timedelta(minutes=120),
    description='franchise_sale'
)


def delRecords():  
    delQuery=f''' 
                delete from {oracleTable} 
                    where 1=1 and invoice_date_ibl between '01-Jul-23' and '20-Nov-23'
            '''

    oracleAlchemy.execute(delQuery)
    print('done.....................')

def getEbsInvoiceOrderDataDfSql():
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
                where 1=1 and invoice_date_ibl between '2023-07-01' and '2023-11-20'                
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

taskDeleteRecrods=PythonOperator(
                task_id='deletingRecords'
                ,python_callable=delRecords
                ,dag=EBS_INVOICE_ORDER
                )

taskInsertingRecords=PythonOperator(
                task_id='insertingInvoiceRecords'
                ,python_callable=getEbsInvoiceOrderDataDfSql
                ,dag=EBS_INVOICE_ORDER
                )

# delRecords()
# getEbsInvoiceOrderDataDfSql()


# taskDeleteRecrods>>taskInsertingRecords

