from datetime import date, datetime,timedelta
from hmac import trans_36
from pickle import NONE
from airflow import DAG
from airflow import models
import airflow.operators.dummy
from airflow.operators.dummy import DummyOperator
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
import pypyodbc as odbc
import psycopg2.extras
from pytest import param
import psycopg2 as pg
import math
import os
import shutil
from numpy import source
# from airflow.providers.postgres.operators.postgres import PostgresOperator
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
from http import client
import pandas as pds
import numpy as np
from datalab.context import Context
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
#from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
import sqlalchemy
# from sqlalchemy import create_engine
import pandas_gbq
from IPython.display import display
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
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from io import StringIO

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
day_diff=42
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=day_diff)
d1 = past_date.strftime("%Y-%m-%d")
# global df
df = pd.DataFrame()
df1=pd.DataFrame()
# verify the architecture of Python
# print("Platform architecture: " + platform.architecture()[0])
today = date.today()
vEndDate = datetime.date(datetime.today()-timedelta(days=day_diff)).strftime("%Y%m%d")
vMaxDate = datetime.date(datetime.today()-timedelta(days=0)).strftime("%Y%m%d")

vEndDate = "'"+vEndDate+"'"
vMaxDate = "'"+vMaxDate+"'"
print('end date: ', vEndDate)
# Initialize your connection

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
creationDate = date + timedelta(hours=5)

conn = dbapi.connect(

    # Option 1, retrieve the connection parameters from the hdbuserstore
    # key='USER1UserKey', # address, port, user and password are retrieved from the hdbuserstore

    # Option2, specify the connection parameters
    address='10.210.134.204',
    port='33015',
    user='Etl',
    password='Etl@2023'

    # Additional parameters
    # encrypt=True, # must be set to True when connecting to HANA as a Service
    # As of SAP HANA Client 2.6, connections on port 443 enable encryption by default (HANA Cloud)
    # sslValidateCertificate=False #Must be set to false when connecting
    # to an SAP HANA, express edition instance that uses a self-signed certificate.
)

print('connected')
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                "SERVER=192.168.130.81\sqldw;"
                                "DATABASE=ibl_dw;"
                                "UID=pbironew;"
                                "PWD=pbiro345-")

cursor = conn.cursor()

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['muhammad.arslan@iblgrp.com'],
    'email_on_failure': True,
    # 'start_date': datetime(2022,2,11)
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

sap_sale_merging = DAG(
    dag_id='test_test',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=2, day=9),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    schedule_interval=None,
    # schedule_interval='0 20,21 * * *',
    dagrun_timeout=timedelta(minutes=120)
)


def readGcsCsvWritePG():
    vEndDate = datetime.date(
        datetime.today()-timedelta(days=day_diff)).strftime("%d-%b-%Y")
    vEndDate = "'"+vEndDate+"'"

    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    filename = f'IBL_SALES_{d1}'
    print('file name : ', filename)
    blob = bucket.blob(f'staging/sales/FiscalYear/2021-22/{filename}.csv')
    csv_data=blob.download_as_text()

    data_frame=pd.read_csv(StringIO(csv_data))
    print(data_frame)
    connect_string = 'postgresql+psycopg2://apiuser:apiIbl$$123$$456@35.216.168.189:5433/DATAWAREHOUSE'
    engine1 = sqlalchemy.create_engine(connect_string)

    sql = f'''DELETE FROM "DW"."IBL_SALES" where billing_Date>=(current_date-{day_diff})
            and company_code in ('6300','6100')
        '''
    result = engine1.execute(sql)


    # data_frame = data_frame.drop('transfer_date', inplace=True, axis=1)
    data_frame.drop(['transfer_date'], axis=1,inplace=True)
    print('column dropped..........')
    print(data_frame.info())
    print('data frame length : ', len(data_frame))
    print(data_frame)
    data_frame = data_frame.rename(
        columns={'ITEM_CATEGORY': 'item_category', 'BRANCH_ID': 'branch_id', 'BRANCH_DESCRIPTION': 'branch_description'
            , 'BILLING_DATE': 'billing_date', 'DOCUMENT_NO': 'document_no', 'BOOKER_ID': 'booker_id'
            , 'BOOKER_NAME': 'booker_name', 'SUPPLIER_ID': 'supplier_id', 'SUPPLIER_NAME': 'supplier_name'
            , 'BUSINESS_LINE_ID': 'business_line_id', 'BUSINESS_LINE_DESCRIPTION': 'business_line_description'
            , 'CHANNEL': 'channel', 'CUSTOMER_ID': 'customer_id', 'CUSTOMER_NUMBER': 'customer_number'
            , 'CUSTOMER_NAME': 'customer_name', 'SALES_ORDER_TYPE': 'sales_order_type'
            , 'INVENTORY_ITEM_ID': 'inventory_item_id', 'ITEM_CODE': 'item_code'
            , 'ITEM_DESCRIPTION': 'item_description', 'UNIT_SELLING_PRICE': 'unit_selling_price'
            , 'SOLD_QTY': 'sold_qty', 'BONUS_QTY': 'bonus_qty', 'CLAIMABLE_DISCOUNT': 'claimable_discount'
            , 'UNCLAIMABLE_DISCOUNT': 'unclaimable_discount', 'TAX_RECOVERABLE': 'tax_recoverable'
            , 'NET_AMOUNT': 'net_amount', 'GROSS_AMOUNT': 'gross_amount', 'TOTAL_DISCOUNT': 'total_discount'
            , 'REASON_CODE': 'reason_code', 'BILL_TYPE_DESCRIPTION': 'bill_type_description'
            , 'COMPANY_CODE': 'company_code', 'BILLING_TYPE': 'billing_type', 'SALES_ORDER_NO': 'sales_order_no'
            , 'ADDRESS_1': 'address_1', 'ADDRESS_2': 'address_2', 'ADDRESS_3': 'address_3'
            , 'SALESFLO_ORDER_NO': 'salesflo_order_no', 'ITEM_NO': 'item_no', 'RETURN_REASON_CODE': 'return_reason_code'})
    if len(data_frame) > 0:
        print('in if block...')
        data_frame.to_sql('IBL_SALES',
                schema='DW',
                con=engine1,
                index=False,
                if_exists='append',
                )


ReadDataGcsTask = PythonOperator(
    task_id="Read_Gcs_Data_Csv",
    python_callable=readGcsCsvWritePG,
    dag=sap_sale_merging,
)


# def insertDataPostgres():
#     # global df1
#     print(df1.info())
#     # conn2 = pg.connect(host="35.216.168.189", port='5433',
#     #                 database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
#     # cursor = conn2.cursor()
#     # delete_sql = f'''DELETE FROM "DW"."IBL_SALES" where billing_Date>=(current_date-{day_diff})
#     #                         and company_code in ('6300','6100')  '''
#     # cursor.execute(delete_sql)
#     # conn2.commit()

#     # if len(df) > 0:
#     #         df_columns = list(df)
#     #         columns = ",".join(df_columns)
#     # values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
#     # insert_stmt = "INSERT INTO {} ({}) {}".format('"DW"."IBL_SALES"',columns,values)
#     # pg.extras.execute_batch(cursor, insert_stmt, df.values)
#     # conn2.commit()
#     # cursor.close()

# insertDataPostTask=PythonOperator(
#         task_id="InsertDataPostgres",
#         python_callable=insertDataPostgres,
#         dag=sap_sale_merging
# )

