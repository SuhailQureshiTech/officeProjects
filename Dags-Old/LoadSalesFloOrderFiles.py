from datetime import date, datetime, timedelta
from hmac import trans_36
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
import pysftp
from datetime import date
import pandas as pd
import glob
import pypyodbc as odbc
from pytest import param
import psycopg2 as pg
import math
import os
import shutil
from numpy import source
import numpy as np
from airflow.providers.postgres.operators.postgres import PostgresOperator
from google.cloud import storage
import pandas as pds
import pandas_gbq
import os
from google.oauth2 import service_account
from airflow.contrib.operators import gcs_to_bq
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.email import send_email_smtp
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import regex

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"

df = pd.DataFrame()
params = None

today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
# past_date = today - pd.DateOffset(days=1)
past_date = today
d1 = past_date.strftime("%Y-%m-%d")


os.system('cls')
today = date.today()

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))

# enable following for range....
# vDay = date.today()-timedelta(days=34)
# vDay = int(vDay.strftime("%d"))

#enable it for daily
vDay=0
# vDay=35

vStartDate = datetime.date(datetime.today()-timedelta(days=vDay))
vEndDate = datetime.date(datetime.today()-timedelta(days=0))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

spec_chars = ["!", '"', "#", "%", "&", "'", "(", ")",
        "*", "+", ",", "-", ".", "/", ":", ";", "<",
        "=", ">", "?", "@", "[", "\\", "]", "^", "_",
        "`", "{", "|", "}", "~", "–"]

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['muhammad.arslan@iblgrp.com'],
    'email_on_failure': True,
    # 'start_date': datetime(2022,2,11)
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}


def send_email(context):
    #dag_run = context.get('dag_run')
    msg = "SalesFlo Data Files, Completed...."
    subject = f"SalesFlo Data Files"
    send_email_smtp(to=['muhammad.suhail@iblgrp.com']                    # , cc=['muhammad.suhail@iblgrp.com', 'Fahad.Kasiri@habitt.com', 'Umer.Altaf@iblgrp.com', 'Arshad.Ali@habitt.com', 'Muhammad.Fayyaz@Markitt.com', 'Asad.Iqbal@Markitt.com', 'Ali.Babur@habitt.com', 'Noman.Ali@habitt.com', 'Muhammad.Shoaib@Markitt.com', 'orasyscojava@gmail.com']
                    , subject=subject, html_content=msg
                    )


LoadSalesFloFiles = DAG(
    dag_id='LoadSalesFloFiles',
    # start date:28-03-2017
    start_date=datetime(year=2023, month=7, day=6),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    catchup=False,
    schedule_interval='30 1 * * *',
    dagrun_timeout=timedelta(minutes=120),
    on_failure_callback=send_email
)



def downloadFilesRange(vfileName):
    hostName = "saturn.retailistan.com"
    userName = "IBL"
    HostPassword = "!2i`*7^im&t23$a*Z"
    # Accept any host key (still wrong see below)
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    today = date.today()
    d6 = today.strftime("%Y_%m_%d")
    fileName = vfileName
    print('d6 :', d6)
    print('fileNames :', filename)
    try:
        with pysftp.Connection(host=hostName, username=userName, password=HostPassword, private_key=".ppk", cnopts=cnopts) as sftp:
            # switching to remote path
            remoteFile = f'''/order-info-data/{fileName}.csv'''
            localFilePath = f'''/home/admin2/airflow/dags/SalesFloDataFiles/{fileName}.csv'''

            sftp.get(remoteFile, localFilePath)
            sftp.close()
    except Exception as e:
            print('Exception : ',e)

def moveFilesBackup():
    sourcePath = '/home/admin2/airflow/dags/SalesFloDataFiles/'
    targetPath = '/home/admin2/airflow/dags/SalesFloDataFiles/bkup'

    listFiles = os.listdir(sourcePath)
    for f in listFiles:
        if os.path.isfile(os.path.join(targetPath, f)):
            os.remove(os.path.join(targetPath, f))
        shutil.move(sourcePath+f, targetPath)

def Incremental_Load_Salesflo_orders():
    vStartMergingDate = "'" + \
        str(datetime.date(datetime.today()-timedelta(days=vDay)))+"'"
    print('ibl....')
    print('start date :', vStartMergingDate)
    print('end date : ', vStartMergingDate)

    df = pd.DataFrame()
    path = '/home/admin2/airflow/dags/SalesFloDataFiles'
    files = glob.glob(path+"/*.csv")
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    project_id = 'data-light-house-prod'
    credentials = service_account.Credentials.from_service_account_file(
        '/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    bigqueryClient = bigquery.Client(
        credentials=credentials, project=project_id)

    truncateQuery = f''' delete from EDW.IBL_SALESFLO_ORDERS
                        where cast(ADDED_ON as date)>={vStartMergingDate}
            '''
    print(truncateQuery)
    job = bigqueryClient.query(truncateQuery)

    salesFloFiles = [f for f in files]
    # print(salesFloFiles)
    # df=pd.DataFrame()
    for f in salesFloFiles:
        try:
            print('sale flo file : ', f)
            fileData = pd.read_csv(f , low_memory=False)
            fileData['fileName'] = f
            dfData = fileData
            dfData["SAP_SKU_CODE"] = dfData["SAP_SKU_CODE"].astype(str)
            dfData['ORDER_DATE'].replace('0000-00-00', '', inplace=True)
            dfData['DELIVERY_DATE'].replace('0000-00-00', '', inplace=True)
            dfData['ADDED_ON'].replace('0000-00-00', '', inplace=True)
            dfData['transfer_date'] = datetime.today()
            dfData.columns = dfData.columns.str.strip()

            dfData["SAP_STORE_CODE"] = dfData["SAP_STORE_CODE"].astype('str')

            for char in spec_chars:
                dfData['SAP_STORE_CODE'] = dfData['SAP_STORE_CODE'].str.replace(
                    char, ' ')
                dfData['SAP_STORE_CODE'] = dfData['SAP_STORE_CODE'].str.split().str.join(" ")

            dfData["SAP_STORE_CODE"] = dfData["SAP_STORE_CODE"].astype('string')
            dfData["SAP_DISTRIBUTOR_CODE"] = dfData["SAP_DISTRIBUTOR_CODE"].astype(
                'string')

            dfData["STORE_LATITUDE"] = dfData["STORE_LATITUDE"].astype(
                'string')
            dfData["STORE_LONGITUDE"] = dfData["STORE_LONGITUDE"].astype(
                'string')

            dfData["SAP_ORDER_BOOKER_CODE"] = dfData["SAP_ORDER_BOOKER_CODE"].astype('string')

            dfData["DISTRIBUTOR_NAME"] = dfData["DISTRIBUTOR_NAME"].astype(
                'string')
            dfData["DISTRIBUTOR_CODE"] = dfData["DISTRIBUTOR_CODE"].astype(
                'string')
            dfData["PJP_CODE"] = dfData["PJP_CODE"].astype(
                'string')
            dfData["PJP_NAME"] = dfData["PJP_NAME"].astype(
                'string')
            dfData["STORE_NAME"] = dfData["STORE_NAME"].astype(
                'string')
            dfData["STORE_CODE"] = dfData["STORE_CODE"].astype(
                'string')

            dfData["SAP_STORE_CODE"] = dfData["SAP_STORE_CODE"].astype('string')
            dfData["SKU_DESCRIPTION"] = dfData["SKU_DESCRIPTION"].astype(
                'string')
            dfData["SKU_MANUFACTURER_CODE"] = dfData["SKU_MANUFACTURER_CODE"].astype(
                'string')
            dfData["STORE_CODE"] = dfData["STORE_CODE"].astype('string')
            dfData["SAP_SKU_CODE"] = dfData["SAP_SKU_CODE"].astype('string')

            dfData["ORDER_BOOKER_CODE"] = dfData["ORDER_BOOKER_CODE"].astype(
                'string')
            dfData["ORDER_BOOKER_NAME"] = dfData["ORDER_BOOKER_NAME"].astype(
                'string')
            dfData["ORDER_NUMBER"] = dfData["ORDER_NUMBER"].astype('string')
            dfData["SAP_SKU_CODE"] = dfData["SAP_SKU_CODE"].astype('string')

            dfData["ORDER_AMOUNT"] = dfData["ORDER_AMOUNT"].astype("int")
            dfData["ORDER_UNITS"] = dfData["ORDER_UNITS"].astype("int")

            dfData["STATUS"] = dfData["STATUS"].astype('string')
            dfData['ORDER_DATE'] = pd.to_datetime(dfData['ORDER_DATE'])
            # dfData["DELIVERY_DATE"] = dfData["DELIVERY_DATE"].astype('datetime64[as]')
            dfData['DELIVERY_DATE'] = pd.to_datetime(dfData['DELIVERY_DATE'])
            dfData['ADDED_ON'] = pd.to_datetime(dfData['ADDED_ON'])

            print('dataframe length....', len(dfData))
            if len(dfData) > 0:
                df_columns = list(dfData)
                columns = ",".join(df_columns)

            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
            file_path = r'/home/admin2/airflow/dag/Google_cloud'

            # print(current_Datetime)

            # filename =f'IBL_SALES_{d1}'
            filename = f.replace(
                '/home/admin2/airflow/dags/SalesFloDataFiles/', '')
            filename = f'SALESFLO_ORDERS_{d1}'
            # print('sales flo gcs name ',filename)

            # df['transfer_date'] = pd.to_datetime(df['transfer_date'])


            print(dfData['transfer_date'])
            dfData.columns = dfData.columns.str.strip()
            dfData.columns = dfData.columns.str.lower()

            dfData1 = dfData[['distributor_name',
                    'distributor_code', 'sap_distributor_code'
                    , 'pjp_code', 'pjp_name'
                    , 'store_name', 'store_code', 'sap_store_code', 'sku_description', 'sku_manufacturer_code'
                    , 'sap_sku_code', 'order_booker_code', 'sap_order_booker_code', 'order_booker_name'
                    , 'order_number', 'order_amount','order_units','status','order_date','delivery_date'
                    , 'added_on', 'filename', 'transfer_date', 'store_latitude', 'store_longitude'
                            ]]

            # dfData1.rename(columns=dfCol,inplace=True)
            print('dfData.......')
            print(dfData.info())

            print('dfData1.......')
            print(dfData1.info())


            # print(dfData.info())
            # print(dfData)
            job = pandas_gbq.to_gbq(
                dfData1, destination_table='data-light-house-prod.EDW.IBL_SALESFLO_ORDERS'
                ,project_id='data-light-house-prod'
                ,if_exists='append'
                # ,table_schema=table_schema
            )

        except Exception as e:
            print('exception.....', e, ' fileName : ', f.replace(
                '/home/admin2/airflow/dags/SalesFloDataFiles/', '')
            )
            pass

def DownloadFtpFiles():
    vStartMergingDate = datetime.date(datetime.today()-timedelta(days=vDay))

    if vDay == 0:
        mergingFile = 'IBL_Order_Info_' + \
            str(str(vStartMergingDate).replace('-', '_'))
        print('day : ',vDay)
        print(mergingFile)
        downloadFilesRange(mergingFile)
    else:
        print('else day : ',vDay)
        vStartDate = datetime.date(datetime.today()-timedelta(days=vDay))
        vEndDate = datetime.date(datetime.today()-timedelta(days=0))
        dateRange = pd.date_range(start=vStartDate, end=vEndDate)

        # for mergingDate in (vStartMergingDate+timedelta(n) for n in range(vDay)):
        for i in dateRange:
            # d1 = past_date.strftime("%Y-%m-%d")
            # print('merging date 1: ', i)
            mergingFile = 'IBL_Order_Info_' + \
                str(str(i).replace('-', '_')).replace(' 00:00:00','')
            print('merging file : ',mergingFile)
            downloadFilesRange(mergingFile)

moveFilesBackup=PythonOperator(
    task_id='moveFilesBackup',
    python_callable=moveFilesBackup,
    dag=LoadSalesFloFiles

)

DownloadFtpFiles = PythonOperator(
    task_id='DownloadFtpFiles',
    python_callable=DownloadFtpFiles,
    dag=LoadSalesFloFiles
)

Incremental_Load_Salesflo_orders = PythonOperator(
    task_id='Incremental_Load_Salesflo_orders',
    python_callable=Incremental_Load_Salesflo_orders,
    dag=LoadSalesFloFiles
)

DownloadFtpFiles >>Incremental_Load_Salesflo_orders>>moveFilesBackup
