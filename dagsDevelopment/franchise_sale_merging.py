
from fileinput import filename
import imp
import psycopg2 as pg

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models

from dateutil import parser
from datetime import datetime, date
import sys
from http import client
import pandas as pds
import io
import numpy as np
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.contrib.operators import gcs_to_bq
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.email import send_email_smtp
from airflow.operators.email_operator import EmailOperator

from google.cloud import bigquery

from google.cloud.exceptions import NotFound
import sqlalchemy

# from sqlalchemy import create_engine
# import pandas_gbq

# from pymysql import Date
# from pymysql import Date
# from pydantic import FilePath
import pysftp
import csv
import pyodbc
from hdbcli import dbapi
#import conf
import pandas as pd
from datetime import date, datetime, timedelta
from google.oauth2.service_account import Credentials
from airflow.contrib.operators import gcs_to_bq
import numpy as np
from regex import F
import connectionClass

connection=connectionClass
postgresEngine=connection.FranchiseAlchmy()
sapConn=connection.sapConn()
spec_chars=connectionClass.getSpecChars()

storageClient = storage.Client.from_service_account_json(
    r'/home/airflow/airflow/data-light-house-prod.json')

storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))

if vTodayDate<=5:
    from dateutil.relativedelta import relativedelta
    print('if block')
    from dateutil.relativedelta import relativedelta
    today = date.today()
    # d = today - relativedelta(months=1)
    d = today - relativedelta(months=2)
    vStartDate = date(d.year, d.month, 1)
    # vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"  
    vStartDate = vStartDate
    vEndDate = date(today.year, today.month, 1) - relativedelta(days=1)

else:

    print('else block')
    vStartDate = datetime.date(datetime.today().replace(day=1))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    print('else : from date :', vStartDate)
    print('else : enmd date :', vEndDate)

vFirstDate = vStartDate
vLastDate = datetime.date(datetime.today())

vEndDate = datetime.date(datetime.today())
vStartDate = "'"+str(vStartDate)+"'"
vEndDate = "'"+str(vEndDate)+"'"

print('else : from date :', vStartDate)
print('else : enmd date :', vEndDate)

filePath = f'''/home/admin2/airflow/franchise/'''
global fileName
fileName = 'FranchiseSales.csv'
# gcsFileName = 'FranchiseSaleData.csv'
GCS_PROJECT = 'data-light-house-prod'
fran_sale_df=pd.DataFrame()

tableId='data-light-house-prod.EDW.FRANCHISE_SALES'

# def success_function(context):
#     #dag_run = context.get('dag_run')
#     msg = "Stock DAG has executed successfully."
#     subject = f"Stock DAG has completed"
#     send_email_smtp(to=['shehzad.lalani@iblgrp.com'],
#                     subject=subject, html_content=msg)


default_args = {
    'owner': 'admin',
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

franchise_sale_merging = DAG(
    dag_id='franchise_sale_merging',
    default_args=default_args,
    catchup=False,
    start_date=datetime(2023, 2, 27),
    #schedule_interval='00 03 * * *',
    schedule_interval=None,
    # on_success_callback=success_function,
    # email_on_failure=failure_email_function,
    dagrun_timeout=timedelta(minutes=120),
    description='franchise_sale'
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/data-light-house-prod.json"

today = date.today()
curr_date = today.strftime("%d-%b-%Y")

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

deleteTempDataFile = GoogleCloudStorageDeleteOperator(
    task_id="DeleteTempDataFile",
    bucket_name="ibloper",
    prefix="staging/temp/FranchiseSales"
)

deleteBQRecordsTask = BigQueryOperator(
     task_id='DeleteBQRecords'
    ,use_legacy_sql=False, sql=f'''delete from {tableId} where invoice_date>={vStartDate} '''
    ,dag=franchise_sale_merging
)

def franchiseSaleDataGeneration():
    global fileName
    global fran_sale_df
    print('oracle start date :', vStartDate)
    print('oracle end date : ', vEndDate)
    # print('global fileName : ',fileName)

    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storageClient
    # storage.Client.from_service_account_json(
    #     r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    query=f'''
        SELECT
        fs2.company_code,
        fs2.ibl_distributor_code,
        u.store_name AS ibl_distributor_desc,
        cast(l.branch_code as text)branch_code,
        cast(u.location_id as text) AS distributor_location_id,
        l.location_name AS distributor_location_desc,
        fs2.order_no,
        fs2.invoice_no as invoice_number,
        fs2.invoice_date,
        fs2.channel,
        fs2.distributor_customer_no as distributor_customer_code,
        fs2.ibl_customer_no as ibl_customer_code,
        fs2.customer_name as ibl_customer_name,
        fs2.distributor_item_code,
        fs2.ibl_item_code,
        fs2.item_description as ibl_item_description,
        fs2.qty_sold AS sold_qty,
        fs2.gross_amount,
        fs2.bonus_qty,
        fs2.discount,
        fs2.reason,
        case when  date(fs2.current_dates)<'2022-08-02'
            then concat(cast(date(fs2.current_dates) as text),' 00:00:00')
                else cast(fs2.current_dates as text)  end data_loading_date
        ,cast(to_char(current_dates,'yyyymmdd')  as numeric) as record_date
        ,fs2.address
        FROM test_schema.test_franchise_sales fs2
            left OUTER JOIN test_schema.users u ON fs2.ibl_distributor_code::text = u.distributor_id::text
            left outer join test_schema.locations l ON u.location_id::text = l.location_id::text
                WHERE 1=1 and branch_code is not null  and fs2.invoice_date>={vStartDate}
    '''

    # print('Query...')
    # print(query)
    # conn = pg.connect(host="192.168.130.51", port='5432',
    #                     database="franchise_portal", user="postgres", password="kamil034366")
    
    df = pds.read_sql(f'''{query}  ''' ,postgresEngine)
    print(df.info())

# following block working.....
    for char in spec_chars:
        df['address'] = df['address'].str.replace(char, ' ')
        df['address'] = df['address'].str.split().str.join(" ")

    # print(df.info())

    # conn1 = dbapi.connect(address='10.210.134.204',
    #                     port='33015',   user='ETL',  password='Etl@2025')

    conn1=sapConn

    cus_df=pds.read_sql(f'''
                        SELECT distinct  KUNNR as "SAP_CUSTOMER_CODE"
                        ,ADRNR
                        ,A.STR_SUPPL1 add1,
                        A.STR_SUPPL2 add2,A.STR_SUPPL3 add3
                        FROM SAPABAP1.KNA1 AS B
                        LEFT OUTER JOIN SAPABAP1.ADRC AS A ON (A.CLIENT=B.MANDT AND A.ADDRNUMBER=B.ADRNR)
                        WHERE 1=1 AND MANDT=300
                        ''',conn1)

    branch_df=pds.read_sql(f'''
                        SELECT distinct  VKBUR "sap_branch_code",BEZEI "branch_desc"
                        FROM SAPABAP1.TVKBT BRANCH WHERE MANDT=300 AND SPRAS ='E'
                        ''',conn1)

    fran_sale_df = df.merge(
        cus_df, how='left', left_on=['ibl_customer_code'], right_on=['SAP_CUSTOMER_CODE'])

    fran_sale_df = fran_sale_df.merge(
        branch_df, how='left', left_on=['branch_code'], right_on=['sap_branch_code'])

    fran_sale_df['ref_customer_code'] = np.where(fran_sale_df['SAP_CUSTOMER_CODE'].isnull(), df['ibl_distributor_code'].astype(
        str)+'-'+df['ibl_customer_code'].astype(str)
                , df['ibl_customer_code']
                    )

    fran_sale_df.drop(['SAP_CUSTOMER_CODE', 'ADRNR','sap_branch_code'], inplace=True, axis=1)
    fran_sale_df['transfer_date'] = datetime.now()

    column_name = ["company_code",
                    "ibl_distributor_code",
                    "ibl_distributor_desc",
                    "branch_code",
                    "branch_desc",
                    "distributor_location_id",
                    "distributor_location_desc",
                    "order_no",
                    "invoice_number",
                    "invoice_date",
                    "channel",
                    "distributor_customer_code",
                    "ibl_customer_code",
                    "ref_customer_code",
                    "ibl_customer_name",
                    "ADD1",
                    "ADD2",
                    "ADD3",
                    "distributor_item_code",
                    "ibl_item_code",
                    "ibl_item_description",
                    "sold_qty",
                    "gross_amount",
                    "bonus_qty",
                    "discount",
                    "reason",
                    "data_loading_date",
                    "transfer_date",
                    "record_date",
                    "address"
                ]
    fran_sale_df=fran_sale_df.reindex(columns=column_name)

    bucket.blob(f'''staging/temp/{fileName}''').upload_from_string(fran_sale_df.to_csv(index=False), 'text/csv')

franchiseSaleDataGenerationTask = PythonOperator(
    task_id="franchiseSaleDataGeneration",
    python_callable=franchiseSaleDataGeneration,
    dag=franchise_sale_merging
)

franchiseSale_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='franchiseSaleBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/{fileName}''',
    # destination_project_dataset_table='data-light-house-prod.EDW.FRANCHISE_SALES',
    destination_project_dataset_table=f"{tableId}",
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'company_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'ibl_distributor_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'ibl_distributor_desc', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'branch_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'branch_desc', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'distributor_location_id', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'distributor_location_desc', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'order_no', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'invoice_number', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'invoice_date', 'type': 'DATE', 'mode': 'NULLABLE'}
        , {'name': 'channel', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'distributor_customer_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'ibl_customer_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'ref_customer_number', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'ibl_customer_name', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'add1', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'add2', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'add3', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'distributor_item_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'ibl_item_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'ibl_item_description', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'sold_qty', 'type': 'Numeric', 'mode': 'NULLABLE'}
        , {'name': 'gross_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        , {'name': 'bonus_qty', 'type': 'Numeric', 'mode': 'NULLABLE'}
        , {'name': 'discount', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        , {'name': 'reason', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'data_loading_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        , {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
        , {'name': 'record_date', 'type': 'Numeric', 'mode': 'NULLABLE'}
        , {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'}

    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=franchise_sale_merging
)

# POSTGRES

deleteTempDataFile>> deleteBQRecordsTask >> franchiseSaleDataGenerationTask >> [franchiseSale_to_BQ]

