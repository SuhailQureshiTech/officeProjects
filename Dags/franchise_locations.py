
from fileinput import filename
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
import numpy as np
from datalab.context import Context
from google.cloud import storage
import os
from google.oauth2 import service_account
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.contrib.operators import gcs_to_bq
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
#from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
from airflow.utils.email import send_email_smtp
from airflow.operators.email_operator import EmailOperator
# from sqlalchemy import create_engine
# import pandas_gbq
from IPython.display import display
# from pydantic import FilePath
import pysftp
import csv
import pyodbc
#import conf
import pandas as pd
from datetime import date, datetime, timedelta
from google.oauth2.service_account import Credentials
from airflow.contrib.operators import gcs_to_bq

# vStartDataDate = datetime.date(datetime.today())
vEndDate = datetime.date(datetime.today()-timedelta(days=1))


vdayDiff = int(vEndDate.strftime("%d"))
# vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

vStartDate = datetime.date(datetime.today()-timedelta(days=35))

vFirstDate=vStartDate
vLastDate = datetime.date(datetime.today())

vStartDate = "'"+str(vStartDate)+"'"
vEndDate = "'"+str(vEndDate)+"'"
# vStartDataDate = "'"+str(vStartDataDate)+"'"

filePath = f'''/home/admin2/airflow/franchise/'''
global fileName
fileName = 'FranchiseLocationsData.csv'
gcsFileName = 'FranchiseSaleData.csv'
GCS_PROJECT = 'data-light-house-prod'

# def success_function(context):
#     #dag_run = context.get('dag_run')
#     msg = "Stock DAG has executed successfully."
#     subject = f"Stock DAG has completed"
#     send_email_smtp(to=['shehzad.lalani@iblgrp.com'],
#                     subject=subject, html_content=msg)


default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 7, 23),
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
    # 'retry_delay': timedelta(minutes=5),
}

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

franchise_locations = DAG(
    dag_id='franchise_locations',
    default_args=default_args,
    start_date=datetime(2022, 7, 23),
    # schedule_interval='05 * * * *',
    # on_success_callback=success_function,
    # email_on_failure=failure_email_function,
    description='franchise_locations'
)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def franchiseLocationsDataGeneration():
    global fileName
    print('oracle start date :', vStartDate)
    print('oracle end date : ', vEndDate)
    print('global fileName : ',fileName)
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    conn = pg.connect(host="192.168.130.51", port='5432',
                        database="franchise_portal", user="postgres", password="kamil034366")
    df = pds.read_sql(
        f'''SELECT
        location_id,
        location_name,
        null sap_branch_code
        FROM franchise.locations''', conn)
    df['transfer_date'] = datetime.today()
    pd.to_datetime(df.transfer_date, format="%Y%m%d%H%M%S")
    # fileName = 'FranchiseSaleData.csv'
    print(df)
    bucket.blob(f'''staging/temp/{fileName}''').upload_from_string(df.to_csv(index=False), 'text/csv')
    bucket.blob(f'''staging/master_tables/Franchise_Locations/{fileName}''').upload_from_string(df.to_csv(index=False), 'text/csv')

franchiseLocationsDataGenerationTask = PythonOperator(
    task_id="franchiseLocationsDataGenerationTask",
    python_callable=franchiseLocationsDataGeneration,
    dag=franchise_locations
)

# def franchiseSalesGCS():
#     # print('oracle start date :', vStartDate)
#     # print('oracle end date : ', vEndDate)
#     # file_path = f'''/home/admin2/airflow/franchise/'''
#     GCS_PROJECT = 'data-light-house-prod'
#     GCS_BUCKET = 'ibloper'
#     storage_client = storage.Client.from_service_account_json(
#         r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

#     client = storage.Client(project=GCS_PROJECT)
#     bucket = client.get_bucket(GCS_BUCKET)

#     conn = pg.connect(host="192.168.130.51", port='5432',
#                         database="franchise_portal", user="postgres", password="kamil034366")
#     # fileName = 'FranchiseSaleData.csv'
#     for single_date in daterange(vFirstDate, vLastDate):
#         current_Datetime = "'"+str(single_date)+"'"
#         fileName = f'''FranchiseSaleData_{single_date}.csv'''
#         # print('Daily File : ', current_Datetime)

#         df = pds.read_sql(
#             f'''SELECT fs2.company_code,
#             fs2.ibl_distributor_code,
#             u.store_name AS ibl_distributor_desc,
#             cast(u.location_id as text) AS distributor_location_id,
#             l.location_name AS distributor_location_desc,
#             fs2.order_no,
#             fs2.invoice_no as invoice_number,
#             fs2.invoice_date,
#             fs2.channel,
#             fs2.distributor_customer_no as distributor_customer_code,
#             fs2.ibl_customer_no as ibl_customer_code,
#             fs2.customer_name as ibl_customer_name,
#             fs2.distributor_item_code,
#             fs2.ibl_item_code,
#             fs2.item_description as ibl_item_description,
#             fs2.qty_sold AS sold_qty,
#             fs2.gross_amount,
#             fs2.bonus_qty,
#             fs2.discount,
#             fs2.reason
#             ,fs2.current_dates+ '00:00:01'::time  as data_loading_date
#                 FROM franchise."FRANCHISE_SALES" fs2
#                 left OUTER JOIN franchise.users u ON fs2.ibl_distributor_code::text = u.distributor_id::text
#                 left outer join franchise.locations l ON u.location_id::text = l.location_id::text
#                     WHERE current_dates={current_Datetime}''', conn)
#         df['created_at'] = datetime.today()

#         bucket.blob(f'''staging/franchise/sales/{fileName}''').upload_from_string(df.to_csv(index=False), 'text/csv')


# franchiseSalesGCSTask = PythonOperator(
#     task_id="franchiseSaleGCS",
#     python_callable=franchiseSalesGCS,
#     dag=franchise_sale
# )

franchiseLocationsBQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    # print('bq file name : ',fileName)
    task_id='franchiseLocationsBQ',
    bucket='ibloper',
    source_objects=f'''staging/temp/{fileName}''',
    # source_objects='staging/franchise/stock/Franchise_Stock_Data_2022-04-12*',
    destination_project_dataset_table='data-light-house-prod.EDW.FRANCHISE_LOCATIONS',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'location_id', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'location_desc', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'sap_branch_code', 'type': 'STRING', 'mode': 'NULLABLE'}
        , {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=franchise_locations
)

# deleteBQRecordsTask=BigQueryOperator(
#     task_id='DeleteBQRecords'
#     ,bigquery_conn_id='bigquery'
#     ,use_legacy_sql=False
#     ,sql=f'''
#         delete from `data-light-house-prod.EDW.FRANCHISE_SALES`
#         where EXTRACT(DATE FROM data_loading_date) between {vStartDate} and{vEndDate}
#         '''
#     ,dag=franchise_sale
# )

# deleteTempDataFileTask=GoogleCloudStorageDeleteOperator(
#     task_id="DeleteTempDataFile",
#     bucket_name="ibloper",
#     prefix="staging/temp/FranchiseSaleData"
# )

# franchise_locations >> franchiseLocationsBQ
