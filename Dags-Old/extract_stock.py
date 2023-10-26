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
from datetime import datetime,date
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
#from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
from airflow.utils.email import send_email_smtp
from airflow.operators.email_operator import EmailOperator
# from sqlalchemy import create_engine
import pandas_gbq
from IPython.display import display
import pysftp
import csv
import pyodbc
#import conf







def success_function(context):
    #dag_run = context.get('dag_run')
    msg = "Stock DAG has executed successfully."
    subject = f"Stock DAG has completed"
    send_email_smtp(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)

def failure_email_function(context):
    dag_run = context.get('dag_run')
    print("mic testing 123")
    msg = "Stock DAG has failed"
    subject = f"Stock DAG {dag_run} has failed"
    send_email_smtp(to=['shehzad.lalani@iblgrp.com'], subject=subject, html_content=msg)

default_args = {
        'owner': 'admin',
        'start_date': datetime(2022, 5, 30),
        # 'end_date': datetime(),
        'email': ['shehzad.lalani@iblgrp.com'],
        'on_success_callback':success_function,
        'email_on_failure': True,
        'on_failure_callback': failure_email_function
        #'on_success_callback': dag_success_alert,
        #'on_failure_callback': failure_email_function,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        }

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
curr_date = today.strftime("%d-%b-%Y")

# def extract_stock_csv_to_postgres():
#      server = '192.168.130.81\sqldw'
#      db = 'ibl_dw'
#      user = 'pbironew'
#      password = 'pbiro345-'
#      schema = 'dbo'
#      tablePOS = 'phnx_consumer_ops_stock'

#      conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password+';TrustServerCertificate=Yes')

#      cursor = conn.cursor()
#      query = 'SELECT * FROM [%s].%s.%s' % (db, schema, tablePOS);
#      df = pds.read_sql(query, conn)
#      df['date']  = today
#      df['created_at'] = datetime.now()
#      data = df['PLANT']
#      df.insert(0,"company_code",data)

#      df.columns = df.columns.str.strip()

#      conn = pg.connect( host="192.168.130.81", port= '5432', database="hanadb_prd", user="postgres", password="ibl@123@456")
#      cursor = conn.cursor()
#      delete_sql = 'DELETE FROM "DW"."PHNX_CONSUMER_OPS_STOCK"'
#      cursor.execute(delete_sql)
#      conn.commit()

#      if len(df) > 0:
#         df_columns = list(df)
#         columns = ",".join(df_columns)
#         #print(columns)



#     # create VALUES('%s', '%s",...) one '%s' per column
#      values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
#     #create INSERT INTO table (columns) VALUES('%s',...)
#      insert_stmt = "INSERT INTO {} ({}) {}".format('"DW"."PHNX_CONSUMER_OPS_STOCK"',columns,values)
#      print(insert_stmt)
#      pg.extras.execute_batch(cursor, insert_stmt, df.values)
#      conn.commit()
#      cursor.close()

def extract_stock_postgres_to_gcs():
    # Establishing Postgres Connection
    conn = pg.connect(host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    # Read data from PostgreSQL database table and load into a DataFrame instance
    df       = pds.read_sql('SELECT "VAL_TYPE" as val_type, "TRADE_PRICE" as trade_price, trim("MANUFACTURING_DATE") as manufacturing_date, trim("MATERIAL") as material,"PLANT" as plant, "STORAGE_LOCATION" as storage_location,NULL as s,NULL as  valuation,NULL as special_stock_number,NULL as sl,"BATCH" as batch,"BUn" as bun,"UNRESTRICTED" as  unrestricted,"CRCY" as crcy,"TOTAL_VALUE_1" as total_value_1,"TRANSIT" as transit,"TOTAL_VALUE_2" as total_value_2,"IN_QUALITY_INSPECTION" as in_quality_inspection,"TOTAL_VALUE_3" as total_value_3,"RESTRICTED_USE" as restricted_use,"TOTAL_VALUE_4" as total_value_4,"BLOCKED" as  "blocked","TOTAL_VALUE_5" as  total_value_5,"RETURNS" as "returns","TOTAL_VALUE_6" as total_value_6,trim("EXPIRY_DATE") as expiry_date from "DW".phnx_consumer_ops_stock', conn);
    df_stock = pds.DataFrame(data=df)
    #df_stock['transit'] = df_stock['transit'].replace('PKR',0)
    print(df_stock)
    df_stock['created_at'] = datetime.now()
    data = df_stock['plant']
    df_stock.insert(0,"company_code",data)
    df_stock.insert(1,"date", today)
    df_stock["manufacturing_date"]= pds.to_datetime(df_stock["manufacturing_date"])
    df_stock["expiry_date"]= pds.to_datetime(df_stock["expiry_date"], errors='coerce')
    df_stock["date"]= pds.to_datetime(df_stock["date"])
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    filename =f'ibl_stock_{curr_date}'

    bucket.blob(f'staging/stock/{filename}.csv').upload_from_string(df_stock.to_csv(index=False), 'text/csv')
    conn.close()





ibl_stock = DAG(
    dag_id='ibl_stock',
    default_args=default_args,
    start_date= datetime(2023, 1, 23),
    catchup=False,
    schedule_interval='0 */2 * * *',
    on_success_callback=success_function,
    #email_on_failure=failure_email_function,
    description='ibl_stock_data',
)


# stock_data_csv_to_postgres = PythonOperator(
#         task_id="stock_data_csv_to_postgres",
#         python_callable=extract_stock_csv_to_postgres,
#         #on_failure_callback=failure_email_function,
#         dag=ibl_stock
# )

stock_data_postgres_to_gcs = PythonOperator(
        task_id="stock_data_postgres_to_gcs",
        python_callable=extract_stock_postgres_to_gcs,
        dag=ibl_stock
)

stock_data_delete_current_date_records_bq = BigQueryOperator(
        task_id="bq_stock_data_del_current_date_records",
        bigquery_conn_id='bigquery',
	use_legacy_sql=False,
        sql='''
	    #standardSQL
	    DELETE FROM  `data-light-house-prod.EDW.IBL_STOCK` WHERE dated = current_date()
	    ''',
        dag=ibl_stock
)


stock_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='stock_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/stock/ibl_stock_{curr_date}.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.IBL_STOCK',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'company_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'dated', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'val_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'trade_price', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'manufacturing_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'material_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'plant_code', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'storage_location', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 's', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'valuation', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'stock_indicator', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'sl', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'batch', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'bun', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'unrestricted', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'crcy', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'total_value_1', 'type': 'BigNumeric', 'mode': 'NULLABLE'},
    {'name': 'transit_stock', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'total_value_2', 'type': 'BigNumeric', 'mode': 'NULLABLE'},
    {'name': 'in_quality_inspection', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'total_value_3', 'type': 'BigNumeric', 'mode': 'NULLABLE'},
    {'name': 'restricted_use_stock', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'total_value_4', 'type': 'BigNumeric', 'mode': 'NULLABLE'},
    {'name': 'blocked_stock', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'total_value_5', 'type': 'BigNumeric', 'mode': 'NULLABLE'},
    {'name': 'returned_stock', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'total_value_6', 'type': 'BigNumeric', 'mode': 'NULLABLE'},
    {'name': 'expiry_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=ibl_stock
)


stock_data_postgres_to_gcs >> stock_data_delete_current_date_records_bq >> stock_data_gcs_to_bq



