from datetime import date, datetime, timedelta
import pyodbc
from IPython.display import display
import pandas_gbq
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from airflow.contrib.operators import gcs_to_bq
from dateutil import parser
import pandas as pd
from google.cloud import storage
from http import client
from datetime import datetime, timedelta
from airflow import DAG
from airflow import models
import airflow.operators.dummy
# import airflow.operators.python
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
# from flask_sqlalchemy import SQLAlchemy
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
# from sqlalchemy import true
# from flask_sqlalchemy import SQLAlchemy
# from sqlalchemy import create_engine


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 11),
    # 'schedule_interval':'* * * * *'

}

dag = models.DAG('Franchise_dag', default_args=default_args,
                 schedule_interval='@once')


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"

# # # # # # # # # # # # # # # # # # # # # # # # # # # Stock Data # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def insert_stock_data_to_gcs():
    df = pd.read_excel(
        f'/home/admin2/airflow/dags/files/AZEE_DT138_06JUN.xlsx', sheet_name='StockView')

    # df.columns = ['ibl_distributor_code','distributor_item_code','ibl_item_code','distributor_item_description','lot_number','expiry_date','stock_qty','stock_value','dated']
    df.columns = ['ibl_distributor_code', 'distributor_item_code', 'dated', 'ibl_item_code',
                  'distributor_item_description', 'lot_number', 'expiry_date', 'stock_qty', 'stock_value']
    # df['dated'] = datetime.today().strftime('%Y-%m-%d')
    df['company_code'] = '1001'
    df['transfer_date'] = datetime.today()
    company_code = '1001'
    # display(df)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
    file_path = r'/home/arslan/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    # buckets = list(storage_client.list_buckets())
    # current_Datetime = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
    past_date = today - pd.DateOffset(days=1)
    current_Datetime = past_date.strftime("%Y-%m-%d")

    # current_Datetime = datetime.today().strftime('%Y-%m-%d')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    # filename =f'Franchise_Stock_Data_{current_Datetime}_{company_code}'
    filename = f'Franchise_Stock_Data_2022-06-06_{company_code}'
    bucket.blob(f'staging/franchise/stock/{filename}.csv').upload_from_string(
        df.to_csv(index=False), 'text/csv')

    # df.to_csv(f'gs://iblopers/staging/franchise/{filename}',index=False)


t1 = PythonOperator(
    task_id='insert_stock_data_from_csv_to_gcs',
    python_callable=insert_stock_data_to_gcs,
    dag=dag,
)

t2 = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='insert_stock_data_from_gcs_to_bq',
    bucket='ibloper',
    source_objects='staging/franchise/stock/Franchise_Stock_Data_2022-06-06*',
    # source_objects='staging/franchise/stock/Franchise_Stock_Data_2022-04-12*',
    destination_project_dataset_table='data-light-house-prod.EDW.FRANCHISE_STOCK',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'ibl_distributor_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'distributor_item_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'dated', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'ibl_item_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'distributor_item_description',
            'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'lot_number', 'type': 'STRING', 'mode': 'NULLABLE'},
       	{'name': 'expiry_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'stock_qty', 'type': 'Numeric', 'mode': 'NULLABLE'},
        {'name': 'stock_value', 'type': 'Numeric', 'mode': 'NULLABLE'},
        # {'name': 'dated', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'company_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=dag)


# # # # # # # # # # # # # # # # # # # # # # # # # # # Sale Data  # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def insert_sale_data_to_gcs():
    df = pd.read_excel(
        f'/home/admin2/airflow/dags/files/AZEE_DT138_06JUN.xlsx', sheet_name='SaleFormate')
    # df.columns = ['company_code','ibl_distributor_code','distributor_item_code','distributor_item_description','lot_number','expiry_date','stock_qty','stock_value','dated']
    df.columns = ['order_no', 'invoice_date', 'invoice_no', 'channel', 'ibl_distributor_code', 'distributor_customer_no', 'ibl_customer_no',
                  'customer_name', 'distributor_item_code', 'ibl_item_code', 'item_description', 'qty_sold', 'gross_amount', 'reason', 'discount', 'bonus_qty']
    # df['dated'] = datetime.today().strftime('%Y-%m-%d')
    df['company_code'] = '1001'
    df['transfer_date'] = datetime.today()
    company_code = '1001'
    # display(df)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
    file_path = r'/home/arslan/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'

    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    # buckets = list(storage_client.list_buckets())
    today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
    past_date = today - pd.DateOffset(days=1)
    current_Datetime = past_date.strftime("%Y-%m-%d")
    # current_Datetime = datetime.today().strftime('%Y-%m-%d')
    # current_Datetime = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)

    # filename =f'Franchise_Sale_Data_{current_Datetime}_{company_code}'
    filename = f'Franchise_Sale_Data_2022-06-06_{company_code}'
    bucket.blob(f'staging/franchise/sales/{filename}.csv').upload_from_string(
        df.to_csv(index=False), 'text/csv')

    # df.to_csv(f'gs://iblopers/staging/franchise/{filename}',index=False)


t3 = PythonOperator(
    task_id='insert_sale_data_from_csv_to_gcs',
    python_callable=insert_sale_data_to_gcs,
    dag=dag,
)

t4 = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id='insert_sale_data_from_gcs_to_bq',
    bucket='ibloper',
    source_objects='staging/franchise/sales/Franchise_Sale_Data_2022-06-06*',
    # source_objects='staging/franchise/sale/Franchise_Sale_Data_2022-04-12*',
    destination_project_dataset_table='data-light-house-prod.EDW.FRANCHISE_SALES',
    schema_fields=[
        # Define schema as per the csv placed in google cloud storage
        {'name': 'order_no', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'invoice_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'invoice_no', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'channel', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ibl_distributor_code', 'type': 'STRING', 'mode': 'NULLABLE'},
       	{'name': 'distributor_customer_no', 'type':  'STRING', 'mode': 'NULLABLE'},
        {'name': 'ibl_customer_no', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'distributor_item_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ibl_item_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'item_description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'qty_sold', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'gross_amount', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'reason', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'discount', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'bonus_qty', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        {'name': 'company_code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'transfer_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    dag=dag)

t1 >> t2 >> t3 >> t4
