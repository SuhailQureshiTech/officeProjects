from datetime import date, datetime,timedelta
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
import  pysftp
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
from airflow.providers.postgres.operators.postgres import PostgresOperator
from google.cloud import storage
import pandas as pds
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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"

df=pd.DataFrame()
params = None

today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=1)
d1 = past_date.strftime("%Y-%m-%d")


os.system('cls')
today = date.today()

vTodayDate = datetime.date(datetime.today())
vTodayDate = int(vTodayDate.strftime("%d"))
vdayDiff=5

if vTodayDate == 1:
    print('two')

    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    vdayDiff = int(vEndDate.strftime("%d"))
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

else:
    print('else')

    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

# vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
# vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"

vStartDate = "'"+str(vStartDate.strftime("%Y-%m-%d"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%Y-%m-%d"))+"'"


# def Lo8ad_SAP_ITEM_BUS_LINE_Data_to_gcs():
#     conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
#     file_path = r'/home/admin2/airflow/dag/Google_cloud'
#     GCS_PROJECT = 'data-light-house-prod'
#     GCS_BUCKET = 'ibloper'
#     storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
#     client = storage.Client(project=GCS_PROJECT)
#     bucket = client.get_bucket(GCS_BUCKET)
#    # print(current_Datetime)
#     df = pds.read_sql(f'''select * from "DW"."SAP_ITEM_BUSLINE" ''',conn)
#     df['transfer_date'] = datetime.today()
#     # filename =f'IBL_SALES_{d1}'
#     filename =f'SAP_ITEM_BUS_LINE'
#     bucket.blob(f'staging/sales/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
#     conn.close()

default_args = {
      'owner': 'admin',
      'depends_on_past': False,
      'email': ['muhammad.arslan@iblgrp.com'],
      'email_on_failure': True,
        # 'start_date': datetime(2022,2,11)
      }

# dag = models.DAG('Insert_Sales_Data', default_args=default_args,schedule_interval='@daily')
with DAG(
    dag_id='Booking_vs_Execution_Final',
    # start date:28-03-2017
    start_date= datetime(year=2022, month=10, day=25),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    dagrun_timeout=timedelta(minutes=120),
    schedule_interval='0 3 * * *',
    catchup=False
    ) as dag:

    Delete_Data_Booking_Execution = BigQueryOperator(
    task_id='Delete_Data_Booking_Execution',
    bigquery_conn_id='bigquery',
    use_legacy_sql=False,
    sql=f'''
          delete from `data-light-house-prod.EDW.IBL_BOOKING_EXECUTION`
          where cast(DELIVERY_DATE as date) BETWEEN {vStartDate} and {vEndDate}
      ''',
    dag=dag)

    def showDate():
          print('v start date : ',vStartDate)
          print('v end date : ',vEndDate)

    showDatePython = PythonOperator(
        task_id="showDate",
        python_callable=showDate,
        dag=dag
    )

    Insert_Data_Booking_Execution = BigQueryOperator(

    task_id='Insert_Data_Booking_Execution',
    bigquery_conn_id='bigquery',
    use_legacy_sql=False,
    sql=f'''
        #standardSQL
        insert into `data-light-house-prod.EDW.IBL_BOOKING_EXECUTION` (company_code,branch_id,branch_description,item_code,item_Description,business_line_id,business_line_description,status,order_number,sales_order_no,order_date,delivery_date,customer_number,customer_name,booker_code,booker_name,supplier_id,supplier_name,order_amount,order_units,sold_qty,unit_selling_price,return_amount,cancelled,gross_amount,data_flag,return_reason_code,pjp_code,pjp_name,transfer_date)
        SELECT
          COMPANY_CODE,
          ORG_ID,
          ORG_DESC,
          ITEM_CODE,
          ITEM_DESC,
          BUSINESS_LINE_ID,
          BUSINESS_LINE_DESC,
          STATUS,
          ORDER_NUMBER,
          SALES_ORDER_NO,
          ORDER_DATE,
          DELIVERY_DATE,
          CUSTOMER_NUMBER,
          CUSTOMER_NAME,
          BOOKER_CODE,
          BOOKER_NAME,
          SUPPLIER_ID,
          SUPPLIER_NAME,
          ORDER_AMOUNT,
          ORDER_UNITS,
          SOLD_QTY,
          UNIT_SELLING_PRICE,
          RETURN_AMOUNT,
          CANCELLED,
          GROSS_AMOUNT,
          DATA_FLAG,
          RETURN_REASON_CODE,
          PJP_CODE,
          PJP_NAME,
          CURRENT_TIMESTAMP() as transfer_date
        FROM (SELECT
          SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 1, 4) COMPANY_CODE,
          SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 5, 4) ORG_ID,
          SO.ORG_DESC,
          CAST(OI.SAP_SKU_CODE AS string) ITEM_CODE,
          SIB.ITEM_DESC,
          SIB.BUSINESS_LINE_ID,
          SIB.business_line_desc,
          OI.STATUS,
          OI.ORDER_NUMBER,
          NULL SALES_ORDER_NO,
          cast(OI.ORDER_DATE as date)ORDER_DATE,
          cast(OI.DELIVERY_DATE as date)DELIVERY_DATE,
          OI.SAP_STORE_CODE CUSTOMER_NUMBER,
          OI.STORE_NAME CUSTOMER_NAME,
          OI.ORDER_BOOKER_CODE BOOKER_CODE,
          OI.ORDER_BOOKER_NAME BOOKER_NAME,
          NULL SUPPLIER_ID,
          NULL SUPPLIER_NAME,
          OI.ORDER_AMOUNT,
          OI.ORDER_UNITS,
          0 SOLD_QTY,
          0 UNIT_SELLING_PRICE,
          NULL RETURN_AMOUNT,
          NULL CANCELLED,
          NULL GROSS_AMOUNT,
          1 DATA_FLAG,
          NULL RETURN_REASON_CODE,
          OI.PJP_CODE,
          OI.PJP_NAME
        FROM `data-light-house-prod.EDW.IBL_SALESFLO_ORDERS` OI
        LEFT OUTER JOIN `data-light-house-prod.EDW.VW_ORG` SO
          ON (SO.ORG_ID = SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 5, 4))
        LEFT OUTER JOIN `data-light-house-prod.EDW.SAP_ITEM_BUSINESS_LINE` SIB
          ON (SIB.ITEM_CODE = OI.SAP_SKU_CODE)
        WHERE 1 = 1
        AND cast(DELIVERY_DATE as date) BETWEEN   {vStartDate} and {vEndDate}
        UNION ALL
        SELECT
          COMPANY_CODE,
          BRANCH_ID,
          BRANCH_DESCRIPTION,
          ITEM_CODE,
          ITEM_DESC,
          BUSINESS_LINE_ID,
          BUSINESS_LINE_DESCRIPTION,
          STATUS,
          ORDER_NUMBER,
          SALES_ORDER_NO,
          cast(ORDER_DATE as date)ORDER_DATE,
          cast(DELIVERY_DATE as date)DELIVERY_DATE,
          CUSTOMER_NUMBER,
          CUSTOMER_NAME,
          BOOKER_CODE,
          BOOKER_NAME,
          SUPPLIER_ID,
          SUPPLIER_NAME,
          sum(ORDER_AMOUNT),
          SUM(ORDER_UNITS) ORDER_UNITS,
          SUM(SOLD_QTY) SOLD_QTY,
          CASE
            WHEN (COALESCE(SUM(SOLD_QTY), 0) = 0) THEN 0
            ELSE SUM(GROSS_AMOUNT) / SUM(SOLD_QTY)
          END UNIT_SELLING_PRICE,
          SUM(RETURN_AMOUNT) RETURN_AMOUNT,
          SUM(CANCELLED) CANCELLED,
          SUM(GROSS_AMOUNT) GROSS_AMOUNT,
          DATA_FLAG,
          RETURN_REASON_CODE,
          NULL PJP_CODE,
          NULL PJP_NAME
        FROM (SELECT
          PSD.COMPANY_CODE,
          PSD.branch_id,
          PSD.branch_description,
          SUBSTRING(PSD.ITEM_CODE, 9, 10) ITEM_CODE,
          PSD.item_DESCRIPTION ITEM_DESC,
          PSD.BUSINESS_LINE_ID,
          PSD.BUSINESS_LINE_description,
          'Invoiced' STATUS,
          CASE
            WHEN LENGTH(salesflo_order_no) = 1 THEN PSD.SALES_ORDER_NO
            ELSE salesflo_order_no
          END ORDER_NUMBER,
          PSD.SALES_ORDER_NO,
          CASE
            WHEN
              LENGTH(salesflo_order_no) = 1 THEN cast(PSD.billing_date as date)
            ELSE (SELECT
              DISTINCT
                cast(OI.ORDER_DATE as date)ORDER_DATE
              FROM `data-light-house-prod.EDW.IBL_SALESFLO_ORDERS` OI
              WHERE 1 = 1
              AND OI.ORDER_NUMBER = PSD.salesflo_order_no AND cast(DELIVERY_DATE as date) >= '2019-04-21'
              AND OI.SAP_SKU_CODE = SUBSTRING(PSD.ITEM_CODE, 9, 10))
          END ORDER_DATE,
          PSD.billing_date DELIVERY_DATE,
          PSD.CUSTOMER_NUMBER,
          PSD.CUSTOMER_NAME,
          PSD.BOOKER_ID BOOKER_CODE,
          PSD.BOOKER_NAME,
          PSD.SUPPLIER_ID,
          PSD.SUPPLIER_NAME,
          COALESCE
          (CASE
            WHEN LENGTH(salesflo_order_no) = 1 THEN PSD.GROSS_AMOUNT
            ELSE CASE
                WHEN UPPER(PSD.SALES_ORDER_TYPE) LIKE '%CANCEL%' THEN 0
                ELSE (SELECT
                    sum(OI.ORDER_AMOUNT)
                  FROM `data-light-house-prod.EDW.IBL_SALESFLO_ORDERS` OI
                  WHERE 1 = 1 AND  cast(DELIVERY_DATE as date)>='2019-04-21'
                  AND OI.ORDER_NUMBER = PSD.salesflo_order_no
                  AND OI.SAP_SKU_CODE = SUBSTRING(PSD.ITEM_CODE, 9, 10))
              END
          END, 0) ORDER_AMOUNT,
          0 ORDER_UNITS,
          PSD.SOLD_QTY,
          PSD.UNIT_SELLING_PRICE,
          COALESCE(
          CASE
            WHEN
              SALES_ORDER_TYPE IN
              (
              'OPS-Sales Returns', 'IBL UB Billing Retur'
              ) THEN (PSD.GROSS_AMOUNT)
          END, 0) RETURN_AMOUNT,
          COALESCE(
          CASE
            WHEN
              SALES_ORDER_TYPE IN
              (
              'Cancel Bill NE Sales',
              'OPS Cancel. Invoice',
              'OPS-Cancel Cred Memo',
              'Cancel of Cred Memo',
              'UB Cancel. Invoice',
              'UB-Cancel Cred Memo'
              ) THEN (PSD.GROSS_AMOUNT)
          END, 0) CANCELLED,
          PSD.GROSS_AMOUNT,
          2 DATA_FLAG,
          RETURN_REASON_CODE
        FROM `data-light-house-prod.EDW.IBL_SALES` PSD
        WHERE 1 = 1
        AND cast(PSD.billing_date as date)  BETWEEN  {vStartDate} and {vEndDate}
        AND ITEM_CATEGORY NOT IN ('ZFOU', 'ZFCL', 'ZBRU', 'ZREI', 'ZBRC')
        ) A
        GROUP BY COMPANY_CODE,
              BRANCH_ID,
              BRANCH_dESCRIPTION,
              ITEM_CODE,
              ITEM_DESC,
              BUSINESS_LINE_ID,
              BUSINESS_LINE_DESCRIPTION,
              STATUS,
              ORDER_NUMBER,
              SALES_ORDER_NO,
              ORDER_DATE,
              DELIVERY_DATE,
              CUSTOMER_NUMBER,
              CUSTOMER_NAME,
              BOOKER_CODE,
              BOOKER_NAME,
              SUPPLIER_ID,
              SUPPLIER_NAME,
              DATA_FLAG,
              RETURN_REASON_CODE
              ) A
              WHERE 1 = 1
    ''',
    dag=dag)

    Delete_Flag1_Records_Booking_Execution = BigQueryOperator(

    task_id='Delete_Flag1_Records_Booking_Execution',
    bigquery_conn_id='bigquery',
    use_legacy_sql=False,
    sql=f'''
        delete from  `data-light-house-prod.EDW.IBL_BOOKING_EXECUTION` booking_exec
        where ORDER_NUMBER  in (SELECT
          DISTINCT
            salesflo_order_no
          FROM `data-light-house-prod.EDW.IBL_SALES` psd
          WHERE cast(billing_Date as date) BETWEEN   {vStartDate} and {vEndDate}
          AND SUBSTRING(PSD.ITEM_CODE, 9, 10) = booking_exec.ITEM_CODE
          and booking_exec.data_flag =1
        )
        and  cast(booking_exec.DELIVERY_DATE as date) BETWEEN   {vStartDate} and {vEndDate}
      ''',
    dag=dag)

    Update_Records_Booking_Execution = BigQueryOperator(

    task_id='Update_Records_Booking_Execution',
    bigquery_conn_id='bigquery',
    use_legacy_sql=False,
    sql=f'''
    UPDATE `data-light-house-prod.EDW.IBL_BOOKING_EXECUTION` BOOKING_EXEC_DATA
            SET ORDER_UNITS = (SELECT
            sum(OI.ORDER_UNITS)
            FROM `data-light-house-prod.EDW.IBL_SALESFLO_ORDERS` OI
            WHERE 1 = 1
            AND cast(OI.DELIVERY_DATE as date) BETWEEN  {vStartDate} and {vEndDate}
            AND OI.ORDER_NUMBER = BOOKING_EXEC_DATA.ORDER_NUMBER
            AND OI.SAP_SKU_CODE = BOOKING_EXEC_DATA.ITEM_CODE
            AND SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 1, 4) = BOOKING_EXEC_DATA.COMPANY_CODE)
            WHERE cast(DELIVERY_DATE as date) BETWEEN  {vStartDate} and {vEndDate}
      ''',
    dag=dag)

    # t1>>Incremental_Load_Salesflo_orders>>Incremental_Load_saleflo_data_gcs_to_bq>>Delete_Data_Booking_Execution>>Insert_Data_Booking_Execution>>Delete_Flag1_Records_Booking_Execution>>Update_Records_Booking_Execution>>t2>>t4
    showDatePython>> Delete_Data_Booking_Execution >> Insert_Data_Booking_Execution >> Delete_Flag1_Records_Booking_Execution >> Update_Records_Booking_Execution



