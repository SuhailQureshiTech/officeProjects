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

if vTodayDate == 1:
    print('two')

    vEndDate = datetime.date(datetime.today()-timedelta(days=1))
    vdayDiff = int(vEndDate.strftime("%d"))
    vStartDate = datetime.date(datetime.today()-timedelta(days=vdayDiff))

else:
    print('else')

    vStartDate = datetime.date(datetime.today()-timedelta(days=6))
    vEndDate = datetime.date(datetime.today()-timedelta(days=1))

vStartDate = "'"+str(vStartDate.strftime("%d-%b-%Y"))+"'"
vEndDate = "'"+str(vEndDate.strftime("%d-%b-%Y"))+"'"


# def downloadFiles():
#     hostName = "saturn.retailistan.com"
#     userName = "IBL"
#     HostPassword = "!2i`*7^im&t23$a*Z"
#     # Accept any host key (still wrong see below)
#     cnopts = pysftp.CnOpts()
#     cnopts.hostkeys = None
#     today = date.today()
#     d6 = today.strftime("%Y_%m_%d")
#     fileName = 'IBL_Order_Info_'+d6
#     print('d6 :',d6)
#     print('fileNames :', filename)
#     with pysftp.Connection(host=hostName, username=userName, password=HostPassword, private_key=".ppk", cnopts=cnopts) as sftp:
#     #switching to remote path
#         remoteFile = f'''/order-info-data/{fileName}.csv'''
#         localFilePath = f'''/home/admin2/airflow/dags/SalesFloDataFiles/{fileName}.csv'''

#         sftp.get(remoteFile,localFilePath)

# def INSERT_DATA_FROM_FILE_TO_ORDER_INFO():
#     global df
#     path = '/home/admin2/airflow/dags/SalesFloDataFiles'
#     files=glob.glob(path+"/*.csv")
#     salesFloFiles= [f for f in files]
#     # print(salesFloFiles)
#     # df=pd.DataFrame()
#     for f in salesFloFiles:
#         fileData=pd.read_csv(f,low_memory=False)
#         fileData['fileName']=f
#         df=fileData
#         df["SAP_SKU_CODE"] = df["SAP_SKU_CODE"].astype(str)
#         df['ORDER_DATE'].replace('0000-00-00','',inplace=True)
#         df['DELIVERY_DATE'].replace('0000-00-00', '', inplace=True)
#         df['ADDED_ON'].replace('0000-00-00', '', inplace=True)
#     df.columns = df.columns.str.strip()
#     conn = pg.connect("dbname='DATAWAREHOUSE' user='postgres' host='35.216.168.189' port='5433' password='ibl@123@456'")
#     cursor = conn.cursor()
#     if len(df) > 0:
#         df_columns = list(df)
#         columns = ",".join(df_columns)

#     # create VALUES('%s', '%s",...) one '%s' per column
#     values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

#     #create INSERT INTO table (columns) VALUES('%s',...)
#     insert_stmt = "INSERT INTO {} ({}) {}".format('"DW"."SALESFLO_ORDERS"',columns,values)

#     pg.extras.execute_batch(cursor, insert_stmt, df.values)
#     conn.commit()
#     cursor.close()

# def DELETE_MOVE_FILE_BKP():
#     sourcePath = '/home/admin2/airflow/dags/SalesFloDataFiles/'
#     targetPath = '/home/admin2/airflow/dags/SalesFloDataFiles/bkup'

#     listFiles=os.listdir(sourcePath)
#     for f in listFiles:
#         if os.path.isfile(os.path.join(targetPath,f)):
#             os.remove(os.path.join(targetPath,f))
#         shutil.move(sourcePath+f, targetPath)

# def Incremental_Load_Salesflo_orders():
#     global df
#     path = '/home/admin2/airflow/dags/SalesFloDataFiles'
#     files=glob.glob(path+"/*.csv")
#     salesFloFiles= [f for f in files]
#     # print(salesFloFiles)
#     # df=pd.DataFrame()
#     for f in salesFloFiles:
#         fileData=pd.read_csv(f,low_memory=False)
#         fileData['fileName']=f
#         df=fileData
#         df["SAP_SKU_CODE"] = df["SAP_SKU_CODE"].astype(str)
#         df['ORDER_DATE'].replace('0000-00-00','',inplace=True)
#         df['DELIVERY_DATE'].replace('0000-00-00', '', inplace=True)
#         df['ADDED_ON'].replace('0000-00-00', '', inplace=True)
#         df['transfer_date'] = datetime.today()
#     df.columns = df.columns.str.strip()
#     if len(df) > 0:
#         df_columns = list(df)
#         columns = ",".join(df_columns)

#     # create VALUES('%s', '%s",...) one '%s' per column
#     values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
#     file_path = r'/home/admin2/airflow/dag/Google_cloud'
#     GCS_PROJECT = 'data-light-house-prod'
#     GCS_BUCKET = 'ibloper'
#     storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
#     client = storage.Client(project=GCS_PROJECT)
#     bucket = client.get_bucket(GCS_BUCKET)
#    # print(current_Datetime)

#     # filename =f'IBL_SALES_{d1}'
#     filename =f'SALESFLO_ORDERS_{d1}'

#     bucket.blob(f'staging/sales/Salesflo_Orders/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
#     #create INSERT INTO table (columns) VALUES('%s',...)
#     # insert_stmt = "INSERT INTO {} ({}) {}".format('"DW"."SALESFLO_ORDERS"',columns,values)

#     # pg.extras.execute_batch(cursor, insert_stmt, df.values)
#     # conn.commit()
#     # cursor.close()


# def Full_Load_Saleflo_Data_to_gcs():

#     conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
#     file_path = r'/home/admin2/airflow/dag/Google_cloud'
#     GCS_PROJECT = 'data-light-house-prod'
#     GCS_BUCKET = 'ibloper'
#     storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
#     client = storage.Client(project=GCS_PROJECT)
#     bucket = client.get_bucket(GCS_BUCKET)
#    # print(current_Datetime)
#     df = pds.read_sql(f'''select * from "DW"."SALESFLO_ORDERS" ''',conn)
#     df['transfer_date'] = datetime.today()
#     # filename =f'IBL_SALES_{d1}'
#     filename =f'SALESFLO_ORDERS'
#     bucket.blob(f'staging/sales/Salesflo_Orders/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
#     conn.close()


# def Load_SAP_ITEM_BUS_LINE_Data_to_gcs():
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

# def Full_Load_Booking_Execution_Data_to_gcs():
#     conn = pg.connect(host="192.168.130.81", port= '5433', database="hanadb_prd", user="postgres", password="ibl@123@456")
#     file_path = r'/home/admin2/airflow/dag/Google_cloud'
#     GCS_PROJECT = 'data-light-house-prod'
#     GCS_BUCKET = 'ibloper'
#     storage_client = storage.Client.from_service_account_json(r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
#     client = storage.Client(project=GCS_PROJECT)
#     bucket = client.get_bucket(GCS_BUCKET)
#    # print(current_Datetime)
#     df = pds.read_sql(f'''select company_code,org_id branch_id,org_desc branch_description,item_code,item_desc item_Description,business_line_id,business_line  business_line_description,status,order_number,sales_order_no,order_date,delivery_date,customer_number,customer_name,booker_code,booker_name,supplier_id,supplier_name,order_amount,order_units,sold_qty,unit_selling_price,return_amount,cancelled,gross_amount,data_flag,return_reason_code,pjp_code,pjp_name
# from "DW"."BOOKING_EXEC_DATA" ''',conn)
#     df['transfer_date'] = datetime.today()
#     # filename =f'IBL_SALES_{d1}'
#     filename =f'IBL_BOOKING_EXECUTION'
#     bucket.blob(f'staging/sales/BOOKING_EXECUTION/{filename}.csv').upload_from_string(df.to_csv(index=False), 'text/csv')
#     conn.close()


default_args = {
      'owner': 'admin',
      'depends_on_past': False,
      'email': ['muhammad.arslan@iblgrp.com'],
	    'email_on_failure': True
      }

with DAG(
    dag_id='Booking_vs_Execution_6200',
    # start date:28-03-2017
    catchup=False,
    start_date= datetime(year=2022, month=10, day=25),
    #schedule_interval='30 02 * * *'
    schedule_interval=None,
    ) as dag:

  #   t1 = PythonOperator(
	# 	task_id='DOWNLOAD_FILE_FROM_FTP',
  #       python_callable= downloadFiles,
  #       dag=dag,

	# )

    # t2= PythonOperator(
    #         task_id='INSERT_DATA_FROM_FILE_TO_ORDER_INFO',
    #         python_callable= INSERT_DATA_FROM_FILE_TO_ORDER_INFO,
    #         dag=dag,

    #     )


  #   Incremental_Load_Salesflo_orders= PythonOperator(
	# 	task_id='Incremental_Load_Salesflo_orders',
  #       python_callable= Incremental_Load_Salesflo_orders,
  #       dag=dag,
	# )

    # Incremental_Load_saleflo_data_gcs_to_bq = GCSToBigQueryOperator(
    # task_id='Incremental_Load_saleflo_data_gcs_to_bq',
    # bucket='ibloper',
    # source_objects=f'staging/sales/Salesflo_Orders/SALESFLO_ORDERS_{d1}.csv',
    # destination_project_dataset_table='data-light-house-prod.EDW.IBL_SALESFLO_ORDERS',
    # schema_fields=[
    # # Define schema as per the csv placed in google cloud storage
    # {'name': 'distributor_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'distributor_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'sap_distributor_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'pjp_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'pjp_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'store_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'store_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'sap_store_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'store_latitude', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'store_longitude', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'sku_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'sku_manufacturer_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'sap_sku_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'order_booker_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'sap_order_booker_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'order_booker_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'order_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'order_amount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'order_units', 'type': 'Numeric', 'mode': 'NULLABLE'},
    # {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'order_date', 'type': 'date', 'mode': 'NULLABLE'},
    # {'name': 'delivery_date', 'type': 'date', 'mode': 'NULLABLE'},
    # {'name': 'added_on', 'type': 'date', 'mode': 'NULLABLE'},
    # {'name': 'filename', 'type': 'STRING', 'mode': 'NULLABLE'},
    # {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    # ],
    # write_disposition='WRITE_APPEND',
    # skip_leading_rows = 1,
    # dag=dag
    # )

  #   t4= PythonOperator(
	# 	task_id='DELETE_MOVE_FILE_BKP',
  #       python_callable= DELETE_MOVE_FILE_BKP,
  #       dag=dag,

	# )

    Delete_Data_Booking_Execution = BigQueryOperator(
    task_id='Delete_Data_Booking_Execution_6200',
    bigquery_conn_id='bigquery',
    use_legacy_sql=False,
    sql='''
            delete from `data-light-house-prod.EDW.IBL_BOOKING_EXECUTION_6200`
            where DELIVERY_DATE BETWEEN (current_date()-10) and (current_date()-1)
        ''',

    dag=dag)

    Insert_Data_Booking_Execution = BigQueryOperator(
    task_id='Insert_Data_Booking_Execution_6200',
    bigquery_conn_id='bigquery',
    use_legacy_sql=False,
    sql='''
    insert into `data-light-house-prod.EDW.IBL_BOOKING_EXECUTION_6200` (company_code,branch_id,branch_description,item_code,item_Description,business_line_id,business_line_description,status,order_number,sales_order_no,order_date,delivery_date,customer_number,customer_name,booker_code,booker_name,supplier_id,supplier_name,order_amount,order_units,sold_qty,unit_selling_price,return_amount,cancelled,gross_amount,data_flag,return_reason_code,pjp_code,pjp_name,transfer_date)
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
    OI.ORDER_DATE,
    OI.DELIVERY_DATE,
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
  AND DELIVERY_DATE BETWEEN (current_date()-10) and (current_date()-1)
  AND SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 1, 4)='6200'
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
    ORDER_DATE,
    DELIVERY_DATE,
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
        LENGTH(salesflo_order_no) = 1 THEN PSD.billing_date
      ELSE (SELECT
        DISTINCT
          OI.ORDER_DATE
        FROM `data-light-house-prod.EDW.IBL_SALESFLO_ORDERS` OI
        WHERE 1 = 1
        AND OI.ORDER_NUMBER = PSD.salesflo_order_no AND  DELIVERY_DATE >= '2019-04-21'
        AND SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 1, 4)='6200'
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
            WHERE 1 = 1 AND  DELIVERY_DATE >= '2019-04-21'
            AND SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 1, 4)='6200'
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
        ('LOGs Ret Billing') THEN (PSD.GROSS_AMOUNT)
    END, 0) RETURN_AMOUNT,
    COALESCE(
    CASE
      WHEN
        SALES_ORDER_TYPE IN
        ('Cancel IBL Log Billl') THEN (PSD.GROSS_AMOUNT)  END, 0) CANCELLED,
    PSD.GROSS_AMOUNT,
    2 DATA_FLAG,
    RETURN_REASON_CODE
  FROM `data-light-house-prod.EDW.LOGISTIC_SALES` PSD
  WHERE 1 = 1
  AND PSD.billing_date BETWEEN  (current_date()-10) and (current_date()-1)
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
    task_id='Delete_Flag1_Records_Booking_Execution_6200',
    bigquery_conn_id='bigquery',
    use_legacy_sql=False,
    sql='''
        delete from  `data-light-house-prod.EDW.IBL_BOOKING_EXECUTION_6200` booking_exec
        where ORDER_NUMBER  in (SELECT
          DISTINCT
            salesflo_order_no
          FROM `data-light-house-prod.EDW.LOGISTIC_SALES` psd
          WHERE billing_Date BETWEEN  (current_date()-10) and (current_date()-1)
          AND SUBSTRING(PSD.ITEM_CODE, 9, 10) = booking_exec.ITEM_CODE
          and booking_exec.data_flag =1
        )
        and booking_exec.DELIVERY_DATE BETWEEN  (current_date()-10) and (current_date()-1)
      ''',
    dag=dag)

    Update_Records_Booking_Execution = BigQueryOperator(

    task_id='Update_Records_Booking_Execution',

    bigquery_conn_id='bigquery',

    use_legacy_sql=False,

    sql='''
    UPDATE `data-light-house-prod.EDW.IBL_BOOKING_EXECUTION_6200` BOOKING_EXEC_DATA
            SET ORDER_UNITS = (SELECT
            sum(OI.ORDER_UNITS)
            FROM `data-light-house-prod.EDW.IBL_SALESFLO_ORDERS` OI
            WHERE 1 = 1
            AND OI.DELIVERY_DATE BETWEEN  (current_date()-10) and (current_date()-1)
            AND SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 1, 4)='6200'
            AND OI.ORDER_NUMBER = BOOKING_EXEC_DATA.ORDER_NUMBER
            AND OI.SAP_SKU_CODE = BOOKING_EXEC_DATA.ITEM_CODE
            AND SUBSTRING(OI.SAP_DISTRIBUTOR_CODE, 1, 4) = BOOKING_EXEC_DATA.COMPANY_CODE)
            WHERE DELIVERY_DATE BETWEEN   (current_date()-10) and (current_date()-1)
          ''',
    dag=dag)

    # Incremental_Load_Salesflo_orders>>
    Delete_Data_Booking_Execution>>Insert_Data_Booking_Execution>>Delete_Flag1_Records_Booking_Execution>>Update_Records_Booking_Execution


