from datetime import date, datetime, timedelta
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
import pysftp
from datetime import date
import pandas as pd
import glob
import pypyodbc as odbc
from pytest import param
import psycopg2 as pg
import psycopg2.extras
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
from datetime import datetime, date, timezone
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
# from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud.exceptions import NotFound
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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
today = date.today()
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=1)
d1 = past_date.strftime("%Y-%m-%d")


df = pd.DataFrame()
day_diff=10
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=day_diff)
d1 = past_date.strftime("%Y-%m-%d")
df = pd.DataFrame()
# verify the architecture of Python
# print("Platform architecture: " + platform.architecture()[0])

today = date.today()
vEndDate = datetime.date(
    datetime.today()-timedelta(days=day_diff)).strftime("%Y%m%d")
vMaxDate = datetime.date(datetime.today()-timedelta(days=1)).strftime("%Y%m%d")

vEndDate1 = datetime.date(
    datetime.today()-timedelta(days=day_diff)).strftime("%Y-%m-%d")

vMaxDate1 = datetime.date(
    datetime.today()-timedelta(days=1)).strftime("%Y-%m-%d")

vEndDate = "'"+vEndDate+"'"
vMaxDate = "'"+vMaxDate+"'"

vEndDate1 = "'"+vEndDate1+"'"
vMaxDate1 = "'"+vMaxDate1+"'"

# Initialize your connection---

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
    password='Etl@2025'

    # Additional parameters
    # encrypt=True, # must be set to True when connecting to HANA as a Service
    # As of SAP HANA Client 2.6, connections on port 443 enable encryption by default (HANA Cloud)
    # sslValidateCertificate=False #Must be set to false when connecting
    # to an SAP HANA, express edition instance that uses a self-signed certificate.
)
# If no errors, print connected
print('connected')
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                "SERVER=192.168.130.81\sqldw;"
                                "DATABASE=ibl_dw;"
                                "UID=pbironew;"
                                "PWD=pbiro345-")

cursor = conn.cursor()
del_command = f'''DELETE FROM PHNX_SALES_DETAIL_DATA_LOG '''
del_command1 = f'''DELETE FROM PHNX_SALES_DATA_LOG '''
del_command2 = f'''DELETE FROM PHNX_SALES_DATA_TMP_KONV_LOG '''


# def fileCheck():
#     path = "/home/admin2/airflow/dags/checkFile/salelogistic.txt"
#     isFile = os.path.isfile(path)
#     if isFile == True:
#         print('Yes file exists', isFile)
#         raise AirflowFailException('Faile file exists cannot merge')
#     else:
#         print('Yes file exists', isFile)


# def writeFile():

#     with open('/home/admin2/airflow/dags/checkFile/salelogistic.txt', 'w') as f:
#         f.write('Create a new text file!')
#     print('File Created...')

def checkSapData():
    print('end date: ', vEndDate)
    print('end date maxDate: ', vMaxDate)
    print('end date - vMaxDate1: ', vMaxDate1)
    # credentialsData = os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
    credentials = service_account.Credentials.from_service_account_file(
        '/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    # pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)
    recCount = f''' select * from data-light-house-prod.EDW.LOGISTIC_SALES
                    where billing_date={vMaxDate1} limit 1
                    '''

    df = pandas_gbq.read_gbq(
        recCount, credentials=credentials,  project_id=project_id)
    vCount = 0
    vCount = len(df)
    print('query ... : ', recCount)
    print('dataframe count :', vCount)
    if vCount>=1:
        raise AirflowFailException('Merging already done')
    else:
        print('Merging pending.....')


def deleteData():
    cursor.execute(del_command)
    cursor.execute(del_command1)
    cursor.execute(del_command2)


def insertSapSales():
    print('sap sales data :',vEndDate)
    print('sap sales max data :', vMaxDate)
    insert_command1 = f'''
    INSERT INTO PHNX_SALES_DETAIL_DATA_LOG
    select
            distinct
            "VBRP"."PSTYV" as ITEM_CATEGORY,
            "VBRK"."FKART" as BILLING_TYPE,
            "VBRK"."KNUMV" ||'-' ||"VBRP"."POSNR"  UP_KEY,
        --	concat("VBRK"."KNUMV", '-', "VBRP"."POSNR") UP_KEY,
            "VBRK"."VBELN" as DOCUMENT_NO,
            (case
                when "VBRK"."FKART" = 'ZUCC' then 'Cancelled'
                when "VBRK"."FKART" = 'ZURB' then 'Return'
            end) as CANCELLED_FLAG,
            "VBRK"."VKORG" as COMPANY_CODE,
            "VBRK"."KNUMV" as DOCUMENT_CONDITION,
            cast("FKDAT" as date) as BILLING_DATE,
            "KDGRP" as CHANNEL,
            "KUNAG" as CUSTOMER_CODE,
            "FKSTO" as BILLING_CANCELLED,
            "ZTERM" as PAYMENT_TYPE,
            "VBRP"."POSNR" as ITEM_NO,
            (case
                when "VBRP"."PSTYV" = 'ZBRU'
                or "VBRP"."PSTYV" = 'ZBRC' then "VBRP"."FKIMG" *-1
                else "VBRP"."FKIMG"
            end)as QUANTITY,
            "VBRP"."MEINS" as UNIT,
            0 GROSS_AMOUNT,
            0 UNIT_SELLING_PRICE,
            "VBRP"."NETWR" as AMOUNT,
            "VBRP"."MATNR" as MATERIAL_CODE,
            "VBRP"."KOSTL" as COST_CENTRE,
            "VBRP"."PRCTR" as PROFIT_CENTRE,
            "VBRP"."VKBUR" as ORG_ID,
            "VBRP"."AUBEL" as SALES_ORDER_NO,
            "VBRP"."MVGR1" as BUSINESS_LINE_ID,
            VBP1."PERNR" BOOKER_ID,
            "VBPA"."PERNR" SUPPLIER_ID,
            "VBAK"."AUART" SALES_ORDER_TYPE,
            "VBAK"."BSTNK" BSTNK,
            "VBRP"."ARKTX" as ITEM_DESC,
            (case when "VBRP"."PSTYV" = 'ZFOU' then "VBRP"."FKIMG"
                when "VBRP"."PSTYV" = 'ZBRU' then "VBRP"."FKIMG" *-1
            end) as UNCLAIM_BONUS_QUANTITY,
            (case
                when "VBRP"."PSTYV" = 'ZFCL' then "VBRP"."FKIMG"
                when "VBRP"."PSTYV" = 'ZBRC' then "VBRP"."FKIMG" *-1
            end) as CLAIM_BONUS_QUANTITY,
            ---"TVFKT"."VTEXT" BILLING_TYPE_TEXT,
            0 CLAIMABLE_DISCOUNT,
            0 UNCLAIMABLE_DISCOUNT,
            0 TAX_RECOVERABLE,
            "TVFKT"."VTEXT" BILLING_TYPE_TEXT,
            "TVAPT"."VTEXT" ITEM_CATEGORY_TEXT,
            "VBRK"."VBTYP" as SD_DOCUMENT_CATEGORY,
            "VBRK"."KNUMV" knumv,
            "VBRP"."POSNR" posnr,
            CASE WHEN "VBAK"."AUGRU"='' THEN VBAP.ABGRU ELSE "VBAK"."AUGRU" END AUGRU
            ---"VBRP"."Loading_Date" loading_date
            from SAPABAP1."VBRK"
        inner  join SAPABAP1.VBRP on ("VBRP"."VBELN" = "VBRK"."VBELN")
        inner  join SAPABAP1.VBPA VBP1 on (vbp1 ."VBELN" = "VBRP"."VBELN" and vbp1 ."PARVW" = 'BK')
        inner  join SAPABAP1.VBPA on ("VBPA"."VBELN" = "VBRP"."VBELN" and "VBPA"."PARVW" = 'ZS')
        inner join SAPABAP1.VBAK on ("VBAK"."VBELN" = "VBRP"."AUBEL")
        INNER JOIN SAPABAP1.VBAP ON (VBRP.AUBEL = VBAP.VBELN and VBRP.AUPOS = VBAP.POSNR)
        left outer join SAPABAP1.TVFKT on ("VBRK"."FKART" = "TVFKT"."FKART" and "TVFKT"."SPRAS"='E')
        left outer join SAPABAP1.TVAPT on ("VBRP"."PSTYV" = "TVAPT"."PSTYV" and "TVAPT"."SPRAS" ='E' )
        where
            1 = 1
            and "FKDAT" between {vEndDate} and {vMaxDate}
	    and  "VBRP"."VKBUR" not in ('8601')
                    and "VBRK"."VKORG" in ('6200')
                    and "VBRK"."FKART" in ( 'ZIUP','ZIBB','ZLRB','ZSII','ZCMR','ZCUP','ZSIC','ZLB2','ZIBC')

                '''
    insert_command2 = f'''
        INSERT INTO phnx_sales_data_tmp_konv_LOG
                (
        select "KNUMV"||'-'||"KPOSN" up_key,KNUMV,KPOSN,sum(CLAIMABLE_DISCOUNT) CLAIMABLE_DISCOUNT,sum(UNCLAIMABLE_DISCOUNT) UNCLAIMABLE_DISCOUNT,sum(TAX_RECOVERABLE) TAX_RECOVERABLE,sum(UNIT_SELLING_PRICE) UNIT_SELLING_PRICE
        from (
        SELECT
            distinct
            KONV1."KNUMV" as KNUMV,
            KONV1."KPOSN" as KPOSN,
            (case when KONV1."KSCHL" ='ZCDP' or KONV1."KSCHL" ='ZCDV' or "KSCHL" ='ZCVD' or KONV1."KSCHL" ='ZCP' then  Sum(KONV1."KWERT")*-1 end ) as CLAIMABLE_DISCOUNT,
            (case when KONV1."KSCHL" ='ZUDP' or KONV1."KSCHL" ='ZUDV' or KONV1."KSCHL" ='ZUP'  then  Sum(KONV1."KWERT")*-1 end ) as UNCLAIMABLE_DISCOUNT,
            (case when KONV1."KSCHL" ='ZMWS' or KONV1."KSCHL" ='ZFTX' or KONV1."KSCHL" ='ZADV' or KONV1."KSCHL" ='ZMRT' or KONV1."KSCHL" ='ZEXT'  or KONV1."KSCHL" ='ZSRG' then  Sum(KONV1."KWERT") end ) as TAX_RECOVERABLE,
            (case when KONV1."KSCHL" ='ZTRP' then sum(KONV1."KBETR") end) as UNIT_SELLING_PRICE
            from SAPABAP1."KONV" KONV1
            where KONV1."KINAK"<>'Y' and  (KONV1."KNUMV", KONV1."KPOSN")  in (select KNUMV ,POSNR
            from PHNX_SALES_DETAIL_DATA_LOG)
            group by  KONV1."KNUMV",KONV1."KPOSN" ,KONV1."KSCHL"
        ) A
        group by KNUMV,KPOSN
        )
        '''
    update_statement = f'''
        UPDATE PHNX_SALES_DETAIL_DATA_LOG psl
        set (CLAIMABLE_DISCOUNT,UNCLAIMABLE_DISCOUNT,TAX_RECOVERABLE,UNIT_SELLING_PRICE,GROSS_AMOUNT) =
        (
        select  CLAIMABLE_DISCOUNT,UNCLAIMABLE_DISCOUNT,TAX_RECOVERABLE,UNIT_SELLING_PRICE,
        (case
                when psl.BILLING_TYPE = 'ZURB' or psl.BILLING_TYPE = 'ZUCC' then (t.UNIT_SELLING_PRICE*psl.QUANTITY)*-1
                else (t.UNIT_SELLING_PRICE*psl.QUANTITY)
            end )GROSS_AMOUNT
        from phnx_sales_data_tmp_konv_LOG t
        where t.up_key= psl.UP_KEY)
            '''
    insert_command3 = f'''
        INSERT INTO PHNX_SALES_DATA_LOG(ITEM_CATEGORY
            , ORG_ID
            , ORG_DESC
            , TRX_DATE
            , TRX_NUMBER
            , BOOKER_ID
            , BOOKER_NAME
            , SUPPLIER_ID
            , SUPPLIER_NAME
            , BUSINESS_LINE_ID
            , BUSINESS_LINE
            , CHANNEL
            , CUSTOMER_ID
            , CUSTOMER_NUMBER
            , CUSTOMER_NAME
            , SALES_ORDER_TYPE
            , INVENTORY_ITEM_ID
            , ITEM_CODE
            , DESCRIPTION
            , UNIT_SELLING_PRICE
            , SOLD_QTY
            , BONUS_QTY
            , CLAIMABLE_DISCOUNT
            , UNCLAIMABLE_DISCOUNT
            , TAX_RECOVERABLE
            , NET_AMOUNT
            , GROSS_AMOUNT
            , TOTAL_DISCOUNT
            , CUSTOMER_TRX_ID
            , REASON_CODE
            , BILL_TYPE_DESC
            , COMPANY_CODE
            , BILLING_TYPE
            , SALES_ORDER_NO
            , ADD1
            , ADD2
            , ADD3
            , BSTNK
            , ITEM_NO
            ,RETURN_REASON_CODE
            )
            (
                SELECT
                ITEM_CATEGORY
                , ORG_ID
                , ORG_DESC
                , TRX_DATE
                , TRX_NUMBER
                , BOOKER_ID
                , BOOKER_NAME
                , SUPPLIER_ID
                , SUPPLIER_NAME
                , BUSINESS_LINE_ID
                , BUSINESS_LINE
                , CHANNEL
                , CUSTOMER_ID
                , CUSTOMER_NUMBER
                , CUSTOMER_NAME
                , SALES_ORDER_TYPE
                , INVENTORY_ITEM_ID
                , ITEM_CODE
                , DESCRIPTION
                , UNIT_SELLING_PRICE
                , SOLD_QTY
                , BONUS_QTY
                , CLAIMABLE_DISCOUNT
                , UNCLAIMABLE_DISCOUNT
                , TAX_RECOVERABLE
                , NET_AMOUNT
                , GROSS_AMOUNT
                , TOTAL_DISCOUNT
                , CUSTOMER_TRX_ID
                , REASON_CODE
                , BILL_TYPE_DESC
                , COMPANY_CODE
                , BILLING_TYPE
                , SALES_ORDER_NO
                , ADD1
                , ADD2
                , ADD3
                , BSTNK
                , ITEM_NO
                ,RETURN_REASON_CODE
                FROM PHNX_LOGISTIC_SALES_VIEW
        )
        '''
    cursor.execute(insert_command1)
    cursor.execute(insert_command2)
    cursor.execute(update_statement)
    cursor.execute(insert_command3)

def insertData():
    sql_command = f'''SELECT
        ITEM_CATEGORY
        , ORG_ID as branch_id
        , ORG_DESC as branch_description
        , TRX_DATE as billing_date
        , CAST(
            TRX_NUMBER AS VARCHAR
        )as document_no
        , BOOKER_ID
        , BOOKER_NAME
        , SUPPLIER_ID
        , SUPPLIER_NAME
        , BUSINESS_LINE_ID
        , BUSINESS_LINE as business_line_description
        ,CHANNEL
        , CUSTOMER_ID
        , CUSTOMER_NUMBER
        , CUSTOMER_NAME
        , SALES_ORDER_TYPE
        , INVENTORY_ITEM_ID
        , ITEM_CODE
        , DESCRIPTION as item_description
        ,UNIT_SELLING_PRICE
        , SOLD_QTY
        , BONUS_QTY
        , CLAIMABLE_DISCOUNT
        , UNCLAIMABLE_DISCOUNT
        , TAX_RECOVERABLE
        , NET_AMOUNT
        , GROSS_AMOUNT
        , TOTAL_DISCOUNT
        , CUSTOMER_TRX_ID
        , REASON_CODE
        , BILL_TYPE_DESC as bill_type_description
        , COMPANY_CODE
        , BILLING_TYPE
        , SALES_ORDER_NO
        , ADD1 as address_1
        , ADD2 as address_2
        , ADD3 as address_3
        , BSTNK as salesflo_order_no
        , ITEM_NO
        , RETURN_REASON_CODE
        ,null as company_code_num
    FROM
        etl.PHNX_SALES_DATA_LOG
    WHERE
        1 = 1
    '''
    # vEndDate = datetime.date(
    # datetime.today()-timedelta(days=day_diff)).strftime("%d-%b-%Y")
    # vEndDate = "'"+vEndDate+"'"
    # print(vEndDate)

    df = pd.read_sql(sql_command, conn)
    print('df sales logistic')
    print(df)
    df.columns = df.columns.str.strip()
    df['UNIT_SELLING_PRICE'] = df['UNIT_SELLING_PRICE'].fillna(0)
    df['SOLD_QTY'] = df['SOLD_QTY'].fillna(0)
    df['BONUS_QTY'] = df['BONUS_QTY'].fillna(0)
    df['CLAIMABLE_DISCOUNT'] = df['CLAIMABLE_DISCOUNT'].fillna(0)
    df['UNCLAIMABLE_DISCOUNT'] = df['UNCLAIMABLE_DISCOUNT'].fillna(0)
    df['TAX_RECOVERABLE'] = df['TAX_RECOVERABLE'].fillna(0)
    df['NET_AMOUNT'] = df['NET_AMOUNT'].fillna(0)
    df['GROSS_AMOUNT'] = df['GROSS_AMOUNT'].fillna(0)
    df['TOTAL_DISCOUNT'] = df['TOTAL_DISCOUNT'].fillna(0)
    df['COMPANY_CODE_NUM'] = df['COMPANY_CODE_NUM'].fillna(0)

    # # # # # # # # # # # #
    # print('Postgress Date : ',vEndDate1)
    conn2 = pg.connect(host="35.216.168.189", port='5433',
                    database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    cursor = conn2.cursor()
    delete_sql = f'''DELETE FROM "DW"."logistic_sales"
                    where billing_Date between {vEndDate1} and {vMaxDate1}
                        and company_code in ('6200')  '''
    cursor.execute(delete_sql)
    conn2.commit()

    if len(df) > 0:
            df_columns = list(df)
            columns = ",".join(df_columns)
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    insert_stmt = "INSERT INTO {} ({}) {}".format(
        '"DW"."logistic_sales"', columns, values)
    pg.extras.execute_batch(cursor, insert_stmt, df.values)
    conn2.commit()
    cursor.close()


def Load_Sale_Data_to_gcs():
    conn = pg.connect(host="35.216.168.189", port='5433',
                    database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    file_path = r'/home/admin2/airflow/dag/Google_cloud'
    GCS_PROJECT = 'data-light-house-prod'
    GCS_BUCKET = 'ibloper'
    storage_client = storage.Client.from_service_account_json(
        r'/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json')
    client = storage.Client(project=GCS_PROJECT)
    bucket = client.get_bucket(GCS_BUCKET)
    # print(current_Datetime)
    # print('Logistic Postgres : ',vEndDate1)
    df = pds.read_sql(f'''select item_category,branch_id,branch_description,billing_date,document_no,booker_id,booker_name,supplier_id,supplier_name,business_line_id,business_line_description,channel,customer_id,customer_number,customer_name,sales_order_type,inventory_item_id,item_code,item_description,unit_selling_price,sold_qty,bonus_qty,claimable_discount,unclaimable_discount,tax_recoverable,net_amount,gross_amount,total_discount,reason_code,bill_type_description,company_code,billing_type,sales_order_no,address_1,address_2,address_3,salesflo_order_no,item_no,return_reason_code
            from "DW"."logistic_sales"
            where billing_date between {vEndDate1} and {vMaxDate1} and company_code in ('6200') ''', conn)

    df['transfer_date'] = creationDate
    # filename =f'IBL_SALES_{d1}'
    filename = f'LOGISTIC_SALES_{d1}'
    bucket.blob(f'staging/sales/FiscalYear/2021-22/{filename}.csv').upload_from_string(
        df.to_csv(index=False), 'text/csv')
    conn.close()

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['muhammad.arslan@iblgrp.com'],
	'email_on_failure': True,
        # 'start_date': datetime(2022,2,11)
    'retries': 2,
    'retry_delay': timedelta(minutes=30)

    }

# dag = models.DAG('Insert_Sales_Data', default_args=default_args,schedule_interval='@daily')
with DAG(
    dag_id='SAP_LOGISTIC_SALES',
    # start date:28-03-2017
    start_date= datetime(year=2023, month=4, day=30),
    # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
    #schedule_interval='0/30 01-01 * * *',
    schedule_interval='*/30 0-1 * * *',
    dagrun_timeout=timedelta(minutes=30),
    catchup=False
    ) as dag:

    t1= PythonOperator(
		task_id='delete_data_from_Sap_tables',
        python_callable= deleteData,
        dag=dag,
	)
    t2= PythonOperator(
		task_id='INSERT_SAP_SALES',
        python_callable= insertSapSales,
        dag=dag,
	)
    t3= PythonOperator(
		task_id='INSERT_INTO_POSTGRES',
        python_callable= insertData,
        dag=dag,
	)

    # checkFileExists = PythonOperator(
    #     task_id="checkFile",
    #     python_callable=fileCheck,
    #     dag=dag
    # )

    # writeFile = PythonOperator(
    #     task_id="writeFile",
    #     python_callable=writeFile,
    #     dag=dag
    # )

    checkDataTask = PythonOperator(
        task_id="CheckSapData",
        python_callable=checkSapData,
        dag=dag
    )

    Load_Sale_Data_to_gcs = PythonOperator(
            task_id="Load_Sale_Data_to_gcs",
            python_callable=Load_Sale_Data_to_gcs,
            dag=dag,
    )

    Delete_From_BQ = BigQueryOperator(
    task_id='Delete_From_BQ',
    bigquery_conn_id='bigquery',
    use_legacy_sql=False,
    sql=f'''delete from  `data-light-house-prod.EDW.LOGISTIC_SALES`
                where Billing_date between {vEndDate1} and {vMaxDate1}  and company_code in ('6200')
        ''',
    dag=dag)

    Load_sale_data_gcs_to_bq = GCSToBigQueryOperator(
    task_id='Load_sale_data_gcs_to_bq',
    bucket='ibloper',
    source_objects=f'staging/sales/FiscalYear/2021-22/LOGISTIC_SALES_{d1}.csv',
    # source_objects=f'staging/sales/FiscalYear/2021-22/IBL_SALES_2022-05-10.csv',
    destination_project_dataset_table='data-light-house-prod.EDW.LOGISTIC_SALES',
    schema_fields=[
    # Define schema as per the csv placed in google cloud storage
    {'name': 'item_category', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'branch_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'branch_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'billing_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'document_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'booker_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'booker_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'supplier_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'supplier_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'business_line_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'business_line_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'channel', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'customer_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'customer_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'customer_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sales_order_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'inventory_item_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'item_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'item_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'unit_selling_price', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'sold_qty', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'bonus_qty', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'claimable_discount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'unclaimable_discount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'tax_recoverable', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'net_amount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'gross_amount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'total_discount', 'type': 'Numeric', 'mode': 'NULLABLE'},
    {'name': 'reason_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'bill_type_description', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'company_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'billing_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sales_order_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'address_1', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'address_2', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'address_3', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'salesflo_order_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'item_no', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'return_reason_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'transfer_date', 'type': 'TIMESTAMP'}
    ],
    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=dag
    )

    # t1>>t2>>t3>>t4>>Load_Sale_Data_to_gcs>>Delete_From_BQ>>Load_sale_data_gcs_to_bq
checkDataTask >>t1 >> t2 >> t3  >> Load_Sale_Data_to_gcs >> Delete_From_BQ >> Load_sale_data_gcs_to_bq

