
# from airflow.contrib.operators import gcs_to_bq
# from airflow.exceptions import AirflowFailException
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryCreateEmptyDatasetOperator,
#     BigQueryDeleteDatasetOperator,
# )
# # from airflow.contrib.sensors.file_sensor import FileSensor
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryCreateEmptyDatasetOperator,
#     BigQueryDeleteDatasetOperator,
# )
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator


from airflow import DAG
from airflow import models
import airflow.operators.dummy
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from datetime import date, datetime, timedelta
from hmac import trans_36
from pickle import NONE
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

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
# from sqlalchemy import create_engine
import pandas_gbq
from IPython.display import display
import pysftp
import csv
from datetime import date
import pandas as pd
import calendar

import connectionClass
connection=connectionClass
# sapConnection=connection.sapConn()
sapEngine=connection.sapSandBox2ConnAlchemy()

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/admin2/airflow/dags/Google_cloud/data-light-house-prod-0baa98f57152.json"
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

# conn = dbapi.connect(
    # Option 1, retrieve the connection parameters from the hdbuserstore
    # key='USER1UserKey', # address, port, user and password are retrieved from the hdbuserstore

    # Option2, specify the connection parameters
    # address='10.210.134.204',
    # port='33015',
    # user='Etl',
    # password='Etl@2025'

    # Additional parameters
    # encrypt=True, # must be set to True when connecting to HANA as a Service
    # As of SAP HANA Client 2.6, connections on port 443 enable encryption by default (HANA Cloud)
    # sslValidateCertificate=False #Must be set to false when connecting
    # to an SAP HANA, express edition instance that uses a self-signed certificate.
# )

# If no errors, print connected
# print('connected')
# params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
#                                 "SERVER=192.168.130.81\sqldw;"
#                                 "DATABASE=ibl_dw;"
#                                 "UID=pbironew;"
#                                 "PWD=pbiro345-")

# cursor = conn.cursor()

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
    sapEngine.execute(del_command)
    sapEngine.execute(del_command1)
    sapEngine.execute(del_command2)


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
            and "FKDAT" between   '20230301' and '20230305'
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
            from SAPABAP1."PRCD_ELEMENTS" KONV1
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
    sapEngine.execute(insert_command1)
    sapEngine.execute(insert_command2)
    sapEngine.execute(update_statement)
    sapEngine.execute(insert_command3)


deleteData()
insertSapSales()
