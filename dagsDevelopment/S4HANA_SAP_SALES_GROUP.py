
# from airflow import DAG
# from airflow import models
# import airflow.operators.dummy
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.python import PythonOperator
# from airflow.contrib.operators import gcs_to_bq
# from airflow.exceptions import AirflowFailException
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryCreateEmptyDatasetOperator,
#     BigQueryDeleteDatasetOperator,
# )
# #from airflow.contrib.sensors.file_sensor import FileSensor
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryCreateEmptyDatasetOperator,
#     BigQueryDeleteDatasetOperator,
# )
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from airflow.contrib.operators.bigquery_operator import BigQueryOperator


from datetime import date, datetime,timedelta
from hmac import trans_36
from pickle import NONE

import time
from datetime import datetime
from time import time, sleep
from fileinput import filename
# import imp
import  pysftp
from datetime import date
import pandas as pd
import glob
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
# sys.path.append('/home/airflow/airflow/dags')
from http import client
import pandas as pds
import numpy as np
from datetime import timedelta, date
from google.cloud import storage
import os
from google.oauth2 import service_account


from google.cloud import bigquery
from google.cloud.exceptions import NotFound
# from sqlalchemy import create_engine
import pandas_gbq
import pysftp
import csv
from datetime import date
import pandas as pd
import calendar
from io import StringIO
import sqlalchemy
import connectionClass
connection=connectionClass
# sapConnection=connection.sapConn()
sapEngine=connection.sapSandBox2ConnAlchemy()

storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024* 1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024* 1024  # 5 MB

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/data-light-house-prod.json"

today = date.today()
day_diff=54

# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=day_diff)
d1 = past_date.strftime("%Y-%m-%d")
# global df
df = pd.DataFrame()
df1=pd.DataFrame()
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


# bigQueryTable ='data-light-house-prod.EDW.IBL_GROUP_SALE'

# Initialize your connection

utc = timezone.utc
# date = datetime.now(utc)

dateNow=datetime.now()
# print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
creationDate =dateNow

# conn=sapConnection
# cursor = conn.cursor()

del_command = f'''DELETE FROM GROUP_SALES_DETAIL_DATA '''
del_command1 = f'''DELETE FROM GROUP_SALES_DATA_TMP_KONV '''

# default_args = {
#     'owner': 'admin',
#     'depends_on_past': False,
#     'email': ['muhammad.arslan@iblgrp.com'],
#     'email_on_failure': True,
#     # 'start_date': datetime(2022,2,11)
#     'retries': 2,
#     'retry_delay': timedelta(minutes=10),
#     'gcp_conn_id': 'google_cloud_default'
# }

# sap_sale_merging = DAG(
#     dag_id='Sap_Group_Sales',
#     # start date:28-03-2017
#     start_date=datetime(year=2023, month=8, day=10),
#     # run this dag at 2 hours 30 min interval from 00:00 28-03-2017
#     catchup=False,
#     # schedule_interval=None,
#     schedule_interval='*/30 3-4 * * *',
#     dagrun_timeout=timedelta(minutes=120)
# )

def deleteData():
    sapEngine.execute(del_command)
    sapEngine.execute(del_command1)
    

def insertSapSales():

    # print('today :',today)
    # print('end date data : ', vEndDate)
    # print('vmaxDate  :   ', vMaxDate)
    # print('vendate1  :   ', vEndDate1)
    # print('vmaxDate1  :   ', vMaxDate1)

    insert_command1 = f'''INSERT INTO GROUP_SALES_DETAIL_DATA
    SELECT
            distinct
            "VBRP"."PSTYV" as ITEM_CATEGORY,
            "VBRK"."FKART" as BILLING_TYPE,
            "VBRK"."KNUMV" ||'-' ||"VBRP"."POSNR"  UP_KEY,
        --	concat("VBRK"."KNUMV", '-', "VBRP"."POSNR") UP_KEY,
            "VBRK"."VBELN" as DOCUMENT_NO,
            (	-- "TVFKT"."VTEXT" BILLING_TYPE_TEXT,
        case
                when "VBRK"."FKART" IN ( 'ZSS1','ZSS2','ZHS1','ZHS2','ZSS1','ZSS2'  ) then 'Cancelled'
                when  (UPPER("TVFKT"."VTEXT")  LIKE  '%RETURN%'     or UPPER("TVFKT"."VTEXT")  LIKE  '%.RET.%') then 'Return'
            end) as CANCELLED_FLAG,
            "VBRK"."VKORG" as COMPANY_CODE,
            "VBRK"."KNUMV" as DOCUMENT_CONDITION,
            cast("FKDAT" as date) as BILLING_DATE,
            "KDGRP" as CHANNEL,   --vbrk.kdgrp
            "KUNAG" as CUSTOMER_CODE,
            "FKSTO" as BILLING_CANCELLED,
            "ZTERM" as PAYMENT_TYPE,
            "VBRP"."POSNR" as ITEM_NO,
            (case
                WHEN UPPER("TVFKT"."VTEXT")  LIKE  '%RETURN%'     or UPPER("TVFKT"."VTEXT")  LIKE  '%.RET.%'
                OR UPPER("TVFKT"."VTEXT")  LIKE  '%CANCEL%'     or UPPER("TVFKT"."VTEXT")  LIKE  '%CANCEL%'
                then "VBRP"."FKIMG" *-1
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
            CASE WHEN "VBAK"."AUGRU"='' THEN VBAP.ABGRU ELSE "VBAK"."AUGRU" END AUGRU,
            "VBRK"."VTWEG",--DIST_CHANNEL,
            TVTWT.VTEXT  ---DIST_CHNL_DESC
            ---"VBRP"."Loading_Date" loading_date
    FROM SAPABAP1.VBRK
 inner  join SAPABAP1.VBRP on ("VBRP"."VBELN" = "VBRK"."VBELN")
 inner  join SAPABAP1.VBPA VBP1 on (vbp1 ."VBELN" = "VBRP"."VBELN"  and vbp1 ."PARVW" IN  ('AG','WE','RE','RG') )
 inner  join SAPABAP1.VBPA on ("VBPA"."VBELN" = "VBRP"."VBELN" and "VBPA"."PARVW" IN  ('AG','WE','RE','RG'))
 inner join SAPABAP1.VBAK on ("VBAK"."VBELN" = "VBRP"."AUBEL")
  INNER JOIN SAPABAP1.VBAP ON (VBRP.AUBEL = VBAP.VBELN and VBRP.AUPOS = VBAP.POSNR)
  left outer join SAPABAP1.TVFKT on ("VBRK"."FKART" = "TVFKT"."FKART" and "TVFKT"."SPRAS"='E')
    left outer join SAPABAP1.TVAPT on ("VBRP"."PSTYV" = "TVAPT"."PSTYV" and "TVAPT"."SPRAS" ='E' )
	--CHANNEL DIST TEXT
	LEFT OUTER JOIN SAPABAP1.TVTWT TVTWT ON (TVTWT.MANDT=300 AND TVTWT.SPRAS='E' AND TVTWT.VTWEG="VBRK"."VTWEG")
    where
            1 = 1
            and "FKDAT"   between   '20230701' and '20230731'
                    and "VBRK"."VKORG" in ('1000','1200', '1900')
                    and "VBRK"."FKART" in ('ZSLP','ZSRP','ZSRC','ZSRI','ZSLC','ZSFE','ZSLE','ZSS1','ZSLI','ZSLT','ZSRE','ZSS2',
					'ZSLW','ZHLS','ZHRE','ZHS1','ZHS2','YOER','YOLR','YOLP','YOTR','YOEP','YOTL','ZSS1','ZSS2' )
                '''

    insert_command2 = f'''INSERT INTO GROUP_SALES_DATA_TMP_KONV
            (
            select "KNUMV"||'-'||"KPOSN" up_key,KNUMV,KPOSN,sum(CLAIMABLE_DISCOUNT) CLAIMABLE_DISCOUNT,sum(UNCLAIMABLE_DISCOUNT) UNCLAIMABLE_DISCOUNT,sum(TAX_RECOVERABLE) TAX_RECOVERABLE,sum(UNIT_SELLING_PRICE) UNIT_SELLING_PRICE
        from (
        SELECT
            distinct
            KONV1."KNUMV" as KNUMV,
            KONV1."KPOSN" as KPOSN,
            (case when KONV1."KSCHL" ='ZDSV' or KONV1."KSCHL" ='ZFOC'  then  Sum(KONV1."KWERT")*-1 end ) as CLAIMABLE_DISCOUNT,
            0  as UNCLAIMABLE_DISCOUNT,
            --(case when KONV1."KSCHL" ='ZUDP' or KONV1."KSCHL" ='ZUDV' or KONV1."KSCHL" ='ZUP'  then  Sum(KONV1."KWERT")*-1 end ) as UNCLAIMABLE_DISCOUNT,
            (case when KONV1."KSCHL" ='ZMWS' or KONV1."KSCHL" ='ZFTX' or KONV1."KSCHL" ='ZADV' or KONV1."KSCHL" ='ZMRT' or KONV1."KSCHL" ='ZEXT'  or KONV1."KSCHL" ='ZSRG' then  Sum(KONV1."KWERT") end ) as TAX_RECOVERABLE,
            (case when KONV1."KSCHL" ='ZEXP' then sum(KONV1."KBETR") end) as UNIT_SELLING_PRICE
            from SAPABAP1."PRCD_ELEMENTS" KONV1
            where KONV1."KINAK"<>'Y' and  (KONV1."KNUMV", KONV1."KPOSN")  in (select KNUMV ,POSNR  from GROUP_SALES_DETAIL_DATA )
            group by  KONV1."KNUMV",KONV1."KPOSN" ,KONV1."KSCHL"
        ) A
        group by KNUMV,KPOSN
        )
        '''
    update_statement = f'''UPDATE GROUP_SALES_DETAIL_DATA  psl
        set (CLAIMABLE_DISCOUNT,UNCLAIMABLE_DISCOUNT,TAX_RECOVERABLE,UNIT_SELLING_PRICE,GROSS_AMOUNT) =
        (
        select  CLAIMABLE_DISCOUNT,UNCLAIMABLE_DISCOUNT,TAX_RECOVERABLE,UNIT_SELLING_PRICE,
        (case   when psl.BILLING_TYPE = 'ZURB' or psl.BILLING_TYPE = 'ZUCC' then (t.UNIT_SELLING_PRICE*psl.QUANTITY)*-1
                else (t.UNIT_SELLING_PRICE*psl.QUANTITY)
            end )GROSS_AMOUNT
        FROM  GROUP_SALES_DATA_TMP_KONV  t
        where t.up_key= psl.UP_KEY)
            '''
    
    sapEngine.execute(insert_command1)
    sapEngine.execute(insert_command2)
    sapEngine.execute(update_statement)
 

deleteData()
insertSapSales()

