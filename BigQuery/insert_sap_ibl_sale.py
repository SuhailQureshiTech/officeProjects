
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
from datetime import date, datetime, timedelta
import psycopg2 as pg
import pandas as pds
import numpy as np
#
from datetime import date, datetime, timedelta
# from hmac import trans_36
# from pickle import NONE
import time
from datetime import datetime
from time import time, sleep
from fileinput import filename
# import imp
import pysftp
from datetime import date
import pypyodbc as odbc
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
# from datalab.context import Context
from datetime import timedelta, date
import csv
from datetime import date
from google.cloud import storage
import sqlalchemy

today = date.today()
global day_diff
day_diff =24
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pd.DateOffset(days=day_diff)
d1 = past_date.strftime("%Y-%m-%d")
df = pd.DataFrame()
# verify the architecture of Python
# print("Platform architecture: " + platform.architecture()[0])
global vEndDate,vmaxDate
today = date.today()
vEndDate = datetime.date(
    datetime.today()-timedelta(days=day_diff)).strftime("%Y%m%d")
vEndDate = "'"+vEndDate+"'"

vmaxDate = datetime.date(
    datetime.today()-timedelta(days=1)).strftime("%Y%m%d")
vmaxDate = "'"+vmaxDate+"'"

print('end date: ', vEndDate)
print('max date : ',vmaxDate)

# Initialize your connection

utc = timezone.utc
date = datetime.now(utc)
print('utc Time : ', date)
# 2022-04-06 05:40:13.025347+00:00
creationDate = date + timedelta(hours=5)
print('creation date :',creationDate)

storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024
storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024

def BigQueryCount():
    credentials = service_account.Credentials.from_service_account_file(
        'd://data-light-house-prod-0baa98f57152 (1).json')

    project_id = 'data-light-house-prod'
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'
    # pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)
    recCount = f''' select count(*)rec_count from data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP
    where billing_date={vmaxDate} '''

    query_job = client.query(recCount)  # API request
    df=query_job.result().to_dataframe()

    # print(df['rec_count'])
    v=df[0]
    print('v : ',v)

    # rows_count  = df.shape[0]
    # print('Number of Rows count is:', rows_count)

# insert sap sale bigquery
def insertSapSaleBigQuery(df):
    credentials = service_account.Credentials.from_service_account_file(
        'd://temp//data-light-house-prod-0baa98f57152.json'
        )

    df = df.replace(r'^\s+$', np.nan, regex=True)
    # df_stock = pds.DataFrame(data=df)
    # df_stock.to_csv('d:\\TEMP\\mango.csv')

    project_id = 'data-light-house-prod'
    pandas_gbq.context.credentials = credentials

    client = bigquery.Client(credentials=credentials, project=project_id)
    table_id = 'data-light-house-prod.EDW.IBL_SALES_DATA_BAKUP'

    print('inserting sale big query')
    pandas_gbq.to_gbq(df, table_id, project_id=project_id,if_exists='append', table_schema=table_schema)

def insertSapSale():
    global vEndDate,vmaxDate

    # Initialize your connection
    utc = timezone.utc
    creationDate = date + timedelta(hours=5)
    print('creation date :',creationDate)

    conn = dbapi.connect(
        # Option2, specify the connection parameters
        address='10.210.134.204',
        port='33015',
        user='Etl',
        password='EtlIbl12345'
    )

    cursor = conn.cursor()
    del_command = f'''DELETE FROM PHNX_SALES_DETAIL_DATA '''
    del_command1 = f'''DELETE FROM PHNX_SALES_DATA '''
    del_command2 = f'''DELETE FROM phnx_sales_data_tmp_konv '''

    print('connected')
    def deleteData():
        print('deleting block')
        cursor.execute(del_command)
        cursor.execute(del_command1)
        cursor.execute(del_command2)

    deleteData()
    insert_command1 = f'''
    INSERT INTO PHNX_SALES_DETAIL_DATA
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
            and "FKDAT" between '20230801' and '20230814'
                    and "VBRK"."VKORG" in ('6300', '6100') and "VBRK"."FKART" in ( 'ZOPC', 'ZOCC', 'ZUBC', 'ZUCC', 'ZORE', 'ZORC', 'ZURB', 'ZUB1', 'ZNES', 'ZNEC')
                --	)
                '''
    insert_command2 = f'''
        INSERT INTO phnx_sales_data_tmp_konv
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
            where KONV1."KINAK"<>'Y' and  (KONV1."KNUMV", KONV1."KPOSN")  in (select KNUMV ,POSNR  from PHNX_SALES_DETAIL_DATA)
            group by  KONV1."KNUMV",KONV1."KPOSN" ,KONV1."KSCHL"
        ) A
        group by KNUMV,KPOSN
        )
        '''
    update_statement = f'''
        UPDATE PHNX_SALES_DETAIL_DATA psl
        set (CLAIMABLE_DISCOUNT,UNCLAIMABLE_DISCOUNT,TAX_RECOVERABLE,UNIT_SELLING_PRICE,GROSS_AMOUNT) =
        (
        select  CLAIMABLE_DISCOUNT,UNCLAIMABLE_DISCOUNT,TAX_RECOVERABLE,UNIT_SELLING_PRICE,
        (case
                when psl.BILLING_TYPE = 'ZURB' or psl.BILLING_TYPE = 'ZUCC' then (t.UNIT_SELLING_PRICE*psl.QUANTITY)*-1
                else (t.UNIT_SELLING_PRICE*psl.QUANTITY)
            end )GROSS_AMOUNT
        from phnx_sales_data_tmp_konv t
        where t.up_key= psl.UP_KEY)
            '''
    insert_command3 = f'''
        INSERT INTO PHNX_SALES_DATA(ITEM_CATEGORY
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
                FROM PHNX_SALES_VIEW
        )
        '''

    cursor.execute(insert_command1)
    cursor.execute(insert_command2)
    cursor.execute(update_statement)
    cursor.execute(insert_command3)
    print('insert sap ')

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
        --, CUSTOMER_TRX_ID
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
    -- ,null as company_code_num
    FROM
        etl.PHNX_SALES_DATA
    WHERE
        1 = 1
    '''

    vEndDate = datetime.date(
        datetime.today()-timedelta(days=day_diff)).strftime("%d-%b-%Y")
    vEndDate = "'"+vEndDate+"'"
    print(vEndDate)
    df = pd.read_sql(sql_command, conn)
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
    # df['COMPANY_CODE_NUM'] = df['COMPANY_CODE_NUM'].fillna(0)
    df['transfer_date'] = creationDate


    def upload_to_bucket():
        global vEndDate, vmaxDate
        #
        print('uloading to gcs')
        print(df.info())
        vEndDate = datetime.date(
            datetime.today()-timedelta(days=day_diff)).strftime("%Y-%m-%d")

        vEndDate = "'"+vEndDate+"'"

        GCS_PROJECT = 'data-light-house-prod'
        GCS_BUCKET = 'ibloper'
        filename = f'IBL_SALES'
        credentials = service_account.Credentials.from_service_account_file(
            'd://temp//data-light-house-prod-0baa98f57152.json'
            )
        project_id = 'data-light-house-prod'

        Bqclient = bigquery.Client(credentials=credentials, project=project_id)
        client=storage.Client(credentials=credentials,project=project_id)

        dml_statement = (f'''
                DELETE FROM  `data-light-house-prod.EDW.IBL_SALES`
                    where billing_date between '2023-08-01' and '2023-08-14'
                '''
                        )

        query_job =Bqclient.query(dml_statement)
        # query_job.result()

        bucket = client.get_bucket('ibloper')
        blob = bucket.blob(f'staging/temp/{filename}.csv')
        blob.upload_from_string(df.to_csv(index=False), 'text/csv')

    upload_to_bucket()
    # delPostGresData(df)

def loadGCSToBQ():

    credentials = service_account.Credentials.from_service_account_file(
                    'd://temp//data-light-house-prod-0baa98f57152.json'
            )
    project_id = 'data-light-house-prod'

    client=bigquery.Client(credentials=credentials,project=project_id)
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("item_category", "STRING"),
        bigquery.SchemaField("branch_id", "STRING"),
        bigquery.SchemaField("branch_description", "STRING"),
        bigquery.SchemaField("billing_date", "DATE"),
        bigquery.SchemaField("document_no", "STRING"),
        bigquery.SchemaField("booker_id", "STRING"),
        bigquery.SchemaField("booker_name", "STRING"),
        bigquery.SchemaField("supplier_id", "STRING"),
        bigquery.SchemaField("supplier_name", "STRING"),
        bigquery.SchemaField("business_line_id", "STRING"),
#
        bigquery.SchemaField("business_line_description", "STRING"),
        bigquery.SchemaField("channel", "STRING"),
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("customer_number", "STRING"),
        bigquery.SchemaField("customer_name", "STRING"),
        bigquery.SchemaField("sales_order_type", "STRING"),
#
        bigquery.SchemaField("inventory_item_id", "STRING"),
        bigquery.SchemaField("item_code", "STRING"),
        bigquery.SchemaField("item_description", "STRING"),
        bigquery.SchemaField("unit_selling_price", "Numeric"),
        bigquery.SchemaField("sold_qty", "Numeric"),
        bigquery.SchemaField("bonus_qty", "Numeric"),
        bigquery.SchemaField("claimable_discount", "Numeric"),
        bigquery.SchemaField("unclaimable_discount", "Numeric"),
        bigquery.SchemaField("tax_recoverable", "Numeric"),

        bigquery.SchemaField("net_amount", "Numeric"),
        bigquery.SchemaField("gross_amount", "Numeric"),
        bigquery.SchemaField("total_discount", "Numeric"),
        # bigquery.SchemaField("customer_trx_id", "STRING"),

        bigquery.SchemaField("reason_code", "STRING"),
        bigquery.SchemaField("bill_type_description", "STRING"),
        bigquery.SchemaField("company_code", "STRING"),
        bigquery.SchemaField("billing_type", "STRING"),
        bigquery.SchemaField("sales_order_no", "STRING"),
        bigquery.SchemaField("address_1", "STRING"),
        bigquery.SchemaField("address_2", "STRING"),
        bigquery.SchemaField("address_3", "STRING"),
        bigquery.SchemaField("salesflo_order_no", "STRING"),
        bigquery.SchemaField("item_no", "STRING"),
        bigquery.SchemaField("return_reason_code", "STRING"),
        # bigquery.SchemaField("company_code_num", "STRING"),
        bigquery.SchemaField("transfer_date", "TIMESTAMP")
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
    )

    table_id = "data-light-house-prod.EDW.IBL_SALES"
    uri = "gs://ibloper/staging/temp/IBL_SALES.csv"

    load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
    )


    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)


    print("Loaded {} rows.".format(destination_table.num_rows))
    return f'check the results in the logs'

def test():
    global vEndDate, vmaxDate,day_diff
    print('day diff  ,',day_diff)
        #
    vEndDate = datetime.date(
        datetime.today()-timedelta(days=day_diff)).strftime("%Y-%m-%d")
    vEndDate = "'"+vEndDate+"'"
    print('test...',vEndDate)


insertSapSale()
loadGCSToBQ()


