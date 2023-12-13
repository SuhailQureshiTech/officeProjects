
# import 
    # Google
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from google.oauth2.service_account import Credentials
    # AirFlow
import urllib
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.email_operator import EmailOperator
from airflow.operators.email import EmailOperator
# from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.python_virtualenv import prepare_virtualenv, write_python_script
from airflow.utils.dates import days_ago
from airflow import models
from airflow.exceptions import AirflowException
from airflow.utils.state import State
# from airflow.contrib.operators import gcs_to_bq
import airflow.operators
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,BigQueryExecuteQueryOperator
)

# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.email import send_email_smtp

from fileinput import filename
# import imp
import psycopg2 as pg

from dateutil import parser
from datetime import datetime, date
import sys
from http import client
import pandas as pds
import io
import numpy as np
import os
import sqlalchemy

# from sqlalchemy.dialects.postgresql import(
#     VARCHAR,INT4RANGE,DATE
# )

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
import pandas_gbq
from datetime import date, datetime, timedelta

# from airflow.contrib.operators import gcs_to_bq

import numpy as np
from regex import F
from suhailLib import returnDataDate

    # Connections
import connectionClass    

# from connectionClass import (pioneerSqlAlchmy,attendanceMachine66)

# import -- >> End
connection=connectionClass
attendance66=connection.attendanceMachine66()
poineerSqlAlchemy=connection.attendanceSqlAlchmy()
sapAlchemy=connection.sapConnAlchemy()


# franchiseEngine=connection.FranchiseAlchmy()
# sapConn=connection.sapConn()



# franchiseDf=pd.DataFrame()

spec_chars=connectionClass.getSpecChars()
global fileName

filePath='/home/airflow/Documents/franchiseDataFiles/'
fileName = 'FranchiseSales.csv'

GCS_PROJECT = 'data-light-house-prod'
DATA_SET_ID='EDW'
# tableId='data-light-house-prod.EDW.FRANCHISE_SALES_NEW'
tableId='FRANCHISE_SALES_NEW'
fran_sale_df=pd.DataFrame()

# storageClient = storage.Client.from_service_account_json(
#     r'/home/airflow/airflow/data-light-house-prod.json')


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/data-light-house-prod.json"
credential_file="/home/airflow/airflow/data-light-house-prod.json"
credentials=Credentials.from_service_account_file(credential_file)

bigQueryClient = bigquery.Client()

storage.blob._DEFAULT_CHUNKSIZE = 5*1024*1024  # 5 MB
storage.blob._MAX_MULTIPART_SIZE = 5*1024*1024 # 5 MB

vStartDate = None
vEndDate = None

vStartDate,vEndDate=returnDataDate()
creationDate = datetime.today()
now = datetime.now()
current_time = now.time()


vStartDate="'"+str(vStartDate)+"'"
vEndDate="'"+str(vEndDate)+"'"

print('else : from date :', vStartDate)
print('else : enmd date :', vEndDate)


# def success_function(context):
#     #dag_run = context.get('dag_run')
#     msg = "Stock DAG has executed successfully."
#     subject = f"Stock DAG has completed"
#     send_email_smtp(to=['shehzad.lalani@iblgrp.com'],
#                     subject=subject, html_content=msg)


default_args = {
    'owner': 'SuhailQureshi',
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
    dag_id='attendanceSynch',
    default_args=default_args,
    catchup=False,
    start_date=datetime(2023, 11, 20),
    schedule_interval='00 04 * * *',
    # schedule_interval=None,
    # on_success_callback=success_function,
    # email_on_failure=failure_email_function,
    dagrun_timeout=timedelta(minutes=120),
    description='Attendance Synchronization'
)

def getAttendance():

    print('vstart date ',vStartDate)
    print('vEnd Date ', vEndDate)

    sqlGetRec=f'''
            SELECT 
                300 mandt,Tran_MachineRawPunchId transaction_id,CardNo employee_id
                ,Dateime1  punch_datetime,machineno device_no,'01' status
                ,P_Day,ISManual            
            from Tran_MachineRawPunch trn
            where 1=1 and cast(PunchDatetime as date)<='2023-12-13'   
            
            '''
    franchiseDf=pd.read_sql(sqlGetRec,con=attendance66)
    franchiseDf['punch_datetime']=pd.to_datetime(franchiseDf['punch_datetime']) 
    franchiseDf['record_datetime']=datetime.now()

    # dataframe type conversion
    # convert_dict = {
    #                 'transaction_id':'string'
    #                 ,'employee_id': 'string'
    #                 ,'device_no': 'string'
    #                 ,'status': 'string'
    #                 ,'P_Day':'string'
    #                 ,'ISManual': 'string'
    #             }
    # franchiseDf=franchiseDf.astype(convert_dict)


    # VARCHAR,INT4RANGE,DATE

    from sqlalchemy.types import VARCHAR, INTEGER, FLOAT,Date
    postgressql_dtypes={
        'mandt'                 :   INTEGER
        ,'transaction_id'       :   VARCHAR
        ,'employee_id'          :   VARCHAR
        ,'device_no'            :   VARCHAR
        ,'status'               :   VARCHAR
        ,'P_Day'                :   VARCHAR
        ,'ISManual'             :   VARCHAR 
    }

    print(franchiseDf.info())

    franchiseDf.to_sql(
         'pioneer_attendance'   
        ,schema='attendance'
        ,con=poineerSqlAlchemy
        ,index=False
        ,if_exists='append'
        ,dtype=postgressql_dtypes
    )

def delteSapAttendanceRecords():    
    delQuery='truncate table sapabap1.ztmpor'
    sapAlchemy.execute(delQuery)

def insertAttendanceIntoSap():
    sqlGetRec=f'''
       select 300 mandt,tmid,cardno ,date1,p_day,ismanual,time,inout1,flag
         from attendance.vw_attendance_inout_rec  
    '''

    dfRec=pd.read_sql(sqlGetRec,con=poineerSqlAlchemy) 

    print(dfRec.info())
    print(dfRec)

    dfRec.to_sql('ztmpor'
                 ,schema='SAPABAP1'
                 ,con=sapAlchemy
                 ,index=False
                 ,if_exists='append'                 
                 )



# getAttendance()
# delteSapAttendanceRecords()
insertAttendanceIntoSap()

# def deleteRecords():

#     print('vstart date ',vStartDate)
#     print('vEnd Date ', vEndDate)

#     delQuery=f'''delete from data-light-house-prod.EDW.FRANCHISE_SALES_NEW
#                     where invoice_date  between {vStartDate} and {vEndDate}
#       '''
#     job=bigQueryClient.query(delQuery)
#     job.result()

# def getFranchiseDataParqeet():
#     # global
#     print('vstart date ',vStartDate)
#     print('vEnd Date ', vEndDate)
#     sqlData=f'''select
#                 '6300' company_code,
#                 ibl_distributor_code,
#                 ibl_distributor_desc,
#                 branch_code,
#                 distributor_location_id,
#                 distributor_location_desc,
#                 order_no,
#                 invoice_number,
#                 invoice_date,
#                 channel,
#                 distributor_customer_code,
#                 ibl_customer_code,
#                 ibl_customer_name,
#                 distributor_item_code,
#                 ibl_item_code,
#                 ibl_item_description,
#                 sold_qty,
#                 gross_amount,
#                 bonus_qty,
#                 discount,
#                 reason,
#                 address,
#                 cast(to_char(record_date,'yyyymmdd')  as numeric) as record_date,
#                 brick_code,brick_name
#             from franchise.franchise_data fd     
#             where 1=1 and invoice_date between {vStartDate} and {vEndDate}
#             '''

#     # dataFile=f'''{filePath}franchiseData.parquet'''

#     franchiseDf=pd.read_sql(sqlData,con=franchiseEngine)
#     franchiseDf['invoice_date']=pd.to_datetime(franchiseDf['invoice_date']) 
#     print(franchiseDf)

#     conn1=sapConn
#     cus_df=pds.read_sql(f'''
#                         SELECT distinct  KUNNR as "SAP_CUSTOMER_CODE"
#                         ,ADRNR
#                         ,A.STR_SUPPL1 add1,
#                         A.STR_SUPPL2 add2,A.STR_SUPPL3 add3
#                         FROM SAPABAP1.KNA1 AS B
#                         LEFT OUTER JOIN SAPABAP1.ADRC AS A ON (A.CLIENT=B.MANDT AND A.ADDRNUMBER=B.ADRNR)
#                         WHERE 1=1 AND MANDT=300
#                         ''',conn1)

#     branch_df=pds.read_sql(f'''
#                         SELECT distinct  VKBUR "sap_branch_code",BEZEI "branch_desc"
#                         FROM SAPABAP1.TVKBT BRANCH WHERE MANDT=300 AND SPRAS ='E'
#                         ''',conn1)

#     fran_sale_df = franchiseDf.merge(
#         cus_df, how='left', left_on=['ibl_customer_code'], right_on=['SAP_CUSTOMER_CODE'])

#     fran_sale_df = fran_sale_df.merge(
#         branch_df, how='left', left_on=['branch_code'], right_on=['sap_branch_code'])

#     fran_sale_df['ref_customer_code'] = np.where(fran_sale_df['SAP_CUSTOMER_CODE'].isnull(), franchiseDf['ibl_distributor_code'].astype(
#         str)+'-'+franchiseDf['ibl_customer_code'].astype(str)
#                 , franchiseDf['ibl_customer_code']
#                     )

#     fran_sale_df.drop(['SAP_CUSTOMER_CODE', 'ADRNR','sap_branch_code'], inplace=True, axis=1)
#     dataFile=f'''{filePath}franchiseData.parquet'''
    
#     fran_sale_df['transfer_date'] = datetime.now()
#     column_name = ["company_code",
#                     "ibl_distributor_code",
#                     "ibl_distributor_desc",
#                     "branch_code",
#                     "branch_desc",
#                     "distributor_location_id",
#                     "distributor_location_desc",
#                     "order_no",
#                     "invoice_number",
#                     "invoice_date",
#                     "channel",
#                     "distributor_customer_code",
#                     "ibl_customer_code",
#                     "ref_customer_code",
#                     "ibl_customer_name",
#                     "brick_code",
#                     "brick_name",
#                     "ADD1",
#                     "ADD2",
#                     "ADD3",
#                     "distributor_item_code",
#                     "ibl_item_code",
#                     "ibl_item_description",
#                     "sold_qty",
#                     "gross_amount",
#                     "bonus_qty",
#                     "discount",
#                     "reason",
#                     "data_loading_date",
#                     "transfer_date",
#                     "record_date",
#                     "address"
#                 ]
#     fran_sale_df=fran_sale_df.reindex(columns=column_name)

#     fran_sale_df.to_parquet(dataFile,index=False)
#     df1=pd.read_parquet(dataFile)
#     print(df1.info())
    
#     project_id = 'data-light-house-prod'
#     client=bigquery.Client(credentials=credentials,project=project_id)
#     job_config = bigquery.LoadJobConfig(
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#         source_format=bigquery.SourceFormat.PARQUET,
#     )    

#     # filePath = dataFile
#     with open(dataFile,"rb") as source_file:
#         load_job = client.load_table_from_file(
#             source_file, tableId, job_config=job_config
#         )  

#         load_job.result()  # Waits for the job to complete.    

#     # df to table
#     # pandas_gbq.to_gbq(franchiseDf, f'{GCS_PROJECT}.{DATA_SET_ID}.{tableId}', project_id=GCS_PROJECT, if_exists='replace')

#     print('done.....................')

# def getFranchiseDataDfSql():
#     # global
#     sqlData=f'''select
#                 '6300' company_code,
#                 ibl_distributor_code,
#                 ibl_distributor_desc,
#                 branch_code,
#                 distributor_location_id,
#                 distributor_location_desc,
#                 order_no,
#                 invoice_number,
#                 invoice_date,
#                 channel,
#                 distributor_customer_code,
#                 ibl_customer_code,
#                 ibl_customer_name,
#                 distributor_item_code,
#                 ibl_item_code,
#                 ibl_item_description,
#                 sold_qty,
#                 gross_amount,
#                 bonus_qty,
#                 discount,
#                 reason,
#                 address,
#                 cast(to_char(record_date,'yyyymmdd')  as numeric) as record_date,
#                 brick_code,brick_name
#             from franchise.franchise_data fd     
#             where 1=1 and invoice_date between {vStartDate} and {vEndDate}
#             '''

#     franchiseDf=pd.read_sql(sqlData,con=franchiseEngine)
#     franchiseDf['invoice_date']=pd.to_datetime(franchiseDf['invoice_date']) 
#     print(franchiseDf)

#     conn1=sapConn
#     cus_df=pds.read_sql(f'''
#                         SELECT distinct  KUNNR as "SAP_CUSTOMER_CODE"
#                         ,ADRNR
#                         ,A.STR_SUPPL1 add1,
#                         A.STR_SUPPL2 add2,A.STR_SUPPL3 add3
#                         FROM SAPABAP1.KNA1 AS B
#                         LEFT OUTER JOIN SAPABAP1.ADRC AS A ON (A.CLIENT=B.MANDT AND A.ADDRNUMBER=B.ADRNR)
#                         WHERE 1=1 AND MANDT=300
#                         ''',conn1)

#     branch_df=pds.read_sql(f'''
#                         SELECT distinct  VKBUR "sap_branch_code",BEZEI "branch_desc"
#                         FROM SAPABAP1.TVKBT BRANCH WHERE MANDT=300 AND SPRAS ='E'
#                         ''',conn1)

#     fran_sale_df = franchiseDf.merge(
#         cus_df, how='left', left_on=['ibl_customer_code'], right_on=['SAP_CUSTOMER_CODE'])

#     fran_sale_df = fran_sale_df.merge(
#         branch_df, how='left', left_on=['branch_code'], right_on=['sap_branch_code'])

#     fran_sale_df['ref_customer_code'] = np.where(fran_sale_df['SAP_CUSTOMER_CODE'].isnull(), franchiseDf['ibl_distributor_code'].astype(
#         str)+'-'+franchiseDf['ibl_customer_code'].astype(str)
#                 , franchiseDf['ibl_customer_code']
#                     )

#     fran_sale_df.drop(['SAP_CUSTOMER_CODE', 'ADRNR','sap_branch_code'], inplace=True, axis=1)
#     dataFile=f'''{filePath}franchiseData.parquet'''
    
#     fran_sale_df['transfer_date'] = datetime.now()
#     column_name = ["company_code",
#                     "ibl_distributor_code",
#                     "ibl_distributor_desc",
#                     "branch_code",
#                     "branch_desc",
#                     "distributor_location_id",
#                     "distributor_location_desc",
#                     "order_no",
#                     "invoice_number",
#                     "invoice_date",
#                     "channel",
#                     "distributor_customer_code",
#                     "ibl_customer_code",
#                     "ref_customer_code",
#                     "ibl_customer_name",
#                     "brick_code",
#                     "brick_name",
#                     "ADD1",
#                     "ADD2",
#                     "ADD3",
#                     "distributor_item_code",
#                     "ibl_item_code",
#                     "ibl_item_description",
#                     "sold_qty",
#                     "gross_amount",
#                     "bonus_qty",
#                     "discount",
#                     "reason",
#                     "data_loading_date",
#                     "transfer_date",
#                     "record_date",
#                     "address"
#                 ]
#     fran_sale_df=fran_sale_df.reindex(columns=column_name)

#     # df to table
#     pandas_gbq.to_gbq(fran_sale_df, f'{GCS_PROJECT}.{DATA_SET_ID}.{tableId}', project_id=GCS_PROJECT, if_exists='append')
#     print('done.....................')

# #Franchise Stock
# def deleteFranchiseStockRecords():
#     print('vstart date ',vStartDate)
#     print('vEnd Date ', vEndDate)

#     delQuery=f'''delete from data-light-house-prod.EDW.franchise_stock
#                     where dated  between {vStartDate} and {vEndDate}
#       '''   

#     job=bigQueryClient.query(delQuery)
#     job.result()

# def getFranchiseStock():
#     tableId='franchise_stock'

#     sqlGetStock=f'''     
#             select 
#             company_code,
#             ibl_distributor_code,
#             dated,
#             distributor_item_code,
#             ibl_item_code,
#             distributor_item_description,
#             lot_number,
#             expiry_date,
#             stock_qty,
#             stock_value,
#             ibl_branch_code,
#             price,
#             in_transit_stock,
#             purchase_unit,
#             created_date
#             from franchise_stock fs2
#             where 1=1 and dated between {vStartDate} and {vEndDate}           
#             '''    
          
#     franchiseStock=pd.read_sql(sqlGetStock,con=franchiseEngine)

#     # dataframe type conversion
#     convert_dict = {
#                     'company_code': 'string'
#                     ,'ibl_distributor_code': 'int32'
#                     ,'distributor_item_code': 'string'
#                     ,'ibl_item_code': 'string'
#                     ,'distributor_item_description': 'string'
#                     ,'lot_number':'string'
#                     ,'stock_qty':'float32'
#                     ,'stock_value':'float32'
#                     ,'ibl_branch_code':'string'
#                     ,'price':'float32'
#                     ,'in_transit_stock':'float32'
#                     ,'purchase_unit':'float32'
#                 }
#     franchiseStock = franchiseStock.astype(convert_dict)
#     franchiseStock['dated']=pd.to_datetime(franchiseStock['dated'])
#     franchiseStock['expiry_date']=pd.to_datetime(franchiseStock['expiry_date'])
#     franchiseStock['created_date']=pd.to_datetime(franchiseStock['created_date'])
#     franchiseStock['transfer_date'] = datetime.now()

#     pandas_gbq.to_gbq(franchiseStock, f'{GCS_PROJECT}.{DATA_SET_ID}.{tableId}', project_id=GCS_PROJECT, if_exists='append')
#     print('done.....................')


# # Franchise Targets..
# def delFranchiseTargets():
#     delQuery=f'''
#                 delete from data-light-house-prod.EDW.franchise_targets
#                 where 1=1 and cast(target_month as date) between {vStartDate} and {vEndDate}
#                 '''                

#     job=bigQueryClient.query(delQuery)
#     job.result()
    
# def insertFranchiseTargets():
#     tableId='franchise_targets'
#     sqlGetStock=f'''     
#             select
#                 tmonth target_month_key,
#                 rd_code,
#                 rd_name,
#                 "Trg_val" target_value,
#                 trg_month target_month,
#                 "Trg_year" target_year
#             from
#                 franchise.rd_targets rt 
#             where 1=1 and cast(trg_month as date) between {vStartDate} and {vEndDate}            '''
            

#     franchiseStock=pd.read_sql(sqlGetStock,con=franchiseEngine)

#     # dataframe type conversion
#     convert_dict = {
#                     'target_month_key': 'string'
#                     ,'rd_code': 'int32'
#                     ,'rd_name': 'string'
#                     ,'target_value':'float32'
#                     ,'target_year': 'int32'
#                 }
    
#     franchiseStock = franchiseStock.astype(convert_dict)
#     franchiseStock['target_month']=pd.to_datetime(franchiseStock['target_month'])
#     franchiseStock['transfer_date'] = datetime.now()

#     pandas_gbq.to_gbq(franchiseStock, f'{GCS_PROJECT}.{DATA_SET_ID}.{tableId}', project_id=GCS_PROJECT, if_exists='append')
#     print('done.....................')


# deleteRecords() 
# getFranchiseDataDfSql()
# deleteFranchiseStockRecords()
# getFranchiseStock()
# delFranchiseTargets()
# insertFranchiseTargets()

# taskDeleteStockRec=PythonOperator(
#                 task_id='deleteFranchiseStock'
#                 ,python_callable=deleteFranchiseStockRecords
#                 ,dag=franchise_sale_merging
# )

# taskInsertStockRec=PythonOperator(
#                 task_id='insertFranchiseStock'
#                 ,python_callable=getFranchiseStock
#                 ,dag=franchise_sale_merging
# )

# taskDeleteRecrods=PythonOperator(
#                 task_id='deletingRecords'
#                 ,python_callable=deleteRecords
#                 ,dag=franchise_sale_merging
#                 )

# taskInsertingRecords=PythonOperator(
#                 task_id='insertingRecords'
#                 ,python_callable=getFranchiseDataDfSql
#                 ,dag=franchise_sale_merging
#                 )

# [
#     taskDeleteRecrods>>taskInsertingRecords
#     ,taskDeleteStockRec>>taskInsertStockRec
#  ]


