# dated: 15Dec12023
# import 
    # Google
from airflow.operators.dummy import DummyOperator
import requests as req
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
from sqlalchemy.types import VARCHAR, INTEGER, FLOAT,Date

import pysftp
import csv
import pyodbc
from hdbcli import dbapi
    #import conf
import pandas as pd
import pandas_gbq
from datetime import date, datetime, timedelta

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

vStartSapDate=vStartDate.strftime('%Y%m%d')
vEndSapDate=date.today().strftime('%Y%m%d')

vStartDate="'"+str(vStartDate)+"'"
vEndDate="'"+str(date.today())+"'"

print('start sap date : ',vStartSapDate)
print('End sap date : ',vEndSapDate)

print('else : from date :', vStartDate)
print('else : enmd date :', vEndDate)


# def success_function(context):
#     #dag_run = context.get('dag_run')
#     msg = "Stock DAG has executed successfully."
#     subject = f"Stock DAG has completed"
#     send_email_smtp(to=['shehzad.lalani@iblgrp.com'],
#                     subject=subject, html_content=msg)


default_args = {
    'owner': 'admin',
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

attendanceSynchDag = DAG(
    dag_id='attendanceSynch',
    default_args=default_args,
    catchup=False,
    start_date=datetime(2023, 11, 20),
    schedule='0 6,10,13,18 * * *',
    # schedule_interval=None,
    # on_success_callback=success_function,
    # email_on_failure=failure_email_function,
    dagrun_timeout=timedelta(minutes=120),
    description='Attendance Synchronization'
)

def delAttendanceRec():
    print('vstart date ',vStartDate)
    print('vEnd Date ', vEndDate)

    sqlDelRec=f'''
                DELETE
                FROM
                attendance.pioneer_attendance pa 
                WHERE
                1 = 1 
                AND Cast(pa.punch_datetime AS DATE) BETWEEN {vStartDate} AND {vEndDate}         
                '''

    result=poineerSqlAlchemy.execute(sqlDelRec)
    print('total number of delete rows ',result.rowcount)

def getAttendance():

    print('vstart date ',vStartDate)
    print('vEnd Date ', vEndDate)

    sqlGetRec=f'''
                SELECT
                300 mandt,
                tran_machinerawpunchid transaction_id,
                cardno employee_id,
                punchdatetime punch_datetime,
                machineno device_no,
                '01' status,
                p_day P_Day,
                ismanual ISManual,
                'DB-RECORDS' data_flag 
                FROM
                tran_machinerawpunch trn 
                WHERE
                1 = 1 
                AND Cast(punchdatetime AS DATE) BETWEEN {vStartDate} AND {vEndDate} 
                AND temp <> 'API'
           '''            
    
    franchiseDf=pd.read_sql(sqlGetRec,con=attendance66)
    franchiseDf['punch_datetime']=pd.to_datetime(franchiseDf['punch_datetime']) 
    franchiseDf['record_datetime']=datetime.now()

    postgressql_dtypes={
        'mandt'                 :   INTEGER
        ,'transaction_id'       :   VARCHAR
        ,'employee_id'          :   VARCHAR
        ,'device_no'            :   VARCHAR
        ,'status'               :   VARCHAR
        ,'P_Day'                :   VARCHAR
        ,'ISManual'             :   VARCHAR 
        ,'data_flag'            :   VARCHAR
    }

    franchiseDf.to_sql(
         'pioneer_attendance'   
        ,schema='attendance'
        ,con=poineerSqlAlchemy
        ,index=False
        ,if_exists='append'
        ,dtype=postgressql_dtypes
    )

    
def getApiRecords():
    vStartDate1= vStartDate.replace("'",'')
    vEndDate1=vEndDate.replace("'",'')

    api_url =f'''http://pioneerattendance.com:94/api/EmployeeData/DateRange/{vStartDate1}/{vEndDate1}'''
    
    print('api url : ',api_url)
    response = req.get(url=api_url
                       )
    r = response.json()
    df = pd.DataFrame.from_dict(r)
    df['mandt']='300'
    df['P_Day']='N'
    df['ISManual']='N'
    df['data_flag']='API'
    df['record_datetime'] = creationDate
    df['No']='999'+df['No'].astype(str)    

    df = df.rename(
        columns={'No': 'transaction_id', 'Employee ID': 'employee_id', 'PunchDatetime': 'punch_datetime'
                 ,'Device No': 'device_no', 'Status': 'status'}
                )


    print(df)
    print(df.info())

    # os.chdir('d:\\Google Drive - Office\\PythonLab\\PoineerAttendance\\')
    # print('working dir ',os.getcwd())
    # df.to_csv('bulkRecordsAPI.csv',index=False)

    df.to_sql(
         'pioneer_attendance'   
        ,schema='attendance'
        ,con=poineerSqlAlchemy
        ,index=False
        ,if_exists='append'
        )     
    print('total number of record from API inserted  : ',len(df))

def delteSapAttendanceRecords():    
    delQuery=f'''
            delete FROM SAPABAP1.ztmpor
            WHERE 1=1 AND cast(DATE1 AS number) BETWEEN  {vStartSapDate} and {vEndSapDate}
            '''
    result=sapAlchemy.execute(delQuery)
    print('total number of rows deleted from sap ',result.rowcount)

def insertAttendanceIntoSap():    
    # sqlGetRec=f'''
    #    select 300 mandt,tmid,cardno ,date1,p_day,ismanual,machine,time,inout1,flag
    #      from attendance.vw_attendance_inout_rec  
    #      where 1=1 and date1 between {vStartSapDate} and {vEndSapDate}     
    # '''    

    sqlGetRec=f'''
                SELECT
                300 mandt,
                Min(tmid)tmid,
                cardno,
                date1,
                p_day,
                ismanual,
                0 machine,
                Min(time)time,
                'In' inout1,
                flag 
                FROM
                attendance.vw_attendance_inout_rec 
                WHERE
                1 = 1 
                AND date1 BETWEEN {vStartSapDate} AND {vEndSapDate} 
                GROUP BY
                cardno,
                date1,
                p_day,
                ismanual,
                flag 
                UNION ALL
                SELECT
                300 mandt,
                Max(tmid)tmid,
                cardno,
                date1,
                p_day,
                ismanual,
                0 machine,
                Max(time)time,
                'Out' inout1,
                flag 
                FROM
                attendance.vw_attendance_inout_rec 
                WHERE
                1 = 1 
                AND date1 BETWEEN {vStartSapDate} AND {vEndSapDate} 
                AND Concat(tmid, cardno, date1, p_day, ismanual, time, flag) NOT IN 
                (
                    SELECT  Concat(Min(tmid), cardno, date1, p_day, ismanual, Min(time), flag) 
                    FROM    attendance.vw_attendance_inout_rec 
                    WHERE   1 = 1 
                    GROUP BY
                        cardno,
                        date1,
                        p_day,
                        ismanual,
                        flag
                )
                GROUP BY
                cardno,
                date1,
                p_day,
                ismanual,
                flag
    '''  

    dfRec=pd.read_sql(sqlGetRec,con=poineerSqlAlchemy) 
    print('Total number of rows inserted in SAP ',len(dfRec))
    # print(dfRec.info())
    print(dfRec)

    dfRec.to_sql('ztmpor'
                 ,schema='SAPABAP1'
                 ,con=sapAlchemy
                 ,index=False
                 ,if_exists='append'                 
                 )

def insertIntoAttendance66():
    sqlGetRecords=f''' 
                    SELECT
                    cardno CardNo,
                    CAST(concat(SUBSTRING(CAST(date1 AS text), 1, 4), '-', SUBSTRING(CAST(date1 AS text), 5, 2), '-', SUBSTRING(CAST(date1 AS text), 7, 2), ' ', SUBSTRING(time, 1, 2), ':', SUBSTRING(time, 3, 2), ':', SUBSTRING(time, 5, 2) ) AS timestamp) PunchDatetime,
                    machine MachineNo,
                    p_day P_Day,
                    ismanual ISManual,
                    inout1 inout,
                    data_flag temp 
                    FROM
                    attendance.vw_attendance_inout_rec a 
                    WHERE
                    1 = 1 
                    AND date1 BETWEEN '{vStartSapDate}' AND '{vEndSapDate}' 
                    AND data_flag = 'API'           
            '''

    df=pd.read_sql(sqlGetRecords,con=poineerSqlAlchemy)
    print(df)

    sqlTruncateTable=f'''delete from Tran_MachineRawPunch_tmp '''    
    attendance66.execute(sqlTruncateTable)   

    df.to_sql('Tran_MachineRawPunch_tmp'
              ,schema='dbo'
              ,con=attendance66
              ,index=False
              ,if_exists='append'
              )
    print('total number of record inserted into 66 machine are  : ',len(df))

    sqlDelExistsRecords=f'''
                delete
                from Tran_MachineRawPunch_tmp 
                WHERE 1 = 1 
                and concat(cardno, punchdatetime, machineno, p_day, ismanual, inout) in 
                (SELECT concat(cardno, punchdatetime, machineno, p_day, ismanual, inout) 
                    from Tran_MachineRawPunch tmrp 
                    where
                        1 = 1 and cast(PunchDatetime as date) BETWEEN {vStartDate} and {vEndDate} and temp = 'API'  )
                '''
    
    attendance66.execute(sqlDelExistsRecords)

    sqlInsertData=f'''
        insert into Tran_MachineRawPunch(CardNo,PunchDatetime,MachineNo,P_Day,ISManual,inout,temp)
                select cardno,punchdatetime,machineno,p_day,ismanual,[inout],temp from Tran_MachineRawPunch_tmp tmp
            '''

    attendance66.execute(sqlInsertData)
    print('done.............')


# # Attendance data synch block
# delAttendanceRec()
# getAttendance()
# getApiRecords()
# delteSapAttendanceRecords()
# insertAttendanceIntoSap()
# insertIntoAttendance66()    


dummy_task = DummyOperator(
    task_id='dummy_task', retries=3, dag=attendanceSynchDag)

taskDelAttendanceRecords=PythonOperator(
    task_id='deleteAttendanceRecords'
    ,python_callable=delAttendanceRec
    ,dag=attendanceSynchDag
)

taskGetAttendanceRecords=PythonOperator(
    task_id='getAttendanceRecords'
    ,python_callable=getAttendance
    ,dag=attendanceSynchDag
)

taskApiAttendanceRecords=PythonOperator(
    task_id='getApiAttendanceRecords'
    ,python_callable=getApiRecords
    ,dag=attendanceSynchDag
)

taskDelSapRecords=PythonOperator(
    task_id='delSapRecords'
    ,python_callable=delteSapAttendanceRecords
    ,dag=attendanceSynchDag
)

taskInsertSapRecords=PythonOperator(
    task_id='insertSapAttendanceRecords'
    ,python_callable=insertAttendanceIntoSap
    ,dag=attendanceSynchDag
)

taskInsertData66=PythonOperator(    
    task_id='insertDataInto66'
    ,python_callable=insertIntoAttendance66
    ,dag=attendanceSynchDag
)
    
taskDelAttendanceRecords>>dummy_task>>[taskGetAttendanceRecords,taskApiAttendanceRecords>>taskInsertData66]>>taskDelSapRecords>>taskInsertSapRecords
