import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime,date
import sys
import pandas as pds
import os
import pyodbc
import psycopg2 as pg
from hdbcli import dbapi
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import numpy as np

default_args = {
        'owner': 'admin',    
        'start_date': datetime(2023, 2, 21),
        # 'end_date': datetime(),
        'depends_on_past': False,
        'email': ['shehzad.lalani@iblgrp.com'],
        'email_on_failure': True,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        }

def get_TM_data():
    # SQL Server Connection
    server = '192.168.130.153' 
    db = 'Pioneertime'
    user = 'sa'
    password = 'Ufone@123'
    schema = 'dbo'
    tablePOS = 'Tran_MachineRawPunch'
    nan = np.nan
    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+password)
    # SAP Qas Connection
    conn_sap =  dbapi.connect(  address='10.210.166.202',   port='32015',   user='ETL',  password='Etl@2024'  )
    conn_sap.isconnected()
    cursor_sap = conn_sap.cursor()
    #Fetching TM Data
    query_TM = "select Tran_MachineRawPunchId, CardNo, convert(varchar, PunchDatetime, 112) as DATE1,P_Day,ISManual,MachineNo,Dateime1  AS TIME,inout  from [Pioneertime].[dbo].[Tran_MachineRawPunch] where cast(Dateime1 as date) between  '2022-03-01' and  '2022-03-31'"
    df_TM = pds.read_sql(query_TM, conn)
    df_TM.columns = df_TM.columns.str.strip()
    df_TM = df_TM[['Tran_MachineRawPunchId','CardNo','DATE1','P_Day','ISManual','MachineNo','TIME','inout']]
    df_TM.insert(0,'MANDT','402')
    df_TM['TIME'] = pds.to_datetime(df_TM['TIME'] ).dt.time.apply(lambda x: x.replace(microsecond=0))
    df_TM['TIME'] = df_TM['TIME'].astype(str)
    df_TM['TIME'] = df_TM['TIME'].str.replace(":","")  
    
   
    df_TM.rename(columns = {'Tran_MachineRawPunchId':'TMID', 'CardNo':'CARDNO', 'P_Day':'P_DAY', 'ISManual':'ISMANUAL',  'MachineNo':'MACHINE', 'inout':'INOUT1'}, inplace = True)
    df_TM['FLAG'] = '1'
    df_TM['INOUT1'] = df_TM['INOUT1'].replace(np.nan,'0')
    print(df_TM)
    # df_TM_reformed = tuple(df_TM.itertuples(index=False, name=None))
    # params = ("?," * len(df_TM_reformed[0]))[:-1]
    # print(params)
    # sql = "INSERT INTO SAPABAP1.ZTMPOR (%s)" % (params)
    # print(sql)
    # cursor_sap.executemany(sql, df_TM_reformed)
    # cursor_sap.close()
    # conn_sap.commit()
    # conn_sap.close()
    if not df_TM.empty:
        for i, row in df_TM.iterrows():
                    #print(row)
                    sql = "INSERT INTO \"SAPABAP1\".\"ZTMPOR\" (MANDT, TMID, CARDNO, DATE1, P_DAY, ISMANUAL, MACHINE, TIME, INOUT1,FLAG) VALUES (" + "?,"*(len(row)-1) + "?)"
                    #sql = 'INSERT INTO \"SAPABAP1\".\"ZTMPOR\" (MANDT, TMID, CARDNO, DATE1, P_DAY, ISMANUAL, PAYCODE, MACHINE, "TIME", INOUT1)  VALUES (" + "?,"*(len(row)-1) + "?)'
                    cursor_sap.execute(sql, tuple(row))
        conn_sap.commit()
    else:
        print("Time Machine data not found") 



Time_Management_Integration = DAG(
    dag_id='Time_Management_Integration',
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    #schedule_interval='30 2/2 * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='TM_Portal',
    max_active_runs=1,
    catchup=False
)

t1 =    PythonOperator(
        task_id="get_TM_data",
        python_callable=get_TM_data,
        dag=Time_Management_Integration
)