from datetime import date, datetime,timedelta
from hmac import trans_36
from airflow import DAG
from airflow import models
import airflow.operators.dummy
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
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
from airflow.utils.email import send_email_smtp
from airflow.operators.email_operator import EmailOperator
import platform
from hdbcli import dbapi
import pandas as pd
import urllib
import psycopg2 as pg
import pyodbc
from datetime import date, datetime, timedelta
df = pd.DataFrame()

def failure_function(context):
    dag_run = context.get('dag_run')
    ti = context['task_instance']
    #print("mic testing 123")
    msg = f"task {ti.task_id } failed in dag { ti.dag_id }"
    subject = f"DAG {dag_run} Failed"
    send_email_smtp(to='shehzad.lalani@iblgrp.com', subject=subject, html_content=msg)

default_args = {
        'owner': 'admin',    
        'start_date': datetime(2022, 10, 4),
        # 'end_date': datetime(),
        'depends_on_past': False,
        #'email': ['shehzad.lalani@iblgrp.com'],
        'email_on_failure': True,
        'on_failure_callback' : failure_function
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        #'retry_delay': timedelta(minutes=5),
        }

def delete_Markitt_Stock_POS():
     server = '172.20.7.71\SQLSERVER2017' 
     db = 'Markitt2021-2022'
     user = 'syed.shujaat'
     password = 'new$5201'
     schema = 'dbo'
     
     conn_sql = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user +';PWD='+password+';TrustServerCertificate=Yes')
     cursor = conn_sql.cursor()
     
     del_command = f'''TRUNCATE TABLE CSV_StockClosingTAB'''
     cursor.execute(del_command)
     cursor.commit()
     cursor.close()
     conn_sql.close()
        

def get_insert_Markitt_Stock_POS():
     server = '172.20.7.71\SQLSERVER2017' 
     db = 'Markitt2021-2022'
     user = 'syed.shujaat'
     password = 'new$5201'
     schema = 'dbo'
     tablePOS = 'CSV_StockClosingTAB'

     conn_sql = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user +';PWD='+password+';TrustServerCertificate=Yes')
     cursor = conn_sql.cursor()



     # Initialize your connection
     conn = dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2025'  )
     cursor_sap = conn.cursor()

     sql = f''' SELECT mi.NORMT ,zi.MAKTX,zi.MATKL,CLABS FROM SAPABAP1.ZMARKIT_INV zi 
                LEFT OUTER JOIN MARKITT_ITEMS mi ON  (mi.MATNR=zi.MATNR) '''
     df = pd.read_sql(sql, conn)
     df.columns = df.columns.str.strip()
     
     if  not df.empty: 
        for index, row in df.iterrows():
            insertStatement = f'''
                    insert into CSV_StockClosingTAB
                            ( BarCode, 
                            ItemName,
                            Department,
                            Inventory
                            )
                            values
                            (
                                ?,
                                ?,
                                ?,
                                ?
                            )
                            '''
        
        cursor.executemany(insertStatement, df.values.tolist())
        cursor.commit()
     else:
         print("Stock is empty")   
     #cursor.close()
     
     del_command_INV = f'''TRUNCATE TABLE SAPABAP1.ZMARKIT_INV'''
    #  del_command_POS = f'''TRUNCATE TABLE Markitt_POSDetailListTAB'''
    #  del_command_SRT = f'''TRUNCATE TABLE Markitt_SRTDetailListTAB'''
    #  del_command_Master_POS = f'''TRUNCATE TABLE Markitt_POSMasterListTAB'''
    #  del_command_Master_SRT = f'''TRUNCATE TABLE Markitt_SRTMasterListTAB'''     

     

     cursor_sap.execute(del_command_INV)
     
     
    #  cursor.execute(del_command_POS)
    #  cursor.commit()
    #  cursor.execute(del_command_SRT)
    #  cursor.commit()
    #  cursor.execute(del_command_Master_POS)
    #  cursor.commit()
    #  cursor.execute(del_command_Master_SRT)
    #  cursor.commit()
    #  cursor.close()
    #  conn_sql.close()

     
     
     #cursor_sap.close()
     #cursor.close()
     #conn.close()
     #conn_sql.close()
          
     
    #  df.rename(columns = {'MATNR':'BarCode', 'MAKTX':'ItemName', 'MATKL':'Department', 'CLABS':'Inventory'}, inplace = True)
    #  conn_pg = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456")
    #  cursor_pg = conn_pg.cursor()     

    #  if len(df) > 0:
    #     df_columns = list(df)    
    #     columns = ",".join(df_columns)
        	    
        
    
    #     # create VALUES('%s', '%s",...) one '%s' per column
    #     values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
    #     #create INSERT INTO table (columns) VALUES('%s',...)
    #     insert_stmt = "INSERT INTO {} ({}) {}".format('"DW".csv_stockclosingtab_staging',columns,values)
    #     print(insert_stmt)
    #     pg.extras.execute_batch(cursor_pg, insert_stmt, df.values)
        
    #     conn_pg.commit()
    #     cursor_pg.close()
     




Markitt_Stock_POS_Prod = DAG(
    dag_id='Markitt_Stock_POS_Prod',
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval='5 2/2 * * *',    
   # dagrun_timeout=timedelta(minutes=60),
    description='Markitt_Stock_POS',
    catchup=False
)

t0 =   PythonOperator(
        task_id="Markitt_Stock_POS_delete",
        python_callable=delete_Markitt_Stock_POS,
        dag=Markitt_Stock_POS_Prod

)
t1 =    PythonOperator(
        task_id="Markitt_Stock_POS",
        python_callable=get_insert_Markitt_Stock_POS,
        dag=Markitt_Stock_POS_Prod
)

t0 >> t1


