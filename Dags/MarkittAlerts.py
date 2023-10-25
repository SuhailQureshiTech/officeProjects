import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, date
import sys
import pandas as pds
import os
import pyodbc
import psycopg2 as pg
from hdbcli import dbapi
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import numpy as np
from sqlalchemy import create_engine
import sqlalchemy
from airflow.utils.email import send_email_smtp
from airflow.operators.email_operator import EmailOperator
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import connectionClass
from airflow.exceptions import AirflowFailException

connClass=connectionClass
sqlServerConn =connClass.markittSqlServer()
sapConn = connClass.sapConn()

today = date.today()
day_diff = 1

past_date=None
dateDate=None
maxDate=None

def generateDate():
    global past_date,dateDate,maxDate
    cursor=sapConn.cursor()
    recCountQuery = 'SELECT ADD_DAYS(max(CAST(BILLDATE AS date)),1)  FROM SAPABAP1.ZMARKIT_ITEM'
    cursor.execute(recCountQuery)
    result=cursor.fetchall()
    for record in result:
        maxDate=record[0]

    past_date = today - pds.DateOffset(days=day_diff)
    dateDate = past_date.strftime("%Y-%m-%d")
    dateDate = "'"+dateDate+"'"
    maxDate = "'"+str(maxDate)+"'"


generateDate()


df_header=None
unmappedItemsDf = pds.DataFrame()
billItemsDf = pds.DataFrame()
itemDataConcatDf = pds.DataFrame()

tblMarkittItems = 'MARKITT_ITEMS'
tblMarkittSales = 'Markitt_POSDetailListTAB'

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 5, 7),
    # 'end_date': datetime(),
    'depends_on_past': False,
    'email': ['shehzad.lalani@iblgrp.com'],
    'email_on_failure': True,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}


def getUnmappedItems():
    global unmappedItemsDf, billItemsDf, itemDataConcatDf

    query = f'''
                SELECT DISTINCT mplt.BarCode barcode,item itemdesc,'Unmapped/Blocked' flag
                FROM {tblMarkittSales} mplt
                left outer join INV_ItemTAB   itm on (mplt.BarCode=itm.BarCode)
                WHERE CAST(BillDate AS date) between  {maxDate} and {dateDate}
            '''
    itemQuery = f'''
                    SELECT NORMT BARCODE,MATNR MATERIAL,MAKTX MATERIAL_DESC FROM {tblMarkittItems}
                '''
    unmappedItemsDf = pds.read_sql_query(query, sqlServerConn)
    billItemsDf = pds.read_sql_query(itemQuery, sapConn)

getUnmappedItems()

recipient ='orasyscojava@gmail.com,muhammad.suhail@iblgrp.com'
recipients = recipient.split(',')

def send_email(context):
    #dag_run = context.get('dag_run')
    msg = "Markitt Alert Completed."
    subject = f"Markitt Alert"
    send_email_smtp(to=['muhammad.suhail@iblgrp.com']
                    ,cc=recipients
                    ,subject=subject, html_content=msg
                    )


def send_email_failure(context):
    #dag_run = context.get('dag_run')
    msg = "Markitt Sales -- DAG to BigQuery Failed."
    subject = f"Markitt Sales DAG to BigQuery Failed"
    send_email_smtp(to=['muhammad.suhail@iblgrp.com'],
                    subject=subject, html_content=msg)

MarkittAlerts = DAG(
    dag_id='Markitt_Alerts',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 10 * * *',
    #schedule_interval=None,
    # dagrun_timeout=timedelta(minutes=60),
    description='Markitt Alerts',
    on_success_callback=send_email,
    # on_failure_callback=send_email_failure,
    max_active_runs=1
)


def email():
    global unmappedItemsDf, billItemsDf, itemDataConcatDf
    print('data date : ',dateDate)
    print('max date : ',maxDate)

    # print(billItemsDf.info())
    itemDataConcatDf = unmappedItemsDf.merge(
        billItemsDf, how='left', left_on=['barcode'], right_on=['BARCODE'])
    odf = itemDataConcatDf.query('MATERIAL.isnull()   ')
    odf.drop(['BARCODE',  'MATERIAL', 'MATERIAL_DESC'],
            inplace=True, axis=1)

    connection_string = connClass.sapConnAlchemy()
    engine1 = sqlalchemy.create_engine(connection_string)

    sql = f'''
            SELECT normt AS barcode,matnr AS sapitemcode,MAKTX  AS itemdesc,'Duplicate'flag
            FROM DUPLICATE_MARKITT_ITEMS
            '''
    df_header = pds.read_sql(sql, engine1)

    data_concat = pds.concat([df_header, odf],             # Append two pandas DataFrames
                            ignore_index=True,
                            sort=False,
                            )
    if len(data_concat) > 0:
        msg = data_concat.to_html()
        send_email_smtp(to=['Asad.Iqbal@Markitt.com','Noman.Ali@habitt.com'
                ,'Muhammad.Shoaib@Markitt.com']
                    ,cc=['Irfan.Parekh@iblgrp.com','muhammad.suhail@iblgrp.com'
                    ,'Fahad.Kasiri@habitt.com','Umer.Altaf@iblgrp.com','Arshad.Ali@habitt.com' 
                    ,'Muhammad.Fayyaz@Markitt.com','Ali.Babur@habitt.com',
                    
                    ]
                        ,subject=f'''Merging between  {maxDate} and {dateDate} failed, beacuase of  Duplicate or Unmapped Items found '''
                        , html_content=msg)
        # raise AirflowFailException(f'''Merging of {dateDate} failed, beacuase of  Duplicate or Unmapped Items found ''')

markittAltersItems = PythonOperator(
    task_id="MarkittAlerts_Items",
    python_callable=email,
    dag=MarkittAlerts
)

# Generate_Markitt_Data = PythonOperator(
#     task_id="Generate_Markitt_Data",
#     python_callable=get_markitt_POS_header_data,
#     dag=Markitt_Sales_POS_Prod
# )

markittAltersItems
