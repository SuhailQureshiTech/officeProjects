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
import pysqldf

connClass = connectionClass
sqlServerConn = connClass.markittSqlServer()
sapConn=connClass.sapConn()
# sqlServerAlchmy=connClass.markittSqlServerAlchmy()

today = date.today()
day_diff = 1
# curr_date = today.strftime("%d-%b-%Y")

past_date = today - pds.DateOffset(days=day_diff)
dateDate = past_date.strftime("%Y-%m-%d")

# dateDate='2023-04-29'

dateDate = "'"+dateDate+"'"

df_header = None
unmappedItemsDf=pds.DataFrame()
billItemsDf=pds.DataFrame()
itemDataConcatDf = pds.DataFrame()

tblMarkittItems = 'MARKITT_ITEMS'
tblMarkittSales = 'Markitt_POSDetailListTAB'

def send_email(context):
    #dag_run = context.get('dag_run')
    msg = "Markitt Sales DAG to BigQuery Completed."
    subject = f"Markitt Sales DAG to BigQuery Completed"
    send_email_smtp(to=['muhammad.suhail@iblgrp.com'],
                    subject=subject, html_content=msg)

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 4, 17),
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
    global unmappedItemsDf,billItemsDf,itemDataConcatDf

    query = f'''
                SELECT DISTINCT mplt.BarCode barcode,item itemdesc,'Unmapped/Blocked' flag
                FROM {tblMarkittSales} mplt
                left outer join INV_ItemTAB   itm on (mplt.BarCode=itm.BarCode)
                WHERE CAST(BillDate AS date)={dateDate}
            '''
    itemQuery = f'''
                    SELECT NORMT BARCODE,MATNR MATERIAL,MAKTX MATERIAL_DESC FROM {tblMarkittItems}
                '''
    unmappedItemsDf=pds.read_sql_query(query, sqlServerConn)
    billItemsDf=pds.read_sql_query(itemQuery,sapConn)


getUnmappedItems()

Markitt_TEST_DATA= DAG(
    dag_id='MARKITT_TEST',
    default_args=default_args,
    catchup=False,
    # schedule_interval='0 1 * * *',
    schedule_interval=None,
    #    dagrun_timeout=timedelta(minutes=60),
    description='Markitt_POS',
    on_success_callback=send_email,
    max_active_runs=1
)

def email():
    global unmappedItemsDf, billItemsDf, itemDataConcatDf
    # print('unmapped')
    # print(unmappedItemsDf.info())
    # print('bill item')
    # print(billItemsDf.info())
    itemDataConcatDf = unmappedItemsDf.merge(
        billItemsDf, how='left', left_on=['barcode'], right_on=['BARCODE'])
    print(itemDataConcatDf)
    odf = itemDataConcatDf.query('MATERIAL.isnull()   ')
    odf.drop(['BARCODE',  'MATERIAL', 'MATERIAL_DESC'],
            inplace=True, axis=1)

    print('odf.........')
    print(odf)
    print(odf.info())

    # odf.to_csv(
    #     '/home/admin2/airflow/dags/checkFile/itemconat.csv',index=False)

    connection_string = connClass.sapConnAlchemy()
    engine1 = sqlalchemy.create_engine(connection_string)

    sql = f'''
                SELECT normt AS barcode,matnr AS sapitemcode,MAKTX  AS itemdesc,'Duplicate'flag
                FROM MARKITT_ITEMS_TEMP limit 1
            '''
    df_header = pds.read_sql(sql, engine1)

    data_concat = pds.concat([df_header, odf],             # Append two pandas DataFrames
                        ignore_index=True,
                        sort=False)
    print('data concat ')
    print(data_concat)
    print('leng data count : ', len(data_concat))
    print(data_concat.info())

    # data_concat.to_csv(
    #     '/home/admin2/airflow/dags/checkFile/itemconat.csv', index=False)

    # print('df length : ', len(data_concat))

    if len(data_concat) > 0:
        msg = data_concat.to_html()
        send_email_smtp(to=['muhammad.suhail@iblgrp.com'],
                        subject='Duplicate/Unmapped.....', html_content=msg)
        raise AirflowFailException('Duplicate or Unmapped Items found, merging failed..')


sendEmail = PythonOperator(
    task_id="markittGrnDataGeneration",
    python_callable=email,
    dag=Markitt_TEST_DATA
)

