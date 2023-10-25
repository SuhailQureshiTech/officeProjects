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

connClass = connectionClass
sqlServerConn = connClass.markittSqlServer()
sapConn = connClass.sapConn()

today = date.today()
day_diff = 1

past_date = None
dateDate = None
maxDate = None
fromSapDate=None
toSapDate=None

def generateDate():
    global past_date, dateDate, maxDate,fromSapDate,toSapDate
    cursor = sapConn.cursor()
    recCountQuery = 'SELECT ADD_DAYS(max(CAST(BILLDATE AS date)),1)  FROM SAPABAP1.ZMARKIT_ITEM'
    cursor.execute(recCountQuery)
    result = cursor.fetchall()
    for record in result:
        maxDate = record[0]

    past_date = today - pds.DateOffset(days=day_diff)
    dateDate = past_date.strftime("%Y-%m-%d")

    # fromSapDate = str(maxDate).replace('-','')
    # toSapDate = str(dateDate).replace('-', '')

    dateDate = "'"+dateDate+"'"
    maxDate = "'"+str(maxDate)+"'"

    # fromSapDate = "'"+fromSapDate+"'"
    # toSapDate = "'"+toSapDate+"'"


generateDate()

df_header = None
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


# def getUnmappedItems():
#     global unmappedItemsDf, billItemsDf, itemDataConcatDf

#     query = f'''SELECT DISTINCT  upper(trim(mplt.BarCode)) barcode,item itemdesc,'Unmapped/Blocked' flag
#                 FROM {tblMarkittSales} mplt
#                 left outer join INV_ItemTAB   itm on (mplt.BarCode=itm.BarCode)
#                 WHERE CAST(BillDate AS date) between  {maxDate} and {dateDate}
#             '''
#     itemQuery = f'''
#                     SELECT NORMT BARCODE,MATNR MATERIAL,MAKTX MATERIAL_DESC FROM {tblMarkittItems}
#                 '''
#     unmappedItemsDf = pds.read_sql_query(query, sqlServerConn)
#     billItemsDf = pds.read_sql_query(itemQuery, sapConn)

# getUnmappedItems()

def get_markitt_POS_header_data():
    global df_header
    print('max date : ', maxDate)
    tablePOS = 'Markitt_POSDetailListTAB'
    nan = np.nan
    print('data date : ', dateDate)
    print('fromSapDate : ',fromSapDate)
    print('toSapDate : ', toSapDate)

    conn = sqlServerConn
    query_header = f'''SELECT BranchCode, convert(varchar, BillDate, 112) as BillDate, BillNo, SerialNo,
        upper(TRIM(BarCode)) AS BarCode, Quantity, Rate, Amount, DiscountAmount, NetAmount, ItemGSTAmount,
        PaymentMode FROM [Markitt2021-2022].dbo.Markitt_POSDetailListTAB
        where cast(billdate as date) between   '2023-05-26' and '2023-05-26'
        '''

    query_return = f'''SELECT BranchCode, convert(varchar, BillDate, 112) as BillDate, BillNo, SerialNo,
        upper(TRIM(BarCode)) AS BarCode, Quantity, Rate, Amount, DiscountAmount, NetAmount, ItemGSTAmount, PaymentMode,
        RecordNo FROM[Markitt2021-2022].dbo.Markitt_SRTDetailListTAB
        where cast(billdate as date) between  '2023-05-26' and '2023-05-26'
        '''
    df_header = pds.read_sql(query_header, conn)
    df_header.columns = df_header.columns.str.strip()
    df_header['BarCode'] = df_header['BarCode'].str.upper()
    df_header.insert(0, 'MANDT', '300')

    df_return = pds.read_sql(query_return, conn)
    df_return.columns = df_return.columns.str.strip()
    df_return.insert(0, 'MANDT', '300')
    df_return['FLAG'] = 'R'

    data_concat = pds.concat([df_header, df_return],             # Append two pandas DataFrames
                            ignore_index=True,
                            sort=False)
    print(data_concat.info())
    print(data_concat)

    data_concat = data_concat.rename(columns={
        'MANDT': 'MANDT', 'BillNo': 'BILLNO', 'SerialNo': 'SERIALNO', 'BranchCode': 'BRANCH', 'BillDate': 'BILLDATE', 'BarCode': 'BARCODE', 'Quantity': 'QUANTITY', 'Rate': 'RATE', 'Amount': 'AMOUNT', 'DiscountAmount': 'ITEMDISCOUNTAMOUNT', 'NetAmount': 'NETAMOUNT', 'ItemGSTAmount': 'ITEMGSTAMOUNT', 'PaymentMode': 'ZMODE', 'BillDate': 'BILLDATE', 'BarCode': 'BARCODE', 'Quantity': 'QUANTITY', 'FLAG': 'FLAG'
                                        })

    rearrange_columns = ['MANDT', 'BILLNO', 'SERIALNO', 'BRANCH', 'BILLDATE', 'QUANTITY', 'RATE', 'AMOUNT'
                        ,'ITEMDISCOUNTAMOUNT', 'NETAMOUNT', 'ITEMGSTAMOUNT', 'ZMODE', 'FLAG','BARCODE']

    data_concat = data_concat.reindex(columns=rearrange_columns)

    connection_string = connClass.sapConnAlchemy()
    engine1 = sqlalchemy.create_engine(connection_string)
    print('engine : ', engine1)
    print(data_concat)

    delRecQuery = f'''
                delete from ZMARKIT_ITEM where cast(billdate as date) between  '2023-05-26' and '2023-05-26'
    '''
    engine1.execute(delRecQuery)
    data_concat.to_sql('ZMARKIT_ITEM',
                    schema='ETL',
                    con=engine1,
                    index=False,
                    if_exists='append'
                    )

    updateMaterial = f''' UPDATE etl.ZMARKIT_ITEM zi
                            SET MATERIAL=(SELECT MATNR  FROM MARKITT_ITEMS  WHERE NORMT = zi.barcode)
                            WHERE CAST(BILLDATE  AS date) between   '2023-05-26' and '2023-05-26'
            '''

    engine1.execute(updateMaterial)

    insertZmarkittItems=f''' INSERT INTO SAPABAP1.ZMARKIT_ITEM  (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG)
            SELECT MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,COALESCE(FLAG,' ')
            FROM ZMARKIT_ITEM zi WHERE  1=1   AND cast(BILLDATE as date) between   '2023-05-26' and '2023-05-26'
    '''

    insertZmarkitt = f''' INSERT INTO SAPABAP1.ZMARKIT(MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG)
                select * from ( SELECT 300, BillNo, SerialNo, Branch, BillDate, TRIM(MATERIAL) AS material, Quantity	, Rate, Amount
				,ITEMDISCOUNTAMOUNT, NetAmount, ItemGSTAmount
                ,ZMODE, row_number() over (partition by BillNo order by BillDate) as row_number
                from ZMARKIT_ITEM zi
                WHERE 1=1 AND cast(billdate as date) between  '2023-05-26' and '2023-05-26'
                ) as rows
                    where row_number = 1
                '''

    engine1.execute(insertZmarkittItems)
    engine1.execute(insertZmarkitt)

def send_email(context):
    #dag_run = context.get('dag_run')
    msg = "Markitt Sales -- DAG to BigQuery Completed."
    subject = f"Markitt Sales Integration DAG to BigQuery Completed"
    send_email_smtp(to=['muhammad.suhail@iblgrp.com']
                ,cc=['muhammad.suhail@iblgrp.com', 'Fahad.Kasiri@habitt.com', 'Umer.Altaf@iblgrp.com','Arshad.Ali@habitt.com'
                        ,'Muhammad.Fayyaz@Markitt.com', 'Asad.Iqbal@Markitt.com','Ali.Babur@habitt.com', 'Noman.Ali@habitt.com'
                        ,'orasyscojava@gmail.com']
                    ,subject=subject
                    , html_content=msg
                    )

def send_email_failure(context):
    #dag_run = context.get('dag_run')
    msg = "Markitt Sales -- DAG to BigQuery Failed."
    subject = f"Markitt Sales DAG to BigQuery Failed"
    send_email_smtp(to=['muhammad.suhail@iblgrp.com']
                    ,cc=['Muhammad.Fayyaz@Markitt.com']
                    ,subject=subject, html_content=msg
                    )

Markitt_Sales_POS_Prod = DAG(
    dag_id='Markitt_Sales_POS_Prod_Manual',
    default_args=default_args,
    catchup=False,
    # schedule_interval='0 3 * * *',
    schedule_interval=None,
    # dagrun_timeout=timedelta(minutes=60),
    description='Markitt_POS',
    on_success_callback=send_email,
    on_failure_callback=send_email_failure,
    max_active_runs=1
)

# def email():
#     global unmappedItemsDf, billItemsDf, itemDataConcatDf
#     # print('unmapped')
#     # print(unmappedItemsDf.info())
#     print('data date : ', dateDate)
#     print('max date : ', maxDate)

#     # print(billItemsDf.info())
#     itemDataConcatDf = unmappedItemsDf.merge(
#         billItemsDf, how='left', left_on=['barcode'], right_on=['BARCODE'])
#     # print(itemDataConcatDf)
#     odf = itemDataConcatDf.query('MATERIAL.isnull()   ')
#     odf.drop(['BARCODE',  'MATERIAL', 'MATERIAL_DESC'],
#              inplace=True, axis=1)

#     connection_string = connClass.sapConnAlchemy()
#     engine1 = sqlalchemy.create_engine(connection_string)

#     sql = f'''
#             SELECT normt AS barcode,matnr AS sapitemcode,MAKTX  AS itemdesc,'Duplicate'flag
#             FROM DUPLICATE_MARKITT_ITEMS
#             '''
#     df_header = pds.read_sql(sql, engine1)

#     data_concat = pds.concat([df_header, odf],             # Append two pandas DataFrames
#                              ignore_index=True,
#                              sort=False,
#                              )
#     print('data concat ')
#     print(data_concat)
#     print('leng data count : ', len(data_concat))
#     print(data_concat.info())

#     # data_concat.to_csv(
#     #     '/home/admin2/airflow/dags/checkFile/itemconat.csv', index=False)

#     # print('df length : ', len(data_concat))

#     if len(data_concat) > 0:
#         msg = data_concat.to_html()
#         send_email_smtp(to=['Irfan.Parekh@iblgrp.com']
#                 ,cc=['muhammad.suhail@iblgrp.com', 'Fahad.Kasiri@habitt.com', 'Umer.Altaf@iblgrp.com','Arshad.Ali@habitt.com'
#                 ,'Muhammad.Fayyaz@Markitt.com', 'Asad.Iqbal@Markitt.com','Ali.Babur@habitt.com', 'Noman.Ali@habitt.com'
#                 , 'orasyscojava@gmail.com' ]
#                         , subject=f'''Merging between  {maxDate} and {dateDate} failed, beacuase of  Duplicate or Unmapped Items found ''', html_content=msg)
#         raise AirflowFailException(
#             f'''Merging of {dateDate} failed, beacuase of  Duplicate or Unmapped Items found ''')

# executDataValidation = PythonOperator(
#     task_id="Execute_Data_Validation",
#     python_callable=email,
#     dag=Markitt_Sales_POS_Prod
# )

Generate_Markitt_Data = PythonOperator(
    task_id="Generate_Markitt_Data",
    python_callable=get_markitt_POS_header_data,
    dag=Markitt_Sales_POS_Prod
)

# executDataValidation >>
Generate_Markitt_Data
