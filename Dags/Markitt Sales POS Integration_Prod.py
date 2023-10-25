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


today = date.today()
day_diff = 1
# curr_date = today.strftime("%d-%b-%Y")
past_date = today - pds.DateOffset(days=day_diff)
dateDate = past_date.strftime("%Y-%m-%d")
dateDate = "'"+dateDate+"'"

default_args = {
        'owner': 'admin',
        'start_date': datetime(2023, 4, 17),
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


def get_markitt_POS_header_data():
    server = '172.20.7.71\SQLSERVER2017'
    db = 'Markitt2021-2022'
    user = 'syed.shujaat'
    password = 'new$5201'
    schema = 'dbo'
    tablePOS = 'Markitt_POSDetailListTAB'
    nan = np.nan
    print('data date : ',dateDate)
    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user +';PWD='+password+';TrustServerCertificate=Yes')

    query_header = f'''select * from ( select BranchCode, convert(varchar, BillDate, 112) as BillDate, BillNo
        , SerialNo, TRIM(BarCode) AS BarCode, Quantity, Rate, Amount, DiscountAmount, NetAmount, ItemGSTAmount
        , PaymentMode, row_number() over (partition by BillNo order by BillDate) as row_number from [Markitt2021-2022].dbo.Markitt_POSDetailListTAB  ) as rows
        where row_number = 1 and  cast(billdate as date)={dateDate} '''

    query_return = f'''select * from ( select BranchCode, convert(varchar, BillDate, 112) as BillDate, BillNo,
            SerialNo,  TRIM(BarCode) AS BarCode, Quantity, Rate, Amount, DiscountAmount, NetAmount, ItemGSTAmount,
            PaymentMode, row_number() over (partition by BillNo order by BillDate) as row_number from [Markitt2021-2022].dbo.Markitt_SRTDetailListTAB ) as rows
            where row_number = 1 and  cast(billdate as date)={dateDate}   '''

    df_header = pds.read_sql(query_header, conn)
    df_header.columns = df_header.columns.str.strip()
    df_return = pds.read_sql(query_return, conn)
    df_return.columns = df_return.columns.str.strip()

    df_header['BarCode']=df_header['BarCode'].str.upper()
    df_return['BarCode']=df_return['BarCode'].str.upper()

    conn_sap =  dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2025'  )
    conn_sap.isconnected()
    cursor_sap = conn_sap.cursor()

    df_header.insert(0,'MANDT','300')
    df_return.insert(0,'MANDT','300')
    df_return['FLAG']='R'

    df_header = df_header.drop('row_number', 1)
    df_return = df_return.drop('row_number', 1)

    #df_return = df_return.drop('ERNAM', 1)
    rearrange_columns = ['MANDT','BillNo','SerialNo','BranchCode','BillDate','BarCode','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']
    rearrange_columns1 = ['MANDT','BillNo','SerialNo','BranchCode','BillDate','BarCode','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode','FLAG']

    df_header = df_header.reindex(columns=rearrange_columns)
    df_return = df_return.reindex(columns=rearrange_columns1)



    df_MARA = pds.read_sql('SELECT a.NORMT,a.MATNR,a.MSTAE,a.ERNAM FROM SAPABAP1."MARA" a INNER JOIN  SAPABAP1."MAKT" b ON a.MATNR=b.MATNR INNER JOIN SAPABAP1."MARC" c ON a.MATNR=c.MATNR  where  b.SPRAS=\'E\' AND  a.MANDT=\'300\' AND a.MSTAE<> \'Z1\' AND a.NORMT<>\'\' AND c.WERKS=\'3300\' ',conn_sap)

    df_header = pds.merge(df_header,df_MARA,left_on='BarCode',right_on='NORMT',how='left')
    df_header = df_header.drop_duplicates(subset=['BillNo'],keep='last')
    df_header = df_header.drop('ERNAM', 1)

    df_header_NORMT_null = df_header[df_header['NORMT'].isnull()]
    df_header_NORMT      =  df_header[df_header['NORMT'].notnull()]

    df_header_NORMT['SerialNo']=df_header_NORMT['SerialNo'].astype(int)

    df_header_NORMT = df_header_NORMT[['MANDT','BillNo','SerialNo','BranchCode','BillDate','MATNR','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']]
    df_header_NORMT_null['SerialNo']=df_header_NORMT_null['SerialNo'].astype(int)

    df_header_NORMT_null = df_header_NORMT_null[['MANDT','BillNo','SerialNo','BranchCode','BillDate','BarCode','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']]

    df_return = pds.merge(df_return,df_MARA,left_on='BarCode',right_on='NORMT',how='left')
    df_return = df_return.drop_duplicates(subset=['BillNo'],keep='last')
    df_return = df_return.drop('ERNAM', 1)

    df_return_NORMT_null = df_return[df_return['NORMT'].isnull()]
    df_return_NORMT      =  df_return[df_return['NORMT'].notnull()]


    df_return_NORMT['SerialNo']=df_return_NORMT['SerialNo'].astype(int)

    df_return_NORMT = df_return_NORMT[['MANDT','BillNo','SerialNo','BranchCode','BillDate','MATNR','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']]
    df_return_NORMT['FLAG']='R'
    df_return_NORMT_null['SerialNo']=df_return_NORMT_null['SerialNo'].astype(int)

    df_return_NORMT_null = df_return_NORMT_null[['MANDT','BillNo','SerialNo','BranchCode','BillDate','BarCode','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']]
    df_return_NORMT_null['FLAG']='R'
    df_header_NORMT_null = df_header_NORMT_null.fillna('')
    df_return_NORMT_null = df_return_NORMT_null.fillna('')

    df_header_NORMT = df_header_NORMT[~df_header_NORMT['BillNo'].isin(df_header_NORMT_null['BillNo'])]
    df_return_NORMT = df_return_NORMT[~df_return_NORMT['BillNo'].isin(df_return_NORMT_null['BillNo'])]

    print(df_header_NORMT)
    print(df_return_NORMT)
    print(df_header_NORMT_null['BillNo'])
    print(df_return_NORMT_null['BillNo'])

    if not df_header_NORMT_null.empty:
        for i, row in df_header_NORMT_null.iterrows():
                    print(row)
                    sql = "INSERT INTO \"SAPABAP1\".\"ZMARKIT_LOG\" (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE) VALUES (" + "?,"*(len(row)-1) + "?)"
                    cursor_sap.execute(sql, tuple(row))
        conn_sap.commit()
    else:
        print("Sales with CIS Code not mapped not found")
    if  not df_return_NORMT.empty:
        for i, row in df_return_NORMT.iterrows():
                    print(row)
                    sql = "INSERT INTO \"SAPABAP1\".\"ZMARKIT\" (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG) VALUES (" + "?,"*(len(row)-1) + "?)"
                    cursor_sap.execute(sql, tuple(row))
        conn_sap.commit()
    else:
        print("Returns not found")
    if  not df_return_NORMT_null.empty:
        for i, row in df_return_NORMT_null.iterrows():
                    print(row)
                    sql = "INSERT INTO \"SAPABAP1\".\"ZMARKIT_LOG\" (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG) VALUES (" + "?,"*(len(row)-1) + "?)"
                    cursor_sap.execute(sql, tuple(row))
        conn_sap.commit()
    else:
        print("Returns with CIS Code not mapped not found")
    if  not df_header_NORMT.empty:
        for i, row in df_header_NORMT.iterrows():
                    print(row)
                    sql = "INSERT INTO \"SAPABAP1\".\"ZMARKIT\" (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE) VALUES (" + "?,"*(len(row)-1) + "?)"
                    cursor_sap.execute(sql, tuple(row))
        conn_sap.commit()
    else:
        print("Sales not found")



def  get_markitt_POS_data():
     #global df_header_NORMT
     #global df_return_NORMT

     server = '172.20.7.71\SQLSERVER2017'
     db = 'Markitt2021-2022'
     user = 'syed.shujaat'
     password = 'new$5201'
     schema = 'dbo'
     tablePOS = 'Markitt_POSDetailListTAB'
     tableSRT = 'Markitt_SRTDetailListTAB'

     conn_sqlserver = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user +';PWD='+password+';TrustServerCertificate=Yes')
     cursor_sqlserver = conn_sqlserver.cursor()
     query = "SELECT * FROM [%s].%s.%s" % (db, schema, tablePOS);

     query_hana = f'''SELECT BranchCode, convert(varchar, BillDate, 112) as BillDate, BillNo, SerialNo,
        TRIM(BarCode) AS BarCode, Quantity, Rate, Amount, DiscountAmount, NetAmount, ItemGSTAmount,
        PaymentMode FROM [Markitt2021-2022].dbo.Markitt_POSDetailListTAB
        where cast(billdate as date)={dateDate} '''

     query_return = f'''SELECT BranchCode, convert(varchar, BillDate, 112) as BillDate, BillNo, SerialNo,
        TRIM(BarCode) AS BarCode, Quantity, Rate, Amount, DiscountAmount, NetAmount, ItemGSTAmount, PaymentMode,
        RecordNo FROM[Markitt2021-2022].dbo.Markitt_SRTDetailListTAB
        where cast(billdate as date)={dateDate} '''

     df_POS = pds.read_sql(query, conn_sqlserver)
     df_POS.columns = df_POS.columns.str.strip()
     df_hana = pds.read_sql(query_hana, conn_sqlserver)
     df_hana.columns = df_hana.columns.str.strip()
     df_return = pds.read_sql(query_return, conn_sqlserver)
     df_return.columns = df_return.columns.str.strip()

     df_hana['BarCode']=df_hana['BarCode'].str.upper()
     df_return['BarCode']=df_return['BarCode'].str.upper()

     conn = pg.connect( host="35.216.168.189", port= '5433', database="DATAWAREHOUSE", user="postgres", password="ibl@123@456" )
     cursor = conn.cursor()

     conn_sap =  dbapi.connect(  address='10.210.134.204',   port='33015',   user='ETL',  password='Etl@2025'  )
     conn_sap.isconnected()
     cursor_sap = conn_sap.cursor()
     #print(df_hana)
     df_hana.insert(0,'MANDT','300')
     df_return.insert(0,'MANDT','300')
     df_return['FLAG']='R'

     rearrange_columns = ['MANDT','BillNo','SerialNo','BranchCode','BillDate','BarCode','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']
     rearrange_columns1 = ['MANDT','BillNo','SerialNo','BranchCode','BillDate','BarCode','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode','FLAG']

     df_hana = df_hana.reindex(columns=rearrange_columns)
     df_return = df_return.reindex(columns=rearrange_columns1)
     #print(df_return)

     df_MARA = pds.read_sql('SELECT a.NORMT,a.MATNR,a.MSTAE,a.ERNAM FROM SAPABAP1."MARA" a INNER JOIN  SAPABAP1."MAKT" b ON a.MATNR=b.MATNR INNER JOIN SAPABAP1."MARC" c ON a.MATNR=c.MATNR  where  b.SPRAS=\'E\' AND  a.MANDT=\'300\' AND a.MSTAE<> \'Z1\' AND a.NORMT<>\'\' AND c.WERKS=\'3300\' ',conn_sap)
     df_hana = pds.merge(df_hana,df_MARA,left_on='BarCode',right_on='NORMT',how='left')
     df_hana = df_hana.drop_duplicates()
     df_hana = df_hana.drop('ERNAM', 1)

     df_hana_NORMT_null = df_hana[df_hana['NORMT'].isnull()]
     df_hana_NORMT      =  df_hana[df_hana['NORMT'].notnull()]
     #print(df_hana_NORMT)
     df_hana_NORMT['SerialNo']=df_hana_NORMT['SerialNo'].astype(int)
     #df_hana_NORMT['Quantity']=df_hana_NORMT['Quantity'].astype(int)
     df_hana_NORMT = df_hana_NORMT[['MANDT','BillNo','SerialNo','BranchCode','BillDate','MATNR','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']]
     df_hana_NORMT_null['SerialNo']=df_hana_NORMT_null['SerialNo'].astype(int)
     #df_hana_NORMT_null['Quantity']=df_hana_NORMT_null['Quantity'].astype(int)
     df_hana_NORMT_null = df_hana_NORMT_null[['MANDT','BillNo','SerialNo','BranchCode','BillDate','BarCode','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']]
     #print(df_hana_NORMT_null)

    #df_return = df_return.drop('ERNAM', 1)

     df_return = pds.merge(df_return,df_MARA,left_on='BarCode',right_on='NORMT',how='left')
     df_return = df_return.drop_duplicates()
     df_return = df_return.drop('ERNAM', 1)

     df_return_NORMT_null = df_return[df_return['NORMT'].isnull()]
     df_return_NORMT      =  df_return[df_return['NORMT'].notnull()]
     #print(df_return_NORMT_null)

     df_return_NORMT['SerialNo']=df_return_NORMT['SerialNo'].astype(int)
     #df_return_NORMT['Quantity']=df_return_NORMT['Quantity'].astype(int)
     df_return_NORMT = df_return_NORMT[['MANDT','BillNo','SerialNo','BranchCode','BillDate','MATNR','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']]
     df_return_NORMT['FLAG']='R'
     df_return_NORMT_null['SerialNo']=df_return_NORMT_null['SerialNo'].astype(int)
     #df_return_NORMT_null['Quantity']=df_return_NORMT_null['Quantity'].astype(int)
     df_return_NORMT_null = df_return_NORMT_null[['MANDT','BillNo','SerialNo','BranchCode','BillDate','BarCode','Quantity','Rate','Amount','DiscountAmount','NetAmount','ItemGSTAmount','PaymentMode']]
     df_return_NORMT_null['FLAG']='R'
     df_hana_NORMT_null = df_hana_NORMT_null.fillna('')
     df_return_NORMT_null = df_return_NORMT_null.fillna('')

     df_hana_NORMT = df_hana_NORMT[~df_hana_NORMT['BillNo'].isin(df_hana_NORMT_null['BillNo'])]
     df_return_NORMT = df_return_NORMT[~df_return_NORMT['BillNo'].isin(df_return_NORMT_null['BillNo'])]

     print(df_hana_NORMT['BillNo'])
     print(df_return_NORMT['BillNo'])
     print(df_hana_NORMT_null['BillNo'])
     print(df_return_NORMT_null['BillNo'])

     if  not df_return_NORMT.empty:
        for i, row in df_return_NORMT.iterrows():
                    print(row)
                    sql = "INSERT INTO \"SAPABAP1\".\"ZMARKIT_ITEM\" (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG) VALUES (" + "?,"*(len(row)-1) + "?)"
                    cursor_sap.execute(sql, tuple(row))
        conn_sap.commit()
     else:
        print("Returns not found")
     if  not df_hana_NORMT.empty:
         for i, row in df_hana_NORMT.iterrows():
                     print(row)
                     sql = "INSERT INTO \"SAPABAP1\".\"ZMARKIT_ITEM\" (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE) VALUES (" + "?,"*(len(row)-1) + "?)"
                     cursor_sap.execute(sql, tuple(row))
         conn_sap.commit()
     else:
         print("Sales not found")

     if not df_hana_NORMT_null.empty:
         for i, row in df_hana_NORMT_null.iterrows():
                     print(row)
                     sql = "INSERT INTO \"SAPABAP1\".\"ZMARKIT_ITEM_LOG\" (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE) VALUES (" + "?,"*(len(row)-1) + "?)"
                     cursor_sap.execute(sql, tuple(row))
         conn_sap.commit()
     else:
         print("Sales with CIS Code not mapped not found")

     if  not df_return_NORMT_null.empty:
        for i, row in df_return_NORMT_null.iterrows():
                    print(row)
                    sql = "INSERT INTO \"SAPABAP1\".\"ZMARKIT_ITEM_LOG\" (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG) VALUES (" + "?,"*(len(row)-1) + "?)"
                    cursor_sap.execute(sql, tuple(row))
        conn_sap.commit()
     else:
        print("Returns with CIS Code not mapped not found")


Markitt_Sales_POS_Prod = DAG(
    dag_id='Markitt_Sales_POS_Prod',
    default_args=default_args,
    catchup=False,
    # schedule_interval='0 3 * * *',
    schedule_interval=None,
   # dagrun_timeout=timedelta(minutes=60),
    description='Markitt_POS',
    max_active_runs=2
)

t1 =    PythonOperator(
        task_id="get_POS_header_data",
        python_callable=get_markitt_POS_header_data,
        dag=Markitt_Sales_POS_Prod
)

t2 =    PythonOperator(
        task_id="get_POS_data",
        python_callable=get_markitt_POS_data,
        dag=Markitt_Sales_POS_Prod
)

t1 >> t2






