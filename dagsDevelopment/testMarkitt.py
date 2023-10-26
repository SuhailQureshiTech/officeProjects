from datetime import datetime, date
import sys
import pandas as pds
import os
import pyodbc
import psycopg2 as pg
from hdbcli import dbapi
import numpy as np
from sqlalchemy import create_engine
import sqlalchemy
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import connectionClass

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


def getUnmappedItems():
    global unmappedItemsDf, billItemsDf, itemDataConcatDf

    query = f'''SELECT DISTINCT  upper(trim(mplt.BarCode)) barcode,item itemdesc,'Unmapped/Blocked' flag
                FROM {tblMarkittSales} mplt
                left outer join INV_ItemTAB   itm on (mplt.BarCode=itm.BarCode)
                WHERE CAST(BillDate AS date) between  {maxDate} and {dateDate}
            '''
    itemQuery = f'''
                    SELECT upper(trim(NORMT)) BARCODE,MATNR MATERIAL,MAKTX MATERIAL_DESC FROM {tblMarkittItems}
                '''
    unmappedItemsDf = pds.read_sql_query(query, sqlServerConn)
    billItemsDf = pds.read_sql_query(itemQuery, sapConn)

getUnmappedItems()

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
        where cast(billdate as date) between   '2023-07-16' and '2023-07-31'
        '''

    query_return = f'''SELECT BranchCode, convert(varchar, BillDate, 112) as BillDate, BillNo, SerialNo,
        upper(TRIM(BarCode)) AS BarCode, Quantity, Rate, Amount, DiscountAmount, NetAmount, ItemGSTAmount, PaymentMode,
        RecordNo FROM[Markitt2021-2022].dbo.Markitt_SRTDetailListTAB
        where cast(billdate as date) between   '2023-07-16' and '2023-07-31'
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
    db_connection = "hana+hdbcli://ETL:Etl@2025@10.210.134.204:33015/ETL"
    engine1 = sqlalchemy.create_engine(db_connection)
    print('engine : ', engine1)
    print(data_concat)

    delRecQuery = f'''
                delete from ZMARKIT_ITEM where cast(billdate as date) between  '2023-07-16' and '2023-07-31'
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
                            WHERE CAST(BILLDATE  AS date) between  '2023-07-16' and '2023-07-31'
            '''

    engine1.execute(updateMaterial)

    insertZmarkittItems=f''' INSERT INTO SAPABAP1.ZMARKIT_ITEM  (MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG)
            SELECT MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,COALESCE(FLAG,' ')
            FROM ZMARKIT_ITEM zi WHERE  1=1   AND cast(BILLDATE as date) between  '2023-07-16' and '2023-07-31'
    '''

    # insertZmarkitt = f''' INSERT INTO SAPABAP1.ZMARKIT(MANDT,BILLNO,SERIALNO,BRANCH,BILLDATE,MATERIAL,QUANTITY,RATE,AMOUNT,ITEMDISCOUNTAMOUNT,NETAMOUNT,ITEMGSTAMOUNT,ZMODE,FLAG)
    #             select * from ( SELECT 300, BillNo, SerialNo, Branch, BillDate, TRIM(MATERIAL) AS material, Quantity	, Rate, Amount
	# 			,ITEMDISCOUNTAMOUNT, NetAmount, ItemGSTAmount
    #             ,ZMODE, row_number() over (partition by BillNo order by BillDate) as row_number
    #             from ZMARKIT_ITEM zi
    #             WHERE 1=1 AND cast(billdate as date) between  '2023-07-16' and '2023-07-31'
    #             ) as rows
    #                 where row_number = 1
    #             '''

    engine1.execute(insertZmarkittItems)
    # engine1.execute(insertZmarkitt)


get_markitt_POS_header_data()
