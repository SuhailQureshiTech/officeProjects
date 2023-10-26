import platform
from tkinter.tix import INTEGER
from hdbcli import dbapi
from numpy import integer
import pandas as pd
import sqlalchemy
import urllib
import psycopg2
from datetime import date, datetime, timedelta
df = pd.DataFrame()
day_diff=1

# verify the architecture of Python
# print("Platform architecture: " + platform.architecture()[0])
today = date.today()
vEndDate = datetime.date(datetime.today()-timedelta(days=day_diff)).strftime("%Y%m%d")
vEndDate = "'"+vEndDate+"'"
print('end date : ', vEndDate)
# Initialize your connection
conn = dbapi.connect(
    # Option 1, retrieve the connection parameters from the hdbuserstore
    # key='USER1UserKey', # address, port, user and password are retrieved from the hdbuserstore

    # Option2, specify the connection parameters
    address='10.210.134.204',
    port='33015',
    user='Etl',
    password='Etl@2022'

    # Additional parameters
    # encrypt=True, # must be set to True when connecting to HANA as a Service
    # As of SAP HANA Client 2.6, connections on port 443 enable encryption by default (HANA Cloud)
    # sslValidateCertificate=False #Must be set to false when connecting
    # to an SAP HANA, express edition instance that uses a self-signed certificate.
)
# If no errors, print connected
print('connected')
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                "SERVER=192.168.130.81\sqldw;"
                                "DATABASE=ibl_dw;"
                                "UID=pbironew;"
                                "PWD=pbiro1234_456")

cursor = conn.cursor()
del_command = f'''DELETE FROM PHNX_SALES_DETAIL_DATA '''
del_command1 = f'''DELETE FROM PHNX_SALES_DATA '''
del_command2 = f'''DELETE FROM phnx_sales_data_tmp_konv '''

def deleteData():
    cursor.execute(del_command)
    cursor.execute(del_command1)
    cursor.execute(del_command2)

def insertSapSales():
    insert_command1=f'''
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
            --VBAP.ABGRU  AUGRU
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
            and "FKDAT"  =  {vEndDate}
                    and "VBRK"."VKORG" in ('6300', '6100') and "VBRK"."FKART" in ( 'ZOPC', 'ZOCC', 'ZUBC', 'ZUCC', 'ZORE', 'ZORC', 'ZURB', 'ZUB1', 'ZNES', 'ZNEC')
                --	)
                '''
    insert_command2=f'''
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
    update_statement=f'''
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
    insert_command3=f'''
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

def insertData():
    sql_command = f'''SELECT
        ITEM_CATEGORY
        , ORG_ID
        , ORG_DESC
        , TRX_DATE
        , CAST(
            TRX_NUMBER AS VARCHAR
        )TRX_NUMBER
        , BOOKER_ID
        , BOOKER_NAME
        , SUPPLIER_ID
        , SUPPLIER_NAME
        , BUSINESS_LINE_ID
        , BUSINESS_LINE
        ,CHANNEL
        , CUSTOMER_ID
        , CUSTOMER_NUMBER
        , CUSTOMER_NAME
        , SALES_ORDER_TYPE
        , INVENTORY_ITEM_ID
        , ITEM_CODE
        , DESCRIPTION
        ,UNIT_SELLING_PRICE
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
        , RETURN_REASON_CODE
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
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect={}".format(params), fast_executemany=True)

    del_sql_statement = f'''delete from PHNX_SALES_DATA
                    where trx_date={vEndDate}'''

    engine.execute(del_sql_statement)
    df.to_sql('PHNX_SALES_DATA',
            schema='dbo',
            con=engine,
            chunksize=500,
            index=False,
            if_exists='append',
            )

    sql_command = f'''
        SELECT
        ITEM_CATEGORY item_category
        ,ORG_ID org_id
        ,ORG_DESC org_desc
        ,TRX_DATE trx_date
        ,TRX_NUMBER trx_number
        ,BOOKER_ID booker_id
        ,BOOKER_NAME booker_name
        ,SUPPLIER_ID supplier_id
        ,SUPPLIER_NAME supplier_name
        ,BUSINESS_LINE_ID business_line_id
        ,BUSINESS_LINE business_line
        ,CHANNEL channel
        ,CUSTOMER_ID customer_id
        ,CUSTOMER_NUMBER customer_number
        ,CUSTOMER_NAME customer_name
        ,SALES_ORDER_TYPE sales_order_type
        ,INVENTORY_ITEM_ID inventory_item_id
        ,ITEM_CODE item_code
        ,DESCRIPTION description
        ,UNIT_SELLING_PRICE unit_selling_price
        ,SOLD_QTY sold_qty
        ,BONUS_QTY bonus_qty
        ,CLAIMABLE_DISCOUNT claimable_discount
        ,UNCLAIMABLE_DISCOUNT unclaimable_discount
        ,TAX_RECOVERABLE tax_recoverable
        ,NET_AMOUNT net_amount
        ,GROSS_AMOUNT gross_amount
        ,TOTAL_DISCOUNT total_discount ,CUSTOMER_TRX_ID  customer_trx_id
        ,REASON_CODE reason_code
        ,BILL_TYPE_DESC bill_type_desc
        ,COMPANY_CODE company_code
        ,BILLING_TYPE billing_type
        ,SALES_ORDER_NO sales_order_no
        ,ADD1 add1
        ,ADD2 add2
        ,ADD3 add3
        ,BSTNK bstnk
        ,ITEM_NO item_no
        FROM
            PHNX_SALES_DATA
        WHERE  trx_date={vEndDate}
    '''
    df = pd.read_sql(sql_command, con=engine)
    # print(df)


    connect_string = 'postgresql+psycopg2://apiuser:Api_Ibl_123_456@192.168.130.81:5433/DATAWAREHOUSE'
    engine1 = sqlalchemy.create_engine(connect_string)

    del_sql_statement = f'''delete from "DW"."phnx_sales_data"
                    where trx_date={vEndDate}
                '''
    engine1.execute(del_sql_statement)

    df.to_sql('phnx_sales_data',
            schema='DW',
            con=engine1,
            index=False,
            if_exists='append',
            )

insertSapSales()

